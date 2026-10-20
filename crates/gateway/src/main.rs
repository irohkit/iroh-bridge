//! Runs next to an existing HTTP backend (behind any NAT) and exposes it over iroh.

use std::{
    collections::HashSet,
    fmt::Write as _,
    future::Future,
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{
    Endpoint, EndpointId, RelayMode, RelayUrl, SecretKey, Watcher as _,
    endpoint::{Connection, QuicTransportConfig, RecvStream, SendStream, VarInt, presets},
};
use futures_util::{SinkExt, StreamExt};
use proto::{ALPN, RequestHead, ResponseHead, WsFrame};
use tokio_tungstenite::tungstenite::{
    self,
    client::IntoClientRequest,
    http::{HeaderName, HeaderValue},
    protocol::{CloseFrame, frame::coding::CloseCode},
    Message,
};
use tokio::sync::Semaphore;
use tracing::{info, warn};

const CHUNK_SIZE: usize = 64 * 1024;
/// How much request data one connection may have in flight across all its streams. QUIC's
/// default is unlimited, which would let a single client park `streams x 1.25 MB` of unread
/// uploads in gateway memory. 16 MiB still keeps several full-speed uploads going at once.
const CONN_RECEIVE_WINDOW: u32 = 16 * 1024 * 1024;

/// Hop-by-hop headers that must not be forwarded by a proxy. `content-length` and
/// `transfer-encoding` are always recomputed for the streamed body, so a client can't make the
/// two disagree (request smuggling); `host` comes from the upstream URL.
const HOP_HEADERS: &[&str] = &[
    "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "te",
    "trailer", "host", "content-length", "proxy-authorization", "proxy-authenticate", "expect",
];

/// Headers the WebSocket client library generates itself during the handshake.
const WS_HANDSHAKE_HEADERS: &[&str] = &[
    "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "sec-websocket-accept",
];

#[derive(Parser, Debug)]
#[command(about = "Expose a local HTTP backend over iroh")]
struct Args {
    /// Base URL of the HTTP backend to forward requests to. Several comma-separated URLs are
    /// load-balanced round-robin, skipping any that recently failed to accept a connection.
    /// With `--route`, this is the default for requests no route matches; without it, those get
    /// a 404. [default without routes: http://127.0.0.1:8080]
    #[arg(long, env = "BACKEND_URL")]
    backend: Option<String>,
    /// Route by host and path: `MATCH=UPSTREAMS`, where MATCH is `host`, `host/prefix` or
    /// `/prefix` (host may be `*`) and UPSTREAMS is a comma-separated list like BACKEND_URL.
    /// Repeatable; in the env var, separate entries with `;` or newlines. The most specific
    /// match wins. The host is taken from the client's `x-iroh-host` header.
    #[arg(long = "route", env = "IROH_ROUTES", value_delimiter = ';', value_name = "MATCH=UPSTREAMS")]
    routes: Vec<String>,
    /// Only accept connections from these endpoint ids (comma-separated). Default: anyone.
    #[arg(long = "allow-client", env = "IROH_ALLOW_CLIENTS", value_delimiter = ',', value_name = "ENDPOINT_ID")]
    allow_clients: Vec<String>,
    /// Only serve paths under these prefixes (comma-separated); others get 403. Default: all.
    #[arg(long = "allow-path", env = "IROH_ALLOW_PATHS", value_delimiter = ',', value_name = "PREFIX")]
    allow_paths: Vec<String>,
    /// How long an upstream that refused a connection is skipped before it is tried again.
    #[arg(long, env = "UPSTREAM_COOLDOWN_SECS", default_value_t = 10)]
    upstream_cooldown_secs: u64,
    /// File holding the endpoint's secret key. Created on first run so the endpoint id stays stable.
    #[arg(long, env = "IROH_KEY_FILE", default_value = "iroh-gateway.key")]
    key_file: PathBuf,
    /// Use your own relay server instead of the public n0 relays (recommended for production).
    #[arg(long, env = "IROH_RELAY_URL")]
    relay_url: Option<RelayUrl>,
    /// Token for a relay that only admits clients with a token (iroh-relay `access.shared_token`).
    #[arg(long, env = "IROH_RELAY_TOKEN", hide_env_values = true, requires = "relay_url")]
    relay_token: Option<String>,
    /// Maximum request body size in bytes.
    #[arg(long, env = "MAX_BODY_BYTES", default_value_t = 32 * 1024 * 1024)]
    max_body: usize,
    /// Maximum size of a request head (method, path and headers, as JSON) in bytes.
    #[arg(long, env = "MAX_HEAD_BYTES", default_value_t = 64 * 1024)]
    max_head: usize,
    /// Maximum size of one WebSocket message from a client, in bytes.
    #[arg(long, env = "MAX_WS_MESSAGE_BYTES", default_value_t = 16 * 1024 * 1024)]
    max_ws_message: usize,
    /// Maximum number of client connections (including ones still in the handshake).
    #[arg(long, env = "IROH_MAX_CONNECTIONS", default_value_t = 4096)]
    max_connections: usize,
    /// Maximum number of requests (streams) one connection may have open at the same time.
    /// Further ones wait in the client until one finishes.
    #[arg(long, env = "IROH_MAX_STREAMS_PER_CONNECTION", default_value_t = 100)]
    max_streams_per_connection: u32,
    /// Maximum number of requests and WebSockets in progress across all clients; above it new
    /// requests get 503.
    #[arg(long, env = "IROH_MAX_REQUESTS", default_value_t = 8192)]
    max_requests: usize,
    /// How long a client may take to send a request head once it opened the stream (0 = no limit).
    #[arg(long, env = "HEAD_TIMEOUT_SECS", default_value_t = 10)]
    head_timeout_secs: u64,
    /// How long a request body may stall between two chunks (0 = no limit).
    #[arg(long, env = "BODY_IDLE_TIMEOUT_SECS", default_value_t = 60)]
    body_idle_timeout_secs: u64,
    /// How long connecting to an upstream (and, for WebSockets, its handshake) may take.
    #[arg(long, env = "UPSTREAM_CONNECT_TIMEOUT_SECS", default_value_t = 10)]
    upstream_connect_timeout_secs: u64,
    /// How long an upstream may take to start its response, counted from the end of the
    /// request body (or its last chunk while it is still uploading). Gives 504 (0 = no limit).
    /// Streaming responses such as SSE are not limited once they started.
    #[arg(long, env = "UPSTREAM_TIMEOUT_SECS", default_value_t = 300)]
    upstream_timeout_secs: u64,
    /// Serve /healthz, /readyz, /status and /metrics over plain HTTP on this address, e.g.
    /// 127.0.0.1:9180. Off by default. It has no authentication, so only loopback addresses are
    /// accepted unless --admin-allow-public is given.
    #[arg(long, env = "IROH_ADMIN_ADDR", value_name = "ADDR")]
    admin_addr: Option<SocketAddr>,
    /// Allow --admin-addr to be a non-loopback address. It exposes upstream URLs and traffic
    /// figures to anyone who can reach it, so put a firewall or authenticating proxy in front.
    #[arg(long, env = "IROH_ADMIN_ALLOW_PUBLIC", requires = "admin_addr")]
    admin_allow_public: bool,
}

/// `0` in a timeout option means "no limit".
fn limit_secs(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_gateway=info".into()),
        )
        .init();
    let started = Instant::now();
    let args = Args::parse();
    let router = Router::parse(&args.routes, args.backend.as_deref(), Duration::from_secs(args.upstream_cooldown_secs))?;
    let allow_clients = args
        .allow_clients
        .iter()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
        .map(|id| id.parse::<EndpointId>().with_context(|| format!("invalid endpoint id in allow list: {id}")))
        .collect::<Result<HashSet<_>>>()?;
    let allow_paths: Vec<String> = args
        .allow_paths
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(parse_prefix)
        .collect::<Result<_>>()?;
    anyhow::ensure!(args.max_connections > 0 && args.max_requests > 0, "connection and request limits must be at least 1");
    anyhow::ensure!(args.max_streams_per_connection > 0, "--max-streams-per-connection must be at least 1");
    // Bound before anything else so a typo or a taken port fails the start, not just the listener.
    let admin_listener = match args.admin_addr {
        Some(addr) => {
            check_admin_addr(addr, args.admin_allow_public)?;
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding admin listener on {addr}"))?;
            Some(listener)
        }
        None => None,
    };
    let secret_key = load_or_create_key(&args.key_file)?;

    // The protocol only uses bidirectional streams, so clients get no unidirectional ones.
    let transport = QuicTransportConfig::builder()
        .max_concurrent_bidi_streams(VarInt::from_u32(args.max_streams_per_connection))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .receive_window(VarInt::from_u32(CONN_RECEIVE_WINDOW))
        .build();
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .transport_config(transport);
    if let Some(url) = args.relay_url.clone() {
        let mut relay = iroh::RelayConfig::from(url);
        if let Some(token) = args.relay_token.clone() {
            relay = relay.with_auth_token(token);
        }
        builder = builder.relay_mode(RelayMode::Custom(relay.into()));
    }
    let endpoint = builder.bind().await?;

    let upstream_connect_timeout = Duration::from_secs(args.upstream_connect_timeout_secs.max(1));
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(upstream_connect_timeout)
        .build()?;
    let ctx = Arc::new(Ctx {
        gateway_id: endpoint.id().to_string(),
        http,
        router,
        allow_clients,
        allow_paths,
        max_body: args.max_body,
        max_head: args.max_head,
        max_ws_message: args.max_ws_message,
        requests: Arc::new(Semaphore::new(args.max_requests)),
        max_requests: args.max_requests,
        head_timeout: limit_secs(args.head_timeout_secs),
        body_idle_timeout: limit_secs(args.body_idle_timeout_secs),
        upstream_connect_timeout,
        upstream_timeout: limit_secs(args.upstream_timeout_secs),
        busy_log: LogThrottle::default(),
        metrics: Metrics::default(),
    });

    // Started before waiting for a relay, so /healthz answers 503 during a slow start instead
    // of hanging.
    if let Some(listener) = admin_listener {
        let addr = listener.local_addr()?;
        if args.admin_allow_public && !is_loopback(addr) {
            warn!("admin listener on http://{addr} is reachable from the network and has no authentication");
        } else {
            info!("admin listener on http://{addr}");
        }
        let admin = Arc::new(Admin { ctx: ctx.clone(), endpoint: endpoint.clone(), relay_url: args.relay_url.clone(), started });
        tokio::spawn(serve_admin(listener, admin));
    }
    endpoint.online().await;

    ctx.router.log_config();
    if !ctx.allow_clients.is_empty() {
        let ids: Vec<_> = ctx.allow_clients.iter().map(|id| id.fmt_short().to_string()).collect();
        info!("accepting only clients {}", ids.join(", "));
    }
    if !ctx.allow_paths.is_empty() {
        info!("serving only paths under {}", ctx.allow_paths.iter().map(|p| if p.is_empty() { "/" } else { p.as_str() }).collect::<Vec<_>>().join(", "));
    }
    println!("\nIROH_ENDPOINT_ID={}\n", endpoint.id());

    let connections = Arc::new(Semaphore::new(args.max_connections));
    let full_log = LogThrottle::default();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };
                // Taken before the handshake, so a flood of handshakes counts against the limit too.
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    if full_log.allow() {
                        warn!("connection limit ({}) reached, refusing new connections", args.max_connections);
                    }
                    ctx.metrics.reject(Reject::ConnectionLimit);
                    incoming.refuse();
                    continue;
                };
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match incoming.await {
                        Ok(conn) => handle_connection(ctx, conn).await,
                        Err(err) => warn!("incoming connection failed: {err:#}"),
                    }
                });
            }
        }
    }
    info!("shutting down");
    endpoint.close().await;
    Ok(())
}

/// Resolves on Ctrl-C, and on SIGTERM where there is one: that is what systemd and
/// `docker stop` send, and the endpoint should still close cleanly then.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
                return;
            }
            Err(err) => warn!("cannot listen for SIGTERM, only Ctrl-C stops the gateway: {err}"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// Rate-limits a warning that an abusive client could otherwise trigger thousands of times a
/// second, so it can't flood the log.
#[derive(Default)]
struct LogThrottle(Mutex<Option<Instant>>);

impl LogThrottle {
    fn allow(&self) -> bool {
        let mut last = self.0.lock().unwrap();
        if last.is_some_and(|at| at.elapsed() < Duration::from_secs(10)) {
            return false;
        }
        *last = Some(Instant::now());
        true
    }
}

struct Ctx {
    /// This gateway's endpoint id, returned as `X-Iroh-Gateway-Id` so clients with several
    /// replicas can tell which one answered.
    gateway_id: String,
    http: reqwest::Client,
    router: Router,
    /// Endpoint ids allowed to connect; empty means anyone.
    allow_clients: HashSet<EndpointId>,
    /// Path prefixes (normalized by `parse_prefix`) that may be requested; empty means all.
    allow_paths: Vec<String>,
    max_body: usize,
    max_head: usize,
    max_ws_message: usize,
    /// One permit per request in progress, across all connections.
    requests: Arc<Semaphore>,
    max_requests: usize,
    head_timeout: Option<Duration>,
    body_idle_timeout: Option<Duration>,
    upstream_connect_timeout: Duration,
    upstream_timeout: Option<Duration>,
    busy_log: LogThrottle,
    metrics: Metrics,
}

/// Picks the upstreams for a request from its host and path.
struct Router {
    /// Sorted most specific first, so the first match is the best one.
    routes: Vec<Route>,
    /// Where requests no route matches go; `None` answers them with 404.
    default: Option<Upstreams>,
}

struct Route {
    /// The MATCH part as configured, for logs.
    label: String,
    /// Lowercase host without port; `None` matches any host.
    host: Option<String>,
    /// Path prefix as normalized by `parse_prefix`; empty matches every path.
    prefix: String,
    upstreams: Upstreams,
}

impl Router {
    fn parse(entries: &[String], backend: Option<&str>, cooldown: Duration) -> Result<Self> {
        let mut routes = Vec::new();
        // clap only splits the env var on `;`, so newline-separated entries are split here.
        for entry in entries.iter().flat_map(|e| e.lines()).map(str::trim).filter(|e| !e.is_empty()) {
            let (matcher, list) = entry
                .split_once('=')
                .with_context(|| format!("route {entry:?} must look like MATCH=UPSTREAMS"))?;
            let matcher = matcher.trim();
            let (host, path) = match matcher.find('/') {
                Some(i) => (&matcher[..i], &matcher[i..]),
                None => (matcher, ""),
            };
            anyhow::ensure!(!host.is_empty() || !path.is_empty(), "route {entry:?} has an empty match");
            let host = match host.to_ascii_lowercase() {
                h if h.is_empty() || h == "*" => None,
                h => Some(h.trim_end_matches('.').to_string()),
            };
            let route = Route {
                label: matcher.to_string(),
                host,
                prefix: parse_prefix(if path.is_empty() { "/" } else { path })?,
                upstreams: Upstreams::parse(list, cooldown).with_context(|| format!("route {matcher}"))?,
            };
            anyhow::ensure!(
                !routes.iter().any(|r: &Route| r.host == route.host && r.prefix == route.prefix),
                "duplicate route {matcher}"
            );
            routes.push(route);
        }
        // A host match beats any path-only match; within each group the longer prefix wins.
        routes.sort_by_key(|r| std::cmp::Reverse((r.host.is_some(), r.prefix.len())));
        // Without routes keep the historical default backend; with routes, an implicit default
        // would silently expose a local service for every unknown host.
        let default = match backend {
            Some(list) => Some(Upstreams::parse(list, cooldown)?),
            None if routes.is_empty() => Some(Upstreams::parse("http://127.0.0.1:8080", cooldown)?),
            None => None,
        };
        Ok(Self { routes, default })
    }

    fn log_config(&self) {
        if self.routes.is_empty() {
            if let Some(default) = &self.default {
                info!("forwarding to {}", default.urls.join(", "));
            }
            return;
        }
        for r in &self.routes {
            info!("route {} -> {}", r.label, r.upstreams.urls.join(", "));
        }
        match &self.default {
            Some(default) => info!("route default -> {}", default.urls.join(", ")),
            None => info!("no default backend: unmatched requests get 404"),
        }
    }

    /// Returns the matched route's label and upstreams. `host` must already be normalized.
    fn select(&self, host: Option<&str>, path: &str) -> Option<(&str, &Upstreams)> {
        self.routes
            .iter()
            .find(|r| r.host.as_deref().is_none_or(|h| Some(h) == host) && prefix_matches(&r.prefix, path))
            .map(|r| (r.label.as_str(), &r.upstreams))
            .or_else(|| self.default.as_ref().map(|d| ("default", d)))
    }

    /// Every route with its label, the default backend last, for the admin listener.
    fn all(&self) -> impl Iterator<Item = (&str, &Upstreams)> {
        self.routes
            .iter()
            .map(|r| (r.label.as_str(), &r.upstreams))
            .chain(self.default.as_ref().map(|d| ("default", d)))
    }
}

/// Normalizes a configured path prefix so matching is a plain comparison: no trailing slash,
/// and `/` becomes the empty prefix that matches everything.
fn parse_prefix(prefix: &str) -> Result<String> {
    anyhow::ensure!(prefix.starts_with('/'), "path prefix {prefix:?} must start with /");
    anyhow::ensure!(!prefix.contains(['?', '#']), "path prefix {prefix:?} must not contain ? or #");
    Ok(prefix.trim_end_matches('/').to_string())
}

/// Matches on segment boundaries, so `/admin` covers `/admin` and `/admin/x` but not
/// `/administrator`.
fn prefix_matches(prefix: &str, path: &str) -> bool {
    path.strip_prefix(prefix).is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// The path part of a request target, without query or fragment.
fn path_only(target: &str) -> &str {
    target.split(['?', '#']).next().unwrap_or_default()
}

/// Checks everything in a request head that ends up in the upstream request line or headers,
/// so a client can't inject a second request, reach another host or confuse routing.
fn validate_head(head: &RequestHead) -> Result<(), &'static str> {
    if !head.websocket && !valid_method(&head.method) {
        return Err("invalid method");
    }
    if !valid_target(&head.path) || has_dot_segment(path_only(&head.path)) {
        return Err("invalid path");
    }
    if !head.headers.iter().all(|(k, v)| valid_header_name(k) && valid_header_value(v)) {
        return Err("invalid header");
    }
    if request_host(&head.headers).is_some_and(|h| !valid_host(&h)) {
        return Err("invalid x-iroh-host");
    }
    Ok(())
}

/// RFC 9110 token characters, used by methods and header names.
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// A method token. CONNECT would turn the backend connection into a tunnel and TRACE reflects
/// request headers (including cookies) into the response, so neither is forwarded.
fn valid_method(method: &str) -> bool {
    !method.is_empty()
        && method.len() <= 32
        && method.bytes().all(is_tchar)
        && !method.eq_ignore_ascii_case("CONNECT")
        && !method.eq_ignore_ascii_case("TRACE")
}

/// An origin-form target: `/`, then printable ASCII only. The upstream URL is built by
/// appending this to the upstream base, so anything not starting with `/` (e.g. `@evil.com`)
/// could change the host. Spaces, controls and non-ASCII are rejected rather than escaped,
/// because URL parsers silently drop tabs and newlines, which would let `/.%09./admin` turn
/// into `/../admin` after the dot-segment check. Backslashes and `#` never come from real
/// clients (they normalize them) and are read differently by different servers.
fn valid_target(target: &str) -> bool {
    target.starts_with('/') && target.bytes().all(|b| b.is_ascii_graphic() && b != b'\\' && b != b'#')
}

/// Whether a path has `.`/`..` segments, also percent-encoded (once or twice), as overlong or
/// fullwidth UTF-8, with backslashes or with `;params` (`..;/`, read as `..` by some Java
/// servers). A backend may resolve them after the gateway matched the raw path, so
/// `/public/../admin` would slip past both routing and the path allow list. Real clients
/// normalize these away before sending.
fn has_dot_segment(path: &str) -> bool {
    let mut decoded = path.to_ascii_lowercase().replace("%25", "%");
    for (from, to) in [
        ("%2e", "."), ("%c0%ae", "."), ("%e0%80%ae", "."), ("%ef%bc%8e", "."),
        ("%2f", "/"), ("%5c", "/"), ("%c0%af", "/"), ("\\", "/"),
    ] {
        decoded = decoded.replace(from, to);
    }
    decoded.split('/').map(|s| s.split(';').next().unwrap_or_default()).any(|s| s == "." || s == "..")
}

fn valid_header_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 256 && name.bytes().all(is_tchar)
}

/// No CR, LF, NUL or other control characters (tab is allowed), so a value can't end the
/// header line and start another one.
fn valid_header_value(value: &str) -> bool {
    value.bytes().all(|b| b == b'\t' || (b >= 0x20 && b != 0x7f))
}

/// What `x-iroh-host` may hold: a host name or IP literal with an optional port. It is passed
/// on as `X-Forwarded-Host`, which backends use to build absolute URLs.
fn valid_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 255
        && host.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._:[]".contains(&b))
}

/// The `x-iroh-host` header as sent: the host the frontend's URL used.
fn request_host(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-iroh-host"))
        .map(|(_, v)| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Lowercases a host and drops a port and trailing dot, so `API.example.com.:443` matches a
/// route for `api.example.com`.
fn normalize_host(host: &str) -> String {
    let host = host.to_ascii_lowercase();
    let host = match host.strip_prefix('[') {
        // IPv6 literal: keep the brackets, drop what follows them.
        Some(rest) => rest.split_once(']').map_or(host.clone(), |(addr, _)| format!("[{addr}]")),
        None => match host.rsplit_once(':') {
            Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => h.to_string(),
            _ => host,
        },
    };
    host.trim_end_matches('.').to_string()
}

/// A client-supplied string for a log line: escaped (no forged log lines) and cut short.
fn for_log(s: &str) -> String {
    let mut end = s.len().min(200);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    let ellipsis = if end < s.len() { "…" } else { "" };
    format!("{:?}{ellipsis}", &s[..end])
}

/// A request path for the log, without its query string: queries often carry secrets
/// (`?access_token=`, signed URLs, API keys) that must not end up in log files.
fn path_for_log(path: &str) -> String {
    match path.split_once('?') {
        Some((p, _)) => format!("{}?…", for_log(p).trim_matches('"')),
        None => for_log(path).trim_matches('"').to_string(),
    }
}

/// The backends a request may go to, with passive health tracking: an upstream is only judged
/// by whether it accepts connections, since any HTTP answer (even a 5xx) is the app's own reply.
struct Upstreams {
    urls: Vec<String>,
    /// When each upstream last failed to connect; `None` once it has served a request again.
    failed: Mutex<Vec<Option<Instant>>>,
    next: AtomicUsize,
    cooldown: Duration,
    /// Connect failures per upstream, for `iroh_gateway_upstream_failures_total`.
    failures: Vec<AtomicU64>,
    /// Traffic of the route these upstreams belong to.
    stats: RouteStats,
}

impl Upstreams {
    fn parse(list: &str, cooldown: Duration) -> Result<Self> {
        let urls: Vec<String> = list
            .split(',')
            .map(|u| u.trim().trim_end_matches('/').to_string())
            .filter(|u| !u.is_empty())
            .collect();
        anyhow::ensure!(!urls.is_empty(), "no backend URL given");
        Ok(Self {
            failed: Mutex::new(vec![None; urls.len()]),
            failures: urls.iter().map(|_| AtomicU64::new(0)).collect(),
            urls,
            next: AtomicUsize::new(0),
            cooldown,
            stats: RouteStats::default(),
        })
    }

    /// The cooldown left for each upstream; `None` means healthy.
    fn health(&self) -> Vec<Option<Duration>> {
        let failed = self.failed.lock().unwrap();
        failed
            .iter()
            .map(|at| at.and_then(|at| self.cooldown.checked_sub(at.elapsed())).filter(|left| !left.is_zero()))
            .collect()
    }

    /// The order to try upstreams in for one request: healthy ones round-robin, then the ones in
    /// cooldown, least recently failed first. Those are still tried so that a full outage
    /// recovers as soon as any backend is back, rather than after the cooldown.
    fn order(&self) -> Vec<usize> {
        let n = self.urls.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let failed = self.failed.lock().unwrap();
        let (mut healthy, mut cooling) = (Vec::new(), Vec::new());
        for i in (0..n).map(|k| (start + k) % n) {
            match failed[i] {
                Some(at) if at.elapsed() < self.cooldown => cooling.push((at, i)),
                _ => healthy.push(i),
            }
        }
        cooling.sort();
        healthy.extend(cooling.into_iter().map(|(_, i)| i));
        healthy
    }

    fn mark_failed(&self, i: usize, err: &dyn std::fmt::Display) {
        self.failures[i].fetch_add(1, Ordering::Relaxed);
        let mut failed = self.failed.lock().unwrap();
        // Only announce the transition; concurrent requests hitting the same dead upstream would
        // otherwise log it once each.
        if !failed[i].is_some_and(|at| at.elapsed() < self.cooldown) {
            warn!("upstream {} marked unhealthy for {:?}: {err}", self.urls[i], self.cooldown);
        }
        failed[i] = Some(Instant::now());
    }

    fn mark_ok(&self, i: usize) {
        let mut failed = self.failed.lock().unwrap();
        if failed[i].take().is_some() {
            info!("upstream {} recovered", self.urls[i]);
        }
    }
}

async fn handle_connection(ctx: Arc<Ctx>, conn: Connection) {
    let remote = conn.remote_id();
    // The id is only known once the handshake has verified it, so refuse right after.
    if !ctx.allow_clients.is_empty() && !ctx.allow_clients.contains(&remote) {
        warn!(remote = %remote.fmt_short(), "client not in allow list, closing connection");
        ctx.metrics.reject(Reject::ClientNotAllowed);
        conn.close(VarInt::from_u32(1), b"not allowed");
        return;
    }
    info!(remote = %remote.fmt_short(), "client connected");
    let _connected = GaugeGuard::new(&ctx.metrics.connections);
    // Each request arrives on its own bidirectional stream, so requests run concurrently.
    while let Ok((mut send, recv)) = conn.accept_bi().await {
        let ctx = ctx.clone();
        let Ok(permit) = ctx.requests.clone().try_acquire_owned() else {
            if ctx.busy_log.allow() {
                warn!("request limit ({}) reached, answering 503", ctx.max_requests);
            }
            ctx.metrics.reject(Reject::TooManyRequests);
            Outcome::new(&ctx.metrics).head(503);
            // Answer without reading the head; dropping `recv` discards whatever else comes.
            tokio::spawn(async move {
                let _ = respond_text(&ctx, &mut send, 503, "gateway busy, try again").await;
                drop(recv);
            });
            continue;
        };
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_request(&ctx, remote, &mut send, recv).await {
                warn!(remote = %remote.fmt_short(), "request failed: {err:#}");
                // Dropping a send stream finishes it, which would make a cut-off response look
                // complete to the client. Resetting tells it the response is broken.
                let _ = send.reset(VarInt::from_u32(1));
            }
        });
    }
    info!(remote = %remote.fmt_short(), "client disconnected");
}

/// Runs `fut` with an optional time limit; `None` means it ran out.
async fn with_limit<F: Future>(limit: Option<Duration>, fut: F) -> Option<F::Output> {
    match limit {
        Some(limit) => tokio::time::timeout(limit, fut).await.ok(),
        None => Some(fut.await),
    }
}

async fn handle_request(ctx: &Ctx, remote: EndpointId, send: &mut SendStream, mut recv: RecvStream) -> Result<()> {
    let mut out = Outcome::new(&ctx.metrics);
    let mut head: RequestHead = match with_limit(ctx.head_timeout, proto::read_head_limited(&mut recv, ctx.max_head)).await {
        Some(Ok(head)) => head,
        Some(Err(err)) => {
            info!(remote = %remote.fmt_short(), "unreadable request head: {err:#}");
            return match err.downcast_ref::<proto::TooLarge>() {
                Some(_) => reply(ctx, &mut out, send, Reject::HeadTooLarge, 431, "request head too large").await,
                None => reply(ctx, &mut out, send, Reject::BadRequest, 400, "bad request: malformed head").await,
            };
        }
        None => {
            info!(remote = %remote.fmt_short(), "request head not received in time");
            return reply(ctx, &mut out, send, Reject::HeadTimeout, 408, "request head timeout").await;
        }
    };
    if let Err(reason) = validate_head(&head) {
        info!("{} {} -> 400 ({reason})", for_log(&head.method), path_for_log(&head.path));
        return reply(ctx, &mut out, send, Reject::BadRequest, 400, &format!("bad request: {reason}")).await;
    }
    // From here on method, path and headers are known to be printable, so they can be logged as-is.
    let method = if head.websocket { "WS" } else { head.method.as_str() };
    let host = request_host(&head.headers);
    let path = path_only(&head.path);
    // Routing and the allow list both look at the validated path. WebSockets get the same
    // checks, before any upstream is contacted.
    if !ctx.allow_paths.is_empty() && !ctx.allow_paths.iter().any(|p| prefix_matches(p, path)) {
        info!("{method} {} -> 403 (path not allowed)", path_for_log(&head.path));
        return reply(ctx, &mut out, send, Reject::PathNotAllowed, 403, "forbidden").await;
    }
    let Some((route, upstreams)) = ctx.router.select(host.as_deref().map(normalize_host).as_deref(), path) else {
        info!("{method} {} -> 404 (no route for host {})", path_for_log(&head.path), host.as_deref().unwrap_or("-"));
        return reply(ctx, &mut out, send, Reject::NoRoute, 404, "no route for this host or path").await;
    };
    out.stats = Some(&upstreams.stats);
    head.headers = proxy_headers(head.headers, remote, host.as_deref());
    if head.websocket {
        return handle_websocket(ctx, upstreams, route, head, send, recv, &mut out).await;
    }
    let upload = Arc::new(Upload::new());
    let body = match with_limit(ctx.body_idle_timeout, recv.read_chunk(CHUNK_SIZE)).await {
        Some(first) => request_body(recv, first?, ctx.max_body, ctx.body_idle_timeout, upload.clone()),
        None => {
            info!("{} {} -> 408 (no request body received)", head.method, path_for_log(&head.path));
            return reply(ctx, &mut out, send, Reject::BodyTimeout, 408, "request body timeout").await;
        }
    };

    // Stop waiting as soon as the client is gone (aborted, or its connection dropped), so an
    // abandoned request doesn't keep an upstream connection and a request slot busy.
    let client_gone = send.stopped();
    let answer = tokio::select! {
        answer = upstream_deadline(forward_any(ctx, upstreams, &head, body), ctx.upstream_timeout, &upload) => answer,
        _ = client_gone => {
            info!("{} {} -> client went away while waiting for the backend", head.method, path_for_log(&head.path));
            return Ok(());
        }
    };
    let (resp, upstream) = match answer {
        Some(Ok(ok)) => ok,
        Some(Err(err)) => {
            // The details (upstream URL, OS error) go to the log only, never to the client.
            let (status, text) = match upload.failure() {
                Some(failure) => failure,
                None => (502, "bad gateway"),
            };
            match status {
                413 => ctx.metrics.reject(Reject::BodyTooLarge),
                408 => ctx.metrics.reject(Reject::BodyTimeout),
                _ => {}
            }
            warn!("{} {} -> {status} (route {route}): {err:#}", head.method, path_for_log(&head.path));
            out.head(status);
            return respond_text(ctx, send, status, text).await;
        }
        None => {
            warn!("{} {} -> 504: backend did not answer in time (route {route})", head.method, path_for_log(&head.path));
            return reply(ctx, &mut out, send, Reject::UpstreamTimeout, 504, "gateway timeout").await;
        }
    };
    info!("{} {} -> {} via {upstream} (route {route})", head.method, path_for_log(&head.path), resp.status().as_u16());
    out.head(resp.status().as_u16());

    let mut headers = header_pairs(resp.headers());
    headers.push(("x-iroh-gateway-id".into(), ctx.gateway_id.clone()));
    proto::write_head(send, &ResponseHead { status: resp.status().as_u16(), headers }).await?;

    // Stream the body through so large or long-lived responses don't sit in memory. A quiet
    // stream (e.g. SSE between events) is still dropped as soon as the client stops reading it.
    let mut resp = resp;
    let client_gone = send.stopped();
    tokio::pin!(client_gone);
    loop {
        let chunk = tokio::select! {
            chunk = resp.chunk() => chunk?,
            _ = &mut client_gone => return Ok(()),
        };
        let Some(chunk) = chunk else { break };
        send.write_all(&chunk).await?;
    }
    send.finish()?;
    Ok(())
}

/// Progress of a request body, shared between the upload stream and the code waiting for the
/// backend's answer: when the upload last moved, and why it failed if the gateway cut it off.
struct Upload {
    started: Instant,
    last_ms: AtomicU64,
    failure: AtomicU16,
}

impl Upload {
    fn new() -> Self {
        Self { started: Instant::now(), last_ms: AtomicU64::new(0), failure: AtomicU16::new(0) }
    }

    fn touch(&self) {
        self.last_ms.store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn last_activity(&self) -> Instant {
        self.started + Duration::from_millis(self.last_ms.load(Ordering::Relaxed))
    }

    fn fail(&self, status: u16) {
        self.failure.store(status, Ordering::Relaxed);
    }

    /// The status to answer with when the gateway itself aborted the upload.
    fn failure(&self) -> Option<(u16, &'static str)> {
        match self.failure.load(Ordering::Relaxed) {
            413 => Some((413, "request body too large")),
            408 => Some((408, "request body timeout")),
            _ => None,
        }
    }
}

/// Waits for the backend's response head. The time limit restarts whenever the upload makes
/// progress, so a large upload on a slow link is never cut off while it is still moving.
async fn upstream_deadline<F: Future>(fut: F, limit: Option<Duration>, upload: &Upload) -> Option<F::Output> {
    let Some(limit) = limit else { return Some(fut.await) };
    tokio::pin!(fut);
    loop {
        let deadline = upload.last_activity() + limit;
        tokio::select! {
            // An answer that is ready wins over a deadline that passed at the same time.
            biased;
            res = &mut fut => return Some(res),
            _ = tokio::time::sleep_until(deadline.into()) => {
                if upload.last_activity() + limit <= Instant::now() {
                    return None;
                }
            }
        }
    }
}

async fn handle_websocket(
    ctx: &Ctx,
    upstreams: &Upstreams,
    route: &str,
    head: RequestHead,
    send: &mut SendStream,
    mut recv: RecvStream,
    out: &mut Outcome<'_>,
) -> Result<()> {
    // A handshake has no body, so a connect failure can always move on to the next upstream.
    let mut attempt = None;
    for i in upstreams.order() {
        let upstream = &upstreams.urls[i];
        let req = ws_request(upstream, &head)?;
        let res = match tokio::time::timeout(ctx.upstream_connect_timeout, tokio_tungstenite::connect_async(req)).await {
            Ok(res) => res,
            Err(_) => Err(tungstenite::Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))),
        };
        match res {
            Err(err @ tungstenite::Error::Io(_)) => {
                upstreams.mark_failed(i, &err);
                attempt = Some((upstream, Err(err)));
            }
            res => {
                upstreams.mark_ok(i);
                attempt = Some((upstream, res));
                break;
            }
        }
    }
    let (upstream, res) = attempt.expect("at least one upstream");

    let (ws, resp) = match res {
        Ok(ok) => ok,
        Err(err) => {
            // Pass the backend's refusal (e.g. 401/404) through; anything else is a 502 whose
            // details stay in the log.
            let (status, headers, body) = match err {
                tungstenite::Error::Http(resp) => {
                    warn!("WS {} -> {} via {upstream} (route {route})", path_for_log(&head.path), resp.status().as_u16());
                    upstreams.stats.ws_result(WsResult::Refused);
                    (resp.status().as_u16(), header_pairs(resp.headers()), resp.body().clone().unwrap_or_default())
                }
                err => {
                    warn!("WS {} -> 502 via {upstream} (route {route}): {err}", path_for_log(&head.path));
                    upstreams.stats.ws_result(WsResult::Failed);
                    (502, vec![("content-type".into(), "text/plain".into())], b"bad gateway".to_vec())
                }
            };
            out.head(status);
            proto::write_head(send, &ResponseHead { status, headers }).await?;
            send.write_all(&body).await?;
            send.finish()?;
            return Ok(());
        }
    };
    info!("WS {} -> 101 via {upstream} (route {route})", path_for_log(&head.path));
    upstreams.stats.ws_result(WsResult::Upgraded);
    out.head(101);
    let headers = header_pairs(resp.headers())
        .into_iter()
        .filter(|(k, _)| !WS_HANDSHAKE_HEADERS.contains(&k.as_str()))
        .collect();
    proto::write_head(send, &ResponseHead { status: 101, headers }).await?;

    let (mut ws_tx, mut ws_rx) = ws.split();
    let max_message = ctx.max_ws_message;
    let client_to_backend = async {
        loop {
            let frame = match proto::read_frame_limited(&mut recv, max_message).await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(err) => {
                    // Tell the backend why, like a WebSocket server would.
                    let code = if err.downcast_ref::<proto::TooLarge>().is_some() { 1009 } else { 1002 };
                    let _ = ws_tx.send(close_message(code, String::new())).await;
                    return Err(err);
                }
            };
            let msg = match frame {
                WsFrame::Text(t) => Message::text(t),
                WsFrame::Binary(b) => Message::binary(b),
                WsFrame::Close(code, reason) => {
                    ws_tx.send(close_message(code, reason)).await?;
                    break;
                }
            };
            ws_tx.send(msg).await?;
        }
        ws_tx.close().await.ok();
        anyhow::Ok(())
    };
    let backend_to_client = async {
        let mut close = WsFrame::Close(1006, "backend connection lost".into());
        while let Some(msg) = ws_rx.next().await {
            match msg? {
                Message::Text(t) => proto::write_frame(send, &WsFrame::Text(t.to_string())).await?,
                Message::Binary(b) => proto::write_frame(send, &WsFrame::Binary(b.to_vec())).await?,
                Message::Close(frame) => {
                    close = match frame {
                        Some(f) => WsFrame::Close(f.code.into(), f.reason.to_string()),
                        None => WsFrame::Close(1005, String::new()),
                    };
                    break;
                }
                _ => {} // ping/pong are answered by tungstenite
            }
        }
        proto::write_frame(send, &close).await?;
        send.finish()?;
        anyhow::Ok(())
    };
    // The tunnel ends when the backend side is done. If the client starts the closing handshake,
    // keep relaying until the backend's close reply has been forwarded (or a grace period passes).
    let res = {
        tokio::pin!(client_to_backend, backend_to_client);
        tokio::select! {
            res = &mut backend_to_client => res,
            res = &mut client_to_backend => {
                res?;
                tokio::time::timeout(Duration::from_secs(5), backend_to_client).await.unwrap_or(Ok(()))
            }
        }
    };
    // Flush tungstenite's queued close reply so the backend sees a clean close.
    let _ = tokio::time::timeout(Duration::from_secs(2), ws_tx.close()).await;
    res
}

/// A close frame built from a client's code and reason that is valid on the wire: codes an
/// endpoint must not send (1005, 1006, 1015, < 1000, ...) become a close without a code, and the
/// reason is cut to the 123 bytes a control frame has room for.
fn close_message(code: u16, reason: String) -> Message {
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
        return Message::Close(None);
    }
    let mut end = reason.len().min(123);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    let mut reason = reason;
    reason.truncate(end);
    Message::Close(Some(CloseFrame { code: CloseCode::from(code), reason: reason.into() }))
}

/// Answers a request with a short plain-text body, for errors the gateway produces itself.
async fn respond_text(ctx: &Ctx, send: &mut SendStream, status: u16, body: &str) -> Result<()> {
    let head = ResponseHead {
        status,
        headers: vec![("content-type".into(), "text/plain".into()), ("x-iroh-gateway-id".into(), ctx.gateway_id.clone())],
    };
    proto::write_head(send, &head).await?;
    send.write_all(body.as_bytes()).await?;
    send.finish()?;
    Ok(())
}

/// `respond_text` for a request the gateway turns away, counted under `reason`.
async fn reply(ctx: &Ctx, out: &mut Outcome<'_>, send: &mut SendStream, reason: Reject, status: u16, body: &str) -> Result<()> {
    ctx.metrics.reject(reason);
    out.head(status);
    respond_text(ctx, send, status, body).await
}

fn ws_request(upstream: &str, head: &RequestHead) -> Result<tungstenite::handshake::client::Request> {
    let base = upstream.replacen("http://", "ws://", 1).replacen("https://", "wss://", 1);
    let mut req = format!("{base}{}", head.path).into_client_request()?;
    for (k, v) in &head.headers {
        let lower = k.to_ascii_lowercase();
        if HOP_HEADERS.contains(&lower.as_str()) || WS_HANDSHAKE_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        req.headers_mut().append(HeaderName::from_bytes(k.as_bytes())?, HeaderValue::from_str(v)?);
    }
    Ok(req)
}

/// Response headers to pass to the client: no hop-by-hop ones, and no `x-iroh-gateway-id` from
/// the backend, since the gateway sets its own.
fn header_pairs(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(k, _)| !HOP_HEADERS.contains(&k.as_str()) && k.as_str() != "x-iroh-gateway-id")
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_string())))
        .collect()
}

/// Streams the rest of the QUIC stream to the backend as the request body, so uploads never
/// sit in memory. `first` is the chunk already read; `None` (an empty body) keeps GET requests
/// body-less. A body over `max_body` or one that stalls for `idle` is aborted, which the
/// backend sees as an incomplete request, never as a complete shorter one.
fn request_body(
    recv: RecvStream,
    first: Option<bytes::Bytes>,
    max_body: usize,
    idle: Option<Duration>,
    upload: Arc<Upload>,
) -> Option<reqwest::Body> {
    let first = first?;
    let chunks = futures_util::stream::try_unfold(
        (recv, Some(first), 0usize),
        move |(mut recv, pending, total)| {
            let upload = upload.clone();
            async move {
                let chunk = match pending {
                    Some(chunk) => Some(chunk),
                    None => match with_limit(idle, recv.read_chunk(CHUNK_SIZE)).await {
                        Some(chunk) => chunk.map_err(std::io::Error::other)?,
                        None => {
                            upload.fail(408);
                            // Not `ErrorKind::TimedOut`: reqwest would report that as a timeout,
                            // and the upstream would be marked unhealthy for the client's stall.
                            return Err(std::io::Error::other("request body stalled"));
                        }
                    },
                };
                upload.touch();
                let Some(chunk) = chunk else { return Ok(None) };
                let total = total + chunk.len();
                if total > max_body {
                    upload.fail(413);
                    return Err(std::io::Error::other(format!("request body exceeds {max_body} bytes")));
                }
                Ok(Some((chunk, (recv, None, total))))
            }
        },
    );
    Some(reqwest::Body::wrap_stream(chunks))
}

/// Headers set by the gateway itself. Clients can't supply them, so the backend can trust them.
/// `x-iroh-host` is the client's routing hint; it reaches the backend as `X-Forwarded-Host`.
/// Every `x-forwarded-*` and `x-iroh-*` header is dropped too, plus the client-address headers
/// other proxies set, since backends behind such proxies tend to trust them.
const GATEWAY_HEADERS: &[&str] = &["forwarded", "x-real-ip", "true-client-ip", "x-client-ip"];
const GATEWAY_HEADER_PREFIXES: &[&str] = &["x-forwarded-", "x-iroh-"];

fn is_gateway_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    GATEWAY_HEADERS.contains(&name.as_str()) || GATEWAY_HEADER_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Drops headers a client must not spoof and adds what a reverse proxy normally adds:
/// `X-Iroh-Remote-Id` (the caller's endpoint id, verified by the QUIC handshake) and
/// `X-Forwarded-Proto: https` (the tunnel is end-to-end encrypted), plus `X-Forwarded-Host`
/// when the client said which host it used. Headers the client listed in `Connection` are
/// hop-by-hop by definition and dropped as well.
fn proxy_headers(headers: Vec<(String, String)>, remote: EndpointId, host: Option<&str>) -> Vec<(String, String)> {
    let listed: HashSet<String> = headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, v)| v.split(','))
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let mut out: Vec<_> = headers
        .into_iter()
        .filter(|(k, _)| !is_gateway_header(k) && !listed.contains(&k.to_ascii_lowercase()))
        .collect();
    out.push(("x-iroh-remote-id".into(), remote.to_string()));
    out.push(("x-forwarded-proto".into(), "https".into()));
    if let Some(host) = host {
        out.push(("x-forwarded-host".into(), host.to_string()));
    }
    out
}

/// Sends the request to the first upstream that accepts a connection. Only body-less requests
/// fail over: a streamed body is consumed by the first attempt and can't be sent again.
async fn forward_any<'a>(
    ctx: &Ctx,
    upstreams: &'a Upstreams,
    head: &RequestHead,
    mut body: Option<reqwest::Body>,
) -> Result<(reqwest::Response, &'a str)> {
    let mut last_err = None;
    for i in upstreams.order() {
        let upstream = &upstreams.urls[i];
        let replayable = body.is_none();
        match forward(ctx, upstream, head, body.take()).await {
            Ok(resp) => {
                upstreams.mark_ok(i);
                return Ok((resp, upstream));
            }
            Err(err) => {
                let connect = err.downcast_ref::<reqwest::Error>().is_some_and(|e| e.is_connect() || e.is_timeout());
                if !connect {
                    return Err(err);
                }
                upstreams.mark_failed(i, &format!("{err:#}"));
                if !replayable {
                    return Err(err);
                }
                last_err = Some(err);
            }
        }
    }
    Err(last_err.expect("at least one upstream"))
}

async fn forward(ctx: &Ctx, upstream: &str, head: &RequestHead, body: Option<reqwest::Body>) -> Result<reqwest::Response> {
    // `validate_head` already guarantees this; it is what keeps the target on `upstream`'s host.
    anyhow::ensure!(head.path.starts_with('/'), "path must start with /");
    let method = reqwest::Method::from_bytes(head.method.as_bytes())?;
    let mut req = ctx.http.request(method, format!("{upstream}{}", head.path));
    for (k, v) in &head.headers {
        if !HOP_HEADERS.contains(&k.to_ascii_lowercase().as_str()) {
            req = req.header(k, v);
        }
    }
    if let Some(body) = body {
        req = req.body(body);
    }
    Ok(req.send().await?)
}

/// Why the gateway turned a connection or request away, for `iroh_gateway_rejected_total`.
#[derive(Clone, Copy)]
enum Reject {
    ClientNotAllowed,
    ConnectionLimit,
    TooManyRequests,
    BadRequest,
    HeadTooLarge,
    HeadTimeout,
    PathNotAllowed,
    NoRoute,
    BodyTooLarge,
    BodyTimeout,
    UpstreamTimeout,
}

impl Reject {
    const ALL: [Reject; 11] = [
        Reject::ClientNotAllowed, Reject::ConnectionLimit, Reject::TooManyRequests, Reject::BadRequest,
        Reject::HeadTooLarge, Reject::HeadTimeout, Reject::PathNotAllowed, Reject::NoRoute,
        Reject::BodyTooLarge, Reject::BodyTimeout, Reject::UpstreamTimeout,
    ];

    fn label(self) -> &'static str {
        match self {
            Reject::ClientNotAllowed => "client_not_allowed",
            Reject::ConnectionLimit => "connection_limit",
            Reject::TooManyRequests => "too_many_requests",
            Reject::BadRequest => "bad_request",
            Reject::HeadTooLarge => "head_too_large",
            Reject::HeadTimeout => "head_timeout",
            Reject::PathNotAllowed => "path_not_allowed",
            Reject::NoRoute => "no_route",
            Reject::BodyTooLarge => "body_too_large",
            Reject::BodyTimeout => "body_timeout",
            Reject::UpstreamTimeout => "upstream_timeout",
        }
    }
}

/// How a WebSocket handshake with the backend ended; indexes `WS_RESULTS`.
#[derive(Clone, Copy)]
enum WsResult {
    Upgraded,
    /// The backend answered with an HTTP status instead of upgrading.
    Refused,
    /// No backend could be reached.
    Failed,
}

const WS_RESULTS: [&str; 3] = ["upgraded", "refused", "failed"];
const STATUS_CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];
/// Upper bounds of the request duration histogram in seconds; one more bucket takes the rest.
const DURATION_BUCKETS: [f64; 9] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
/// Route label for requests answered before a route was picked.
const UNROUTED: &str = "unrouted";

/// Counters for one route, or for requests that never got one. Plain atomics in fixed slots,
/// so counting a request takes no lock and no allocation.
#[derive(Default)]
struct RouteStats {
    /// Responses by status class; 1xx is WebSocket upgrades.
    responses: [AtomicU64; 5],
    websockets: [AtomicU64; 3],
}

impl RouteStats {
    fn response(&self, status: u16) {
        let class = usize::from(status / 100).clamp(1, 5) - 1;
        self.responses[class].fetch_add(1, Ordering::Relaxed);
    }

    fn ws_result(&self, result: WsResult) {
        self.websockets[result as usize].fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Metrics {
    /// Established client connections, after the allow list check.
    connections: AtomicUsize,
    unrouted: RouteStats,
    rejected: [AtomicU64; Reject::ALL.len()],
    /// Histogram buckets, not cumulative; the last one is everything above the largest bound.
    durations: [AtomicU64; DURATION_BUCKETS.len() + 1],
    duration_sum_micros: AtomicU64,
}

impl Metrics {
    fn reject(&self, reason: Reject) {
        self.rejected[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    fn observe(&self, took: Duration) {
        let secs = took.as_secs_f64();
        let bucket = DURATION_BUCKETS.iter().position(|&le| secs <= le).unwrap_or(DURATION_BUCKETS.len());
        self.durations[bucket].fetch_add(1, Ordering::Relaxed);
        self.duration_sum_micros.fetch_add(took.as_micros() as u64, Ordering::Relaxed);
    }
}

/// Counts one request once its response head is known: status class and time to the head.
struct Outcome<'a> {
    metrics: &'a Metrics,
    started: Instant,
    /// The matched route's counters; `None` until the request is routed.
    stats: Option<&'a RouteStats>,
    recorded: bool,
}

impl<'a> Outcome<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        Self { metrics, started: Instant::now(), stats: None, recorded: false }
    }

    /// Called right before the response head goes out; later calls are ignored.
    fn head(&mut self, status: u16) {
        if std::mem::replace(&mut self.recorded, true) {
            return;
        }
        self.metrics.observe(self.started.elapsed());
        self.stats.unwrap_or(&self.metrics.unrouted).response(status);
    }
}

/// Keeps a gauge raised while it lives, so every exit path lowers it again.
struct GaugeGuard<'a>(&'a AtomicUsize);

impl<'a> GaugeGuard<'a> {
    fn new(gauge: &'a AtomicUsize) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self(gauge)
    }
}

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A label value in the Prometheus text format. Values only come from the configuration
/// (route labels, upstream URLs), never from requests, but a quote or backslash in them must
/// still not break the exposition.
fn prom_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// The `/metrics` page. Writing to a `String` can't fail, hence the ignored results.
fn render_metrics(router: &Router, metrics: &Metrics, inflight: usize) -> String {
    let mut out = String::new();
    let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
    let routes: Vec<(&str, &Upstreams)> = router.all().collect();

    let _ = writeln!(out, "# HELP iroh_gateway_requests_total Requests answered, by route and status class.");
    let _ = writeln!(out, "# TYPE iroh_gateway_requests_total counter");
    let stats = routes.iter().map(|(label, u)| (*label, &u.stats)).chain([(UNROUTED, &metrics.unrouted)]);
    for (label, stats) in stats {
        for (class, count) in STATUS_CLASSES.iter().zip(&stats.responses) {
            let _ = writeln!(out, "iroh_gateway_requests_total{{route=\"{}\",status_class=\"{class}\"}} {}", prom_label(label), get(count));
        }
    }

    let _ = writeln!(out, "# HELP iroh_gateway_websocket_upgrades_total WebSocket handshakes with a backend, by route and result.");
    let _ = writeln!(out, "# TYPE iroh_gateway_websocket_upgrades_total counter");
    for (label, upstreams) in &routes {
        for (result, count) in WS_RESULTS.iter().zip(&upstreams.stats.websockets) {
            let _ = writeln!(out, "iroh_gateway_websocket_upgrades_total{{route=\"{}\",result=\"{result}\"}} {}", prom_label(label), get(count));
        }
    }

    // The route is part of these labels because the same URL may serve several routes, each
    // with its own health state.
    let _ = writeln!(out, "# HELP iroh_gateway_upstream_failures_total Failed connection attempts to an upstream.");
    let _ = writeln!(out, "# TYPE iroh_gateway_upstream_failures_total counter");
    for (label, upstreams) in &routes {
        for (url, count) in upstreams.urls.iter().zip(&upstreams.failures) {
            let _ = writeln!(out, "iroh_gateway_upstream_failures_total{{route=\"{}\",upstream=\"{}\"}} {}", prom_label(label), prom_label(url), get(count));
        }
    }
    let _ = writeln!(out, "# HELP iroh_gateway_upstream_healthy Whether an upstream is out of its failure cooldown (1) or not (0).");
    let _ = writeln!(out, "# TYPE iroh_gateway_upstream_healthy gauge");
    for (label, upstreams) in &routes {
        for (url, left) in upstreams.urls.iter().zip(upstreams.health()) {
            let _ = writeln!(out, "iroh_gateway_upstream_healthy{{route=\"{}\",upstream=\"{}\"}} {}", prom_label(label), prom_label(url), u8::from(left.is_none()));
        }
    }

    let _ = writeln!(out, "# HELP iroh_gateway_rejected_total Connections and requests the gateway turned away, by reason.");
    let _ = writeln!(out, "# TYPE iroh_gateway_rejected_total counter");
    for (reason, count) in Reject::ALL.iter().zip(&metrics.rejected) {
        let _ = writeln!(out, "iroh_gateway_rejected_total{{reason=\"{}\"}} {}", reason.label(), get(count));
    }

    let _ = writeln!(out, "# HELP iroh_gateway_connections Established client connections.");
    let _ = writeln!(out, "# TYPE iroh_gateway_connections gauge");
    let _ = writeln!(out, "iroh_gateway_connections {}", metrics.connections.load(Ordering::Relaxed));
    let _ = writeln!(out, "# HELP iroh_gateway_inflight_requests Requests and WebSockets in progress.");
    let _ = writeln!(out, "# TYPE iroh_gateway_inflight_requests gauge");
    let _ = writeln!(out, "iroh_gateway_inflight_requests {inflight}");

    let _ = writeln!(out, "# HELP iroh_gateway_request_duration_seconds Time from a request's start to its response head.");
    let _ = writeln!(out, "# TYPE iroh_gateway_request_duration_seconds histogram");
    let mut total = 0;
    for (i, count) in metrics.durations.iter().enumerate() {
        total += get(count);
        let le = DURATION_BUCKETS.get(i).map_or("+Inf".to_string(), |le| le.to_string());
        let _ = writeln!(out, "iroh_gateway_request_duration_seconds_bucket{{le=\"{le}\"}} {total}");
    }
    let sum = get(&metrics.duration_sum_micros) as f64 / 1e6;
    let _ = writeln!(out, "iroh_gateway_request_duration_seconds_sum {sum}");
    let _ = writeln!(out, "iroh_gateway_request_duration_seconds_count {total}");
    out
}

/// At most this many admin connections are served at once; more are closed right away.
const ADMIN_MAX_CONNECTIONS: usize = 16;
/// Largest admin request head accepted. The endpoints take no input, so this is generous.
const ADMIN_MAX_HEAD: usize = 8 * 1024;
/// Time limit for a whole admin exchange, so a client that never finishes can't hold a slot.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(5);

/// What the admin listener reports on.
struct Admin {
    ctx: Arc<Ctx>,
    endpoint: Endpoint,
    /// The relay configured with `--relay-url`, if any.
    relay_url: Option<RelayUrl>,
    started: Instant,
}

/// IPv4-mapped addresses like `::ffff:127.0.0.1` count as loopback too.
fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().to_canonical().is_loopback()
}

/// The admin listener has no authentication and shows upstream URLs and traffic, so a
/// non-loopback address (including 0.0.0.0) needs an explicit opt-in.
fn check_admin_addr(addr: SocketAddr, allow_public: bool) -> Result<()> {
    anyhow::ensure!(
        allow_public || is_loopback(addr),
        "--admin-addr {addr} is not a loopback address; the admin listener has no authentication, \
         so also pass --admin-allow-public if it really should be reachable from the network"
    );
    Ok(())
}

async fn serve_admin(listener: tokio::net::TcpListener, admin: Arc<Admin>) {
    let slots = Arc::new(Semaphore::new(ADMIN_MAX_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                // Usually out of file descriptors; back off instead of spinning.
                warn!("admin listener: accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else { continue };
        let admin = admin.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = tokio::time::timeout(ADMIN_TIMEOUT, admin_connection(&admin, stream)).await;
        });
    }
}

/// Answers one request and closes the connection.
async fn admin_connection(admin: &Admin, mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    // The whole head is read before answering: closing a socket with unread data makes the
    // kernel send a reset, which can cut off the response.
    let (status, content_type, body) = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]);
            let mut parts = head.lines().next().unwrap_or_default().split(' ');
            let (method, target) = (parts.next().unwrap_or_default(), parts.next().unwrap_or_default());
            break admin_response(admin, method, path_only(target));
        }
        if buf.len() >= ADMIN_MAX_HEAD {
            break (431, ADMIN_TEXT, "request head too large\n".to_string());
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        _ => "Service Unavailable",
    };
    let allow = if status == 405 { "Allow: GET\r\n" } else { "" };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\n{allow}Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

const ADMIN_TEXT: &str = "text/plain; charset=utf-8";

fn admin_response(admin: &Admin, method: &str, path: &str) -> (u16, &'static str, String) {
    if !matches!(path, "/healthz" | "/readyz" | "/status" | "/metrics") {
        return (404, ADMIN_TEXT, "not found\n".into());
    }
    if method != "GET" {
        return (405, ADMIN_TEXT, "method not allowed\n".into());
    }
    match path {
        "/healthz" if admin.online() => (200, ADMIN_TEXT, "ok\n".into()),
        "/healthz" => (503, ADMIN_TEXT, "endpoint offline\n".into()),
        "/readyz" => {
            let (status, body) = admin.readiness();
            (status, ADMIN_TEXT, body)
        }
        "/status" => (200, "application/json", admin.status().to_string()),
        _ => (200, "text/plain; version=0.0.4; charset=utf-8", render_metrics(&admin.ctx.router, &admin.ctx.metrics, admin.inflight())),
    }
}

/// Whole seconds left, rounded up so a cooldown never shows as 0 while it is still running.
fn secs_ceil(d: Duration) -> u64 {
    d.as_secs() + u64::from(d.subsec_nanos() > 0)
}

impl Admin {
    /// Whether clients can reach the gateway: the endpoint is open and connected to at least
    /// one home relay (what `Endpoint::online` waits for at startup).
    fn online(&self) -> bool {
        !self.endpoint.is_closed() && self.endpoint.home_relay_status().get().iter().any(|r| r.is_connected())
    }

    fn inflight(&self) -> usize {
        self.ctx.max_requests - self.ctx.requests.available_permits()
    }

    /// Ready when online and every route (and the default backend) has an upstream outside
    /// its cooldown; otherwise lists what is missing.
    fn readiness(&self) -> (u16, String) {
        let mut problems = String::new();
        if !self.online() {
            problems.push_str("endpoint offline\n");
        }
        for (label, upstreams) in self.ctx.router.all() {
            let health = upstreams.health();
            if health.iter().any(Option::is_none) {
                continue;
            }
            let list: Vec<String> = upstreams
                .urls
                .iter()
                .zip(&health)
                .map(|(url, left)| format!("{url} (cooldown {}s)", left.map_or(0, secs_ceil)))
                .collect();
            let _ = writeln!(problems, "route {label}: no healthy upstream: {}", list.join(", "));
        }
        if problems.is_empty() { (200, "ready\n".into()) } else { (503, problems) }
    }

    fn status(&self) -> serde_json::Value {
        use serde_json::json;
        let relays: Vec<_> = self
            .endpoint
            .home_relay_status()
            .get()
            .iter()
            .map(|r| json!({ "url": r.url().to_string(), "connected": r.is_connected(), "last_error": r.last_error().map(|e| e.to_string()) }))
            .collect();
        let routes: Vec<_> = self
            .ctx
            .router
            .all()
            .map(|(label, upstreams)| {
                let list: Vec<_> = upstreams
                    .urls
                    .iter()
                    .zip(upstreams.health())
                    .map(|(url, left)| match left {
                        None => json!({ "url": url, "healthy": true }),
                        Some(left) => json!({ "url": url, "healthy": false, "cooldown_remaining_secs": secs_ceil(left) }),
                    })
                    .collect();
                json!({ "route": label, "upstreams": list })
            })
            .collect();
        json!({
            "endpoint_id": self.endpoint.id().to_string(),
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_secs": self.started.elapsed().as_secs(),
            "online": self.online(),
            "relay_url": self.relay_url.as_ref().map(|u| u.to_string()),
            "home_relays": relays,
            "connections": self.ctx.metrics.connections.load(Ordering::Relaxed),
            "inflight_requests": self.inflight(),
            "max_requests": self.ctx.max_requests,
            "routes": routes,
        })
    }
}

/// Loads the endpoint's secret key, or creates it on the very first run.
fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        // Only a missing file means "first run". Any other error (permissions, I/O) must not
        // quietly replace the identity clients know this gateway by.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return create_key(path),
        Err(err) => return Err(err).with_context(|| format!("reading key file {}", path.display())),
    };
    check_key_permissions(&file, path)?;
    let mut hex_key = String::new();
    file.read_to_string(&mut hex_key).with_context(|| format!("reading key file {}", path.display()))?;
    let bytes: [u8; 32] = hex::decode(hex_key.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
        .with_context(|| format!("key file {} must hold 32 bytes of hex", path.display()))?;
    Ok(SecretKey::from_bytes(&bytes))
}

fn create_key(path: &Path) -> Result<SecretKey> {
    let key = SecretKey::generate();
    let mut options = std::fs::OpenOptions::new();
    // `create_new` refuses to follow a planted file or symlink, and the mode applies from the
    // first byte, so the key is never readable by others, not even briefly.
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).with_context(|| format!("creating key file {}", path.display()))?;
    file.write_all(hex::encode(key.to_bytes()).as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("writing key file {}", path.display()))?;
    info!("created new key at {}", path.display());
    Ok(key)
}

/// Refuses a key other users can read or write: anyone who has it can impersonate the gateway.
#[cfg(unix)]
fn check_key_permissions(file: &std::fs::File, path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = file.metadata()?.permissions().mode() & 0o777;
    anyhow::ensure!(
        mode & 0o007 == 0,
        "key file {} is accessible by other users (mode {mode:03o}); fix it with: chmod 600 {}",
        path.display(),
        path.display()
    );
    if mode & 0o070 != 0 {
        warn!("key file {} is accessible by its group (mode {mode:03o}); chmod 600 is recommended", path.display());
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_key_permissions(_file: &std::fs::File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn log_paths_drop_query_strings() {
        assert_eq!(super::path_for_log("/ws-auth?access_token=eyJ.secret"), "/ws-auth?…");
        assert_eq!(super::path_for_log("/hello"), "/hello");
    }

    use super::*;

    fn head(method: &str, path: &str, headers: &[(&str, &str)]) -> RequestHead {
        RequestHead {
            method: method.into(),
            path: path.into(),
            headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            websocket: false,
        }
    }

    #[test]
    fn targets_stay_paths() {
        for ok in ["/", "/a/b?x=1&y=%20", "//evil.example/x", "/@evil", "/a:b", "/files/100%25", "/q?u=http://x/"] {
            assert!(valid_target(ok), "{ok}");
        }
        // Anything that could change the upstream's host, or that URL parsers rewrite.
        for bad in [
            "", "x", "@evil.com/", "http://evil/", "https://user@evil/", ".evil.com", "?x",
            "/\\evil", "/a b", "/a\tb", "/a\r\nHost: evil", "/a\0", "/é", "/a#frag",
        ] {
            assert!(!valid_target(bad), "{bad:?}");
        }
    }

    #[test]
    fn dot_segments_are_found_however_encoded() {
        for bad in [
            "/..", "/a/../b", "/a/./b", "/a/%2e%2e/b", "/a/%2E%2e/b", "/a%2f..%2fb", "/a%5c..%5cb",
            "/a/.%2e/b", "/a/%252e%252e/b", "/a/..;/b", "/a/%c0%ae%c0%ae/b", "/a/%ef%bc%8e%ef%bc%8e/b",
            "/a\\..\\b",
        ] {
            assert!(has_dot_segment(bad), "{bad}");
        }
        for ok in ["/", "/a/b", "/a/...", "/a/.b", "/a..b/c", "/.well-known/x", "/v1.2/x"] {
            assert!(!has_dot_segment(ok), "{ok}");
        }
    }

    #[test]
    fn heads_are_validated() {
        assert_eq!(validate_head(&head("GET", "/x", &[("accept", "*/*"), ("x-iroh-host", "api.example.com:443")])), Ok(()));
        assert_eq!(validate_head(&head("PROPFIND", "/x", &[])), Ok(()));
        for m in ["", "GET /x HTTP/1.1\r\n", "G T", "CONNECT", "connect", "TRACE", &"A".repeat(33)] {
            assert_eq!(validate_head(&head(m, "/", &[])), Err("invalid method"), "{m:?}");
        }
        assert_eq!(validate_head(&head("GET", "/a/../admin", &[])), Err("invalid path"));
        for (k, v) in [
            ("x-a", "1\r\nx-iroh-remote-id: spoofed"), ("x-a", "a\nb"), ("x-a", "a\0"), ("x a", "1"),
            ("x-a\r\n", "1"), ("", "1"), (":authority", "evil"), ("x-a", "\x7f"),
        ] {
            assert_eq!(validate_head(&head("GET", "/", &[(k, v)])), Err("invalid header"), "{k:?}: {v:?}");
        }
        for h in ["evil.com/x", "a b", "a@b", "", " "] {
            let res = validate_head(&head("GET", "/", &[("x-iroh-host", h)]));
            // An empty host is treated as absent.
            assert_eq!(res.is_ok(), h.trim().is_empty(), "{h:?}");
        }
        assert_eq!(validate_head(&head("GET", "/", &[("x-a", "tab\tand ünïcode")])), Ok(()));
        // The method of a WebSocket head is not used, so it is not checked.
        let mut ws = head("whatever", "/ws", &[]);
        ws.websocket = true;
        assert_eq!(validate_head(&ws), Ok(()));
    }

    #[test]
    fn spoofable_headers_are_replaced() {
        let remote = SecretKey::generate().public();
        let out = proxy_headers(
            head("GET", "/", &[
                ("X-Iroh-Remote-Id", "spoofed"), ("x-forwarded-for", "1.2.3.4"), ("X-Forwarded-Proto", "http"),
                ("x-forwarded-port", "1"), ("forwarded", "for=x"), ("x-real-ip", "1.2.3.4"), ("x-iroh-host", "a"),
                ("x-iroh-gateway-id", "spoofed"), ("connection", "close, X-Secret"), ("x-secret", "hop"), ("accept", "*/*"),
            ])
            .headers,
            remote,
            Some("a.example"),
        );
        let names: Vec<_> = out.iter().map(|(k, _)| k.as_str()).collect();
        // `connection` itself is removed later with the other hop-by-hop headers.
        assert_eq!(names, ["connection", "accept", "x-iroh-remote-id", "x-forwarded-proto", "x-forwarded-host"]);
        assert_eq!(out[2].1, remote.to_string());
        assert_eq!(out[3].1, "https");
        assert_eq!(out[4].1, "a.example");
    }

    #[test]
    fn hosts_normalize() {
        assert_eq!(normalize_host("API.example.com.:443"), "api.example.com");
        assert_eq!(normalize_host("[::1]:8080"), "[::1]");
        assert_eq!(normalize_host("example.com"), "example.com");
    }

    #[test]
    fn routes_match_whole_segments() {
        let router = Router::parse(
            &["/admin=http://127.0.0.1:1".into(), "api.example.com=http://127.0.0.1:2".into()],
            None,
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(router.select(None, "/admin/x").map(|r| r.0), Some("/admin"));
        assert_eq!(router.select(None, "/administrator").map(|r| r.0), None);
        assert_eq!(router.select(Some("api.example.com"), "/admin").map(|r| r.0), Some("api.example.com"));
        assert!(prefix_matches("", "/anything"));
    }

    #[test]
    fn close_frames_are_valid_on_the_wire() {
        assert_eq!(close_message(1005, "x".into()), Message::Close(None));
        assert_eq!(close_message(1006, String::new()), Message::Close(None));
        assert_eq!(close_message(999, String::new()), Message::Close(None));
        let Message::Close(Some(frame)) = close_message(4000, "é".repeat(100)) else { panic!() };
        assert_eq!(u16::from(frame.code), 4000);
        // Cut to 123 bytes on a character boundary.
        assert_eq!(frame.reason.len(), 122);
    }

    #[test]
    fn logged_values_are_escaped() {
        assert_eq!(for_log("/a\r\nb"), r#""/a\r\nb""#);
        assert!(for_log(&"é".repeat(300)).ends_with('…'));
    }

    #[tokio::test]
    async fn upstream_deadline_restarts_on_upload_progress() {
        let upload = Upload::new();
        let limit = Some(Duration::from_millis(100));
        assert_eq!(upstream_deadline(std::future::pending::<()>(), limit, &upload).await, None);
        assert_eq!(upstream_deadline(async { 7 }, limit, &upload).await, Some(7), "ready beats an expired deadline");
        // An upload that keeps moving holds the deadline off.
        let upload = Upload::new();
        let slow = async {
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(60)).await;
                upload.touch();
            }
            tokio::time::sleep(Duration::from_millis(60)).await;
        };
        assert_eq!(upstream_deadline(slow, limit, &upload).await, Some(()));
    }

    #[test]
    fn admin_listener_is_loopback_only_unless_allowed() {
        for ok in ["127.0.0.1:9180", "127.8.9.10:1", "[::1]:9180", "[::ffff:127.0.0.1]:9180"] {
            assert!(check_admin_addr(ok.parse().unwrap(), false).is_ok(), "{ok}");
        }
        for public in ["0.0.0.0:9180", "[::]:9180", "192.168.1.5:9180", "[2001:db8::1]:9180", "[::ffff:10.0.0.1]:9180"] {
            let addr: SocketAddr = public.parse().unwrap();
            assert!(check_admin_addr(addr, false).is_err(), "{public}");
            assert!(check_admin_addr(addr, true).is_ok(), "{public}");
        }
    }

    #[test]
    fn prometheus_labels_are_escaped() {
        assert_eq!(prom_label(r#"a"b\c"#), r#"a\"b\\c"#);
        assert_eq!(prom_label("x\ny"), r"x\ny");
        assert_eq!(prom_label("http://127.0.0.1:8080"), "http://127.0.0.1:8080");
    }

    #[test]
    fn metrics_render_in_text_format() {
        let router = Router::parse(
            &[r#"/q"uote=http://127.0.0.1:1"#.into()],
            Some("http://127.0.0.1:1,http://127.0.0.1:2"),
            Duration::from_secs(60),
        )
        .unwrap();
        let metrics = Metrics::default();
        let default = router.default.as_ref().unwrap();
        default.mark_failed(0, &"refused");
        let mut out = Outcome::new(&metrics);
        out.stats = Some(&default.stats);
        out.head(200);
        out.head(500); // only the first head counts
        Outcome::new(&metrics).head(404);
        default.stats.ws_result(WsResult::Upgraded);
        metrics.reject(Reject::PathNotAllowed);
        metrics.observe(Duration::from_millis(30));
        metrics.observe(Duration::from_secs(60));

        let text = render_metrics(&router, &metrics, 3);
        for line in [
            r#"iroh_gateway_requests_total{route="default",status_class="2xx"} 1"#,
            r#"iroh_gateway_requests_total{route="default",status_class="5xx"} 0"#,
            r#"iroh_gateway_requests_total{route="unrouted",status_class="4xx"} 1"#,
            r#"iroh_gateway_requests_total{route="/q\"uote",status_class="2xx"} 0"#,
            r#"iroh_gateway_websocket_upgrades_total{route="default",result="upgraded"} 1"#,
            r#"iroh_gateway_upstream_failures_total{route="default",upstream="http://127.0.0.1:1"} 1"#,
            r#"iroh_gateway_upstream_healthy{route="default",upstream="http://127.0.0.1:1"} 0"#,
            r#"iroh_gateway_upstream_healthy{route="default",upstream="http://127.0.0.1:2"} 1"#,
            r#"iroh_gateway_rejected_total{reason="path_not_allowed"} 1"#,
            "iroh_gateway_connections 0",
            "iroh_gateway_inflight_requests 3",
            // The two recorded heads took next to no time.
            r#"iroh_gateway_request_duration_seconds_bucket{le="0.01"} 2"#,
            r#"iroh_gateway_request_duration_seconds_bucket{le="0.05"} 3"#,
            r#"iroh_gateway_request_duration_seconds_bucket{le="10"} 3"#,
            r#"iroh_gateway_request_duration_seconds_bucket{le="+Inf"} 4"#,
            "iroh_gateway_request_duration_seconds_count 4",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line:?} in\n{text}");
        }
        // Every sample line is `name{labels} value` or `name value`, and every name has a TYPE.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            let name = series.split('{').next().unwrap();
            let base = name.trim_end_matches("_bucket").trim_end_matches("_sum").trim_end_matches("_count");
            assert!(text.contains(&format!("# TYPE {base} ")), "{line}");
        }
    }

    #[test]
    fn key_file_is_private_and_never_replaced() {
        let dir = std::env::temp_dir().join(format!("iroh-gateway-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("key");
        let _ = std::fs::remove_file(&path);
        let created = load_or_create_key(&path).unwrap();
        assert_eq!(load_or_create_key(&path).unwrap().public(), created.public());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load_or_create_key(&path).is_err(), "world-readable key must be refused");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        std::fs::write(&path, "not hex").unwrap();
        assert!(load_or_create_key(&path).is_err(), "a broken key must not be replaced");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "not hex");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
