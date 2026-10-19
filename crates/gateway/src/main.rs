//! Runs next to an existing HTTP backend (behind any NAT) and exposes it over iroh.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Mutex, atomic::{AtomicUsize, Ordering}},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{
    Endpoint, EndpointId, RelayMode, RelayUrl, SecretKey,
    endpoint::{Connection, RecvStream, SendStream, VarInt, presets},
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
use tracing::{info, warn};

const CHUNK_SIZE: usize = 64 * 1024;

/// Hop-by-hop headers that must not be forwarded by a proxy.
const HOP_HEADERS: &[&str] = &[
    "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "te",
    "trailer", "host", "content-length",
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
    /// Maximum request body size in bytes.
    #[arg(long, env = "MAX_BODY_BYTES", default_value_t = 32 * 1024 * 1024)]
    max_body: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_gateway=info".into()),
        )
        .init();
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
    let secret_key = load_or_create_key(&args.key_file)?;

    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()]);
    if let Some(url) = args.relay_url.clone() {
        builder = builder.relay_mode(RelayMode::Custom(url.into()));
    }
    let endpoint = builder.bind().await?;
    endpoint.online().await;

    router.log_config();
    if !allow_clients.is_empty() {
        let ids: Vec<_> = allow_clients.iter().map(|id| id.fmt_short().to_string()).collect();
        info!("accepting only clients {}", ids.join(", "));
    }
    if !allow_paths.is_empty() {
        info!("serving only paths under {}", allow_paths.iter().map(|p| if p.is_empty() { "/" } else { p }).collect::<Vec<_>>().join(", "));
    }
    println!("\nIROH_ENDPOINT_ID={}\n", endpoint.id());

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let ctx = std::sync::Arc::new(Ctx {
        gateway_id: endpoint.id().to_string(),
        http,
        router,
        allow_clients,
        allow_paths,
        max_body: args.max_body,
    });

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(conn) => handle_connection(ctx, conn).await,
                        Err(err) => warn!("incoming connection failed: {err:#}"),
                    }
                });
            }
        }
    }
    endpoint.close().await;
    Ok(())
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

/// Whether a path has `.`/`..` segments, also percent-encoded or with backslashes. A backend
/// may resolve them after the gateway matched the raw path, so `/public/../admin` would slip
/// past both routing and the path allow list. Real clients normalize these away before sending.
fn has_dot_segment(path: &str) -> bool {
    let decoded = path
        .replace("%2e", ".")
        .replace("%2E", ".")
        .replace("%2f", "/")
        .replace("%2F", "/")
        .replace("%5c", "/")
        .replace("%5C", "/")
        .replace('\\', "/");
    decoded.split('/').any(|s| s == "." || s == "..")
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

/// The backends a request may go to, with passive health tracking: an upstream is only judged
/// by whether it accepts connections, since any HTTP answer (even a 5xx) is the app's own reply.
struct Upstreams {
    urls: Vec<String>,
    /// When each upstream last failed to connect; `None` once it has served a request again.
    failed: Mutex<Vec<Option<Instant>>>,
    next: AtomicUsize,
    cooldown: Duration,
}

impl Upstreams {
    fn parse(list: &str, cooldown: Duration) -> Result<Self> {
        let urls: Vec<String> = list
            .split(',')
            .map(|u| u.trim().trim_end_matches('/').to_string())
            .filter(|u| !u.is_empty())
            .collect();
        anyhow::ensure!(!urls.is_empty(), "no backend URL given");
        Ok(Self { failed: Mutex::new(vec![None; urls.len()]), urls, next: AtomicUsize::new(0), cooldown })
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

async fn handle_connection(ctx: std::sync::Arc<Ctx>, conn: Connection) {
    let remote = conn.remote_id();
    // The id is only known once the handshake has verified it, so refuse right after.
    if !ctx.allow_clients.is_empty() && !ctx.allow_clients.contains(&remote) {
        warn!(remote = %remote.fmt_short(), "client not in allow list, closing connection");
        conn.close(VarInt::from_u32(1), b"not allowed");
        return;
    }
    info!(remote = %remote.fmt_short(), "client connected");
    // Each request arrives on its own bidirectional stream, so requests run concurrently.
    while let Ok((send, recv)) = conn.accept_bi().await {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_request(&ctx, remote, send, recv).await {
                warn!("request failed: {err:#}");
            }
        });
    }
    info!(remote = %remote.fmt_short(), "client disconnected");
}

async fn handle_request(ctx: &Ctx, remote: EndpointId, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    let mut head: RequestHead = proto::read_head(&mut recv).await?;
    let method = if head.websocket { "WS" } else { head.method.as_str() };
    let host = request_host(&head.headers);
    let path = path_only(&head.path);
    // Routing and the allow list both look at the path, so reject anything they could misread
    // before either runs. WebSockets get the same checks, before any upstream is contacted.
    if !path.starts_with('/') || has_dot_segment(path) {
        info!("{method} {} -> 400 (invalid path)", head.path);
        return respond_text(ctx, &mut send, 400, "bad request: invalid path").await;
    }
    if !ctx.allow_paths.is_empty() && !ctx.allow_paths.iter().any(|p| prefix_matches(p, path)) {
        info!("{method} {} -> 403 (path not allowed)", head.path);
        return respond_text(ctx, &mut send, 403, "forbidden").await;
    }
    let Some((route, upstreams)) = ctx.router.select(host.as_deref().map(normalize_host).as_deref(), path) else {
        info!("{method} {} -> 404 (no route for host {})", head.path, host.as_deref().unwrap_or("-"));
        return respond_text(ctx, &mut send, 404, "no route for this host or path").await;
    };
    head.headers = proxy_headers(head.headers, remote, host.as_deref());
    if head.websocket {
        return handle_websocket(upstreams, route, head, send, recv).await;
    }
    let body = request_body(recv, ctx.max_body).await?;

    let (resp, upstream) = match forward_any(ctx, upstreams, &head, body).await {
        Ok(ok) => ok,
        Err(err) => {
            warn!("{} {} -> backend error (route {route}): {err:#}", head.method, head.path);
            return respond_text(ctx, &mut send, 502, &format!("bad gateway: {err}")).await;
        }
    };
    info!("{} {} -> {} via {upstream} (route {route})", head.method, head.path, resp.status().as_u16());

    let mut headers = header_pairs(resp.headers());
    headers.push(("x-iroh-gateway-id".into(), ctx.gateway_id.clone()));
    proto::write_head(&mut send, &ResponseHead { status: resp.status().as_u16(), headers }).await?;

    // Stream the body through so large or long-lived responses don't sit in memory.
    let mut resp = resp;
    while let Some(chunk) = resp.chunk().await? {
        send.write_all(&chunk).await?;
    }
    send.finish()?;
    Ok(())
}

async fn handle_websocket(upstreams: &Upstreams, route: &str, head: RequestHead, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    // A handshake has no body, so a connect failure can always move on to the next upstream.
    let mut attempt = None;
    for i in upstreams.order() {
        let upstream = &upstreams.urls[i];
        let req = ws_request(upstream, &head)?;
        let res = match tokio::time::timeout(Duration::from_secs(10), tokio_tungstenite::connect_async(req)).await {
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
            // Pass the backend's refusal (e.g. 401/404) through; anything else is a 502.
            let (status, headers, body) = match err {
                tungstenite::Error::Http(resp) => (
                    resp.status().as_u16(),
                    header_pairs(resp.headers()),
                    resp.body().clone().unwrap_or_default(),
                ),
                err => (502, vec![("content-type".into(), "text/plain".into())], format!("bad gateway: {err}").into_bytes()),
            };
            warn!("WS {} -> {status} via {upstream} (route {route})", head.path);
            proto::write_head(&mut send, &ResponseHead { status, headers }).await?;
            send.write_all(&body).await?;
            send.finish()?;
            return Ok(());
        }
    };
    info!("WS {} -> 101 via {upstream} (route {route})", head.path);
    let headers = header_pairs(resp.headers())
        .into_iter()
        .filter(|(k, _)| !WS_HANDSHAKE_HEADERS.contains(&k.as_str()))
        .collect();
    proto::write_head(&mut send, &ResponseHead { status: 101, headers }).await?;

    let (mut ws_tx, mut ws_rx) = ws.split();
    let client_to_backend = async {
        while let Some(frame) = proto::read_frame(&mut recv).await? {
            let msg = match frame {
                WsFrame::Text(t) => Message::text(t),
                WsFrame::Binary(b) => Message::binary(b),
                WsFrame::Close(code, reason) => {
                    ws_tx.send(Message::Close(Some(CloseFrame { code: CloseCode::from(code), reason: reason.into() }))).await?;
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
                Message::Text(t) => proto::write_frame(&mut send, &WsFrame::Text(t.to_string())).await?,
                Message::Binary(b) => proto::write_frame(&mut send, &WsFrame::Binary(b.to_vec())).await?,
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
        proto::write_frame(&mut send, &close).await?;
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

fn header_pairs(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(k, _)| !HOP_HEADERS.contains(&k.as_str()))
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_string())))
        .collect()
}

/// Streams the rest of the QUIC stream to the backend as the request body, so uploads never
/// sit in memory. Returns `None` for an empty body so GET requests stay body-less.
async fn request_body(mut recv: RecvStream, max_body: usize) -> Result<Option<reqwest::Body>> {
    let Some(first) = recv.read_chunk(CHUNK_SIZE).await? else {
        return Ok(None);
    };
    let chunks = futures_util::stream::try_unfold(
        (recv, Some(first), 0usize),
        move |(mut recv, pending, total)| async move {
            let chunk = match pending {
                Some(chunk) => Some(chunk),
                None => recv.read_chunk(CHUNK_SIZE).await.map_err(std::io::Error::other)?,
            };
            let Some(chunk) = chunk else { return Ok(None) };
            let total = total + chunk.len();
            if total > max_body {
                return Err(std::io::Error::other(format!("request body exceeds {max_body} bytes")));
            }
            Ok(Some((chunk, (recv, None, total))))
        },
    );
    Ok(Some(reqwest::Body::wrap_stream(chunks)))
}

/// Headers set by the gateway itself. Clients can't supply them, so the backend can trust them.
/// `x-iroh-host` is the client's routing hint; it reaches the backend as `X-Forwarded-Host`.
const GATEWAY_HEADERS: &[&str] = &[
    "x-iroh-remote-id", "x-forwarded-proto", "x-forwarded-for", "x-forwarded-host", "forwarded", "x-iroh-host",
];

/// Drops headers a client must not spoof and adds what a reverse proxy normally adds:
/// `X-Iroh-Remote-Id` (the caller's endpoint id, verified by the QUIC handshake) and
/// `X-Forwarded-Proto: https` (the tunnel is end-to-end encrypted), plus `X-Forwarded-Host`
/// when the client said which host it used.
fn proxy_headers(headers: Vec<(String, String)>, remote: EndpointId, host: Option<&str>) -> Vec<(String, String)> {
    let mut out: Vec<_> = headers
        .into_iter()
        .filter(|(k, _)| !GATEWAY_HEADERS.contains(&k.to_ascii_lowercase().as_str()))
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

fn load_or_create_key(path: &PathBuf) -> Result<SecretKey> {
    if let Ok(hex_key) = std::fs::read_to_string(path) {
        let bytes: [u8; 32] = hex::decode(hex_key.trim())?
            .try_into()
            .map_err(|_| anyhow::anyhow!("key file must hold 32 bytes of hex"))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    std::fs::write(path, hex::encode(key.to_bytes())).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    info!("created new key at {}", path.display());
    Ok(key)
}
