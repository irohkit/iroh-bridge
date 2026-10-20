//! The iroh-http/1 client behind iroh-fetch, shared by the WASM build (`web-client`, browsers and
//! Node.js without the native addon) and the native Node.js addon (`node-client`).
//!
//! It keeps one connection per gateway on a single local endpoint, dials with relay hints (plus a
//! plain dial after 1.5 s), streams request and response bodies, and tunnels WebSockets. The
//! bindings only translate between these types and their JavaScript counterparts.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointId, RelayMode, TransportAddr, Watcher,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use n0_future::StreamExt;
pub use proto::{RequestHead, ResponseHead, WsFrame};
use proto::ALPN;
use serde::Serialize;
use tokio::sync::Mutex;

/// Largest piece of a response body handed out at once.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// How the local endpoint is set up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// For browsers (and the WASM build in Node.js): only address lookups, no publishing. Nobody
    /// dials a client, so publishing its address would just cost every page load a DNS round trip,
    /// and without UDP there is no direct address worth publishing anyway.
    Browser,
    /// iroh's default (n0) preset: UDP sockets, address publishing and lookup, so the gateway and
    /// this client can hole-punch a direct path and only use a relay until then (or if that fails).
    Native,
}

/// Options for [`Client::create`].
#[derive(Debug, Default, Clone)]
pub struct Options {
    /// Comma-separated relay URLs; `n0` adds n0's public relays. Empty means n0's relays.
    pub relay_url: Option<String>,
    /// Token for relays that only admit clients that present one.
    pub relay_token: Option<String>,
    /// 64 hex chars; without it a new identity is generated.
    pub secret_key: Option<String>,
}

/// A request body: bytes known up front, or a stream uploaded as it is produced.
pub enum Body {
    Bytes(Vec<u8>),
    /// `Send` outside the browser, so the upload can run on another thread there.
    Stream(n0_future::boxed::BoxStream<Result<Vec<u8>>>),
}

/// One client endpoint that can talk to any number of gateways.
pub struct Client {
    endpoint: Endpoint,
    /// The relays this client was configured with. Dialing a gateway "via" them lets the
    /// connection start right away instead of waiting for an address lookup (which also keeps
    /// running, so a gateway on another relay is still found).
    relay_hints: Vec<iroh::RelayUrl>,
    /// One connection per gateway, all sharing this endpoint (and its relay connection).
    conns: StdMutex<HashMap<EndpointId, Arc<Mutex<Option<Connection>>>>>,
}

impl Client {
    pub async fn create(profile: Profile, options: Options) -> Result<Self> {
        let builder = match profile {
            Profile::Browser => Endpoint::builder(presets::Minimal)
                .address_lookup(iroh::address_lookup::PkarrResolver::n0_dns())
                .relay_mode(RelayMode::Default),
            Profile::Native => Endpoint::builder(presets::N0),
        };
        // Notice a dead relay or gateway within seconds instead of the ~30 s defaults, so
        // requests fail over to another replica quickly. Keep-alives over a relay are cheap.
        let mut builder = builder.transport_config(
            iroh::endpoint::QuicTransportConfig::builder()
                .keep_alive_interval(Duration::from_secs(1))
                .default_path_keep_alive_interval(Duration::from_secs(1))
                .default_path_max_idle_timeout(Duration::from_secs(4))
                .max_idle_timeout(Some(Duration::from_secs(5).try_into().expect("valid idle timeout")))
                .build(),
        );
        let mut relay_hints = Vec::new();
        if let Some(hex) = options.secret_key.filter(|k| !k.is_empty()) {
            builder = builder.secret_key(parse_secret_key(&hex)?);
        }
        if let Some(spec) = options.relay_url.filter(|u| !u.is_empty()) {
            // In the browser the token travels as `?token=` on the relay WebSocket URL.
            let map = proto::relay_map(&spec, options.relay_token.as_deref())?;
            relay_hints = proto::listed_relays(&spec);
            builder = builder.relay_mode(RelayMode::Custom(map));
        }
        let endpoint = builder.bind().await?;
        Ok(Self { endpoint, relay_hints, conns: Default::default() })
    }

    /// Why the home relay can't be used, if it can't: e.g. it refused the relay token. `None`
    /// while the relay is fine or nothing has failed yet.
    pub fn relay_problem(&self) -> Option<String> {
        let statuses = self.endpoint.home_relay_status().get();
        statuses.iter().find_map(|status| {
            if let Some(reason) = status.auth_denied_reason() {
                return Some(format!("relay {} refused this client: {reason}", status.url()));
            }
            status.last_error().map(|err| format!("relay {} is unreachable: {err}", status.url()))
        })
    }

    /// This client's secret key as hex. Whoever has it can act as this client.
    pub fn secret_key(&self) -> String {
        self.endpoint.secret_key().to_bytes().iter().map(|b| format!("{b:02x}")).collect()
    }

    /// This client's own endpoint id.
    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Opens the connection to `remote` now instead of on the first request.
    pub async fn connect(&self, remote: &str) -> Result<()> {
        self.connection(parse_remote(remote)?).await.map(drop)
    }

    /// Sends one HTTP request; resolves once the response head arrived. The body uploads in the
    /// background meanwhile, so the backend may answer (or stream back) before it is complete.
    pub async fn request(&self, remote: &str, head: RequestHead, body: Body) -> Result<(ResponseHead, ResponseBody)> {
        let remote = parse_remote(remote)?;
        let (send, mut recv) = self.open(remote, &head).await?;
        let upload_error = Arc::new(StdMutex::new(None::<String>));
        n0_future::task::spawn({
            let upload_error = upload_error.clone();
            async move {
                if let Err(err) = upload(body, send).await {
                    *upload_error.lock().expect("poisoned") = Some(format!("{err:#}"));
                }
            }
        });
        match proto::read_head::<ResponseHead>(&mut recv).await {
            Ok(resp) => Ok((resp, ResponseBody(Some(recv)))),
            Err(err) => match upload_error.lock().expect("poisoned").take() {
                Some(upload) => Err(anyhow::anyhow!("uploading request body failed: {upload}")),
                None => Err(err),
            },
        }
    }

    /// Opens a WebSocket to `path`. Returns the socket only when the backend accepted the upgrade
    /// (status 101); otherwise the body holds the backend's error response.
    pub async fn websocket(&self, remote: &str, path: String, headers: Vec<(String, String)>) -> Result<(ResponseHead, WsOutcome)> {
        let remote = parse_remote(remote)?;
        let head = RequestHead { method: "GET".into(), path, headers, websocket: true };
        let (send, mut recv) = self.open(remote, &head).await?;
        let resp: ResponseHead = proto::read_head(&mut recv).await?;
        let outcome = if resp.status == 101 {
            WsOutcome::Socket(WebSocket { send: Arc::new(Mutex::new(send)), recv: Arc::new(Mutex::new(recv)) })
        } else {
            WsOutcome::Rejected(ResponseBody(Some(recv)))
        };
        Ok((resp, outcome))
    }

    /// The paths of the current connection to `remote`, or `None` if there is none.
    pub fn connection_info(&self, remote: &str) -> Result<Option<ConnectionInfo>> {
        let remote = parse_remote(remote)?;
        let Some(slot) = self.conns.lock().expect("poisoned").get(&remote).cloned() else { return Ok(None) };
        // Never wait here: a dial in progress just means there is nothing to report yet.
        let Ok(guard) = slot.try_lock() else { return Ok(None) };
        let Some(conn) = guard.as_ref().filter(|c| c.close_reason().is_none()) else { return Ok(None) };
        let paths: Vec<PathInfo> = conn
            .paths()
            .iter()
            .map(|p| {
                let (kind, addr) = match p.remote_addr() {
                    TransportAddr::Relay(url) => ("relay", url.to_string()),
                    TransportAddr::Ip(addr) => ("direct", addr.to_string()),
                    other => ("custom", format!("{other:?}")),
                };
                PathInfo { kind, addr, selected: p.is_selected(), rtt_ms: p.rtt().as_secs_f64() * 1000.0 }
            })
            .collect();
        let selected = paths.iter().find(|p| p.selected).map(|p| p.kind);
        Ok(Some(ConnectionInfo { kind: selected.unwrap_or("none"), paths }))
    }

    /// Closes all connections and the local endpoint.
    pub async fn close(&self) {
        let slots: Vec<_> = self.conns.lock().expect("poisoned").drain().map(|(_, slot)| slot).collect();
        for slot in slots {
            if let Some(conn) = slot.lock().await.take() {
                conn.close(0u32.into(), b"bye");
            }
        }
        self.endpoint.close().await;
    }

    fn slot(&self, remote: EndpointId) -> Arc<Mutex<Option<Connection>>> {
        self.conns.lock().expect("poisoned").entry(remote).or_default().clone()
    }

    /// Returns the live connection, dialing (again) if there is none or it was lost.
    async fn connection(&self, remote: EndpointId) -> Result<Connection> {
        // Each gateway has its own slot, so dialing one doesn't hold up requests to another.
        let slot = self.slot(remote);
        let mut guard = slot.lock().await;
        if let Some(conn) = guard.as_ref() {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        let addr = self
            .relay_hints
            .iter()
            .fold(iroh::EndpointAddr::new(remote), |addr, url| addr.with_relay_url(url.clone()));
        // Hinting our own relays makes gateways on them fast to reach, but for a gateway on some
        // other relay (e.g. a standby on n0's) the handshake first goes nowhere, and QUIC's
        // retransmit backoff can then stretch the dial past 15 s. So if the hinted dial hasn't
        // finished after a moment, also dial with address lookup alone and take whichever wins.
        let mut hinted = std::pin::pin!(self.endpoint.connect(addr, ALPN));
        let mut plain = std::pin::pin!(async {
            n0_future::time::sleep(Duration::from_millis(1500)).await;
            self.endpoint.connect(iroh::EndpointAddr::new(remote), ALPN).await
        });
        let first = n0_future::future::race(async { (true, hinted.as_mut().await) }, async { (false, plain.as_mut().await) }).await;
        let conn = match first {
            (_, Ok(conn)) => Ok(conn),
            (true, Err(_)) => plain.await,
            (false, Err(_)) => hinted.await,
        }
        .context("connecting to gateway")?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Opens a fresh stream and sends the request head, retrying once on a stale connection.
    async fn open(&self, remote: EndpointId, head: &RequestHead) -> Result<(SendStream, RecvStream)> {
        let conn = self.connection(remote).await?;
        let (mut send, recv) = match conn.open_bi().await {
            Ok(streams) => streams,
            Err(_) => {
                *self.slot(remote).lock().await = None;
                self.connection(remote).await?.open_bi().await?
            }
        };
        proto::write_head(&mut send, head).await?;
        Ok((send, recv))
    }
}

async fn upload(body: Body, mut send: SendStream) -> Result<()> {
    let res = async {
        match body {
            Body::Bytes(bytes) => send.write_all(&bytes).await?,
            Body::Stream(mut chunks) => {
                while let Some(chunk) = chunks.next().await {
                    send.write_all(&chunk?).await?;
                }
            }
        }
        anyhow::Ok(())
    }
    .await;
    match res {
        Ok(()) => send.finish()?,
        // Abort the stream so the gateway doesn't forward a truncated body as complete.
        Err(_) => {
            let _ = send.reset(1u32.into());
        }
    }
    res
}

/// The rest of a response stream. Dropping it tells the gateway to stop sending.
pub struct ResponseBody(Option<RecvStream>);

impl ResponseBody {
    /// The next piece of the body, or `None` at its end.
    pub async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>> {
        let Some(recv) = self.0.as_mut() else { return Ok(None) };
        match recv.read_chunk(CHUNK_SIZE).await {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.0 = None;
                Ok(None)
            }
            Err(err) => {
                self.0 = None;
                Err(err.into())
            }
        }
    }

    /// Stops the transfer.
    pub fn cancel(&mut self) {
        if let Some(mut recv) = self.0.take() {
            let _ = recv.stop(0u32.into());
        }
    }
}

pub enum WsOutcome {
    Socket(WebSocket),
    Rejected(ResponseBody),
}

/// One WebSocket tunnelled over an iroh stream. Clones share the stream.
#[derive(Clone)]
pub struct WebSocket {
    send: Arc<Mutex<SendStream>>,
    recv: Arc<Mutex<RecvStream>>,
}

impl WebSocket {
    /// The next message, or `None` at the end.
    pub async fn recv(&self) -> Result<Option<WsFrame>> {
        proto::read_frame(&mut *self.recv.lock().await).await
    }

    /// Sends a frame; `finish` also ends our side of the stream (after a close frame).
    pub async fn send(&self, frame: WsFrame, finish: bool) -> Result<()> {
        let mut send = self.send.lock().await;
        proto::write_frame(&mut send, &frame).await?;
        if finish {
            send.finish()?;
        }
        Ok(())
    }
}

/// What [`Client::connection_info`] reports.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    /// The kind of the path in use: `direct`, `relay`, or `none` while none is selected.
    pub kind: &'static str,
    pub paths: Vec<PathInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PathInfo {
    pub kind: &'static str,
    pub addr: String,
    pub selected: bool,
    pub rtt_ms: f64,
}

pub fn parse_secret_key(hex: &str) -> Result<iroh::SecretKey> {
    let hex = hex.trim();
    anyhow::ensure!(hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()), "secret key must be 64 hex characters");
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
    }
    Ok(iroh::SecretKey::from_bytes(&bytes))
}

pub fn parse_remote(remote: &str) -> Result<EndpointId> {
    remote.trim().parse().map_err(|e| anyhow::anyhow!("invalid endpoint id {remote:?}: {e}"))
}
