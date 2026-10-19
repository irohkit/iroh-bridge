//! Runs next to an existing HTTP backend (behind any NAT) and exposes it over iroh.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{
    Endpoint, RelayMode, RelayUrl, SecretKey,
    endpoint::{Connection, RecvStream, SendStream, presets},
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
    /// Base URL of the HTTP backend to forward requests to.
    #[arg(long, env = "BACKEND_URL", default_value = "http://127.0.0.1:8080")]
    backend: String,
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
    let secret_key = load_or_create_key(&args.key_file)?;

    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()]);
    if let Some(url) = args.relay_url.clone() {
        builder = builder.relay_mode(RelayMode::Custom(url.into()));
    }
    let endpoint = builder.bind().await?;
    endpoint.online().await;

    info!("forwarding to {}", args.backend);
    println!("\nIROH_ENDPOINT_ID={}\n", endpoint.id());

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let ctx = std::sync::Arc::new(Ctx { http, backend: args.backend.trim_end_matches('/').to_string(), max_body: args.max_body });

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
    http: reqwest::Client,
    backend: String,
    max_body: usize,
}

async fn handle_connection(ctx: std::sync::Arc<Ctx>, conn: Connection) {
    let remote = conn.remote_id();
    info!(remote = %remote.fmt_short(), "client connected");
    // Each request arrives on its own bidirectional stream, so requests run concurrently.
    while let Ok((send, recv)) = conn.accept_bi().await {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_request(&ctx, send, recv).await {
                warn!("request failed: {err:#}");
            }
        });
    }
    info!(remote = %remote.fmt_short(), "client disconnected");
}

async fn handle_request(ctx: &Ctx, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    let head: RequestHead = proto::read_head(&mut recv).await?;
    if head.websocket {
        return handle_websocket(ctx, head, send, recv).await;
    }
    let body = request_body(recv, ctx.max_body).await?;

    let resp = match forward(ctx, &head, body).await {
        Ok(resp) => resp,
        Err(err) => {
            warn!("{} {} -> backend error: {err:#}", head.method, head.path);
            let head = ResponseHead { status: 502, headers: vec![("content-type".into(), "text/plain".into())] };
            proto::write_head(&mut send, &head).await?;
            send.write_all(format!("bad gateway: {err}").as_bytes()).await?;
            send.finish()?;
            return Ok(());
        }
    };
    info!("{} {} -> {}", head.method, head.path, resp.status().as_u16());

    let headers = header_pairs(resp.headers());
    proto::write_head(&mut send, &ResponseHead { status: resp.status().as_u16(), headers }).await?;

    // Stream the body through so large or long-lived responses don't sit in memory.
    let mut resp = resp;
    while let Some(chunk) = resp.chunk().await? {
        send.write_all(&chunk).await?;
    }
    send.finish()?;
    Ok(())
}

async fn handle_websocket(ctx: &Ctx, head: RequestHead, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    anyhow::ensure!(head.path.starts_with('/'), "path must start with /");
    let base = ctx.backend.replacen("http://", "ws://", 1).replacen("https://", "wss://", 1);
    let mut req = format!("{base}{}", head.path).into_client_request()?;
    for (k, v) in &head.headers {
        let lower = k.to_ascii_lowercase();
        if HOP_HEADERS.contains(&lower.as_str()) || WS_HANDSHAKE_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        req.headers_mut().append(HeaderName::from_bytes(k.as_bytes())?, HeaderValue::from_str(v)?);
    }

    let (ws, resp) = match tokio_tungstenite::connect_async(req).await {
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
            warn!("WS {} -> {status}", head.path);
            proto::write_head(&mut send, &ResponseHead { status, headers }).await?;
            send.write_all(&body).await?;
            send.finish()?;
            return Ok(());
        }
    };
    info!("WS {} -> 101", head.path);
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

async fn forward(ctx: &Ctx, head: &RequestHead, body: Option<reqwest::Body>) -> Result<reqwest::Response> {
    anyhow::ensure!(head.path.starts_with('/'), "path must start with /");
    let method = reqwest::Method::from_bytes(head.method.as_bytes())?;
    let mut req = ctx.http.request(method, format!("{}{}", ctx.backend, head.path));
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
