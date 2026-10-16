//! Wire format shared by the gateway and the web client.
//!
//! Every HTTP request uses one bidirectional QUIC stream:
//!   client -> gateway: [u32 BE len][RequestHead JSON][body bytes...] then FIN
//!   gateway -> client: [u32 BE len][ResponseHead JSON][body bytes...] then FIN
//!
//! A WebSocket uses the same stream setup with `RequestHead::websocket = true`. The gateway answers
//! with status 101 once the backend accepted the upgrade; after that both directions carry
//! [`WsFrame`]s: [u8 opcode][u32 BE len][payload].

use anyhow::{Context, Result, ensure};
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const ALPN: &[u8] = b"iroh-http/1";
const MAX_HEAD_LEN: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHead {
    pub method: String,
    /// Path plus query string, e.g. `/api/users?page=2`.
    pub path: String,
    pub headers: Vec<(String, String)>,
    /// Open a WebSocket to `path` on the backend instead of sending a plain HTTP request.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub websocket: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

pub async fn write_head<T: Serialize>(send: &mut SendStream, head: &T) -> Result<()> {
    let json = serde_json::to_vec(head)?;
    ensure!(json.len() <= MAX_HEAD_LEN, "head too large");
    send.write_all(&(json.len() as u32).to_be_bytes()).await?;
    send.write_all(&json).await?;
    Ok(())
}

pub async fn read_head<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await.context("reading head length")?;
    let len = u32::from_be_bytes(len) as usize;
    ensure!(len <= MAX_HEAD_LEN, "head too large: {len}");
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.context("reading head")?;
    Ok(serde_json::from_slice(&buf)?)
}

const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsFrame {
    Text(String),
    Binary(Vec<u8>),
    /// Close code (1000 = normal) and reason.
    Close(u16, String),
}

impl WsFrame {
    const TEXT: u8 = 1;
    const BINARY: u8 = 2;
    const CLOSE: u8 = 8;
}

pub async fn write_frame(send: &mut SendStream, frame: &WsFrame) -> Result<()> {
    let (op, payload): (u8, std::borrow::Cow<[u8]>) = match frame {
        WsFrame::Text(t) => (WsFrame::TEXT, t.as_bytes().into()),
        WsFrame::Binary(b) => (WsFrame::BINARY, b.as_slice().into()),
        WsFrame::Close(code, reason) => {
            let mut p = code.to_be_bytes().to_vec();
            p.extend_from_slice(reason.as_bytes());
            (WsFrame::CLOSE, p.into())
        }
    };
    ensure!(payload.len() <= MAX_FRAME_LEN, "frame too large");
    let mut header = [0u8; 5];
    header[0] = op;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    send.write_all(&header).await?;
    send.write_all(&payload).await?;
    Ok(())
}

/// Reads the next frame, or `None` when the peer finished the stream.
pub async fn read_frame(recv: &mut RecvStream) -> Result<Option<WsFrame>> {
    let mut header = [0u8; 5];
    match recv.read_exact(&mut header).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    ensure!(len <= MAX_FRAME_LEN, "frame too large: {len}");
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload).await.context("reading frame")?;
    Ok(Some(match header[0] {
        WsFrame::TEXT => WsFrame::Text(String::from_utf8(payload).context("text frame is not utf-8")?),
        WsFrame::BINARY => WsFrame::Binary(payload),
        WsFrame::CLOSE => {
            ensure!(payload.len() >= 2, "close frame too short");
            let code = u16::from_be_bytes([payload[0], payload[1]]);
            WsFrame::Close(code, String::from_utf8_lossy(&payload[2..]).into_owned())
        }
        op => anyhow::bail!("unknown frame opcode {op}"),
    }))
}
