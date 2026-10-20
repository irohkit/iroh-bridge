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
/// The largest head either side writes or accepts by default. A reader may pass a lower limit
/// to [`read_head_limited`].
pub const MAX_HEAD_LEN: usize = 256 * 1024;
/// The largest WebSocket message either side writes or accepts by default.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

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

/// A length prefix above the reader's limit. A distinct type so the gateway can answer it with
/// a precise status instead of a generic error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLarge {
    pub what: &'static str,
    pub len: usize,
    pub max: usize,
}

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} too large: {} bytes (limit {})", self.what, self.len, self.max)
    }
}

impl std::error::Error for TooLarge {}

/// Checks a length prefix before anything is allocated for it.
fn check_len(what: &'static str, len: usize, max: usize) -> Result<(), TooLarge> {
    if len > max { Err(TooLarge { what, len, max }) } else { Ok(()) }
}

pub async fn write_head<T: Serialize>(send: &mut SendStream, head: &T) -> Result<()> {
    let json = serde_json::to_vec(head)?;
    check_len("head", json.len(), MAX_HEAD_LEN)?;
    send.write_all(&(json.len() as u32).to_be_bytes()).await?;
    send.write_all(&json).await?;
    Ok(())
}

pub async fn read_head<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    read_head_limited(recv, MAX_HEAD_LEN).await
}

/// Like [`read_head`], with a custom size limit (at most [`MAX_HEAD_LEN`] is useful, since
/// writers never send more).
pub async fn read_head_limited<T: DeserializeOwned>(recv: &mut RecvStream, max_len: usize) -> Result<T> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await.context("reading head length")?;
    let len = u32::from_be_bytes(len) as usize;
    check_len("head", len, max_len)?;
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.context("reading head")?;
    Ok(serde_json::from_slice(&buf)?)
}

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

    /// Builds a frame from its opcode and payload as read off the wire.
    fn decode(op: u8, payload: Vec<u8>) -> Result<Self> {
        Ok(match op {
            Self::TEXT => Self::Text(String::from_utf8(payload).context("text frame is not utf-8")?),
            Self::BINARY => Self::Binary(payload),
            Self::CLOSE => {
                ensure!(payload.len() >= 2, "close frame too short");
                let code = u16::from_be_bytes([payload[0], payload[1]]);
                Self::Close(code, String::from_utf8_lossy(&payload[2..]).into_owned())
            }
            op => anyhow::bail!("unknown frame opcode {op}"),
        })
    }
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
    check_len("frame", payload.len(), MAX_FRAME_LEN)?;
    let mut header = [0u8; 5];
    header[0] = op;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    send.write_all(&header).await?;
    send.write_all(&payload).await?;
    Ok(())
}

/// Reads the next frame, or `None` when the peer finished the stream.
pub async fn read_frame(recv: &mut RecvStream) -> Result<Option<WsFrame>> {
    read_frame_limited(recv, MAX_FRAME_LEN).await
}

/// Like [`read_frame`], refusing frames larger than `max_len` before reading their payload.
pub async fn read_frame_limited(recv: &mut RecvStream, max_len: usize) -> Result<Option<WsFrame>> {
    let mut header = [0u8; 5];
    match recv.read_exact(&mut header).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    check_len("frame", len, max_len)?;
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload).await.context("reading frame")?;
    WsFrame::decode(header[0], payload).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_limits() {
        assert!(check_len("head", MAX_HEAD_LEN, MAX_HEAD_LEN).is_ok());
        let err = check_len("head", u32::MAX as usize, MAX_HEAD_LEN).unwrap_err();
        assert_eq!(err, TooLarge { what: "head", len: u32::MAX as usize, max: MAX_HEAD_LEN });
        // Callers tell a size refusal apart from other errors by downcasting.
        let any: anyhow::Error = err.into();
        assert!(any.downcast_ref::<TooLarge>().is_some());
    }

    #[test]
    fn frame_decoding() {
        assert_eq!(WsFrame::decode(1, b"hi".to_vec()).unwrap(), WsFrame::Text("hi".into()));
        assert_eq!(WsFrame::decode(2, vec![0, 255]).unwrap(), WsFrame::Binary(vec![0, 255]));
        assert_eq!(WsFrame::decode(8, vec![0x03, 0xe8, b'o', b'k']).unwrap(), WsFrame::Close(1000, "ok".into()));
        assert!(WsFrame::decode(1, vec![0xff, 0xfe]).is_err(), "text must be utf-8");
        assert!(WsFrame::decode(8, vec![0x03]).is_err(), "close needs a code");
        assert!(WsFrame::decode(9, Vec::new()).is_err(), "unknown opcode");
    }

    #[test]
    fn heads_are_backward_compatible() {
        // Heads from 0.1.x clients omit `websocket`; it must default to false.
        let head: RequestHead = serde_json::from_str(r#"{"method":"GET","path":"/","headers":[["a","b"]]}"#).unwrap();
        assert!(!head.websocket);
        assert_eq!(serde_json::to_string(&head).unwrap(), r#"{"method":"GET","path":"/","headers":[["a","b"]]}"#);
    }
}

/// Builds a relay map from a comma-separated list of relay URLs. The entry `n0` (or `public`) adds
/// n0's public relays, e.g. `https://relay.example.com,n0` uses your relay with the public ones as a
/// fallback when it is down. The token goes only to the URLs you listed, never to n0's relays.
pub fn relay_map(spec: &str, token: Option<&str>) -> Result<iroh::RelayMap> {
    let map = iroh::RelayMap::empty();
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if entry.eq_ignore_ascii_case("n0") || entry.eq_ignore_ascii_case("public") {
            map.extend(&iroh::RelayMode::Default.relay_map());
            continue;
        }
        let url: iroh::RelayUrl = entry.parse().with_context(|| format!("invalid relay url {entry:?}"))?;
        let mut relay = iroh::RelayConfig::from(url.clone());
        if let Some(token) = token.filter(|t| !t.is_empty()) {
            relay = relay.with_auth_token(token);
        }
        map.insert(url, std::sync::Arc::new(relay));
    }
    ensure!(!map.is_empty(), "no relay given");
    Ok(map)
}

/// The relay URLs explicitly listed in a relay spec (everything but `n0` / `public`).
pub fn listed_relays(spec: &str) -> Vec<iroh::RelayUrl> {
    spec.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("n0") && !e.eq_ignore_ascii_case("public"))
        .filter_map(|e| e.parse().ok())
        .collect()
}

#[cfg(test)]
mod relay_tests {
    #[test]
    fn relay_lists_and_public_fallback() {
        let own = super::relay_map("https://relay.example.com", Some("t")).unwrap();
        assert_eq!(own.len(), 1);
        let both = super::relay_map("https://relay.example.com, n0", Some("t")).unwrap();
        assert!(both.len() > 1);
        // The token is only attached to the relay we listed, never to the public ones.
        for relay in both.relays::<Vec<_>>() {
            let ours = relay.url.as_str().contains("relay.example.com");
            assert_eq!(relay.auth_token.is_some(), ours, "{}", relay.url);
        }
        assert!(super::relay_map(" , ", None).is_err());
        assert!(super::relay_map("not a url", None).is_err());
    }
}
