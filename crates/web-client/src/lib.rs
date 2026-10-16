//! iroh HTTP client compiled to WebAssembly. Runs in browsers and in Node.js >= 22.
//! Browsers can't send raw UDP, so connections always go through an iroh relay.

use std::rc::Rc;

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointId, RelayMode, RelayUrl,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use js_sys::{Object, Promise, Reflect, Uint8Array};
use n0_future::StreamExt;
use proto::{ALPN, RequestHead, ResponseHead, WsFrame};
use tokio::sync::Mutex;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

const CHUNK_SIZE: usize = 64 * 1024;

#[wasm_bindgen(start)]
fn start() {
    console_error_panic_hook::set_once();
}

/// Turn on iroh's tracing output in the JS console. `level` is e.g. "info" or "debug".
#[wasm_bindgen(js_name = enableLogging)]
pub fn enable_logging(level: &str) {
    let level: tracing::Level = level.parse().unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(tracing_subscriber_wasm::MakeConsoleWriter::default())
        .without_time()
        .with_ansi(false)
        .try_init();
}

struct Inner {
    endpoint: Endpoint,
    remote: EndpointId,
    conn: Mutex<Option<Connection>>,
}

#[wasm_bindgen]
pub struct IrohClient(Rc<Inner>);

#[wasm_bindgen]
impl IrohClient {
    /// Create a client that talks to the gateway with the given endpoint id.
    /// `relay_url` must match the gateway's relay if it uses a custom one.
    pub async fn create(remote: String, relay_url: Option<String>) -> Result<IrohClient, JsError> {
        let remote: EndpointId = remote.trim().parse().context("invalid endpoint id").map_err(js)?;
        let mut builder = Endpoint::builder(presets::N0);
        if let Some(url) = relay_url.filter(|u| !u.is_empty()) {
            let url: RelayUrl = url.parse().context("invalid relay url").map_err(js)?;
            builder = builder.relay_mode(RelayMode::Custom(url.into()));
        }
        let endpoint = builder.bind().await.map_err(js)?;
        Ok(IrohClient(Rc::new(Inner { endpoint, remote, conn: Mutex::new(None) })))
    }

    /// This client's own endpoint id.
    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.0.endpoint.id().to_string()
    }

    /// Open the connection now instead of on the first request.
    pub fn connect(&self) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            inner.connection().await.map_err(js_val)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Send one HTTP request. `headers` is an array of `[name, value]` pairs.
    /// Resolves to `{ status, headers, body: ReadableStream<Uint8Array> }`; the body streams
    /// in as the backend produces it.
    pub fn request(&self, method: String, path: String, headers: JsValue, body: Option<Uint8Array>) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            let head = RequestHead { method, path, headers: headers_from_js(headers)?, websocket: false };
            let body = body.map(|b| b.to_vec()).unwrap_or_default();
            let (resp, recv) = inner.start(&head, &body).await.map_err(js_val)?;
            let out = response_object(&resp)?;
            Reflect::set(&out, &"body".into(), &body_stream(recv).into())?;
            Ok(out.into())
        })
    }

    /// Open a WebSocket to `path` on the backend. Resolves to `{ status, headers, socket }` where
    /// `socket` is set only when the backend accepted the upgrade (status 101); otherwise `body`
    /// holds the backend's error response.
    pub fn websocket(&self, path: String, headers: JsValue) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            let head = RequestHead { method: "GET".into(), path, headers: headers_from_js(headers)?, websocket: true };
            let (send, recv) = inner.open(&head).await.map_err(js_val)?;
            let mut recv = recv;
            let resp: ResponseHead = proto::read_head(&mut recv).await.map_err(js_val)?;
            let out = response_object(&resp)?;
            if resp.status == 101 {
                let socket = WebSocketConn { send: Rc::new(Mutex::new(send)), recv: Rc::new(Mutex::new(recv)) };
                Reflect::set(&out, &"socket".into(), &socket.into())?;
            } else {
                Reflect::set(&out, &"body".into(), &body_stream(recv).into())?;
            }
            Ok(out.into())
        })
    }

    /// Close the connection and the local endpoint.
    pub fn close(&self) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            if let Some(conn) = inner.conn.lock().await.take() {
                conn.close(0u32.into(), b"bye");
            }
            inner.endpoint.close().await;
            Ok(JsValue::UNDEFINED)
        })
    }
}

impl Inner {
    /// Returns the live connection, dialing (again) if there is none or it was lost.
    async fn connection(&self) -> Result<Connection> {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref() {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        let conn = self.endpoint.connect(self.remote, ALPN).await.context("connecting to gateway")?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Opens a fresh stream and sends the request head, retrying once on a stale connection.
    async fn open(&self, head: &RequestHead) -> Result<(SendStream, RecvStream)> {
        let conn = self.connection().await?;
        let (mut send, recv) = match conn.open_bi().await {
            Ok(streams) => streams,
            Err(_) => {
                *self.conn.lock().await = None;
                self.connection().await?.open_bi().await?
            }
        };
        proto::write_head(&mut send, head).await?;
        Ok((send, recv))
    }

    async fn start(&self, head: &RequestHead, body: &[u8]) -> Result<(ResponseHead, RecvStream)> {
        let (mut send, mut recv) = self.open(head).await?;
        send.write_all(body).await?;
        send.finish()?;
        let resp: ResponseHead = proto::read_head(&mut recv).await?;
        Ok((resp, recv))
    }
}

/// One WebSocket tunnelled over an iroh stream. Driven by the JS `IrohWebSocket` wrapper.
#[wasm_bindgen]
pub struct WebSocketConn {
    send: Rc<Mutex<SendStream>>,
    recv: Rc<Mutex<RecvStream>>,
}

#[wasm_bindgen]
impl WebSocketConn {
    /// Resolves to the next message: `{ type: "text", data: string }`,
    /// `{ type: "binary", data: Uint8Array }`, `{ type: "close", code, reason }`, or `null` at the end.
    pub fn recv(&self) -> Promise {
        let recv = self.recv.clone();
        future_to_promise(async move {
            let frame = proto::read_frame(&mut *recv.lock().await).await.map_err(js_val)?;
            let Some(frame) = frame else { return Ok(JsValue::NULL) };
            let out = Object::new();
            match frame {
                WsFrame::Text(t) => {
                    Reflect::set(&out, &"type".into(), &"text".into())?;
                    Reflect::set(&out, &"data".into(), &t.into())?;
                }
                WsFrame::Binary(b) => {
                    Reflect::set(&out, &"type".into(), &"binary".into())?;
                    Reflect::set(&out, &"data".into(), &Uint8Array::from(&b[..]))?;
                }
                WsFrame::Close(code, reason) => {
                    Reflect::set(&out, &"type".into(), &"close".into())?;
                    Reflect::set(&out, &"code".into(), &code.into())?;
                    Reflect::set(&out, &"reason".into(), &reason.into())?;
                }
            }
            Ok(out.into())
        })
    }

    #[wasm_bindgen(js_name = sendText)]
    pub fn send_text(&self, text: String) -> Promise {
        self.send_frame(WsFrame::Text(text), false)
    }

    #[wasm_bindgen(js_name = sendBinary)]
    pub fn send_binary(&self, data: Uint8Array) -> Promise {
        self.send_frame(WsFrame::Binary(data.to_vec()), false)
    }

    /// Sends a close frame and finishes our side of the stream.
    pub fn close(&self, code: u16, reason: String) -> Promise {
        self.send_frame(WsFrame::Close(code, reason), true)
    }

    fn send_frame(&self, frame: WsFrame, finish: bool) -> Promise {
        let send = self.send.clone();
        future_to_promise(async move {
            let mut send = send.lock().await;
            proto::write_frame(&mut send, &frame).await.map_err(js_val)?;
            if finish {
                send.finish().map_err(|e| js_val(e.into()))?;
            }
            Ok(JsValue::UNDEFINED)
        })
    }
}

fn headers_from_js(headers: JsValue) -> Result<Vec<(String, String)>, JsValue> {
    if headers.is_undefined() || headers.is_null() {
        return Ok(Vec::new());
    }
    serde_wasm_bindgen::from_value(headers).map_err(|e| JsError::new(&e.to_string()).into())
}

fn response_object(resp: &ResponseHead) -> Result<Object, JsValue> {
    let out = Object::new();
    Reflect::set(&out, &"status".into(), &resp.status.into())?;
    Reflect::set(&out, &"headers".into(), &serde_wasm_bindgen::to_value(&resp.headers)?)?;
    Ok(out)
}

/// Exposes the rest of a QUIC stream as a JS `ReadableStream<Uint8Array>`.
/// Cancelling the JS stream drops the `RecvStream`, which tells the gateway to stop sending.
fn body_stream(recv: RecvStream) -> wasm_streams::readable::sys::ReadableStream {
    let chunks = n0_future::stream::unfold(Some(recv), |recv| async move {
        let mut recv = recv?;
        match recv.read_chunk(CHUNK_SIZE).await {
            Ok(Some(bytes)) => Some((Ok(JsValue::from(Uint8Array::from(&bytes[..]))), Some(recv))),
            Ok(None) => None,
            Err(err) => Some((Err(JsValue::from(JsError::new(&err.to_string()))), None)),
        }
    });
    wasm_streams::ReadableStream::from_stream(chunks.boxed_local()).into_raw()
}

fn js(err: impl Into<anyhow::Error>) -> JsError {
    JsError::new(&format!("{:#}", err.into()))
}

fn js_val(err: anyhow::Error) -> JsValue {
    js(err).into()
}
