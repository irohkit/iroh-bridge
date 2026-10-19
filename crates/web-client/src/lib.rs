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
use wasm_bindgen::{JsCast, prelude::*};
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
    /// One connection per gateway, all sharing this endpoint (and its relay connection).
    conns: std::cell::RefCell<std::collections::HashMap<EndpointId, Rc<Mutex<Option<Connection>>>>>,
}

#[wasm_bindgen]
pub struct IrohClient(Rc<Inner>);

#[wasm_bindgen]
impl IrohClient {
    /// Create a local endpoint that can talk to any number of gateways.
    /// `relay_url` must match the gateways' relay if they use a custom one.
    pub async fn create(relay_url: Option<String>) -> Result<IrohClient, JsError> {
        let mut builder = Endpoint::builder(presets::N0);
        if let Some(url) = relay_url.filter(|u| !u.is_empty()) {
            let url: RelayUrl = url.parse().context("invalid relay url").map_err(js)?;
            builder = builder.relay_mode(RelayMode::Custom(url.into()));
        }
        let endpoint = builder.bind().await.map_err(js)?;
        Ok(IrohClient(Rc::new(Inner { endpoint, conns: Default::default() })))
    }

    /// This client's own endpoint id.
    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.0.endpoint.id().to_string()
    }

    /// Open the connection to the gateway `remote` now instead of on the first request.
    pub fn connect(&self, remote: String) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            inner.connection(parse_remote(&remote)?).await.map_err(js_val)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Send one HTTP request. `headers` is an array of `[name, value]` pairs and `body` is
    /// `undefined`, a `Uint8Array`, or a `ReadableStream<Uint8Array>` that is uploaded as it is read.
    /// Resolves to `{ status, headers, body: ReadableStream<Uint8Array> }`; the response body
    /// streams in as the backend produces it.
    pub fn request(&self, remote: String, method: String, path: String, headers: JsValue, body: JsValue) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            let remote = parse_remote(&remote)?;
            let head = RequestHead { method, path, headers: headers_from_js(headers)?, websocket: false };
            let body = RequestBody::from_js(body)?;
            let (resp, recv) = inner.start(remote, &head, body).await.map_err(js_val)?;
            let out = response_object(&resp)?;
            Reflect::set(&out, &"body".into(), &body_stream(recv).into())?;
            Ok(out.into())
        })
    }

    /// Open a WebSocket to `path` on the backend. Resolves to `{ status, headers, socket }` where
    /// `socket` is set only when the backend accepted the upgrade (status 101); otherwise `body`
    /// holds the backend's error response.
    pub fn websocket(&self, remote: String, path: String, headers: JsValue) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            let remote = parse_remote(&remote)?;
            let head = RequestHead { method: "GET".into(), path, headers: headers_from_js(headers)?, websocket: true };
            let (send, recv) = inner.open(remote, &head).await.map_err(js_val)?;
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

    /// Close all connections and the local endpoint.
    pub fn close(&self) -> Promise {
        let inner = self.0.clone();
        future_to_promise(async move {
            let slots: Vec<_> = inner.conns.borrow_mut().drain().map(|(_, slot)| slot).collect();
            for slot in slots {
                if let Some(conn) = slot.lock().await.take() {
                    conn.close(0u32.into(), b"bye");
                }
            }
            inner.endpoint.close().await;
            Ok(JsValue::UNDEFINED)
        })
    }
}

impl Inner {
    /// Returns the live connection, dialing (again) if there is none or it was lost.
    async fn connection(&self, remote: EndpointId) -> Result<Connection> {
        // Each gateway has its own slot, so dialing one doesn't hold up requests to another.
        let slot = self.conns.borrow_mut().entry(remote).or_default().clone();
        let mut guard = slot.lock().await;
        if let Some(conn) = guard.as_ref() {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        let conn = self.endpoint.connect(remote, ALPN).await.context("connecting to gateway")?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Opens a fresh stream and sends the request head, retrying once on a stale connection.
    async fn open(&self, remote: EndpointId, head: &RequestHead) -> Result<(SendStream, RecvStream)> {
        let conn = self.connection(remote).await?;
        let (mut send, recv) = match conn.open_bi().await {
            Ok(streams) => streams,
            Err(_) => {
                let slot = self.conns.borrow().get(&remote).cloned();
                if let Some(slot) = slot {
                    *slot.lock().await = None;
                }
                self.connection(remote).await?.open_bi().await?
            }
        };
        proto::write_head(&mut send, head).await?;
        Ok((send, recv))
    }

    async fn start(&self, remote: EndpointId, head: &RequestHead, body: RequestBody) -> Result<(ResponseHead, RecvStream)> {
        let (send, mut recv) = self.open(remote, head).await?;
        // Upload while waiting for the response so the backend may answer (or stream back)
        // before the request body is complete.
        let upload_error = Rc::new(std::cell::RefCell::new(None::<String>));
        wasm_bindgen_futures::spawn_local({
            let upload_error = upload_error.clone();
            async move {
                if let Err(err) = body.upload(send).await {
                    *upload_error.borrow_mut() = Some(format!("{err:#}"));
                }
            }
        });
        match proto::read_head::<ResponseHead>(&mut recv).await {
            Ok(resp) => Ok((resp, recv)),
            Err(err) => match upload_error.borrow_mut().take() {
                Some(upload) => Err(anyhow::anyhow!("uploading request body failed: {upload}")),
                None => Err(err),
            },
        }
    }
}

enum RequestBody {
    Bytes(Vec<u8>),
    Stream(wasm_streams::ReadableStream),
}

impl RequestBody {
    fn from_js(body: JsValue) -> Result<Self, JsValue> {
        if body.is_undefined() || body.is_null() {
            return Ok(Self::Bytes(Vec::new()));
        }
        if let Some(bytes) = body.dyn_ref::<Uint8Array>() {
            return Ok(Self::Bytes(bytes.to_vec()));
        }
        match body.dyn_into::<wasm_streams::readable::sys::ReadableStream>() {
            Ok(stream) => Ok(Self::Stream(wasm_streams::ReadableStream::from_raw(stream))),
            Err(_) => Err(JsError::new("body must be a Uint8Array or a ReadableStream<Uint8Array>").into()),
        }
    }

    async fn upload(self, mut send: SendStream) -> Result<()> {
        let res = async {
            match self {
                Self::Bytes(bytes) => send.write_all(&bytes).await?,
                Self::Stream(stream) => {
                    let mut chunks = stream.into_stream();
                    while let Some(chunk) = chunks.next().await {
                        let chunk = chunk.map_err(|e| anyhow::anyhow!("body stream errored: {e:?}"))?;
                        let chunk = chunk
                            .dyn_into::<Uint8Array>()
                            .map_err(|_| anyhow::anyhow!("body stream chunks must be Uint8Array"))?;
                        send.write_all(&chunk.to_vec()).await?;
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

fn parse_remote(remote: &str) -> Result<EndpointId, JsValue> {
    remote.trim().parse().map_err(|e| JsError::new(&format!("invalid endpoint id {remote:?}: {e}")).into())
}

fn js(err: impl Into<anyhow::Error>) -> JsError {
    JsError::new(&format!("{:#}", err.into()))
}

fn js_val(err: anyhow::Error) -> JsValue {
    js(err).into()
}
