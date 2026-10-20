//! iroh HTTP client compiled to WebAssembly. Runs in browsers and in Node.js >= 22.
//! Browsers can't send raw UDP, so connections always go through an iroh relay.
//! The protocol logic lives in `client-core`; this crate only maps it to JS values.
//! Native Node.js uses `node-client` instead; built for any other target this crate is empty.
#![cfg(target_family = "wasm")]

use std::rc::Rc;

use client_core::{Body, Client, Options, Profile, RequestHead, ResponseBody, ResponseHead, WsFrame, WsOutcome};
use js_sys::{Object, Promise, Reflect, Uint8Array};
use n0_future::StreamExt;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::future_to_promise;

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

#[wasm_bindgen]
pub struct IrohClient(Rc<Client>);

#[wasm_bindgen]
impl IrohClient {
    /// Create a local endpoint that can talk to any number of gateways.
    /// `relay_url` must match the gateways' relay if they use a custom one; `relay_token` is
    /// for relays that only admit clients with a token.
    ///
    /// `secret_key` (64 hex chars) gives the client a fixed endpoint id; without it a new one is
    /// generated, which `secretKey()` returns so it can be kept.
    pub async fn create(
        relay_url: Option<String>,
        relay_token: Option<String>,
        secret_key: Option<String>,
    ) -> Result<IrohClient, JsError> {
        let client = Client::create(Profile::Browser, Options { relay_url, relay_token, secret_key }).await.map_err(js)?;
        Ok(IrohClient(Rc::new(client)))
    }

    /// Why the home relay can't be used, if it can't: e.g. it refused the relay token. `None`
    /// while the relay is fine or nothing has failed yet.
    #[wasm_bindgen(js_name = relayProblem)]
    pub fn relay_problem(&self) -> Option<String> {
        self.0.relay_problem()
    }

    /// This client's secret key as hex, to keep its endpoint id across sessions. Treat it like a
    /// password: whoever has it can act as this client.
    #[wasm_bindgen(js_name = secretKey)]
    pub fn secret_key(&self) -> String {
        self.0.secret_key()
    }

    /// This client's own endpoint id.
    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.0.endpoint_id()
    }

    /// The network paths of the connection to `remote`: `{ kind: "relay" | "direct" | "none",
    /// paths: [{ kind, addr, selected, rttMs }] }`, or `null` without a connection.
    #[wasm_bindgen(js_name = connectionInfo)]
    pub fn connection_info(&self, remote: String) -> Result<JsValue, JsError> {
        let info = self.0.connection_info(&remote).map_err(js)?;
        serde_wasm_bindgen::to_value(&info).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Open the connection to the gateway `remote` now instead of on the first request.
    /// `relay` (also on `request` and `websocket`) is the gateway's relay URL if known: it is
    /// hinted instead of the configured relays, with an address lookup as the fallback.
    pub fn connect(&self, remote: String, relay: Option<String>) -> Promise {
        let client = self.0.clone();
        future_to_promise(async move {
            client.connect(&remote, relay.as_deref()).await.map_err(js_val)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Send one HTTP request. `headers` is an array of `[name, value]` pairs and `body` is
    /// `undefined`, a `Uint8Array`, or a `ReadableStream<Uint8Array>` that is uploaded as it is read.
    /// Resolves to `{ status, headers, body: ReadableStream<Uint8Array> }`; the response body
    /// streams in as the backend produces it.
    pub fn request(&self, remote: String, method: String, path: String, headers: JsValue, body: JsValue, relay: Option<String>) -> Promise {
        let client = self.0.clone();
        future_to_promise(async move {
            let head = RequestHead { method, path, headers: headers_from_js(headers)?, websocket: false };
            let body = body_from_js(body)?;
            let (resp, body) = client.request(&remote, relay.as_deref(), head, body).await.map_err(js_val)?;
            let out = response_object(&resp)?;
            Reflect::set(&out, &"body".into(), &body_stream(body).into())?;
            Ok(out.into())
        })
    }

    /// Open a WebSocket to `path` on the backend. Resolves to `{ status, headers, socket }` where
    /// `socket` is set only when the backend accepted the upgrade (status 101); otherwise `body`
    /// holds the backend's error response.
    pub fn websocket(&self, remote: String, path: String, headers: JsValue, relay: Option<String>) -> Promise {
        let client = self.0.clone();
        future_to_promise(async move {
            let (resp, outcome) = client.websocket(&remote, relay.as_deref(), path, headers_from_js(headers)?).await.map_err(js_val)?;
            let out = response_object(&resp)?;
            match outcome {
                WsOutcome::Socket(socket) => Reflect::set(&out, &"socket".into(), &WebSocketConn(socket).into())?,
                WsOutcome::Rejected(body) => Reflect::set(&out, &"body".into(), &body_stream(body).into())?,
            };
            Ok(out.into())
        })
    }

    /// Close all connections and the local endpoint.
    pub fn close(&self) -> Promise {
        let client = self.0.clone();
        future_to_promise(async move {
            client.close().await;
            Ok(JsValue::UNDEFINED)
        })
    }
}

fn body_from_js(body: JsValue) -> Result<Body, JsValue> {
    if body.is_undefined() || body.is_null() {
        return Ok(Body::Bytes(Vec::new()));
    }
    if let Some(bytes) = body.dyn_ref::<Uint8Array>() {
        return Ok(Body::Bytes(bytes.to_vec()));
    }
    let stream = body
        .dyn_into::<wasm_streams::readable::sys::ReadableStream>()
        .map_err(|_| JsError::new("body must be a Uint8Array or a ReadableStream<Uint8Array>"))?;
    let chunks = wasm_streams::ReadableStream::from_raw(stream).into_stream().map(|chunk| {
        let chunk = chunk.map_err(|e| anyhow::anyhow!("body stream errored: {e:?}"))?;
        let chunk = chunk
            .dyn_into::<Uint8Array>()
            .map_err(|_| anyhow::anyhow!("body stream chunks must be Uint8Array"))?;
        Ok(chunk.to_vec())
    });
    Ok(Body::Stream(Box::pin(chunks)))
}

/// One WebSocket tunnelled over an iroh stream. Driven by the JS `IrohWebSocket` wrapper.
#[wasm_bindgen]
pub struct WebSocketConn(client_core::WebSocket);

#[wasm_bindgen]
impl WebSocketConn {
    /// Resolves to the next message: `{ type: "text", data: string }`,
    /// `{ type: "binary", data: Uint8Array }`, `{ type: "close", code, reason }`, or `null` at the end.
    pub fn recv(&self) -> Promise {
        let socket = self.0.clone();
        future_to_promise(async move {
            let frame = socket.recv().await.map_err(js_val)?;
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
        let socket = self.0.clone();
        future_to_promise(async move {
            socket.send(frame, finish).await.map_err(js_val)?;
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

/// Exposes the rest of a response as a JS `ReadableStream<Uint8Array>`.
/// Cancelling the JS stream drops the body, which tells the gateway to stop sending.
fn body_stream(body: ResponseBody) -> wasm_streams::readable::sys::ReadableStream {
    let chunks = n0_future::stream::unfold(Some(body), |body| async move {
        let mut body = body?;
        match body.next_chunk().await {
            Ok(Some(bytes)) => Some((Ok(JsValue::from(Uint8Array::from(&bytes[..]))), Some(body))),
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
