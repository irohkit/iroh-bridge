//! Native Node.js addon for iroh-fetch. Same client as the WASM build (`client-core`), but on
//! iroh's default preset with real UDP sockets, so connections to a gateway hole-punch a direct
//! path and only use a relay until then (or when that fails).
//!
//! The JS wrapper `packages/iroh-fetch/src/native.js` turns this into exactly the API of the WASM
//! `IrohClient` (WHATWG streams in and out); the classes here deal in chunks so that no JS object
//! has to be touched off the main thread.

use std::sync::{Arc, Mutex as StdMutex};

use client_core::{Body, Client, Options, Profile, RequestHead, ResponseBody, WsFrame, WsOutcome};
use napi::{
    Env,
    bindgen_prelude::{Either, PromiseRaw, Uint8Array},
};
use napi_derive::napi;
use tokio::sync::{Mutex, mpsc, watch};

type NapiResult<T> = napi::Result<T>;

fn err(e: anyhow::Error) -> napi::Error {
    napi::Error::from_reason(format!("{e:#}"))
}

/// Logs iroh's tracing output to stderr. `level` is e.g. "info" or "debug".
#[napi(js_name = "enableLogging")]
pub fn enable_logging(level: String) {
    let level: tracing::Level = level.parse().unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt().with_max_level(level).with_writer(std::io::stderr).try_init();
}

/// Creates a client endpoint; see `IrohClient.create` in the WASM build.
#[napi(js_name = "createClient")]
pub fn create_client(
    env: &Env,
    relay_url: Option<String>,
    relay_token: Option<String>,
    secret_key: Option<String>,
) -> NapiResult<PromiseRaw<'_, NativeClient>> {
    env.spawn_future(async move {
        let client = Client::create(Profile::Native, Options { relay_url, relay_token, secret_key }).await.map_err(err)?;
        Ok(NativeClient(Arc::new(client)))
    })
}

#[napi]
pub struct NativeClient(Arc<Client>);

#[napi]
impl NativeClient {
    #[napi(js_name = "relayProblem")]
    pub fn relay_problem(&self) -> Option<String> {
        self.0.relay_problem()
    }

    #[napi(js_name = "secretKey")]
    pub fn secret_key(&self) -> String {
        self.0.secret_key()
    }

    #[napi(js_name = "endpointId")]
    pub fn endpoint_id(&self) -> String {
        self.0.endpoint_id()
    }

    /// `{ kind: "direct" | "relay" | "none", paths: [{ kind, addr, selected, rttMs }] }` or `null`.
    #[napi(js_name = "connectionInfo")]
    pub fn connection_info(&self, remote: String) -> NapiResult<serde_json::Value> {
        let info = self.0.connection_info(&remote).map_err(err)?;
        serde_json::to_value(info).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    #[napi]
    pub fn connect<'e>(&self, env: &'e Env, remote: String) -> NapiResult<PromiseRaw<'e, ()>> {
        let client = self.0.clone();
        env.spawn_future(async move { client.connect(&remote).await.map_err(err) })
    }

    /// Sends one request. The body is `bytes`, or what is written to `upload` (streamed).
    /// Resolves once the response head arrived.
    #[napi]
    pub fn request<'e>(
        &self,
        env: &'e Env,
        remote: String,
        method: String,
        path: String,
        headers: Vec<Vec<String>>,
        bytes: Option<Uint8Array>,
        upload: Option<&NativeUpload>,
    ) -> NapiResult<PromiseRaw<'e, NativeResponse>> {
        let client = self.0.clone();
        // Take the upload first: if anything below fails, dropping it unblocks the JS writer.
        let body = match upload {
            Some(upload) => {
                let rx = upload.rx.lock().expect("poisoned").take();
                let rx = rx.ok_or_else(|| napi::Error::from_reason("upload already used"))?;
                Body::Stream(Box::pin(upload_stream(rx)))
            }
            None => Body::Bytes(bytes.map(|b| b.to_vec()).unwrap_or_default()),
        };
        let headers = pairs(headers)?;
        env.spawn_future(async move {
            let head = RequestHead { method, path, headers, websocket: false };
            let (resp, body) = client.request(&remote, head, body).await.map_err(err)?;
            Ok(NativeResponse::new(resp, Some(body), None))
        })
    }

    /// Opens a WebSocket; `takeSocket()` on the result is set only for status 101.
    #[napi]
    pub fn websocket<'e>(&self, env: &'e Env, remote: String, path: String, headers: Vec<Vec<String>>) -> NapiResult<PromiseRaw<'e, NativeResponse>> {
        let client = self.0.clone();
        let headers = pairs(headers)?;
        env.spawn_future(async move {
            let (resp, outcome) = client.websocket(&remote, path, headers).await.map_err(err)?;
            Ok(match outcome {
                WsOutcome::Socket(socket) => NativeResponse::new(resp, None, Some(socket)),
                WsOutcome::Rejected(body) => NativeResponse::new(resp, Some(body), None),
            })
        })
    }

    #[napi]
    pub fn close<'e>(&self, env: &'e Env) -> NapiResult<PromiseRaw<'e, ()>> {
        let client = self.0.clone();
        env.spawn_future(async move {
            client.close().await;
            Ok(())
        })
    }
}

fn pairs(headers: Vec<Vec<String>>) -> NapiResult<Vec<(String, String)>> {
    headers
        .into_iter()
        .map(|pair| match <[String; 2]>::try_from(pair) {
            Ok([name, value]) => Ok((name, value)),
            Err(_) => Err(napi::Error::from_reason("headers must be [name, value] pairs")),
        })
        .collect()
}

/// Adapts the upload channel to the stream the core uploads from.
fn upload_stream(rx: mpsc::Receiver<Result<Vec<u8>, String>>) -> impl n0_future::Stream<Item = anyhow::Result<Vec<u8>>> + Send + 'static {
    n0_future::stream::unfold(rx, |mut rx| async move {
        let item = rx.recv().await?;
        Some((item.map_err(|e| anyhow::anyhow!("body stream errored: {e}")), rx))
    })
}

/// A request body that JS writes while it is being uploaded.
#[napi]
pub struct NativeUpload {
    tx: StdMutex<Option<mpsc::Sender<Result<Vec<u8>, String>>>>,
    rx: StdMutex<Option<mpsc::Receiver<Result<Vec<u8>, String>>>>,
}

#[napi]
impl NativeUpload {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // A few chunks in flight keep the QUIC stream busy; more would just buffer memory.
        let (tx, rx) = mpsc::channel(4);
        Self { tx: StdMutex::new(Some(tx)), rx: StdMutex::new(Some(rx)) }
    }

    /// Queues a chunk. Resolves once there is room for the next one; rejects once the upload
    /// stopped (the request failed or the gateway stopped reading).
    #[napi]
    pub fn write<'e>(&self, env: &'e Env, chunk: Uint8Array) -> NapiResult<PromiseRaw<'e, ()>> {
        let tx = self.tx.lock().expect("poisoned").clone();
        let chunk = chunk.to_vec();
        env.spawn_future(async move {
            let tx = tx.ok_or_else(|| napi::Error::from_reason("upload already finished"))?;
            tx.send(Ok(chunk)).await.map_err(|_| napi::Error::from_reason("upload stopped"))
        })
    }

    /// Ends the body.
    #[napi]
    pub fn finish(&self) {
        self.tx.lock().expect("poisoned").take();
    }

    /// Fails the body, which resets the stream so the gateway drops the request.
    #[napi]
    pub fn abort(&self, reason: String) {
        // Not handed to a request (yet): drop the receiver so pending writes fail.
        self.rx.lock().expect("poisoned").take();
        if let Some(tx) = self.tx.lock().expect("poisoned").take() {
            napi::bindgen_prelude::spawn(async move {
                let _ = tx.send(Err(reason)).await;
            });
        }
    }
}

/// A response head plus its body (or, for an accepted WebSocket, the socket).
#[napi]
pub struct NativeResponse {
    status: u32,
    headers: Vec<Vec<String>>,
    body: Option<NativeBody>,
    socket: StdMutex<Option<client_core::WebSocket>>,
}

impl NativeResponse {
    fn new(head: client_core::ResponseHead, body: Option<ResponseBody>, socket: Option<client_core::WebSocket>) -> Self {
        Self {
            status: head.status.into(),
            headers: head.headers.into_iter().map(|(k, v)| vec![k, v]).collect(),
            body: body.map(NativeBody::new),
            socket: StdMutex::new(socket),
        }
    }
}

#[napi]
impl NativeResponse {
    #[napi(getter)]
    pub fn status(&self) -> u32 {
        self.status
    }

    #[napi(getter)]
    pub fn headers(&self) -> Vec<Vec<String>> {
        self.headers.clone()
    }

    /// The next body chunk, or `null` at the end.
    #[napi]
    pub fn read<'e>(&self, env: &'e Env) -> NapiResult<PromiseRaw<'e, Option<Uint8Array>>> {
        let body = self.body.clone();
        env.spawn_future(async move {
            match body {
                Some(body) => body.read().await,
                None => Ok(None),
            }
        })
    }

    /// Stops the body transfer; a pending `read()` resolves to `null`.
    #[napi]
    pub fn cancel(&self) {
        if let Some(body) = &self.body {
            body.cancel();
        }
    }

    #[napi(js_name = "takeSocket")]
    pub fn take_socket(&self) -> Option<NativeWebSocket> {
        self.socket.lock().expect("poisoned").take().map(NativeWebSocket)
    }
}

#[derive(Clone)]
struct NativeBody {
    body: Arc<Mutex<ResponseBody>>,
    cancelled: watch::Sender<bool>,
}

impl NativeBody {
    fn new(body: ResponseBody) -> Self {
        Self { body: Arc::new(Mutex::new(body)), cancelled: watch::Sender::new(false) }
    }

    async fn read(&self) -> NapiResult<Option<Uint8Array>> {
        let mut cancelled = self.cancelled.subscribe();
        let mut body = self.body.lock().await;
        if *cancelled.borrow() {
            body.cancel();
            return Ok(None);
        }
        tokio::select! {
            chunk = body.next_chunk() => Ok(chunk.map_err(err)?.map(|b| Uint8Array::new(b.to_vec()))),
            _ = cancelled.wait_for(|c| *c) => {
                body.cancel();
                Ok(None)
            }
        }
    }

    fn cancel(&self) {
        self.cancelled.send_replace(true);
        // Without a read in progress, stop the transfer right away.
        if let Ok(mut body) = self.body.try_lock() {
            body.cancel();
        }
    }
}

/// One WebSocket tunnelled over an iroh stream; same API as the WASM `WebSocketConn`.
#[napi]
pub struct NativeWebSocket(client_core::WebSocket);

#[napi(object)]
pub struct WsMessage {
    #[napi(js_name = "type")]
    pub kind: String,
    pub data: Option<Either<String, Uint8Array>>,
    pub code: Option<u32>,
    pub reason: Option<String>,
}

#[napi]
impl NativeWebSocket {
    /// Resolves to `{ type: "text", data }`, `{ type: "binary", data }`, `{ type: "close", code,
    /// reason }`, or `null` at the end.
    #[napi]
    pub fn recv<'e>(&self, env: &'e Env) -> NapiResult<PromiseRaw<'e, Option<WsMessage>>> {
        let socket = self.0.clone();
        env.spawn_future(async move {
            let frame = socket.recv().await.map_err(err)?;
            Ok(frame.map(|frame| match frame {
                WsFrame::Text(t) => WsMessage { kind: "text".into(), data: Some(Either::A(t)), code: None, reason: None },
                WsFrame::Binary(b) => WsMessage { kind: "binary".into(), data: Some(Either::B(Uint8Array::new(b))), code: None, reason: None },
                WsFrame::Close(code, reason) => WsMessage { kind: "close".into(), data: None, code: Some(code.into()), reason: Some(reason) },
            }))
        })
    }

    #[napi(js_name = "sendText")]
    pub fn send_text<'e>(&self, env: &'e Env, text: String) -> NapiResult<PromiseRaw<'e, ()>> {
        self.send_frame(env, WsFrame::Text(text), false)
    }

    #[napi(js_name = "sendBinary")]
    pub fn send_binary<'e>(&self, env: &'e Env, data: Uint8Array) -> NapiResult<PromiseRaw<'e, ()>> {
        self.send_frame(env, WsFrame::Binary(data.to_vec()), false)
    }

    /// Sends a close frame and finishes our side of the stream.
    #[napi]
    pub fn close<'e>(&self, env: &'e Env, code: u32, reason: String) -> NapiResult<PromiseRaw<'e, ()>> {
        let code = u16::try_from(code).map_err(|_| napi::Error::from_reason("close code out of range"))?;
        self.send_frame(env, WsFrame::Close(code, reason), true)
    }

    fn send_frame<'e>(&self, env: &'e Env, frame: WsFrame, finish: bool) -> NapiResult<PromiseRaw<'e, ()>> {
        let socket = self.0.clone();
        env.spawn_future(async move { socket.send(frame, finish).await.map_err(err) })
    }
}
