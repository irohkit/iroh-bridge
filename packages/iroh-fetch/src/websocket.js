// A WebSocket-compatible object whose traffic is tunnelled over iroh to the gateway,
// which opens the real WebSocket to the backend.

const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;

function validCloseCode(code) {
  return Number.isInteger(code) && ((code >= 1000 && code <= 1003) || (code >= 1007 && code <= 1014) || (code >= 3000 && code <= 4999));
}

function truncateUtf8(text, max) {
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= max) return text;
  // Decoding a cut-off sequence would add U+FFFD; drop the partial character instead.
  return new TextDecoder().decode(bytes.subarray(0, max)).replace(/\uFFFD$/, "");
}

function makeEvent(type, props) {
  const Ctor =
    type === "message" && typeof MessageEvent === "function"
      ? MessageEvent
      : type === "close" && typeof CloseEvent === "function"
        ? CloseEvent
        : null;
  if (Ctor) return new Ctor(type, props);
  const ev = new Event(type);
  for (const [k, v] of Object.entries(props ?? {})) Object.defineProperty(ev, k, { value: v });
  return ev;
}

export class IrohWebSocket extends EventTarget {
  static CONNECTING = CONNECTING;
  static OPEN = OPEN;
  static CLOSING = CLOSING;
  static CLOSED = CLOSED;
  CONNECTING = CONNECTING;
  OPEN = OPEN;
  CLOSING = CLOSING;
  CLOSED = CLOSED;

  onopen = null;
  onmessage = null;
  onerror = null;
  onclose = null;

  readyState = CONNECTING;
  protocol = "";
  extensions = "";
  bufferedAmount = 0;
  /** "blob" (default, like browsers) or "arraybuffer". */
  binaryType = "blob";

  #conn = null;
  #sendQueue = Promise.resolve();
  #closeEmitted = false;

  constructor(client, path, { protocols, headers, url } = {}) {
    super();
    this.url = url ?? path;
    const list = protocols == null ? [] : Array.isArray(protocols) ? protocols : [protocols];
    const pairs = [...new Headers(headers ?? {})];
    if (list.length) pairs.push(["sec-websocket-protocol", list.join(", ")]);
    this.#start(client, path, pairs);
  }

  async #start(client, path, headers) {
    let res;
    try {
      res = await client.websocket(path, headers);
    } catch (err) {
      return this.#fail(err);
    }
    if (!res.socket) {
      const detail = await new Response(res.body).text().catch(() => "");
      return this.#fail(new Error(`backend refused WebSocket: ${res.status} ${detail}`.trim()));
    }
    this.#conn = res.socket;
    this.protocol = new Headers(res.headers).get("sec-websocket-protocol") ?? "";
    if (this.readyState === CONNECTING) {
      this.readyState = OPEN;
      this.#emit("open");
    } else {
      // close() was called while connecting.
      this.#conn.close(1000, "").catch(() => {});
    }
    this.#pump();
  }

  async #pump() {
    try {
      for (;;) {
        const msg = await this.#conn.recv();
        if (msg === null) return this.#finish(1006, "", false);
        if (msg.type === "close") {
          // Echo the close frame back if the backend started the closing handshake.
          if (this.readyState === OPEN) {
            this.readyState = CLOSING;
            await this.#conn.close(msg.code === 1005 ? 1000 : msg.code, "").catch(() => {});
          }
          return this.#finish(msg.code, msg.reason, msg.code !== 1006);
        }
        if (this.readyState !== OPEN) continue;
        let data = msg.data;
        if (msg.type === "binary") {
          data = this.binaryType === "arraybuffer" ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : new Blob([data]);
        }
        this.#emit("message", { data });
      }
    } catch (err) {
      this.#fail(err);
    }
  }

  send(data) {
    if (this.readyState === CONNECTING) throw new DOMException("WebSocket is still connecting", "InvalidStateError");
    if (this.readyState !== OPEN) return;
    const size = typeof data === "string" ? new TextEncoder().encode(data).byteLength : (data.byteLength ?? data.size ?? 0);
    this.bufferedAmount += size;
    // Queue sends so messages keep their order even when a Blob needs async reading.
    this.#sendQueue = this.#sendQueue
      .then(async () => {
        if (typeof data === "string") return this.#conn.sendText(data);
        if (data instanceof Blob) return this.#conn.sendBinary(new Uint8Array(await data.arrayBuffer()));
        if (ArrayBuffer.isView(data)) return this.#conn.sendBinary(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        return this.#conn.sendBinary(new Uint8Array(data));
      })
      .then(
        () => (this.bufferedAmount -= size),
        (err) => this.#fail(err),
      );
  }

  close(code = 1000, reason = "") {
    if (this.readyState === CLOSING || this.readyState === CLOSED) return;
    // Only codes an endpoint may put on the wire, and a reason that fits a control frame
    // (RFC 6455 5.5, 7.4), so the gateway never has to forward an invalid close frame.
    if (!validCloseCode(code)) code = 1000;
    reason = truncateUtf8(String(reason), 123);
    const wasConnecting = this.readyState === CONNECTING;
    this.readyState = CLOSING;
    if (wasConnecting) return; // #start finishes the close once connected
    this.#sendQueue = this.#sendQueue.then(() => this.#conn.close(code, reason)).catch((err) => this.#fail(err));
  }

  #fail(err) {
    if (this.#closeEmitted) return;
    this.#emit("error", { error: err, message: String(err?.message ?? err) });
    this.#finish(1006, "", false);
  }

  #finish(code, reason, wasClean) {
    if (this.#closeEmitted) return;
    this.#closeEmitted = true;
    this.readyState = CLOSED;
    this.#emit("close", { code, reason, wasClean });
  }

  #emit(type, props) {
    const ev = makeEvent(type, props);
    try {
      this[`on${type}`]?.call(this, ev);
    } finally {
      this.dispatchEvent(ev);
    }
  }
}
