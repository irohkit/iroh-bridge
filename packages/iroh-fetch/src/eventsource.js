// A standard EventSource (Server-Sent Events) over iroh-fetch's streaming responses,
// with automatic reconnects and Last-Event-ID.

const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 2;

export class IrohEventSource extends EventTarget {
  static CONNECTING = CONNECTING;
  static OPEN = OPEN;
  static CLOSED = CLOSED;
  CONNECTING = CONNECTING;
  OPEN = OPEN;
  CLOSED = CLOSED;

  onopen = null;
  onmessage = null;
  onerror = null;
  readyState = CONNECTING;
  withCredentials;
  url;

  #fetch;
  #headers;
  #lastEventId = "";
  #retry = 3000;
  #abort = null;
  #timer = null;

  /**
   * @param {typeof fetch} fetchImpl the iroh fetch to use
   * @param {string} url
   * @param {{ withCredentials?: boolean, headers?: HeadersInit }} [init] `headers` is an extension:
   *   the browser EventSource can't send custom headers.
   */
  constructor(fetchImpl, url, init = {}) {
    super();
    this.#fetch = fetchImpl;
    this.url = url;
    this.withCredentials = Boolean(init.withCredentials);
    this.#headers = init.headers;
    this.#connect();
  }

  close() {
    this.readyState = CLOSED;
    clearTimeout(this.#timer);
    this.#abort?.abort();
  }

  async #connect() {
    if (this.readyState === CLOSED) return;
    this.#abort = new AbortController();
    const headers = new Headers(this.#headers);
    headers.set("accept", "text/event-stream");
    headers.set("cache-control", "no-cache");
    if (this.#lastEventId) headers.set("last-event-id", this.#lastEventId);
    let res;
    try {
      // Like browsers, never from (or into) an HTTP cache.
      res = await this.#fetch(this.url, { headers, cache: "no-store", signal: this.#abort.signal });
    } catch (err) {
      return this.#reconnect(err);
    }
    // Like browsers: a wrong status or content type is fatal, not retried.
    if (res.status !== 200 || !res.headers.get("content-type")?.startsWith("text/event-stream")) {
      res.body?.cancel();
      this.readyState = CLOSED;
      return this.#emit("error", { message: `EventSource got ${res.status} ${res.headers.get("content-type") ?? ""}` });
    }
    if (this.readyState === CLOSED) return res.body?.cancel();
    this.readyState = OPEN;
    this.#emit("open");
    try {
      await this.#read(res.body);
    } catch (err) {
      return this.#reconnect(err);
    }
    this.#reconnect();
  }

  async #read(body) {
    const reader = body.pipeThrough(new TextDecoderStream()).getReader();
    let buf = "";
    let data = [];
    let event = "";
    let first = true;
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buf += value;
      if (first && buf.charCodeAt(0) === 0xfeff) buf = buf.slice(1);
      first = false;
      const lines = buf.split(/\r\n|\r|\n/);
      buf = lines.pop();
      for (const line of lines) {
        if (line === "") {
          if (data.length) {
            this.#emit(event || "message", { data: data.join("\n"), lastEventId: this.#lastEventId, origin: originOf(this.url) });
          }
          data = [];
          event = "";
          continue;
        }
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let val = colon === -1 ? "" : line.slice(colon + 1);
        if (val.startsWith(" ")) val = val.slice(1);
        if (field === "data") data.push(val);
        else if (field === "event") event = val;
        else if (field === "id" && !val.includes("\0")) this.#lastEventId = val;
        else if (field === "retry" && /^\d+$/.test(val)) this.#retry = Number(val);
      }
    }
  }

  #reconnect(err) {
    if (this.readyState === CLOSED) return;
    this.readyState = CONNECTING;
    this.#emit("error", err ? { error: err, message: String(err?.message ?? err) } : undefined);
    if (this.readyState === CLOSED) return; // closed from onerror
    this.#timer = setTimeout(() => this.#connect(), this.#retry);
  }

  #emit(type, props) {
    let ev;
    if (type !== "open" && type !== "error" && typeof MessageEvent === "function") ev = new MessageEvent(type, props);
    else {
      ev = new Event(type);
      for (const [k, v] of Object.entries(props ?? {})) Object.defineProperty(ev, k, { value: v });
    }
    try {
      if (type === "open" || type === "message" || type === "error") this[`on${type}`]?.call(this, ev);
    } finally {
      this.dispatchEvent(ev);
    }
  }
}

function originOf(url) {
  try {
    return new URL(url).origin;
  } catch {
    return "";
  }
}
