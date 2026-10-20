// XMLHttpRequest over iroh-fetch, so libraries that use XHR (axios's browser default, jQuery,
// upload widgets with progress bars) work unchanged.

const UNSENT = 0;
const OPENED = 1;
const HEADERS_RECEIVED = 2;
const LOADING = 3;
const DONE = 4;

const XHR_EVENTS = ["readystatechange", "loadstart", "progress", "load", "error", "abort", "timeout", "loadend"];
const UPLOAD_EVENTS = ["loadstart", "progress", "load", "error", "abort", "timeout", "loadend"];
const NULL_BODY_METHODS = new Set(["GET", "HEAD"]);

function progressEvent(type, loaded = 0, total = 0) {
  const init = { lengthComputable: total > 0, loaded, total };
  if (typeof ProgressEvent === "function") return new ProgressEvent(type, init);
  const ev = new Event(type);
  for (const [k, v] of Object.entries(init)) Object.defineProperty(ev, k, { value: v });
  return ev;
}

/** An EventTarget that also calls `on<type>` handlers, like XHR and XHR.upload do. */
class HandlerTarget extends EventTarget {
  emit(ev) {
    try {
      this[`on${ev.type}`]?.call(this, ev);
    } finally {
      this.dispatchEvent(ev);
    }
  }
}

class Upload extends HandlerTarget {
  onloadstart = null;
  onprogress = null;
  onload = null;
  onerror = null;
  onabort = null;
  ontimeout = null;
  onloadend = null;
}

/** XMLHttpRequest that always goes over iroh. `fetchImpl` is the iroh fetch. */
export class IrohXMLHttpRequest extends HandlerTarget {
  static UNSENT = UNSENT;
  static OPENED = OPENED;
  static HEADERS_RECEIVED = HEADERS_RECEIVED;
  static LOADING = LOADING;
  static DONE = DONE;
  UNSENT = UNSENT;
  OPENED = OPENED;
  HEADERS_RECEIVED = HEADERS_RECEIVED;
  LOADING = LOADING;
  DONE = DONE;

  onreadystatechange = null;
  onloadstart = null;
  onprogress = null;
  onload = null;
  onerror = null;
  onabort = null;
  ontimeout = null;
  onloadend = null;

  readyState = UNSENT;
  status = 0;
  statusText = "";
  responseURL = "";
  responseType = "";
  timeout = 0;
  withCredentials = false;
  upload = new Upload();
  responseXML = null;

  #fetch;
  #method = "GET";
  #url = "";
  #headers = new Headers();
  #response = null;
  #text = "";
  #resHeaders = null;
  #ctrl = null;
  #sent = false;

  constructor(fetchImpl) {
    super();
    this.#fetch = fetchImpl;
  }

  get response() {
    if (this.responseType === "" || this.responseType === "text") return this.readyState >= LOADING ? this.#text : "";
    return this.readyState === DONE ? this.#response : null;
  }

  get responseText() {
    if (this.responseType !== "" && this.responseType !== "text") {
      throw new DOMException("responseText is only available for responseType '' or 'text'", "InvalidStateError");
    }
    return this.readyState >= LOADING ? this.#text : "";
  }

  open(method, url, async = true) {
    if (async === false) throw new DOMException("synchronous XMLHttpRequest is not supported over iroh", "InvalidAccessError");
    this.#ctrl?.abort();
    this.#method = String(method).toUpperCase();
    this.#url = String(url);
    this.#headers = new Headers();
    this.#sent = false;
    this.#reset();
    this.#setState(OPENED);
  }

  setRequestHeader(name, value) {
    if (this.readyState !== OPENED || this.#sent) throw new DOMException("setRequestHeader before send()", "InvalidStateError");
    this.#headers.append(name, value);
  }

  getResponseHeader(name) {
    return this.#resHeaders?.get(name) ?? null;
  }

  getAllResponseHeaders() {
    if (!this.#resHeaders) return "";
    return [...this.#resHeaders].map(([k, v]) => `${k}: ${v}`).join("\r\n") + "\r\n";
  }

  overrideMimeType() {}

  abort() {
    if (!this.#ctrl) return;
    this.#ctrl.abort(new DOMException("aborted", "AbortError"));
  }

  send(body = null) {
    if (this.readyState !== OPENED || this.#sent) throw new DOMException("send() needs open() first", "InvalidStateError");
    this.#sent = true;
    this.#run(NULL_BODY_METHODS.has(this.#method) ? null : body);
  }

  async #run(body) {
    const ctrl = (this.#ctrl = new AbortController());
    let timer;
    let timedOut = false;
    if (this.timeout > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        ctrl.abort(new DOMException("timeout", "TimeoutError"));
      }, this.timeout);
    }
    this.emit(progressEvent("loadstart"));
    try {
      const upload = await this.#uploadBody(body, ctrl.signal);
      const res = await this.#fetch(this.#url, {
        method: this.#method,
        headers: this.#headers,
        body: upload,
        signal: ctrl.signal,
        credentials: this.withCredentials ? "include" : "same-origin",
      });
      this.status = res.status;
      this.statusText = res.statusText;
      this.responseURL = res.url;
      this.#resHeaders = res.headers;
      this.#setState(HEADERS_RECEIVED);

      const total = Number(res.headers.get("content-length")) || 0;
      const chunks = [];
      let loaded = 0;
      const decoder = new TextDecoder();
      const textual = this.responseType === "" || this.responseType === "text";
      if (res.body) {
        for await (const chunk of res.body) {
          chunks.push(chunk);
          loaded += chunk.byteLength;
          if (textual) this.#text += decoder.decode(chunk, { stream: true });
          if (this.readyState !== LOADING) this.#setState(LOADING);
          else this.#emitState();
          this.emit(progressEvent("progress", loaded, total));
        }
      }
      if (textual) this.#text += decoder.decode();
      if (this.readyState !== LOADING) this.#setState(LOADING);
      this.#response = this.#decode(chunks, res.headers.get("content-type") ?? "");
      clearTimeout(timer);
      this.#setState(DONE);
      this.emit(progressEvent("load", loaded, total));
      this.emit(progressEvent("loadend", loaded, total));
    } catch (err) {
      clearTimeout(timer);
      const type = timedOut ? "timeout" : err?.name === "AbortError" ? "abort" : "error";
      this.#fail(type);
    } finally {
      if (this.#ctrl === ctrl) this.#ctrl = null;
    }
  }

  /** Turns the body into a stream that reports upload progress as iroh reads it. */
  async #uploadBody(body, signal) {
    if (body == null) return undefined;
    let stream;
    let total = 0;
    if (typeof Blob !== "undefined" && body instanceof Blob) {
      stream = body.stream();
      total = body.size;
      if (body.type && !this.#headers.has("content-type")) this.#headers.set("content-type", body.type);
    } else if (body instanceof ReadableStream) {
      stream = body;
    } else {
      // Strings, FormData, URLSearchParams, ArrayBuffer(View): let Request encode them and set the
      // content type (e.g. the multipart boundary), like a browser's XHR does.
      const req = new Request("http://iroh.invalid/", { method: "POST", body, headers: this.#headers });
      if (!this.#headers.has("content-type") && req.headers.get("content-type")) {
        this.#headers.set("content-type", req.headers.get("content-type"));
      }
      const bytes = new Uint8Array(await req.arrayBuffer());
      total = bytes.byteLength;
      stream = new Blob([bytes]).stream();
    }
    const upload = this.upload;
    let sent = 0;
    upload.emit(progressEvent("loadstart", 0, total));
    // Hand out 64 KiB slices on demand: iroh pulls the next one only once it has sent the previous,
    // so progress follows what actually went over the wire, even when the source is one big chunk.
    const reader = stream.getReader();
    let pending = null;
    let offset = 0;
    const SLICE = 64 * 1024;
    const onAbort = () => reader.cancel(signal.reason).catch(() => {});
    signal.addEventListener("abort", onAbort, { once: true });
    return new ReadableStream(
      {
        async pull(ctrl) {
          if (!pending || offset >= pending.byteLength) {
            const { value, done } = await reader.read();
            if (done) {
              signal.removeEventListener("abort", onAbort);
              upload.emit(progressEvent("load", sent, total || sent));
              upload.emit(progressEvent("loadend", sent, total || sent));
              return ctrl.close();
            }
            pending = value instanceof Uint8Array ? value : new Uint8Array(value);
            offset = 0;
          }
          const slice = pending.subarray(offset, offset + SLICE);
          offset += slice.byteLength;
          sent += slice.byteLength;
          ctrl.enqueue(slice);
          upload.emit(progressEvent("progress", sent, total));
        },
        cancel(reason) {
          return reader.cancel(reason);
        },
      },
      { highWaterMark: 0 },
    );
  }

  #decode(chunks, contentType) {
    const bytes = () => {
      const out = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
      let at = 0;
      for (const c of chunks) {
        out.set(c, at);
        at += c.byteLength;
      }
      return out;
    };
    switch (this.responseType) {
      case "arraybuffer":
        return bytes().buffer;
      case "blob":
        return new Blob([bytes()], { type: contentType });
      case "json":
        try {
          return JSON.parse(new TextDecoder().decode(bytes()));
        } catch {
          return null;
        }
      case "document":
        return null;
      default:
        return this.#text;
    }
  }

  #fail(type) {
    this.status = 0;
    this.statusText = "";
    this.#resHeaders = null;
    this.#response = null;
    this.#text = "";
    this.#setState(DONE);
    for (const t of [type, "loadend"]) {
      if (this.#uploadPending()) this.upload.emit(progressEvent(t));
    }
    this.emit(progressEvent(type));
    this.emit(progressEvent("loadend"));
    // An aborted request goes back to UNSENT without another readystatechange, as in browsers.
    if (type === "abort") this.readyState = UNSENT;
  }

  #uploadPending() {
    return UPLOAD_EVENTS.some((t) => this.upload[`on${t}`]);
  }

  #reset() {
    this.status = 0;
    this.statusText = "";
    this.responseURL = "";
    this.#response = null;
    this.#text = "";
    this.#resHeaders = null;
  }

  #setState(state) {
    this.readyState = state;
    this.#emitState();
  }

  #emitState() {
    this.emit(new Event("readystatechange"));
  }
}

/**
 * The XMLHttpRequest that install() puts on the global: the URL is only known at open(), so it
 * picks iroh or the native XHR then, and forwards events and properties to whichever it chose.
 */
export function routedXMLHttpRequest(NativeXHR, makeIroh, matches) {
  const props = ["responseType", "timeout", "withCredentials"];
  const readOnly = ["readyState", "status", "statusText", "response", "responseText", "responseURL", "responseXML"];

  class XMLHttpRequest extends HandlerTarget {
    static UNSENT = UNSENT;
    static OPENED = OPENED;
    static HEADERS_RECEIVED = HEADERS_RECEIVED;
    static LOADING = LOADING;
    static DONE = DONE;
    UNSENT = UNSENT;
    OPENED = OPENED;
    HEADERS_RECEIVED = HEADERS_RECEIVED;
    LOADING = LOADING;
    DONE = DONE;
    upload = new Upload();

    #target = null;
    #pending = { responseType: "", timeout: 0, withCredentials: false };

    open(method, url, ...rest) {
      const target = matches(url) ? makeIroh() : NativeXHR ? new NativeXHR() : null;
      if (!target) throw new TypeError(`XMLHttpRequest is not available for ${url}`);
      this.#target = target;
      for (const type of XHR_EVENTS) target.addEventListener(type, (e) => this.emit(clone(e)));
      for (const type of UPLOAD_EVENTS) target.upload.addEventListener(type, (e) => this.upload.emit(clone(e)));
      target.open(method, url, ...rest);
      for (const p of props) target[p] = this.#pending[p];
    }

    setRequestHeader(...args) {
      return this.#need().setRequestHeader(...args);
    }
    send(body) {
      return this.#need().send(body);
    }
    abort() {
      return this.#target?.abort();
    }
    getResponseHeader(name) {
      return this.#target?.getResponseHeader(name) ?? null;
    }
    getAllResponseHeaders() {
      return this.#target?.getAllResponseHeaders() ?? "";
    }
    overrideMimeType(mime) {
      return this.#target?.overrideMimeType?.(mime);
    }

    #need() {
      if (!this.#target) throw new DOMException("call open() first", "InvalidStateError");
      return this.#target;
    }

    static {
      for (const p of props) {
        Object.defineProperty(this.prototype, p, {
          get() {
            return this.#target ? this.#target[p] : this.#pending[p];
          },
          set(v) {
            this.#pending[p] = v;
            if (this.#target) this.#target[p] = v;
          },
        });
      }
      for (const p of readOnly) {
        Object.defineProperty(this.prototype, p, {
          get() {
            if (this.#target) return this.#target[p];
            return p === "readyState" || p === "status" ? 0 : p === "response" || p === "responseXML" ? null : "";
          },
        });
      }
      for (const type of XHR_EVENTS) this.prototype[`on${type}`] = null;
    }
  }
  return XMLHttpRequest;
}

function clone(e) {
  return e.type === "readystatechange" || !("loaded" in e) ? new Event(e.type) : progressEvent(e.type, e.loaded, e.total);
}
