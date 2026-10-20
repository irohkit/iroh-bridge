// Node.js only: finds the native addon (crates/node-client) for this platform and wraps it in the
// interface of the WASM IrohClient, so core.js works with either. The addon has real UDP sockets,
// so connections hole-punch a direct path to the gateway and use a relay only until then (or when
// that fails). The WASM build always goes through a relay.
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);

/** `linux-x64-gnu`, `darwin-arm64`, `win32-x64-msvc`, ...: the suffix of the prebuild packages. */
export function platformTag() {
  const { platform, arch } = process;
  if (platform === "linux") {
    let glibc = false;
    try {
      glibc = Boolean(process.report?.getReport?.().header?.glibcVersionRuntime);
    } catch {}
    return `linux-${arch}-${glibc ? "gnu" : "musl"}`;
  }
  if (platform === "win32") return `win32-${arch}-msvc`;
  return `${platform}-${arch}`;
}

let loaded;

/**
 * Loads the addon: a local build in `native/` (from `npm run build:native`), else the
 * `iroh-fetch-<platform>` prebuild package. Returns `{ IrohClient, enableLogging }` or throws.
 */
export function loadNative() {
  if (loaded) {
    if (loaded.error) throw loaded.error;
    return loaded.backend;
  }
  const tag = platformTag();
  const local = new URL(`../native/iroh-fetch.${tag}.node`, import.meta.url);
  const tried = [];
  for (const id of [existsSync(local) ? fileURLToPath(local) : null, `iroh-fetch-${tag}`]) {
    if (!id) continue;
    try {
      const backend = wrap(require(id));
      loaded = { backend };
      return backend;
    } catch (err) {
      tried.push(`${id}: ${err?.message ?? err}`);
    }
  }
  loaded = { error: new Error(`iroh-fetch: no native addon for ${tag} (${tried.join("; ")})`) };
  throw loaded.error;
}

/** Wraps the addon's chunk-based classes in the WASM IrohClient's stream-based API. */
function wrap(addon) {
  class IrohClient {
    #c;
    constructor(native) {
      this.#c = native;
    }
    static async create(relayUrl, relayToken, secretKey) {
      return new IrohClient(await addon.createClient(relayUrl, relayToken, secretKey));
    }
    relayProblem() {
      return this.#c.relayProblem() ?? undefined;
    }
    secretKey() {
      return this.#c.secretKey();
    }
    endpointId() {
      return this.#c.endpointId();
    }
    /** The network paths to `remote`: `{ kind: "direct" | "relay" | "none", paths }` or null. */
    connectionInfo(remote) {
      return this.#c.connectionInfo(remote) ?? null;
    }
    connect(remote, relay) {
      return this.#c.connect(remote, relay ?? undefined);
    }
    async request(remote, method, path, headers, body, relay) {
      let bytes;
      let upload;
      if (body instanceof ReadableStream) {
        upload = new addon.NativeUpload();
      } else if (body instanceof Uint8Array) {
        bytes = body;
      } else if (body != null) {
        throw new TypeError("body must be a Uint8Array or a ReadableStream<Uint8Array>");
      }
      let pending;
      try {
        pending = this.#c.request(remote, method, path, headers ?? [], bytes, upload, relay ?? undefined);
      } catch (err) {
        upload?.abort(String(err?.message ?? err));
        throw err;
      }
      // Upload while waiting for the response, so the backend may answer before the body is done.
      if (upload) pump(body, upload);
      const res = await pending;
      return { status: res.status, headers: res.headers, body: bodyStream(res) };
    }
    async websocket(remote, path, headers, relay) {
      const res = await this.#c.websocket(remote, path, headers ?? [], relay ?? undefined);
      const out = { status: res.status, headers: res.headers };
      const socket = res.takeSocket();
      if (socket) out.socket = socket;
      else out.body = bodyStream(res);
      return out;
    }
    close() {
      return this.#c.close();
    }
  }
  return { IrohClient, enableLogging: (level) => addon.enableLogging(String(level)), native: true };
}

async function pump(stream, upload) {
  let reader;
  try {
    reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) throw new TypeError("body stream chunks must be Uint8Array");
      await upload.write(value);
    }
    upload.finish();
  } catch (err) {
    // Resets the QUIC stream, so the gateway doesn't forward a truncated body as complete.
    upload.abort(String(err?.message ?? err));
    reader?.cancel(err).catch(() => {});
  }
}

/** The response body as a ReadableStream; cancelling it stops the transfer on the gateway too. */
function bodyStream(res) {
  return new ReadableStream({
    async pull(controller) {
      const chunk = await res.read();
      if (chunk == null) controller.close();
      else controller.enqueue(chunk);
    },
    cancel() {
      res.cancel();
    },
  });
}
