import { IrohClient, enableLogging } from "../wasm/web_client.js";
import { IrohWebSocket } from "./websocket.js";

// Responses with these statuses must not carry a body in the Fetch API.
const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304]);

/**
 * @param {() => Promise<void>} init loads the WASM module for the current runtime
 * @returns {import("./index.d.ts").createIrohFetch}
 */
export function makeCreate(init) {
  return async function createIrohFetch(options) {
    const { endpointId, relayUrl, logLevel, connect = true, connectTimeout = 15_000 } = options ?? {};
    if (!endpointId) throw new Error("iroh-fetch: endpointId is required");
    await init();
    if (logLevel) enableLogging(logLevel);
    const client = await IrohClient.create(endpointId, relayUrl ?? undefined);
    if (connect) {
      try {
        await withTimeout(client.connect(), connectTimeout, () =>
          new DOMException(
            `iroh-fetch: could not reach gateway ${endpointId.slice(0, 10)}… within ${connectTimeout} ms ` +
              "(is iroh-gateway running, and is the endpoint id right?)",
            "TimeoutError",
          ),
        );
      } catch (err) {
        client.close().catch(() => {});
        if (err?.name === "TimeoutError") throw err;
        throw new Error(
          `iroh-fetch: could not connect to gateway ${endpointId.slice(0, 10)}… ` +
            `(is iroh-gateway running, and is the endpoint id right?): ${err?.message ?? err}`,
          { cause: err },
        );
      }
    }

    async function irohFetch(input, init) {
      const url = typeof input === "string" && input.startsWith("/") ? `http://iroh${input}` : input;
      // A ReadableStream body is uploaded as-is. Everything else goes through the platform's
      // Request, which normalises strings, FormData, Blob, URLSearchParams... into a stream and
      // fills in headers such as content-type.
      const streamBody = init?.body instanceof ReadableStream ? init.body : null;
      const req = new Request(url, streamBody ? { ...init, body: undefined } : init);
      const { signal } = req;
      const hasSignal = Boolean(init?.signal || (input instanceof Request && input.signal));
      signal.throwIfAborted();
      const { pathname, search } = new URL(req.url);

      let body = streamBody ?? req.body ?? undefined;
      // Aborting errors the upload, which resets the stream so the gateway drops the request.
      if (body && hasSignal) body = body.pipeThrough(new TransformStream(), { signal });

      const pending = client.request(req.method, pathname + search, [...req.headers], body);
      const res = hasSignal ? await raceAbort(pending, signal, (late) => late.body.cancel()) : await pending;

      // The body streams in as the backend sends it. Aborting the request cancels the stream,
      // which stops the transfer on the gateway too.
      let stream = res.body;
      if (NULL_BODY_STATUS.has(res.status)) {
        stream.cancel();
        stream = null;
      } else if (hasSignal) {
        stream = stream.pipeThrough(new TransformStream(), { signal });
      }
      return new Response(stream, { status: res.status, headers: res.headers });
    }

    return {
      fetch: irohFetch,
      websocket: (path, options) => new IrohWebSocket(client, path, options),
      endpointId: client.endpointId(),
      close: () => client.close(),
    };
  };
}

function withTimeout(promise, ms, makeError) {
  if (!ms || ms === Infinity) return promise;
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(makeError()), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/** Rejects as soon as `signal` aborts; a result that arrives later is handed to `onLate`. */
function raceAbort(promise, signal, onLate) {
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    if (signal.aborted) onAbort();
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        if (signal.aborted) onLate(value);
        else resolve(value);
      },
      (err) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      },
    );
  });
}
