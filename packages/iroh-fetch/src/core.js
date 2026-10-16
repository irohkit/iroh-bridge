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
    const { endpointId, relayUrl, logLevel, connect = true } = options ?? {};
    if (!endpointId) throw new Error("iroh-fetch: endpointId is required");
    await init();
    if (logLevel) enableLogging(logLevel);
    const client = await IrohClient.create(endpointId, relayUrl ?? undefined);
    if (connect) await client.connect();

    async function irohFetch(input, init) {
      // Let the platform's Request normalise bodies (string, FormData, Blob, URLSearchParams...)
      // and fill in headers such as content-type.
      const url = typeof input === "string" && input.startsWith("/") ? `http://iroh${input}` : input;
      const req = new Request(url, init);
      req.signal.throwIfAborted();
      const { pathname, search } = new URL(req.url);
      const body = req.body ? new Uint8Array(await req.arrayBuffer()) : undefined;
      const res = await client.request(req.method, pathname + search, [...req.headers], body);
      req.signal.throwIfAborted();
      // The body streams in as the backend sends it. Aborting the request cancels the stream,
      // which stops the transfer on the gateway too.
      let stream = res.body;
      if (NULL_BODY_STATUS.has(res.status)) {
        stream.cancel();
        stream = null;
      } else if (init?.signal) {
        stream = stream.pipeThrough(new TransformStream(), { signal: req.signal });
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
