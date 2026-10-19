export interface IrohFetchOptions {
  /** Endpoint id printed by `iroh-gateway` on startup. */
  endpointId: string;
  /** Relay URL, only needed when the gateway runs with a custom relay. */
  relayUrl?: string;
  /** Print iroh logs to the console: "error" | "warn" | "info" | "debug" | "trace". */
  logLevel?: string;
  /** Dial the gateway immediately (default true). If false, the first request dials. */
  connect?: boolean;
  /**
   * How long to wait for the first connection, in ms (default 15000). When it runs out,
   * `createIrohFetch` rejects with a `TimeoutError` instead of hanging. `0` waits forever.
   */
  connectTimeout?: number;
}

export interface IrohWebSocketOptions {
  /** Subprotocols, sent as `Sec-WebSocket-Protocol`. */
  protocols?: string | string[];
  /** Extra handshake headers, e.g. `Authorization` (browsers' WebSocket can't set these). */
  headers?: HeadersInit;
}

export interface IrohFetch {
  /**
   * Same shape as the global fetch; pass a path such as "/api/users?page=2".
   * The response body streams (works for SSE / NDJSON / large downloads). Request bodies stream too:
   * pass a `ReadableStream<Uint8Array>`, `File` or `Blob` to upload without buffering it.
   * `init.signal` aborts at any point, including while waiting for the response
   * (e.g. `AbortSignal.timeout(5000)`).
   */
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
  /**
   * Open a WebSocket to `path` on the backend. Returns an object with the standard WebSocket
   * API (`send`, `close`, `onmessage`, `addEventListener`, `readyState`, `binaryType`...).
   */
  websocket(path: string, options?: IrohWebSocketOptions): WebSocket;
  /** This client's own iroh endpoint id. */
  endpointId: string;
  close(): Promise<void>;
}

export function createIrohFetch(options: IrohFetchOptions): Promise<IrohFetch>;
