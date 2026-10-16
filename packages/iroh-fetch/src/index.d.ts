export interface IrohFetchOptions {
  /** Endpoint id printed by `iroh-gateway` on startup. */
  endpointId: string;
  /** Relay URL, only needed when the gateway runs with a custom relay. */
  relayUrl?: string;
  /** Print iroh logs to the console: "error" | "warn" | "info" | "debug" | "trace". */
  logLevel?: string;
  /** Dial the gateway immediately (default true). If false, the first request dials. */
  connect?: boolean;
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
   * The response body streams (works for SSE / NDJSON / large downloads); `init.signal` aborts it.
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
