export interface BackendConfig {
  endpointId?: string;
  /**
   * Several gateways serving the same backend, for high availability. Requests go to a healthy one;
   * when one is unreachable the client fails over to the next and parks the bad one for
   * `failoverCooldown` ms.
   */
  endpointIds?: string[];
  /** "failover" (default): prefer them in order. "round-robin": spread requests across healthy ones. */
  strategy?: "failover" | "round-robin";
  /** Receives path-only requests such as fetch("/api/x") (default: the first backend). */
  default?: boolean;
}

export interface IrohFetchOptions {
  /** Endpoint id printed by `iroh-gateway` on startup. Use this or `backends`. */
  endpointId?: string;
  /** Several gateways for one backend (high availability); see `BackendConfig.endpointIds`. */
  endpointIds?: string[];
  /** How replicas are picked when `endpointIds` has several (default "failover"). */
  strategy?: "failover" | "round-robin";
  /** How long an unreachable replica is skipped before it is tried again, in ms (default 10000). */
  failoverCooldown?: number;
  /**
   * Several gateways for one frontend, keyed by the origin your code uses for each:
   * `{ "https://api.example.com": "<endpoint id>", "https://files.example.com": { endpointId } }`.
   * Requests are routed by URL host; all gateways share one local endpoint.
   */
  backends?: Record<string, string | string[] | BackendConfig>;
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
  /**
   * Keep cookies the backend sets and send them back, like a browser does for a normal site
   * (default true). Requests with `credentials: "omit"` skip it.
   */
  cookies?: boolean;
  /**
   * Keep cookies across page reloads: `true` uses localStorage, or pass any Storage.
   * Off by default: unlike real cookies, stored values are readable by page scripts (no HttpOnly).
   */
  persistCookies?: boolean | Storage;
}

export interface IrohWebSocketOptions {
  /** Subprotocols, sent as `Sec-WebSocket-Protocol`. */
  protocols?: string | string[];
  /** Extra handshake headers, e.g. `Authorization` (browsers' WebSocket can't set these). */
  headers?: HeadersInit;
}

export interface InstallOptions {
  /** Origins whose requests go through iroh, e.g. ["https://api.example.com"] (default: the `backends` origins). */
  origins?: string[];
  /** Custom matcher instead of `origins`. */
  match?: (url: URL) => boolean;
  /** Also route matching `new WebSocket(url)` (default true). */
  websocket?: boolean;
  /** Also route matching `new EventSource(url)` (default true). */
  eventSource?: boolean;
}

export interface IrohCookies {
  get(name: string): string | undefined;
  /** The `Cookie` header that would be sent for `path`. */
  header(path: string): string;
  clear(): void;
}

export interface IrohFetch {
  /**
   * Same as the global fetch. Pass a path ("/api/users?page=2") or a full URL; only the path and
   * query reach the backend. Bodies stream both ways (SSE, NDJSON, large downloads and uploads of
   * `File`, `Blob` or `ReadableStream`). Redirects are followed like fetch does, cookies are kept,
   * and `init.signal` aborts at any point (e.g. `AbortSignal.timeout(5000)`).
   */
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
  /** Open a WebSocket to `path` (or a ws:// / wss:// URL) with custom handshake headers. */
  websocket(path: string | URL, options?: IrohWebSocketOptions): WebSocket;
  /** `new iroh.WebSocket(url, protocols)`: the standard constructor, over iroh. */
  WebSocket: { new (url: string | URL, protocols?: string | string[]): WebSocket };
  /**
   * `new iroh.EventSource(url)`: standard Server-Sent Events with reconnects and Last-Event-ID.
   * Extension: `headers` in the init dict.
   */
  EventSource: { new (url: string | URL, init?: EventSourceInit & { headers?: HeadersInit }): EventSource };
  /**
   * Route the global fetch (and WebSocket / EventSource) for matching URLs through iroh, so existing
   * code and libraries work unchanged. Returns a function that undoes it.
   */
  install(options: InstallOptions): () => void;
  /** A view scoped to one backend (path-only URLs go there), e.g. `iroh.backend("https://files.example.com")`. */
  backend(origin: string): Pick<IrohFetch, "fetch" | "websocket" | "WebSocket" | "EventSource" | "cookies"> & { endpointIds: string[] };
  /** The configured backends, their replicas and whether each is currently healthy. */
  backends: ReadonlyArray<{
    origin: string | null;
    replicas: ReadonlyArray<{ endpointId: string; readonly healthy: boolean }>;
    readonly connected: boolean;
  }>;
  /** Cookies stored from the (first) backend's `Set-Cookie` headers. Each backend has its own jar. */
  cookies: IrohCookies;
  /** This client's own iroh endpoint id (the backend sees it as `X-Iroh-Remote-Id`). */
  endpointId: string;
  close(): Promise<void>;
}

export function createIrohFetch(options: IrohFetchOptions): Promise<IrohFetch>;
