/**
 * One gateway: its endpoint id, or `{ endpointId, relayUrl }` when you know which relay it is on
 * (its home relay, e.g. "https://aps1-1.relay.n0.iroh.link" for a gateway on n0's public relays).
 * The client then dials it via exactly that relay instead of via the relays in `relayUrl`, which
 * makes the first connection fast for a gateway on another relay than yours. The hint is a speed-up,
 * not a requirement: if the gateway moved to another relay, the dial still falls back to an address
 * lookup (a few seconds slower). A token-protected relay must also be listed in `relayUrl`.
 */
export type Replica = string | { endpointId: string; relayUrl?: string };

export interface BackendConfig {
  endpointId?: string;
  /** The relay the `endpointId` gateway is on; see {@link Replica}. */
  relayUrl?: string;
  /**
   * Several gateways serving the same backend, for high availability. Requests go to a healthy one;
   * when one is unreachable the client fails over to the next and parks the bad one for
   * `failoverCooldown` ms. Each may name its relay: `[{ endpointId, relayUrl }, ...]`.
   */
  endpointIds?: Replica[];
  /** "failover" (default): prefer them in order. "round-robin": spread requests across healthy ones. */
  strategy?: "failover" | "round-robin";
  /** Receives path-only requests such as fetch("/api/x") (default: the first backend). */
  default?: boolean;
}

export interface CacheOptions {
  /** Most responses kept (default 500); the least recently used go first. */
  maxEntries?: number;
  /** Most bytes kept, bodies plus headers (default 50 MB). Larger responses are not stored. */
  maxBytes?: number;
  /**
   * "memory" (default): gone on reload. "cache-api": the browser's Cache API, kept across reloads
   * (falls back to memory where it's missing, e.g. Node or plain-http pages).
   */
  storage?: "memory" | "cache-api";
}

export interface IrohFetchOptions {
  /** Endpoint id printed by `iroh-gateway` on startup. Use this or `backends`. */
  endpointId?: string;
  /** Several gateways for one backend (high availability); see `BackendConfig.endpointIds`. */
  endpointIds?: Replica[];
  /** How replicas are picked when `endpointIds` has several (default "failover"). */
  strategy?: "failover" | "round-robin";
  /**
   * A fixed identity (64 hex chars, e.g. from exportSecretKey()), so gateways can allow-list or
   * rate-limit this client by endpoint id. Mainly for servers and CLIs; keep it secret.
   */
  secretKey?: string;
  /**
   * Keep the same endpoint id across page loads: `true` stores the secret key in localStorage, or pass
   * any Storage. Off by default: page scripts can read it, so an XSS bug could impersonate the device.
   */
  persistIdentity?: boolean | Storage;
  /** How long an unreachable replica is skipped before it is tried again, in ms (default 10000). */
  failoverCooldown?: number;
  /**
   * Several gateways for one frontend, keyed by the origin your code uses for each:
   * `{ "https://api.example.com": "<endpoint id>", "https://files.example.com": { endpointId } }`.
   * Requests are routed by URL host; all gateways share one local endpoint.
   */
  backends?: Record<string, string | Replica[] | BackendConfig>;
  /**
   * Your own relay (recommended for production), or several: `["https://relay.example.com", "n0"]`
   * uses yours and falls back to n0's public relays if it's down ("n0" never gets your token).
   * Several own relays (`["https://relay-a.example.com", "https://relay-b.example.com", "n0"]`) all
   * get the same `relayToken`. Gateways without a {@link Replica} relay are dialed via all relays
   * listed here (not n0's), plus an address lookup. Default: n0's public relays.
   */
  relayUrl?: string | string[];
  /**
   * Token for a relay that only admits clients with a token (iroh-relay `access.shared_token`).
   * Prefer a function that gets it from your backend at runtime (e.g. only for logged-in users):
   * `relayToken: () => fetch("/api/relay-token").then((r) => r.text())`. It is called before the
   * client is created, again whenever the relay refuses the client (then the dial is retried once),
   * on `refreshRelayToken()` and every `relayTokenRefreshMs`. A token that differs from the current
   * one replaces the underlying client: same endpoint id, cookies and cache, but open WebSockets,
   * streams and in-flight requests are cut. A token string in a web page is visible to anyone, so it
   * limits who uses your relay's bandwidth, not who reaches your backend; authenticate users in the
   * backend for that.
   */
  relayToken?: string | (() => string | Promise<string>);
  /**
   * With a `relayToken` function: call it every this many ms (default 0: only when the relay
   * refuses the client), so a rotated token is picked up before a reconnect to the relay needs it.
   */
  relayTokenRefreshMs?: number;
  /** Print iroh logs to the console: "error" | "warn" | "info" | "debug" | "trace". */
  logLevel?: string;
  /**
   * Node.js only: "native" uses the native addon (direct UDP paths via hole punching, relay as
   * fallback), "wasm" the WASM build (always through a relay). Default "auto": native when a
   * prebuild for the platform is installed, else WASM. Overrides the `IROH_FETCH_BACKEND` env var.
   */
  engine?: "auto" | "native" | "wasm";
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
  /**
   * Cache GET / HEAD responses on the client like a browser's HTTP cache (off by default). Requests
   * over iroh skip the browser's cache, so otherwise every GET crosses the relay again. Honours
   * Cache-Control (max-age, no-store, no-cache, public, private), Expires, ETag / Last-Modified
   * revalidation, Vary and `RequestInit.cache`. Requests with Authorization or cookies are only
   * cached when the response says `public`.
   */
  cache?: boolean | CacheOptions;
}

export interface IrohWebSocketOptions {
  /** Subprotocols, sent as `Sec-WebSocket-Protocol`. */
  protocols?: string | string[];
  /** Extra handshake headers, e.g. `Authorization` (browsers' WebSocket can't set these). */
  headers?: HeadersInit;
}

export interface InstallOptions {
  /**
   * Origins whose requests go through iroh, e.g. ["https://api.example.com"] (default: the `backends` origins).
   * Must be absolute URLs; only http(s) and ws(s) URLs on these hosts are routed.
   */
  origins?: string[];
  /** Custom matcher instead of `origins`. */
  match?: (url: URL) => boolean;
  /** Also route matching `new WebSocket(url)` (default true). */
  websocket?: boolean;
  /** Also route matching `new EventSource(url)` (default true). */
  eventSource?: boolean;
  /** Also route matching `XMLHttpRequest` (axios's browser default, jQuery...) (default true). */
  xhr?: boolean;
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
  /** `new iroh.XMLHttpRequest()`: the standard XHR (incl. upload/download progress), over iroh. */
  XMLHttpRequest: { new (): XMLHttpRequest };
  /**
   * Register the service worker so the browser's own loads of backend URLs (<img>, <video>, CSS,
   * fonts) go over iroh. Serve `node_modules/iroh-fetch/src/sw.js` from your origin at `url`
   * (default "/iroh-sw.js"). Resolves once the worker controls the page.
   */
  serviceWorker(options?: { url?: string; scope?: string; hosts?: string[] }): Promise<{
    registration: ServiceWorkerRegistration;
    /** False if the worker didn't take control within 5 s (it may still later). */
    controlled: boolean;
    unregister(): Promise<void>;
  }>;
  /**
   * A same-origin URL that the service worker routes to `url` on a backend, for links the browser
   * opens itself (downloads, "open in new tab"), which a service worker can't catch cross-origin.
   */
  proxyUrl(url: string | URL): string;
  /**
   * Fetch `url` over iroh and return a `blob:` URL of the body, for `<img>` / `<video>` where no
   * service worker is available (iOS in-app browsers such as LINE, some private modes). Rejects on
   * non-2xx responses. Call `URL.revokeObjectURL()` when done; in React use `useIrohObjectUrl` or
   * `<IrohImage>`, which do that for you.
   */
  objectUrl(url: string | URL, init?: RequestInit): Promise<string>;
  /** This client's secret key (hex): pass it as `secretKey` later to keep the same endpoint id. */
  exportSecretKey(): string;
  /** Empties the HTTP cache (the `cache` option), e.g. on logout. */
  clearCache(): Promise<void>;
  /**
   * Calls the `relayToken` function now. If the token changed, closes the underlying client and
   * creates a new one with it (same endpoint id; open WebSockets and streams are cut) and resolves
   * true; otherwise (same token, a string `relayToken`, or closed) resolves false.
   */
  refreshRelayToken(): Promise<boolean>;
  /**
   * Route the global fetch (and WebSocket / EventSource / XMLHttpRequest) for matching URLs through iroh, so existing
   * code and libraries work unchanged. Returns a function that undoes it.
   */
  install(options: InstallOptions): () => void;
  /** A view scoped to one backend (path-only URLs go there), e.g. `iroh.backend("https://files.example.com")`. */
  backend(origin: string): Pick<IrohFetch, "fetch" | "websocket" | "WebSocket" | "EventSource" | "XMLHttpRequest" | "cookies"> & { endpointIds: string[] };
  /** The configured backends, their replicas and whether each is currently healthy. */
  backends: ReadonlyArray<{
    origin: string | null;
    /** `connected`: has a live connection; `healthy`: not parked after a failure. */
    replicas: ReadonlyArray<{ endpointId: string; relayUrl?: string; readonly healthy: boolean; readonly connected: boolean }>;
    readonly connected: boolean;
  }>;
  /** Cookies stored from the (first) backend's `Set-Cookie` headers. Each backend has its own jar. */
  cookies: IrohCookies;
  /** This client's own iroh endpoint id (the backend sees it as `X-Iroh-Remote-Id`). */
  endpointId: string;
  /** Node.js only: the client implementation in use (see the `engine` option). */
  engine?: "native" | "wasm";
  /**
   * Node.js only: how this client reaches a gateway (default: the first configured one) right now,
   * or null without a connection. `kind` is the selected path's: "direct" once hole punching
   * succeeded, else "relay".
   */
  connectionInfo?(endpointId?: string): ConnectionInfo | null;
  close(): Promise<void>;
}

export interface ConnectionInfo {
  kind: "direct" | "relay" | "none";
  paths: Array<{ kind: "direct" | "relay" | "custom"; addr: string; selected: boolean; rttMs: number }>;
}

export function createIrohFetch(options: IrohFetchOptions): Promise<IrohFetch>;
