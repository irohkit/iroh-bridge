import { IrohClient, enableLogging } from "../wasm/web_client.js";
import { createHttpCache } from "./cache.js";
import { CookieJar } from "./cookies.js";
import { IrohEventSource } from "./eventsource.js";
import { IrohWebSocket } from "./websocket.js";
import { IrohXMLHttpRequest, routedXMLHttpRequest } from "./xhr.js";

// Responses with these statuses must not carry a body in the Fetch API.
const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304]);
const REDIRECT_STATUS = new Set([301, 302, 303, 307, 308]);
const MAX_REDIRECTS = 20;
const REQUEST_HAS_BODY_STREAM = typeof Request !== "undefined" && "body" in Request.prototype;
// Used as the origin for path-only requests such as fetch("/api/users").
const DEFAULT_ORIGIN = "http://iroh.invalid";
const DEFAULT_HOST = "iroh.invalid";
// URL schemes install() may route; anything else (data:, blob:, file:, ...) is never sent to a gateway.
const ROUTABLE_PROTOCOLS = new Set(["http:", "https:", "ws:", "wss:"]);

/**
 * @param {() => Promise<void>} init loads the WASM module for the current runtime
 * @returns {import("./index.d.ts").createIrohFetch}
 */
export function makeCreate(init) {
  return async function createIrohFetch(options) {
    const {
      relayUrl,
      relayToken,
      secretKey,
      persistIdentity = false,
      logLevel,
      connect = true,
      connectTimeout = 15_000,
      failoverCooldown: cooldown = 10_000,
      cookies = true,
      persistCookies = false,
    } = options ?? {};
    const single = !options?.backends;
    const backends = parseBackends(options);
    const httpCache = createHttpCache(options?.cache);
    await init();
    if (logLevel) enableLogging(logLevel);
    // One local endpoint serves every gateway; each gateway gets its own connection.
    if (relayToken && !relayUrl) throw new Error("iroh-fetch: relayToken needs relayUrl");
    // A fixed identity lets gateways allow-list or rate-limit this client by endpoint id.
    const idStore = persistIdentity ? (persistIdentity === true ? globalThis.localStorage : persistIdentity) : undefined;
    const idKey = "iroh-fetch:secret-key";
    let storedKey;
    try {
      storedKey = idStore?.getItem(idKey) ?? undefined;
    } catch {}
    // Several relays (e.g. your own plus "n0" as a fallback) arrive as one comma-separated list.
    const relays = Array.isArray(relayUrl) ? relayUrl.join(",") : relayUrl;
    const client = await IrohClient.create(relays || undefined, relayToken ?? undefined, secretKey ?? storedKey ?? undefined);
    if (idStore && !storedKey && !secretKey) {
      try {
        idStore.setItem(idKey, client.secretKey());
      } catch {}
    }

    const storage = persistCookies ? (persistCookies === true ? globalThis.localStorage : persistCookies) : undefined;
    let closed = false;
    const timers = new Set();
    for (const b of backends) {
      // One jar per backend, shared by its replicas, so a session survives a failover.
      b.jar = cookies ? new CookieJar(storage && { storage, key: `iroh-fetch:cookies:${b.origin ?? b.replicas[0].endpointId}` }) : null;
      b.cooldown = cooldown;
      b.cacheKey = b.origin ?? b.replicas[0].endpointId;
      for (const r of b.replicas) {
        let pending = null;
        let retryDelay = 0;
        let retryTimer = null;
        r.reset = () => (pending = null);
        // Dials once and shares the result; a failure is forgotten so a later request retries.
        r.ensureConnected = () =>
          (pending ??= connectWithTimeout(client, r, connectTimeout).then(
            () => {
              retryDelay = 0;
            },
            (err) => {
              pending = null;
              r.downUntil = Date.now() + cooldown;
              // Keep retrying in the background, so a standby that missed its first dial is warm
              // before it's needed instead of waiting for a request to wake it up.
              // Back off (up to 5 min) so a replica that's gone for good costs little.
              if (!closed && !retryTimer && b.replicas.length > 1) {
                retryDelay = Math.min(retryDelay ? retryDelay * 2 : cooldown, 300_000);
                retryTimer = setTimeout(() => {
                  timers.delete(retryTimer);
                  retryTimer = null;
                  if (!closed && !r.connected) r.ensureConnected().catch(() => {});
                }, retryDelay);
                retryTimer.unref?.();
                timers.add(retryTimer);
              }
              throw err;
            },
          ));
      }
    }

    if (connect) {
      // A backend is usable as soon as one of its replicas connects.
      const results = await Promise.allSettled(backends.map((b) => Promise.any(b.replicas.map((r) => r.ensureConnected()))));
      // With one backend, being unreachable is fatal. With several, the others keep working and
      // requests to the unreachable one fail (and retry) on their own.
      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length === backends.length) {
        closed = true;
        for (const t of timers) clearTimeout(t);
        client.close().catch(() => {});
        const reason = failed[0].reason;
        throw reason instanceof AggregateError ? reason.errors[0] : reason;
      }
    }

    const byHost = new Map(backends.filter((b) => b.host).map((b) => [b.host, b]));
    const fallback = backends.find((b) => b.default) ?? backends[0];
    /** Picks the gateway for a URL by its host; path-only URLs go to the default one. */
    const route = (url, forced) => {
      if (forced) return forced;
      if (single) return backends[0];
      const b = byHost.get(url.host) ?? (url.host === DEFAULT_HOST ? fallback : null);
      if (!b) throw new TypeError(`iroh-fetch: no backend configured for ${url.origin}`);
      return b;
    };

    const api = (forced) => {
      const irohFetch = (input, init) => fetchOverIroh(client, route, forced, input, init, httpCache);
      class WebSocket extends IrohWebSocket {
        constructor(url, protocols) {
          const target = toUrl(url);
          const b = route(target, forced);
          const headers = new Headers();
          setHost(headers, target);
          const cookie = b.jar?.header(target.pathname);
          if (cookie) headers.set("cookie", cookie);
          super(gatewayClient(client, b), target.pathname + target.search, { protocols, headers, url: String(url) });
        }
      }
      class EventSource extends IrohEventSource {
        constructor(url, init) {
          super(irohFetch, String(url), init);
        }
      }
      class XMLHttpRequest extends IrohXMLHttpRequest {
        constructor() {
          super(irohFetch);
        }
      }
      return {
        fetch: irohFetch,
        websocket: (path, { protocols, headers } = {}) => {
          const target = toUrl(path);
          const b = route(target, forced);
          const all = new Headers(headers);
          setHost(all, target);
          const cookie = b.jar?.header(target.pathname);
          if (cookie && !all.has("cookie")) all.set("cookie", cookie);
          return new IrohWebSocket(gatewayClient(client, b), target.pathname + target.search, {
            protocols,
            headers: all,
            url: String(path),
          });
        },
        WebSocket,
        EventSource,
        XMLHttpRequest,
      };
    };

    const root = api(null);
    const hosts = backends.map((b) => b.host).filter(Boolean);
    return {
      ...root,
      serviceWorker: (opts) => registerServiceWorker(root.fetch, opts?.hosts ?? hosts, opts),
      proxyUrl: (url) => proxyUrl(url, hosts),
      /** Fetches `url` over iroh and returns a blob: URL for <img>/<video> without a service worker. */
      objectUrl: (url, init) => objectUrl(root.fetch, url, init),
      /** Scope everything to one backend, so path-only URLs go there: `iroh.backend("https://files.example.com")`. */
      backend(origin) {
        const host = new URL(origin).host;
        const b = backends.find((x) => x.host === host);
        if (!b) throw new TypeError(`iroh-fetch: no backend configured for ${origin}`);
        return { ...api(b), cookies: cookieApi(b.jar), endpointIds: b.replicas.map((r) => r.endpointId) };
      },
      backends: backends.map((b) => ({
        origin: b.origin,
        replicas: b.replicas.map((r) => ({
          endpointId: r.endpointId,
          get healthy() {
            return r.downUntil <= Date.now();
          },
          get connected() {
            return r.connected;
          },
        })),
        get connected() {
          return b.replicas.some((r) => r.downUntil <= Date.now() && r.connected);
        },
      })),
      install(opts = {}) {
        if (!opts.origins && !opts.match && single) {
          throw new TypeError("iroh-fetch: install() needs origins (or backends in createIrohFetch)");
        }
        const origins = opts.origins ?? backends.map((b) => b.origin).filter(Boolean);
        return install(root, { ...opts, origins });
      },
      cookies: cookieApi(backends[0].jar),
      endpointId: client.endpointId(),
      /** This client's secret key (hex), to give it the same endpoint id next time. Keep it secret. */
      exportSecretKey: () => client.secretKey(),
      /** Empties the HTTP cache (`cache` option), e.g. on logout. */
      clearCache: async () => httpCache?.clear(),
      close: () => {
        closed = true;
        for (const t of timers) clearTimeout(t);
        return client.close();
      },
    };
  };
}

function parseBackends(options) {
  const replicasOf = (cfg, what) => {
    const ids = typeof cfg === "string" ? [cfg] : Array.isArray(cfg) ? cfg : (cfg?.endpointIds ?? (cfg?.endpointId ? [cfg.endpointId] : []));
    if (!ids.length) throw new Error(`iroh-fetch: ${what} needs an endpointId`);
    return ids.map((id) => ({ endpointId: String(id).trim(), downUntil: 0, connected: false }));
  };
  const strategyOf = (cfg) => {
    const s = (cfg && typeof cfg === "object" && !Array.isArray(cfg) && cfg.strategy) || "failover";
    if (s !== "failover" && s !== "round-robin") throw new Error(`iroh-fetch: unknown strategy ${s}`);
    return s;
  };
  if (options?.backends) {
    const entries = Object.entries(options.backends);
    if (!entries.length) throw new Error("iroh-fetch: backends is empty");
    return entries.map(([origin, cfg]) => {
      const url = new URL(origin);
      return {
        origin: url.origin,
        host: url.host,
        replicas: replicasOf(cfg, `backend ${origin}`),
        strategy: strategyOf(cfg),
        // Start each client at a random replica, so round-robin also spreads first and long-lived
        // connections (WebSockets) across many clients instead of piling them onto the first one.
        next: Math.floor(Math.random() * 1024),
        default: Boolean(cfg && typeof cfg === "object" && !Array.isArray(cfg) && cfg.default),
      };
    });
  }
  const ids = options?.endpointIds ?? options?.endpointId;
  if (!ids || (Array.isArray(ids) && !ids.length)) throw new Error("iroh-fetch: endpointId (or backends) is required");
  return [{ origin: null, host: null, replicas: replicasOf(ids, "endpointId"), strategy: options.strategy ?? "failover", next: Math.floor(Math.random() * 1024), default: true }];
}

/**
 * Replicas to try, best first: healthy ones in configured order (failover) or rotating
 * (round-robin), then the unhealthy ones, least recently failed first.
 */
function replicaOrder(b) {
  const now = Date.now();
  // Replicas with a live connection go first, so a dead or still-dialing one never delays a request.
  let healthy = b.replicas.filter((r) => r.downUntil <= now);
  healthy = [...healthy.filter((r) => r.connected), ...healthy.filter((r) => !r.connected)];
  if (b.strategy === "round-robin" && healthy.length > 1) {
    const k = b.next++ % healthy.length;
    healthy = [...healthy.slice(k), ...healthy.slice(0, k)];
  }
  const down = b.replicas.filter((r) => r.downUntil > now).sort((x, y) => x.downUntil - y.downUntil);
  // Reconnect replicas that came out of cooldown in the background, off the request path, so a
  // recovered primary is preferred again as soon as it's back.
  for (const r of healthy) if (!r.connected) r.ensureConnected().catch(() => {});
  return [...healthy, ...down];
}

/** Runs `attempt(replica)` on replicas in order until one succeeds; `retryable(err)` gates moving on. */
async function withFailover(b, attempt, retryable = () => true) {
  let lastErr;
  for (const r of replicaOrder(b)) {
    try {
      await r.ensureConnected();
    } catch (err) {
      lastErr = err; // never reached the gateway, so trying the next one is always safe
      continue;
    }
    try {
      return await attempt(r);
    } catch (err) {
      lastErr = err;
      if (err?.name === "AbortError" || err?.name === "TimeoutError") throw err;
      // Park it and make the next use re-dial (with the connect timeout) instead of trusting
      // a connection that may be dead.
      r.downUntil = Date.now() + b.cooldown;
      r.connected = false;
      r.reset();
      if (!retryable(err)) throw err;
    }
  }
  throw lastErr;
}

function connectWithTimeout(client, r, ms) {
  const id = r.endpointId;
  // The relay's own complaint (e.g. a refused relay token) explains a failure better than a timeout.
  const relayHint = () => {
    const problem = client.relayProblem?.();
    return problem ? ` (${problem})` : "";
  };
  // Address lookups and relay connections sometimes fail transiently (a dropped request, a gateway
  // that just restarted), so keep retrying with backoff until the deadline instead of giving up on
  // the first error. A relay that refuses us won't change its mind, so that fails right away.
  let stopped = false;
  let lastErr = null;
  const attempt = async () => {
    for (let delay = 250; ; delay = Math.min(delay * 2, 2000)) {
      try {
        return await client.connect(id);
      } catch (err) {
        lastErr = err;
        if (stopped || !ms || /refused this client/.test(client.relayProblem?.() ?? "")) throw err;
        await new Promise((resolve) => setTimeout(resolve, delay));
        if (stopped) throw err;
      }
    }
  };
  return withTimeout(attempt(), ms, () => {
    stopped = true;
    const last = lastErr ? `; last error: ${lastErr?.message ?? lastErr}` : "";
    return new DOMException(
      `iroh-fetch: could not reach gateway ${id.slice(0, 10)}… within ${ms} ms ` +
        "(is iroh-gateway running, and is the endpoint id right?)" +
        relayHint() +
        last,
      "TimeoutError",
    );
  }).then(
    () => {
      r.connected = true;
      r.downUntil = 0;
    },
    (err) => {
      r.connected = false;
      if (err?.name === "TimeoutError") throw err;
      throw new Error(
        `iroh-fetch: could not connect to gateway ${id.slice(0, 10)}… ` +
          `(is iroh-gateway running, and is the endpoint id right?): ${err?.message ?? err}${relayHint()}`,
        { cause: err },
      );
    },
  );
}

/** The per-backend view of the shared client that IrohWebSocket expects, with failover. */
function gatewayClient(client, b) {
  return {
    websocket: (path, headers) => withFailover(b, (r) => client.websocket(r.endpointId, path, headers)),
  };
}

function cookieApi(jar) {
  return jar ?? { get: () => undefined, clear() {}, header: () => "" };
}

async function fetchOverIroh(client, route, forced, input, init, cache) {
  // A ReadableStream body is uploaded as-is. Everything else goes through the platform's
  // Request, which normalises strings, FormData, Blob, URLSearchParams... into a stream and
  // fills in headers such as content-type.
  const streamBody = init?.body instanceof ReadableStream ? init.body : null;
  const req = new Request(toRequestInput(input), streamBody ? { ...init, body: undefined } : init);
  const { signal } = req;
  const hasSignal = Boolean(init?.signal || (input instanceof Request && input.signal));
  signal.throwIfAborted();

  let url = new URL(req.url);
  const backend = route(url, forced);
  const jar = req.credentials !== "omit" ? backend.jar : null;
  // With the cache option, GET / HEAD go through src/cache.js, which sends with the cache off.
  if (cache && (req.method === "GET" || req.method === "HEAD")) {
    const ctx = { backend: backend.cacheKey, cookie: jar?.header(url.pathname) ?? "" };
    return cache.handle(req, ctx, (sent) => fetchOverIroh(client, route, forced, sent, undefined, null));
  }
  let method = req.method;
  let headers = new Headers(req.headers);
  let body = streamBody ?? req.body ?? undefined;
  // Firefox has no Request.body stream, so read the (already encoded) body in one piece there.
  if (!body && !REQUEST_HAS_BODY_STREAM && req.method !== "GET" && req.method !== "HEAD") {
    const bytes = new Uint8Array(await req.arrayBuffer());
    if (bytes.byteLength) body = bytes;
  }
  let redirected = false;

  // Requests without a body can be replayed, so a failure on one replica moves on to the next.
  // Ones with a (streamed) body only fail over while connecting, before anything was sent.
  // Bytes (Firefox's buffered body) can be sent again; a stream is consumed by the first attempt.
  const replayable = !(body instanceof ReadableStream);
  const send = (path, sentHeaders, upload) =>
    withFailover(
      backend,
      async (r) => {
        const pending = client.request(r.endpointId, method, path, sentHeaders, upload);
        return hasSignal ? await raceAbort(pending, signal, (late) => late.body.cancel()) : await pending;
      },
      () => replayable,
    );

  for (let hop = 0; ; hop++) {
    const sent = new Headers(headers);
    setHost(sent, url);
    if (jar && !sent.has("cookie")) {
      const cookie = jar.header(url.pathname);
      if (cookie) sent.set("cookie", cookie);
    }
    // Aborting errors the upload, which resets the stream so the gateway drops the request.
    const upload = body instanceof ReadableStream && hasSignal ? body.pipeThrough(new TransformStream(), { signal }) : body;
    const res = await send(url.pathname + url.search, [...sent], upload);
    jar?.store(res.headers, url.pathname);

    const location = REDIRECT_STATUS.has(res.status) ? headerValue(res.headers, "location") : null;
    const next = location ? new URL(location, url) : null;
    // Follow like fetch does, as long as the redirect stays on this gateway. A redirect to another
    // site (e.g. an OAuth provider) is returned as-is, like redirect: "manual".
    if (next && req.redirect !== "manual" && next.origin === url.origin) {
      if (req.redirect === "error") {
        res.body.cancel();
        throw new TypeError(`iroh-fetch: redirected to ${next.href} with redirect: "error"`);
      }
      if (hop >= MAX_REDIRECTS) {
        res.body.cancel();
        throw new TypeError("iroh-fetch: too many redirects");
      }
      const toGet = res.status === 303 ? method !== "HEAD" : (res.status === 301 || res.status === 302) && method === "POST";
      // 307/308 must replay the body, which is already consumed once streamed; hand those back.
      if (toGet || !(body instanceof ReadableStream)) {
        res.body.cancel();
        if (toGet) {
          method = method === "HEAD" ? "HEAD" : "GET";
          body = undefined;
          for (const h of ["content-type", "content-length", "content-encoding", "content-language", "content-location"]) headers.delete(h);
        }
        url = next;
        redirected = true;
        continue;
      }
    }

    // The body streams in as the backend sends it. Aborting the request cancels the stream,
    // which stops the transfer on the gateway too.
    let stream = res.body;
    if (NULL_BODY_STATUS.has(res.status) || method === "HEAD") {
      stream.cancel();
      stream = null;
    } else if (hasSignal) {
      stream = stream.pipeThrough(new TransformStream(), { signal });
    }
    const response = new Response(stream, { status: res.status, headers: res.headers });
    Object.defineProperty(response, "url", { value: url.href });
    Object.defineProperty(response, "redirected", { value: redirected });
    if (cache) await cache.invalidate(backend.cacheKey, req.url, response);
    return response;
  }
}

/**
 * Routes the global fetch (and optionally WebSocket / EventSource) through iroh for matching
 * URLs, so existing code and libraries keep working unchanged. Returns an uninstall function.
 */
function install(impl, { origins = [], match, websocket = true, eventSource = true, xhr = true } = {}) {
  // Origins must be absolute: a bare "api.example.com" would resolve against the page's URL and
  // silently send the page's own origin (and its credentials) to the gateway.
  const wanted = origins.map((o) => {
    try {
      return new URL(o).host;
    } catch {
      throw new TypeError(`iroh-fetch: install() origins must be absolute URLs such as "https://api.example.com", got ${JSON.stringify(o)}`);
    }
  });
  const matches = match ?? ((url) => ROUTABLE_PROTOCOLS.has(url.protocol) && wanted.includes(url.host));
  const test = (u) => {
    try {
      return matches(new URL(String(u), globalThis.location?.href ?? DEFAULT_ORIGIN));
    } catch {
      return false;
    }
  };

  const saved = {
    fetch: globalThis.fetch,
    WebSocket: globalThis.WebSocket,
    EventSource: globalThis.EventSource,
    XMLHttpRequest: globalThis.XMLHttpRequest,
  };
  if (xhr) globalThis.XMLHttpRequest = routedXMLHttpRequest(saved.XMLHttpRequest, () => new impl.XMLHttpRequest(), test);
  globalThis.fetch = function fetch(input, init) {
    const url = input instanceof Request ? input.url : String(input);
    return test(url) ? impl.fetch(input, init) : saved.fetch.call(this, input, init);
  };
  if (websocket && saved.WebSocket) {
    globalThis.WebSocket = new Proxy(saved.WebSocket, {
      construct: (Native, args) => (test(args[0]) ? new impl.WebSocket(...args) : new Native(...args)),
    });
  }
  if (eventSource) {
    // Node has no global EventSource; provide one that only serves matching URLs.
    const Native =
      saved.EventSource ??
      class {
        constructor(url) {
          throw new TypeError(`EventSource is not available for ${url}`);
        }
      };
    globalThis.EventSource = new Proxy(Native, {
      construct: (N, args) => (test(args[0]) ? new impl.EventSource(...args) : new N(...args)),
    });
  }
  return function uninstall() {
    globalThis.fetch = saved.fetch;
    if (websocket && saved.WebSocket) globalThis.WebSocket = saved.WebSocket;
    if (xhr) {
      if (saved.XMLHttpRequest) globalThis.XMLHttpRequest = saved.XMLHttpRequest;
      else delete globalThis.XMLHttpRequest;
    }
    if (eventSource) {
      if (saved.EventSource) globalThis.EventSource = saved.EventSource;
      else delete globalThis.EventSource;
    }
  };
}

/**
 * Registers the service worker (src/sw.js, served from your origin) so the browser's own loads of
 * backend URLs (<img>, <video>, CSS, fonts, downloads) go over iroh, and answers its requests.
 */
async function registerServiceWorker(irohFetch, hosts, { url = "/iroh-sw.js", scope } = {}) {
  const sw = globalThis.navigator?.serviceWorker;
  if (!sw) throw new Error("iroh-fetch: service workers are not available here (they need https or localhost)");
  if (!hosts.length) throw new Error("iroh-fetch: serviceWorker() needs backends with origins, or { hosts }");

  const onMessage = (event) => {
    if (event.data?.type === "iroh-fetch:request" && event.ports[0]) serveForWorker(irohFetch, event.data, event.ports[0]);
  };
  sw.addEventListener("message", onMessage);
  sw.startMessages?.();
  const registration = await sw.register(`${url}?hosts=${encodeURIComponent(hosts.join(","))}`, scope ? { scope } : undefined);
  // The worker claims open pages when it activates; wait for that so the first images already work,
  // but never hang the app on it (some in-app browsers register workers that never take control).
  const controlled = await withTimeout(
    (async () => {
      await sw.ready;
      if (!sw.controller) await new Promise((resolve) => sw.addEventListener("controllerchange", resolve, { once: true }));
      return true;
    })(),
    5000,
    () => new Error("service worker did not take control"),
  ).catch(() => false);
  return {
    registration,
    controlled,
    async unregister() {
      sw.removeEventListener("message", onMessage);
      await registration.unregister();
    },
  };
}

/** Sends one request for the service worker and streams the response back, a chunk per pull. */
async function serveForWorker(irohFetch, req, port) {
  let reader;
  port.onmessage = async ({ data }) => {
    try {
      if (data.type === "cancel") return void reader?.cancel();
      if (data.type !== "pull" || !reader) return;
      const { value, done } = await reader.read();
      if (done) port.postMessage({ type: "end" });
      else {
        const chunk = value.buffer.byteLength === value.byteLength ? value.buffer : value.slice().buffer;
        port.postMessage({ type: "chunk", chunk }, [chunk]);
      }
    } catch (err) {
      port.postMessage({ type: "error", message: String(err?.message ?? err) });
    }
  };
  try {
    const res = await irohFetch(req.url, { method: req.method, headers: req.headers, body: req.body, redirect: req.redirect });
    reader = res.body?.getReader();
    port.postMessage({ type: "head", status: res.status, statusText: res.statusText, headers: [...res.headers] });
    if (!reader) port.postMessage({ type: "end" });
  } catch (err) {
    port.postMessage({ type: "error", message: String(err?.message ?? err) });
  }
}

/**
 * Loads a backend URL with iroh-fetch and returns a `blob:` URL for it, for places the browser would
 * load it by itself (<img>, <video>) where no service worker is available. Revoke it when done.
 */
async function objectUrl(irohFetch, url, init) {
  const res = await irohFetch(url, init);
  if (!res.ok) {
    await res.body?.cancel().catch(() => {});
    throw new Error(`iroh-fetch: ${String(url)} -> ${res.status}`);
  }
  return URL.createObjectURL(await res.blob());
}

/** A same-origin URL for a backend URL, for links the browser opens itself (downloads, new tabs). */
function proxyUrl(input, hosts) {
  const url = new URL(String(input));
  if (!hosts.includes(url.host)) throw new TypeError(`iroh-fetch: no backend configured for ${url.origin}`);
  const base = new URL("__iroh__/", globalThis.location?.href ?? DEFAULT_ORIGIN);
  return `${base.href}${url.host}${url.pathname}${url.search}`;
}

/** Accepts a path, an absolute http(s)/ws(s) URL or a URL object; only path + query reach the backend. */
function toUrl(input) {
  return new URL(String(input), DEFAULT_ORIGIN);
}

function toRequestInput(input) {
  if (input instanceof Request) return input;
  const s = String(input);
  // Request only accepts http(s) URLs; ws(s) URLs and bare paths are mapped onto a placeholder origin.
  return /^https?:/i.test(s) ? s : new URL(s, DEFAULT_ORIGIN).href;
}

/**
 * Tells the gateway which host the app addressed (it becomes X-Forwarded-Host and lets one gateway
 * route several hosts). Path-only URLs carry no host.
 */
function setHost(headers, url) {
  if (url.host !== DEFAULT_HOST && !headers.has("x-iroh-host")) headers.set("x-iroh-host", url.host);
}

function headerValue(pairs, name) {
  const found = pairs.find(([k]) => k.toLowerCase() === name);
  return found ? found[1] : null;
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
