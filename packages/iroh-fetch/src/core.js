import { IrohClient, enableLogging } from "../wasm/web_client.js";
import { CookieJar } from "./cookies.js";
import { IrohEventSource } from "./eventsource.js";
import { IrohWebSocket } from "./websocket.js";

// Responses with these statuses must not carry a body in the Fetch API.
const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304]);
const REDIRECT_STATUS = new Set([301, 302, 303, 307, 308]);
const MAX_REDIRECTS = 20;
// Used as the origin for path-only requests such as fetch("/api/users").
const DEFAULT_ORIGIN = "http://iroh.invalid";
const DEFAULT_HOST = "iroh.invalid";

/**
 * @param {() => Promise<void>} init loads the WASM module for the current runtime
 * @returns {import("./index.d.ts").createIrohFetch}
 */
export function makeCreate(init) {
  return async function createIrohFetch(options) {
    const {
      relayUrl,
      logLevel,
      connect = true,
      connectTimeout = 15_000,
      failoverCooldown: cooldown = 10_000,
      cookies = true,
      persistCookies = false,
    } = options ?? {};
    const single = !options?.backends;
    const backends = parseBackends(options);
    await init();
    if (logLevel) enableLogging(logLevel);
    // One local endpoint serves every gateway; each gateway gets its own connection.
    const client = await IrohClient.create(relayUrl ?? undefined);

    const storage = persistCookies ? (persistCookies === true ? globalThis.localStorage : persistCookies) : undefined;
    for (const b of backends) {
      // One jar per backend, shared by its replicas, so a session survives a failover.
      b.jar = cookies ? new CookieJar(storage && { storage, key: `iroh-fetch:cookies:${b.origin ?? b.replicas[0].endpointId}` }) : null;
      b.cooldown = cooldown;
      for (const r of b.replicas) {
        let pending = null;
        r.reset = () => (pending = null);
        // Dials once and shares the result; a failure is forgotten so a later request retries.
        r.ensureConnected = () =>
          (pending ??= connectWithTimeout(client, r, connectTimeout).catch((err) => {
            pending = null;
            r.downUntil = Date.now() + cooldown;
            throw err;
          }));
      }
    }

    if (connect) {
      // A backend is usable as soon as one of its replicas connects.
      const results = await Promise.allSettled(backends.map((b) => Promise.any(b.replicas.map((r) => r.ensureConnected()))));
      // With one backend, being unreachable is fatal. With several, the others keep working and
      // requests to the unreachable one fail (and retry) on their own.
      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length === backends.length) {
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
      const irohFetch = (input, init) => fetchOverIroh(client, route, forced, input, init);
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
      };
    };

    const root = api(null);
    return {
      ...root,
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
      close: () => client.close(),
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
        next: 0,
        default: Boolean(cfg && typeof cfg === "object" && !Array.isArray(cfg) && cfg.default),
      };
    });
  }
  const ids = options?.endpointIds ?? options?.endpointId;
  if (!ids || (Array.isArray(ids) && !ids.length)) throw new Error("iroh-fetch: endpointId (or backends) is required");
  return [{ origin: null, host: null, replicas: replicasOf(ids, "endpointId"), strategy: options.strategy ?? "failover", next: 0, default: true }];
}

/**
 * Replicas to try, best first: healthy ones in configured order (failover) or rotating
 * (round-robin), then the unhealthy ones, least recently failed first.
 */
function replicaOrder(b) {
  const now = Date.now();
  let healthy = b.replicas.filter((r) => r.downUntil <= now);
  if (b.strategy === "round-robin" && healthy.length > 1) {
    const k = b.next++ % healthy.length;
    healthy = [...healthy.slice(k), ...healthy.slice(0, k)];
  }
  const down = b.replicas.filter((r) => r.downUntil > now).sort((x, y) => x.downUntil - y.downUntil);
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
  return withTimeout(client.connect(id), ms, () =>
    new DOMException(
      `iroh-fetch: could not reach gateway ${id.slice(0, 10)}… within ${ms} ms ` +
        "(is iroh-gateway running, and is the endpoint id right?)",
      "TimeoutError",
    ),
  ).then(
    () => {
      r.connected = true;
      r.downUntil = 0;
    },
    (err) => {
      r.connected = false;
      if (err?.name === "TimeoutError") throw err;
      throw new Error(
        `iroh-fetch: could not connect to gateway ${id.slice(0, 10)}… ` +
          `(is iroh-gateway running, and is the endpoint id right?): ${err?.message ?? err}`,
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

async function fetchOverIroh(client, route, forced, input, init) {
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
  let method = req.method;
  let headers = new Headers(req.headers);
  let body = streamBody ?? req.body ?? undefined;
  let redirected = false;

  // Requests without a body can be replayed, so a failure on one replica moves on to the next.
  // Ones with a (streamed) body only fail over while connecting, before anything was sent.
  const replayable = !body;
  const send = (path, sentHeaders, upload) =>
    withFailover(
      backend,
      async (r) => {
        const pending = client.request(r.endpointId, method, path, sentHeaders, upload);
        return hasSignal ? await raceAbort(pending, signal, (late) => late.body.cancel()) : await pending;
      },
      () => replayable && !upload,
    );

  for (let hop = 0; ; hop++) {
    const sent = new Headers(headers);
    setHost(sent, url);
    if (jar && !sent.has("cookie")) {
      const cookie = jar.header(url.pathname);
      if (cookie) sent.set("cookie", cookie);
    }
    // Aborting errors the upload, which resets the stream so the gateway drops the request.
    const upload = body && hasSignal ? body.pipeThrough(new TransformStream(), { signal }) : body;
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
      if (toGet || !body) {
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
    return response;
  }
}

/**
 * Routes the global fetch (and optionally WebSocket / EventSource) through iroh for matching
 * URLs, so existing code and libraries keep working unchanged. Returns an uninstall function.
 */
function install(impl, { origins = [], match, websocket = true, eventSource = true } = {}) {
  const wanted = origins.map((o) => new URL(o, globalThis.location?.href).host);
  const matches = match ?? ((url) => wanted.includes(url.host));
  const test = (u) => {
    try {
      return matches(new URL(String(u), globalThis.location?.href ?? DEFAULT_ORIGIN));
    } catch {
      return false;
    }
  };

  const saved = { fetch: globalThis.fetch, WebSocket: globalThis.WebSocket, EventSource: globalThis.EventSource };
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
    if (eventSource) {
      if (saved.EventSource) globalThis.EventSource = saved.EventSource;
      else delete globalThis.EventSource;
    }
  };
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
