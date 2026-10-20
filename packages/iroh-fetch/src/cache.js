// Opt-in HTTP cache (`cache: true` in createIrohFetch). Requests over iroh bypass the browser's own
// HTTP cache, so without this every GET crosses the relay again. It behaves like a browser's private
// cache (RFC 9111) with the Fetch standard's `Request.cache` modes:
// - Cache-Control max-age / no-store / no-cache / private (fine here) / public, and Expires; s-maxage is
//   for shared caches and ignored. No heuristic freshness: without max-age or Expires a response is
//   only kept when it has a validator, and then revalidated on every use.
// - Stale (or no-cache) entries are revalidated with If-None-Match / If-Modified-Since; a 304 refreshes
//   the stored headers and the cached body is returned as a 200.
// - Requests with Authorization or cookies (explicit or from the cookie jar) are only stored, and only
//   served from the cache, when the response says `public`.
// - Keyed by backend + URL, with Vary honoured; only GET and HEAD are cached, and a successful unsafe
//   request (POST, PUT, ...) drops the entries for its URL.

const CACHEABLE_STATUS = new Set([200, 203, 204, 404, 405, 410, 414, 501]);
const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304]);
// The app doing its own conditional or range requests gets them sent as-is.
const CONDITIONAL = ["if-none-match", "if-modified-since", "if-match", "if-unmodified-since", "if-range", "range"];
// Headers a 304 must not overwrite in the stored response.
const KEEP_ON_304 = new Set(["content-length", "content-encoding", "content-range", "transfer-encoding"]);
const MODES = new Set(["default", "no-store", "reload", "no-cache", "force-cache", "only-if-cached"]);

/**
 * @param {boolean | { maxEntries?: number, maxBytes?: number, storage?: "memory" | "cache-api" }} [option]
 * @returns {null | ReturnType<typeof httpCache>}
 */
export function createHttpCache(option) {
  if (!option) return null;
  const { maxEntries = 500, maxBytes = 50_000_000, storage = "memory" } = option === true ? {} : option;
  if (storage !== "memory" && storage !== "cache-api") throw new TypeError(`iroh-fetch: unknown cache storage ${storage}`);
  if (!(maxEntries > 0) || !(maxBytes > 0)) throw new TypeError("iroh-fetch: cache maxEntries and maxBytes must be positive");
  // The Cache API needs a secure context; elsewhere (Node, plain http pages) fall back to memory.
  const store = storage === "cache-api" && globalThis.caches ? cacheApiStore(globalThis.caches, maxEntries, maxBytes) : memoryStore(maxEntries, maxBytes);
  return httpCache(store, maxBytes);
}

function httpCache(store, maxBytes) {
  /**
   * Answers a GET or HEAD from the cache where allowed, otherwise sends it with `send(request)`.
   * `ctx.backend` keys the entry; `ctx.cookie` is the jar's Cookie header that will go with it.
   */
  async function handle(req, ctx, send) {
    const method = req.method;
    let mode = MODES.has(req.cache) ? req.cache : "default";
    const conditional = CONDITIONAL.some((h) => req.headers.has(h));
    if (mode === "default") {
      const cc = parseCacheControl(req.headers.get("cache-control"));
      if (conditional || cc.has("no-store")) mode = "no-store";
      else if (cc.has("no-cache") || cc.get("max-age") === "0" || /no-cache/i.test(req.headers.get("pragma") ?? "")) mode = "no-cache";
    } else if (conditional && mode !== "force-cache" && mode !== "only-if-cached") {
      mode = "no-store";
    }
    if (mode === "no-store") return send(req);

    const credentialed = req.headers.has("authorization") || req.headers.has("cookie") || Boolean(ctx.cookie);
    // What the backend sees, including the jar's cookie, so Vary: Cookie works.
    const sentHeader = (name) => (name === "cookie" ? (req.headers.get("cookie") ?? (ctx.cookie || null)) : req.headers.get(name));
    const keyOf = (m) => `${m} ${ctx.backend} ${req.url}`;
    const usable = (e) => (!credentialed || e.public) && e.vary.every(([name, value]) => sentHeader(name) === value);

    let entry = null;
    if (mode !== "reload") {
      entry = await store.find(keyOf(method), usable);
      // A HEAD can be answered from a stored GET.
      if (!entry && method === "HEAD") entry = await store.find(keyOf("GET"), usable);
    }
    if (entry) {
      const fresh = !entry.noCache && currentAge(entry, Date.now()) < entry.lifetime;
      if (mode === "force-cache" || mode === "only-if-cached" || (mode === "default" && fresh)) return respond(entry, method);
    }
    if (mode === "only-if-cached") throw new TypeError(`iroh-fetch: nothing cached for ${req.url} (cache: "only-if-cached")`);

    let sent = req;
    const etag = entry?.headers.find(([k]) => k === "etag")?.[1];
    const lastModified = entry?.headers.find(([k]) => k === "last-modified")?.[1];
    if (etag || lastModified) {
      const headers = new Headers(req.headers);
      if (etag) headers.set("if-none-match", etag);
      if (lastModified) headers.set("if-modified-since", lastModified);
      sent = new Request(req, { headers });
    }
    const requestTime = Date.now();
    const res = await send(sent);
    const responseTime = Date.now();

    if (sent !== req && res.status === 304) {
      await res.body?.cancel();
      // Freshen the stored entry with the 304's headers.
      const merged = new Map(entry.headers);
      for (const [k, v] of res.headers) if (!KEEP_ON_304.has(k) && k !== "set-cookie") merged.set(k, v);
      // The age is the 304's: an old Date or Age would make the refreshed entry look stale.
      if (!res.headers.has("date")) merged.set("date", new Date(responseTime).toUTCString());
      if (!res.headers.has("age")) merged.delete("age");
      const headers = [...merged];
      const updated = { ...entry, headers, ...timing(headers, requestTime, responseTime) };
      if (updated.noStore) await store.remove(entry);
      else await store.update(entry, updated);
      return respond(updated, method);
    }
    // A new answer replaces the old one, even when the new one may not be stored.
    if (entry && res.status < 500) await store.remove(entry).catch(() => {});
    return keep(keyOf(method), method, credentialed, sentHeader, res, requestTime, responseTime);
  }

  /** Returns `res`, storing it (once its body has been read to the end) when it may be reused. */
  async function keep(key, method, credentialed, sentHeader, res, requestTime, responseTime) {
    if (!CACHEABLE_STATUS.has(res.status) || res.redirected) return res;
    const headers = [...res.headers].filter(([k]) => k !== "set-cookie");
    const meta = timing(headers, requestTime, responseTime);
    const varyNames = (res.headers.get("vary") ?? "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
    const hasValidator = res.headers.has("etag") || res.headers.has("last-modified");
    if (
      meta.noStore ||
      varyNames.includes("*") ||
      (credentialed && !meta.public) ||
      ((meta.noCache || !(meta.lifetime > 0)) && !hasValidator) ||
      Number(res.headers.get("content-length")) > maxBytes
    ) {
      return res;
    }
    const entry = {
      key,
      url: res.url,
      status: res.status,
      headers,
      vary: varyNames.map((name) => [name, sentHeader(name)]),
      ...meta,
    };
    if (!res.body || method === "HEAD") {
      await store.put(entry, new Uint8Array(0)).catch(() => {});
      return res;
    }
    // Pass the body through untouched and keep a copy (the reader may transfer the chunks it gets).
    const chunks = [];
    let size = 0;
    const tap = new TransformStream({
      transform(chunk, controller) {
        controller.enqueue(chunk);
        if (size <= maxBytes) {
          size += chunk.byteLength;
          if (size <= maxBytes) chunks.push(chunk.slice());
          else chunks.length = 0;
        }
      },
      // The body ends once stored, so a request right after it already finds the entry.
      flush: () => (size <= maxBytes ? store.put(entry, concat(chunks, size)).catch(() => {}) : undefined),
    });
    const out = new Response(res.body.pipeThrough(tap), { status: res.status, statusText: res.statusText, headers: res.headers });
    Object.defineProperty(out, "url", { value: res.url });
    return out;
  }

  async function respond(entry, method) {
    const bytes = method === "HEAD" || NULL_BODY_STATUS.has(entry.status) ? null : await store.body(entry);
    const res = new Response(bytes, { status: entry.status, headers: entry.headers });
    Object.defineProperty(res, "url", { value: entry.url });
    return res;
  }

  return {
    handle,
    /** After an unsafe request succeeded, drops what is cached for its URL (and Location / Content-Location). */
    async invalidate(backend, url, res) {
      if (res.status >= 400) return;
      const base = new URL(url);
      const urls = new Set([base.href, res.url]);
      for (const name of ["location", "content-location"]) {
        const value = res.headers.get(name);
        if (!value) continue;
        try {
          const u = new URL(value, base);
          if (u.origin === base.origin) urls.add(u.href.replace(/#.*$/, ""));
        } catch {}
      }
      for (const u of urls) for (const m of ["GET", "HEAD"]) await store.removeKey(`${m} ${backend} ${u}`).catch(() => {});
    },
    clear: () => store.clear(),
  };
}

/** Freshness information for stored response headers. */
function timing(headers, requestTime, responseTime) {
  const get = (name) => headers.find(([k]) => k === name)?.[1] ?? null;
  const cc = parseCacheControl(get("cache-control"));
  let lifetime = null;
  if (cc.has("max-age")) {
    const n = Number.parseInt(cc.get("max-age"), 10);
    lifetime = n >= 0 ? n * 1000 : 0;
  } else if (get("expires") !== null) {
    // An invalid Expires (such as "0") means already expired.
    const expires = Date.parse(get("expires"));
    const date = Date.parse(get("date") ?? "");
    lifetime = Number.isNaN(expires) ? 0 : Math.max(0, expires - (Number.isNaN(date) ? responseTime : date));
  }
  // RFC 9111 4.2.3: corrected initial age.
  const date = Date.parse(get("date") ?? "");
  const apparentAge = Number.isNaN(date) ? 0 : Math.max(0, responseTime - date);
  const ageValue = Math.max(0, Number.parseInt(get("age") ?? "0", 10) || 0) * 1000;
  return {
    lifetime: lifetime ?? 0,
    initialAge: Math.max(apparentAge, ageValue + (responseTime - requestTime)),
    responseTime,
    noStore: cc.has("no-store"),
    noCache: cc.has("no-cache"),
    public: cc.has("public"),
  };
}

function currentAge(entry, now) {
  return entry.initialAge + (now - entry.responseTime);
}

/** Cache-Control directives, lowercased, with unquoted values. */
export function parseCacheControl(value) {
  const out = new Map();
  for (const part of (value ?? "").split(",")) {
    const [name, ...rest] = part.split("=");
    const key = name.trim().toLowerCase();
    if (key) out.set(key, rest.join("=").trim().replace(/^"(.*)"$/, "$1"));
  }
  return out;
}

function concat(chunks, size) {
  const out = new Uint8Array(size);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.byteLength;
  }
  return out;
}

const entrySize = (entry, bytes) => bytes.byteLength + JSON.stringify(entry.headers).length + 200;

/** In-memory store with LRU eviction. */
function memoryStore(maxEntries, maxBytes) {
  const byKey = new Map(); // key -> entries (one per Vary variant)
  const lru = new Set(); // least recently used first
  let total = 0;
  const drop = (e) => {
    if (!lru.delete(e)) return;
    total -= e.size;
    const list = byKey.get(e.key)?.filter((x) => x !== e);
    if (list?.length) byKey.set(e.key, list);
    else byKey.delete(e.key);
  };
  const touch = (e) => {
    lru.delete(e);
    lru.add(e);
  };
  return {
    async find(key, match) {
      const e = byKey.get(key)?.find(match);
      if (e) touch(e);
      return e ?? null;
    },
    async body(e) {
      return e.bytes;
    },
    async put(entry, bytes) {
      const e = { ...entry, bytes, size: entrySize(entry, bytes) };
      if (e.size > maxBytes) return;
      const same = JSON.stringify(e.vary);
      for (const old of byKey.get(e.key) ?? []) if (JSON.stringify(old.vary) === same) drop(old);
      byKey.set(e.key, [...(byKey.get(e.key) ?? []), e]);
      lru.add(e);
      total += e.size;
      for (const old of lru) {
        if (lru.size <= maxEntries && total <= maxBytes) break;
        drop(old);
      }
    },
    async update(old, entry) {
      if (!lru.has(old)) return;
      Object.assign(old, entry, { bytes: old.bytes, size: old.size });
      touch(old);
    },
    async remove(e) {
      drop(e);
    },
    async removeKey(key) {
      for (const e of byKey.get(key) ?? []) drop(e);
    },
    async clear() {
      byKey.clear();
      lru.clear();
      total = 0;
    },
  };
}

/**
 * Persistent store in the browser's Cache API (survives reloads). Each variant is saved under a
 * synthetic URL; the entry's metadata travels in a header of the stored response.
 */
function cacheApiStore(caches, maxEntries, maxBytes) {
  const NAME = "iroh-fetch";
  const BASE = "https://iroh-fetch.invalid/";
  const META = "x-iroh-cache-meta";
  let opened;
  const open = () => (opened ??= caches.open(NAME));
  const urlOf = (key) => BASE + encodeURIComponent(key);
  const readMeta = (res) => {
    try {
      return JSON.parse(decodeURIComponent(res.headers.get(META) ?? ""));
    } catch {
      return null;
    }
  };
  // Use order and sizes for eviction: rebuilt from the stored entries on first write.
  let index;
  const loadIndex = () =>
    (index ??= (async () => {
      const cache = await open();
      const map = new Map();
      for (const req of await cache.keys()) {
        const res = await cache.match(req);
        const meta = res && readMeta(res);
        if (meta) map.set(req.url, meta.size ?? 0);
        else await cache.delete(req);
      }
      return map;
    })());
  const forget = async (url) => {
    (await loadIndex()).delete(url);
    await (await open()).delete(url);
  };
  return {
    async find(key, match) {
      const cache = await open();
      for (const res of await cache.matchAll(urlOf(key), { ignoreSearch: true, ignoreVary: true })) {
        const meta = readMeta(res);
        if (meta && meta.key === key && match(meta)) {
          meta.response = res;
          const map = await loadIndex();
          if (map.has(meta.cacheUrl)) {
            map.delete(meta.cacheUrl);
            map.set(meta.cacheUrl, meta.size);
          }
          return meta;
        }
      }
      return null;
    },
    async body(e) {
      return new Uint8Array(await e.response.arrayBuffer());
    },
    async put(entry, bytes) {
      const size = entrySize(entry, bytes);
      if (size > maxBytes) return;
      const cacheUrl = `${urlOf(entry.key)}?v=${encodeURIComponent(JSON.stringify(entry.vary))}`;
      const meta = { ...entry, cacheUrl, size };
      const map = await loadIndex();
      const cache = await open();
      await cache.put(cacheUrl, new Response(bytes.byteLength ? bytes : null, { headers: { [META]: encodeURIComponent(JSON.stringify(meta)) } }));
      map.delete(cacheUrl);
      map.set(cacheUrl, size);
      let total = 0;
      for (const s of map.values()) total += s;
      for (const [url, s] of map) {
        if (map.size <= maxEntries && total <= maxBytes) break;
        total -= s;
        await forget(url);
      }
    },
    async update(old, entry) {
      const cache = await open();
      const current = await cache.match(old.cacheUrl);
      if (!current) return;
      const { response, ...meta } = { ...old, ...entry, response: undefined };
      const bytes = await current.arrayBuffer();
      await cache.put(old.cacheUrl, new Response(bytes.byteLength ? bytes : null, { headers: { [META]: encodeURIComponent(JSON.stringify(meta)) } }));
      // The caller still reads the body from the entry it holds.
      entry.response = new Response(bytes.byteLength ? bytes : null);
    },
    async remove(e) {
      await forget(e.cacheUrl);
    },
    async removeKey(key) {
      const cache = await open();
      for (const req of await cache.keys(urlOf(key), { ignoreSearch: true, ignoreVary: true })) await forget(req.url);
    },
    async clear() {
      index = undefined;
      opened = undefined;
      await caches.delete(NAME);
    },
  };
}
