// Runs without a gateway: the HTTP cache (src/cache.js) against a fake network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHttpCache } from "../src/cache.js";

const URL_A = "https://api.demo/item";

/**
 * A fake backend: `respond(req, n)` returns [status, headers, body]. Counts requests and records the
 * headers each one carried.
 */
function network(respond) {
  const net = { calls: 0, requests: [] };
  net.send = async (req) => {
    net.calls++;
    net.requests.push(req);
    const [status, headers = {}, body = null] = respond(req, net.calls);
    const res = new Response(status === 304 || req.method === "HEAD" ? null : body, {
      status,
      headers: { date: new Date().toUTCString(), ...headers },
    });
    Object.defineProperty(res, "url", { value: req.url });
    return res;
  };
  return net;
}

/** fetch through the cache; `ctx` defaults to backend "A" without cookies. */
const get = (cache, net, url = URL_A, init = {}, ctx = {}) => cache.handle(new Request(url, init), { backend: "A", cookie: "", ...ctx }, net.send);
const text = async (p) => (await p).text();

test("cache is off unless asked for", () => {
  assert.equal(createHttpCache(undefined), null);
  assert.equal(createHttpCache(false), null);
  assert.ok(createHttpCache(true));
  assert.throws(() => createHttpCache({ storage: "disk" }), /unknown cache storage/);
});

test("a fresh response is served without a network request", async () => {
  const cache = createHttpCache(true);
  const net = network((_, n) => [200, { "cache-control": "max-age=60" }, `v${n}`]);
  assert.equal(await text(get(cache, net)), "v1");
  const again = await get(cache, net);
  assert.equal(await again.text(), "v1");
  assert.equal(again.status, 200);
  assert.equal(again.url, URL_A);
  assert.equal(again.headers.get("cache-control"), "max-age=60");
  assert.equal(net.calls, 1);
  // The query is part of the key.
  assert.equal(await text(get(cache, net, `${URL_A}?page=2`)), "v2");
});

test("s-maxage is ignored and Expires is honoured", async () => {
  const cache = createHttpCache(true);
  const shared = network((_, n) => [200, { "cache-control": "s-maxage=60" }, `v${n}`]);
  await text(get(cache, shared));
  await text(get(cache, shared));
  assert.equal(shared.calls, 2);

  const expires = network((_, n) => [200, { expires: new Date(Date.now() + 60_000).toUTCString() }, `v${n}`]);
  await text(get(cache, expires, "https://api.demo/expires"));
  assert.equal(await text(get(cache, expires, "https://api.demo/expires")), "v1");
  assert.equal(expires.calls, 1);

  const invalid = network((_, n) => [200, { expires: "0" }, `v${n}`]);
  await text(get(cache, invalid, "https://api.demo/expired"));
  await text(get(cache, invalid, "https://api.demo/expired"));
  assert.equal(invalid.calls, 2);
});

test("max-age wins over Expires, and Age counts against it", async () => {
  const cache = createHttpCache(true);
  const net = network((_, n) => [200, { "cache-control": "max-age=60", expires: "0" }, `v${n}`]);
  await text(get(cache, net));
  assert.equal(await text(get(cache, net)), "v1");
  const aged = network((_, n) => [200, { "cache-control": "max-age=60", age: "120" }, `v${n}`]);
  await text(get(cache, aged, "https://api.demo/aged"));
  await text(get(cache, aged, "https://api.demo/aged"));
  assert.equal(aged.calls, 2);
});

test("a stale response is revalidated with If-None-Match and a 304 returns the cached body", async () => {
  const cache = createHttpCache(true);
  const net = network((req, n) =>
    req.headers.get("if-none-match") === '"v1"'
      ? [304, { etag: '"v1"', "cache-control": "max-age=60", "x-extra": "new" }]
      : [200, { etag: '"v1"', "cache-control": "max-age=0", "content-type": "text/plain" }, `body ${n}`],
  );
  assert.equal(await text(get(cache, net)), "body 1");
  const revalidated = await get(cache, net);
  assert.equal(revalidated.status, 200);
  assert.equal(await revalidated.text(), "body 1");
  assert.equal(net.requests[1].headers.get("if-none-match"), '"v1"');
  // The 304's headers refresh the entry: now fresh for 60 s, and the new header is kept.
  assert.equal(revalidated.headers.get("x-extra"), "new");
  assert.equal(revalidated.headers.get("content-type"), "text/plain");
  assert.equal(await text(get(cache, net)), "body 1");
  assert.equal(net.calls, 2);
});

test("Last-Modified is revalidated with If-Modified-Since", async () => {
  const cache = createHttpCache(true);
  const modified = new Date(Date.now() - 3600_000).toUTCString();
  const net = network((req, n) =>
    req.headers.get("if-modified-since") === modified ? [304, {}] : [200, { "last-modified": modified }, `body ${n}`],
  );
  assert.equal(await text(get(cache, net)), "body 1");
  assert.equal(await text(get(cache, net)), "body 1");
  assert.equal(net.requests[1].headers.get("if-modified-since"), modified);
  assert.equal(net.calls, 2);
});

test("a changed resource replaces the entry", async () => {
  const cache = createHttpCache(true);
  let version = 1;
  const net = network((req) =>
    req.headers.get("if-none-match") === `"${version}"` ? [304, {}] : [200, { etag: `"${version}"`, "cache-control": "no-cache" }, `v${version}`],
  );
  assert.equal(await text(get(cache, net)), "v1");
  version = 2;
  assert.equal(await text(get(cache, net)), "v2");
  assert.equal(await text(get(cache, net)), "v2");
  assert.equal(net.requests[2].headers.get("if-none-match"), '"2"');

  // A new answer that may not be stored still drops the old entry.
  const flip = network((_, n) => (n === 1 ? [200, { etag: '"1"', "cache-control": "no-cache" }, "old"] : [200, { "cache-control": "no-store" }, `new ${n}`]));
  await text(get(cache, flip, "https://api.demo/flip"));
  assert.equal(await text(get(cache, flip, "https://api.demo/flip")), "new 2");
  assert.equal(await text(get(cache, flip, "https://api.demo/flip", { cache: "force-cache" })), "new 3");
});

test("no-store is never stored and no-cache is always revalidated", async () => {
  const cache = createHttpCache(true);
  const noStore = network((_, n) => [200, { "cache-control": "no-store, max-age=60", etag: '"x"' }, `v${n}`]);
  await text(get(cache, noStore));
  assert.equal(await text(get(cache, noStore)), "v2");
  assert.equal(noStore.requests[1].headers.get("if-none-match"), null);

  const noCache = network((req, n) => (req.headers.get("if-none-match") ? [304, {}] : [200, { "cache-control": "no-cache, max-age=60", etag: '"x"' }, `v${n}`]));
  await text(get(cache, noCache, "https://api.demo/nc"));
  assert.equal(await text(get(cache, noCache, "https://api.demo/nc")), "v1");
  assert.equal(noCache.calls, 2);

  // Without max-age, Expires or a validator there is nothing to reuse.
  const plain = network((_, n) => [200, {}, `v${n}`]);
  await text(get(cache, plain, "https://api.demo/plain"));
  await text(get(cache, plain, "https://api.demo/plain"));
  assert.equal(plain.calls, 2);
});

test("only successful, cacheable statuses are stored", async () => {
  const cache = createHttpCache(true);
  const err = network((_, n) => [500, { "cache-control": "max-age=60" }, `v${n}`]);
  await text(get(cache, err));
  await text(get(cache, err));
  assert.equal(err.calls, 2);
  const gone = network((_, n) => [404, { "cache-control": "max-age=60" }, `v${n}`]);
  await text(get(cache, gone, "https://api.demo/gone"));
  assert.equal((await get(cache, gone, "https://api.demo/gone")).status, 404);
  assert.equal(gone.calls, 1);
});

test("Request.cache modes", async () => {
  const cache = createHttpCache(true);
  let etag = '"1"';
  const net = network((req, n) => (req.headers.get("if-none-match") === etag ? [304, {}] : [200, { "cache-control": "max-age=60", etag }, `v${n}`]));
  await text(get(cache, net));

  // no-store: straight to the network, nothing stored or used.
  assert.equal(await text(get(cache, net, URL_A, { cache: "no-store" })), "v2");
  assert.equal(net.requests[1].headers.get("if-none-match"), null);
  // no-cache: revalidated although fresh.
  assert.equal(await text(get(cache, net, URL_A, { cache: "no-cache" })), "v1");
  assert.equal(net.requests[2].headers.get("if-none-match"), '"1"');
  // reload: a full request, and the answer is stored.
  etag = '"2"';
  assert.equal(await text(get(cache, net, URL_A, { cache: "reload" })), "v4");
  assert.equal(net.requests[3].headers.get("if-none-match"), null);
  assert.equal(await text(get(cache, net)), "v4");
  assert.equal(net.calls, 4);

  // force-cache and only-if-cached use a stale entry as it is.
  const stale = network((_, n) => [200, { "cache-control": "max-age=0", etag: '"s"' }, `s${n}`]);
  await text(get(cache, stale, "https://api.demo/stale"));
  assert.equal(await text(get(cache, stale, "https://api.demo/stale", { cache: "force-cache" })), "s1");
  assert.equal(await text(get(cache, stale, "https://api.demo/stale", { cache: "only-if-cached", mode: "same-origin" })), "s1");
  assert.equal(stale.calls, 1);
  await assert.rejects(get(cache, stale, "https://api.demo/missing", { cache: "only-if-cached", mode: "same-origin" }), TypeError);
  // force-cache with nothing cached goes to the network and stores.
  assert.equal(await text(get(cache, stale, "https://api.demo/fc", { cache: "force-cache" })), "s2");
  assert.equal(await text(get(cache, stale, "https://api.demo/fc", { cache: "force-cache" })), "s2");
  assert.equal(stale.calls, 2);
});

test("request Cache-Control and the app's own conditional requests", async () => {
  const cache = createHttpCache(true);
  const net = network((req, n) => (req.headers.get("if-none-match") === '"a"' ? [304, {}] : [200, { "cache-control": "max-age=60", etag: '"a"' }, `v${n}`]));
  await text(get(cache, net));
  await text(get(cache, net, URL_A, { headers: { "cache-control": "no-cache" } }));
  assert.equal(net.requests[1].headers.get("if-none-match"), '"a"');
  await text(get(cache, net, URL_A, { headers: { "cache-control": "no-store" } }));
  assert.equal(net.requests[2].headers.get("if-none-match"), null);
  // The app's own If-None-Match is sent as-is and its 304 handed back.
  const res = await get(cache, net, URL_A, { headers: { "if-none-match": '"a"' } });
  assert.equal(res.status, 304);
  assert.equal(net.calls, 4);
});

test("requests with Authorization or cookies are only cached when the response is public", async () => {
  const cache = createHttpCache(true);
  const net = network((req, n) => [200, { "cache-control": req.url.includes("public") ? "public, max-age=60" : "private, max-age=60" }, `v${n}`]);
  const auth = { headers: { authorization: "Bearer t" } };
  await text(get(cache, net, "https://api.demo/me", auth));
  await text(get(cache, net, "https://api.demo/me", auth));
  assert.equal(net.calls, 2);
  // Cookies from the jar count too.
  await text(get(cache, net, "https://api.demo/jar", {}, { cookie: "session=1" }));
  await text(get(cache, net, "https://api.demo/jar", {}, { cookie: "session=1" }));
  assert.equal(net.calls, 4);
  await text(get(cache, net, "https://api.demo/jar2", { headers: { cookie: "a=1" } }));
  await text(get(cache, net, "https://api.demo/jar2", { headers: { cookie: "a=1" } }));
  assert.equal(net.calls, 6);
  // `public` makes them cacheable.
  await text(get(cache, net, "https://api.demo/public", auth));
  assert.equal(await text(get(cache, net, "https://api.demo/public", auth)), "v7");
  assert.equal(net.calls, 7);
  // A private response to an anonymous request is fine in this (private) cache, but is not handed
  // to a request with credentials.
  await text(get(cache, net, "https://api.demo/anon"));
  assert.equal(await text(get(cache, net, "https://api.demo/anon")), "v8");
  assert.equal(await text(get(cache, net, "https://api.demo/anon", auth)), "v9");
  assert.equal(net.calls, 9);
});

test("Vary picks the variant, and Vary: * is not stored", async () => {
  const cache = createHttpCache(true);
  const net = network((req, n) => [200, { "cache-control": "max-age=60", vary: "Accept-Language" }, `${req.headers.get("accept-language")} ${n}`]);
  const lang = (l) => ({ headers: { "accept-language": l } });
  assert.equal(await text(get(cache, net, URL_A, lang("th"))), "th 1");
  assert.equal(await text(get(cache, net, URL_A, lang("en"))), "en 2");
  assert.equal(await text(get(cache, net, URL_A, lang("th"))), "th 1");
  assert.equal(await text(get(cache, net, URL_A, lang("en"))), "en 2");
  assert.equal(await text(get(cache, net, URL_A)), "null 3");
  assert.equal(net.calls, 3);

  const star = network((_, n) => [200, { "cache-control": "max-age=60", vary: "*" }, `v${n}`]);
  await text(get(cache, star, "https://api.demo/star"));
  await text(get(cache, star, "https://api.demo/star"));
  assert.equal(star.calls, 2);
});

test("entries are kept per backend", async () => {
  const cache = createHttpCache(true);
  const net = network((_, n) => [200, { "cache-control": "max-age=60" }, `v${n}`]);
  assert.equal(await text(get(cache, net, "http://iroh.invalid/x", {}, { backend: "A" })), "v1");
  assert.equal(await text(get(cache, net, "http://iroh.invalid/x", {}, { backend: "B" })), "v2");
  assert.equal(await text(get(cache, net, "http://iroh.invalid/x", {}, { backend: "A" })), "v1");
  assert.equal(net.calls, 2);
});

test("only GET and HEAD are cached; a HEAD can use a stored GET; unsafe requests invalidate", async () => {
  const cache = createHttpCache(true);
  const net = network((req, n) => [200, { "cache-control": "max-age=60", "content-type": "text/plain" }, `${req.method} ${n}`]);
  await text(get(cache, net));
  const head = await get(cache, net, URL_A, { method: "HEAD" });
  assert.equal(head.status, 200);
  assert.equal(head.body, null);
  assert.equal(head.headers.get("content-type"), "text/plain");
  assert.equal(net.calls, 1);

  // An unsafe method that succeeded drops the URL's entries.
  const post = await net.send(new Request(URL_A, { method: "POST", body: "x" }));
  await cache.invalidate("A", URL_A, post);
  assert.equal(await text(get(cache, net)), "GET 3");
  // A failed one doesn't.
  const failed = new Response(null, { status: 500 });
  Object.defineProperty(failed, "url", { value: URL_A });
  await cache.invalidate("A", URL_A, failed);
  assert.equal(await text(get(cache, net)), "GET 3");
});

test("a body is stored once read to the end, and not when the read is cancelled", async () => {
  const cache = createHttpCache(true);
  const net = network((_, n) => [200, { "cache-control": "max-age=60" }, `v${n}`]);
  const unread = await get(cache, net);
  await unread.body.cancel();
  assert.equal(await text(get(cache, net)), "v2");
  assert.equal(await text(get(cache, net)), "v2");
  assert.equal(net.calls, 2);
});

test("maxEntries and maxBytes evict the least recently used entries", async () => {
  const cache = createHttpCache({ maxEntries: 2 });
  const net = network((req, n) => [200, { "cache-control": "max-age=60" }, `${new URL(req.url).pathname} ${n}`]);
  for (const p of ["/a", "/b"]) await text(get(cache, net, `https://api.demo${p}`));
  await text(get(cache, net, "https://api.demo/a")); // /a is now the most recently used
  await text(get(cache, net, "https://api.demo/c")); // evicts /b
  assert.equal(net.calls, 3);
  assert.equal(await text(get(cache, net, "https://api.demo/a")), "/a 1");
  assert.equal(await text(get(cache, net, "https://api.demo/b")), "/b 4");

  const small = createHttpCache({ maxBytes: 4096 });
  const big = network((_, n) => [200, { "cache-control": "max-age=60" }, "x".repeat(5000) + n]);
  await text(get(small, big));
  await text(get(small, big));
  assert.equal(big.calls, 2);
});

test("clear() empties the cache", async () => {
  const cache = createHttpCache(true);
  const net = network((_, n) => [200, { "cache-control": "max-age=60" }, `v${n}`]);
  await text(get(cache, net));
  await cache.clear();
  assert.equal(await text(get(cache, net)), "v2");
});

test('storage: "cache-api" keeps entries in the Cache API and falls back to memory without it', async () => {
  // Without globalThis.caches (Node), memory is used.
  const fallback = createHttpCache({ storage: "cache-api" });
  const net0 = network((_, n) => [200, { "cache-control": "max-age=60" }, `v${n}`]);
  await text(get(fallback, net0));
  assert.equal(await text(get(fallback, net0)), "v1");

  const saved = globalThis.caches;
  globalThis.caches = fakeCacheStorage();
  try {
    const cache = createHttpCache({ storage: "cache-api", maxEntries: 2 });
    const net = network((req, n) =>
      req.headers.get("if-none-match") ? [304, { "cache-control": "max-age=60" }] : [200, { "cache-control": "max-age=0", etag: '"e"', vary: "accept" }, `v${n}`],
    );
    assert.equal(await text(get(cache, net)), "v1");
    assert.equal(await text(get(cache, net)), "v1"); // revalidated: 304
    assert.equal(await text(get(cache, net)), "v1"); // fresh now
    assert.equal(net.calls, 2);
    // A new client on the same storage (a page reload) finds the entry.
    const reloaded = createHttpCache({ storage: "cache-api", maxEntries: 2 });
    assert.equal(await text(get(reloaded, net)), "v1");
    assert.equal(net.calls, 2);
    // Vary variants and eviction.
    assert.equal(await text(get(reloaded, net, URL_A, { headers: { accept: "text/html" } })), "v3");
    assert.equal(await text(get(reloaded, net, "https://api.demo/other")), "v4");
    assert.equal((await (await globalThis.caches.open("iroh-fetch")).keys()).length, 2);
    await reloaded.clear();
    assert.equal(await text(get(reloaded, net, "https://api.demo/other")), "v5");
  } finally {
    if (saved) globalThis.caches = saved;
    else delete globalThis.caches;
  }
});

/** Just enough of CacheStorage for the store: exact URL keys, plus ignoreSearch. */
function fakeCacheStorage() {
  const all = new Map();
  const strip = (u) => u.split("?")[0];
  const open = (name) => {
    if (!all.has(name)) {
      const items = new Map(); // url -> { body, headers, status }
      const toRes = (it) => new Response(it.body, { status: it.status, headers: it.headers });
      const matching = (url, opts) => [...items.entries()].filter(([u]) => (opts?.ignoreSearch ? strip(u) === strip(String(url)) : u === String(url)));
      all.set(name, {
        async put(url, res) {
          items.delete(String(url));
          items.set(String(url), { body: res.body ? await res.arrayBuffer() : null, headers: [...res.headers], status: res.status });
        },
        async match(req, opts) {
          const url = typeof req === "string" ? req : req.url;
          const it = matching(url, opts)[0];
          return it ? toRes(it[1]) : undefined;
        },
        async matchAll(url, opts) {
          return matching(url, opts).map(([, it]) => toRes(it));
        },
        async keys(url, opts) {
          return (url === undefined ? [...items.entries()] : matching(url, opts)).map(([u]) => new Request(u));
        },
        async delete(req) {
          return items.delete(typeof req === "string" ? req : req.url);
        },
      });
    }
    return all.get(name);
  };
  return {
    open: async (name) => open(name),
    delete: async (name) => all.delete(name),
  };
}
