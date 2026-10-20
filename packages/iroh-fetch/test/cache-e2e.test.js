// The `cache` option end to end, against the demo backends' /cache/* routes. Needs the demo setup:
//   IROH_ENDPOINT_ID   gateway A -> backend A
//   IROH_ENDPOINT_ID_B gateway B -> backend B
import { test, after } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { createIrohFetch } from "iroh-fetch";

const A = process.env.IROH_ENDPOINT_ID;
const B = process.env.IROH_ENDPOINT_ID_B;
const opts = { skip: !A, timeout: 60_000 };
const clients = [];
after(() => Promise.all(clients.map((c) => c.close())));

async function create(options) {
  const relay = process.env.IROH_RELAY_URL ? { relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN } : {};
  const c = await createIrohFetch({ connectTimeout: 10_000, ...relay, ...options });
  clients.push(c);
  return c;
}

const newId = () => randomBytes(6).toString("hex");
/** What the backend really received for `id` (always over the network: /cache/hits is no-store). */
const hits = async (iroh, id) => (await iroh.fetch(`/cache/hits?id=${id}`)).json();

test("a cached GET is served without a network round trip", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  const path = `/cache/resource?id=${id}&max-age=60`;
  const first = await (await iroh.fetch(path)).json();
  assert.equal(first.hit, 1);
  for (let i = 0; i < 3; i++) {
    const res = await iroh.fetch(path);
    assert.equal(res.status, 200);
    assert.equal(res.headers.get("cache-control"), "max-age=60");
    assert.deepEqual(await res.json(), first);
  }
  assert.equal((await hits(iroh, id)).hits, 1, "the backend saw only the first request");
});

test("without the option nothing is cached", opts, async () => {
  const iroh = await create({ endpointId: A });
  const id = newId();
  const path = `/cache/resource?id=${id}&max-age=60&etag=1`;
  assert.equal((await (await iroh.fetch(path)).json()).hit, 1);
  const second = await iroh.fetch(path);
  assert.equal(second.status, 200);
  assert.equal((await second.json()).hit, 2);
  assert.deepEqual(await hits(iroh, id), { id, hits: 2, notModified: 0, version: 1 });
});

test("ETag revalidation turns a 304 into the cached response", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  const path = `/cache/resource?id=${id}&cc=no-cache&etag=1`;
  const first = await (await iroh.fetch(path)).json();
  const second = await iroh.fetch(path);
  assert.equal(second.status, 200);
  assert.equal(second.headers.get("etag"), `"${id}-v1"`);
  assert.deepEqual(await second.json(), first);
  assert.deepEqual(await hits(iroh, id), { id, hits: 2, notModified: 1, version: 1 });

  // A POST changes the resource and drops the entry; the next GET gets the new version.
  const post = await iroh.fetch(path, { method: "POST" });
  assert.equal((await post.json()).version, 2);
  const third = await (await iroh.fetch(path)).json();
  assert.equal(third.version, 2);
  assert.equal((await hits(iroh, id)).notModified, 1);
});

test("Last-Modified revalidation and expiry", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  const path = `/cache/resource?id=${id}&max-age=2&lm=1`;
  const first = await (await iroh.fetch(path)).json();
  assert.deepEqual(await (await iroh.fetch(path)).json(), first);
  assert.equal((await hits(iroh, id)).hits, 1);
  await new Promise((r) => setTimeout(r, 3100));
  const stale = await iroh.fetch(path);
  assert.equal(stale.status, 200);
  assert.deepEqual(await stale.json(), first);
  assert.deepEqual(await hits(iroh, id), { id, hits: 2, notModified: 1, version: 1 });
});

test("no-store responses and cache: no-store requests always go to the backend", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  await (await iroh.fetch(`/cache/resource?id=${id}&cc=no-store`)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&cc=no-store`)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`, { cache: "no-store" })).text();
  assert.equal((await hits(iroh, id)).hits, 4);
});

test("Authorization requests are only cached when the response is public", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  const auth = { headers: { authorization: "Bearer x" } };
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`, auth)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`, auth)).text();
  assert.equal((await hits(iroh, id)).hits, 2);
  await (await iroh.fetch(`/cache/resource?id=${id}&cc=public&max-age=60`, auth)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&cc=public&max-age=60`, auth)).text();
  assert.equal((await hits(iroh, id)).hits, 3);
});

test("cookies from the jar make requests uncacheable unless public", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  await (await iroh.fetch("/login")).text();
  const id = newId();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`)).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`)).text();
  assert.equal((await hits(iroh, id)).hits, 2);
  // credentials: "omit" sends no cookie, so it can be cached.
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`, { credentials: "omit" })).text();
  await (await iroh.fetch(`/cache/resource?id=${id}&max-age=60`, { credentials: "omit" })).text();
  assert.equal((await hits(iroh, id)).hits, 3);
});

test("Vary keeps one variant per header value", opts, async () => {
  const iroh = await create({ endpointId: A, cache: true });
  const id = newId();
  const path = `/cache/resource?id=${id}&max-age=60&vary=accept-language`;
  const lang = (l) => ({ headers: { "accept-language": l } });
  assert.equal((await (await iroh.fetch(path, lang("th"))).json())["accept-language"], "th");
  assert.equal((await (await iroh.fetch(path, lang("en"))).json())["accept-language"], "en");
  assert.equal((await (await iroh.fetch(path, lang("th"))).json())["accept-language"], "th");
  assert.equal((await hits(iroh, id)).hits, 2);
});

test("entries are kept per backend", { ...opts, skip: !(A && B) }, async () => {
  const iroh = await create({ backends: { "https://api.demo": A, "https://files.demo": B }, cache: true });
  const id = newId();
  const path = `/cache/resource?id=${id}&max-age=60`;
  assert.equal((await (await iroh.fetch(`https://api.demo${path}`)).json()).backend, "A");
  assert.equal((await (await iroh.fetch(`https://files.demo${path}`)).json()).backend, "B");
  assert.equal((await (await iroh.fetch(`https://api.demo${path}`)).json()).backend, "A");
  assert.equal((await (await iroh.fetch(`https://files.demo${path}`)).json()).backend, "B");
  assert.equal((await (await iroh.fetch(`https://api.demo/cache/hits?id=${id}`)).json()).hits, 1);
  assert.equal((await (await iroh.fetch(`https://files.demo/cache/hits?id=${id}`)).json()).hits, 1);
});

test("the installed global fetch and XMLHttpRequest use the cache too", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A }, cache: true });
  const uninstall = iroh.install();
  try {
    const id = newId();
    const url = `https://api.demo/cache/resource?id=${id}&max-age=60`;
    const first = await (await fetch(url)).json();
    assert.deepEqual(await (await fetch(url)).json(), first);
    const xhr = new XMLHttpRequest();
    xhr.open("GET", url);
    const loaded = new Promise((resolve) => (xhr.onload = resolve));
    xhr.send();
    await loaded;
    assert.deepEqual(JSON.parse(xhr.responseText), first);
    assert.equal((await (await fetch(`https://api.demo/cache/hits?id=${id}`)).json()).hits, 1);
  } finally {
    uninstall();
  }
});
