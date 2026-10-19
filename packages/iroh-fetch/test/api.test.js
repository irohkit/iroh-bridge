// Fetch-compatibility features. Needs a running iroh-gateway in front of examples/backend:
// IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const opts = { skip: !endpointId, timeout: 60_000 };
let iroh;

before(async () => {
  if (endpointId) iroh = await createIrohFetch({ endpointId, relayUrl: process.env.IROH_RELAY_URL });
});
after(() => iroh?.close());

test("absolute URLs work and res.url reflects them", opts, async () => {
  const res = await iroh.fetch("https://api.example.com/hello?x=1");
  assert.equal((await res.json()).url, "/hello?x=1");
  assert.equal(res.url, "https://api.example.com/hello?x=1");
  assert.equal(res.redirected, false);
});

test("redirects are followed like fetch", opts, async () => {
  const res = await iroh.fetch("/redirect");
  assert.equal(res.status, 200);
  assert.equal(res.redirected, true);
  assert.equal((await res.json()).url, "/hello?from=redirect");
  assert.match(res.url, /\/hello\?from=redirect$/);

  const post = await iroh.fetch("/redirect-303", { method: "POST", body: "x" });
  const data = await post.json();
  assert.equal(data.method, "GET", "303 turns POST into GET");
  assert.equal(data.url, "/hello?from=303");
});

test("redirect: manual and error modes", opts, async () => {
  const manual = await iroh.fetch("/redirect", { redirect: "manual" });
  assert.equal(manual.status, 302);
  assert.equal(manual.headers.get("location"), "/hello?from=redirect");
  await assert.rejects(iroh.fetch("/redirect", { redirect: "error" }), TypeError);
  // Leaving the gateway is handed back instead of followed.
  const away = await iroh.fetch("/redirect-away");
  assert.equal(away.status, 302);
});

test("cookies are kept, sent back and cleared like a browser", opts, async () => {
  iroh.cookies.clear();
  await (await iroh.fetch("/login")).text();
  assert.equal(iroh.cookies.get("session"), "abc123");
  const who = await (await iroh.fetch("/whoami")).json();
  assert.match(who.cookie, /session=abc123/);
  assert.match(who.cookie, /theme=dark/);

  const omitted = await (await iroh.fetch("/whoami", { credentials: "omit" })).json();
  assert.equal(omitted.cookie, null);

  await (await iroh.fetch("/logout")).text();
  assert.equal(iroh.cookies.get("session"), undefined);
  assert.doesNotMatch((await (await iroh.fetch("/whoami")).json()).cookie ?? "", /session=/);
});

test("gateway adds X-Iroh-Remote-Id and X-Forwarded-Proto, and they can't be spoofed", opts, async () => {
  const who = await (
    await iroh.fetch("/whoami", { headers: { "x-iroh-remote-id": "spoofed", "x-forwarded-proto": "http" } })
  ).json();
  assert.equal(who.remoteId, iroh.endpointId);
  assert.equal(who.proto, "https");
});

test("EventSource: named events, reconnect and Last-Event-ID", opts, async () => {
  const es = new iroh.EventSource("/sse-once");
  const got = [];
  let opens = 0;
  es.onopen = () => opens++;
  await new Promise((resolve, reject) => {
    es.addEventListener("tick", (e) => {
      got.push({ ...JSON.parse(e.data), lastEventId: e.lastEventId });
      if (got.length === 4) resolve();
    });
    setTimeout(() => reject(new Error(`only got ${JSON.stringify(got)}`)), 15_000);
  });
  es.close();
  assert.equal(es.readyState, es.CLOSED);
  assert.deepEqual(got.map((g) => g.id), [1, 2, 3, 4]);
  // The second connection resumed from the last id it saw.
  assert.equal(got[2].resumedFrom, 2);
  assert.equal(got[3].lastEventId, "4");
  assert.ok(opens >= 2, "reconnected");
});

test("EventSource: a non-SSE response is a fatal error", opts, async () => {
  const es = new iroh.EventSource("/hello");
  await new Promise((resolve) => (es.onerror = resolve));
  assert.equal(es.readyState, es.CLOSED);
});

test("standard WebSocket constructor sends stored cookies", opts, async () => {
  await (await iroh.fetch("/login")).text();
  const ws = new iroh.WebSocket("wss://api.example.com/ws", ["chat.v1"]);
  assert.equal(ws.url, "wss://api.example.com/ws");
  const first = await new Promise((resolve, reject) => {
    ws.onmessage = (e) => resolve(JSON.parse(e.data));
    ws.onerror = (e) => reject(e.error ?? new Error(e.message));
  });
  assert.equal(first.protocol, "chat.v1");
  ws.close();
});

test("install() routes the global fetch for matching origins only", opts, async () => {
  const realFetch = globalThis.fetch;
  const uninstall = iroh.install({ origins: ["https://api.example.com"] });
  try {
    assert.notEqual(globalThis.fetch, realFetch);
    const res = await fetch("https://api.example.com/hello?via=global");
    assert.equal((await res.json()).url, "/hello?via=global");
    const es = new EventSource("https://api.example.com/sse-once");
    const msg = await new Promise((resolve) => es.addEventListener("tick", resolve, { once: true }));
    es.close();
    assert.equal(JSON.parse(msg.data).id, 1);
    // Other origins still use the real fetch (an unroutable address fails fast).
    await assert.rejects(fetch("http://127.0.0.1:1/"));
  } finally {
    uninstall();
  }
  assert.equal(globalThis.fetch, realFetch);
});
