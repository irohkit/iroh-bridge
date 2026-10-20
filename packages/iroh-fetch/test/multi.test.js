// One frontend, several backends, and HA replicas. Needs the demo setup:
//   IROH_ENDPOINT_ID    gateway A1 -> backend A
//   IROH_ENDPOINT_ID_A2 gateway A2 -> backend A (replica)
//   IROH_ENDPOINT_ID_B  gateway B  -> backend B
import { test, after } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { createIrohFetch } from "iroh-fetch";

const A1 = process.env.IROH_ENDPOINT_ID;
const A2 = process.env.IROH_ENDPOINT_ID_A2;
const B = process.env.IROH_ENDPOINT_ID_B;
const opts = { skip: !(A1 && A2 && B), timeout: 60_000 };
const clients = [];
after(() => Promise.all(clients.map((c) => c.close())));

/** A valid endpoint id nobody listens on: a dead replica. */
function deadId() {
  const { x } = generateKeyPairSync("ed25519").publicKey.export({ format: "jwk" });
  return Buffer.from(x, "base64url").toString("hex");
}

async function create(options) {
  const relay = process.env.IROH_RELAY_URL ? { relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN } : {};
  const c = await createIrohFetch({ connectTimeout: 10_000, ...relay, ...options });
  clients.push(c);
  return c;
}

const backendOf = async (res) => (await res.json()).backend;

test("routes by origin to different backends, path-only to the default", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A1, "https://files.demo": B } });
  assert.equal(await backendOf(await iroh.fetch("https://api.demo/hello")), "A");
  assert.equal(await backendOf(await iroh.fetch("https://files.demo/hello")), "B");
  assert.equal(await backendOf(await iroh.fetch("/hello")), "A");
  assert.equal(await backendOf(await iroh.backend("https://files.demo").fetch("/hello")), "B");
  await assert.rejects(iroh.fetch("https://unknown.demo/hello"), /no backend configured/);
});

test("each backend keeps its own cookies", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A1, "https://files.demo": B } });
  await (await iroh.fetch("https://api.demo/login")).text();
  assert.match((await (await iroh.fetch("https://api.demo/whoami")).json()).cookie, /session=abc123/);
  assert.equal((await (await iroh.fetch("https://files.demo/whoami")).json()).cookie, null);
});

test("WebSocket and EventSource route by origin too", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A1, "https://files.demo": B } });
  const ws = new iroh.WebSocket("wss://files.demo/ws");
  const hello = await new Promise((resolve, reject) => {
    ws.onmessage = (e) => resolve(JSON.parse(e.data));
    ws.onerror = (e) => reject(e.error ?? new Error(e.message));
  });
  ws.close();
  assert.equal(hello.backend, "B");
  const es = new iroh.EventSource("https://files.demo/sse-once");
  const tick = await new Promise((resolve) => es.addEventListener("tick", resolve, { once: true }));
  es.close();
  assert.equal(JSON.parse(tick.data).id, 1);
});

test("install() with backends routes the global fetch per origin", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A1, "https://files.demo": B } });
  const uninstall = iroh.install();
  try {
    assert.equal(await backendOf(await fetch("https://api.demo/hello")), "A");
    assert.equal(await backendOf(await fetch("https://files.demo/hello")), "B");
  } finally {
    uninstall();
  }
});

test("one unreachable backend doesn't break the others", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": A1, "https://down.demo": deadId() } });
  assert.equal(await backendOf(await iroh.fetch("https://api.demo/hello")), "A");
  const down = iroh.backends.find((b) => b.origin === "https://down.demo");
  assert.equal(down.connected, false);
  await assert.rejects(iroh.fetch("https://down.demo/hello"), /could not (connect to|reach) gateway/);
});

test("HA failover: a dead replica is skipped", opts, async () => {
  const dead = deadId();
  const started = Date.now();
  const iroh = await create({ backends: { "https://api.demo": [dead, A1] } });
  for (let i = 0; i < 5; i++) {
    const res = await iroh.fetch("https://api.demo/hello");
    assert.equal(res.headers.get("x-iroh-gateway-id"), A1);
    assert.equal(await backendOf(res), "A");
  }
  assert.ok(Date.now() - started < 10_000, "failover shouldn't wait for the dead replica each time");
  const [api] = iroh.backends;
  assert.equal(api.replicas[0].connected, false);
  assert.equal(api.replicas[1].connected, true);
});

test("HA failover also works for WebSocket", opts, async () => {
  const iroh = await create({ backends: { "https://api.demo": [deadId(), A1] } });
  const ws = new iroh.WebSocket("wss://api.demo/ws");
  const hello = await new Promise((resolve, reject) => {
    ws.onmessage = (e) => resolve(JSON.parse(e.data));
    ws.onerror = (e) => reject(e.error ?? new Error(e.message));
  });
  ws.close();
  assert.equal(hello.backend, "A");
});

test("HA round-robin spreads requests over healthy replicas", opts, async () => {
  // A2 sits on n0's public relays, where a first dial now and then takes over 10 s.
  const iroh = await create({ connectTimeout: 30_000, backends: { "https://api.demo": { endpointIds: [A1, A2], strategy: "round-robin" } } });
  const seen = new Set();
  for (let i = 0; i < 6; i++) {
    const res = await iroh.fetch("https://api.demo/hello");
    seen.add(res.headers.get("x-iroh-gateway-id"));
    assert.equal(await backendOf(res), "A");
  }
  assert.deepEqual([...seen].sort(), [A1, A2].sort());
});

test("single-backend shorthand accepts endpointIds", opts, async () => {
  const iroh = await create({ connectTimeout: 30_000, endpointIds: [deadId(), A2] });
  const res = await iroh.fetch("/hello");
  assert.equal(res.headers.get("x-iroh-gateway-id"), A2);
});
