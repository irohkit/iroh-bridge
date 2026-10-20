// JWT bearer auth over iroh, the way an app would use it against a public API.
// Needs a gateway in front of examples/backend: IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const opts = { skip: !endpointId, timeout: 60_000 };
const API = "https://api.example.com";
let iroh;

before(async () => {
  if (endpointId) iroh = await createIrohFetch({ backends: { [API]: endpointId }, relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN });
});
after(() => iroh?.close());

async function login(query = "") {
  const res = await iroh.fetch(`${API}/auth/login${query}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: "demo", password: "demo" }),
  });
  assert.equal(res.status, 200);
  return (await res.json()).token;
}

const me = (token) => iroh.fetch(`${API}/auth/me`, token ? { headers: { authorization: `Bearer ${token}` } } : {});

test("login rejects bad credentials with 401 and WWW-Authenticate", opts, async () => {
  const res = await iroh.fetch(`${API}/auth/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: "demo", password: "nope" }),
  });
  assert.equal(res.status, 401);
  assert.match(res.headers.get("www-authenticate"), /^Bearer/);
});

test("a JWT from login opens protected routes", opts, async () => {
  const token = await login();
  assert.equal(token.split(".").length, 3);
  const res = await me(token);
  assert.equal(res.status, 200);
  const data = await res.json();
  assert.equal(data.user, "demo");
  assert.equal(data.role, "admin");
});

test("missing, tampered and expired tokens are rejected", opts, async () => {
  assert.equal((await me()).status, 401);

  const token = await login();
  const [h, p, s] = token.split(".");
  const forged = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(p, "base64url")), role: "root" })).toString("base64url");
  assert.equal((await me(`${h}.${forged}.${s}`)).status, 401, "payload changed, signature kept");
  assert.equal((await me(`${h}.${p}.${s.slice(0, -2)}AA`)).status, 401, "signature changed");
  const none = Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString("base64url");
  assert.equal((await me(`${none}.${p}.`)).status, 401, "alg none");

  const short = await login("?ttl=1");
  assert.equal((await me(short)).status, 200);
  await new Promise((r) => setTimeout(r, 2100));
  assert.equal((await me(short)).status, 401, "expired");
});

test("Authorization works through the installed global fetch", opts, async () => {
  const token = await login();
  const uninstall = iroh.install();
  try {
    const res = await fetch(`${API}/auth/me`, { headers: { Authorization: `Bearer ${token}` } });
    assert.equal((await res.json()).user, "demo");
  } finally {
    uninstall();
  }
});

test("WebSocket with a bearer header, and with ?access_token=", opts, async () => {
  const token = await login();
  const hello = (ws) =>
    new Promise((resolve, reject) => {
      ws.onmessage = (e) => (resolve(JSON.parse(e.data)), ws.close());
      ws.onclose = (e) => reject(new Error(`closed ${e.code}`));
    });
  const viaHeader = await hello(iroh.websocket(`wss://api.example.com/ws-auth`, { headers: { authorization: `Bearer ${token}` } }));
  assert.equal(viaHeader.user, "demo");
  const viaQuery = await hello(new iroh.WebSocket(`wss://api.example.com/ws-auth?access_token=${token}`));
  assert.equal(viaQuery.user, "demo");

  const denied = new iroh.WebSocket("wss://api.example.com/ws-auth");
  const ev = await new Promise((resolve) => (denied.onclose = resolve));
  assert.equal(ev.code, 1006);
  assert.equal(ev.wasClean, false);
});

test("EventSource with a bearer header", opts, async () => {
  const token = await login();
  const es = new iroh.EventSource(`${API}/auth/events`, { headers: { authorization: `Bearer ${token}` } });
  const first = await new Promise((resolve, reject) => {
    es.addEventListener("private", resolve, { once: true });
    setTimeout(() => reject(new Error("no event")), 10_000);
  });
  es.close();
  assert.deepEqual(JSON.parse(first.data), { n: 1, user: "demo" });

  const anon = new iroh.EventSource(`${API}/auth/events`);
  await new Promise((resolve) => (anon.onerror = resolve));
  assert.equal(anon.readyState, anon.CLOSED, "401 is fatal for EventSource, like in browsers");
});
