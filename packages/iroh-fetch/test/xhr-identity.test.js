// XMLHttpRequest over iroh and stable client identities.
// Needs a gateway in front of examples/backend: IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const relay = { relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN };
const opts = { skip: !endpointId, timeout: 60_000 };
const API = "https://api.example.com";
let iroh;

before(async () => {
  if (endpointId) iroh = await createIrohFetch({ backends: { [API]: endpointId }, ...relay });
});
after(() => iroh?.close());

/** Runs an XHR to completion and records the events it fired. */
function run(xhr, body) {
  const events = [];
  for (const t of ["readystatechange", "loadstart", "progress", "load", "error", "abort", "timeout", "loadend"]) {
    xhr.addEventListener(t, () => events.push(t === "readystatechange" ? `rs${xhr.readyState}` : t));
  }
  const uploads = [];
  xhr.upload.onprogress = (e) => uploads.push(e.loaded);
  return new Promise((resolve) => {
    xhr.onloadend = () => resolve({ events, uploads });
    xhr.send(body);
  });
}

test("XMLHttpRequest: GET with responseType json, headers and readyState sequence", opts, async () => {
  const xhr = new iroh.XMLHttpRequest();
  xhr.open("GET", `${API}/hello?x=1`);
  xhr.responseType = "json";
  xhr.setRequestHeader("X-Test", "yes");
  const { events } = await run(xhr);
  assert.equal(xhr.status, 200);
  assert.equal(xhr.response.url, "/hello?x=1");
  assert.equal(xhr.response.headers["x-test"], "yes");
  assert.match(xhr.getResponseHeader("content-type"), /json/);
  assert.match(xhr.getAllResponseHeaders(), /content-type: application\/json/);
  assert.equal(xhr.responseURL, `${API}/hello?x=1`);
  const states = events.filter((e) => e.startsWith("rs"));
  assert.equal(states[0], "rs2");
  assert.ok(states.includes("rs3"));
  assert.equal(states.at(-1), "rs4");
  assert.ok(events.indexOf("load") > events.indexOf("rs4") && events.at(-1) === "loadend");
});

test("XMLHttpRequest: POST body with upload progress, text response", opts, async () => {
  const xhr = new iroh.XMLHttpRequest();
  xhr.open("POST", `${API}/upload`);
  const blob = new Blob([new Uint8Array(3 * 1024 * 1024)]);
  const { uploads } = await run(xhr, blob);
  const data = JSON.parse(xhr.responseText);
  assert.equal(data.bytes, blob.size);
  assert.ok(uploads.length >= 2, `upload progress events: ${uploads.length}`);
  assert.equal(uploads.at(-1), blob.size);
});

test("XMLHttpRequest: arraybuffer and blob response types", opts, async () => {
  const a = new iroh.XMLHttpRequest();
  a.open("GET", `${API}/hello`);
  a.responseType = "arraybuffer";
  await run(a);
  assert.ok(a.response instanceof ArrayBuffer && a.response.byteLength > 10);
  const b = new iroh.XMLHttpRequest();
  b.open("GET", `${API}/hello`);
  b.responseType = "blob";
  await run(b);
  assert.ok(b.response instanceof Blob);
  assert.match(b.response.type, /json/);
});

test("XMLHttpRequest: timeout and abort", opts, async () => {
  const t = new iroh.XMLHttpRequest();
  t.open("GET", `${API}/slow`);
  t.timeout = 300;
  const r1 = await run(t);
  assert.ok(r1.events.includes("timeout"));
  assert.equal(t.status, 0);

  const a = new iroh.XMLHttpRequest();
  a.open("GET", `${API}/slow`);
  const done = run(a);
  setTimeout(() => a.abort(), 100);
  const r2 = await done;
  assert.ok(r2.events.includes("abort"));
  assert.equal(a.readyState, a.UNSENT);
});

test("install() routes the global XMLHttpRequest (what axios uses in browsers)", opts, async () => {
  const uninstall = iroh.install();
  try {
    // The way axios's xhr adapter drives it: open, headers, responseType, onloadend, send.
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API}/items`, true);
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.responseType = "json";
    await new Promise((resolve, reject) => {
      xhr.onloadend = resolve;
      xhr.onerror = reject;
      xhr.send(JSON.stringify({ via: "xhr" }));
    });
    assert.equal(xhr.status, 200);
    assert.equal(xhr.response.body, '{"via":"xhr"}');
    // Other origins fall through to the native XHR, which Node doesn't have.
    assert.throws(() => new XMLHttpRequest().open("GET", "https://elsewhere.example/"), /not available/);
  } finally {
    uninstall();
  }
  assert.equal(typeof globalThis.XMLHttpRequest, "undefined");
});

test("persistIdentity keeps the same endpoint id; secretKey sets it explicitly", opts, async () => {
  const store = new Map();
  const storage = { getItem: (k) => store.get(k) ?? null, setItem: (k, v) => store.set(k, String(v)) };
  const a = await createIrohFetch({ endpointId, persistIdentity: storage, ...relay });
  const firstId = a.endpointId;
  await a.close();
  const b = await createIrohFetch({ endpointId, persistIdentity: storage, ...relay });
  assert.equal(b.endpointId, firstId);
  // The backend sees the stable id as X-Iroh-Remote-Id.
  assert.equal((await (await b.fetch("/whoami")).json()).remoteId, firstId);
  const key = b.exportSecretKey();
  await b.close();
  assert.match(key, /^[0-9a-f]{64}$/);
  const c = await createIrohFetch({ endpointId, secretKey: key, ...relay });
  assert.equal(c.endpointId, firstId);
  await c.close();
  await assert.rejects(createIrohFetch({ endpointId, secretKey: "nothex", ...relay }), /64 hex/);
});
