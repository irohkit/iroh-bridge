// Needs a running iroh-gateway in front of examples/backend: IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const opts = { skip: !endpointId, timeout: 60_000 };
let iroh;

before(async () => {
  if (endpointId) iroh = await createIrohFetch({ endpointId, relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN });
});
after(() => iroh?.close());

test("response body streams chunk by chunk", opts, async () => {
  const res = await iroh.fetch("/stream");
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  const arrivals = [];
  let text = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    arrivals.push(Date.now());
    text += value;
  }
  assert.equal(text.trim().split("\n").length, 5);
  // The backend waits 300 ms between chunks; buffering would deliver them all at once.
  assert.ok(arrivals.length >= 3, `expected several reads, got ${arrivals.length}`);
  assert.ok(arrivals.at(-1) - arrivals[0] >= 900, "chunks arrived too close together");
});

test("SSE can be read and aborted", opts, async () => {
  const ctrl = new AbortController();
  const res = await iroh.fetch("/sse", { signal: ctrl.signal });
  assert.equal(res.headers.get("content-type"), "text/event-stream");
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = "";
  while ((buf.match(/\n\n/g) ?? []).length < 2) buf += (await reader.read()).value;
  ctrl.abort();
  await assert.rejects(reader.read());
  assert.match(buf, /data: \{"n":1/);
});

test("WebSocket text, binary, subprotocol and server close", opts, async () => {
  const ws = iroh.websocket("/ws", { protocols: ["chat.v1"], headers: { authorization: "Bearer test" } });
  ws.binaryType = "arraybuffer";
  const messages = [];
  const closed = new Promise((resolve) => ws.addEventListener("close", resolve));
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = (e) => reject(e.error ?? new Error(e.message));
  });
  assert.equal(ws.protocol, "chat.v1");
  ws.onmessage = (e) => messages.push(e.data);

  ws.send("hi");
  ws.send(new Uint8Array([1, 2, 3]));
  ws.send("close-me");
  const ev = await closed;

  assert.deepEqual(JSON.parse(messages[0]), { hello: "from backend", backend: "A", protocol: "chat.v1" });
  assert.equal(messages[1], "hi");
  assert.deepEqual([...new Uint8Array(messages[2])], [1, 2, 3]);
  assert.equal(ev.code, 4000);
  assert.equal(ev.reason, "bye");
  assert.equal(ev.wasClean, true);
  assert.equal(ws.readyState, ws.CLOSED);
});

test("WebSocket client close", opts, async () => {
  const ws = iroh.websocket("/ws");
  await new Promise((r) => (ws.onopen = r));
  const ev = await new Promise((resolve) => {
    ws.onclose = resolve;
    ws.close(1000, "done");
  });
  assert.equal(ev.code, 1000);
  assert.equal(ev.wasClean, true);
});

test("WebSocket to a path the backend rejects fires error + close", opts, async () => {
  const ws = iroh.websocket("/not-a-ws");
  const ev = await new Promise((resolve) => (ws.onclose = resolve));
  assert.equal(ev.code, 1006);
  assert.equal(ev.wasClean, false);
});
