// The iroh-proxy CLI, driven with plain HTTP and WebSocket clients. Needs a running iroh-gateway
// in front of examples/backend: IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import net from "node:net";
import { once } from "node:events";

const endpointId = process.env.IROH_ENDPOINT_ID;
const opts = { skip: !endpointId, timeout: 60_000 };
let child;
let base;

before(async () => {
  if (!endpointId) return;
  const args = [new URL("../bin/iroh-proxy.js", import.meta.url).pathname, endpointId, "--port", "0"];
  if (process.env.IROH_RELAY_URL) args.push("--relay-url", process.env.IROH_RELAY_URL);
  child = spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (d) => (stderr += d));
  // --port 0 picks a free port; the ready line says which.
  base = await new Promise((resolve, reject) => {
    let out = "";
    child.stdout.on("data", (d) => {
      out += d;
      const m = out.match(/iroh-proxy: (http:\/\/\S+) -> gateway/);
      if (m) resolve(m[1]);
    });
    child.once("exit", (code) => reject(new Error(`iroh-proxy exited with ${code}: ${stderr}`)));
  });
});
after(async () => {
  if (!child || child.exitCode !== null) return;
  child.kill("SIGTERM");
  await once(child, "exit");
});

test("GET is forwarded", opts, async () => {
  const res = await fetch(`${base}/hello?x=1`);
  assert.equal(res.status, 200);
  const body = await res.json();
  assert.equal(body.backend, "A");
  assert.equal(body.url, "/hello?x=1");
  // The gateway can route by the host the client used.
  assert.equal(body.headers["x-iroh-host"], new URL(base).host);
});

test("POST body is streamed through", opts, async () => {
  const res = await fetch(`${base}/items`, { method: "POST", headers: { "content-type": "application/json" }, body: '{"name":"x"}' });
  const body = await res.json();
  assert.equal(body.method, "POST");
  assert.equal(body.body, '{"name":"x"}');
  assert.equal(body.headers["content-type"], "application/json");
});

test("redirects are passed through, not followed", opts, async () => {
  const res = await fetch(`${base}/redirect`, { redirect: "manual" });
  assert.equal(res.status, 302);
  assert.equal(res.headers.get("location"), "/hello?from=redirect");
});

test("several set-cookie headers stay separate", opts, async () => {
  const res = await fetch(`${base}/login`);
  assert.deepEqual(res.headers.getSetCookie(), ["session=abc123; Path=/; HttpOnly", "theme=dark; Path=/; Max-Age=3600"]);
});

test("response body streams chunk by chunk", opts, async () => {
  const res = await fetch(`${base}/stream`);
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let reads = 0;
  let text = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    reads++;
    text += value;
  }
  assert.equal(text.trim().split("\n").length, 5);
  assert.ok(reads >= 3, `expected several reads, got ${reads}`);
});

test("WebSocket: hello, echo and server close", opts, async () => {
  const ws = new WebSocket(`${base.replace(/^http/, "ws")}/ws`, ["chat.v1"]);
  ws.binaryType = "arraybuffer";
  const messages = [];
  let notify;
  ws.onmessage = (e) => {
    messages.push(e.data);
    notify?.();
  };
  const next = (n) => new Promise((resolve) => (notify = () => messages.length >= n && resolve()));
  const closed = new Promise((resolve) => (ws.onclose = resolve));
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = () => reject(new Error("WebSocket failed"));
  });
  assert.equal(ws.protocol, "chat.v1");

  if (!messages.length) await next(1);
  assert.deepEqual(JSON.parse(messages[0]), { hello: "from backend", backend: "A", protocol: "chat.v1" });
  ws.send("hi");
  await next(2);
  assert.equal(messages[1], "hi");
  // 70 kB needs the 64-bit length form in both directions.
  const big = randomBytes(70_000);
  ws.send(big);
  await next(3);
  assert.deepEqual(Buffer.from(messages[2]), big);

  ws.send("close-me");
  const ev = await closed;
  assert.equal(ev.code, 4000);
  assert.equal(ev.reason, "bye");
  assert.equal(ev.wasClean, true);
});

test("WebSocket: fragmented message, ping and client close on a raw socket", opts, async () => {
  const { hostname, port } = new URL(base);
  const sock = net.connect(Number(port), hostname);
  await once(sock, "connect");
  sock.write(
    "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
  );
  const frames = readFrames(sock);
  const head = await frames.handshake;
  assert.match(head, /^HTTP\/1\.1 101/);
  // The RFC 6455 example key and its accept value.
  assert.ok(head.includes("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), head);
  assert.equal(JSON.parse((await frames.next()).payload).backend, "A");

  // A 300-byte text message (16-bit length) in three fragments, with a ping in the middle.
  const text = "x".repeat(150) + "y".repeat(150);
  sock.write(clientFrame(0x1, Buffer.from(text.slice(0, 100)), false));
  sock.write(clientFrame(0x0, Buffer.from(text.slice(100, 200)), false));
  sock.write(clientFrame(0x9, Buffer.from("p")));
  sock.write(clientFrame(0x0, Buffer.from(text.slice(200)), true));
  const pong = await frames.next();
  assert.equal(pong.opcode, 0xa);
  assert.equal(pong.payload.toString(), "p");
  const echo = await frames.next();
  assert.equal(echo.opcode, 0x1);
  assert.equal(echo.payload.toString(), text);

  const close = Buffer.alloc(2 + 4);
  close.writeUInt16BE(1000, 0);
  close.write("done", 2);
  sock.write(clientFrame(0x8, close));
  const reply = await frames.next();
  assert.equal(reply.opcode, 0x8);
  assert.equal(reply.payload.readUInt16BE(0), 1000);
  await once(sock, "close");
});

/** A masked client frame (RFC 6455 5.2), for payloads under 64 kB. */
function clientFrame(opcode, payload, fin = true) {
  const mask = randomBytes(4);
  const len = payload.length;
  const header = len < 126 ? Buffer.from([0, 0x80 | len]) : Buffer.from([0, 0x80 | 126, len >> 8, len & 0xff]);
  header[0] = (fin ? 0x80 : 0) | opcode;
  const body = Buffer.from(payload);
  for (let i = 0; i < body.length; i++) body[i] ^= mask[i & 3];
  return Buffer.concat([header, mask, body]);
}

/** Reads the HTTP handshake, then unmasked server frames, from a raw socket. */
function readFrames(sock) {
  let buf = Buffer.alloc(0);
  let waiting = null;
  let gotHandshake;
  const handshake = new Promise((r) => (gotHandshake = r));
  let inHead = true;
  const queue = [];
  const pump = () => {
    if (inHead) {
      const end = buf.indexOf("\r\n\r\n");
      if (end < 0) return;
      gotHandshake(buf.subarray(0, end).toString());
      buf = buf.subarray(end + 4);
      inHead = false;
    }
    for (;;) {
      if (buf.length < 2) break;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) [len, off] = [buf.readUInt16BE(2), 4];
      else if (len === 127) [len, off] = [Number(buf.readBigUInt64BE(2)), 10];
      if (buf.length < off + len) break;
      queue.push({ opcode: buf[0] & 0x0f, payload: buf.subarray(off, off + len) });
      buf = buf.subarray(off + len);
    }
    if (waiting && queue.length) {
      const w = waiting;
      waiting = null;
      w(queue.shift());
    }
  };
  sock.on("data", (d) => {
    buf = Buffer.concat([buf, d]);
    pump();
  });
  return {
    handshake,
    next: () => (queue.length ? Promise.resolve(queue.shift()) : new Promise((r) => (waiting = r))),
  };
}
