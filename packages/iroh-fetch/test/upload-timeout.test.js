// Needs a running iroh-gateway in front of examples/backend: IROH_ENDPOINT_ID=<id> npm test
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, randomBytes } from "node:crypto";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const opts = { skip: !endpointId, timeout: 90_000 };
let iroh;

before(async () => {
  if (endpointId) iroh = await createIrohFetch({ endpointId, relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN });
});
after(() => iroh?.close());

const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");

test("ReadableStream body is uploaded as it is produced", opts, async () => {
  const parts = Array.from({ length: 5 }, () => randomBytes(64 * 1024));
  let pulled = 0;
  const body = new ReadableStream({
    async pull(ctrl) {
      if (pulled === parts.length) return ctrl.close();
      await new Promise((r) => setTimeout(r, 50));
      ctrl.enqueue(new Uint8Array(parts[pulled++]));
    },
  });
  const res = await iroh.fetch("/upload", { method: "POST", body, headers: { "content-type": "application/octet-stream" } });
  const data = await res.json();
  assert.equal(data.bytes, 5 * 64 * 1024);
  assert.equal(data.sha256, sha256(Buffer.concat(parts)));
  assert.equal(data.contentType, "application/octet-stream");
});

test("Blob body of 20 MB streams through", opts, async () => {
  const buf = randomBytes(20 * 1024 * 1024);
  const res = await iroh.fetch("/upload", { method: "PUT", body: new Blob([buf], { type: "image/png" }) });
  const data = await res.json();
  assert.equal(data.bytes, buf.length);
  assert.equal(data.sha256, sha256(buf));
  assert.equal(data.contentType, "image/png");
});

test("FormData body keeps its multipart content-type", opts, async () => {
  const form = new FormData();
  form.append("name", "rover");
  form.append("file", new Blob(["hello"]), "hello.txt");
  const data = await (await iroh.fetch("/upload", { method: "POST", body: form })).json();
  assert.match(data.contentType, /^multipart\/form-data; boundary=/);
  assert.ok(data.bytes > 0);
});

test("AbortSignal.timeout aborts while waiting for the response", opts, async () => {
  const started = Date.now();
  await assert.rejects(iroh.fetch("/slow", { signal: AbortSignal.timeout(500) }), { name: "TimeoutError" });
  assert.ok(Date.now() - started < 2000, "abort should not wait for the backend");
  // The connection stays usable afterwards.
  assert.equal((await iroh.fetch("/after-abort")).status, 200);
});

test("connecting to an unknown gateway fails fast with a clear error", opts, async () => {
  // A freshly generated ed25519 key: a valid endpoint id that nobody is listening on.
  const { x } = generateKeyPairSync("ed25519").publicKey.export({ format: "jwk" });
  const ghost = Buffer.from(x, "base64url").toString("hex");
  const started = Date.now();
  await assert.rejects(createIrohFetch({ endpointId: ghost, connectTimeout: 5000 }), (err) => {
    // Either the address lookup fails, or the timeout fires first.
    assert.match(err.message, /could not (connect to|reach) gateway/);
    return true;
  });
  assert.ok(Date.now() - started < 8000);
});
