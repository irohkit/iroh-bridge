// Engine selection (native addon vs WASM). Run the suite with IROH_FETCH_BACKEND=native or =wasm
// to force one; the default picks native when a prebuild (or a local `npm run build:native`) exists.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch, selectEngine } from "../src/node.js";
import { loadNative, platformTag } from "../src/native.js";

const forced = process.env.IROH_FETCH_BACKEND;
const endpointId = process.env.IROH_ENDPOINT_ID;
let nativeAvailable = true;
try {
  loadNative();
} catch {
  nativeAvailable = false;
}

test("platform tag looks like a prebuild package suffix", () => {
  assert.match(platformTag(), /^(linux-(x64|arm64)-(gnu|musl)|darwin-(x64|arm64)|win32-(x64|arm64)-msvc|\w+-\w+)$/);
});

test("engine: wasm is always available", async () => {
  const engine = await selectEngine("wasm");
  assert.equal(engine.native, false);
  assert.equal(typeof engine.IrohClient.create, "function");
});

test("engine: unknown names are rejected", async () => {
  await assert.rejects(selectEngine("quantum"), /unknown engine/);
});

test("engine: auto falls back to WASM without a prebuild, native: fails loudly", async () => {
  const auto = await selectEngine("auto");
  assert.equal(auto.native, nativeAvailable);
  if (nativeAvailable) assert.equal((await selectEngine("native")).native, true);
  else await assert.rejects(selectEngine("native"), /no native addon/);
});

test("forced native: the addon must load", { skip: forced !== "native" }, () => {
  assert.ok(nativeAvailable, "IROH_FETCH_BACKEND=native but no native addon could be loaded");
});

test("native client creates an endpoint and keeps its identity", { skip: !nativeAvailable }, async () => {
  const { IrohClient } = await selectEngine("native");
  const a = await IrohClient.create(undefined, undefined, undefined);
  try {
    assert.match(a.endpointId(), /^[0-9a-f]{64}$/);
    const b = await IrohClient.create(undefined, undefined, a.secretKey());
    assert.equal(b.endpointId(), a.endpointId());
    assert.equal(b.connectionInfo(a.endpointId()), null);
    await b.close();
    await assert.rejects(a.connect("not-an-id"), /invalid endpoint id/);
  } finally {
    await a.close();
  }
});

test("reports the engine and the connection path", { skip: !endpointId, timeout: 60_000 }, async () => {
  const iroh = await createIrohFetch({ endpointId, relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN });
  try {
    assert.equal(iroh.engine, forced !== "wasm" && nativeAvailable ? "native" : "wasm");
    assert.equal((await iroh.fetch("/hello")).status, 200);
    const info = iroh.connectionInfo();
    assert.ok(["direct", "relay"].includes(info.kind), JSON.stringify(info));
    // The WASM build has no UDP: it can only ever use a relay.
    if (iroh.engine === "wasm") assert.ok(info.paths.every((p) => p.kind === "relay"), JSON.stringify(info));
  } finally {
    await iroh.close();
  }
});
