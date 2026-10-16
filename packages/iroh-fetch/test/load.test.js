// Runs without a gateway: catches broken WASM builds (e.g. a bad wasm-opt pass) before publishing.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import init, { IrohClient } from "../wasm/web_client.js";

test("WASM module instantiates", async () => {
  const bytes = await readFile(new URL("../wasm/web_client_bg.wasm", import.meta.url));
  await init({ module_or_path: bytes });
  assert.equal(typeof IrohClient.create, "function");
});
