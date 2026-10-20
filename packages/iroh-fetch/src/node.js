import { readFile } from "node:fs/promises";
import initWasm, { IrohClient, enableLogging } from "../wasm/web_client.js";
import { makeCreate } from "./core.js";
import { loadNative } from "./native.js";

if (typeof globalThis.WebSocket !== "function") {
  throw new Error("iroh-fetch needs a global WebSocket (Node.js >= 22)");
}

const ENGINES = new Set(["auto", "native", "wasm"]);

let wasmReady;
const wasm = () =>
  (wasmReady ??= readFile(new URL("../wasm/web_client_bg.wasm", import.meta.url))
    .then((bytes) => initWasm({ module_or_path: bytes }))
    .then(() => ({ IrohClient, enableLogging, native: false })));

/**
 * Picks the client implementation: the native addon (UDP, hole punching to direct paths) when a
 * prebuild for this platform is installed, else the WASM build (always through a relay).
 * `engine` (option), else IROH_FETCH_BACKEND (env): "native" or "wasm" forces one, "auto" (default)
 * prefers native and falls back to WASM silently.
 */
export async function selectEngine(engine) {
  const wanted = String(engine ?? (process.env.IROH_FETCH_BACKEND || "auto")).toLowerCase();
  if (!ENGINES.has(wanted)) throw new Error(`iroh-fetch: unknown engine ${JSON.stringify(wanted)} (use "auto", "native" or "wasm")`);
  if (wanted === "wasm") return wasm();
  try {
    return loadNative();
  } catch (err) {
    if (wanted === "native") throw err;
    return wasm();
  }
}

/** @type {import("./index.d.ts").createIrohFetch} */
export async function createIrohFetch(options) {
  const chosen = await selectEngine(options?.engine);
  let client;
  // Keep a handle on the client core.js creates, for connectionInfo().
  class TrackedClient {
    static async create(...args) {
      return (client = await chosen.IrohClient.create(...args));
    }
  }
  const iroh = await makeCreate(async () => ({ ...chosen, IrohClient: TrackedClient }))(options);
  iroh.engine = chosen.native ? "native" : "wasm";
  iroh.connectionInfo = (endpointId) => client.connectionInfo(endpointId ?? iroh.backends[0].replicas[0].endpointId) ?? null;
  return iroh;
}
