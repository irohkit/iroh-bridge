import { readFile } from "node:fs/promises";
import initWasm, { IrohClient, enableLogging } from "../wasm/web_client.js";
import { makeCreate } from "./core.js";
import { loadNative } from "./native.js";

if (typeof globalThis.WebSocket !== "function") {
  throw new Error("iroh-fetch needs a global WebSocket (Node.js >= 22)");
}

const BACKENDS = new Set(["auto", "native", "wasm"]);

let wasmReady;
const wasm = () =>
  (wasmReady ??= readFile(new URL("../wasm/web_client_bg.wasm", import.meta.url))
    .then((bytes) => initWasm({ module_or_path: bytes }))
    .then(() => ({ IrohClient, enableLogging, native: false })));

/**
 * Picks the client implementation: the native addon (direct UDP paths, hole punching) when a
 * prebuild for this platform is installed, else the WASM build (always via a relay).
 * `backend` (option) or IROH_FETCH_BACKEND (env) = "native" | "wasm" forces one; "auto" is the default.
 */
export async function selectBackend(backend) {
  const wanted = String(backend ?? (process.env.IROH_FETCH_BACKEND || "auto")).toLowerCase();
  if (!BACKENDS.has(wanted)) throw new Error(`iroh-fetch: unknown backend ${JSON.stringify(wanted)} (use "auto", "native" or "wasm")`);
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
  const chosen = await selectBackend(options?.backend);
  let client;
  // Keep a handle on the client core.js creates, for connectionInfo().
  class IrohClient {
    static async create(...args) {
      return (client = await chosen.IrohClient.create(...args));
    }
  }
  const iroh = await makeCreate(async () => ({ ...chosen, IrohClient }))(options);
  iroh.backend = chosen.native ? "native" : "wasm";
  /** How this client reaches a gateway right now: `{ kind: "direct" | "relay" | "none", paths }`, or null. */
  iroh.connectionInfo = (endpointId) => client.connectionInfo(endpointId ?? iroh.backends[0].replicas[0].endpointId) ?? null;
  return iroh;
}
