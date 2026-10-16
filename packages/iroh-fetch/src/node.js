import { readFile } from "node:fs/promises";
import initWasm from "../wasm/web_client.js";
import { makeCreate } from "./core.js";

if (typeof globalThis.WebSocket !== "function") {
  throw new Error("iroh-fetch needs a global WebSocket (Node.js >= 22)");
}

let ready;
const init = () =>
  (ready ??= readFile(new URL("../wasm/web_client_bg.wasm", import.meta.url)).then((bytes) =>
    initWasm({ module_or_path: bytes }),
  ));

export const createIrohFetch = makeCreate(init);
