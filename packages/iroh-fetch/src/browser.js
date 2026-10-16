import initWasm from "../wasm/web_client.js";
import { makeCreate } from "./core.js";

let ready;
const init = () =>
  (ready ??= initWasm({ module_or_path: new URL("../wasm/web_client_bg.wasm", import.meta.url) }));

export const createIrohFetch = makeCreate(init);
