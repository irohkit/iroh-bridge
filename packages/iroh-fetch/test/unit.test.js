// Runs without a gateway: client-side safety checks.
import { test } from "node:test";
import assert from "node:assert/strict";
import { CookieJar } from "../src/cookies.js";
import { IrohWebSocket } from "../src/websocket.js";

const setCookie = (...values) => values.map((v) => ["set-cookie", v]);

test("cookie jar ignores Domain and bad Max-Age, and stays bounded", () => {
  const jar = new CookieJar();
  jar.store(setCookie("a=1; Domain=other.example; Path=/", "b=2; Max-Age=soon", "c=3; Max-Age=0"), "/x");
  assert.equal(jar.get("a"), "1");
  // An unparsable Max-Age is ignored instead of making the cookie silently invisible.
  assert.equal(jar.get("b"), "2");
  assert.equal(jar.get("c"), undefined);
  jar.store(setCookie(`big=${"x".repeat(5000)}`), "/");
  assert.equal(jar.get("big"), undefined);
  for (let i = 0; i < 500; i++) jar.store(setCookie(`n${i}=${i}; Path=/`), "/");
  assert.equal(jar.header("/").split("; ").length, 180);
  assert.equal(jar.get("n499"), "499");
});

test("cookie paths match whole segments", () => {
  const jar = new CookieJar();
  jar.store(setCookie("s=1; Path=/admin"), "/");
  assert.equal(jar.header("/admin/x"), "s=1");
  assert.equal(jar.header("/administrator"), "");
});

test("persisted cookies that page scripts tampered with are skipped", () => {
  const storage = new Map();
  const store = { getItem: (k) => storage.get(k) ?? null, setItem: (k, v) => storage.set(k, v) };
  store.setItem("k", JSON.stringify([["ok", { value: "1", path: "/", expires: null }], ["bad", { value: { x: 1 } }], [42, {}]]));
  const jar = new CookieJar({ storage: store, key: "k" });
  assert.equal(jar.header("/"), "ok=1");
});

test("WebSocket close sends only codes and reasons that are valid on the wire", async () => {
  const sent = [];
  const socket = {
    recv: () => new Promise(() => {}),
    close: async (code, reason) => sent.push([code, reason]),
  };
  const client = { websocket: async () => ({ status: 101, headers: [], socket }) };
  for (const [code, reason, want] of [
    [1005, "", [1000, ""]],
    [1006, "", [1000, ""]],
    [4000, "é".repeat(100), [4000, "é".repeat(61)]],
  ]) {
    const ws = new IrohWebSocket(client, "/ws");
    await new Promise((r) => (ws.onopen = r));
    ws.close(code, reason);
    await new Promise((r) => setTimeout(r, 10));
    assert.deepEqual(sent.pop(), want);
  }
});
