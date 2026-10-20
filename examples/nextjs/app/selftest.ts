// Checks run by the "Run self-test" button. They use only the standard browser APIs (fetch,
// EventSource, WebSocket), which iroh-fetch routes over iroh for the configured origins.
import { API, FILES } from "../iroh.config";

import type { IrohFetch } from "iroh-fetch";

export type Check = { name: string; run: (ctx: { endpointId: string; client: IrohFetch }) => Promise<string> };

function assert(ok: unknown, message: string): asserts ok {
  if (!ok) throw new Error(message);
}

const json = async (url: string, init?: RequestInit) => {
  const res = await fetch(url, init);
  assert(res.ok, `${url} -> ${res.status}`);
  return res.json();
};

export const checks: Check[] = [
  {
    name: "GET on backend A (api.demo)",
    run: async () => {
      const data = await json(`${API}/hello?x=1`);
      assert(data.backend === "A" && data.url === "/hello?x=1", JSON.stringify(data));
      return `backend ${data.backend}`;
    },
  },
  {
    name: "GET on backend B (files.demo)",
    run: async () => {
      const data = await json(`${FILES}/hello`);
      assert(data.backend === "B", JSON.stringify(data));
      return `backend ${data.backend}`;
    },
  },
  {
    name: "POST JSON body",
    run: async () => {
      const data = await json(`${API}/items`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "rover" }),
      });
      assert(data.method === "POST" && data.body === '{"name":"rover"}', JSON.stringify(data));
      return "echoed";
    },
  },
  {
    name: "Redirect is followed",
    run: async () => {
      const res = await fetch(`${API}/redirect`);
      const data = await res.json();
      assert(res.redirected && data.url === "/hello?from=redirect", `${res.redirected} ${data.url}`);
      return res.url;
    },
  },
  {
    name: "Cookie session (login → whoami)",
    run: async () => {
      await (await fetch(`${API}/login`)).text();
      const who = await json(`${API}/whoami`);
      assert(/session=abc123/.test(who.cookie ?? ""), `cookie: ${who.cookie}`);
      const other = await json(`${FILES}/whoami`);
      assert(!other.cookie, "cookies must not leak to another backend");
      return who.cookie;
    },
  },
  {
    name: "JWT: login, Bearer token, tampered token rejected",
    run: async () => {
      const login = await json(`${API}/auth/login`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username: "demo", password: "demo" }),
      });
      const auth = { authorization: `Bearer ${login.token}` };
      const me = await json(`${API}/auth/me`, { headers: auth });
      assert(me.user === "demo", JSON.stringify(me));
      const anon = await fetch(`${API}/auth/me`);
      assert(anon.status === 401, `no token -> ${anon.status}`);
      const forged = await fetch(`${API}/auth/me`, { headers: { authorization: `Bearer ${login.token.slice(0, -3)}AAA` } });
      assert(forged.status === 401, `forged -> ${forged.status}`);
      return `signed in as ${me.name}`;
    },
  },
  {
    name: "JWT: authenticated WebSocket (?access_token=)",
    run: async () => {
      const { token } = await json(`${API}/auth/login`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username: "demo", password: "demo" }),
      });
      return new Promise((resolve, reject) => {
        const ws = new WebSocket(`${API.replace("https", "wss")}/ws-auth?access_token=${encodeURIComponent(token)}`);
        const timer = setTimeout(() => (ws.close(), reject(new Error("timeout"))), 10_000);
        ws.onmessage = (e) => {
          clearTimeout(timer);
          ws.close();
          const hello = JSON.parse(e.data);
          hello.user === "demo" ? resolve(`hello ${hello.user}`) : reject(new Error(e.data));
        };
        ws.onerror = () => (clearTimeout(timer), reject(new Error("websocket error")));
      });
    },
  },
  {
    name: "Backend sees the caller (X-Iroh-Remote-Id)",
    run: async ({ endpointId }) => {
      const who = await json(`${API}/whoami`);
      assert(who.remoteId === endpointId && who.proto === "https", JSON.stringify(who));
      return `${who.remoteId.slice(0, 10)}…`;
    },
  },
  {
    name: "HA: requests spread over both gateways of A",
    run: async () => {
      const seen = new Set<string>();
      for (let i = 0; i < 6; i++) {
        const res = await fetch(`${API}/hello`);
        await res.text();
        seen.add(res.headers.get("x-iroh-gateway-id") ?? "?");
      }
      assert(seen.size >= 2, `only ${[...seen].join(", ")}`);
      return `${seen.size} gateways`;
    },
  },
  {
    name: "Streaming response arrives in chunks",
    run: async () => {
      const res = await fetch(`${API}/stream`);
      const reader = res.body!.getReader();
      let reads = 0;
      const started = performance.now();
      while (!(await reader.read()).done) reads++;
      const ms = performance.now() - started;
      assert(reads >= 3 && ms > 900, `${reads} reads in ${Math.round(ms)} ms`);
      return `${reads} chunks over ${Math.round(ms)} ms`;
    },
  },
  {
    name: "EventSource (reconnects with Last-Event-ID)",
    run: () =>
      new Promise((resolve, reject) => {
        const es = new EventSource(`${API}/sse-once`);
        const ids: number[] = [];
        const timer = setTimeout(() => (es.close(), reject(new Error(`got ${ids.join(",")}`))), 10_000);
        es.addEventListener("tick", (e) => {
          ids.push(JSON.parse((e as MessageEvent).data).id);
          if (ids.length === 4) {
            clearTimeout(timer);
            es.close();
            ids.join() === "1,2,3,4" ? resolve(`ids ${ids.join(",")}`) : reject(new Error(ids.join(",")));
          }
        });
      }),
  },
  {
    name: "WebSocket echo on backend B",
    run: () =>
      new Promise((resolve, reject) => {
        const ws = new WebSocket(`${FILES.replace("https", "wss")}/ws`);
        const timer = setTimeout(() => (ws.close(), reject(new Error("timeout"))), 10_000);
        let hello: { backend?: string } | null = null;
        ws.onmessage = (e) => {
          if (!hello) {
            hello = JSON.parse(e.data);
            ws.send("ping-over-iroh");
          } else {
            clearTimeout(timer);
            ws.close();
            e.data === "ping-over-iroh" && hello.backend === "B" ? resolve("echo from B") : reject(new Error(String(e.data)));
          }
        };
        ws.onerror = () => (clearTimeout(timer), reject(new Error("websocket error")));
      }),
  },
  {
    name: "Streaming upload (2 MB, sha-256 checked)",
    run: async () => {
      const bytes = new Uint8Array(2 * 1024 * 1024);
      for (let i = 0; i < bytes.length; i += 65536) crypto.getRandomValues(bytes.subarray(i, i + 65536));
      const digest = await crypto.subtle.digest("SHA-256", bytes);
      const sha = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
      const data = await json(`${API}/upload`, { method: "POST", body: new Blob([bytes]) });
      assert(data.bytes === bytes.length && data.sha256 === sha, JSON.stringify(data));
      return `${(data.bytes / 1024 / 1024).toFixed(0)} MB ok`;
    },
  },
  {
    name: "XMLHttpRequest (axios's browser default) with upload progress",
    run: () =>
      new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        let progress = 0;
        xhr.open("POST", `${API}/upload`);
        xhr.responseType = "json";
        xhr.upload.onprogress = () => progress++;
        xhr.onload = () =>
          xhr.status === 200 && xhr.response.bytes === 1024 * 1024 && progress > 1
            ? resolve(`200, ${progress} progress events`)
            : reject(new Error(`${xhr.status} ${JSON.stringify(xhr.response)} progress=${progress}`));
        xhr.onerror = () => reject(new Error("xhr error"));
        xhr.send(new Blob([new Uint8Array(1024 * 1024)]));
      }),
  },
  {
    name: "<img src> from a backend (service worker)",
    run: () =>
      new Promise((resolve, reject) => {
        const img = new Image();
        img.onload = () => (img.naturalWidth === 96 ? resolve(`${img.naturalWidth}×${img.naturalHeight}`) : reject(new Error(`${img.naturalWidth}`)));
        img.onerror = () => reject(new Error("image failed to load"));
        img.src = `${FILES}/image.svg?t=${Date.now()}`;
      }),
  },
  {
    name: "Download link via proxyUrl (service worker)",
    run: async ({ client }) => {
      const url = client.proxyUrl(`${API}/download`);
      // A plain same-origin request, as a browser download or new tab would make.
      const res = await window.fetch(url);
      const text = await res.text();
      assert(res.ok && text.startsWith("report from backend A"), `${res.status} ${text}`);
      return `${res.headers.get("content-disposition")}`;
    },
  },
  {
    name: "AbortSignal.timeout cancels a slow request",
    run: async () => {
      try {
        await fetch(`${API}/slow`, { signal: AbortSignal.timeout(300) });
      } catch (e) {
        assert((e as Error).name === "TimeoutError", (e as Error).name);
        return "TimeoutError";
      }
      throw new Error("request was not aborted");
    },
  },
];
