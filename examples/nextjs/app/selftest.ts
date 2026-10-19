// Checks run by the "Run self-test" button. They use only the standard browser APIs (fetch,
// EventSource, WebSocket), which iroh-fetch routes over iroh for the configured origins.
import { API, FILES } from "../iroh.config";

export type Check = { name: string; run: (ctx: { endpointId: string }) => Promise<string> };

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
