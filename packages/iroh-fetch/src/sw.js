// iroh-fetch service worker. Serve this file from your site's origin (e.g. copy it to
// public/iroh-sw.js) and register it with iroh.serviceWorker(). It lets the browser's own loads
// (<img>, <video>, <audio>, CSS url(), fonts, downloads, links opened in a new tab) reach your iroh
// backends: matching requests are handed to an open page of your app, which sends them with
// iroh-fetch and streams the response back.
//
// Configuration comes from the registration URL:  /iroh-sw.js?hosts=api.example.com,files.example.com
// Same-origin URLs under /__iroh__/<host>/<path> are routed too (see iroh.proxyUrl()), which is
// how downloads and new-tab links work: browsers don't let a service worker catch navigations to
// other origins.

const params = new URL(self.location.href).searchParams;
const HOSTS = new Set((params.get("hosts") ?? "").split(",").filter(Boolean));
const PREFIX = new URL("__iroh__/", self.registration.scope).pathname;
const TIMEOUT_MS = 30_000;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
  const target = irohTarget(new URL(event.request.url));
  if (target) event.respondWith(bridge(event, target));
});

/** The backend URL a request maps to, or null if it isn't an iroh request. */
function irohTarget(url) {
  if (HOSTS.has(url.host)) return url.href;
  if (url.origin === self.location.origin && url.pathname.startsWith(PREFIX)) {
    const rest = url.pathname.slice(PREFIX.length);
    const slash = rest.indexOf("/");
    const host = slash === -1 ? rest : rest.slice(0, slash);
    if (!HOSTS.has(host)) return null;
    return `https://${host}${slash === -1 ? "/" : rest.slice(slash)}${url.search}`;
  }
  return null;
}

async function bridge(event, target) {
  const page = await findPage(event.clientId);
  if (!page) return new Response("iroh-fetch: open the app to load this resource", { status: 503 });

  const req = event.request;
  const body = req.method === "GET" || req.method === "HEAD" ? undefined : await req.arrayBuffer();
  const { port1, port2 } = new MessageChannel();
  page.postMessage(
    {
      type: "iroh-fetch:request",
      url: target,
      method: req.method,
      headers: [...req.headers],
      body,
      redirect: req.mode === "navigate" ? "follow" : req.redirect,
    },
    body ? [port2, body] : [port2],
  );

  // The page answers with the head, then the body in chunks, over the port.
  return new Promise((resolve) => {
    let controller;
    const timer = setTimeout(() => fail("timeout"), TIMEOUT_MS);
    const fail = (why) => {
      clearTimeout(timer);
      port1.close();
      if (controller) controller.error(new Error(why));
      else resolve(new Response(`iroh-fetch: ${why}`, { status: 502 }));
    };
    port1.onmessage = ({ data }) => {
      if (data.type === "head") {
        clearTimeout(timer);
        const stream = new ReadableStream({
          start: (c) => (controller = c),
          pull: () => port1.postMessage({ type: "pull" }),
          cancel: () => {
            port1.postMessage({ type: "cancel" });
            port1.close();
          },
        });
        const nullBody = data.status === 204 || data.status === 304 || req.method === "HEAD";
        resolve(new Response(nullBody ? null : stream, { status: data.status, statusText: data.statusText, headers: data.headers }));
      } else if (data.type === "chunk") {
        controller?.enqueue(new Uint8Array(data.chunk));
      } else if (data.type === "end") {
        controller?.close();
        port1.close();
      } else if (data.type === "error") {
        fail(data.message);
      }
    };
    port1.start();
  });
}

/** The page that made the request if it can serve it, otherwise any open window of the app. */
async function findPage(clientId) {
  if (clientId) {
    const own = await self.clients.get(clientId);
    if (own) return own;
  }
  const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  return windows.find((w) => w.focused) ?? windows[0] ?? null;
}
