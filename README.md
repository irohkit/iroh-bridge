# iroh-bridge

[![npm](https://img.shields.io/npm/v/iroh-fetch)](https://www.npmjs.com/package/iroh-fetch)
[![ci](https://github.com/irohkit/iroh-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/irohkit/iroh-bridge/actions/workflows/ci.yml)

Use backends that sit behind NAT as if they had a public URL. Your frontend (Next.js on Vercel, any static host, or
Node.js) keeps calling `fetch("https://api.example.com/...")`, `new EventSource(...)` and `new WebSocket(...)`;
the traffic goes over [iroh](https://www.iroh.computer/) to a small gateway next to your backend. There's no open
port, no public IP, no DNS, no TLS certificate and no tunnel service to run.

```
Browser / Node.js                      iroh relay                    backend machine (behind NAT)
┌───────────────────┐   WebSocket   ┌──────────────┐   QUIC/UDP   ┌───────────────┐  HTTP  ┌──────────┐
│ iroh-fetch (WASM) │ ────────────▶ │  n0 or yours │ ◀─────────── │ iroh-gateway  │ ─────▶ │ backends │
└───────────────────┘               └──────────────┘  (outbound)  └───────────────┘        └──────────┘
```

**Live demo:** <https://iroh-bridge.vercel.app>: press **Run self-test**. The page talks to two backends behind NAT, one
of them through two HA gateways, using only the standard `fetch` / `EventSource` / `WebSocket`.

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [Frontend: `iroh-fetch`](#frontend-iroh-fetch)
  - [Use the normal fetch, EventSource and WebSocket (`install`)](#use-the-normal-fetch-eventsource-and-websocket-install)
  - [React / Next.js](#react--nextjs)
  - [XMLHttpRequest and axios](#xmlhttprequest-and-axios)
  - [Images, video and downloads (service worker)](#images-video-and-downloads-service-worker)
  - [Stable client identity](#stable-client-identity)
  - [Several backends](#several-backends)
  - [High availability](#high-availability)
  - [fetch behaviour](#fetch-behaviour)
  - [Streaming, uploads, timeouts](#streaming-uploads-timeouts)
  - [WebSocket and EventSource](#websocket-and-eventsource)
  - [Authentication (JWT, sessions)](#authentication-jwt-sessions)
  - [Node.js](#nodejs)
  - [Options reference](#options-reference)
- [Backend: `iroh-gateway`](#backend-iroh-gateway)
  - [Install](#install)
  - [Configuration](#configuration)
  - [Several upstreams, routing by host and path](#several-upstreams-routing-by-host-and-path)
  - [Access control](#access-control)
  - [Changing routes and allow lists without a restart](#changing-routes-and-allow-lists-without-a-restart)
  - [What your backend sees](#what-your-backend-sees)
  - [Monitoring](#monitoring)
- [Local port: `iroh-proxy`](#local-port-iroh-proxy)
- [Run your own relay](#run-your-own-relay)
- [Tested](#tested)
- [Production notes](#production-notes)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Features

| | |
|---|---|
| **Works like a public URL** | `install()` routes the global `fetch`, `XMLHttpRequest`, `EventSource` and `WebSocket` for chosen origins, so existing code, axios, SWR, React Query, jQuery and friends work unchanged |
| **Images, video, downloads** | an optional service worker makes `<img>`, `<video>`, CSS `url()`, fonts and downloads of backend URLs work too |
| **Full fetch semantics** | absolute or relative URLs, all methods and headers, `res.url` / `res.redirected`, redirects (`follow` / `manual` / `error`), cookies (`credentials`), `AbortSignal` (incl. `AbortSignal.timeout`) |
| **Client cache** | opt-in HTTP cache (`cache: true`): `Cache-Control` / `Expires`, ETag and Last-Modified revalidation, `Vary`, `Request.cache` modes, in memory or the Cache API |
| **Streaming both ways** | responses stream (SSE, NDJSON, downloads); uploads of `File`, `Blob`, `FormData` or `ReadableStream` stream without buffering |
| **WebSocket** | standard API, text and binary, subprotocols, clean close codes, plus custom handshake headers (e.g. `Authorization`) |
| **EventSource** | standard API with named events, automatic reconnect and `Last-Event-ID` |
| **Auth as usual** | JWT / bearer tokens on fetch, WebSocket and EventSource; cookie sessions with a cookie jar per backend |
| **Many backends** | one frontend, any number of backends, routed by origin over one shared connection |
| **High availability** | several gateways per backend with `failover` or `round-robin`; several upstreams per gateway with health checks |
| **Gateway routing** | one gateway can serve many internal services by host and path prefix |
| **Access control** | allow-list client endpoint ids and paths, rate-limit each client at the gateway |
| **Hardened** | strict path and header validation, limits and timeouts against slow or flooding clients, no error leaks, sandboxed service; checked by attacking test gateways |
| **Trustworthy caller identity** | the backend gets `X-Iroh-Remote-Id`, the caller's cryptographically verified endpoint id |
| **Stable client identity** | optional fixed endpoint id per device or server (`persistIdentity`, `secretKey`) for allow-lists and rate limits |
| **Monitoring** | gateway `/healthz`, `/readyz`, `/status` and Prometheus `/metrics` on a local admin port |
| **React bindings** | `iroh-fetch/react`: `<IrohProvider>`, `useIroh()`, `useIrohFetch()`, `<IrohImage>`, `useIrohObjectUrl()` |
| **Runs anywhere** | browsers and Node.js ≥ 22; gateway binaries for Linux x86_64/arm64 and macOS, a Docker image, and a systemd unit |
| **Local port** | `iroh-proxy` exposes a gateway as `http://127.0.0.1:PORT` for curl, Postman or any HTTP client |

## Quick start

**1. Next to your backend**, start a gateway (it prints the endpoint id, which is your backend's address):

```bash
BACKEND_URL=http://127.0.0.1:8080 ./iroh-gateway
# IROH_ENDPOINT_ID=acc0c8d6bc20700875b5fe1da1a3379f77f28dd16f8a66e3368856473ecc7a53
```

**2. In the frontend**, map an origin of your choice to that id and keep writing normal code:

```bash
npm install iroh-fetch
```

```ts
import { createIrohFetch } from "iroh-fetch";

const iroh = await createIrohFetch({
  backends: { "https://api.example.com": "acc0c8d6bc20700875b5fe1da1a3379f77f28dd16f8a66e3368856473ecc7a53" },
});
iroh.install(); // from now on these go over iroh:

const users = await (await fetch("https://api.example.com/users")).json();
const events = new EventSource("https://api.example.com/events");
const socket = new WebSocket("wss://api.example.com/ws");
```

The origin (`https://api.example.com`) is only a name your code uses; it doesn't need to exist in DNS. Only the path and
query reach the backend.

## Frontend: `iroh-fetch`

### Use the normal fetch, EventSource and WebSocket (`install`)

```ts
const uninstall = iroh.install();                                   // the origins from `backends`
const uninstall = iroh.install({ origins: ["https://api.example.com"] }); // or explicit origins
const uninstall = iroh.install({ match: (url) => url.pathname.startsWith("/api/") }); // or any rule
```

Origins must be absolute URLs (`https://...`). Requests to other origins use the real `fetch` / `WebSocket` /
`EventSource` untouched. Pass `websocket: false` or
`eventSource: false` to leave those globals alone. Call `uninstall()` to restore them.

Without `install`, use the client directly. It has the same API:

```ts
await iroh.fetch("/users");                      // path-only URLs go to the default backend
await iroh.fetch("https://api.example.com/users");
new iroh.EventSource("https://api.example.com/events");
new iroh.WebSocket("wss://api.example.com/ws", ["chat.v1"]);
```

### React / Next.js

```tsx
"use client";
import { IrohProvider, useIroh, useIrohFetch } from "iroh-fetch/react";

export function Providers({ children }) {
  return (
    <IrohProvider backends={{ "https://api.example.com": "<endpoint id>" }} install>
      {children}
    </IrohProvider>
  );
}

function Users() {
  const iroh = useIroh(); // { status: "connecting" | "ready" | "error", client, error }
  const fetchIroh = useIrohFetch(); // waits for the connection, usable immediately
  // with `install`, plain fetch("https://api.example.com/...") works too once status is "ready"
}
```

The WASM only loads in the browser (inside an effect), so server rendering is unaffected. A full example lives in
[`examples/nextjs`](examples/nextjs): [`iroh.config.ts`](examples/nextjs/iroh.config.ts) maps origins to gateways, and
the page uses only the standard APIs. Endpoint ids are public addresses, so they can live in the repo. You can also
override them with `NEXT_PUBLIC_IROH_BACKENDS` (JSON, same shape).

### XMLHttpRequest and axios

`install()` also replaces the global `XMLHttpRequest` (turn it off with `xhr: false`), so code that uses XHR, including
**axios with its default browser adapter**, jQuery and upload widgets, needs no changes. Upload and download progress
events work:

```ts
iroh.install();
const res = await axios.post("https://api.example.com/files", file, {
  onUploadProgress: (e) => setProgress(e.loaded / e.total),
});
```

The URL is only known at `open()`, so the installed class decides there: matching URLs go over iroh, everything else uses
the browser's own XHR. Without `install`, use `new iroh.XMLHttpRequest()`. Synchronous XHR isn't supported.

### Images, video and downloads (service worker)

The browser loads `<img src>`, `<video>`, `<audio>`, CSS `url()` and fonts by itself, without going through `fetch`. To
make those work for backend URLs, register the service worker that ships with the package:

1. Serve `node_modules/iroh-fetch/src/sw.js` from your site's origin, e.g. copy it to `public/iroh-sw.js` in a
   `prebuild` script (the Next.js example does this).
2. Register it after connecting: `await iroh.serviceWorker()` (or `<IrohProvider serviceWorker>`). It resolves once
   the worker controls the page.

```tsx
<img src="https://api.example.com/avatars/42.png" />          {/* just works */}
<video src="https://files.example.com/intro.mp4" controls />   {/* Range requests pass through */}
<a href={iroh.proxyUrl("https://api.example.com/report.pdf")} download>Download</a>
```

The worker hands each request to an open page of your app, which sends it with iroh-fetch and streams the response
back. Browsers don't let a service worker catch navigations to other origins, so for downloads and "open in new tab"
links use `iroh.proxyUrl(url)`, which gives a same-origin `/__iroh__/<host>/...` URL the worker routes. Service workers
need HTTPS (or localhost).

Some browsers have no usable service worker: in-app browsers on iOS (LINE, Facebook, Instagram) and some private
modes. There `serviceWorker()` stops waiting for control after 5 s and returns `controlled: false`, and
`<IrohProvider serviceWorker>` logs a warning and carries on. `fetch`, XHR, `WebSocket` and `EventSource` keep
working; only browser-initiated loads (`<img>`, `<video>`, CSS, downloads) of backend URLs don't by themselves. For
images and media, use the fallbacks below, which load the bytes over iroh into a `blob:` URL.

In React, `<IrohImage>` takes the same props as `<img>`. It lets the service worker load the image when one controls
the page, and otherwise fetches it over iroh into a `blob:` URL, which it revokes on unmount or when `src` changes.
`useIrohObjectUrl(url)` does the fetching part for anything else, such as `<video>` or a background image:

```tsx
import { IrohImage, useIrohObjectUrl } from "iroh-fetch/react";

<IrohImage src="https://api.example.com/avatars/42.png" alt="" />  {/* worker or blob: URL, whichever works */}

function Clip({ src }: { src: string }) {
  const { url, loading, error } = useIrohObjectUrl(src); // revoked on unmount or src change
  if (error) return <p>Couldn't load the video: {error.message}</p>;
  return loading ? <p>Loading…</p> : <video src={url} controls />;
}
```

Without React, `iroh.objectUrl(url)` returns a `blob:` URL (and rejects on a non-2xx response); revoke it when done:

```ts
const url = await iroh.objectUrl("https://api.example.com/avatars/42.png");
img.src = url;
img.onload = () => URL.revokeObjectURL(url);
```

The fallback downloads the whole file before showing it, so `<video>` can't seek before it has loaded and large files
sit in memory; with a service worker, Range requests stream instead. Relative, `data:` and `blob:` sources, and URLs
of origins that aren't backends, pass through `<IrohImage>` unchanged.

### Stable client identity

Each client gets a random endpoint id per session, which the backend sees as `X-Iroh-Remote-Id`. To recognise a device
or server across restarts (for `IROH_ALLOW_CLIENTS`, rate limits or audit logs), give it a fixed one:

```ts
createIrohFetch({ persistIdentity: true, ... });           // browser: keep it in localStorage
createIrohFetch({ secretKey: process.env.IROH_CLIENT_KEY }); // server: from a secret (iroh.exportSecretKey())
```

The secret key is like a password: whoever has it can act as that client. In a browser, page scripts can read
localStorage, so an XSS bug could steal it; prefer it for servers and trusted apps.

### Several backends

```ts
const iroh = await createIrohFetch({
  backends: {
    "https://api.example.com": "<gateway A>",
    "https://files.example.com": { endpointId: "<gateway B>" },
    "https://admin.example.com": { endpointId: "<gateway C>", default: true }, // gets path-only URLs
  },
});

await iroh.fetch("https://files.example.com/report.pdf");
await iroh.backend("https://files.example.com").fetch("/report.pdf"); // scoped view, same thing
iroh.backends; // [{ origin, replicas: [{ endpointId, healthy }], connected }]
```

All backends share one local iroh endpoint (one relay connection) and get one QUIC connection each. Each backend has its
own cookie jar. A backend that is down doesn't affect the others: `createIrohFetch` only fails if none can be reached,
and requests to the unreachable one fail (and retry later) on their own.

### High availability

Run several gateways for the same backend (on different machines, each with its own key) and list them all:

```ts
backends: {
  "https://api.example.com": {
    endpointIds: ["<gateway 1>", "<gateway 2>", "<gateway 3>"],
    strategy: "round-robin", // or "failover" (default): use them in order
  },
}
// single-backend shorthand: createIrohFetch({ endpointIds: [...], strategy })
```

- A replica that can't be reached is skipped for `failoverCooldown` ms (default 10 s); it reconnects in the background
  and, with `"failover"`, becomes the preferred replica again once it's back.
- Clients notice a dead connection within ~5 s (keep-alives every second), so in-flight body-less requests move to the
  next replica instead of hanging.
- Put replicas on **different relays** (e.g. one on your relay, one on the public relays) to survive a relay outage; see
  [Redundancy](deploy/relay/README.md#7-redundancy-surviving-a-relay-outage).
- Requests without a body are retried on the next replica if one fails mid-request. Requests with a body (it streams
  and can't be replayed) only fail over while connecting, before anything was sent.
- WebSockets and EventSource connect to a healthy replica; EventSource reconnects to the next one if the stream drops.
- Each response carries `X-Iroh-Gateway-Id`, so you can see which replica answered.

On the backend side, one gateway can also spread load over several upstream processes; see
[Several upstreams](#several-upstreams-routing-by-host-and-path).

### fetch behaviour

`iroh.fetch` (and the installed global `fetch`) follow the Fetch standard:

- **URLs**: paths (`/x?y=1`), absolute `http(s)` URLs, `URL` objects or `Request`s. `res.url` is the URL you asked for
  (after redirects), and `res.redirected` is set when a redirect was followed.
- **Redirects**: `redirect: "follow"` (default, up to 20, 301/302 turn POST into GET and 303 always does),
  `"manual"` (the 3xx is returned) or `"error"`. Redirects to another origin are returned as-is instead of followed.
- **Cookies**: `Set-Cookie` from the backend is stored (Path, Max-Age and Expires are honoured) and sent back. Use
  `credentials: "omit"` to skip, `cookies: false` to disable, or `persistCookies: true` to keep them across reloads
  (localStorage, readable by page scripts, unlike real HttpOnly cookies). Inspect them with
  `iroh.cookies.get(name)`, and use `iroh.cookies.clear()` to log out locally.
- **Headers**: everything is passed through except hop-by-hop headers. Browsers can't read `Set-Cookie` from a
  `Response`, but the cookie jar can.
- **Caching**: requests over iroh bypass the browser's HTTP cache, so by default every GET crosses the relay again.
  Pass `cache: true` for a client-side HTTP cache (below).

#### HTTP cache (`cache` option)

```ts
const iroh = await createIrohFetch({ backends: { ... }, cache: true });
// or: cache: { maxEntries: 500, maxBytes: 50_000_000, storage: "memory" | "cache-api" }
```

With the option on, `GET` and `HEAD` responses are cached like a browser's private HTTP cache (for `iroh.fetch`, the
installed global `fetch` and `XMLHttpRequest`; never EventSource). A fresh hit costs no network round trip at all; a
stale entry with a validator costs one small request with an empty 304 body. With the option off nothing changes.

- **Freshness**: `Cache-Control: max-age` (else `Expires`, else not fresh) minus the response's age. `s-maxage` is for
  shared caches and ignored. There is no heuristic freshness: a response without `max-age` / `Expires` is only kept if
  it has an `ETag` or `Last-Modified`, and then revalidated on every use.
- **Directives**: `no-store` is never stored; `no-cache` is stored but always revalidated; `private` is fine (this is
  a private cache); `public` allows caching credentialed requests (next point).
- **Credentials**: requests carrying `Authorization` or cookies (their own `Cookie` header, or cookies from the jar)
  are only stored, and only answered from the cache, when the response says `public`. Once the backend sets a session
  cookie, use `credentials: "omit"` for public resources you want cached.
- **Revalidation**: stale (or `no-cache`) entries are revalidated with `If-None-Match` / `If-Modified-Since`; a `304`
  refreshes the stored headers and you get the cached body as a normal `200`. Requests where the app sets its own
  conditional or `Range` headers are sent as-is.
- **`Request.cache`**: `default`, `no-store` (bypass), `reload` (fetch and store), `no-cache` (always revalidate),
  `force-cache` (use any entry, even stale) and `only-if-cached` (any entry, else a `TypeError`; the Fetch standard
  requires `mode: "same-origin"` with it). Request `Cache-Control: no-cache` / `no-store` and `Pragma: no-cache` work too.
- **Keys**: per backend and URL (including the query), one entry per `Vary` variant; `Vary: *` is not stored. Only
  `200`, `203`, `204`, `404`, `405`, `410`, `414` and `501` responses are stored, and only once their body has been
  read to the end. A successful `POST` / `PUT` / `PATCH` / `DELETE` drops the entries for its URL (and its
  `Location` / `Content-Location`).
- **Storage**: `"memory"` (default) is per page load; `"cache-api"` keeps entries in the browser's Cache API across
  reloads (falls back to memory where there is none, such as Node or plain-http pages). The least recently used
  entries are evicted beyond `maxEntries` (500) or `maxBytes` (50 MB). `iroh.clearCache()` empties it, e.g. on logout.

The demo backend's `/cache/resource?id=…&max-age=60&etag=1` route sends these headers and `/cache/hits?id=…` counts
what really reached it; [`test/cache-e2e.test.js`](packages/iroh-fetch/test/cache-e2e.test.js) uses them.

### Streaming, uploads, timeouts

```ts
// Streaming response (SSE, NDJSON, big downloads); aborting stops the backend transfer too.
const res = await fetch("https://api.example.com/export", { signal: AbortSignal.timeout(60_000) });
for await (const chunk of res.body) process(chunk);

// Streaming upload: the file is never read into memory.
await fetch("https://api.example.com/files", { method: "POST", body: input.files[0] });

// Any ReadableStream works too.
await fetch("https://api.example.com/ingest", { method: "POST", body: someReadableStream });
```

`createIrohFetch` rejects with a `TimeoutError` after `connectTimeout` (default 15 s) when a gateway is unreachable, so
the app never hangs on "connecting".

### WebSocket and EventSource

Both follow the browser APIs (`readyState`, `onopen` / `onmessage` / `onerror` / `onclose`, `addEventListener`,
`binaryType`, `protocol`, close codes and reasons). Two extensions the browser versions lack:

```ts
// Custom handshake headers.
iroh.websocket("wss://api.example.com/ws", { protocols: ["chat.v1"], headers: { authorization: `Bearer ${token}` } });
// EventSource with headers.
new iroh.EventSource("https://api.example.com/events", { headers: { authorization: `Bearer ${token}` } });
```

Stored cookies are sent with WebSocket handshakes, like a browser does.

### Authentication (JWT, sessions)

Auth works exactly as it does against a public URL, because the backend receives the same headers:

```ts
// JWT / OAuth bearer tokens
const { token } = await (await fetch("https://api.example.com/auth/login", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ username, password }),
})).json();
const me = await fetch("https://api.example.com/auth/me", { headers: { Authorization: `Bearer ${token}` } });

// WebSocket: a header (iroh extension) or the usual ?access_token=
iroh.websocket("wss://api.example.com/ws", { headers: { Authorization: `Bearer ${token}` } });
new WebSocket(`wss://api.example.com/ws?access_token=${token}`);

// EventSource with a header (iroh extension)
new iroh.EventSource("https://api.example.com/events", { headers: { Authorization: `Bearer ${token}` } });
```

Cookie sessions work too (see [fetch behaviour](#fetch-behaviour)). The example backend
([`examples/backend`](examples/backend/server.mjs)) has HS256 JWT login, protected routes, SSE and WebSocket, and
[`test/jwt.test.js`](packages/iroh-fetch/test/jwt.test.js) checks that tampered, `alg: none` and expired tokens are
rejected.

### Node.js

The same package works in Node.js ≥ 22 (`import { createIrohFetch } from "iroh-fetch"` picks the Node build). In
serverless functions, keep the client in a module-level variable so warm invocations reuse the connection (a cold start
connects in about a second).

**Native engine: direct connections.** Browsers can't send UDP, so the WASM client always goes through a relay. In
Node.js, iroh-fetch instead loads a native addon (the same Rust client, built with napi-rs) when a prebuild for the
platform is installed: linux x64/arm64 (glibc), linux x64 (musl, e.g. Alpine), macOS x64/arm64 and Windows x64. npm
installs the matching `iroh-fetch-<platform>` package automatically as an optional dependency. The native client has
real UDP sockets, so it starts over the relay like the browser does, then hole-punches a direct path to the gateway and
moves the connection there within about a second. Where hole punching fails (e.g. both sides behind symmetric NATs), it
simply stays on the relay. On other platforms, or if the addon can't be loaded, the WASM engine is used without a word.

```ts
const iroh = await createIrohFetch({ endpointId, engine: "native" }); // "auto" (default) | "native" | "wasm"
iroh.engine;           // "native" or "wasm": what is in use
iroh.connectionInfo(); // { kind: "direct", paths: [{ kind: "direct", addr: "203.0.113.7:51820", rttMs: 3.1, ... }, ...] }
```

`engine: "native"` fails if the addon can't be loaded instead of falling back; the `IROH_FETCH_BACKEND=native|wasm`
environment variable does the same without code changes (the option wins over it).

Measured against one demo gateway with [`scripts/bench-engines.mjs`](scripts/bench-engines.mjs) (Node 24, 500
sequential `GET /hello`, 30 MB streamed `POST /upload`, own relay on a VPS ~20 ms away). **Note:** the gateway ran on the
same machine as the client, so the native engine's direct path here is a host-local hop (RTT ~0.2 ms); across the internet
expect the network's RTT instead of the relay detour, not these absolute numbers.

| Engine | Path | First response | Latency p50 | p99 | Upload |
|---|---|---|---|---|---|
| native | direct (host-local) | 75 ms | 0.56 ms | 1.5–2.1 ms | 210–230 MB/s |
| WASM | relay | 90–165 ms | 14 ms | 16 ms | 9.2 MB/s |

### Options reference

| Option | Default | |
|---|---|---|
| `endpointId` / `endpointIds` | | one backend: a gateway id, or several replicas |
| `backends` | | `{ [origin]: id \| id[] \| { endpointId?, endpointIds?, strategy?, default? } }` |
| `strategy` | `"failover"` | `"failover"` or `"round-robin"` over replicas |
| `failoverCooldown` | `10000` | ms an unreachable replica is skipped |
| `connect` | `true` | dial all gateways up front (otherwise on first use) |
| `connectTimeout` | `15000` | ms to wait for a gateway before `TimeoutError` (`0` = forever) |
| `cookies` | `true` | keep and send cookies |
| `persistCookies` | `false` | `true` (localStorage) or a `Storage` |
| `cache` | `false` | client HTTP cache: `true` or `{ maxEntries?: 500, maxBytes?: 50e6, storage?: "memory" \| "cache-api" }` ([details](#http-cache-cache-option)) |
| `relayUrl` | n0 public relays | your own relay ([guide](deploy/relay/README.md)) |
| `relayToken` | | token for a relay that requires one |
| `secretKey` | random | fixed client identity (64 hex chars) |
| `persistIdentity` | `false` | keep the identity in localStorage (`true`) or a `Storage` |
| `logLevel` | off | `"info"`, `"debug"`, ... iroh logs in the console |
| `engine` | `"auto"` | Node.js only: `"native"` (direct UDP paths) or `"wasm"` (always via relay); see [Node.js](#nodejs) |

The client object: `fetch`, `XMLHttpRequest`, `EventSource`, `WebSocket`, `websocket(url, { protocols, headers })`,
`install(opts)`, `serviceWorker(opts)`, `proxyUrl(url)`, `objectUrl(url, init)`, `backend(origin)`, `backends`, `cookies`, `endpointId` (this
client's id, which the backend sees as `X-Iroh-Remote-Id`), `exportSecretKey()`, `clearCache()`, `close()`, and in
Node.js `engine` and `connectionInfo(endpointId?)`.

## Backend: `iroh-gateway`

### Install

- **Binary**: download it for Linux x86_64/arm64 or macOS arm64 from the
  [releases](https://github.com/irohkit/iroh-bridge/releases), or `cargo build --release -p iroh-gateway`.
- **systemd** (starts on boot, its own user, key in `/var/lib/iroh-gateway`):
  ```bash
  sudo ./deploy/install.sh http://127.0.0.1:8080
  ```
- **Docker**: `ghcr.io/irohkit/iroh-gateway`; see [`deploy/docker`](deploy/docker) for `docker run` and a compose file.

The gateway prints `IROH_ENDPOINT_ID=...` at startup (`journalctl -u iroh-gateway | grep IROH_ENDPOINT_ID`). The id stays
the same as long as you keep the key file. Keep that file secret: whoever has it can impersonate your backend. It only
needs outbound UDP/HTTPS, and no inbound port.

### Configuration

| Variable / flag | Default | |
|---|---|---|
| `BACKEND_URL` / `--backend` | `http://127.0.0.1:8080` | upstream(s), comma-separated |
| `IROH_ROUTES` / `--route` | | host/path routing, see below |
| `IROH_ALLOW_CLIENTS` / `--allow-client` | everyone | allowed client endpoint ids |
| `IROH_ALLOW_PATHS` / `--allow-path` | everything | allowed path prefixes |
| `IROH_RATE_LIMIT` / `--rate-limit` | off | requests per second per client endpoint id, 429 beyond ([below](#rate-limits-per-client)) |
| `IROH_RATE_BURST` / `--rate-burst` | 2 × rate, at least 1 | requests a client may make at once before the rate applies |
| `IROH_MAX_CONNECTIONS_PER_CLIENT` / `--max-connections-per-client` | off | open connections per client endpoint id |
| `IROH_RATE_LIMIT_EXEMPT` / `--rate-limit-exempt` | | endpoint ids the per-client limits don't apply to |
| `IROH_KEY_FILE` / `--key-file` | `iroh-gateway.key` | secret key (created on first run) |
| `IROH_RELAY_URL` / `--relay-url` | n0 public relays | your own relay ([guide](deploy/relay/README.md)) |
| `IROH_RELAY_TOKEN` / `--relay-token` | | token for a relay that requires one |
| `MAX_BODY_BYTES` / `--max-body` | 32 MiB | request body limit |
| `UPSTREAM_COOLDOWN_SECS` | `10` | how long an unreachable upstream is skipped |
| `RUST_LOG` | `iroh_gateway=info` | log level. Logs go to stdout through a buffer: if the log reader stalls, lines are dropped instead of blocking requests |
| `IROH_ADMIN_ADDR` / `--admin-addr` | off | local admin port for health and metrics ([Monitoring](#monitoring)) |
| `IROH_CONFIG_FILE` / `--config-file` | | file with settings that reload without a restart ([below](#changing-routes-and-allow-lists-without-a-restart)) |

Limits and timeouts, all with flags of the same name (`--max-head`, `--head-timeout-secs`, ...). The defaults are generous
for normal use and stop a single client from tying the gateway up:

| Variable | Default | |
|---|---|---|
| `MAX_HEAD_BYTES` | 64 KiB | request line + headers |
| `MAX_WS_MESSAGE_BYTES` | 16 MiB | one WebSocket message (the backend gets 1009 when exceeded) |
| `IROH_MAX_CONNECTIONS` | 4096 | concurrent client connections |
| `IROH_MAX_STREAMS_PER_CONNECTION` | 100 | concurrent requests per client |
| `IROH_MAX_REQUESTS` | 8192 | concurrent requests overall (503 beyond) |
| `HEAD_TIMEOUT_SECS` | 10 | time to send the request head (408) |
| `BODY_IDLE_TIMEOUT_SECS` | 60 | an upload that stalls this long is dropped (408) |
| `UPSTREAM_CONNECT_TIMEOUT_SECS` | 10 | connecting to an upstream |
| `UPSTREAM_TIMEOUT_SECS` | 300 | waiting for the upstream's response head (504; `0` = off). Streaming responses such as SSE are never cut once they started |

### Several upstreams, routing by host and path

```bash
# Spread over several backend processes; ones that refuse connections are skipped for a while.
BACKEND_URL=http://10.0.0.5:8080,http://10.0.0.6:8080 iroh-gateway

# One gateway in front of several services. Most specific match wins; BACKEND_URL is the fallback.
IROH_ROUTES='api.example.com=http://127.0.0.1:8080;files.example.com=http://10.0.0.7:9000,http://10.0.0.8:9000;/admin=http://127.0.0.1:9100' iroh-gateway
```

The host is the one your frontend used in the URL (`iroh-fetch` sends it as `x-iroh-host`), so a single gateway id can
back several origins in `backends`. Path prefixes match whole segments and are not stripped.

### Access control

```bash
IROH_ALLOW_CLIENTS=<endpoint id>,<endpoint id>   # refuse other clients at connection time
IROH_ALLOW_PATHS=/api,/ws                        # everything else gets 403
```

Client endpoint ids are random per browser session unless you give the client a fixed key, so the client allow-list is
mostly useful for server-to-server use (Node, `iroh-proxy`). For browser users, authenticate in your backend as you would
on a public URL (sessions, tokens). `X-Iroh-Remote-Id` gives you a verified id to rate-limit or audit by.

#### Rate limits per client

Anyone who knows the gateway's endpoint id can call it, like a public URL. Besides the global limits, the gateway can
limit each client, keyed by the same verified endpoint id the backend sees as `X-Iroh-Remote-Id`:

```bash
IROH_RATE_LIMIT=10                     # sustained requests per second per client (fractions like 0.5 work)
IROH_RATE_BURST=20                     # how many may come at once (default: twice the rate, at least 1)
IROH_MAX_CONNECTIONS_PER_CLIENT=4      # further connections from the same id are refused
IROH_RATE_LIMIT_EXEMPT=<endpoint id>   # e.g. your own servers; comma-separated
```

- It is a token bucket per client: a client may send `IROH_RATE_BURST` requests at once, then `IROH_RATE_LIMIT` per
  second. A WebSocket upgrade counts as one request; messages on an open WebSocket don't count.
- A request over the limit gets `429 Too Many Requests` with `retry-after` (seconds) straight from the gateway, before
  its head is even read, so it costs no request slot and never reaches the backend. It is counted as
  `iroh_gateway_rejected_total{reason="rate_limited"}`, and a refused connection as `reason="client_connection_limit"`.
  The log shows at most one line per 10 seconds for each, so a flood can't flood the log.
- Memory stays bounded. A bucket that has refilled completely holds no information and is dropped (swept every
  minute), and at most 16384 clients are tracked (`iroh_gateway_rate_limit_tracked_clients`). When the table is full
  of clients that are all still being limited, new clients share a single overflow bucket instead of pushing anyone
  out: endpoint ids cost nothing to make, so evicting would let a flood of fresh ids reset the limits of the clients
  being throttled. A flood of new ids then gets one client's allowance in total, and clients already tracked are
  unaffected, at the cost of genuinely new clients being throttled together until room frees up.
- Per-id limits stop a single misbehaving client (a buggy retry loop, a scraper), not someone who generates new ids;
  the global limits (`IROH_MAX_CONNECTIONS`, `IROH_MAX_REQUESTS`) and `IROH_ALLOW_CLIENTS` remain the defence against
  that. Browser clients get a new id per session unless they use a fixed key.
- All four settings reload with the config file below. A lower connection limit applies to new connections; open ones
  stay up.

### Changing routes and allow lists without a restart

Put the settings you want to change at runtime in a file and point `IROH_CONFIG_FILE` at it. It uses the systemd
`EnvironmentFile` format, so the same file can hold the gateway's other variables too (only these keys and the
[per-client rate limits](#rate-limits-per-client) are reloaded):

```bash
# /etc/iroh-gateway.env
BACKEND_URL=http://127.0.0.1:8080
IROH_ROUTES="api.example.com=http://127.0.0.1:8080;/admin=http://127.0.0.1:9100"
IROH_ALLOW_CLIENTS=
IROH_ALLOW_PATHS=
IROH_RATE_LIMIT=10
IROH_RATE_BURST=
IROH_MAX_CONNECTIONS_PER_CLIENT=
IROH_RATE_LIMIT_EXEMPT=
```

Edit it, then send `SIGHUP`: `systemctl reload iroh-gateway` (the shipped unit has `ExecReload`), or
`docker kill -s HUP <container>`. Open connections stay up, requests in flight finish on the config they started
with, and new requests use the new one. A client removed from `IROH_ALLOW_CLIENTS` is disconnected on its next request.
A file with an error is rejected as a whole; the gateway logs why and keeps running the previous config. Values in the
file override flags and environment (an empty value means unset); a key left out of the file falls back to them. Upstream health and per-route
counters start fresh for the reloaded routes; `iroh_gateway_config_reloads_total{result="ok"|"error"}` counts reloads.

### Monitoring

Start the gateway with a local admin port:

```bash
IROH_ADMIN_ADDR=127.0.0.1:9180 iroh-gateway
```

| Endpoint | |
|---|---|
| `GET /healthz` | `200 ok` once the gateway is online on its relay (for systemd/Docker/Kubernetes health checks) |
| `GET /readyz` | `200` when every route has a healthy upstream, else `503` listing the unhealthy ones |
| `GET /status` | JSON: endpoint id, version, uptime, relay status, connections, in-flight requests, rate limits, upstream health |
| `GET /metrics` | Prometheus: requests by route and status class, WebSocket upgrades, upstream failures and health, rejections by reason (including `rate_limited`), connections, in-flight requests, rate-limited clients tracked, request duration histogram |

It only binds loopback addresses unless you add `--admin-allow-public` (it has no authentication). Labels never contain
request paths or queries.

### Security

The gateway's endpoint id is public, so the gateway treats every client as untrusted:

- Paths must be clean, printable ASCII starting with `/`. Dot segments in any encoding (`..`, `%2e%2e`, `..;`, tabs or
  newlines inside them) are rejected, so routing and `--allow-path` can't be bypassed, and requests can't leave the
  configured upstream.
- Header names and values with control characters, `CONNECT` and `TRACE` are rejected. Hop-by-hop headers and every
  header the gateway sets itself (`X-Iroh-*`, `X-Forwarded-*`, `X-Real-IP`) are stripped from clients.
- Clients only ever see generic errors. Upstream URLs and OS errors go to the gateway log.
- Slow or oversized requests are cut off by the limits above, so one client can't starve others.
- The key file is created with mode `0600`, and the gateway refuses to start with a world-readable key.
- The systemd unit runs it with no capabilities and a tight sandbox (see [`deploy/iroh-gateway.service`](deploy/iroh-gateway.service)).

These were checked by attacking test gateways with a raw iroh client (smuggling, path tricks, slow-loris, floods, oversized
WebSocket frames, error leaks).

### What your backend sees

A normal HTTP request: same method, path, query, headers and body, plus:

- `X-Iroh-Remote-Id`: the caller's endpoint id, verified by the QUIC handshake (clients can't spoof it)
- `X-Forwarded-Proto: https`, because the tunnel is end-to-end encrypted
- `X-Forwarded-Host`: the host the frontend used (e.g. `api.example.com`)

WebSockets are proxied to `ws://` (or `wss://`) on the same upstream.

## Local port: `iroh-proxy`

Expose a gateway as a local URL, like `ssh -L`, for curl, Postman, scripts, or a dev server's proxy setting:

```bash
npx -p iroh-fetch iroh-proxy <endpoint id>[,<replica id>...] --port 8080
curl http://127.0.0.1:8080/users
```

HTTP (streaming both ways), redirects and cookies are passed through untouched, and WebSockets work too
(`ws://127.0.0.1:8080/ws`). It listens on `127.0.0.1` only by default, and to stop other websites from using it through
your browser (DNS rebinding, cross-site requests) it only accepts `Host: localhost`/IP and same-origin `Origin` headers.
Add exceptions with `--allow-host` and `--allow-origin`.

## Run your own relay

**Default: n0's public relays.** If you set no relay anywhere, gateways, clients and `iroh-proxy` all use them, and
everything works out of the box.

Browsers can't send UDP, so every browser request passes through a relay, and your own relay close to your users is the
biggest speed-up you can get. On the demo it cut request latency from 69 ms to 14 ms and raised uploads from 1.3 to
11.5 MB/s. It can also be limited to clients holding a token.

**[Full guide: deploy/relay/README.md](deploy/relay/README.md)** covers installing on a fresh server
(`sudo ./deploy/relay/install.sh relay.example.com you@example.com`) or next to an existing Caddy/nginx/Traefik,
creating and rotating tokens, and troubleshooting. In short, once it runs:

```bash
IROH_RELAY_URL=https://relay.example.com IROH_RELAY_TOKEN=<token, if required> iroh-gateway
```

```ts
createIrohFetch({ relayUrl: "https://relay.example.com", relayToken: "<token, if required>", backends: { ... } });
```

Leave both unset to stay on the public relays.

## Tested

- **Browsers:** the demo's 17-check self-test passes in all of these ([`scripts/browser-test.py`](scripts/browser-test.py),
  Playwright):

  | | |
  |---|---|
  | Desktop | Google Chrome 155, Microsoft Edge 155, Chromium 156 (the engine of Brave, Opera, Vivaldi), Firefox 157, WebKit 27 (Safari's engine) |
  | Mobile (emulated) | iPhone 15 and iPad Pro 11 (Safari), Pixel 7 (Chrome), Galaxy (Samsung Internet), Facebook in-app on Android |
  | No service worker | LINE in-app on iPhone, Firefox with service workers blocked: 15/15, the 2 service-worker checks skipped |

  Browser-specific bugs found and fixed this way: Firefox has no `Request.body` stream; WebKit rejects relay hostnames
  ending in a dot, for WebSockets and for the relay latency probes; and apps never became ready where service workers
  can't take control. Running the matrix repeatedly also showed that a standby gateway on another relay sometimes took
  15 s+ to dial. Clients now add a plain address-lookup dial after 1.5 s, and replicas that failed keep retrying in the
  background with backoff, so standbys are connected before they're needed.
- **Load** ([`scripts/loadtest.mjs`](scripts/loadtest.mjs), relay on a 2-core VPS): 0 errors at 1,000 req/s from 100
  clients, 100 concurrent WebSockets and 50 clients of mixed traffic, with ~13 ms latency throughout. The gateways used
  ~15% of a core at 500 req/s each; the relay's TLS front end (Caddy) is what saturates first (~58% CPU at 1,000 req/s).
  Opening a connection (page load to first response) takes ~85 ms p50 with your own relay configured.
- **Relay outage:** see [Redundancy](deploy/relay/README.md#7-redundancy-surviving-a-relay-outage).
- **Security:** see [Security](#security).

## Production notes

- **Run your own relay**: n0's public relays are fine for development but have no SLA and may be rate limited. See
  [Run your own relay](#run-your-own-relay), or use n0's hosted relay service.
- **Security**: traffic is end-to-end encrypted with QUIC/TLS, and the relay can't read it. Anyone who knows an endpoint id
  can reach that gateway, just like a public URL, so keep authenticating in your backend, and use the gateway's allow-lists
  where they fit.
- **Latency and throughput**: browsers can't send UDP, so the browser side always goes through a relay. Through n0's public
  relays we measured about 70–120 ms per request and 1–3.5 MB/s for uploads. A relay close to your users does better.
  Node.js on a platform with a prebuild uses the [native engine](#nodejs) and usually connects directly (UDP must be
  allowed outbound), relying on the relay only to set the connection up.
- **WASM size**: ~3.4 MB (~1.4 MB gzipped), cached after the first load. Load it lazily (the React provider does).

## Troubleshooting

- **`TimeoutError: could not reach gateway`**: the gateway is offline, or the endpoint id is wrong or old. Check its log for
  the current `IROH_ENDPOINT_ID`.
- **`no backend configured for https://...`**: the URL's origin isn't in `backends`.
- **Node.js: `address lookup ... fetch failed` / `ETIMEDOUT`**: usually a machine whose IPv6 is configured but doesn't
  actually work, so Node's `fetch` stalls on IPv6. Run Node with `NODE_OPTIONS=--dns-result-order=ipv4first`.

## Development

```
crates/gateway     iroh-gateway (Rust)
crates/client-core the iroh client itself (connections, dialing, streaming), shared by:
crates/web-client  its WASM build (browsers, and Node.js without the native addon)
crates/node-client its native Node.js addon (napi-rs)
crates/proto       wire format: one request or WebSocket per QUIC stream
packages/iroh-fetch npm package (JS API, React bindings, iroh-proxy CLI, tests)
examples/nextjs    demo app with a self-test (deployed to Vercel)
examples/backend   mock backend used by the demo and tests
deploy/            systemd unit, install script, Docker, relay setup
```

Build the WASM (needs Rust with `wasm32-unknown-unknown`, `clang`, `wasm-bindgen-cli` matching `Cargo.lock`, optionally
`wasm-opt`):

```bash
./scripts/build-wasm.sh
```

Build the native addon for this machine into `packages/iroh-fetch/native/`, where iroh-fetch looks before the prebuild
packages (plain `cargo`, no `@napi-rs/cli`; pass a Rust target and platform tag to cross-build):

```bash
./scripts/build-native.sh
```

Run the end-to-end tests against running gateways in front of `examples/backend` (A on two gateways, B on one), the
browser self-test, and the load test:

```bash
cd packages/iroh-fetch
IROH_ENDPOINT_ID=<A1> IROH_ENDPOINT_ID_A2=<A2> IROH_ENDPOINT_ID_B=<B> npm test
# add IROH_RELAY_URL / IROH_RELAY_TOKEN when the gateways use your own relay;
# IROH_FETCH_BACKEND=native or =wasm runs the suite on one engine (CI runs both)
python3 scripts/browser-test.py https://your-demo.example.com/
node scripts/loadtest.mjs --help
```

**Releasing**: bump `packages/iroh-fetch/package.json`, commit, and push a matching tag (`git tag v0.2.0 && git push origin v0.2.0`).
CI then publishes to npm (trusted publishing, no token in the repo): first the six `iroh-fetch-<platform>` native addon
packages built by `native-prebuilds.yml`, then `iroh-fetch` pinning them as optional dependencies. It also attaches gateway binaries to the GitHub release
and pushes the Docker image.

## License

MIT
