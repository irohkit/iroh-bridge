# iroh-bridge

Let a frontend (Next.js on Vercel or any other host) and Node.js reach an HTTP backend that sits behind NAT —
no open ports, no public IP and no tunnel service like ngrok. Everything runs on [iroh](https://www.iroh.computer/).

```
Browser / Node.js                     iroh relay                    backend machine (behind NAT)
┌──────────────────┐   WebSocket   ┌──────────────┐   QUIC/UDP   ┌───────────────┐  HTTP  ┌─────────┐
│ iroh-fetch (WASM)│ ────────────▶ │  n0 or yours │ ◀─────────── │ iroh-gateway  │ ─────▶ │ backend │
└──────────────────┘               └──────────────┘  (outbound)  └───────────────┘        └─────────┘
```

- **`crates/gateway`** (`iroh-gateway`): a Rust program that runs next to your existing backend, accepts requests over iroh
  and forwards them to `BACKEND_URL`. All of its connections are outbound, so it works behind any NAT, CGNAT or firewall.
- **`packages/iroh-fetch`**: an npm package (Rust → WASM) that gives you a standard-looking `fetch()` and `WebSocket`,
  in browsers and in Node.js ≥ 22.
- **`crates/proto`**: the shared wire format. One request (or one WebSocket) per QUIC stream; many run concurrently on one connection.
- **`examples/nextjs`**: an example app (provider + `useIroh()` hook) with HTTP, SSE and WebSocket demos.
- **`examples/backend`**: a mock backend to try things with (JSON echo, `/stream`, `/sse`, WebSocket echo on `/ws`).

> Browsers can't send UDP, so the client side always goes through a relay. Traffic is end-to-end encrypted; the relay
> can't read it. Through n0's public relays we measured roughly 70–120 ms per request once connected.

## Getting started

### 1. Backend side: run the gateway

```bash
cargo build --release -p iroh-gateway
```

```bash
BACKEND_URL=http://127.0.0.1:8080 ./target/release/iroh-gateway
```

On startup it prints `IROH_ENDPOINT_ID=...`, which is your backend's "address". It stays the same as long as you keep
`iroh-gateway.key`. Keep that file secret: anyone who has it can impersonate your backend.

| Variable | Default | Meaning |
|---|---|---|
| `BACKEND_URL` | `http://127.0.0.1:8080` | HTTP backend that requests are forwarded to |
| `IROH_KEY_FILE` | `iroh-gateway.key` | The endpoint's secret key file |
| `IROH_RELAY_URL` | (n0 relays) | Use your own relay server |
| `MAX_BODY_BYTES` | 32 MiB | Maximum request body size |

Example systemd unit to keep it running:

```ini
[Service]
Environment=BACKEND_URL=http://127.0.0.1:8080
Environment=IROH_KEY_FILE=/var/lib/iroh-gateway/key
ExecStart=/usr/local/bin/iroh-gateway
Restart=always
```

### 2. Frontend side: use `iroh-fetch`

```bash
npm install iroh-fetch
```

```ts
import { createIrohFetch } from "iroh-fetch";

const iroh = await createIrohFetch({ endpointId: process.env.NEXT_PUBLIC_IROH_ENDPOINT_ID! });
const res = await iroh.fetch("/api/users?page=2"); // a standard Response
const users = await res.json();
await iroh.fetch("/api/users", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ name: "a" }),
});
```

**Streaming responses** (SSE, NDJSON, large files): the body arrives chunk by chunk as the backend sends it, and an
`AbortController` cancels it (the gateway stops pulling from the backend right away).

```ts
const ctrl = new AbortController();
const res = await iroh.fetch("/api/events", { signal: ctrl.signal });
const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  console.log(value);
}
// call ctrl.abort() to stop
```

**WebSocket**: returns an object with the standard `WebSocket` API (`onopen`, `onmessage`, `send`, `close`, `binaryType`,
`protocol`, `addEventListener`). The gateway opens the real WebSocket to `ws://<BACKEND_URL>/<path>`.

```ts
const ws = iroh.websocket("/ws", {
  protocols: ["chat.v1"],
  headers: { authorization: `Bearer ${token}` }, // something a browser's WebSocket can't do
});
ws.onmessage = (e) => console.log(e.data);
ws.onopen = () => ws.send("hello");
```

In Next.js, create the client once in the browser; see [`examples/nextjs/lib/iroh.tsx`](examples/nextjs/lib/iroh.tsx)
(it imports the package dynamically inside `useEffect` so the WASM never loads during SSR).

```bash
cd examples/nextjs && cp .env.example .env.local   # set NEXT_PUBLIC_IROH_ENDPOINT_ID
npm install && npm run dev
```

### 3. Deploy to Vercel

The app needs no server of its own, so it can be a static export or run on Vercel, Netlify or Cloudflare Pages.
Set `NEXT_PUBLIC_IROH_ENDPOINT_ID` in the project's environment variables. To deploy the example straight from this
repository, set the Root Directory to `examples/nextjs`; the prebuilt WASM in `packages/iroh-fetch/wasm/` is committed.

**From Node.js (API routes / servers)**: works on Node ≥ 22, but in serverless functions every cold start has to connect
again (~1 s). If you call it often from the server, keep the client in a module-level variable so it is reused, or run on a
long-lived server.

## Building the package

You need Rust with the `wasm32-unknown-unknown` target, `clang` (for the `ring` crate), `wasm-bindgen-cli` at the same
version as in `Cargo.lock`, and optionally `wasm-opt`.

```bash
./scripts/build-wasm.sh
```

End-to-end tests (start `examples/backend` and the gateway first; they cover HTTP, streaming, SSE abort and WebSocket):

```bash
cd packages/iroh-fetch && IROH_ENDPOINT_ID=<id> npm test
```

## Releasing to npm

The [`publish-npm`](.github/workflows/publish-npm.yml) workflow builds the WASM and publishes `packages/iroh-fetch`
when you push a tag like `v0.1.0`. It needs an `NPM_TOKEN` repository secret (an npm *automation* or granular access token
with publish rights).

```bash
git tag v0.1.0 && git push origin v0.1.0
```

## Production notes

- **Run your own relay**: n0's public relays are fine for development but have no SLA and may be rate limited.
  Run [`iroh-relay`](https://docs.iroh.computer/) on a VPS with a domain and TLS, then set the same URL as
  `IROH_RELAY_URL` on the gateway and `relayUrl` on the client — or use n0's hosted relay service.
- **Security**: anyone who knows the endpoint id can send requests to your backend (just like a public URL), so the
  backend must do its own auth (for example with the `Authorization` header, which the gateway forwards).
  Traffic is end-to-end encrypted with QUIC/TLS.
- **WASM size**: ~3.4 MB (~1.4 MB gzipped). Browsers cache it after the first load; load it lazily as the example does.
- **Current limits**: request bodies (uploads) are buffered before sending, up to `MAX_BODY_BYTES`; response bodies stream.

## License

MIT
