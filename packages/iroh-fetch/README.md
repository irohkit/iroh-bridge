# iroh-fetch

`fetch()` over [iroh](https://www.iroh.computer/): call an HTTP backend that sits behind any NAT, from browsers and Node.js >= 22.
The backend side runs `iroh-gateway` (see the repository root).

```ts
import { createIrohFetch } from "iroh-fetch";

const iroh = await createIrohFetch({ endpointId: "<id printed by iroh-gateway>" });
const res = await iroh.fetch("/api/users");
console.log(await res.json());
await iroh.close();
```

Bodies stream both ways: responses (SSE, NDJSON, large downloads) and uploads (`File`, `Blob`, `ReadableStream`).
`init.signal` aborts at any point, so `AbortSignal.timeout(ms)` works, and `createIrohFetch` gives up after
`connectTimeout` (default 15 s) with a `TimeoutError` when the gateway is unreachable.

WebSockets use the standard `WebSocket` API, plus custom handshake headers:

```ts
const ws = iroh.websocket("/ws", { protocols: ["chat.v1"], headers: { authorization: "Bearer ..." } });
ws.onmessage = (e) => console.log(e.data);
```

Options: `endpointId` (required), `relayUrl` (when the gateway uses a custom relay), `logLevel`, `connect` (default `true`),
`connectTimeout` (ms, default `15000`).
