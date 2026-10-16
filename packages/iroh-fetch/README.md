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

Response bodies stream (SSE, NDJSON, large downloads) and `init.signal` cancels them.

WebSockets use the standard `WebSocket` API, plus custom handshake headers:

```ts
const ws = iroh.websocket("/ws", { protocols: ["chat.v1"], headers: { authorization: "Bearer ..." } });
ws.onmessage = (e) => console.log(e.data);
```

Options: `endpointId` (required), `relayUrl` (when the gateway uses a custom relay), `logLevel`, `connect` (default `true`).
