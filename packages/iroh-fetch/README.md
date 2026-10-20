# iroh-fetch

Call backends behind NAT as if they had a public URL. Use the standard `fetch`, `EventSource` and `WebSocket`, and the
traffic goes over [iroh](https://www.iroh.computer/) to an `iroh-gateway` next to your backend. It runs in browsers and
Node.js ≥ 22.

```ts
import { createIrohFetch } from "iroh-fetch";

const iroh = await createIrohFetch({
  backends: {
    "https://api.example.com": "<gateway endpoint id>",
    "https://files.example.com": { endpointIds: ["<replica 1>", "<replica 2>"], strategy: "round-robin" },
  },
});
iroh.install(); // route these origins through iroh

const users = await (await fetch("https://api.example.com/users")).json();
const events = new EventSource("https://api.example.com/events");
const socket = new WebSocket("wss://files.example.com/ws");
```

- Full fetch semantics: redirects, cookies, streaming in both directions, `AbortSignal`
- Standard `WebSocket` and `EventSource` (reconnect, `Last-Event-ID`), plus custom headers
- Many backends from one frontend, and HA replicas with failover or round-robin
- React: `import { IrohProvider, useIroh, useIrohFetch } from "iroh-fetch/react"`
- CLI: `npx -p iroh-fetch iroh-proxy <endpoint id> --port 8080` exposes a gateway on localhost
- Uses n0's public relays by default; for your own (faster, token-protected) relay pass `relayUrl` and `relayToken`
  ([guide](https://github.com/irohkit/iroh-bridge/blob/main/deploy/relay/README.md))

Full documentation, the gateway and a live demo: <https://github.com/irohkit/iroh-bridge>
