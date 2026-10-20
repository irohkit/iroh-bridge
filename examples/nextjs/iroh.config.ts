import type { IrohFetchOptions } from "iroh-fetch";

/**
 * Which gateway serves which origin. The app code below just calls fetch("https://api.demo/..."),
 * new EventSource(...) and new WebSocket(...) as usual; iroh-fetch routes these origins over iroh.
 *
 * Endpoint ids are public addresses (like hostnames), so they can live in the repo. Override them
 * without a code change by setting NEXT_PUBLIC_IROH_BACKENDS to the same shape as JSON.
 */
// Each backend has two gateways: the first on the demo's own (fast) relay, the second on n0's public
// relays. "failover" uses the first and moves to the second if the own relay or that gateway goes
// down, then comes back once it recovers.
const demoBackends: IrohFetchOptions["backends"] = {
  "https://api.demo": {
    endpointIds: [
      "acc0c8d6bc20700875b5fe1da1a3379f77f28dd16f8a66e3368856473ecc7a53",
      "12cc92042d6d8048cc6ded53115b1b85c5ed1b08d2ff893695bac33228224558",
    ],
    strategy: "failover",
  },
  "https://files.demo": {
    endpointIds: [
      "61cf27b0bc13636193a9d64544acc08a1f7cf264801be90bfc88b6a10a5ec1f2",
      "b436bfe3c115b845eede894d278fc65b36922db65eb852da16019fd0d971a931",
    ],
    strategy: "failover",
  },
};

export const backends: IrohFetchOptions["backends"] = process.env.NEXT_PUBLIC_IROH_BACKENDS
  ? JSON.parse(process.env.NEXT_PUBLIC_IROH_BACKENDS)
  : demoBackends;

// Relay: n0's public relays unless you set your own (see deploy/relay/README.md). Set the same relay
// the gateways use.
export const relayUrl = process.env.NEXT_PUBLIC_IROH_RELAY_URL || undefined;

// If that relay requires a token, set IROH_RELAY_TOKEN (server-only): app/api/relay-token serves it
// at runtime, so it isn't in the bundle and rotating it needs no redeploy. NEXT_PUBLIC_IROH_RELAY_TOKEN
// is the older fallback; it ends up in the page source, where anyone can read it.
const bundledRelayToken = process.env.NEXT_PUBLIC_IROH_RELAY_TOKEN || "";

/** The relay token, asked from our own backend each time iroh-fetch needs one. */
export async function relayToken(): Promise<string> {
  try {
    const res = await fetch("/api/relay-token", { cache: "no-store" });
    if (res.ok) return await res.text();
  } catch {}
  return bundledRelayToken;
}

export const API = "https://api.demo";
export const FILES = "https://files.demo";
