import type { IrohFetchOptions } from "iroh-fetch";

/**
 * Which gateway serves which origin. The app code below just calls fetch("https://api.demo/..."),
 * new EventSource(...) and new WebSocket(...) as usual; iroh-fetch routes these origins over iroh.
 *
 * Endpoint ids are public addresses (like hostnames), so they can live in the repo. Override them
 * without a code change by setting NEXT_PUBLIC_IROH_BACKENDS to the same shape as JSON.
 */
const demoBackends: IrohFetchOptions["backends"] = {
  // Backend A behind two gateways (HA): requests alternate between them and fail over if one is down.
  "https://api.demo": {
    endpointIds: [
      "acc0c8d6bc20700875b5fe1da1a3379f77f28dd16f8a66e3368856473ecc7a53",
      "12cc92042d6d8048cc6ded53115b1b85c5ed1b08d2ff893695bac33228224558",
    ],
    strategy: "round-robin",
  },
  // Backend B, a separate service behind its own gateway.
  "https://files.demo": "61cf27b0bc13636193a9d64544acc08a1f7cf264801be90bfc88b6a10a5ec1f2",
};

export const backends: IrohFetchOptions["backends"] = process.env.NEXT_PUBLIC_IROH_BACKENDS
  ? JSON.parse(process.env.NEXT_PUBLIC_IROH_BACKENDS)
  : demoBackends;

// The demo gateways use this self-hosted relay. It only admits clients with a token; this one is
// for the public demo page, so it is not a secret (anyone can read it from the page). It only
// limits who can use the relay's bandwidth; the backends authenticate users themselves (JWT).
export const relayUrl = process.env.NEXT_PUBLIC_IROH_RELAY_URL || "https://relay.119-10-142-208.sslip.io";
export const relayToken = process.env.NEXT_PUBLIC_IROH_RELAY_TOKEN || "web_50c3173cc8739abf636406fcd9db4c3a";

export const API = "https://api.demo";
export const FILES = "https://files.demo";
