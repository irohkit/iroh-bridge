#!/usr/bin/env node
// Compares iroh-fetch's two Node.js engines (native addon vs WASM) against one gateway: time to
// first response, request latency (p50/p99, sequential GET /hello), upload throughput (POST
// /upload, examples/backend) and which network path the connection used.
//
//   NODE_OPTIONS=--dns-result-order=ipv4first IROH_RELAY_URL=... IROH_RELAY_TOKEN=... \
//   node scripts/bench-engines.mjs <endpoint id> [--requests 500] [--upload-mb 30] [--rounds 3]
import { createIrohFetch } from "../packages/iroh-fetch/src/node.js";

const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? Number(args[i + 1]) : def;
};
const endpointId = args.find((a) => /^[0-9a-f]{64}$/.test(a)) ?? process.env.IROH_ENDPOINT_ID;
if (!endpointId) {
  console.error("usage: node scripts/bench-engines.mjs <endpoint id> [--requests N] [--upload-mb N] [--rounds N]");
  process.exit(2);
}
const REQUESTS = opt("requests", 500);
// The gateway refuses bodies over MAX_BODY_BYTES (default 32 MiB).
const UPLOAD_MB = opt("upload-mb", 30);
const ROUNDS = opt("rounds", 3);
const relay = { relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN };

const pct = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function uploadBody(bytes) {
  const chunk = new Uint8Array(64 * 1024).fill(97);
  let left = bytes;
  return new ReadableStream({
    pull(controller) {
      if (left <= 0) return controller.close();
      const n = Math.min(left, chunk.length);
      controller.enqueue(n === chunk.length ? chunk.slice() : chunk.slice(0, n));
      left -= n;
    },
  });
}

async function bench(engine) {
  const out = { engine };
  // Time to first response, fresh client each time.
  const firsts = [];
  for (let i = 0; i < ROUNDS; i++) {
    const t = performance.now();
    const iroh = await createIrohFetch({ endpointId, engine, ...relay });
    await (await iroh.fetch("/hello")).text();
    firsts.push(performance.now() - t);
    await iroh.close();
  }
  out.firstResponseMs = median(firsts);

  const iroh = await createIrohFetch({ endpointId, engine, ...relay });
  try {
    await (await iroh.fetch("/hello")).text();
    // Give hole punching a moment; the WASM engine stays on the relay regardless.
    const deadline = Date.now() + 10_000;
    while (iroh.connectionInfo()?.kind !== "direct" && Date.now() < deadline) await sleep(200);
    const info = iroh.connectionInfo();
    const sel = info?.paths.find((p) => p.selected);
    out.path = sel ? `${sel.kind} ${sel.addr} (rtt ${sel.rttMs.toFixed(2)} ms)` : "none";

    for (let i = 0; i < 20; i++) await (await iroh.fetch("/hello")).text();
    const lat = [];
    for (let i = 0; i < REQUESTS; i++) {
      const t = performance.now();
      await (await iroh.fetch(`/hello?i=${i}`)).text();
      lat.push(performance.now() - t);
    }
    lat.sort((a, b) => a - b);
    out.p50 = pct(lat, 50);
    out.p99 = pct(lat, 99);

    const bytes = UPLOAD_MB * 1024 * 1024;
    const rates = [];
    for (let i = 0; i < ROUNDS; i++) {
      const t = performance.now();
      const res = await iroh.fetch("/upload", { method: "POST", body: uploadBody(bytes), duplex: "half" });
      if (!res.ok) throw new Error(`upload: HTTP ${res.status}: ${await res.text()}`);
      const got = await res.json();
      if (got.bytes !== bytes) throw new Error(`upload: backend got ${got.bytes} of ${bytes} bytes`);
      rates.push(bytes / 1e6 / ((performance.now() - t) / 1000));
    }
    out.uploadMBps = median(rates);
    out.pathAfter = iroh.connectionInfo()?.kind;
  } finally {
    await iroh.close();
  }
  return out;
}

const results = [];
for (const engine of ["native", "wasm"]) {
  const r = await bench(engine);
  results.push(r);
  console.log(
    `${engine.padEnd(6)}  first response ${r.firstResponseMs.toFixed(0)} ms | GET p50 ${r.p50.toFixed(2)} ms, p99 ${r.p99.toFixed(2)} ms ` +
      `(${REQUESTS} sequential) | upload ${r.uploadMBps.toFixed(1)} MB/s (${UPLOAD_MB} MB, median of ${ROUNDS}) | path: ${r.path}`,
  );
}
if (process.argv.includes("--json")) console.log(JSON.stringify(results));
