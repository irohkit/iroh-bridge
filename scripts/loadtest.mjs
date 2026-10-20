#!/usr/bin/env node
// Load-test tool for iroh-bridge: simulates many independent iroh-fetch clients (each one its
// own iroh endpoint, like one browser tab) against running gateways and reports latency
// percentiles, throughput, errors by kind and bytes moved.
//
// Usage:
//   NODE_OPTIONS=--dns-result-order=ipv4first \
//   node scripts/loadtest.mjs --clients 50 --duration 30 --scenario get [--rate 2]
//
// Scenarios:
//   get      every client loops GET https://api.demo/hello
//   mixed    ~70% GET /hello, 15% POST /items, 5% 256 KB POST /upload (files backend),
//            5% GET /stream read to the end, 5% POST /auth/login + GET /auth/me
//   ws       every client holds one WebSocket to /ws and echoes a message every second
//   connect  every client repeatedly connects, does one GET /hello and closes
//
// Environment (all optional except the relay token when the relay needs one):
//   IROH_RELAY_URL       relay URL (default https://relay.119-10-142-208.sslip.io)
//   IROH_RELAY_TOKEN     relay token for clients (falls back to $WEB, as set by
//                        `. ~/.local/share/iroh-bridge-demo/relay-tokens`)
//   API_ENDPOINT_IDS     comma-separated gateway ids for https://api.demo (round-robin)
//   FILES_ENDPOINT_ID    gateway id for https://files.demo
//   GATEWAY_ADMIN_URL    gateway admin base URL to diff /metrics before/after (default
//                        http://127.0.0.1:9180; set to "" to skip)
//   LOADTEST_VPS_SSH     shell command prefix that runs a command on the relay host, e.g.
//                        "ssh -F ~/.ssh/iroh_relay_vps.conf relay-vps"; enables docker stats
//                        sampling of the relay host (read-only) and the CPU guard below
//   LOADTEST_GUARD_URL   URL that must keep answering 200 during the test (checked every 10 s);
//                        the test aborts when it fails twice in a row
//
// Options: see --help. Results are printed as text; --json FILE also writes them as JSON.

import { fork, execFile } from "node:child_process";
import { writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileP = promisify(execFile);
const HERE = fileURLToPath(import.meta.url);
const LIB = new URL("../packages/iroh-fetch/src/node.js", import.meta.url).href;

const DEFAULTS = {
  relayUrl: "https://relay.119-10-142-208.sslip.io",
  apiIds: "acc0c8d6bc20700875b5fe1da1a3379f77f28dd16f8a66e3368856473ecc7a53,12cc92042d6d8048cc6ded53115b1b85c5ed1b08d2ff893695bac33228224558",
  filesId: "61cf27b0bc13636193a9d64544acc08a1f7cf264801be90bfc88b6a10a5ec1f2",
  admin: "http://127.0.0.1:9180",
};

const HELP = `node scripts/loadtest.mjs --clients N --duration S --scenario NAME [options]

  --clients N        simulated clients (default 10)
  --duration S       seconds the scenario runs after all clients connected (default 30)
  --scenario NAME    get | mixed | ws | connect (default get)
  --rate R           requests per second per client (default: closed loop, no think time;
                     ws: messages per second per client, default 1)
  --workers W        worker processes (default min(4, ceil(N/25)))
  --connect-timeout MS  per-client connect timeout (default 15000)
  --request-timeout MS  per-request timeout (default 30000)
  --ramp S           spread client connects over S seconds (default 0 = all at once)
  --max-relay-cpu P  abort when the iroh-relay or caddy container is above P% CPU for 2
                     samples in a row (default 80; needs LOADTEST_VPS_SSH)
  --json FILE        also write the results as JSON
  --help

Environment: IROH_RELAY_URL, IROH_RELAY_TOKEN (or WEB), API_ENDPOINT_IDS, FILES_ENDPOINT_ID,
GATEWAY_ADMIN_URL, LOADTEST_VPS_SSH, LOADTEST_GUARD_URL (see the header of this file).
Run Node with NODE_OPTIONS=--dns-result-order=ipv4first on hosts with broken IPv6.`;

// ---------------------------------------------------------------- shared helpers

function classify(err) {
  const msg = String(err?.message ?? err);
  if (err?.name === "TimeoutError" || /timed? ?out/i.test(msg)) return "timeout";
  if (err?.name === "AbortError") return "aborted";
  if (/could not (reach|connect)/i.test(msg)) return "connect_failed";
  if (/refused this client|relay/i.test(msg)) return "relay";
  if (/closed|reset|lost|stopped/i.test(msg)) return "connection_closed";
  return "error:" + msg.replace(/[0-9a-f]{10,}/g, "<id>").slice(0, 60);
}

function pct(sorted, p) {
  if (!sorted.length) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))];
}

function summarize(values) {
  const s = Float64Array.from(values).sort();
  return { n: s.length, p50: pct(s, 50), p90: pct(s, 90), p99: pct(s, 99), max: s.length ? s[s.length - 1] : NaN };
}

const fmtMs = (v) => (Number.isFinite(v) ? (v >= 1000 ? (v / 1000).toFixed(2) + "s" : v.toFixed(1) + "ms") : "-");
const fmtBytes = (b) => (b >= 1 << 20 ? (b / (1 << 20)).toFixed(1) + " MiB" : b >= 1024 ? (b / 1024).toFixed(1) + " KiB" : b + " B");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- worker side

async function workerMain() {
  const cfg = JSON.parse(process.env.LOADTEST_CFG);
  const { createIrohFetch } = await import(LIB);
  const backends = {
    "https://api.demo": { endpointIds: cfg.apiIds, strategy: "round-robin" },
    "https://files.demo": cfg.filesId,
  };
  const make = () =>
    createIrohFetch({ backends, relayUrl: cfg.relayUrl, relayToken: cfg.relayToken, connectTimeout: cfg.connectTimeout });

  const stats = {
    connect: [], // ms per successful client connect
    connectErrors: {},
    lat: {}, // op -> [ms]
    errors: {}, // kind -> count
    requests: 0,
    bytesUp: 0,
    bytesDown: 0,
  };
  const err = (kind) => (stats.errors[kind] = (stats.errors[kind] ?? 0) + 1);
  const rec = (op, ms) => (stats.lat[op] ??= []).push(ms);

  let running = false;
  let deadline = 0;
  const go = new Promise((resolve) => process.on("message", (m) => m.type === "go" && resolve(m)));

  async function timedFetch(iroh, op, url, init = {}) {
    const t0 = performance.now();
    try {
      const res = await iroh.fetch(url, { ...init, signal: AbortSignal.timeout(cfg.requestTimeout) });
      const buf = await res.arrayBuffer();
      const ms = performance.now() - t0;
      stats.requests++;
      stats.bytesDown += buf.byteLength;
      if (!res.ok) {
        err(`http_${res.status}`);
        return null;
      }
      rec(op, ms);
      return buf;
    } catch (e) {
      stats.requests++;
      err(classify(e));
      return null;
    }
  }

  const upload = new Uint8Array(256 * 1024).map((_, i) => i & 0xff);
  const ops = {
    async get(iroh) {
      await timedFetch(iroh, "GET /hello", "https://api.demo/hello");
    },
    async items(iroh) {
      const body = JSON.stringify({ name: "widget", n: Math.random() });
      stats.bytesUp += body.length;
      await timedFetch(iroh, "POST /items", "https://api.demo/items", { method: "POST", headers: { "content-type": "application/json" }, body });
    },
    async upload(iroh) {
      stats.bytesUp += upload.byteLength;
      const buf = await timedFetch(iroh, "POST /upload 256K", "https://files.demo/upload", { method: "POST", body: upload });
      if (buf && JSON.parse(new TextDecoder().decode(buf)).bytes !== upload.byteLength) err("upload_size_mismatch");
    },
    async stream(iroh) {
      await timedFetch(iroh, "GET /stream", "https://api.demo/stream");
    },
    async auth(iroh) {
      const body = JSON.stringify({ username: "demo", password: "demo" });
      stats.bytesUp += body.length;
      const buf = await timedFetch(iroh, "POST /auth/login", "https://api.demo/auth/login", { method: "POST", headers: { "content-type": "application/json" }, body });
      if (!buf) return;
      const { token } = JSON.parse(new TextDecoder().decode(buf));
      await timedFetch(iroh, "GET /auth/me", "https://api.demo/auth/me", { headers: { authorization: `Bearer ${token}` } });
    },
  };
  const mixed = [
    [0.7, ops.get],
    [0.85, ops.items],
    [0.9, ops.upload],
    [0.95, ops.stream],
    [1, ops.auth],
  ];
  const pickMixed = () => {
    const x = Math.random();
    return mixed.find(([p]) => x < p)[1];
  };

  // Paces a loop at `rate` iterations/s (with jitter on the first one), or runs it closed-loop.
  async function loop(body) {
    const interval = cfg.rate > 0 ? 1000 / cfg.rate : 0;
    if (interval) await sleep(Math.random() * interval);
    let next = performance.now();
    while (running && Date.now() < deadline) {
      await body();
      if (interval) {
        next += interval;
        const wait = next - performance.now();
        if (wait > 0) await sleep(wait);
        else next = performance.now(); // fell behind; don't burst to catch up
      }
    }
  }

  async function connectOne(i) {
    if (cfg.ramp > 0) await sleep((i / cfg.clients) * cfg.ramp * 1000);
    const t0 = performance.now();
    try {
      const iroh = await make();
      stats.connect.push(performance.now() - t0);
      return iroh;
    } catch (e) {
      const k = classify(e);
      stats.connectErrors[k] = (stats.connectErrors[k] ?? 0) + 1;
      return null;
    }
  }

  // ---- connect scenario: the measured loop is connect + one request + close.
  if (cfg.scenario === "connect") {
    process.send({ type: "connected", ok: cfg.clients, failed: 0 });
    const m = await go;
    deadline = m.deadline;
    running = true;
    await Promise.all(
      Array.from({ length: cfg.clients }, async (_, i) => {
        await sleep((i / cfg.clients) * 1000); // de-synchronise the clients
        await loop(async () => {
          const t0 = performance.now();
          let iroh;
          try {
            iroh = await make();
            stats.connect.push(performance.now() - t0);
          } catch (e) {
            const k = classify(e);
            stats.connectErrors[k] = (stats.connectErrors[k] ?? 0) + 1;
            return;
          }
          await ops.get(iroh);
          const tc = performance.now();
          await iroh.close().catch((e) => err("close:" + classify(e)));
          rec("close", performance.now() - tc);
          rec("connect+GET+close", performance.now() - t0);
        });
      }),
    );
    process.send({ type: "result", stats });
    return;
  }

  const clients = (await Promise.all(Array.from({ length: cfg.clients }, (_, i) => connectOne(i)))).filter(Boolean);
  process.send({ type: "connected", ok: clients.length, failed: cfg.clients - clients.length });
  const m = await go;
  deadline = m.deadline;
  running = true;

  if (cfg.scenario === "get") {
    await Promise.all(clients.map((iroh) => loop(() => ops.get(iroh))));
  } else if (cfg.scenario === "mixed") {
    await Promise.all(clients.map((iroh) => loop(() => pickMixed()(iroh))));
  } else if (cfg.scenario === "ws") {
    const rate = cfg.rate > 0 ? cfg.rate : 1;
    await Promise.all(clients.map((iroh, i) => wsClient(iroh, i, rate)));
  }

  async function wsClient(iroh, idx, rate) {
    // One long-lived socket per client; reconnects (counted) if it drops before the end.
    let opened = 0;
    while (running && Date.now() < deadline) {
      const t0 = performance.now();
      const ws = iroh.websocket("https://api.demo/ws");
      const pending = new Map();
      let timer = null;
      let seq = 0;
      const done = new Promise((resolve) => {
        ws.onopen = () => {
          rec("ws open", performance.now() - t0);
          opened++;
          if (opened > 1) err("ws_reconnect");
        };
        ws.onmessage = (ev) => {
          const text = typeof ev.data === "string" ? ev.data : "";
          stats.bytesDown += text.length;
          let msg;
          try {
            msg = JSON.parse(text);
          } catch {
            return;
          }
          if (msg.hello) {
            rec("ws hello", performance.now() - t0);
            // Start echoing after the backend's greeting, jittered so clients don't tick together.
            setTimeout(() => {
              timer = setInterval(() => {
                if (!running || Date.now() >= deadline) return ws.close(1000, "done");
                const id = seq++;
                const payload = JSON.stringify({ seq: id, client: idx, pad: "x".repeat(64) });
                pending.set(id, performance.now());
                stats.bytesUp += payload.length;
                ws.send(payload);
              }, 1000 / rate);
            }, Math.random() * (1000 / rate));
            return;
          }
          if (typeof msg.seq === "number" && pending.has(msg.seq)) {
            rec("ws echo", performance.now() - pending.get(msg.seq));
            pending.delete(msg.seq);
            stats.requests++;
          }
        };
        ws.onerror = (ev) => err("ws:" + classify(ev.error ?? ev.message));
        ws.onclose = (ev) => {
          clearInterval(timer);
          if (running && Date.now() < deadline - 500) err(`ws_closed_${ev.code}`);
          resolve();
        };
      });
      await done;
      // Messages that never came back count as lost.
      for (const [, t] of pending) if (performance.now() - t > 2000) err("ws_echo_lost");
      if (running && Date.now() < deadline) await sleep(1000);
    }
  }

  await Promise.all(clients.map((c) => c.close().catch(() => {})));
  process.send({ type: "result", stats });
}

// ---------------------------------------------------------------- monitoring (main side)

function parseProm(text) {
  const out = {};
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const i = line.lastIndexOf(" ");
    out[line.slice(0, i)] = Number(line.slice(i + 1));
  }
  return out;
}

async function scrapeMetrics(admin) {
  if (!admin) return null;
  try {
    const res = await fetch(admin + "/metrics", { signal: AbortSignal.timeout(3000) });
    return parseProm(await res.text());
  } catch {
    return null;
  }
}

function metricsDelta(before, after) {
  if (!before || !after) return null;
  const sum = (m, re) => Object.entries(m).filter(([k]) => re.test(k)).reduce((a, [, v]) => a + v, 0);
  const d = (re) => sum(after, re) - sum(before, re);
  const rejected = {};
  for (const k of Object.keys(after)) {
    const mm = /^iroh_gateway_rejected_total\{reason="(.+)"\}$/.exec(k);
    if (mm && after[k] - (before[k] ?? 0) > 0) rejected[mm[1]] = after[k] - (before[k] ?? 0);
  }
  const byClass = {};
  for (const c of ["1xx", "2xx", "3xx", "4xx", "5xx"]) byClass[c] = d(new RegExp(`^iroh_gateway_requests_total\\{.*status_class="${c}"`));
  return {
    requests: d(/^iroh_gateway_requests_total/),
    byClass,
    wsUpgraded: d(/^iroh_gateway_websocket_upgrades_total\{.*result="upgraded"/),
    wsFailed: d(/^iroh_gateway_websocket_upgrades_total\{.*result="(failed|refused)"/),
    upstreamFailures: d(/^iroh_gateway_upstream_failures_total/),
    rejected,
    durationCount: d(/^iroh_gateway_request_duration_seconds_count/),
    durationSum: d(/^iroh_gateway_request_duration_seconds_sum/),
    connectionsAfter: after["iroh_gateway_connections"],
  };
}

async function psGateways() {
  try {
    const { stdout } = await execFileP("ps", ["-o", "pid=,rss=", "-C", "iroh-gateway"]);
    const rows = stdout.trim().split("\n").filter(Boolean);
    return Promise.all(
      rows.map(async (l) => {
        const [pid, rss] = l.trim().split(/\s+/).map(Number);
        // utime + stime in clock ticks (fields 14 and 15; assumes CLK_TCK = 100), finer than ps.
        let cpuSecs = NaN;
        try {
          const stat = await readFile(`/proc/${pid}/stat`, "utf8");
          const f = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
          cpuSecs = (Number(f[11]) + Number(f[12])) / 100;
        } catch {}
        // A label from the process environment (key file name / backend), never secrets.
        let name = "";
        try {
          const env = Object.fromEntries((await readFile(`/proc/${pid}/environ`, "utf8")).split("\0").map((kv) => [kv.slice(0, kv.indexOf("=")), kv.slice(kv.indexOf("=") + 1)]));
          name = [env.IROH_KEY_FILE?.split("/").pop(), env.BACKEND_URL, env.IROH_ADMIN_ADDR && "admin"].filter(Boolean).join(" ");
        } catch {}
        return { pid, name, rssKb: rss, cpuSecs };
      }),
    );
  } catch {
    return [];
  }
}

async function vpsStats(sshPrefix) {
  if (!sshPrefix) return null;
  try {
    const { stdout } = await execFileP("sh", ["-c", `${sshPrefix} 'docker stats --no-stream --format "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}"; cat /proc/loadavg' 2>/dev/null`], { timeout: 20000 });
    const out = {};
    for (const line of stdout.split("\n")) {
      const p = line.split("|");
      if (p.length === 4) out[p[0]] = { cpu: parseFloat(p[1]), mem: p[2].split(" / ")[0], net: p[3] };
      else if (/^\d+\.\d+ \d+\.\d+/.test(line)) out.loadavg = line.split(" ").slice(0, 3).join(" ");
    }
    return out;
  } catch {
    return null;
  }
}

/** Checks the guard URL; returns { ok, detail } where detail is "200 35ms" or the failure. */
async function guardCheck(url) {
  const t0 = performance.now();
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
    await res.arrayBuffer();
    return { ok: res.status === 200, detail: `${res.status} ${Math.round(performance.now() - t0)}ms` };
  } catch (e) {
    return { ok: false, detail: `${e.name}: ${e.cause?.code ?? e.message} after ${Math.round(performance.now() - t0)}ms` };
  }
}

// ---------------------------------------------------------------- main side

function parseArgs(argv) {
  const o = { clients: 10, duration: 30, scenario: "get", rate: 0, workers: 0, connectTimeout: 15000, requestTimeout: 30000, ramp: 0, maxRelayCpu: 80, json: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = () => argv[++i];
    if (a === "--help" || a === "-h") o.help = true;
    else if (a === "--clients") o.clients = Number(v());
    else if (a === "--duration") o.duration = Number(v());
    else if (a === "--scenario") o.scenario = v();
    else if (a === "--rate") o.rate = Number(v());
    else if (a === "--workers") o.workers = Number(v());
    else if (a === "--connect-timeout") o.connectTimeout = Number(v());
    else if (a === "--request-timeout") o.requestTimeout = Number(v());
    else if (a === "--ramp") o.ramp = Number(v());
    else if (a === "--max-relay-cpu") o.maxRelayCpu = Number(v());
    else if (a === "--json") o.json = v();
    else throw new Error(`unknown option ${a} (see --help)`);
  }
  if (!["get", "mixed", "ws", "connect"].includes(o.scenario)) throw new Error(`unknown scenario ${o.scenario}`);
  if (!(o.clients > 0) || !(o.duration > 0)) throw new Error("--clients and --duration must be positive");
  return o;
}

function merge(results) {
  const all = { connect: [], connectErrors: {}, lat: {}, errors: {}, requests: 0, bytesUp: 0, bytesDown: 0 };
  for (const s of results) {
    all.connect.push(...s.connect);
    for (const [k, v] of Object.entries(s.connectErrors)) all.connectErrors[k] = (all.connectErrors[k] ?? 0) + v;
    for (const [k, v] of Object.entries(s.errors)) all.errors[k] = (all.errors[k] ?? 0) + v;
    for (const [k, v] of Object.entries(s.lat)) (all.lat[k] ??= []).push(...v);
    all.requests += s.requests;
    all.bytesUp += s.bytesUp;
    all.bytesDown += s.bytesDown;
  }
  return all;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) return console.log(HELP);
  const relayToken = process.env.IROH_RELAY_TOKEN ?? process.env.WEB;
  const cfgBase = {
    scenario: opts.scenario,
    rate: opts.rate,
    connectTimeout: opts.connectTimeout,
    requestTimeout: opts.requestTimeout,
    ramp: opts.ramp,
    relayUrl: process.env.IROH_RELAY_URL ?? DEFAULTS.relayUrl,
    relayToken,
    apiIds: (process.env.API_ENDPOINT_IDS ?? DEFAULTS.apiIds).split(",").map((s) => s.trim()).filter(Boolean),
    filesId: process.env.FILES_ENDPOINT_ID ?? DEFAULTS.filesId,
  };
  const admin = process.env.GATEWAY_ADMIN_URL ?? DEFAULTS.admin;
  const sshPrefix = process.env.LOADTEST_VPS_SSH ?? "";
  const guardUrl = process.env.LOADTEST_GUARD_URL ?? "";
  const W = opts.workers > 0 ? opts.workers : Math.min(4, Math.ceil(opts.clients / 25));
  const log = (...a) => console.error(`[${new Date().toISOString().slice(11, 19)}]`, ...a);

  log(`scenario=${opts.scenario} clients=${opts.clients} workers=${W} duration=${opts.duration}s rate=${opts.rate || "closed-loop"}`);

  const [m0, ps0, vps0] = await Promise.all([scrapeMetrics(admin), psGateways(), vpsStats(sshPrefix)]);

  // Spawn workers.
  const workers = [];
  for (let w = 0; w < W; w++) {
    const n = Math.floor(opts.clients / W) + (w < opts.clients % W ? 1 : 0);
    if (!n) continue;
    const child = fork(HERE, ["--worker"], { env: { ...process.env, LOADTEST_CFG: JSON.stringify({ ...cfgBase, clients: n }) }, stdio: ["ignore", "inherit", "inherit", "ipc"] });
    const state = { child, connected: null, result: null };
    state.connectedP = new Promise((res) => child.on("message", (m) => m.type === "connected" && res((state.connected = m))));
    state.resultP = new Promise((res, rej) => {
      child.on("message", (m) => m.type === "result" && res((state.result = m.stats)));
      child.on("exit", (code) => (state.result ? res(state.result) : rej(new Error(`worker exited with ${code}`))));
    });
    workers.push(state);
  }

  const tConnect = performance.now();
  await Promise.all(workers.map((w) => w.connectedP));
  const connectWall = performance.now() - tConnect;
  const ok = workers.reduce((a, w) => a + w.connected.ok, 0);
  const failed = workers.reduce((a, w) => a + w.connected.failed, 0);
  if (opts.scenario !== "connect") log(`connected ${ok}/${opts.clients} clients in ${(connectWall / 1000).toFixed(1)}s (${failed} failed)`);

  // Run.
  const start = Date.now();
  const deadline = start + opts.duration * 1000;
  for (const w of workers) w.child.send({ type: "go", deadline });

  // Sample local gateway processes and (optionally) the relay host while running.
  const samples = { ps: [], vps: [], guard: [], guardFailures: 0, aborted: null };
  let hot = 0;
  let guardBad = 0;
  let stop = false;
  const sampler = (async () => {
    let tick = 0;
    while (!stop) {
      samples.ps.push(await psGateways());
      if (tick % 3 === 0) {
        const [v, gr] = await Promise.all([vpsStats(sshPrefix), guardUrl ? guardCheck(guardUrl) : { ok: true, detail: "" }]);
        const g = gr.ok;
        if (guardUrl) samples.guard.push(gr.detail);
        if (v) {
          samples.vps.push(v);
          // The relay container, and caddy (the TLS front that relay traffic shares with other sites).
          const cpu = Math.max(v["iroh-relay"]?.cpu ?? 0, v.caddy?.cpu ?? 0);
          hot = cpu > opts.maxRelayCpu ? hot + 1 : 0;
          log(`relay cpu=${v["iroh-relay"]?.cpu}% mem=${v["iroh-relay"]?.mem} caddy cpu=${v.caddy?.cpu}% load=${v.loadavg}${guardUrl ? ` guard=${g ? "ok" : "FAIL"} (${gr.detail})` : ""}`);
        }
        guardBad = g ? 0 : guardBad + 1;
        if (!g) samples.guardFailures++;
        if (hot >= 2 || guardBad >= 2) {
          samples.aborted = hot >= 2 ? `iroh-relay or caddy CPU above ${opts.maxRelayCpu}% twice in a row` : `guard URL ${guardUrl} failed twice in a row`;
          log(`ABORTING: ${samples.aborted}`);
          for (const w of workers) w.child.kill("SIGTERM");
          break;
        }
      }
      tick++;
      await sleep(2000);
    }
  })();

  const settled = await Promise.allSettled(workers.map((w) => w.resultP));
  stop = true;
  await sampler;
  const elapsed = (Date.now() - start) / 1000;
  const results = settled.filter((r) => r.status === "fulfilled").map((r) => r.value);
  const workerFailures = settled.filter((r) => r.status === "rejected").length;

  await sleep(1500); // let the gateways finish counting closed streams
  const [m1, ps1, vps1] = await Promise.all([scrapeMetrics(admin), psGateways(), vpsStats(sshPrefix)]);
  const s = merge(results);
  const runSecs = Math.min(elapsed, opts.duration);

  // Gateway CPU: average over the run from cumulative cpu time; peak from ps samples.
  const gw = ps1.map((p) => {
    const before = ps0.find((q) => q.pid === p.pid);
    const peakRss = Math.max(p.rssKb, ...samples.ps.flat().filter((q) => q.pid === p.pid).map((q) => q.rssKb));
    return {
      pid: p.pid,
      name: p.name,
      avgCpuPct: before ? ((p.cpuSecs - before.cpuSecs) / elapsed) * 100 : NaN,
      rssBeforeMb: before ? before.rssKb / 1024 : NaN,
      rssPeakMb: peakRss / 1024,
      rssAfterMb: p.rssKb / 1024,
    };
  });
  const relayCpu = samples.vps.map((v) => v["iroh-relay"]?.cpu).filter(Number.isFinite);
  const caddyCpu = samples.vps.map((v) => v.caddy?.cpu).filter(Number.isFinite);

  const report = {
    scenario: opts.scenario,
    clients: opts.clients,
    workers: W,
    rate: opts.rate,
    durationSecs: runSecs,
    aborted: samples.aborted,
    workerFailures,
    connect: { ok: opts.scenario === "connect" ? s.connect.length : ok, failed: opts.scenario === "connect" ? Object.values(s.connectErrors).reduce((a, b) => a + b, 0) : failed, errors: s.connectErrors, wallMs: connectWall, ...summarize(s.connect) },
    requests: s.requests,
    requestsPerSec: s.requests / runSecs,
    errors: s.errors,
    errorCount: Object.values(s.errors).reduce((a, b) => a + b, 0),
    latency: Object.fromEntries(Object.entries(s.lat).map(([k, v]) => [k, summarize(v)])),
    bytes: { up: s.bytesUp, down: s.bytesDown },
    gatewayA1Metrics: metricsDelta(m0, m1),
    gatewayProcesses: gw,
    relay: {
      before: vps0 && { relay: vps0["iroh-relay"], caddy: vps0.caddy, load: vps0.loadavg },
      after: vps1 && { relay: vps1["iroh-relay"], caddy: vps1.caddy, load: vps1.loadavg },
      relayCpuMax: relayCpu.length ? Math.max(...relayCpu) : null,
      relayCpuAvg: relayCpu.length ? relayCpu.reduce((a, b) => a + b, 0) / relayCpu.length : null,
      caddyCpuMax: caddyCpu.length ? Math.max(...caddyCpu) : null,
      guardFailures: samples.guardFailures,
      guard: samples.guard,
    },
  };

  // ---- print
  const L = [];
  L.push(`\n=== ${opts.scenario}: ${opts.clients} clients, ${runSecs.toFixed(0)} s${samples.aborted ? `  (ABORTED: ${samples.aborted})` : ""}`);
  const c = report.connect;
  L.push(`connect   ok=${c.ok} failed=${c.failed}  p50=${fmtMs(c.p50)} p90=${fmtMs(c.p90)} p99=${fmtMs(c.p99)} max=${fmtMs(c.max)}${Object.keys(c.errors).length ? "  " + JSON.stringify(c.errors) : ""}`);
  L.push(`requests  ${s.requests} total, ${report.requestsPerSec.toFixed(1)} req/s, errors=${report.errorCount}${report.errorCount ? " " + JSON.stringify(s.errors) : ""}`);
  L.push(`bytes     up=${fmtBytes(s.bytesUp)} down=${fmtBytes(s.bytesDown)}`);
  L.push(`latency   ${"op".padEnd(20)} ${"n".padStart(7)} ${"p50".padStart(8)} ${"p90".padStart(8)} ${"p99".padStart(8)} ${"max".padStart(8)}`);
  for (const [k, v] of Object.entries(report.latency)) {
    L.push(`          ${k.padEnd(20)} ${String(v.n).padStart(7)} ${fmtMs(v.p50).padStart(8)} ${fmtMs(v.p90).padStart(8)} ${fmtMs(v.p99).padStart(8)} ${fmtMs(v.max).padStart(8)}`);
  }
  const md = report.gatewayA1Metrics;
  if (md) {
    L.push(`gateway A1 /metrics delta: requests=${md.requests} ${JSON.stringify(md.byClass)} ws_upgraded=${md.wsUpgraded} ws_failed=${md.wsFailed} upstream_failures=${md.upstreamFailures} rejected=${JSON.stringify(md.rejected)} mean_head_latency=${md.durationCount ? fmtMs((md.durationSum / md.durationCount) * 1000) : "-"} connections_after=${md.connectionsAfter}`);
  }
  for (const g of gw) L.push(`gateway ${g.pid} (${g.name}): avg cpu ${g.avgCpuPct.toFixed(1)}%  rss ${g.rssBeforeMb.toFixed(1)} -> peak ${g.rssPeakMb.toFixed(1)} -> ${g.rssAfterMb.toFixed(1)} MB`);
  if (samples.vps.length || vps0) {
    L.push(`relay host: iroh-relay cpu max=${report.relay.relayCpuMax ?? "-"}% avg=${report.relay.relayCpuAvg?.toFixed(1) ?? "-"}% mem ${vps0?.["iroh-relay"]?.mem} -> ${vps1?.["iroh-relay"]?.mem}; caddy cpu max=${report.relay.caddyCpuMax ?? "-"}%; load ${vps0?.loadavg} -> ${vps1?.loadavg}; relay net ${vps0?.["iroh-relay"]?.net} -> ${vps1?.["iroh-relay"]?.net}${guardUrl ? `; guard failures=${samples.guardFailures}` : ""}`);
  }
  if (workerFailures) L.push(`WARNING: ${workerFailures} worker(s) died`);
  console.log(L.join("\n"));
  if (opts.json) writeFileSync(opts.json, JSON.stringify(report, null, 2));
  process.exit(samples.aborted || workerFailures ? 2 : 0);
}

if (process.argv.includes("--worker")) {
  workerMain().catch((e) => {
    console.error("worker failed:", e);
    process.exit(1);
  });
} else {
  main().catch((e) => {
    console.error(e.message ?? e);
    process.exit(1);
  });
}
