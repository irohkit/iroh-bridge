#!/usr/bin/env node
// iroh-proxy: makes a remote iroh gateway available as a plain local HTTP URL (like `ssh -L`),
// so curl, Postman, browsers and any other HTTP or WebSocket client work unchanged.
import http from "node:http";
import { createHash } from "node:crypto";
import net from "node:net";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { parseArgs } from "node:util";
import { createIrohFetch } from "../src/node.js";

const USAGE = `Usage: iroh-proxy <endpoint-id>[,<endpoint-id>...] [options]

Serves a remote iroh gateway on a local HTTP port. Plain HTTP requests and
WebSocket upgrades are forwarded as-is, so any HTTP client works unchanged.
Several comma-separated endpoint ids are replicas of one backend (failover).

Options:
  --port <n>              Local port to listen on (default 8080, 0 = any free port)
  --host <addr>           Local address to bind (default 127.0.0.1). Anything else
                          lets other machines use your gateway access, so it warns.
  --allow-host <names>    Extra Host names to accept, comma-separated ("*" = any).
                          IP addresses, localhost and *.localhost always work; other
                          names are refused so a website can't reach the proxy through
                          DNS rebinding.
  --allow-origin <list>   Extra browser origins that may call the proxy, comma-separated
                          ("*" = any). By default only pages served from the proxy itself
                          or from localhost may, so other sites can't use your gateway
                          access (cross-site requests and WebSockets).
  --relay-url <url>       Your own relay (the gateway should use the same one)
  --relay-token <token>   Token for a relay that requires one (or env IROH_RELAY_TOKEN)
  --connect-timeout <ms>  How long to wait for the gateway (default 15000, 0 = forever)
  -h, --help              Show this help

Example:
  iroh-proxy acc0c8d6bc20... --port 8080
  curl http://127.0.0.1:8080/hello`;

// Placeholder origin for iroh.fetch: with a single backend only the path and query matter. Using a
// full URL (not a bare path) keeps paths such as "//x" from being read as protocol-relative URLs.
const PLACEHOLDER_ORIGIN = "http://iroh.invalid";
// Per-connection headers that must not be forwarded (RFC 9110 section 7.6.1), plus the ones the
// tunnel sets itself.
const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-connection",
  "proxy-authorization",
  "transfer-encoding",
  "upgrade",
  "te",
  "trailer",
  "host",
  "content-length",
  "expect",
]);
// The WebSocket handshake is negotiated separately on each side of the proxy. Extensions such as
// permessage-deflate are not offered upstream because this side does not implement them.
const WS_HANDSHAKE = new Set(["sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "sec-websocket-protocol", "sec-websocket-accept"]);
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
// Messages larger than this are refused (close 1009) instead of growing memory without bound.
const MAX_MESSAGE = 64 * 1024 * 1024;
// Stop reading from a WebSocket client while this much is still queued towards the gateway.
const HIGH_WATER = 4 * 1024 * 1024;

main().catch((err) => {
  console.error(`iroh-proxy: ${err?.message ?? err}`);
  process.exit(1);
});

async function main() {
  const { values, positionals } = parseCli();
  if (values.help) {
    console.log(USAGE);
    return;
  }
  if (positionals.length !== 1) {
    console.error(USAGE);
    process.exit(1);
  }
  const endpointIds = positionals[0]
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!endpointIds.length) fail("no endpoint id given");
  const port = toNumber(values.port, "--port");
  if (!Number.isInteger(port) || port < 0 || port > 65535) fail(`invalid --port ${values.port}`);
  const connectTimeout = toNumber(values["connect-timeout"], "--connect-timeout");
  const policy = {
    hosts: new Set(list(values["allow-host"]).map((h) => h.toLowerCase())),
    origins: new Set(list(values["allow-origin"]).map((o) => (o === "*" || o === "null" ? o : originOf(o)))),
  };

  let iroh;
  try {
    // cookies: false keeps the proxy transparent: the real client keeps its own cookie jar.
    iroh = await createIrohFetch({ endpointIds, relayUrl: values["relay-url"], relayToken: values["relay-token"] ?? process.env.IROH_RELAY_TOKEN, connectTimeout, cookies: false });
  } catch (err) {
    fail(err?.message ?? String(err));
  }

  const sockets = new Set();
  const server = http.createServer((req, res) => {
    const refused = refusal(req, policy);
    if (refused) return res.writeHead(refused[0], { "content-type": "text/plain; charset=utf-8", connection: "close" }).end(`iroh-proxy: ${refused[1]}\n`);
    // A rejection here would otherwise end the process (Node's default for unhandled ones).
    proxyRequest(iroh, req, res).catch((err) => {
      console.error(`iroh-proxy: ${req.method} ${req.url}: ${err?.message ?? err}`);
      res.destroy();
    });
  });
  server.on("upgrade", (req, socket, head) => {
    const refused = refusal(req, policy);
    if (refused) return rejectUpgrade(socket, refused[0], refused[0] === 403 ? "Forbidden" : "Bad Request", `iroh-proxy: ${refused[1]}\n`);
    try {
      proxyUpgrade(iroh, req, socket, head, sockets);
    } catch (err) {
      console.error(`iroh-proxy: WebSocket ${req.url}: ${err?.message ?? err}`);
      socket.destroy();
    }
  });
  // CONNECT (forward-proxy tunnels) is not supported: without a "connect" listener Node closes
  // those connections, which is what we want, so none is added.
  server.on("clientError", (err, socket) => {
    // Node's parser already caps the head at 16 KB (--max-http-header-size) and rejects malformed
    // requests; nothing of such a request is forwarded.
    const status = err.code === "HPE_HEADER_OVERFLOW" ? "431 Request Header Fields Too Large" : "400 Bad Request";
    if (socket.writable) socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);
    else socket.destroy();
  });

  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, values.host, resolve);
    });
  } catch (err) {
    await iroh.close().catch(() => {});
    fail(err.code === "EADDRINUSE" ? `${values.host}:${port} is already in use (try another --port)` : err.message);
  }

  const { address, port: actual } = server.address();
  const shown = address.includes(":") ? `[${address}]` : address;
  const replicas = endpointIds.length > 1 ? ` (${endpointIds.length} replicas)` : "";
  console.log(`iroh-proxy: http://${shown}:${actual} -> gateway ${endpointIds[0].slice(0, 8)}…${replicas}`);
  if (!isLoopback(address)) {
    console.error(
      `iroh-proxy: WARNING: listening on ${shown}, not a loopback address. Anyone who can reach this ` +
        "port uses the gateway as this machine (same iroh identity, same access). Bind 127.0.0.1 unless you mean it.",
    );
  }

  let stopping = false;
  const shutdown = async () => {
    // A second Ctrl-C means "now", in case something hangs while closing.
    if (stopping) process.exit(1);
    stopping = true;
    server.close();
    server.closeAllConnections();
    for (const s of sockets) s.destroy();
    await iroh.close().catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

function parseCli() {
  try {
    return parseArgs({
      allowPositionals: true,
      options: {
        port: { type: "string", default: "8080" },
        host: { type: "string", default: "127.0.0.1" },
        "allow-host": { type: "string", default: "" },
        "allow-origin": { type: "string", default: "" },
        "relay-url": { type: "string" },
        "relay-token": { type: "string" },
        "connect-timeout": { type: "string", default: "15000" },
        help: { type: "boolean", short: "h" },
      },
    });
  } catch (err) {
    console.error(`iroh-proxy: ${err.message}\n\n${USAGE}`);
    process.exit(1);
  }
}

function toNumber(value, flag) {
  const n = Number(value);
  if (value === "" || !Number.isFinite(n) || n < 0) fail(`invalid ${flag} ${value}`);
  return n;
}

function list(value) {
  return String(value ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function originOf(value) {
  try {
    return new URL(value).origin;
  } catch {
    fail(`invalid origin ${value} (expected e.g. http://localhost:3000)`);
  }
}

/** The host part of a Host header or URL host, without port or IPv6 brackets, lowercased. */
function hostname(host) {
  const h = String(host).toLowerCase();
  if (h.startsWith("[")) return h.slice(1, h.indexOf("]") >>> 0);
  const colon = h.lastIndexOf(":");
  return colon === -1 ? h : h.slice(0, colon);
}

function isLoopback(name) {
  const h = hostname(name.includes(":") && !name.startsWith("[") && net.isIPv6(name) ? `[${name}]` : name);
  return h === "localhost" || h.endsWith(".localhost") || h === "::1" || (net.isIPv4(h) && h.startsWith("127."));
}

/**
 * Why a request must not be proxied, as [status, message], or null. The proxy acts with this
 * machine's iroh identity, so it only serves the local user: no forward-proxy requests for other
 * hosts, no Host names a website could point at 127.0.0.1 (DNS rebinding), and no browser
 * requests from other sites (CSRF, cross-site WebSocket hijacking).
 */
function refusal(req, policy) {
  const host = req.headers.host;
  const target = req.url ?? "/";
  if (!target.startsWith("/")) {
    let url;
    try {
      url = new URL(target);
    } catch {
      return [400, "invalid request target"];
    }
    if (url.host !== host) return [400, "not a forward proxy: only requests for this proxy's own host are served"];
  }
  if (host !== undefined && !policy.hosts.has("*")) {
    const name = hostname(host);
    const ok = net.isIP(name) !== 0 || isLoopback(host) || policy.hosts.has(name) || policy.hosts.has(String(host).toLowerCase());
    if (!ok) return [403, `Host ${JSON.stringify(host)} is not allowed (add it with --allow-host)`];
  }
  const origin = req.headers.origin;
  if (origin !== undefined && !policy.origins.has("*") && !policy.origins.has(origin)) {
    let url = null;
    try {
      url = new URL(origin);
    } catch {}
    const ok = url && (url.host === host || isLoopback(url.host) || policy.origins.has(url.origin));
    if (!ok) return [403, `requests from ${JSON.stringify(origin)} are not allowed (add it with --allow-origin)`];
  }
  return null;
}

function fail(message) {
  console.error(`iroh-proxy: ${message}`);
  process.exit(1);
}

/** Headers to forward upstream: everything except hop-by-hop ones, plus the client's Host. */
function forwardHeaders(req, skip) {
  // Headers listed in Connection are hop-by-hop too.
  const listed = new Set(
    String(req.headers.connection ?? "")
      .split(",")
      .map((s) => s.trim().toLowerCase()),
  );
  const out = [];
  const raw = req.rawHeaders;
  for (let i = 0; i < raw.length; i += 2) {
    const name = raw[i].toLowerCase();
    if (HOP_BY_HOP.has(name) || listed.has(name) || skip?.has(name)) continue;
    out.push([name, raw[i + 1]]);
  }
  // The gateway routes by host; the tunnel itself has none, so pass on the one the client used.
  if (req.headers.host) out.push(["x-iroh-host", req.headers.host]);
  return out;
}

/** Path and query of the request; absolute-form targets (already checked to name this proxy) are reduced to that. */
function targetPath(req) {
  const url = req.url ?? "/";
  if (url.startsWith("/")) return url;
  try {
    const u = new URL(url);
    return u.pathname + u.search;
  } catch {
    return "/";
  }
}

async function proxyRequest(iroh, req, res) {
  const ctrl = new AbortController();
  // The client went away before the response finished: stop the transfer on the gateway too.
  res.once("close", () => {
    if (!res.writableFinished) ctrl.abort();
  });

  const hasBody =
    req.method !== "GET" && req.method !== "HEAD" && (req.headers["transfer-encoding"] !== undefined || Number(req.headers["content-length"] ?? 0) > 0);
  let upstream;
  try {
    upstream = await iroh.fetch(PLACEHOLDER_ORIGIN + targetPath(req), {
      method: req.method,
      headers: forwardHeaders(req),
      body: hasBody ? Readable.toWeb(req) : undefined,
      // Transparent: the real client follows redirects and keeps cookies itself.
      redirect: "manual",
      credentials: "omit",
      signal: ctrl.signal,
    });
  } catch (err) {
    if (ctrl.signal.aborted) return;
    console.error(`iroh-proxy: ${req.method} ${req.url}: ${err?.message ?? err}`);
    if (!res.headersSent) {
      res.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
      res.end(`iroh-proxy: ${err?.message ?? err}\n`);
    } else res.destroy();
    return;
  }

  for (const [name, value] of upstream.headers) {
    if (name === "set-cookie" || name === "connection" || name === "keep-alive" || name === "transfer-encoding") continue;
    res.setHeader(name, value);
  }
  // Each cookie must stay its own header line; joining them with commas would break them.
  const cookies = upstream.headers.getSetCookie();
  if (cookies.length) res.setHeader("set-cookie", cookies);
  res.writeHead(upstream.status);
  if (!upstream.body) return res.end();
  // Send headers right away so streamed responses (SSE, NDJSON) start without waiting for data.
  res.flushHeaders();
  try {
    await pipeline(Readable.fromWeb(upstream.body), res);
  } catch (err) {
    if (!ctrl.signal.aborted) console.error(`iroh-proxy: ${req.method} ${req.url}: response broken: ${err?.message ?? err}`);
    res.destroy();
  }
}

function proxyUpgrade(iroh, req, socket, head, sockets) {
  const key = req.headers["sec-websocket-key"];
  if (req.method !== "GET" || String(req.headers.upgrade).toLowerCase() !== "websocket" || !key) {
    return rejectUpgrade(socket, 400, "Bad Request", "only WebSocket upgrades are supported\n");
  }
  if (req.headers["sec-websocket-version"] !== "13") {
    socket.end("HTTP/1.1 426 Upgrade Required\r\nSec-WebSocket-Version: 13\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    return;
  }
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
  socket.on("error", () => {});

  const protocols = String(req.headers["sec-websocket-protocol"] ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const remote = iroh.websocket(targetPath(req), { protocols, headers: forwardHeaders(req, WS_HANDSHAKE) });
  remote.binaryType = "arraybuffer";

  let link = null;
  // The 101 is only sent once the backend accepted, so the client learns the chosen subprotocol
  // and a refused upgrade can still be answered with a real HTTP error.
  remote.onopen = () => {
    if (socket.destroyed) return remote.close(1001, "");
    const accept = createHash("sha1").update(key + WS_GUID).digest("base64");
    const lines = ["HTTP/1.1 101 Switching Protocols", "Upgrade: websocket", "Connection: Upgrade", `Sec-WebSocket-Accept: ${accept}`];
    if (remote.protocol) lines.push(`Sec-WebSocket-Protocol: ${remote.protocol}`);
    socket.write(lines.join("\r\n") + "\r\n\r\n");
    link = new WebSocketLink(socket, remote);
    if (head?.length) link.receive(head);
  };
  let failure = "the backend did not accept the WebSocket";
  remote.onerror = (e) => {
    if (link) return;
    failure = e.message ?? e.error?.message ?? failure;
    console.error(`iroh-proxy: WebSocket ${req.url}: ${failure}`);
  };
  remote.onclose = (e) => {
    if (link) link.remoteClosed(e.code, e.reason);
    else rejectUpgrade(socket, 502, "Bad Gateway", `iroh-proxy: ${failure}\n`);
  };
  // The client gave up while the gateway side was still connecting.
  socket.once("close", () => {
    if (!link) remote.close(1001, "");
  });
}

function rejectUpgrade(socket, status, text, body) {
  if (!socket.writable) return socket.destroy();
  socket.end(
    `HTTP/1.1 ${status} ${text}\r\nContent-Type: text/plain; charset=utf-8\r\n` +
      `Content-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`,
  );
}

/**
 * The server side of RFC 6455 on the client's raw socket, relaying messages to and from the
 * iroh WebSocket. Only what a relay needs: no extensions, messages are reassembled from
 * fragments and forwarded whole, pings are answered here.
 */
class WebSocketLink {
  #socket;
  #remote;
  #buf = Buffer.alloc(0);
  // Chunks not yet joined into #buf, and how many bytes the next frame needs at least.
  #pending = [];
  #pendingLen = 0;
  #need = 2;
  #fragments = null; // { text, parts, size } while a fragmented message is arriving
  #closeSent = false;
  #closeReceived = false;
  #remoteDone = false;

  constructor(socket, remote) {
    this.#socket = socket;
    this.#remote = remote;
    socket.on("data", (chunk) => this.receive(chunk));
    // TCP dropped without a closing handshake: tell the backend the client is gone.
    socket.once("close", () => {
      if (!this.#remoteDone) remote.close(this.#closeReceived ? 1000 : 1001, "");
    });
    remote.onmessage = (e) => {
      if (typeof e.data === "string") this.#send(0x1, Buffer.from(e.data));
      else this.#send(0x2, Buffer.from(e.data));
    };
  }

  /** Feeds bytes from the client and handles every complete frame in them. */
  receive(chunk) {
    this.#pending.push(chunk);
    this.#pendingLen += chunk.length;
    // Join only once the next frame can be complete, so a large frame arriving in many small
    // chunks is copied once instead of once per chunk.
    if (this.#buf.length + this.#pendingLen < this.#need) return;
    this.#buf = this.#buf.length || this.#pending.length > 1 ? Buffer.concat([this.#buf, ...this.#pending]) : this.#pending[0];
    this.#pending = [];
    this.#pendingLen = 0;
    for (;;) {
      const frame = this.#parseFrame();
      if (!frame) return;
      this.#handleFrame(frame);
      if (this.#socket.destroyed) return;
    }
  }

  /** Removes one frame from the buffer, or returns null when it is not complete yet. */
  #parseFrame() {
    const b = this.#buf;
    this.#need = 2;
    if (b.length < 2) return null;
    const fin = (b[0] & 0x80) !== 0;
    const rsv = b[0] & 0x70;
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (b.length < (this.#need = 4)) return null;
      len = b.readUInt16BE(2);
      offset = 4;
    } else if (len === 127) {
      if (b.length < (this.#need = 10)) return null;
      const big = b.readBigUInt64BE(2);
      if (big > BigInt(MAX_MESSAGE)) return this.#protocolError(1009, "message too big");
      len = Number(big);
      offset = 10;
    }
    // No extensions were negotiated, so reserved bits must be 0; clients must mask (RFC 6455 5.1).
    if (rsv) return this.#protocolError(1002, "reserved bits set");
    if (!masked) return this.#protocolError(1002, "client frames must be masked");
    if (len > MAX_MESSAGE) return this.#protocolError(1009, "message too big");
    if (b.length < (this.#need = offset + 4 + len)) return null;
    const mask = b.subarray(offset, offset + 4);
    const payload = Buffer.from(b.subarray(offset + 4, offset + 4 + len));
    for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    this.#buf = b.subarray(offset + 4 + len);
    this.#need = 2;
    return { fin, opcode, payload };
  }

  #handleFrame({ fin, opcode, payload }) {
    if (opcode >= 0x8) {
      // Control frames may arrive between fragments but are never fragmented themselves.
      if (!fin || payload.length > 125) return this.#protocolError(1002, "bad control frame");
      if (opcode === 0x8) return this.#onClose(payload);
      if (opcode === 0x9) return this.#send(0xa, payload);
      if (opcode === 0xa) return; // pong: nothing to do
      return this.#protocolError(1002, "unknown opcode");
    }
    if (this.#closeReceived) return;
    if (opcode === 0x0) {
      if (!this.#fragments) return this.#protocolError(1002, "unexpected continuation frame");
    } else if (opcode === 0x1 || opcode === 0x2) {
      if (this.#fragments) return this.#protocolError(1002, "expected a continuation frame");
      this.#fragments = { text: opcode === 0x1, parts: [], size: 0 };
    } else {
      return this.#protocolError(1002, "unknown opcode");
    }
    const msg = this.#fragments;
    msg.parts.push(payload);
    msg.size += payload.length;
    if (msg.size > MAX_MESSAGE) return this.#protocolError(1009, "message too big");
    if (!fin) return;
    this.#fragments = null;
    const data = msg.parts.length === 1 ? msg.parts[0] : Buffer.concat(msg.parts);
    if (msg.text) {
      let text;
      try {
        text = new TextDecoder("utf-8", { fatal: true }).decode(data);
      } catch {
        return this.#protocolError(1007, "invalid UTF-8");
      }
      this.#remote.send(text);
    } else {
      this.#remote.send(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    }
    this.#throttle();
  }

  /** Pauses reading from the client while the gateway side has a lot queued. */
  #throttle() {
    if (this.#remote.bufferedAmount <= HIGH_WATER || this.#socket.isPaused()) return;
    this.#socket.pause();
    const timer = setInterval(() => {
      if (this.#remote.bufferedAmount <= HIGH_WATER / 2 || this.#remote.readyState !== 1) {
        clearInterval(timer);
        this.#socket.resume();
      }
    }, 20);
  }

  #onClose(payload) {
    if (payload.length === 1) return this.#protocolError(1002, "bad close frame");
    const code = payload.length >= 2 ? payload.readUInt16BE(0) : 1005;
    if (payload.length >= 2 && !validCloseCode(code)) return this.#protocolError(1002, "bad close code");
    let reason = "";
    try {
      reason = new TextDecoder("utf-8", { fatal: true }).decode(payload.subarray(2));
    } catch {
      return this.#protocolError(1007, "invalid UTF-8 in close reason");
    }
    this.#closeReceived = true;
    if (this.#closeSent) {
      // This answers a close we started, so the handshake is complete.
      return this.#socket.end();
    }
    // Client started closing: echo it, then pass it on to the backend.
    this.#sendClose(code, reason);
    this.#socket.end();
    if (!this.#remoteDone) this.#remote.close(code === 1005 ? 1000 : code, reason);
  }

  /** The backend side closed (or failed): relay the code and wait for the client's answer. */
  remoteClosed(code, reason) {
    this.#remoteDone = true;
    if (this.#closeSent || this.#socket.destroyed) return;
    // 1006 means the tunnel broke; dropping the connection is how the client learns the same.
    if (code === 1006 || code === 1015) return this.#socket.destroy();
    this.#sendClose(code, reason);
    if (this.#closeReceived) return this.#socket.end();
    // Don't wait forever for a client that never answers the close.
    setTimeout(() => this.#socket.destroy(), 5000).unref();
  }

  #sendClose(code, reason) {
    if (this.#closeSent) return;
    this.#closeSent = true;
    if (code === 1005 || !validCloseCode(code)) return this.#send(0x8, Buffer.alloc(0));
    const text = Buffer.from(reason ?? "");
    // A control frame payload is at most 125 bytes: 2 for the code, up to 123 for the reason.
    const payload = Buffer.alloc(2 + Math.min(text.length, 123));
    payload.writeUInt16BE(code, 0);
    text.copy(payload, 2, 0, payload.length - 2);
    this.#send(0x8, payload);
  }

  #protocolError(code, reason) {
    this.#buf = Buffer.alloc(0);
    this.#pending = [];
    this.#pendingLen = 0;
    if (!this.#socket.destroyed) {
      this.#sendClose(code, reason);
      this.#socket.end();
    }
    if (!this.#remoteDone) this.#remote.close(code === 1002 || code === 1007 || code === 1009 ? 1011 : 1000, reason);
    this.#remoteDone = true;
    return null;
  }

  /** Writes one unmasked, unfragmented frame (servers never mask, RFC 6455 5.1). */
  #send(opcode, payload) {
    if (this.#socket.destroyed || !this.#socket.writable) return;
    // After our close frame only the close itself may still go out.
    if (this.#closeSent && opcode !== 0x8) return;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    this.#socket.write(Buffer.concat([header, payload]));
  }
}

/** Close codes an endpoint may put on the wire (RFC 6455 7.4). */
function validCloseCode(code) {
  return (code >= 1000 && code <= 1003) || (code >= 1007 && code <= 1014) || (code >= 3000 && code <= 4999);
}
