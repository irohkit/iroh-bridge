// Tiny HTTP + WebSocket backend used to try the bridge. Replace with your real backend.
import http from "node:http";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { WebSocketServer } from "ws";

const port = Number(process.env.PORT ?? 8080);
// Shown in responses so a frontend talking to several backends can tell them apart.
const name = process.env.BACKEND_NAME ?? "A";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Minimal HS256 JWTs, to show token auth working over iroh exactly as over a public URL.
const JWT_SECRET = process.env.JWT_SECRET ?? "demo-secret-change-me";
const b64url = (buf) => Buffer.from(buf).toString("base64url");
const hmac = (data) => createHmac("sha256", JWT_SECRET).update(data).digest();

function signJwt(payload, ttlSeconds = 3600) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify({ ...payload, iat: now, exp: now + ttlSeconds }));
  return `${head}.${body}.${b64url(hmac(`${head}.${body}`))}`;
}

/** Returns the payload of a valid, unexpired HS256 token, or null. */
function verifyJwt(token) {
  const parts = token?.split(".");
  if (parts?.length !== 3) return null;
  try {
    if (JSON.parse(Buffer.from(parts[0], "base64url")).alg !== "HS256") return null;
    const sig = Buffer.from(parts[2], "base64url");
    const want = hmac(`${parts[0]}.${parts[1]}`);
    if (sig.length !== want.length || !timingSafeEqual(sig, want)) return null;
    const payload = JSON.parse(Buffer.from(parts[1], "base64url"));
    return payload.exp > Date.now() / 1000 ? payload : null;
  } catch {
    return null;
  }
}

const bearer = (req) => /^Bearer (.+)$/i.exec(req.headers.authorization ?? "")?.[1];

function unauthorized(res) {
  res.writeHead(401, { "content-type": "application/json", "www-authenticate": 'Bearer realm="demo"' });
  res.end(JSON.stringify({ error: "invalid or missing token" }));
}

// A client that disconnects mid-upload makes reading the body throw ("aborted"); catch it per
// request instead of letting the unhandled rejection take the whole process down.
const server = http.createServer((req, res) => {
  handle(req, res).catch((err) => {
    console.warn(`${req.method} ${req.url}: ${err.message}`);
    res.destroy();
  });
});

async function handle(req, res) {
  // JWT auth: POST /auth/login {"username":"demo","password":"demo"} -> { token }; the other
  // /auth/* routes need "Authorization: Bearer <token>".
  if (req.url?.startsWith("/auth/")) {
    const path = req.url.split("?")[0];
    if (path === "/auth/login" && req.method === "POST") {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      let creds = {};
      try {
        creds = JSON.parse(Buffer.concat(chunks).toString() || "{}");
      } catch {}
      if (creds.username !== "demo" || creds.password !== "demo") return unauthorized(res);
      const ttl = Math.min(Math.max(Number(new URL(req.url, "http://x").searchParams.get("ttl")) || 3600, 1), 86400);
      res.setHeader("content-type", "application/json");
      return res.end(JSON.stringify({ token: signJwt({ sub: "demo", name: "Demo User", role: "admin" }, ttl), expiresIn: ttl }));
    }
    const user = verifyJwt(bearer(req));
    if (!user) return unauthorized(res);
    if (path === "/auth/me") {
      res.setHeader("content-type", "application/json");
      return res.end(JSON.stringify({ backend: name, user: user.sub, name: user.name, role: user.role, exp: user.exp }));
    }
    if (path === "/auth/events") {
      res.writeHead(200, { "content-type": "text/event-stream" });
      for (let i = 1; i <= 2; i++) {
        res.write(`event: private\ndata: ${JSON.stringify({ n: i, user: user.sub })}\n\n`);
        await sleep(50);
      }
      return res.end();
    }
    res.writeHead(404).end();
    return;
  }

  // Streaming upload: hash the body as it arrives instead of holding it in memory.
  if (req.url?.startsWith("/upload")) {
    const hash = createHash("sha256");
    let bytes = 0;
    for await (const c of req) {
      hash.update(c);
      bytes += c.length;
    }
    res.setHeader("content-type", "application/json");
    return res.end(JSON.stringify({ bytes, sha256: hash.digest("hex"), contentType: req.headers["content-type"] ?? null }));
  }

  // Redirects: 302 keeps GET; 303 turns a POST into a GET.
  if (req.url === "/redirect") return res.writeHead(302, { location: "/hello?from=redirect" }).end();
  if (req.url === "/redirect-303") return res.writeHead(303, { location: "/hello?from=303" }).end();
  if (req.url === "/redirect-away") return res.writeHead(302, { location: "https://example.com/" }).end();

  // Cookie session: /login sets it, /whoami echoes what the backend sees.
  if (req.url?.startsWith("/login")) {
    res.writeHead(200, { "set-cookie": ["session=abc123; Path=/; HttpOnly", "theme=dark; Path=/; Max-Age=3600"] });
    return res.end("ok");
  }
  if (req.url?.startsWith("/logout")) {
    res.writeHead(200, { "set-cookie": "session=; Path=/; Max-Age=0" });
    return res.end("bye");
  }
  if (req.url?.startsWith("/whoami")) {
    res.setHeader("content-type", "application/json");
    return res.end(
      JSON.stringify({
        backend: name,
        cookie: req.headers.cookie ?? null,
        remoteId: req.headers["x-iroh-remote-id"] ?? null,
        proto: req.headers["x-forwarded-proto"] ?? null,
      }),
    );
  }

  // SSE that ends after two events, to exercise EventSource reconnects and Last-Event-ID.
  if (req.url?.startsWith("/sse-once")) {
    const last = Number(req.headers["last-event-id"] ?? 0);
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write("retry: 200\n\n");
    for (let id = last + 1; id <= last + 2; id++) {
      res.write(`id: ${id}\nevent: tick\ndata: ${JSON.stringify({ id, resumedFrom: last })}\n\n`);
      await sleep(50);
    }
    return res.end();
  }

  // Resources the browser loads by itself (<img>, downloads), for the service worker demo.
  if (req.url?.startsWith("/image.svg")) {
    res.writeHead(200, { "content-type": "image/svg+xml", "cache-control": "no-store" });
    return res.end(
      `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96">` +
        `<rect width="96" height="96" rx="16" fill="#3b82f6"/><text x="48" y="58" font-size="28" text-anchor="middle" fill="#fff" font-family="sans-serif">${name}</text></svg>`,
    );
  }
  if (req.url?.startsWith("/download")) {
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8", "content-disposition": 'attachment; filename="report.txt"' });
    return res.end(`report from backend ${name}\n`);
  }

  // Answers after a delay, to try timeouts and aborts.
  if (req.url?.startsWith("/slow")) {
    await sleep(3000);
    return res.end("finally");
  }

  const chunks = [];
  for await (const c of req) chunks.push(c);
  const body = Buffer.concat(chunks).toString();

  // Chunked response: one line every 300 ms.
  if (req.url?.startsWith("/stream")) {
    res.setHeader("content-type", "text/plain; charset=utf-8");
    for (let i = 1; i <= 5; i++) {
      res.write(`chunk ${i} at ${new Date().toISOString()}\n`);
      await sleep(300);
    }
    return res.end();
  }

  // Server-Sent Events: a tick every second until the client goes away.
  if (req.url?.startsWith("/sse")) {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    let n = 0;
    const timer = setInterval(() => res.write(`data: ${JSON.stringify({ n: ++n, time: new Date().toISOString() })}\n\n`), 1000);
    req.socket.on("close", () => {
      clearInterval(timer);
      console.log("sse client gone after", n, "events");
    });
    return;
  }

  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify({ backend: name, method: req.method, url: req.url, headers: req.headers, body, time: new Date().toISOString() }));
}

// WebSocket echo on /ws. Send "close-me" and the server closes with code 4000.
// /ws-auth needs a JWT, as "Authorization: Bearer" or ?access_token= (what browsers usually do).
const wss = new WebSocketServer({ noServer: true, handleProtocols: (protocols) => [...protocols][0] ?? false });
server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url ?? "/", "http://x");
  if (url.pathname === "/ws") return wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  if (url.pathname === "/ws-auth") {
    const user = verifyJwt(bearer(req) ?? url.searchParams.get("access_token"));
    if (!user) {
      socket.end('HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm="demo"\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    return wss.handleUpgrade(req, socket, head, (ws) => {
      ws.send(JSON.stringify({ hello: "authenticated", user: user.sub, backend: name }));
      ws.on("message", (data, isBinary) => ws.send(data, { binary: isBinary }));
    });
  }
  socket.end("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
});
wss.on("connection", (ws, req) => {
  console.log("ws connected", req.headers["authorization"] ?? "");
  ws.send(JSON.stringify({ hello: "from backend", backend: name, protocol: ws.protocol }));
  ws.on("message", (data, isBinary) => {
    if (!isBinary && data.toString() === "close-me") return ws.close(4000, "bye");
    ws.send(data, { binary: isBinary });
  });
  ws.on("close", (code) => console.log("ws closed", code));
});

server.listen(port, "127.0.0.1", () => console.log(`backend ${name} on http://127.0.0.1:${port}`));
