// Tiny HTTP + WebSocket backend used to try the bridge. Replace with your real backend.
import http from "node:http";
import { createHash } from "node:crypto";
import { WebSocketServer } from "ws";

const port = Number(process.env.PORT ?? 8080);
// Shown in responses so a frontend talking to several backends can tell them apart.
const name = process.env.BACKEND_NAME ?? "A";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const server = http.createServer(async (req, res) => {
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
});

// WebSocket echo on /ws. Send "close-me" and the server closes with code 4000.
const wss = new WebSocketServer({ server, path: "/ws", handleProtocols: (protocols) => [...protocols][0] ?? false });
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
