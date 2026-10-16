// Tiny HTTP + WebSocket backend used to try the bridge. Replace with your real backend.
import http from "node:http";
import { WebSocketServer } from "ws";

const port = Number(process.env.PORT ?? 8080);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const server = http.createServer(async (req, res) => {
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
  res.end(JSON.stringify({ method: req.method, url: req.url, headers: req.headers, body, time: new Date().toISOString() }));
});

// WebSocket echo on /ws. Send "close-me" and the server closes with code 4000.
const wss = new WebSocketServer({ server, path: "/ws", handleProtocols: (protocols) => [...protocols][0] ?? false });
wss.on("connection", (ws, req) => {
  console.log("ws connected", req.headers["authorization"] ?? "");
  ws.send(JSON.stringify({ hello: "from backend", protocol: ws.protocol }));
  ws.on("message", (data, isBinary) => {
    if (!isBinary && data.toString() === "close-me") return ws.close(4000, "bye");
    ws.send(data, { binary: isBinary });
  });
  ws.on("close", (code) => console.log("ws closed", code));
});

server.listen(port, "127.0.0.1", () => console.log(`backend on http://127.0.0.1:${port}`));
