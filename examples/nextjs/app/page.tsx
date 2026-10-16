"use client";

import { useState } from "react";
import { useIroh } from "@/lib/iroh";
import { StreamDemo } from "./components/StreamDemo";
import { WebSocketDemo } from "./components/WebSocketDemo";

export default function Home() {
  const iroh = useIroh();
  const [path, setPath] = useState("/hello");
  const [method, setMethod] = useState("GET");
  const [body, setBody] = useState('{"name":"rover"}');
  const [result, setResult] = useState<string>("");
  const [busy, setBusy] = useState(false);

  async function send() {
    if (iroh.status !== "ready") return;
    setBusy(true);
    const started = performance.now();
    try {
      const res = await iroh.client.fetch(path, {
        method,
        headers: method === "GET" ? undefined : { "content-type": "application/json" },
        body: method === "GET" ? undefined : body,
      });
      const text = await res.text();
      const ms = Math.round(performance.now() - started);
      setResult(`${res.status} · ${ms} ms\n\n${text}`);
    } catch (e) {
      setResult(`error: ${e instanceof Error ? e.message : e}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main>
      <h1>Next.js → iroh → backend</h1>
      <h2>HTTP request</h2>
      <p data-testid="status">
        {iroh.status === "connecting" && "Connecting to gateway…"}
        {iroh.status === "ready" && `Connected (this browser: ${iroh.client.endpointId.slice(0, 10)}…)`}
        {iroh.status === "error" && `Error: ${iroh.error}`}
      </p>
      <div style={{ display: "flex", gap: 8 }}>
        <select value={method} onChange={(e) => setMethod(e.target.value)}>
          {["GET", "POST", "PUT", "DELETE"].map((m) => (
            <option key={m}>{m}</option>
          ))}
        </select>
        <input value={path} onChange={(e) => setPath(e.target.value)} style={{ flex: 1 }} />
        <button onClick={send} disabled={iroh.status !== "ready" || busy}>
          Send
        </button>
      </div>
      {method !== "GET" && (
        <textarea value={body} onChange={(e) => setBody(e.target.value)} rows={4} style={{ width: "100%", marginTop: 8 }} />
      )}
      <pre data-testid="result" style={{ background: "#f4f4f4", padding: 12, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
        {result}
      </pre>
      <StreamDemo />
      <WebSocketDemo />
    </main>
  );
}
