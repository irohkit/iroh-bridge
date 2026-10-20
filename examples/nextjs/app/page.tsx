"use client";

import { useEffect, useRef, useState } from "react";
import { useIroh } from "iroh-fetch/react";
import { API, FILES } from "../iroh.config";
import { checks } from "./selftest";

const pre = { background: "#f4f4f4", padding: 12, minHeight: 40, whiteSpace: "pre-wrap", wordBreak: "break-all" } as const;
const row = { display: "flex", gap: 8, flexWrap: "wrap" } as const;

export default function Home() {
  const iroh = useIroh();
  return (
    <main>
      <h1>Next.js → iroh → backends</h1>
      <p>
        This page calls <code>{API}</code> and <code>{FILES}</code> with the normal <code>fetch</code>,{" "}
        <code>EventSource</code> and <code>WebSocket</code>. <code>iroh-fetch</code> routes those origins to gateways
        behind NAT; nothing else in the code knows about iroh.
      </p>
      <Status />
      <SelfTest disabled={iroh.status !== "ready"} />
      <RequestDemo />
      <EventSourceDemo />
      <WebSocketDemo />
      <UploadDemo />
    </main>
  );
}

function Status() {
  const iroh = useIroh();
  if (iroh.status === "connecting") return <p data-testid="status">Connecting to gateways…</p>;
  if (iroh.status === "error") return <p data-testid="status">Error: {iroh.error.message}</p>;
  return (
    <div>
      <p data-testid="status">Connected (this browser: {iroh.client.endpointId.slice(0, 10)}…)</p>
      <ul>
        {iroh.client.backends.map((b) => (
          <li key={b.origin}>
            <code>{b.origin}</code>:{" "}
            {b.replicas.map((r) => `${r.endpointId.slice(0, 8)}… ${r.healthy ? "✓" : "✗"}`).join(", ")}
          </li>
        ))}
      </ul>
    </div>
  );
}

type Result = { name: string; ok: boolean; detail: string; ms: number };

function SelfTest({ disabled }: { disabled: boolean }) {
  const iroh = useIroh();
  const [results, setResults] = useState<Result[]>([]);
  const [running, setRunning] = useState(false);

  async function run() {
    if (iroh.status !== "ready") return;
    setRunning(true);
    setResults([]);
    for (const check of checks) {
      const started = performance.now();
      let result: Result;
      try {
        const detail = await check.run({ endpointId: iroh.client.endpointId, client: iroh.client });
        result = { name: check.name, ok: true, detail, ms: performance.now() - started };
      } catch (e) {
        result = { name: check.name, ok: false, detail: e instanceof Error ? e.message : String(e), ms: performance.now() - started };
      }
      setResults((prev) => [...prev, result]);
    }
    setRunning(false);
  }

  const passed = results.filter((r) => r.ok).length;
  return (
    <section>
      <h2>Self-test</h2>
      <button onClick={run} disabled={disabled || running} data-testid="run-selftest">
        {running ? "Running…" : "Run self-test"}
      </button>
      {results.length > 0 && (
        <p data-testid="selftest-summary">
          {passed}/{checks.length} passed{running ? " so far" : ""}
        </p>
      )}
      <ul data-testid="selftest-results" style={{ listStyle: "none", padding: 0 }}>
        {results.map((r) => (
          <li key={r.name}>
            {r.ok ? "✅" : "❌"} {r.name} <small>({Math.round(r.ms)} ms) {r.detail}</small>
          </li>
        ))}
      </ul>
    </section>
  );
}

function RequestDemo() {
  const iroh = useIroh();
  const [url, setUrl] = useState(`${API}/hello`);
  const [method, setMethod] = useState("GET");
  const [body, setBody] = useState('{"name":"rover"}');
  const [result, setResult] = useState("");

  async function send() {
    const started = performance.now();
    try {
      // A completely ordinary fetch call.
      const res = await fetch(url, {
        method,
        headers: method === "GET" ? undefined : { "content-type": "application/json" },
        body: method === "GET" ? undefined : body,
      });
      const text = await res.text();
      const gateway = res.headers.get("x-iroh-gateway-id")?.slice(0, 8);
      setResult(`${res.status} · ${Math.round(performance.now() - started)} ms · via gateway ${gateway}…\n\n${text}`);
    } catch (e) {
      setResult(`error: ${e instanceof Error ? e.message : e}`);
    }
  }

  return (
    <section>
      <h2>fetch()</h2>
      <div style={row}>
        <select value={method} onChange={(e) => setMethod(e.target.value)}>
          {["GET", "POST", "PUT", "DELETE"].map((m) => (
            <option key={m}>{m}</option>
          ))}
        </select>
        <input value={url} onChange={(e) => setUrl(e.target.value)} style={{ flex: 1, minWidth: 200 }} />
        <button onClick={send} disabled={iroh.status !== "ready"}>
          Send
        </button>
      </div>
      {method !== "GET" && <textarea value={body} onChange={(e) => setBody(e.target.value)} rows={3} style={{ width: "100%", marginTop: 8 }} />}
      <pre data-testid="result" style={pre}>{result}</pre>
    </section>
  );
}

function EventSourceDemo() {
  const iroh = useIroh();
  const [events, setEvents] = useState<string[]>([]);
  const es = useRef<EventSource | null>(null);
  const [open, setOpen] = useState(false);
  useEffect(() => () => es.current?.close(), []);

  function toggle() {
    if (es.current) {
      es.current.close();
      es.current = null;
      setOpen(false);
      return;
    }
    setEvents([]);
    // A completely ordinary EventSource.
    const source = new EventSource(`${API}/sse`);
    source.onmessage = (e) => setEvents((prev) => [...prev, e.data].slice(-10));
    es.current = source;
    setOpen(true);
  }

  return (
    <section>
      <h2>EventSource</h2>
      <button onClick={toggle} disabled={iroh.status !== "ready"}>
        {open ? "Stop" : `Start ${API}/sse`}
      </button>
      <pre data-testid="sse" style={pre}>{events.join("\n")}</pre>
    </section>
  );
}

function WebSocketDemo() {
  const iroh = useIroh();
  const ws = useRef<WebSocket | null>(null);
  const [state, setState] = useState<"closed" | "connecting" | "open">("closed");
  const [log, setLog] = useState<string[]>([]);
  const [text, setText] = useState("hello over iroh");
  const add = (line: string) => setLog((prev) => [...prev, line].slice(-30));
  useEffect(() => () => ws.current?.close(), []);

  function connect() {
    // A completely ordinary WebSocket.
    const socket = new WebSocket("wss://files.demo/ws", ["chat.v1"]);
    ws.current = socket;
    setState("connecting");
    socket.onopen = () => {
      setState("open");
      add(`open (protocol: ${socket.protocol || "none"})`);
    };
    socket.onmessage = (e) => add(`← ${typeof e.data === "string" ? e.data : "[binary]"}`);
    socket.onerror = () => add("error");
    socket.onclose = (e) => {
      setState("closed");
      add(`closed ${e.code} ${e.reason}`);
    };
  }

  return (
    <section>
      <h2>WebSocket</h2>
      <div style={row}>
        {state === "closed" ? (
          <button onClick={connect} disabled={iroh.status !== "ready"}>
            Connect wss://files.demo/ws
          </button>
        ) : (
          <button onClick={() => ws.current?.close(1000, "bye")}>Disconnect</button>
        )}
        <input value={text} onChange={(e) => setText(e.target.value)} style={{ flex: 1, minWidth: 160 }} disabled={state !== "open"} />
        <button
          onClick={() => {
            ws.current?.send(text);
            add(`→ ${text}`);
          }}
          disabled={state !== "open"}
        >
          Send
        </button>
      </div>
      <pre data-testid="ws" style={pre}>{log.join("\n")}</pre>
    </section>
  );
}

function UploadDemo() {
  const iroh = useIroh();
  const [result, setResult] = useState("");
  const [busy, setBusy] = useState(false);

  async function upload(file: File) {
    setBusy(true);
    setResult(`uploading ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)…`);
    const started = performance.now();
    try {
      // The file streams up without being read into memory first.
      const res = await fetch(`${API}/upload`, { method: "POST", body: file });
      const data = await res.json();
      const secs = (performance.now() - started) / 1000;
      setResult(`${res.status} · ${secs.toFixed(1)} s · ${(data.bytes / 1024 / 1024 / secs).toFixed(2)} MB/s\n\n${JSON.stringify(data, null, 2)}`);
    } catch (e) {
      setResult(`error: ${e instanceof Error ? e.message : e}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <h2>Streaming upload</h2>
      <input
        type="file"
        data-testid="upload"
        disabled={iroh.status !== "ready" || busy}
        onChange={(e) => e.target.files?.[0] && upload(e.target.files[0])}
      />
      <pre data-testid="upload-result" style={pre}>{result}</pre>
    </section>
  );
}
