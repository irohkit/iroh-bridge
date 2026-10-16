"use client";

import { useEffect, useRef, useState } from "react";
import { useIroh } from "@/lib/iroh";
import { preStyle } from "./StreamDemo";

/** Opens a WebSocket to the backend through iroh and echoes messages. */
export function WebSocketDemo() {
  const iroh = useIroh();
  const ws = useRef<WebSocket | null>(null);
  const [state, setState] = useState<"closed" | "connecting" | "open">("closed");
  const [log, setLog] = useState<string[]>([]);
  const [text, setText] = useState("hello over iroh");
  const add = (line: string) => setLog((prev) => [...prev, line].slice(-30));

  useEffect(() => () => ws.current?.close(), []);

  function connect() {
    if (iroh.status !== "ready") return;
    const socket = iroh.client.websocket("/ws", { protocols: ["chat.v1"] });
    ws.current = socket;
    setState("connecting");
    socket.onopen = () => {
      setState("open");
      add(`open (protocol: ${socket.protocol || "none"})`);
    };
    socket.onmessage = (e) => add(`← ${typeof e.data === "string" ? e.data : "[binary]"}`);
    socket.onerror = (e) => add(`error: ${(e as ErrorEvent).message ?? "unknown"}`);
    socket.onclose = (e) => {
      setState("closed");
      add(`closed ${e.code} ${e.reason}`);
    };
  }

  function send() {
    ws.current?.send(text);
    add(`→ ${text}`);
  }

  return (
    <section>
      <h2>WebSocket</h2>
      <div style={{ display: "flex", gap: 8 }}>
        {state === "closed" ? (
          <button onClick={connect} disabled={iroh.status !== "ready"}>Connect /ws</button>
        ) : (
          <button onClick={() => ws.current?.close(1000, "bye")}>Disconnect</button>
        )}
        <input value={text} onChange={(e) => setText(e.target.value)} style={{ flex: 1 }} disabled={state !== "open"} />
        <button onClick={send} disabled={state !== "open"}>Send</button>
      </div>
      <pre data-testid="ws" style={preStyle}>{log.join("\n")}</pre>
    </section>
  );
}
