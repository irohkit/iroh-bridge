"use client";

import { useRef, useState } from "react";
import { useIroh } from "@/lib/iroh";

/** Reads a Server-Sent Events endpoint through iroh-fetch's streaming body. */
export function StreamDemo() {
  const iroh = useIroh();
  const [events, setEvents] = useState<string[]>([]);
  const abort = useRef<AbortController | null>(null);
  const running = abort.current !== null;
  const [, rerender] = useState(0);

  async function start() {
    if (iroh.status !== "ready") return;
    const ctrl = new AbortController();
    abort.current = ctrl;
    rerender((n) => n + 1);
    setEvents([]);
    try {
      const res = await iroh.client.fetch("/sse", { signal: ctrl.signal });
      const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += value;
        const parts = buf.split("\n\n");
        buf = parts.pop()!;
        const data = parts.map((p) => p.replace(/^data: /, ""));
        setEvents((prev) => [...prev, ...data].slice(-20));
      }
    } catch (e) {
      if (!ctrl.signal.aborted) setEvents((prev) => [...prev, `error: ${e instanceof Error ? e.message : e}`]);
    } finally {
      abort.current = null;
      rerender((n) => n + 1);
    }
  }

  return (
    <section>
      <h2>Streaming (SSE)</h2>
      <button onClick={running ? () => abort.current?.abort() : start} disabled={iroh.status !== "ready"}>
        {running ? "Stop" : "Start /sse"}
      </button>
      <pre data-testid="sse" style={preStyle}>{events.join("\n")}</pre>
    </section>
  );
}

export const preStyle = {
  background: "#f4f4f4",
  padding: 12,
  minHeight: 40,
  whiteSpace: "pre-wrap",
  wordBreak: "break-all",
} as const;
