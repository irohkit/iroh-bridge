"use client";

import { useState } from "react";
import { useIroh } from "@/lib/iroh";
import { preStyle } from "./StreamDemo";

/** Streams a picked file to the backend without reading it into memory first. */
export function UploadDemo() {
  const iroh = useIroh();
  const [result, setResult] = useState("");
  const [busy, setBusy] = useState(false);

  async function upload(file: File) {
    if (iroh.status !== "ready") return;
    setBusy(true);
    setResult(`uploading ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)…`);
    const started = performance.now();
    try {
      const res = await iroh.client.fetch("/upload", { method: "POST", body: file });
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
      <pre data-testid="upload-result" style={preStyle}>{result}</pre>
    </section>
  );
}
