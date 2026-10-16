"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import type { IrohFetch } from "iroh-fetch";

type State = { status: "connecting" } | { status: "ready"; client: IrohFetch } | { status: "error"; error: string };

const IrohContext = createContext<State>({ status: "connecting" });

/** Opens one iroh connection for the whole app and shares it via context. */
export function IrohProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ status: "connecting" });

  useEffect(() => {
    const endpointId = process.env.NEXT_PUBLIC_IROH_ENDPOINT_ID;
    if (!endpointId) {
      setState({ status: "error", error: "NEXT_PUBLIC_IROH_ENDPOINT_ID is not set" });
      return;
    }
    let client: IrohFetch | undefined;
    let cancelled = false;
    // Import lazily so the WASM only loads in the browser, never during SSR.
    import("iroh-fetch")
      .then(({ createIrohFetch }) =>
        createIrohFetch({ endpointId, relayUrl: process.env.NEXT_PUBLIC_IROH_RELAY_URL || undefined }),
      )
      .then((c) => {
        client = c;
        if (cancelled) c.close();
        else setState({ status: "ready", client: c });
      })
      .catch((e) => !cancelled && setState({ status: "error", error: String(e?.message ?? e) }));
    return () => {
      cancelled = true;
      client?.close();
    };
  }, []);

  return <IrohContext.Provider value={state}>{children}</IrohContext.Provider>;
}

export function useIroh() {
  return useContext(IrohContext);
}
