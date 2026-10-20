"use client";

import type { ReactNode } from "react";
import { IrohProvider } from "iroh-fetch/react";
import { backends, relayToken, relayUrl } from "../iroh.config";

/** One iroh connection set for the whole app; install routes the global fetch for those origins. */
export function Providers({ children }: { children: ReactNode }) {
  return (
    // Only a custom relay takes a token; it's fetched from app/api/relay-token when needed.
    <IrohProvider backends={backends} relayUrl={relayUrl} relayToken={relayUrl ? relayToken : undefined} install serviceWorker>
      {children}
    </IrohProvider>
  );
}
