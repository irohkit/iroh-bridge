"use client";

import type { ReactNode } from "react";
import { IrohProvider } from "iroh-fetch/react";
import { backends, relayToken, relayUrl } from "../iroh.config";

/** One iroh connection set for the whole app; install routes the global fetch for those origins. */
export function Providers({ children }: { children: ReactNode }) {
  return (
    <IrohProvider backends={backends} relayUrl={relayUrl} relayToken={relayToken} install serviceWorker>
      {children}
    </IrohProvider>
  );
}
