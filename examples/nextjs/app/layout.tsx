import type { ReactNode } from "react";
import { IrohProvider } from "@/lib/iroh";

export const metadata = { title: "Next.js + iroh" };

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui, sans-serif", maxWidth: 720, margin: "40px auto", padding: "0 16px" }}>
        <IrohProvider>{children}</IrohProvider>
      </body>
    </html>
  );
}
