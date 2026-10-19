import type { ReactNode } from "react";
import { Providers } from "./providers";

export const metadata = { title: "Next.js + iroh" };

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui, sans-serif", maxWidth: 760, margin: "40px auto", padding: "0 16px", lineHeight: 1.45 }}>
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
