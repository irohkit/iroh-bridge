// Hands the relay token to the browser at runtime instead of baking it into the bundle, so it can
// be rotated without a redeploy (iroh-fetch asks again when the relay refuses the old one).
// IROH_RELAY_TOKEN is a server-only env var (no NEXT_PUBLIC_ prefix). The demo is public; a real app
// checks the user's session here and answers 401 to anyone who isn't logged in.
export const dynamic = "force-dynamic";

export function GET() {
  const token = process.env.IROH_RELAY_TOKEN;
  if (!token) return new Response(null, { status: 404 });
  return new Response(token, { headers: { "content-type": "text/plain", "cache-control": "no-store" } });
}
