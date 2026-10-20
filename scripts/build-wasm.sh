#!/usr/bin/env bash
# Builds the Rust web client to WASM and places the bindings in packages/iroh-fetch/wasm.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE="${PROFILE:-release}"
cargo build -p web-client --target wasm32-unknown-unknown --profile "$PROFILE"
rm -rf packages/iroh-fetch/wasm
wasm-bindgen "target/wasm32-unknown-unknown/$PROFILE/web_client.wasm" \
  --out-dir packages/iroh-fetch/wasm --target web --weak-refs
# Safari/WebKit refuses hosts spelled with a trailing dot ("relay.n0.iroh.link."), which is how
# n0's public relay URLs are written, so iroh's relay WebSockets and fetches strip it (same server
# either way).
python3 - packages/iroh-fetch/wasm/web_client.js <<'PY'
import sys
path = sys.argv[1]
js = open(path).read()
needle = "new WebSocket(getStringFromWasm0(arg0, arg1)"
count = js.count(needle)
if count == 0:
    sys.exit("build-wasm: WebSocket constructor not found in the wasm-bindgen output; update the patch")
js = js.replace(needle, "new WebSocket(irohRelayUrl(getStringFromWasm0(arg0, arg1))")
# Relay latency probes (https://<relay>./ping) are fetches; WebKit rejects their TLS certificate too.
request = "new Request(getStringFromWasm0(arg0, arg1)"
if request not in js:
    sys.exit("build-wasm: Request constructor not found in the wasm-bindgen output; update the patch")
count += js.count(request)
js = js.replace(request, "new Request(irohRelayUrl(getStringFromWasm0(arg0, arg1))")
helper = (
    "// Added by scripts/build-wasm.sh: WebKit rejects hostnames with a trailing dot.\n"
    "function irohRelayUrl(url) {\n"
    "    try {\n"
    "        const u = new URL(url);\n"
    "        if (u.hostname.endsWith('.')) { u.hostname = u.hostname.slice(0, -1); return u.href; }\n"
    "    } catch {}\n"
    "    return url;\n"
    "}\n\n"
)
open(path, "w").write(helper + js)
print(f"patched {count} WebSocket/Request constructor(s) for trailing-dot relay hosts")
PY

# Optional: shrink the binary further when binaryen's wasm-opt is installed.
if command -v wasm-opt >/dev/null; then
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
    -o packages/iroh-fetch/wasm/web_client_bg.wasm packages/iroh-fetch/wasm/web_client_bg.wasm
fi
ls -lh packages/iroh-fetch/wasm/web_client_bg.wasm
