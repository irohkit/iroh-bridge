#!/usr/bin/env bash
# Builds the Rust web client to WASM and places the bindings in packages/iroh-fetch/wasm.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE="${PROFILE:-release}"
cargo build -p web-client --target wasm32-unknown-unknown --profile "$PROFILE"
rm -rf packages/iroh-fetch/wasm
wasm-bindgen "target/wasm32-unknown-unknown/$PROFILE/web_client.wasm" \
  --out-dir packages/iroh-fetch/wasm --target web --weak-refs
# Optional: shrink the binary further when binaryen's wasm-opt is installed.
if command -v wasm-opt >/dev/null; then
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
    -o packages/iroh-fetch/wasm/web_client_bg.wasm packages/iroh-fetch/wasm/web_client_bg.wasm
fi
ls -lh packages/iroh-fetch/wasm/web_client_bg.wasm
