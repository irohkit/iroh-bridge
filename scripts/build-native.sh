#!/usr/bin/env bash
# Builds the native Node.js addon (crates/node-client) and copies it to
# packages/iroh-fetch/native/iroh-fetch.<platform>.node, where iroh-fetch looks first.
#
#   scripts/build-native.sh                    # this machine
#   scripts/build-native.sh <rust-target> <platform-tag> [out-dir]
#     e.g. scripts/build-native.sh aarch64-apple-darwin darwin-arm64
#
# The platform tag is what packages/iroh-fetch/src/native.js computes (linux-x64-gnu,
# linux-x64-musl, darwin-arm64, win32-x64-msvc, ...). No @napi-rs/cli needed: a napi addon is just
# a cdylib with a .node extension.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE="${PROFILE:-release}"
TARGET="${1:-}"
TAG="${2:-}"
OUT="${3:-packages/iroh-fetch/native}"

if [ -z "$TAG" ]; then
  TAG=$(node -e 'import("./packages/iroh-fetch/src/native.js").then((m) => console.log(m.platformTag()))')
fi

args=(-p node-client --profile "$PROFILE")
dir="target/$PROFILE"
[ "$PROFILE" = dev ] && dir=target/debug
if [ -n "$TARGET" ]; then
  args+=(--target "$TARGET")
  dir="target/$TARGET/${dir#target/}"
fi
cargo build "${args[@]}"

case "$TAG" in
  win32-*) lib="$dir/node_client.dll" ;;
  darwin-*) lib="$dir/libnode_client.dylib" ;;
  *) lib="$dir/libnode_client.so" ;;
esac
mkdir -p "$OUT"
cp "$lib" "$OUT/iroh-fetch.$TAG.node"
ls -lh "$OUT/iroh-fetch.$TAG.node"
