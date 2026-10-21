#!/usr/bin/env bash
# Runs the full iroh-fetch test suite end to end on one machine, the way the demo runs, but with
# everything local and throwaway: a token-protected iroh-relay in dev mode, the two demo backends,
# and three gateways (A1 and B on the local relay, A2 on n0's public relays as a standby).
#
#   scripts/e2e-local.sh                 # both engines
#   ENGINES=wasm scripts/e2e-local.sh    # one engine
#
# Needs, already built: the gateway (GATEWAY_BIN, default target/release/iroh-gateway), iroh-relay
# (RELAY_BIN, default iroh-relay on PATH), packages/iroh-fetch/wasm (scripts/build-wasm.sh) and,
# for the native engine, packages/iroh-fetch/native (scripts/build-native.sh); plus
# `npm ci` in examples/backend. Ports: 3340 (relay), 18080/18081 (backends), 19181-19183 (admin).
# Exits non-zero if a test fails; logs are kept in $E2E_LOG_DIR (default: a temp dir) on failure.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

GATEWAY_BIN=${GATEWAY_BIN:-$root/target/release/iroh-gateway}
RELAY_BIN=${RELAY_BIN:-iroh-relay}
ENGINES=${ENGINES:-wasm,native}
RELAY_PORT=${RELAY_PORT:-3340}
logs=${E2E_LOG_DIR:-$(mktemp -d)}
mkdir -p "$logs"
relay_url="http://127.0.0.1:$RELAY_PORT"

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

wait_for() { # description, seconds, command...
  local what=$1 secs=$2
  shift 2
  for _ in $(seq "$secs"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "timed out waiting for $what" >&2
  return 1
}

# Fresh tokens each run: one for gateways, one for clients, as in deploy/relay/README.md.
gw_token="gw_$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
web_token="web_$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
cat > "$logs/relay.toml" <<EOF
enable_relay = true
http_bind_addr = "127.0.0.1:$RELAY_PORT"
enable_quic_addr_discovery = false
enable_metrics = false
[access]
shared_token = ["$gw_token", "$web_token"]
EOF

echo "==> relay ($("$RELAY_BIN" --version 2>/dev/null || echo "$RELAY_BIN")) on $relay_url"
"$RELAY_BIN" --dev -c "$logs/relay.toml" > "$logs/relay.log" 2>&1 &
pids+=($!)
wait_for "the relay" 30 curl -s -o /dev/null "$relay_url/"

echo "==> backends A (18080) and B (18081)"
(cd examples/backend && PORT=18080 BACKEND_NAME=A exec node server.mjs) > "$logs/backend-a.log" 2>&1 &
pids+=($!)
(cd examples/backend && PORT=18081 BACKEND_NAME=B exec node server.mjs) > "$logs/backend-b.log" 2>&1 &
pids+=($!)
wait_for "backend A" 30 curl -sf http://127.0.0.1:18080/hello
wait_for "backend B" 30 curl -sf http://127.0.0.1:18081/hello

gateway() { # name backend admin-port [local]
  local name=$1 backend=$2 admin=$3 relay=${4:-}
  local env=(IROH_KEY_FILE="$logs/$name.key" BACKEND_URL="$backend" IROH_ADMIN_ADDR="127.0.0.1:$admin")
  [ -n "$relay" ] && env+=(IROH_RELAY_URL="$relay_url" IROH_RELAY_TOKEN="$gw_token")
  env "${env[@]}" "$GATEWAY_BIN" > "$logs/gateway-$name.log" 2>&1 &
  pids+=($!)
}
echo "==> gateways ($("$GATEWAY_BIN" --version)): A1 and B on the local relay, A2 on n0's"
gateway a1 http://127.0.0.1:18080 19181 local
gateway a2 http://127.0.0.1:18080 19182
gateway b http://127.0.0.1:18081 19183 local
endpoint_id() { sed -n 's/^IROH_ENDPOINT_ID=\([0-9a-f]\{64\}\)$/\1/p' "$logs/gateway-$1.log"; }
for name in a1 a2 b; do
  wait_for "gateway $name" 90 grep -q '^IROH_ENDPOINT_ID=' "$logs/gateway-$name.log"
done
A1=$(endpoint_id a1) A2=$(endpoint_id a2) B=$(endpoint_id b)
# A2's home relay, so the relay-hint tests hint the right one.
A2_RELAY=$(curl -s http://127.0.0.1:19182/status | node -e 'const s = JSON.parse(require("fs").readFileSync(0)); process.stdout.write((s.home_relays?.[0]?.url ?? "").replace(/\.?\/$/, ""))')
echo "    A1 ${A1:0:10}…  A2 ${A2:0:10}… (${A2_RELAY:-no relay yet})  B ${B:0:10}…"

failed=0
IFS=, read -ra engines <<< "$ENGINES"
for engine in "${engines[@]}"; do
  echo "==> iroh-fetch tests, $engine engine"
  if (cd packages/iroh-fetch && env IROH_FETCH_BACKEND="$engine" NODE_OPTIONS="--dns-result-order=ipv4first ${NODE_OPTIONS:-}" \
      IROH_RELAY_URL="$relay_url" IROH_RELAY_TOKEN="$web_token" IROH_ENDPOINT_ID="$A1" IROH_ENDPOINT_ID_A2="$A2" \
      IROH_ENDPOINT_ID_B="$B" ${A2_RELAY:+IROH_RELAY_URL_A2="$A2_RELAY"} npm test) > "$logs/test-$engine.log" 2>&1; then
    echo "    $(grep -E '^ℹ (pass|fail|skipped)' "$logs/test-$engine.log" | tr '\n' ' ')"
  else
    failed=1
    echo "    FAILED: $(grep -E '^ℹ (pass|fail|skipped)' "$logs/test-$engine.log" | tr '\n' ' ')"
    grep -E '^✖' "$logs/test-$engine.log" | sort -u | sed 's/^/    /' || true
  fi
done

if [ "$failed" = 1 ]; then
  echo "logs: $logs" >&2
  exit 1
fi
[ -n "${E2E_LOG_DIR:-}" ] || rm -rf "$logs"
echo "==> all passed"
