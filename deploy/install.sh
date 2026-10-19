#!/usr/bin/env bash
# Installs iroh-gateway as a systemd service that starts on boot.
#
#   sudo ./deploy/install.sh [BACKEND_URL] [path/to/iroh-gateway]
#
# BACKEND_URL defaults to http://127.0.0.1:8080. Without a binary path it uses
# target/release/iroh-gateway (run `cargo build --release -p iroh-gateway` first)
# or one downloaded from the GitHub releases page.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
cd "$(dirname "$0")/.."

BACKEND_URL="${1:-http://127.0.0.1:8080}"
BIN="${2:-target/release/iroh-gateway}"
[ -x "$BIN" ] || { echo "no gateway binary at $BIN" >&2; exit 1; }

id iroh-gateway >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin iroh-gateway
install -m 755 "$BIN" /usr/local/bin/iroh-gateway
sed "s#^Environment=BACKEND_URL=.*#Environment=BACKEND_URL=$BACKEND_URL#" deploy/iroh-gateway.service \
  > /etc/systemd/system/iroh-gateway.service
systemctl daemon-reload
systemctl enable --now iroh-gateway

# The endpoint id is printed once the gateway is online.
for _ in $(seq 30); do
  id=$(journalctl -u iroh-gateway -o cat --no-pager | grep -o 'IROH_ENDPOINT_ID=[0-9a-f]*' | tail -1 || true)
  [ -n "$id" ] && { echo "$id"; exit 0; }
  sleep 1
done
echo "gateway started; see: journalctl -u iroh-gateway" >&2
