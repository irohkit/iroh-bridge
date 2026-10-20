#!/usr/bin/env bash
# Installs your own iroh relay with an automatic Let's Encrypt certificate.
#
#   sudo ./deploy/relay/install.sh relay.example.com admin@example.com [path/to/iroh-relay]
#
# Point the DNS name at this machine and open 80/tcp, 443/tcp and 7842/udp first. Without a
# binary path it uses ~/.cargo/bin/iroh-relay or runs
# `cargo install iroh-relay --version 1.3.0 --features server --locked`.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
cd "$(dirname "$0")"

HOST="${1:?usage: install.sh <hostname> <contact-email> [iroh-relay binary]}"
EMAIL="${2:?usage: install.sh <hostname> <contact-email> [iroh-relay binary]}"
[[ "$HOST" =~ ^[A-Za-z0-9.-]+$ ]] || { echo "invalid hostname: $HOST" >&2; exit 1; }
[[ "$EMAIL" =~ ^[^[:space:]\"\\]+@[^[:space:]\"\\]+$ ]] || { echo "invalid email: $EMAIL" >&2; exit 1; }

BIN="${3:-}"
if [ -z "$BIN" ]; then
  owner_home=$(getent passwd "${SUDO_USER:-root}" | cut -d: -f6)
  BIN="$owner_home/.cargo/bin/iroh-relay"
  if [ ! -x "$BIN" ]; then
    sudo -u "${SUDO_USER:-root}" "$owner_home/.cargo/bin/cargo" install iroh-relay --version 1.3.0 --features server --locked
  fi
fi
[ -x "$BIN" ] || { echo "no iroh-relay binary at $BIN" >&2; exit 1; }

id iroh-relay >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin iroh-relay
install -m 755 "$BIN" /usr/local/bin/iroh-relay
install -d -m 755 /etc/iroh-relay
sed -e "s/relay.example.com/$HOST/" -e "s/admin@example.com/$EMAIL/" iroh-relay.toml > /etc/iroh-relay/iroh-relay.toml
chmod 644 /etc/iroh-relay/iroh-relay.toml
install -m 644 iroh-relay.service /etc/systemd/system/iroh-relay.service
systemctl daemon-reload
systemctl enable --now iroh-relay

echo "relay starting; the first certificate can take a minute: journalctl -u iroh-relay -f"
echo "use it with:  IROH_RELAY_URL=https://$HOST  (gateways)  and  relayUrl: \"https://$HOST\"  (clients)"
