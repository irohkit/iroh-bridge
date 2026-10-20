#!/bin/sh
# Installs the iroh-gateway binary from the GitHub releases.
#
#   curl -fsSL https://raw.githubusercontent.com/irohkit/iroh-bridge/main/scripts/install-gateway.sh | sh
#   curl -fsSL .../install-gateway.sh | sudo sh -s -- --systemd http://127.0.0.1:8080
#
# Options:
#   --systemd BACKEND_URL   also install and start the systemd service (needs root; Linux only)
#   --version vX.Y.Z        install this release instead of the latest
#   --dir DIR               install the binary into DIR
#
# Environment (same meaning as the options):
#   IROH_GATEWAY_VERSION      release tag, default: the latest release
#   IROH_GATEWAY_INSTALL_DIR  default: /usr/local/bin as root or when writable, else ~/.local/bin
#   IROH_GATEWAY_BASE_URL     where release assets live, as BASE/<tag>/<asset>; an http(s) URL or a
#                             local directory (default: https://github.com/irohkit/iroh-bridge/releases/download)
#   IROH_GATEWAY_RAW_URL      where deploy/install.sh is fetched from for --systemd, as RAW/<ref>/deploy/install.sh
#                             (default: https://raw.githubusercontent.com/irohkit/iroh-bridge)
#
# Needs only: curl or wget, tar, sha256sum or shasum.
set -eu

REPO=irohkit/iroh-bridge
VERSION=${IROH_GATEWAY_VERSION:-}
INSTALL_DIR=${IROH_GATEWAY_INSTALL_DIR:-}
BASE_URL=${IROH_GATEWAY_BASE_URL:-https://github.com/$REPO/releases/download}
RAW_URL=${IROH_GATEWAY_RAW_URL:-https://raw.githubusercontent.com/$REPO}
SYSTEMD_BACKEND=

say() { printf '%s\n' "$*"; }
err() { printf 'install-gateway: %s\n' "$*" >&2; }
die() { err "$*"; exit 1; }

usage() {
  sed -n '2,20s/^# \{0,1\}//p' "$0" 2>/dev/null || true
  say "usage: install-gateway.sh [--systemd BACKEND_URL] [--version vX.Y.Z] [--dir DIR]"
}

while [ $# -gt 0 ]; do
  case $1 in
    --systemd) [ $# -ge 2 ] || die "--systemd needs a BACKEND_URL, e.g. --systemd http://127.0.0.1:8080"
      SYSTEMD_BACKEND=$2; shift 2 ;;
    --systemd=*) SYSTEMD_BACKEND=${1#*=}; shift ;;
    --version) [ $# -ge 2 ] || die "--version needs a tag"; VERSION=$2; shift 2 ;;
    --version=*) VERSION=${1#*=}; shift ;;
    --dir) [ $# -ge 2 ] || die "--dir needs a directory"; INSTALL_DIR=$2; shift 2 ;;
    --dir=*) INSTALL_DIR=${1#*=}; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }
  fetch_stdout() { curl -fsSL --retry 3 "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q -O "$2" "$1"; }
  fetch_stdout() { wget -q -O - "$1"; }
else
  die "needs curl or wget"
fi

# Downloads $1 to $2. Local paths are copied, so the script can be tested against freshly built
# archives.
get() {
  case $1 in
    http://*|https://*) fetch "$1" "$2" ;;
    file://*) cp "${1#file://}" "$2" 2>/dev/null ;;
    *) cp "$1" "$2" 2>/dev/null ;;
  esac
}

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  die "needs sha256sum or shasum to verify the download"
fi
command -v tar >/dev/null 2>&1 || die "needs tar"

# --- platform -------------------------------------------------------------------------------
os=$(uname -s)
arch=$(uname -m)
case $arch in
  x86_64|amd64) arch=x86_64 ;;
  aarch64|arm64) arch=aarch64 ;;
  *) die "no prebuilt iroh-gateway for $arch; build it with: cargo build --release -p iroh-gateway" ;;
esac
case $os in
  Linux)
    # The static musl build runs everywhere (Alpine, old glibc); older releases only have glibc builds.
    if ls /lib/ld-musl-* >/dev/null 2>&1 || (ldd --version 2>&1 | grep -qi musl); then
      targets="$arch-unknown-linux-musl"
    else
      targets="$arch-unknown-linux-musl $arch-unknown-linux-gnu"
    fi ;;
  Darwin)
    # A shell under Rosetta reports x86_64 on Apple silicon; the native build is better there.
    if [ "$arch" = x86_64 ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = 1 ]; then
      arch=aarch64
    fi
    targets="$arch-apple-darwin" ;;
  MINGW*|MSYS*|CYGWIN*)
    die "on Windows use PowerShell: irm https://raw.githubusercontent.com/$REPO/main/scripts/install-gateway.ps1 | iex" ;;
  *) die "no prebuilt iroh-gateway for $os; build it with: cargo build --release -p iroh-gateway" ;;
esac

# --- checks before downloading ------------------------------------------------------------
is_root=false
[ "$(id -u)" = 0 ] && is_root=true
if [ -n "$SYSTEMD_BACKEND" ]; then
  [ "$os" = Linux ] || die "--systemd is only for Linux"
  command -v systemctl >/dev/null 2>&1 || die "--systemd: systemctl not found (no systemd on this machine?)"
  command -v bash >/dev/null 2>&1 || die "--systemd needs bash for deploy/install.sh"
  $is_root || die "--systemd installs a system service and needs root. Rerun with sudo:
  curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install-gateway.sh | sudo sh -s -- --systemd $SYSTEMD_BACKEND"
  # The unit file runs /usr/local/bin/iroh-gateway.
  [ -z "$INSTALL_DIR" ] || [ "$INSTALL_DIR" = /usr/local/bin ] \
    || die "--systemd always installs to /usr/local/bin; drop IROH_GATEWAY_INSTALL_DIR / --dir"
  INSTALL_DIR=/usr/local/bin
fi
if [ -z "$INSTALL_DIR" ]; then
  if $is_root || { [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; }; then
    INSTALL_DIR=/usr/local/bin
  else
    [ -n "${HOME:-}" ] || die "HOME is not set; choose a directory with IROH_GATEWAY_INSTALL_DIR"
    INSTALL_DIR=$HOME/.local/bin
  fi
fi

if [ -z "$VERSION" ]; then
  VERSION=$(fetch_stdout "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1) || true
  [ -n "$VERSION" ] || die "could not look up the latest release; set IROH_GATEWAY_VERSION=vX.Y.Z"
fi
case $VERSION in v*) ;; *) VERSION=v$VERSION ;; esac

# --- download and verify --------------------------------------------------------------------
tmp=$(mktemp -d 2>/dev/null || mktemp -d -t iroh-gateway)
trap 'rm -rf "$tmp"' EXIT
trap 'exit 130' INT TERM

asset=
for target in $targets; do
  name="iroh-gateway-$VERSION-$target"
  if get "$BASE_URL/$VERSION/$name.tar.gz" "$tmp/$name.tar.gz" 2>/dev/null; then
    asset=$name
    break
  fi
  rm -f "$tmp/$name.tar.gz"
  say "no $target build in $VERSION"
done
[ -n "$asset" ] || die "no iroh-gateway $VERSION release asset for $(echo "$targets" | sed 's/ / or /') (looked in $BASE_URL/$VERSION/)"
say "downloaded $asset.tar.gz"

get "$BASE_URL/$VERSION/$asset.tar.gz.sha256" "$tmp/$asset.tar.gz.sha256" || die "could not download $asset.tar.gz.sha256"
expected=$(cut -d' ' -f1 < "$tmp/$asset.tar.gz.sha256" | tr -d '\r\n')
actual=$(sha256 "$tmp/$asset.tar.gz")
if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
  die "checksum mismatch for $asset.tar.gz: expected $expected, got $actual"
fi
say "sha256 verified"

mkdir "$tmp/x"
tar -xzf "$tmp/$asset.tar.gz" -C "$tmp/x"
bin="$tmp/x/$asset/iroh-gateway"
[ -f "$bin" ] || die "$asset.tar.gz does not contain $asset/iroh-gateway"
chmod 755 "$bin"

# --- install ----------------------------------------------------------------------------------
if [ -n "$SYSTEMD_BACKEND" ]; then
  # deploy/install.sh does the system part (user, unit, state dir) and installs the binary.
  mkdir "$tmp/repo" "$tmp/repo/deploy"
  if [ -f "$tmp/x/$asset/iroh-gateway.service" ]; then
    cp "$tmp/x/$asset/iroh-gateway.service" "$tmp/repo/deploy/"
  fi
  for ref in "$VERSION" main; do
    get "$RAW_URL/$ref/deploy/install.sh" "$tmp/repo/deploy/install.sh" || continue
    [ -f "$tmp/repo/deploy/iroh-gateway.service" ] \
      || get "$RAW_URL/$ref/deploy/iroh-gateway.service" "$tmp/repo/deploy/iroh-gateway.service" || continue
    break
  done
  if [ ! -s "$tmp/repo/deploy/install.sh" ] || [ ! -s "$tmp/repo/deploy/iroh-gateway.service" ]; then
    die "could not download deploy/install.sh and deploy/iroh-gateway.service from $RAW_URL"
  fi
  say "installing the systemd service (backend $SYSTEMD_BACKEND)"
  bash "$tmp/repo/deploy/install.sh" "$SYSTEMD_BACKEND" "$bin"
  say ""
  say "iroh-gateway $VERSION is installed at /usr/local/bin/iroh-gateway and running as a systemd service."
  say "  status:       systemctl status iroh-gateway"
  say "  endpoint id:  journalctl -u iroh-gateway | grep IROH_ENDPOINT_ID"
  say "  settings:     /etc/systemd/system/iroh-gateway.service (then: systemctl restart iroh-gateway)"
  exit 0
fi

if ! mkdir -p "$INSTALL_DIR" 2>/dev/null || [ ! -w "$INSTALL_DIR" ]; then
  die "cannot write to $INSTALL_DIR. Rerun as root (curl ... | sudo sh) or pick a directory you own:
  curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install-gateway.sh | IROH_GATEWAY_INSTALL_DIR=\$HOME/.local/bin sh"
fi
# Copy then rename, so a running gateway keeps its old binary and nobody sees a half-written file.
cp "$bin" "$INSTALL_DIR/.iroh-gateway.new"
mv -f "$INSTALL_DIR/.iroh-gateway.new" "$INSTALL_DIR/iroh-gateway"

say ""
say "iroh-gateway $VERSION installed to $INSTALL_DIR/iroh-gateway"
case ":${PATH:-}:" in
  *":$INSTALL_DIR:"*) cmd=iroh-gateway ;;
  *) cmd="$INSTALL_DIR/iroh-gateway"
     say "note: $INSTALL_DIR is not on your PATH; add it with: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
say ""
say "Next steps:"
say "  run it next to your backend:   BACKEND_URL=http://127.0.0.1:8080 $cmd"
say "  all options:                   $cmd --help"
if [ "$os" = Linux ] && command -v systemctl >/dev/null 2>&1; then
  say "  run it as a systemd service:   curl -fsSL https://raw.githubusercontent.com/$REPO/main/scripts/install-gateway.sh | sudo sh -s -- --systemd http://127.0.0.1:8080"
fi
say "It prints IROH_ENDPOINT_ID=... once online; clients connect with that id. Docs: https://github.com/$REPO#backend-iroh-gateway"
