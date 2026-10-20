#!/usr/bin/env bash
# One-time setup of the iroh-fetch-<platform> npm packages (the native Node.js addon).
#
# npm only lets a package use trusted publishing (GitHub Actions, no token) once the package exists,
# so the very first version of each platform package has to be published by a logged-in maintainer.
# This script does that from the prebuilds of an existing release, connects each package to
# .github/workflows/publish-npm.yml as a trusted publisher, and logs out again. Later releases then
# publish all of them from CI.
#
#   ./scripts/npm-bootstrap-native.sh [version]     # default: packages/iroh-fetch/package.json
#
# Needs: gh (logged in), npm >= 11.10 (for `npm trust`), and an npm account that owns iroh-fetch.
set -euo pipefail
cd "$(dirname "$0")/.."

repo=irohkit/iroh-bridge
workflow=publish-npm.yml
version=${1:-$(node -p "require('./packages/iroh-fetch/package.json').version")}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Some hosts have an unreachable IPv6 DNS server, which makes Node's lookups hang; ask for IPv4 only.
cat > "$work/ipv4.cjs" <<'EOF'
const dns = require("node:dns");
const lookup = dns.lookup;
dns.lookup = (host, options, cb) =>
  typeof options === "function"
    ? lookup.call(dns, host, { family: 4 }, options)
    : lookup.call(dns, host, { ...(typeof options === "number" ? { family: options } : options), family: 4 }, cb);
EOF
export NODE_OPTIONS="--require $work/ipv4.cjs ${NODE_OPTIONS:-}"

echo "==> prebuilds of v$version"
run=$(gh run list -R "$repo" -w "$workflow" -b "v$version" --json databaseId,conclusion \
  -q '[.[] | select(.conclusion == "success")][0].databaseId')
[ -n "$run" ] || { echo "no successful $workflow run for v$version"; exit 1; }
gh run download "$run" -R "$repo" -p 'native-*' -D "$work/prebuilds"
# Lay the packages out from that release's sources, so this checkout's package.json isn't touched.
mkdir "$work/src" && git archive "v$version" | tar -x -C "$work/src"
(cd "$work/src" && node scripts/native-packages.mjs "$work/prebuilds" "$work/npm")

echo "==> npm login"
# npm's web login, done with curl: `npm login` can crash ("Exit handler never called") when it
# falls back to prompting. Open the printed link, sign in and approve; the token goes to ~/.npmrc.
if ! npm whoami >/dev/null 2>&1; then
  start=$(curl -4 -sf -X POST -H 'content-type: application/json' -H 'npm-auth-type: web' \
    -H 'npm-command: login' -d '{}' https://registry.npmjs.org/-/v1/login)
  login_url=$(printf '%s' "$start" | node -e 'process.stdout.write(JSON.parse(require("fs").readFileSync(0)).loginUrl)')
  done_url=$(printf '%s' "$start" | node -e 'process.stdout.write(JSON.parse(require("fs").readFileSync(0)).doneUrl)')
  printf '\nOpen this link, sign in to npm and approve (it expires after about 5 minutes):\n\n  %s\n\n' "$login_url"
  while :; do
    code=$(curl -4 -s -o "$work/done.json" -w '%{http_code}' "$done_url")
    [ "$code" = 200 ] && break
    [ "$code" = 202 ] || { echo "login link expired or was declined (HTTP $code); run the script again"; exit 1; }
    sleep 2
  done
  npm config set //registry.npmjs.org/:_authToken "$(node -p 'require(process.argv[1]).token' "$work/done.json")" >/dev/null
  rm -f "$work/done.json"
fi
echo "logged in as $(npm whoami)"

names=()
for dir in "$work"/npm/*/; do
  name=$(node -p "require('$dir/package.json').name")
  names+=("$name")
  if npm view "$name@$version" version >/dev/null 2>&1; then
    echo "==> $name@$version is already published"
  else
    echo "==> publishing $name@$version"
    (cd "$dir" && npm publish --access public --provenance=false)
  fi
done

for name in "${names[@]}"; do
  if npm trust list "$name" 2>/dev/null | grep -q "$workflow"; then
    echo "==> $name already trusts $repo/$workflow"
  else
    echo "==> trusting $repo/$workflow for $name"
    npm trust github "$name" --repo "$repo" --file "$workflow" --allow-publish --yes
  fi
done

echo "==> logging out (revokes this session's token)"
npm logout || true
echo "Done. The next release (git tag vX.Y.Z) publishes iroh-fetch with these as optionalDependencies."
