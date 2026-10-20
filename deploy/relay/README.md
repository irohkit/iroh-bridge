# Running your own iroh relay

By default everything uses **n0's public relays**: if you don't set a relay, `iroh-gateway`, `iroh-fetch` and
`iroh-proxy` all work out of the box. This guide is for when you want your own relay: it's much faster for users close
to it, removes a shared dependency, and can be limited to clients that hold a token.

Measured on the demo with the same gateways and backend:

| | n0 public relay | own relay (small VPS, same country) |
|---|---|---|
| request latency (p50) | 69 ms | 14 ms |
| upload throughput | 1.3 MB/s | 11.5 MB/s |

**Contents**

1. [What you need](#1-what-you-need)
2. [Install: a server with nothing on 80/443](#2-install-a-server-with-nothing-on-80443)
3. [Install: next to an existing reverse proxy](#3-install-next-to-an-existing-reverse-proxy)
4. [Point gateways and clients at it](#4-point-gateways-and-clients-at-it)
5. [Tokens: only let authorized clients use it](#5-tokens-only-let-authorized-clients-use-it)
6. [Check that it works](#6-check-that-it-works)
7. [Redundancy: surviving a relay outage](#7-redundancy-surviving-a-relay-outage)
8. [Operations](#8-operations)
9. [Troubleshooting](#9-troubleshooting)

## 1. What you need

- A Linux server with a **public IP**. A small VPS is plenty (the demo relay runs next to other services on 2 cores
  and 3 GB of RAM).
- A **DNS name** pointing at it, e.g. `relay.example.com`. If you have none yet, `<ip-with-dashes>.sslip.io` resolves to
  your IP (e.g. `relay.203-0-113-7.sslip.io`) and works with Let's Encrypt.
- Open ports: **443/tcp** (browsers and gateways connect here; also used for Let's Encrypt), **80/tcp**, and
  optionally **7842/udp** (QUIC address discovery, which helps native clients find direct paths).

Pick the same relay for all gateways and clients of an app.

## 2. Install: a server with nothing on 80/443

The relay terminates TLS itself and gets its Let's Encrypt certificate automatically.

```bash
git clone https://github.com/irohkit/iroh-bridge && cd iroh-bridge

# open relay (anyone may use it):
sudo ./deploy/relay/install.sh relay.example.com you@example.com

# or token-protected, with two fresh tokens printed at the end (see section 5):
sudo RELAY_TOKENS=generate ./deploy/relay/install.sh relay.example.com you@example.com
```

The script:

- installs `iroh-relay` 1.3 (from `~/.cargo/bin`, or with `cargo install iroh-relay --features server`; pass a binary path as
  the third argument to use another one);
- writes [`iroh-relay.toml`](iroh-relay.toml) to `/etc/iroh-relay/` with your hostname and email;
- runs it as a sandboxed systemd service ([`iroh-relay.service`](iroh-relay.service)) under its own `iroh-relay` user,
  whose only privilege is binding ports 80/443.

The first certificate takes up to a minute: `journalctl -u iroh-relay -f`.

## 3. Install: next to an existing reverse proxy

If Caddy, nginx or Traefik already owns 80/443 (common on a server that hosts other sites), run the relay as a container
behind it. The proxy keeps handling TLS, and other sites are untouched. This is how the demo relay runs.

1. Copy [`docker-compose.yml`](docker-compose.yml) and [`iroh-relay-behind-proxy.toml`](iroh-relay-behind-proxy.toml) to
   the server, e.g. into `/opt/iroh-relay/`.
2. In `docker-compose.yml`, set the network name to the Docker network your proxy container is on
   (`docker network ls`; for a compose project called `web` it's usually `web_default`).
3. Edit the tokens in `iroh-relay-behind-proxy.toml` (section 5), or delete the `[access]` table for an open relay.
4. Start it: `docker compose up -d`. It publishes no host ports.
5. Add a site to the proxy that forwards to `iroh-relay:3340`. WebSocket upgrades must pass through, which they do by
   default in Caddy and Traefik.

   **Caddy:**
   ```
   relay.example.com {
       reverse_proxy iroh-relay:3340
   }
   ```
   then `docker exec <caddy> caddy reload --config /etc/caddy/Caddyfile`. Caddy gets the certificate.

   **nginx:**
   ```nginx
   server {
       listen 443 ssl;
       server_name relay.example.com;
       # ssl_certificate ... (e.g. from certbot)
       location / {
           proxy_pass http://iroh-relay:3340;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection "upgrade";
           proxy_set_header Host $host;
           proxy_read_timeout 1h;
       }
   }
   ```

If your proxy runs directly on the host instead of in Docker, publish the relay on loopback only
(`ports: ["127.0.0.1:3340:3340"]`) and proxy to `127.0.0.1:3340`.

## 4. Point gateways and clients at it

Every gateway:

```bash
IROH_RELAY_URL=https://relay.example.com iroh-gateway
# systemd: add  Environment=IROH_RELAY_URL=https://relay.example.com  to the unit
# Docker:  -e IROH_RELAY_URL=https://relay.example.com
```

Clients:

```ts
createIrohFetch({ relayUrl: "https://relay.example.com", backends: { ... } });
// React:    <IrohProvider relayUrl="https://relay.example.com" ...>
// Next.js example: NEXT_PUBLIC_IROH_RELAY_URL=https://relay.example.com
// iroh-proxy: --relay-url https://relay.example.com
```

Moving the gateways alone already makes things fast for everyone: a client reaches a gateway through the gateway's relay,
even if the client itself still uses the public relays (as long as your relay is open, see section 5).

## 5. Tokens: only let authorized clients use it

With tokens, the relay refuses any client that doesn't present one of them. Relays only forward end-to-end encrypted
traffic, so this is about who may use your relay and its bandwidth, not about who may read your data.

**Generate** one token per audience, so each can be revoked on its own, e.g. a secret one for gateways and servers, and
one for the web app:

```bash
echo "gw_$(openssl rand -hex 24)"    # gateways, iroh-proxy, server-side Node: keep secret
echo "web_$(openssl rand -hex 16)"   # browser apps: visible in the page source
```

**Configure the relay:** in `/etc/iroh-relay/iroh-relay.toml` (section 2) or `iroh-relay-behind-proxy.toml` (section 3),
remove `access = "everyone"` if it's there and add:

```toml
[access]
shared_token = ["gw_...", "web_..."]
```

Restart: `sudo systemctl restart iroh-relay`, or `docker compose up -d --force-recreate`. Keep the file readable only
by root and the relay (`chmod 640`), since it holds the tokens. The install script with `RELAY_TOKENS=...` does all of
this for you.

**Give each side its token:**

```bash
# gateway
IROH_RELAY_URL=https://relay.example.com IROH_RELAY_TOKEN=gw_... iroh-gateway
```

For systemd, keep the token out of the world-readable unit file. Put it in an environment file that only root can read:

```bash
echo 'IROH_RELAY_TOKEN=gw_...' | sudo tee /etc/iroh-gateway.env >/dev/null && sudo chmod 600 /etc/iroh-gateway.env
# in the unit:  EnvironmentFile=/etc/iroh-gateway.env
```

```ts
// browser / Node client
createIrohFetch({ relayUrl: "https://relay.example.com", relayToken: "web_...", backends: { ... } });
// React: <IrohProvider relayUrl="..." relayToken="..." ...>
// Next.js example: NEXT_PUBLIC_IROH_RELAY_URL + NEXT_PUBLIC_IROH_RELAY_TOKEN
```

```bash
# iroh-proxy
iroh-proxy <endpoint id> --relay-url https://relay.example.com --relay-token gw_...
# or IROH_RELAY_TOKEN=gw_... iroh-proxy ...
```

**Rotate or revoke:** add the new token to `shared_token`, restart the relay, switch the clients over, then remove the
old token and restart again. Removing a token cuts off everyone using it.

**What a token does and doesn't protect.** A token embedded in a public web page can be read by anyone who opens the
page, so it only stops casual use of your relay. It does not decide who may reach your data. Do that in the backend
(sessions, JWT; the gateway forwards `Authorization` and cookies), and, for server-to-server setups, with the gateway's
`IROH_ALLOW_CLIENTS` allow-list. For a private web app, serve the token only to logged-in users (e.g. from your own
login endpoint) rather than baking it into the bundle.

Other access modes in the relay config: `access.allowlist = ["<endpoint id>", ...]` (fixed clients only),
`access.denylist = [...]`, or `access.http = { url = "https://auth.example.com/relay" }` to ask your own service about
each endpoint id.

## 6. Check that it works

```bash
curl -sI https://relay.example.com/ | head -1       # relay answers over HTTPS (HTTP/2 200)
journalctl -u iroh-relay -n 20                      # or: docker logs iroh-relay
```

With tokens, a client without one fails right away with:

```
iroh-fetch: could not reach gateway ... (relay https://relay.example.com/ refused this client: not authorized)
```

and the relay logs `The relay denied our authentication (not authorized)`. The test suite can run against your relay:

```bash
cd packages/iroh-fetch
IROH_RELAY_URL=https://relay.example.com IROH_RELAY_TOKEN=web_... IROH_ENDPOINT_ID=<gateway id> npm test
```

## 7. Redundancy: surviving a relay outage

A single relay is a single point of failure for browser users. The dependable pattern is to run **two gateways per
backend on different relays** and let clients fail over between them:

```bash
# on the backend machine (or two machines)
IROH_RELAY_URL=https://relay.example.com IROH_RELAY_TOKEN=gw_... IROH_KEY_FILE=/var/lib/iroh-gateway/key iroh-gateway   # primary
IROH_KEY_FILE=/var/lib/iroh-gateway/key-standby iroh-gateway                                                            # standby on n0's public relays
```

```ts
createIrohFetch({
  relayUrl: ["https://relay.example.com", "n0"],   // own relay, plus the public relays to reach the standby
  relayToken: "web_...",
  backends: {
    "https://api.example.com": { endpointIds: ["<primary id>", "<standby id>"], strategy: "failover" },
  },
});
```

What happens when your relay dies (tested by stopping the demo relay for 30 s while a client kept calling two backends):

- clients notice the dead connection within about 5 seconds (1 s keep-alives) and move body-less requests to the
  standby; at most the request that was in flight at that moment is delayed or fails;
- everything then flows through the standby on the public relays;
- when your relay is back, clients reconnect to the primary in the background and use it again.

In the test, 624 requests over 130 s saw one failure (the in-flight request hit its 8 s timeout) and two slow requests.

`relayUrl` / `IROH_RELAY_URL` also accept several relays, e.g. `https://relay-a.example.com,https://relay-b.example.com,n0`
(`n0` = the public relays; your token is never sent to them). An endpoint then picks the lowest-latency relay that
works. iroh decides that by measurement, not by list order, so use the two-gateway pattern above when you need to know
which relay is primary.

## 8. Operations

- **Upgrades:** change the version (`cargo install iroh-relay --version X --features server --locked` then restart, or
  the image tag in `docker-compose.yml`). Keep relay and clients on compatible iroh versions (this project uses 1.3).
- **Limits:** `[limits]` in the config caps new connections per second and each client's upload rate (defaults: 50/s,
  10 MiB/s per client). Raise them for heavy traffic.
- **Metrics:** Prometheus metrics on `127.0.0.1:9090` (section 2 setup). Reach them over SSH; don't expose them.
- **Several relays:** for users in different regions, run one relay per region and point each region's gateways at
  the nearest one. Clients reach any gateway through that gateway's relay.

## 9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `refused this client: not authorized` | missing or wrong token on that client or gateway; token removed from the relay |
| clients time out, gateway logs nothing | gateway and clients disagree on the relay, or the gateway can't reach it (`IROH_RELAY_URL` typo, firewall) |
| no certificate (section 2) | DNS not pointing at the server yet, or port 443 blocked/taken |
| `502` from the proxy (section 3) | relay container not on the proxy's network, or wrong upstream name/port |
| works with curl, not in browsers | proxy strips WebSocket upgrade headers (see the nginx snippet) |
