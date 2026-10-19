# iroh-gateway in Docker

The image `ghcr.io/irohkit/iroh-gateway` runs `iroh-gateway` as a non-root user.
It only makes outbound connections (UDP for iroh, HTTPS for the relays, HTTP to
your backend), so no ports need to be published.

## docker run

```sh
docker run -d --name iroh-gateway --restart unless-stopped \
  -e BACKEND_URL=http://host.docker.internal:8080 \
  --add-host=host.docker.internal:host-gateway \
  -v iroh-gateway:/data \
  ghcr.io/irohkit/iroh-gateway:latest
```

`BACKEND_URL` is not set in the image, so always pass it. Without it the gateway
falls back to `http://127.0.0.1:8080`, which inside a container points at the
container itself. The `--add-host` flag makes `host.docker.internal` resolve to
the Docker host on Linux (Docker Desktop provides it already). Alternatively use
`--network host` and `BACKEND_URL=http://127.0.0.1:8080`.

## docker compose

[`docker-compose.yml`](docker-compose.yml) runs a placeholder backend
(`traefik/whoami`) and the gateway pointing at it over the compose network:

```sh
cd deploy/docker
docker compose up -d            # use the published image
docker compose up -d --build    # or build it from this checkout
```

Replace the `backend` service (or `BACKEND_URL`) with your own service.

## Endpoint id

The gateway prints its endpoint id once it is online. Clients connect to it.

```sh
docker logs iroh-gateway | grep IROH_ENDPOINT_ID
docker compose logs gateway | grep IROH_ENDPOINT_ID
```

## Keeping the endpoint id stable

The secret key is created on first start at `IROH_KEY_FILE` (`/data/key`). Keep
the `/data` volume and the id stays the same across restarts, upgrades and
re-created containers. Removing the volume (`docker volume rm iroh-gateway`,
`docker compose down -v`) generates a new key and a new id. Back up the volume
if the id matters; treat the key file as a secret.

## Multiple upstreams

`BACKEND_URL` accepts several comma-separated URLs. Requests are load-balanced
round-robin, and an upstream that refuses a connection is skipped for
`UPSTREAM_COOLDOWN_SECS` (default 10):

```sh
docker run -d --name iroh-gateway \
  -e BACKEND_URL=http://10.0.0.11:8080,http://10.0.0.12:8080 \
  -e UPSTREAM_COOLDOWN_SECS=30 \
  -v iroh-gateway:/data \
  ghcr.io/irohkit/iroh-gateway:latest
```

## Environment variables

| Variable                 | Default                  | Description                                  |
| ------------------------ | ------------------------ | -------------------------------------------- |
| `BACKEND_URL`            | (must be set)            | Upstream URL(s), comma-separated             |
| `IROH_KEY_FILE`          | `/data/key`              | Secret key file, created on first run        |
| `IROH_RELAY_URL`         | public n0 relays         | Your own relay server (recommended for prod) |
| `MAX_BODY_BYTES`         | `33554432` (32 MiB)      | Maximum request body size                    |
| `UPSTREAM_COOLDOWN_SECS` | `10`                     | How long a failed upstream is skipped        |
| `RUST_LOG`               | `iroh_gateway=info`      | Log filter                                   |

## Building the image

From the repository root:

```sh
docker build -f deploy/docker/Dockerfile -t iroh-gateway .
```

Tagged releases (`v*`) are built for `linux/amd64` and `linux/arm64` and pushed
to GHCR by [`.github/workflows/docker.yml`](../../.github/workflows/docker.yml).
