# syntax=docker/dockerfile:1
#
# Container image for iroh-gateway.
#
# Build from the repository root:
#   docker build -f deploy/docker/Dockerfile -t iroh-gateway .
#
# Run (BACKEND_URL is required in practice; see deploy/docker/README.md):
#   docker run -d -e BACKEND_URL=http://host.docker.internal:8080 \
#     --add-host=host.docker.internal:host-gateway -v iroh-gateway:/data iroh-gateway

ARG RUST_VERSION=1.98
ARG DEBIAN_RELEASE=bookworm

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS build
WORKDIR /src

# The whole workspace is copied because cargo needs every member manifest to
# resolve it. Only iroh-gateway (and its path dependency proto) is compiled;
# the wasm crate crates/web-client is never built here.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Cache mounts keep the registry and target dir between builds. The binary is
# copied out of the cached target dir because cache mounts are not part of the
# resulting layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked -p iroh-gateway \
    && install -D -m 0755 target/release/iroh-gateway /out/iroh-gateway

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 iroh \
    && useradd --system --uid 10001 --gid iroh --home-dir /data --shell /usr/sbin/nologin iroh \
    && mkdir -p /data \
    && chown iroh:iroh /data

COPY --from=build /out/iroh-gateway /usr/local/bin/iroh-gateway

LABEL org.opencontainers.image.title="iroh-gateway" \
      org.opencontainers.image.description="Expose a local HTTP backend over iroh" \
      org.opencontainers.image.source="https://github.com/irohkit/iroh-bridge" \
      org.opencontainers.image.url="https://github.com/irohkit/iroh-bridge" \
      org.opencontainers.image.documentation="https://github.com/irohkit/iroh-bridge/tree/main/deploy/docker" \
      org.opencontainers.image.licenses="MIT"

# The secret key lives in the /data volume so the endpoint id survives
# container re-creation. Keep the volume to keep the id.
ENV IROH_KEY_FILE=/data/key \
    RUST_LOG=iroh_gateway=info
# BACKEND_URL is intentionally not set here. Pass it at run time, e.g.
#   -e BACKEND_URL=http://backend:8080
# (comma-separate several URLs to load-balance). If it is missing, the binary
# falls back to http://127.0.0.1:8080, which inside a container is the
# container itself and is almost never what you want.
# Other optional settings: IROH_RELAY_URL, MAX_BODY_BYTES, UPSTREAM_COOLDOWN_SECS.

VOLUME ["/data"]
WORKDIR /data
USER iroh:iroh

# The gateway only makes outbound connections (UDP for iroh, HTTPS for the
# relays, HTTP to the backend), so no ports are exposed.

# The gateway shuts down cleanly on Ctrl-C (SIGINT); it has no SIGTERM handler,
# so make `docker stop` send SIGINT instead of waiting for the kill timeout.
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/iroh-gateway"]
