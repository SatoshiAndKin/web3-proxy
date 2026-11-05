# syntax=docker/dockerfile:1
# Run this helper with --no-cache: mount contents do not enter RUN cache keys.
# Use only the same two cache mounts as production.Dockerfile. Never copy the
# build context, Cargo credentials, source tree, or runtime configuration.
FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS base

FROM base AS restore
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=bind,target=/cache \
    cd /cache && sha256sum --check SHA256SUMS && \
    tar --extract --same-owner --same-permissions --file registry.tar --directory /usr/local/cargo/registry && \
    tar --extract --same-owner --same-permissions --file target.tar --directory /app/target

FROM base AS archive
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    mkdir /archives && \
    tar --format=posix --create --file /archives/registry.tar --directory /usr/local/cargo/registry . && \
    tar --format=posix --create --file /archives/target.tar --directory /app/target . && \
    cd /archives && sha256sum registry.tar target.tar > SHA256SUMS

FROM scratch AS export
COPY --from=archive /archives/ /
