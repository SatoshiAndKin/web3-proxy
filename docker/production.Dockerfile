# syntax=docker/dockerfile:1
# This image contains application code only. Production endpoints and secrets
# belong in the operator's private deployment repository.
FROM rust:slim-bookworm@sha256:ebd900bae66fd508b466cef82d64a83a5fb34682e4c8b2797a42908bddc95a57 AS build
WORKDIR /app
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential clang cmake pkg-config libssl-dev ca-certificates
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain && rustup component add clippy rustfmt
ENV CARGO_BUILD_JOBS=2 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0 \
    CARGO_PROFILE_RELEASE_DEBUG=0 \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    RUSTFLAGS="--cfg tokio_unstable --cfg uuid_unstable -C target-cpu=x86-64"
COPY Cargo.toml Cargo.lock ./
COPY deduped_broadcast/Cargo.toml deduped_broadcast/Cargo.toml
COPY deduped_broadcast/src deduped_broadcast/src
COPY latency/Cargo.toml latency/Cargo.toml
COPY latency/src latency/src
COPY web3_proxy/Cargo.toml web3_proxy/Cargo.toml
COPY web3_proxy/src web3_proxy/src
COPY web3_proxy_cli/Cargo.toml web3_proxy_cli/Cargo.toml
COPY web3_proxy_cli/src web3_proxy_cli/src
COPY web3_proxy_cli/tests web3_proxy_cli/tests
COPY docs/block-relay.example.toml docs/block-relay.example.toml

# Compile all test targets, but run only these mock-only library suites.
# Never install or start Anvil, an execution client, or a Beacon node here.
FROM build AS checked
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo fmt --all --check && \
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings && \
    cargo test --locked --all-features -p web3_proxy -p deduped_broadcast -p latency --lib

FROM checked AS binary
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p web3_proxy_cli && \
    install -m 0755 target/release/web3_proxy_cli /usr/local/bin/web3_proxy_cli

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates curl libssl3 && \
    useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin web3proxy && \
    install -d -o web3proxy -g web3proxy /var/lib/web3-proxy
COPY --from=binary /usr/local/bin/web3_proxy_cli /usr/local/bin/web3_proxy_cli
COPY LICENSE /usr/share/doc/web3-proxy/LICENSE
ARG VCS_REF
LABEL org.opencontainers.image.source="https://github.com/SatoshiAndKin/web3-proxy" \
      org.opencontainers.image.revision=$VCS_REF
USER 10001:10001
WORKDIR /var/lib/web3-proxy
ENV RUST_LOG="info,alloy_transport=error"
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/web3_proxy_cli"]
CMD ["--config", "/run/config/proxy.toml", "proxyd", "--port", "8544"]
