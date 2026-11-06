# syntax=docker/dockerfile:1
# This image contains application code only. Production endpoints and secrets
# belong in the operator's private deployment repository.
FROM rust:slim-bookworm@sha256:86234e857a413004348b471d11a4ebe1aa92d2eae2b6b143cac68fd1ac3c48be AS build
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
COPY web3_proxy/tests web3_proxy/tests
COPY web3_proxy_cli/Cargo.toml web3_proxy_cli/Cargo.toml
COPY web3_proxy_cli/src web3_proxy_cli/src
COPY web3_proxy_cli/tests web3_proxy_cli/tests
COPY config/example.toml config/example.toml
COPY docs/block-relay.example.toml docs/block-relay.example.toml

# Compile all test targets; run mock-only libraries and CLI filesystem tests.
# Never install or start Anvil, an execution client, or a Beacon node here.
FROM build AS checked
# A cold cache needs fresh execution even when source layers match. A changing
# argument preserves mounts; --no-cache would prune their contents.
ARG CARGO_CACHE_SEED=
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    printf 'Cargo cache seed: %s\n' "$CARGO_CACHE_SEED" && \
    cargo fmt --all --check && \
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings && \
    cargo test --locked --all-features -p web3_proxy -p deduped_broadcast -p latency \
      -p web3_proxy_cli --lib --bin web3_proxy_cli --test compare_rpcs

FROM checked AS binary
ARG CARGO_CACHE_SEED=
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    printf 'Cargo cache seed: %s\n' "$CARGO_CACHE_SEED" && \
    cargo build --release --locked -p web3_proxy_cli --features mimalloc && \
    install -m 0755 target/release/web3_proxy_cli /usr/local/bin/web3_proxy_cli

FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS runtime
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates curl libssl3 && \
    sed -i -e 's|^mozilla/emSign_ECC_Root_CA_-_G3.crt|!&|' \
      -e 's|^mozilla/Comodo_AAA_Services_root.crt|!&|' /etc/ca-certificates.conf && \
    update-ca-certificates && \
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
