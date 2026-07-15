FROM rust:1-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/weir /usr/local/bin/weir
# BSL 1.1 requires the License be displayed on each copy of the Licensed Work.
# A distributed image is a copy, so it ships with one.
COPY LICENSE /usr/local/share/weir/LICENSE
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/weir"]
