# Build stage: statically compile against bundled SQLite.
FROM rust:1.90-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config make gcc && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin fafo

# Runtime stage. ca-certificates: rustls must verify R2's TLS chain.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/fafo /usr/local/bin/fafo

# Container disk is ephemeral on Cloudflare — exactly what fafo assumes.
# Live SQLite files are disposable working copies; R2 is the truth.
ENV HOST=0.0.0.0 \
    PORT=8080 \
    DATA_DIR=/tmp/fafo \
    BLOB_STORE=r2 \
    LOGICAL_WORKERS=64 \
    CLAIM=auto:16

EXPOSE 8080
CMD ["fafo"]
