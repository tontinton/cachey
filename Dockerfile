# Build stage
FROM rust:latest AS builder

WORKDIR /build

# Use Docker BuildKit cache mounts for faster builds
RUN --mount=type=bind,source=src,target=/build/src \
    --mount=type=bind,source=rsmp,target=/build/rsmp \
    --mount=type=bind,source=rsmp-derive,target=/build/rsmp-derive \
    --mount=type=bind,source=Cargo.toml,target=/build/Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=/build/Cargo.lock \
    --mount=type=cache,id=cachey-rust,sharing=locked,target=/build/target \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/git \
    cargo build --release --bin cachey

# Copy the binary from the cache volume
RUN --mount=type=cache,id=cachey-rust,sharing=locked,target=/cache \
    mkdir -p /build/target/release/ && \
    cp /cache/release/cachey /build/target/release/cachey

# Runtime stage
FROM ubuntu:latest

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /build/target/release/cachey /app/cachey

# Run the server
CMD ["./cachey"]
