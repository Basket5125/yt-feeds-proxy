# Multi-stage build: compile with Rust, copy release binary into a slim image
FROM rust:1.85 AS builder

WORKDIR /app
COPY . /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates ffmpeg libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/yt-feeds-proxy /usr/local/bin/yt-feeds-proxy
EXPOSE 3000
CMD ["yt-feeds-proxy"]