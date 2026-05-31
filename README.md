# yt-feeds-proxy

A lightweight Rust proxy that exposes YouTube-style Atom feed endpoints using Invidious backends.

## Overview

`yt-feeds-proxy` translates Invidious API responses into YouTube-compatible Atom feeds and lightweight endpoints. It is implemented in Rust with async I/O and is intended to run as a small, high-performance proxy.

## Features

- Standard Atom feeds: `/feeds/api/standardfeeds/:region/:feed`
- Video search: `/feeds/api/videos?q=...`
- Video metadata: `/feeds/api/videos/:video_id`
- Comments feed: `/feeds/api/videos/:video_id/comments`
- Related videos: `/feeds/api/videos/:video_id/related`
- User profile and uploads: `/feeds/api/users/:username` and `/feeds/api/users/:username/uploads`
- Redirects to Invidious embeds: `/embed/:video_id`
- Admin stats endpoints: `/admin/stats` and `/admin/stats.json`
- Protobuf-based Invidious browse endpoint: `/youtubei/v1/browse`

## Repository layout

- `src/` — Rust application sources
- `proto/` — protobuf schemas used by the proxy
- `rustypipe-patched/` — vendored patched RustyPipe dependency
- `Cargo.toml` — project manifest
- `Dockerfile`, `docker-compose.yml` — container deployment support

## Requirements

- Rust stable 1.70+
- Optional: `ffmpeg` for HD muxing support

## Build & run

```bash
git clone https://github.com/Basket5125/yt-feeds-proxy.git
cd yt-feeds-proxy
cargo build --release
```

Run:

```bash
cp config.example.json config.json
./target/release/yt-feeds-proxy
```

If `config.json` is missing, the proxy uses built-in defaults and supports `PORT` / `HOST` environment variables.

## Configuration

Copy `config.example.json` to `config.json` and update the values for your environment.

```json
{
  "port": 8080,
  "host": "0.0.0.0",
  "primary_instance": "http://inv.example.org",
  "primary_instances": [
    "http://inv.example.org",
    "http://inv2.example.org"
  ],
  "primary_rpm_limit": 5,
  "instances_list_url": "http://example.org/instances.json",
  "instances_refresh_secs": 300,
  "server_host": "example.com:80"
}
```

The proxy will use `config.json` when present. If the file is absent, defaults are loaded from the environment or built-in values.

## Docker

Build the image:

```bash
docker build -t yt-feeds-proxy:latest .
```

Run with mounted config:

```bash
docker run -d --name yt-feeds-proxy -p 8080:8080 \
  -v $(pwd)/config.json:/app/config.json:ro \
  yt-feeds-proxy:latest
```

Or use:

```bash
docker-compose up -d
```
