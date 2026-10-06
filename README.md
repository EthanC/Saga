<div align="center">

# Saga

Saga archives YouTube channels with the Internet Archive and posts new uploads to Discord.

[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Build](https://img.shields.io/github/actions/workflow/status/EthanC/Saga/ci.yaml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?style=flat-square)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)

</div>

## Features

- Monitors videos, Shorts, and livestreams across multiple channels.
- Posts new uploads to Discord webhooks.
- Archives media and watch pages.
- Resumes interrupted work and creates verified SQLite backups.

## Docker Compose

Create the `config` and `data` directories. Copy `config.example.toml` to `config/config.toml` and `.env.example` to `.env`, then edit both files. Save this as `compose.yaml`:

```yaml
services:
  saga:
    container_name: saga
    image: ghcr.io/ethanc/saga:latest
    init: true
    env_file:
      - .env
    volumes:
      - ./config:/config:ro
      - ./data:/data
    restart: unless-stopped
    stop_grace_period: 120s
```

Start the container:

```console
docker compose up -d
```

Keep `stop_grace_period` longer than `SAGA_SHUTDOWN_TIMEOUT`, which defaults to 90 seconds.

### Health checks

The container probe reads a small health snapshot without importing the downloader or database libraries. Saga publishes it every 30 seconds, using a dedicated thread for health queries. Missing snapshots, snapshots older than two minutes, stale channel polling, and stalled transfers fail the probe.

Startup reports progress while resolving channels and establishing notification baselines. Five minutes without startup progress fails the probe. Publication timestamps are resolved by a separate worker using a persistent SQLite queue, so metadata lookups do not delay startup or polling. Channel scans are limited to two recent scans and one history scan at a time; upload and download worker limits are separate.

## Python

Install Python 3.14+, [`uv`](https://docs.astral.sh/uv/), ffmpeg, and Deno, then install the dependencies:

```console
uv sync
```

Create `config`, copy the example files to `config/config.toml` and `.env`, then run Saga:

```console
uv run python saga.py
```

Saga stores its database and temporary downloads in `data`. The first scan archives existing videos without posting them to Discord.

## Environment Variables

Saga reads `.env` and process variables, with process values taking precedence. Set `INTERNET_ARCHIVE_EMAIL` and `INTERNET_ARCHIVE_PASSWORD` to archive media. See [`.env.example`](.env.example) for paths, schedules, logging, rate limits, and other options.

## Database Backups

Saga creates online SQLite snapshots and checks each one with `PRAGMA quick_check`. By default, backups run daily at 7:00 AM in the system timezone, with the latest seven kept in `data/backups`. The schedule, timezone, location, and retention are configurable in `.env`.

## Configuration

Each YouTube instance defines its channels, polling interval, and retry settings:

```toml
[instances]

[[instances.youtube]]
name = "example"

[[instances.youtube.channels]]
reference = "@ExampleHandle"
discord_webhook_urls = [
  "https://discord.com/api/webhooks/000000000000000000/example-token",
]
```

References accept handles, channel IDs, and YouTube channel URLs. Videos, Shorts, and livestreams can be toggled per channel. See [`config.example.toml`](config.example.toml) for all options.

## Archival Reports

Set `REPORT_DISCORD_WEBHOOK_URL` for Discord reports covering discoveries, uploaded videos and bytes, lifetime totals, and unfinished work. Reports run at midnight UTC by default and are written to the log when no webhook is set.

## Preservation

Saga merges yt-dlp's `bestvideo+bestaudio` selection into MKV without re-encoding and uploads it to `opensource_movies`. It saves the Internet Archive upload acknowledgement before deleting local files. Optional remote validation runs separately; enable it with `SAGA_VALIDATION_ENABLED=true`.

Saga retries failed transfers, resumes interrupted downloads, and pauses new downloads when disk space is low. Livestreams wait until a replay is available. See `data/transfers.json` for transfer progress, queue status, and staging space.

## Webpage Capture

Saga submits watch pages directly to the Internet Archive unless both [Ziggy](https://github.com/EthanC/Ziggy) variables are set. Ziggy queues captures separately and does not require Internet Archive credentials:

```dotenv
ZIGGY_HOST=http://ziggy:9449
ZIGGY_IDENTIFIER=saga
```

Ziggy's listener has no authentication and should remain on a trusted network.

## YouTube Authentication

Cookies are needed only for account-restricted videos. Follow yt-dlp's [cookie export guide](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies) and save the file as `config/cookies.txt`. Saga loads it automatically; use `YOUTUBE_COOKIES_PATH` for another location. Keep the file private and restart Saga after replacing it.

When cookies are configured, add [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider) and the corresponding Saga dependency to `compose.yaml`:

```yaml
services:
  saga:
    depends_on:
      saga-pot:
        condition: service_healthy

  saga-pot:
    image: brainicism/bgutil-ytdlp-pot-provider:2.0.0
    init: true
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://127.0.0.1:4416/ping').then(r => { if (!r.ok) process.exit(1) }).catch(() => process.exit(1))"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s
    restart: unless-stopped
```

The provider generates PO tokens on the private Compose network. Do not publish its unauthenticated port. For a native installation, run provider server version `2.0.0` and set `YOUTUBE_POT_PROVIDER_URL=http://127.0.0.1:4416` in `.env`. The server and Python plugin versions must match.
