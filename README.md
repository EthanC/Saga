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
    stop_grace_period: 30s
```

Start the container:

```console
docker compose up -d
```

## Python

Install Python 3.14+, [`uv`](https://docs.astral.sh/uv/), ffmpeg, ffprobe, and Deno, then install the dependencies:

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

Saga creates online SQLite snapshots and checks each one with `PRAGMA quick_check`. By default, it runs daily at 7:00 AM, keeps seven backups, and stores them in `data/backups`. The schedule, timezone, location, and retention are configurable in `.env`.

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

Set `REPORT_DISCORD_WEBHOOK_URL` for Discord reports covering discoveries, archived media, and unfinished work. Reports run at midnight UTC by default and are written to the log when no webhook is set.

## Preservation

Saga merges yt-dlp's `bestvideo+bestaudio` selection into MKV without re-encoding, validates it, and uploads it to `opensource_movies`. Remote files are verified before local deletion. Failed work is retried, and scheduled livestreams wait for a replay.

## Webpage Capture

Saga submits watch pages directly to the Internet Archive unless both [Ziggy](https://github.com/EthanC/Ziggy) variables are set. [Ziggy](https://github.com/EthanC/Ziggy) queues captures separately and does not require Internet Archive credentials:

```dotenv
ZIGGY_HOST=http://ziggy:9449
ZIGGY_IDENTIFIER=saga
```

[Ziggy](https://github.com/EthanC/Ziggy)'s listener has no authentication and should remain on a trusted network.

## YouTube Authentication

Cookies are needed only for account-restricted videos. Follow yt-dlp's [cookie export guide](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies) and save the file as `config/cookies.txt`. Saga loads it automatically; use `YOUTUBE_COOKIES_PATH` for another location. Keep the file private and restart Saga after replacing it.
