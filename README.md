<div align="center">

# Saga

Saga archives YouTube channels to the Internet Archive and posts new uploads to Discord.

[![Python 3.15+](https://img.shields.io/badge/Python-3.15%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Build](https://img.shields.io/github/actions/workflow/status/EthanC/Saga/ci.yaml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?style=flat-square)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)

</div>

## Features

- Monitors videos, Shorts, and livestreams across any number of channels, including unlisted uploads found in playlists.
- Posts new uploads to Discord webhooks.
- Uploads the best available quality, merged into MKV without re-encoding, to the Internet Archive.
- Captures each watch page with the Wayback Machine, directly or through [Ziggy](https://github.com/EthanC/Ziggy).
- Survives restarts and outages: work is saved in SQLite, and local files are kept until the Archive confirms each upload.
- Sends scheduled archival reports and alerts to Discord, and takes daily verified database backups.

## Getting Started

### Quick Start: Docker (Recommended)

> [!IMPORTANT]
> An [Internet Archive](https://archive.org/account/signup) account is required to archive media. Without one, Saga only posts new uploads to Discord.

1. Create `config` and `data` directories.
2. Copy [`config.example.toml`](config.example.toml) to `config/config.toml` and add your channels.
3. Copy [`.env.example`](.env.example) to `.env` and set your Internet Archive credentials.
4. Save this `compose.yaml` and run `docker compose up -d`.

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

Keep `stop_grace_period` longer than `SAGA_SHUTDOWN_TIMEOUT` (90 seconds by default) so in-progress work can be saved on shutdown.

### Standalone

> [!NOTE]
> Saga targets Python 3.15 and newer, and requires [ffmpeg](https://ffmpeg.org/) and [Deno](https://deno.com/).

Install the dependencies with [uv](https://docs.astral.sh/uv/):

```console
uv sync
```

Copy `config.example.toml` to `config/config.toml` and `.env.example` to `.env`, then run Saga:

```console
uv run python saga.py
```

## Configuration

### Channels

Channels are configured in `config/config.toml`. Each instance groups channels that share a polling schedule.

```toml
[[instances.youtube]]
name = "example"

[[instances.youtube.channels]]
reference = "@ExampleHandle"
discord_webhook_urls = [
  "https://discord.com/api/webhooks/000000000000000000/example-token",
]
```

| **Option** | **Description** | **Default** |
|---|---|---|
| `name` | Name of the instance. | Required |
| `cooldown` | Seconds between scans for new uploads. | `60` |
| `reconciliation_interval` | Seconds between full channel history scans. | `21600` |
| `retries` | Attempts for each failed YouTube request. | `3` |
| `retry_delay` | Seconds between those attempts. | `5` |
| `channels.reference` | Channel handle, ID, or URL. | Required |
| `channels.videos` / `shorts` / `livestreams` | Whether to include each type of upload. | `true` |
| `channels.discord_webhook_urls` | Discord webhooks that receive new uploads. | Required |

The first scan of a channel archives its existing uploads without posting them to Discord.

### Environment Variables

Settings are read from `.env` and the process environment, with process values taking precedence. Sizes are in bytes and durations in seconds unless noted.

#### General

| **Environment Variable** | **Description** | **Default** |
|---|---|---|
| `INTERNET_ARCHIVE_EMAIL` | Internet Archive account email. Required to archive media. | N/A |
| `INTERNET_ARCHIVE_PASSWORD` | Internet Archive account password. Required to archive media. | N/A |
| `LOG_LEVEL` | [Loguru level](https://loguru.readthedocs.io/en/stable/api/logger.html#levels) of log events to print to the console. | `INFO` |
| `LOG_DISCORD_WEBHOOK_URL` | Discord webhook to forward log events to. | N/A |
| `LOG_DISCORD_LEVEL` | Loguru level of log events to forward to Discord. | `WARNING` |
| `REPORT_DISCORD_WEBHOOK_URL` | Discord webhook for archival reports. Reports are logged when unset. | N/A |
| `SAGA_REPORT_SCHEDULE` | Cron schedule for archival reports, in UTC. | `0 0 * * *` |
| `DISCORD_TIMEOUT` | Timeout for Discord requests. | `30` |
| `PUID` / `PGID` | Docker user and group that run Saga and own its data. | `1000` |
| `TZ` | [IANA](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) time zone of the host. | `Etc/UTC` |
| `SAGA_CONFIG_PATH` | Path to the TOML configuration. | `config/config.toml` |
| `SAGA_DATA_PATH` | Directory for the database, downloads, and backups. | `data` |
| `SAGA_SHUTDOWN_TIMEOUT` | Time allowed to save work on shutdown. Minimum `10`. | `90` |

#### Transfers and Storage

| **Environment Variable** | **Description** | **Default** |
|---|---|---|
| `SAGA_DOWNLOAD_WORKERS` | Concurrent downloads, from 1 to 4. | `1` |
| `SAGA_UPLOAD_WORKERS` | Concurrent uploads, from 1 to 4. | `1` |
| `SAGA_UPLOAD_READY_BUFFER` | Downloaded videos to keep ready for upload. | `2` |
| `SAGA_DOWNLOAD_BYTES_PER_SECOND` | Download bandwidth limit. `0` disables it. | `0` |
| `SAGA_UPLOAD_BYTES_PER_SECOND` | Upload bandwidth limit. `0` disables it. | `0` |
| `SAGA_MAX_DATA_BYTES` | Space downloads may use in the data directory, including the database and backups. `0` disables it. | `0` |
| `SAGA_MINIMUM_FREE_BYTES` | Free disk space to keep; downloads pause below it. | `5000000000` |
| `SAGA_UNKNOWN_DOWNLOAD_BYTES` | Space to reserve for a download of unknown size. | `10000000000` |
| `SAGA_PARTIAL_RETENTION_MODE` | Cleanup of inactive partial downloads: `auto`, `enforce`, or `observe` (report only). | `auto` |
| `SAGA_PARTIAL_MAX_BYTES` | Space partial downloads may use before cleanup. | `100000000000` |
| `SAGA_PARTIAL_MIN_AGE` | Age a partial download must reach before cleanup. | `86400` |
| `SAGA_TRANSFER_STALL_TIMEOUT` | Time without progress before a transfer counts as stalled. | `900` |

#### Internet Archive

| **Environment Variable** | **Description** | **Default** |
|---|---|---|
| `SAGA_ARCHIVE_DERIVE` | When the Archive makes playable formats and thumbnails: `deferred` (once the account's task backlog has room), `immediate` (with each upload), or `never`. | `deferred` |
| `SAGA_ARCHIVE_DERIVE_BACKLOG_TARGET` | Request deferred derives only while the account has fewer queued and running tasks than this. | `20` |
| `SAGA_ARCHIVE_DERIVE_PAUSED` | Stop requesting new derives. | `false` |
| `SAGA_ARCHIVE_DERIVE_SUBMIT_INTERVAL` | Time between derive requests. | `5` |
| `SAGA_ARCHIVE_DERIVE_POLL_INTERVAL` | Time between status checks of each requested derive. | `900` |
| `SAGA_ARCHIVE_DERIVE_MAX_LAG` | Alert when the oldest pending derive waits longer than this. Accepts `s`, `m`, `h`, or `d`. | `14d` |
| `SAGA_ARCHIVE_DERIVE_STALE_DAYS` | Days without derivative files before a derive fails. | `7` |
| `SAGA_ARCHIVE_TASK_QUEUE_LIMIT` | Pause uploads while the account has this many queued and running tasks. `0` disables it. | `0` |
| `SAGA_ARCHIVE_INTERACTIVE_PRIORITY` | Request the Archive's express task queue. Change only while no uploads are pending. | `true` |
| `SAGA_ARCHIVE_MULTIPART_THRESHOLD` | Upload files at least this large in resumable parts. `0` disables it. | `0` |
| `SAGA_ARCHIVE_MULTIPART_PART_SIZE` | Size of each part, up to 5 GiB. | `536870912` |
| `SAGA_ARCHIVE_RETRY_DELAY` | Base delay before retrying a failed upload. | `300` |
| `SAGA_ARCHIVE_REQUEST_TIMEOUT` | Timeout for Archive requests. | `60` |
| `SAGA_VALIDATION_ENABLED` | Verify each upload against the Archive after it completes. | `false` |
| `ZIGGY_HOST` | [Ziggy](https://github.com/EthanC/Ziggy) URL for watch page captures. Keep Ziggy on a trusted network. | N/A |
| `ZIGGY_IDENTIFIER` | Identifier Saga submits to Ziggy with. | N/A |

#### Backups

| **Environment Variable** | **Description** | **Default** |
|---|---|---|
| `SAGA_BACKUP_ENABLED` | Take scheduled, verified SQLite backups. | `true` |
| `SAGA_BACKUP_SCHEDULE` | Cron schedule for backups. | `0 7 * * *` |
| `SAGA_BACKUP_TIMEZONE` | IANA time zone for the schedule. Empty uses the system time zone. | N/A |
| `SAGA_BACKUP_DIRECTORY` | Backup directory, relative to `SAGA_DATA_PATH`. | `backups` |
| `SAGA_BACKUP_RETENTION_COUNT` | Backups to keep. | `7` |

### YouTube Cookies

Cookies are only needed for account-restricted videos. Export them with yt-dlp's [cookie guide](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies) and save the file as `config/cookies.txt`, or set `YOUTUBE_COOKIES_PATH`. Saga picks up a replaced file without a restart.

With cookies, also run [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider) alongside Saga. Do not publish its port.

```yaml
services:
  saga:
    depends_on:
      saga-pot:
        condition: service_healthy

  saga-pot:
    image: brainicism/bgutil-ytdlp-pot-provider:2.0.2
    init: true
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://127.0.0.1:4416/ping').then(r => { if (!r.ok) process.exit(1) }).catch(() => process.exit(1))"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s
    restart: unless-stopped
```

For a standalone install, run the provider server locally and set `YOUTUBE_POT_PROVIDER_URL=http://127.0.0.1:4416`.

## Usage

### Monitoring

Saga writes its progress, queues, storage use, and recent errors to `data/transfers.json`. Problems that need attention are also logged and, with `LOG_DISCORD_WEBHOOK_URL` set, sent to Discord.

The Docker image includes a health check. It fails only when Saga itself stops making progress; Internet Archive outages, rejected credentials, and full storage are reported as alerts instead, so the container is not restarted for them.

### Retrying Failed Derives

Failed derives are not retried automatically. Queue a new derive for chosen items, by video ID or Archive identifier, or for every failed derive:

```console
uv run python saga.py retry-derives IDENTIFIER...
uv run python saga.py retry-derives --all
```

In Docker, run the command as your `PUID:PGID`:

```console
docker compose exec -u 1000:1000 saga python saga.py retry-derives --all
```

### Updating

Pull the new image and recreate the container. Saga upgrades its database automatically.

```console
docker compose pull saga
docker compose up -d --force-recreate saga
```

## Disclaimer

Saga is not affiliated with or endorsed by YouTube, Google, Discord, or the Internet Archive.

All trademarks belong to their respective owners.
