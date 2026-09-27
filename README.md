<div align="center">

# Saga

Saga preserves YouTube channels in the Internet Archive and sends new uploads to Discord webhooks.

[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Build](https://img.shields.io/github/actions/workflow/status/EthanC/Saga/ci.yaml?branch=main&style=flat-square&label=build)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?style=flat-square)](https://github.com/EthanC/Saga/actions/workflows/ci.yaml)

</div>

## Features

- Watch multiple YouTube channels from one process.
- Detect regular uploads, Shorts, and livestreams.
- Send new upload links to one or more Discord webhooks.
- Preserve media and watch pages in the Internet Archive.
- Resume interrupted downloads and archive work after a restart.
- Report discovery and preservation totals on fixed UTC intervals.
- Create verified SQLite backups on a cron schedule.
- Run with Docker Compose or Python and [`uv`](https://docs.astral.sh/uv/).

## Docker Compose

Create `config/config.toml` from `config.example.toml` and `.env` from `.env.example`. Add your channels, webhooks, and Internet Archive credentials, then save this as `compose.yaml`:

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

Create `data`, then start the container. The process runs as UID and GID `1000` by default; set `PUID` and `PGID` in `.env` to match the owner of the host directory when needed.

```console
docker compose up -d
```

The image includes Python 3.14, ffmpeg, ffprobe, Deno, and yt-dlp's JavaScript support.

Leave the optional path overrides in `.env` commented out to keep the image's `/config/config.toml`, `/config/cookies.txt`, `/data`, and `/data/healthy` defaults. The cookie file is optional. The health check expires after three times the longest configured polling cooldown, with a minimum window of five minutes. Saga clears the previous heartbeat before initialization and removes it on shutdown.

## Python

Install Python 3.14 or newer, [`uv`](https://docs.astral.sh/uv/), ffmpeg, ffprobe, and Deno. Install the dependencies from the repository root:

```console
uv sync
```

Create and edit `config/config.toml` and `.env` from their examples, then run Saga:

```console
uv run python saga.py
```

Saga stores its SQLite database and temporary downloads under `data`. Its first scan sets the notification baseline; existing videos are archived without sending Discord messages.

## Environment Variables

Saga reads environment variables from the process and an optional `.env` file. Process values take precedence.

| Variable | Description | Type | Required | Default |
| --- | --- | --- | :---: | --- |
| `LOG_LEVEL` | Console log level | String | No | `"INFO"` |
| `LOG_DISCORD_WEBHOOK_URL` | Discord webhook for application log events | URL | No | None |
| `LOG_DISCORD_LEVEL` | Minimum level forwarded to the logging webhook | String | No | `"WARNING"` |
| `PUID` | Container process user ID | Integer | No | `1000` |
| `PGID` | Container process group ID | Integer | No | `1000` |
| `TZ` | System timezone used by the backup schedule | IANA timezone | No | `Etc/UTC` in `.env.example` |
| `SAGA_CONFIG_PATH` | TOML configuration path | Path | No | `"config/config.toml"` |
| `SAGA_DATA_PATH` | Database and download directory | Path | No | `"data"` |
| `SAGA_HEALTHCHECK_FILE` | File refreshed after successful channel scans | Path | No | `"data/healthy"` |
| `SAGA_BACKUP_ENABLED` | Enable the backup scheduler | Boolean | No | `true` |
| `SAGA_BACKUP_SCHEDULE` | Five-field cron expression | String | No | `"0 7 * * *"` |
| `SAGA_BACKUP_DIRECTORY` | Snapshot destination, relative to `SAGA_DATA_PATH` | Path | No | `"backups"` |
| `SAGA_BACKUP_RETENTION_COUNT` | Successful snapshots to keep; `0` keeps all | Integer | No | `7` |
| `SAGA_BACKUP_TIMEZONE` | Backup schedule timezone override | IANA timezone | No | System local timezone |
| `DISCORD_TIMEOUT` | Discord request deadline in seconds | Number | No | `30` |
| `REPORT_DISCORD_WEBHOOK_URL` | Discord webhook for combined archival reports | URL | No | None |
| `SAGA_REPORT_INTERVAL` | Fixed UTC report interval in seconds | Number | No | `86400` |
| `SAGA_REPORT_FINALIZATION_GRACE` | Delay after a closed report window, in seconds | Number | No | `300` |
| `YOUTUBE_COOKIES_PATH` | Netscape cookie export loaded without modifying the source file | Path | No | `"config/cookies.txt"` when present |
| `INTERNET_ARCHIVE_EMAIL` | Internet Archive account email | String | Yes | None |
| `INTERNET_ARCHIVE_PASSWORD` | Internet Archive account password | String | Yes | None |
| `SAGA_ARCHIVE_WORKERS` | Concurrent archive workers, from 1 through 4 | Integer | No | `1` |
| `SAGA_ARCHIVE_RETRY_DELAY` | Delay before retrying temporary archive failures, in seconds | Number | No | `300` |
| `SAGA_ARCHIVE_REQUEST_TIMEOUT` | Archive request timeout in seconds | Number | No | `60` |
| `SAGA_ARCHIVE_POLL_TIMEOUT` | Ingest and recovery polling limit in seconds | Number | No | `900` |
| `SAGA_ARCHIVE_POLL_INTERVAL` | Archive and Wayback polling interval in seconds | Number | No | `5` |
| `SAGA_MINIMUM_FREE_BYTES` | Free disk space reserved beyond the estimated streams and merged output | Integer | No | `5000000000` |
| `SAGA_DOWNLOAD_BYTES_PER_SECOND` | Combined YouTube media download allowance in payload bytes per second; `0` is unlimited | Integer | No | `0` |
| `SAGA_UPLOAD_BYTES_PER_SECOND` | Combined Internet Archive media upload allowance in payload bytes per second; `0` is unlimited | Integer | No | `0` |
| `ZIGGY_HOST` | Ziggy HTTP origin; set with the identifier | URL | Conditional | None |
| `ZIGGY_IDENTIFIER` | Ziggy submission attribution, up to 128 characters | String | Conditional | None |

The Internet Archive credentials must be set together. The two Ziggy variables are optional but must also be set together.

Set `LOG_DISCORD_WEBHOOK_URL` to forward warning-and-above events through `loguru-discord`. This logging webhook is separate from the per-channel notification webhooks in `config.toml`; lower its threshold with `LOG_DISCORD_LEVEL` only when diagnosing a problem.

## Database Backups

Saga uses SQLite's online-backup API to include committed WAL data in a standalone snapshot while the service remains running. Each snapshot passes `PRAGMA quick_check` before Saga publishes it. The default schedule runs at 7:00 AM in the system timezone and keeps the seven newest successful snapshots under `data/backups`.

Set `TZ`, such as `TZ=America/New_York`, to change the container's system timezone. `SAGA_BACKUP_TIMEZONE` overrides the timezone for backups only. Local clock schedules stay at the configured time across daylight-saving changes.

Backups run one at a time. Saga waits for the next cron occurrence after startup and does not replay missed runs. A failed run leaves existing snapshots unchanged and the scheduler continues to the next occurrence. Changes to backup environment variables require a restart.

To restore a snapshot, stop Saga and any other process using the database. Move `saga.sqlite` to a safe location, remove stale `saga.sqlite-wal` and `saga.sqlite-shm` files, copy the selected snapshot to `saga.sqlite`, then restart Saga.

## Configuration

Each `[[instances.youtube]]` table has its own channels, schedule, and retry settings. Each channel has one or more Discord webhooks and can independently enable regular videos, Shorts, and livestreams.

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

| Key | Description | Type | Required | Default |
| --- | --- | --- | :---: | --- |
| `name` | Unique lowercase instance name | String | Yes | None |
| `cooldown` | Seconds between completed scans | Number | No | `60` |
| `retries` | Retries per YouTube request | Integer | No | `3` |
| `retry_delay` | Seconds between retries | Number | No | `5` |
| `reference` | Channel handle, ID, or YouTube channel URL | String | Yes | None |
| `discord_webhook_urls` | Discord webhooks for this channel | String array | Yes | None |
| `videos` | Monitor regular videos for this channel | Boolean | No | `true` |
| `shorts` | Monitor Shorts for this channel | Boolean | No | `true` |
| `livestreams` | Monitor livestreams for this channel | Boolean | No | `true` |

A channel reference can be an `@handle`, a canonical channel ID, or an HTTPS YouTube channel, handle, custom, or user URL. Changing how the same channel is referenced or rotating a webhook does not reset channel progress. Each webhook receives only the types enabled for its channel entry. A type disabled in every entry for the same canonical channel is not scanned or preserved.

Saga notifies each configured webhook independently. Uploads found after downtime still receive notifications. Discord webhooks do not support exactly-once delivery, so a lost response followed by a retry can produce a duplicate message.

## Archival Reports

Set `REPORT_DISCORD_WEBHOOK_URL` to send one combined report for every configured YouTube instance. Reports use fixed UTC windows. The default interval is 24 hours, and Saga waits five minutes after each window closes before freezing its metrics.

The first reporting startup stores the latest closed boundary without sending a historical report. After downtime, Saga generates missed windows from oldest to newest and resumes any pending delivery. Each snapshot is committed before its Discord request, so retries retain the original values while discovery and archival continue.

Reports include:

- Discoveries during the window and lifetime, split into videos, Shorts, and livestreams.
- Media verified on Internet Archive during the window and lifetime.
- Verified media bytes, including files already removed by local cleanup.
- Active channels, unfinished workflows, videos awaiting replay, unavailable videos, and workflows requiring operator action.

Historical backfill contributes to discovery totals and is not labeled as new uploads. Media totals count remote verification, not upload attempts or webpage submissions. Current status is measured when Saga generates the report. Video duration is not tracked.

Without a report webhook, Saga writes report summaries to its log. It suppresses Discord delivery when a window has no discoveries, verified media, outstanding work, or problems. Active channels alone do not trigger a message. Configuration changes take effect after a service restart.

Report delivery has the same webhook limitation as upload notifications: a lost response after Discord accepts a message can cause a duplicate on retry.

## Preservation

Saga downloads yt-dlp's exact `bestvideo+bestaudio` selection and merges it into MKV without re-encoding. Missing media fragments fail the download and trigger a retry rather than producing an incomplete archive. ffprobe must find both audio and video before Saga uploads the file.

Before downloading, Saga requires free space for twice the estimated media size plus `SAGA_MINIMUM_FREE_BYTES`, accounting for the input streams and merged output existing together. If the size is unknown, only the configured reserve can be checked.

Set `SAGA_DOWNLOAD_BYTES_PER_SECOND` and `SAGA_UPLOAD_BYTES_PER_SECOND` to cap bulk media payloads. Each nonzero value is a shared allowance for all transfers in that direction within one Saga process. For example, 50 Mbps down and 10 Mbps up are `6250000` and `1250000` bytes per second:

```dotenv
SAGA_DOWNLOAD_BYTES_PER_SECOND=6250000
SAGA_UPLOAD_BYTES_PER_SECOND=1250000
```

The limiter permits bounded bursts of at most one second or 64 KiB, whichever is smaller. Changes require a restart. Download limits cover YouTube media responses, including native HLS and DASH fragments, retries, and resumed transfers. Upload limits cover initial and recovery media PUTs to the Internet Archive. Channel scans, metadata and polling requests, Discord traffic, webpage submissions, protocol overhead, local hashing, and traffic from a separate Ziggy service are not capped. Saga blocks ffmpeg from opening network media inputs while download limiting is active, but still uses ffmpeg to merge local files.

Bandwidth settings limit payload throughput rather than every packet on the network interface. Keep the values below measured usable capacity to leave room for protocol and control traffic. A low upload allowance can make shutdown take longer than the Docker example's 30-second stop grace period because Saga drains active archive work before exiting.

Each video is stored as an Internet Archive item in `opensource_movies`. Saga records the source URL, checksums, and a unique operation ID, then verifies the remote file before deleting its local copy. Temporary failures retry later; conflicts retain the local files for operator review.

Scheduled livestreams trigger a Discord notification when discovered. Their download waits until yt-dlp reports that a replay is available.

## Webpage Capture

By default, Saga submits each canonical YouTube watch URL to the Internet Archive. It stores the Wayback job ID and resumes polling after a restart.

Set both Ziggy variables to send webpage captures to its HTTP queue instead:

```dotenv
ZIGGY_HOST=http://ziggy:9449
ZIGGY_IDENTIFIER=saga
```

Saga requires a `202 Accepted` response from `/v1/queue`. This confirms that Ziggy queued the URL, not that the capture completed. Saga does not fall back to a direct Internet Archive submission if Ziggy fails.

Ziggy's listener has no authentication and should be reachable only over a trusted network.

## YouTube Authentication

Cookies are optional for public videos. They are needed for content that requires a signed-in account, such as age-restricted, private, or members-only videos.

Follow yt-dlp's [Exporting YouTube cookies](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies) guide and save the exported file as `config/cookies.txt`. Keep the file private; it grants access to the signed-in account.

Local Python installations and the Docker Compose example load this file automatically when it exists. Set `YOUTUBE_COOKIES_PATH` only to use another location.

Restart Saga after replacing expired cookies.
