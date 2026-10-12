<div align="center">

# Saga

Saga archives YouTube channels with the Internet Archive and posts new uploads to Discord.

[![Python 3.15+](https://img.shields.io/badge/Python-3.15%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
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

The image provides a writable home and yt-dlp/Deno caches under `/home/saga`. The entrypoint assigns cache ownership to the configured `PUID` and `PGID`; no cache volume or environment override is required.

### Health checks

The container probe reads a small health snapshot without importing the downloader or database libraries. Saga publishes it every 30 seconds, using a dedicated thread for health queries. Missing snapshots, snapshots older than two minutes, stale channel polling, and stalled transfers fail the probe.

Startup reports progress while resolving channels and establishing notification baselines. Five minutes without startup progress fails the probe. Publication timestamps are resolved by a separate worker using a persistent SQLite queue, so metadata lookups do not delay startup or polling. Channel scans are limited to two recent scans and one history scan at a time; upload and download worker limits are separate.

The probe measures worker liveness. Active recovery, remote-service retry backoff, rejected credentials, storage limits, and jobs requiring operator review are degraded conditions, not restart triggers. Prolonged storage or authentication failures and required-job failures emit ERROR alerts while retries continue. Undispatched due work or an active worker without progress beyond `SAGA_TRANSFER_STALL_TIMEOUT` fails the probe. Optional validation and webpage outcomes remain warnings.

Alerts log immediately when their message or severity changes. Unchanged conditions repeat every 15 minutes, and clearing a condition logs its recovery once.

## Python

Install Python 3.15+, [`uv`](https://docs.astral.sh/uv/), ffmpeg, and Deno, then install the dependencies:

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

Retries prefer the video/audio formats recorded for validated cached inputs. If those formats disappear or their descriptors change, the claimed download resets only its owned, validated inputs and downloads the new selection. Completed originals and ambiguous inputs remain protected. Startup reschedules format-change failures left by older releases.

Upload recovery keeps retrying with backoff, including recovery jobs expired by older releases. Empty item responses trigger a guarded create-only retry; existing items still require matching Saga provenance before file recovery. Local originals remain until upload acknowledgement is saved. Attempt and deadline limits apply to optional background work, not upload recovery.

Startup restores the entire expired recovery backlog and removes legacy recovery deadlines before workers start. It preserves attempt history, pending retry backoff, and ownership conflicts requiring review.

Upload display text drops control and surrogate characters rejected by the Internet Archive. Startup requeues uploads rejected for those characters by older releases, without changing their item identifier, operation ID, or media.

Internet Archive authentication failures also retry with backoff. Startup restores uploads and recovery checks abandoned for authentication by older releases. Persistent failures emit an ERROR asking for account credentials to be checked. Upload errors retain their underlying service error and HTTP status for diagnosis.

Generic HTML HTTP 400 login errors and malformed CSRF/login replies are service failures, not password rejections. Queue records retain the error category, HTTP status, and request phase. Startup corrects older generic HTTP 400 login errors that were mislabeled as authentication failures.

Transfer and recovery workers share a durable Archive cooldown after network, service, or authentication failures. One worker probes recovery at a time, with backoff capped at 15 minutes unless Archive supplies a longer `Retry-After`. A successful upload or verified remote original releases queued service retries. A metadata-only probe hands a missing original to one upload worker before reopening normal traffic. Legacy multi-hour delays do not postpone the initial probe. `data/transfers.json` exposes the cooldown, next retry, and probe status under `archive_service`.

The upload-ready buffer counts due uploads and active downloads, not originals waiting in retry backoff. Saga can prepare the next video while Archive recovers; retained originals still count toward the data budget and remain until acknowledgement is saved.

### Storage budget

Set `SAGA_MAX_DATA_BYTES` to limit the data-directory space available for downloads and temporary merge output. For a 250 GB budget:

```dotenv
SAGA_MAX_DATA_BYTES=250000000000
```

The default is `0` (disabled). Saga counts regular-file sizes under `SAGA_DATA_PATH`, including active downloads, files awaiting upload, partials, the database, and backups stored there. It reserves estimated download and merge space across workers, pauses downloads that do not fit, and rechecks the budget during download callbacks and managed merge progress. Uploads continue and downloads become eligible again as files are removed. `data/transfers.json` includes the limit, measured usage, and remaining budget under `storage`.

`SAGA_MINIMUM_FREE_BYTES` independently reserves free space on the underlying filesystem. It is not subtracted from the 250 GB data budget. `SAGA_PARTIAL_MAX_BYTES` remains a separate cleanup target for eligible inactive partials.

Partial retention defaults to `auto`. Saga reclaims validated inactive inputs, including partials for unavailable videos, when their retention target is exceeded or queued downloads need storage headroom. It removes unavailable-video inputs first, then the oldest retryable inputs, after `SAGA_PARTIAL_MIN_AGE` (one day by default). Completed originals, active operations, unknown directories, and invalid manifests remain protected. `observe` reports without deleting; `enforce` also reclaims inputs. Cleanup decisions and remaining storage pressure appear in `data/transfers.json`.

This is an application-level transfer budget, not a filesystem quota. Database and backup writes continue, and writes between checks or underestimated merge output can exceed the configured value. Files outside `SAGA_DATA_PATH`, symlink targets, and filesystem allocation overhead are not counted.

## Webpage Capture

Saga submits watch pages directly to the Internet Archive unless both [Ziggy](https://github.com/EthanC/Ziggy) variables are set. Ziggy queues captures separately and does not require Internet Archive credentials:

```dotenv
ZIGGY_HOST=http://ziggy:9449
ZIGGY_IDENTIFIER=saga
```

Ziggy's listener has no authentication and should remain on a trusted network.

## YouTube Authentication

Cookies are needed only for account-restricted videos. Follow yt-dlp's [cookie export guide](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies) and save the file as `config/cookies.txt`. Saga loads it automatically; use `YOUTUBE_COOKIES_PATH` for another location. Each extraction reads the cookie file again, so replacing it takes effect on subsequent attempts without a restart.

Age checks, sign-in requirements, and bot challenges retry with backoff instead of marking videos permanently unavailable. On startup, Saga also requeues access failures marked unavailable by older releases, retaining their operation and partial inputs.

Saga checks for cookie changes every 30 seconds and reschedules failures from earlier sessions. Metadata rechecks get priority even when the normal metadata buffer is full; download space and upload-buffer limits still apply. Authentication alerts count only failures observed with the current session. Older failures awaiting another attempt are listed separately under `youtube_access.awaiting_recheck` in `data/transfers.json`.

When cookies are configured, add [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider) and the corresponding Saga dependency to `compose.yaml`:

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

The provider generates PO tokens on the private Compose network. Do not publish its unauthenticated port. For a native installation, run provider server version `2.0.0` and set `YOUTUBE_POT_PROVIDER_URL=http://127.0.0.1:4416` in `.env`. The server and Python plugin versions must match.
