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

Create the `config` and `data` directories. Copy `config.example.toml` to `config/config.toml` and `.env.example` to `.env.saga`, then edit both files. Keep `.env.saga` private (`chmod 600 .env.saga`). Save this as `compose.yaml`, replacing the image placeholder with the digest of your tested release:

```yaml
services:
  saga:
    container_name: saga
    image: ghcr.io/ethanc/saga@sha256:<tested-release-digest>
    init: true
    env_file:
      - .env.saga
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

For a separate staging disk, add its existing host directory as a third bind mount to `/data/downloads`, beneath the `/data` mount. Keep the config, database, and staging mappings in your private deployment record. Saga uses `/data/downloads` for downloads and merge space even when it is a different filesystem.

Compose's project `.env` supplies interpolation values; it does not automatically populate container environments. Give Saga its own `env_file`. In Arcane, remove Saga credentials and webhooks from project-wide injection so the PO-token provider receives only its own settings. Verify environment **key names** after recreating both services, without printing secret values. Confirm that Arcane and `docker compose config` resolve the same mounts, settings, and image references. Values with spaces, including cron schedules, belong on one line in `.env.saga`.

CI publishes immutable `sha-<full-commit>` image tags. Resolve a tested tag to its digest before promotion, and record the prior and replacement digests, rendered configuration, snapshot path, queue totals, and deployment time. Roll back by selecting the recorded previous image and its configuration against compatible current state. [Recovery](RECOVERY.md) covers restoration when a database copy is needed.

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

Saga creates online SQLite snapshots and checks each one with `PRAGMA quick_check`. By default, it runs daily at 7:00 AM, keeps seven backups, and stores them in `data/backups`. The schedule, timezone, location, and retention are configurable in `.env`.

Local snapshots do not cover host loss. [Backup and recovery](RECOVERY.md) lists off-host inputs, freshness checks, restore rehearsals, and reconciliation when restoring older state.

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

Set `REPORT_DISCORD_WEBHOOK_URL` for Discord reports covering discoveries, videos uploaded, bytes uploaded, lifetime uploads, and outstanding transfers. Reports run at midnight UTC by default and are written to the log when no webhook is set. Validation and webpage results are reported separately.

Report semantics version 2 counts a video when its upload acknowledgement commits. Recovered and migrated uploads contribute to lifetime totals but are excluded from the reporting window's newly transferred videos and bytes. Existing version 1 snapshots retain their stored counts and labels.

## Preservation

Saga merges yt-dlp's `bestvideo+bestaudio` selection into MKV without re-encoding and uploads it to `opensource_movies`. Download completion is recorded after a successful downloader/merge result, a nonempty output, and an atomic rename. A leftover MKV without its completion receipt is downloaded again.

Internet Archive's acknowledgement of the expected original file completes the transfer. Saga saves the item ID, operation ID, filename, size, receipt, ETag, and completion time before scheduling staging deletion. Derivative processing, public visibility, validation, and webpage capture do not delay cleanup or upload reporting.

The default pipeline has one upload worker, one download worker, and a buffer of two upload-ready files. Downloads reserve space for both their streams and ffmpeg's merge output. Saga pauses new downloads when the buffer or disk budget is full and keeps uploads running. `SAGA_UNKNOWN_DOWNLOAD_BYTES` supplies an estimate when YouTube omits one; progress hooks adjust reservations as stream sizes become known. Failed transfers release their worker and retry with exponential backoff and jitter.

Validation is disabled by default. Enable `SAGA_VALIDATION_ENABLED` for one remote-only validation worker using saved receipts, sizes, and existing checksums. Background validation, recovery checks, and captures each have separate claims, request timeouts, a deadline, and a retry budget. No automatic upload or status-check path hashes staging files or runs ffprobe.

If a connection is lost before acknowledgement, Saga retains staging and checks the persisted operation's remote original in the recovery queue. Confirmed originals become recovered completions; confirmed missing files return to upload priority. Ambiguous outcomes expire after the background budget while other transfers continue. Saga pins Archivist 1.3.0 and supplies its transport-only recovery extension in `core/transport.py`; recovery rechecks account ownership and Saga provenance before a missing-file PUT.

`data/transfers.json` records the last successful upload, active transfer byte progress, oldest due ready file, staging usage, disk reservations, download-pause reason, and cumulative worker time. The healthcheck requires both a polling heartbeat and a recent transfer-health record. Upload work with no progress or waiting reason for `SAGA_TRANSFER_STALL_TIMEOUT` seconds produces an error and an unhealthy result.

### Migrating an Existing Queue

Download byte counts and shrinking reservations are durable checkpoints that can lag by five seconds. Disk headroom is checked on every callback using read-only reservation totals; an increased estimate receives an immediate transactional reservation check. Stream boundaries, merge boundaries, disk pauses, and claim release flush progress sooner. Completion receipts, upload acknowledgements, recovery outcomes, and cleanup transitions commit immediately. Repeated identical byte callbacks do not renew the last-progress timestamp.

Partial inputs are inventoried by operation and selected format in `inputs.json`. Compatible legacy `media.f<format>.<ext>.part` files and downloader sidecars are renamed as a restart-safe set before resumption. Conflicting formats, symlinks, malformed sidecars, and unknown operations are retained with an inventory finding. No partial creates a download-completion receipt.

The download cadence favors recent, resume, recent, then backfill work. Resume credit reduces only remaining input/merge allocations, not the free-space floor. Retention runs independently each minute. `SAGA_PARTIAL_RETENTION_MODE=observe` reports inactive candidates without deleting them; after a day of review, `enforce` can remove recognized inputs exceeding `SAGA_PARTIAL_MAX_BYTES` that have been inactive for `SAGA_PARTIAL_MIN_AGE`. Claimed, completed, upload-ready, uploading, and reconciliation work is protected. In enforcement mode, protected bytes above the target pause fresh downloads while resumable work and uploads continue.

Recent-work priority follows the durable notification-candidate flag, including discoveries delayed for days. Recent metadata has its own allowance equal to the ready buffer; backfill metadata has twice that allowance. Due prepared downloads and claimed metadata consume their class's allowance. Retry-delayed and disk-blocked downloads do not. Metadata capacity does not bypass download disk or ready-buffer limits. Workers favor recent work for three successful claims, then favor backfill for one; empty polls do not spend a turn.

1. Stop the old Saga instance and retain a consistent SQLite backup. The new version also creates `saga.sqlite.pre-upload-first.sqlite3` before changing a legacy schema.
2. Rehearse with a consistent backup using the tested version:

   ```console
   uv run python -m core.migration data/saga.sqlite --directory data/migration-rehearsal
   ```

   The command migrates an online SQLite snapshot twice, checks database integrity and foreign keys, and prints its path. It leaves the source database unchanged.
3. Deploy the tested image pinned to its `sha-<full-commit>` tag or digest. Start one Saga instance against the original data directory. Startup applies the idempotent schema and queue migration under Saga's process lock.
4. Verified historical successes become upload completions with their original verification timestamp. Their pending staging cleanup and capture jobs are scheduled independently. Interrupted uploads enter remote recovery using their saved file facts. Staging is deleted only after those originals are confirmed. Upload-ready rows with missing media return to downloading.
5. Check `transfers.json`, the logs, and the next version 2 report as transfers resume. Keep the pre-migration backup and the pinned previous image until the queue and reclaimed staging have been checked.

## Webpage Capture

Saga submits watch pages directly to the Internet Archive unless both [Ziggy](https://github.com/EthanC/Ziggy) variables are set. [Ziggy](https://github.com/EthanC/Ziggy) queues captures separately and does not require Internet Archive credentials:

```dotenv
ZIGGY_HOST=http://ziggy:9449
ZIGGY_IDENTIFIER=saga
```

[Ziggy](https://github.com/EthanC/Ziggy)'s listener has no authentication and should remain on a trusted network.

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
    image: brainicism/bgutil-ytdlp-pot-provider@sha256:<tested-provider-digest>
    init: true
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://127.0.0.1:4416/ping').then(r => { if (!r.ok) process.exit(1) }).catch(() => process.exit(1))"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s
    restart: unless-stopped
```

The provider generates video-bound PO tokens on the private Compose network. It is not needed when Saga runs without authentication cookies. Do not publish its unauthenticated port. For a native installation, run provider server version `2.0.0` and set `YOUTUBE_POT_PROVIDER_URL=http://127.0.0.1:4416`. The server and Python plugin versions must match.
