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

Videos that YouTube reports as unavailable, private, removed, or members-only are recorded in SQLite and no longer looked up for publication times. Other lookup failures, including scheduled livestreams that are still offline, retry with backoff from 5 minutes to 1 hour. A change in a livestream's status triggers the next lookup immediately.

YouTube, Archive, and webpage requests run in reusable child processes, so yt-dlp is not reloaded for every request. A process is replaced after a failed request, a forced stop, or 100 requests. Idle processes exit after 10 minutes.

The probe measures worker liveness. Active recovery, remote-service retry backoff, rejected credentials, storage limits, and jobs requiring operator review are degraded conditions, not restart triggers. Prolonged storage or authentication failures and required-job failures emit ERROR alerts while retries continue. Undispatched due work or an active worker without progress beyond `SAGA_TRANSFER_STALL_TIMEOUT` fails the probe. Optional validation outcomes remain warnings. Failed, expired, or stalled webpage preservation produces an actionable ERROR notification without making the container unhealthy.

Archive service cooldowns log once at WARNING. An outage escalates to ERROR after `SAGA_TRANSFER_STALL_TIMEOUT` without acknowledged uploads or advancing upload bytes, once a retry has also failed or a dispatched retry has stopped progressing. A single failure followed by Saga's own retry delay stays at WARNING. Healthy long transfers suppress service escalations and reminders; stalled workers still fail the health check. Unchanged escalated incidents repeat at most once per hour. Confirmed authentication rejection or item-permission denial alerts immediately, including while another upload is progressing. Failure counts, request IDs, request phases, and retry timestamps do not reset the reminder limit.

An acknowledged probe begins a 30-minute recovery-observation window. A new failure during that window continues the same notification incident and reminder budget. Only sustained recovery closes it. Identity, observation, escalation, and reminder timestamps are saved in SQLite, so a restart preserves those limits. Recovery of an escalated Archive incident logs once at INFO. Like every log, it reaches the Discord webhook only when `LOG_DISCORD_LEVEL` allows INFO. Other pipeline conditions retain their 15-minute reminders and log recovery once.

### Archive diagnostics and retries

Saga uses Archivist 1.5.0 response diagnostics to report Archive/S3 error codes, messages, request IDs, HTTP status, and request phase. `SlowDown` is throttling; explicit credential-rejection evidence produces authentication guidance; item access denials produce item-permission guidance. A generic HTTP 503 does not establish an account quota or password problem. Network failures report the transport exception type, such as `ReadTimeout` or `ConnectionError`, as their reason. Missing reasons are reported as `unknown`, and unfamiliar codes stay visible with status-based retry handling.

`data/transfers.json` includes the shared circuit's cooldown/probe status, next retry, latest diagnostics, incident category and age, escalation and notification timestamps, and upload/download progress. Its `archive_diagnostics` section contains transfer and background-job errors. `error_kind` is Saga's classification; `error_code`, `error_message`, `error_request_id`, and `error_headers` are response facts. These nullable fields are also saved in the durable queues and shared circuit. Older rows remain readable without fabricated response facts.

Diagnostic text is redacted and bounded: codes and request IDs to 256 characters, messages to 1,024, and each diagnostic header to 256. Only `content-type`, `retry-after`, and `x-amz-request-id` headers are retained. The same representation is used in SQLite, snapshots, console logs, and Discord alerts.

Archive incident diagnostics also show the latest preservation-progress timestamp, time without progress, acknowledged upload count and bytes, active upload bytes, and whether recovery is under observation. `retry_delay_source` distinguishes a server `Retry-After` from locally calculated pacing.

Archive cooldowns are capped at 15 minutes unless an explicit `Retry-After` requires longer. Only one retry probe runs at a time. An acknowledged upload releases shared backoffs; metadata recovery alone does not establish that writes have recovered. Uncertain transfers retain their originals for guarded reconciliation. Downloads can continue within staging, buffer, and disk limits during an outage.

Before creating an item or sending a recovery transfer, Saga asks IA-S3 whether the account's task queue is over its limit (`check_limit`). An over-limit reply defers the write as throttling without sending it. An unreadable or failed check does not block the write and is logged as a warning. Each failed write records the queue counters from its check as `error_queue` in alerts and `data/transfers.json`: account and item tasks queued against their rations, the global queue against its limit, whether rationing is engaged, and IA's limit reason when given. After throttling, successful probes enter a 30-minute probation period instead of restoring all upload workers at once. Archive operations remain serialized, with at least 60 seconds between completed writes and the next admission. Repeated throttling doubles this interval up to 15 minutes. Each probation period that ends without throttling halves the interval and starts another probation period; full upload concurrency returns only once the interval would fall below 15 seconds. Throttling during this release doubles the current interval. A successful probe starts the stable-recovery window; later successful writes do not extend it. Probation and pacing survive restarts, and explicit server delays still take precedence. The default upload-worker count is one.

A managed Archive outage is degraded, so it does not trigger an auto-heal restart. A stalled worker or an eligible retry probe left undispatched still fails the health check. Alert timing requires no new environment variables. To deploy an update, pull the rebuilt image and recreate Saga:

```console
docker compose pull saga
docker compose up -d --force-recreate saga
```

Saga adds the diagnostic and incident columns automatically. No credential rotation, cache-volume change, or manual database edit is required.

## Python

Install Python 3.15+, [`uv`](https://docs.astral.sh/uv/), ffmpeg, and Deno, then install the dependencies:

```console
uv sync
```

Create `config`, copy the example files to `config/config.toml` and `.env`, then run Saga:

```console
uv run python saga.py
```

Saga stores its database and temporary downloads in `data`. The first scan archives existing videos without posting them to Discord. Full history scans also read every playlist on the channel's Playlists tab, so unlisted uploads that appear only in playlists are discovered and archived. Playlist videos from other channels are ignored.

## Environment Variables

Saga reads `.env` and process variables, with process values taking precedence. Set `INTERNET_ARCHIVE_EMAIL` and `INTERNET_ARCHIVE_PASSWORD` to archive media. See [`.env.example`](.env.example) for paths, schedules, logging, rate limits, and other options.

## Webpage submission recovery

Ziggy connection refusal and DNS lookup failures before the POST is sent are retried with durable backoff. A lost or malformed receipt, a read timeout after a response, and a redirected submission remain uncertain and require reconciliation before resubmission. Saga never follows submission redirects, which prevents a later connection failure from being mistaken for an unsent original POST.

A one-time startup migration restores legacy Ziggy jobs only when their saved error exactly identifies a refused connection before submission and no Ziggy or Wayback receipt exists. This includes the refusal records produced by older Saga releases. Other uncertain jobs retain their operator-review state. No manual database edit is required.

Webpage outcome notifications include video IDs and bounded, redacted reasons. Each changed set of failed, expired, or stalled jobs is announced once, with the notification fingerprint saved in SQLite. Detail-only changes and restarts do not repeat the notification. Clearing the condition logs one INFO recovery, which follows `LOG_DISCORD_LEVEL` like every other log. Failed/expired counts remain visible in scheduled reports and `transfers.json`; its `webpage_notification` section records notification state. Health-publication failures do not announce false recoveries.

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

`cooldown` sets the seconds between recent scans. `reconciliation_interval` sets the seconds between full history scans and defaults to 21600 (six hours). The last completed history scan is saved, so a restart waits out the rest of the interval instead of rescanning every channel. A channel in several instances uses the shortest interval.

## Archival Reports

Set `REPORT_DISCORD_WEBHOOK_URL` for Discord reports covering discoveries, uploaded videos and bytes, lifetime totals, and unfinished work. Reports run at midnight UTC by default and are written to the log when no webhook is set.

## Preservation

Saga merges yt-dlp's `bestvideo+bestaudio` selection into MKV without re-encoding and uploads it to `opensource_movies`. It saves the Internet Archive upload acknowledgement before deleting local files. Optional remote validation runs separately; enable it with `SAGA_VALIDATION_ENABLED=true`.

The Internet Archive derives each uploaded item by default: it makes playable formats and thumbnails from the original. Set `SAGA_ARCHIVE_QUEUE_DERIVE=false` to skip this. Items then keep only their original files, with no playable derivatives or thumbnails. Skipping derives puts fewer tasks in the Archive's task queue, and `SlowDown` throttling is tied to that queue. Uploads and recovery transfers send the same setting. Change it only while no uploads are pending, so that every write to an item uses one value.

Item creation, uploads, and recovery transfers ask for the Archive's express task queue by default. Set `SAGA_ARCHIVE_INTERACTIVE_PRIORITY=false` to use the normal queue instead. Every write sends or omits the request together, because the Archive warns that mixed priorities on one item can reorder its writes. For the same reason, change this setting only while no uploads are pending.

Set `SAGA_ARCHIVE_MULTIPART_THRESHOLD` to a byte size to send originals of at least that size in resumable parts. It is `0`, disabled, by default. `SAGA_ARCHIVE_MULTIPART_PART_SIZE` sets the part size, from 1 byte to 5 GiB, and defaults to 512 MiB. Originals that would need more than 1000 parts use larger parts. Saga saves the upload's resume state in SQLite after each part the Archive acknowledges, before it sends the next request. An interrupted, throttled, or restarted transfer resumes from that state; it checks which parts the Archive holds and sends only the rest. The saved state wins over later setting changes, so a started upload always finishes in parts. If a completion's reply is lost, Saga confirms the result from the item listing or the Archive's task history instead of completing it again. When neither proves the outcome, the transfer goes to guarded reconciliation. Local originals remain until the upload acknowledgement is saved. Before the first part, the Archive client reads the whole original once to checksum it; this counts as transfer activity for stall detection but not as bytes sent.

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
