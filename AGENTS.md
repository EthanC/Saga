# AGENTS.md

Saga archives YouTube channels to the Internet Archive and posts new uploads to Discord. It is a long-running asyncio service backed by SQLite, shipped as a Docker image (`ghcr.io/ethanc/saga`).

## Setup and commands

Requires Python 3.15+, [`uv`](https://docs.astral.sh/uv/), ffmpeg, and Deno.

```console
uv sync --all-groups          # install runtime + dev dependencies
uv run python saga.py         # run locally (needs config/config.toml and .env)
```

Run the full CI quality gate before considering a change done:

```console
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest --cov=core --cov-branch --cov-report=term-missing --cov-fail-under=100
```

- Coverage of `core/` must stay at **100% (line and branch)**. New code needs tests for every branch; do not add `pragma: no cover` or `exclude_also` entries to get there.
- Ruff is configured with `fix = true`, so `ruff check` rewrites files. Formatting uses LF line endings and ignores magic trailing commas.
- Run a single test file with `uv run pytest tests/test_state.py`.

## Layout

- `saga.py` — entrypoint: loads settings, takes the `data/saga.lock` process lock, wires `StateStore`, `YouTubeClient`, limiters, and `SagaService`.
- `core/` — all application code. Each module opens with a one-line docstring stating its responsibility; read it before editing.
  - `service.py` — coordinates discovery, notification, and worker scheduling.
  - `state.py` — SQLModel tables, schema upgrades, and short transactional workflow operations. Largest module; most durable behavior lives here.
  - `archival.py`, `archive_*.py` — Internet Archive uploads, recovery, shared cooldown/circuit, diagnostics, and incidents. `archivist-py` is pinned to `1.6.0`.
  - `youtube.py`, `youtube_errors.py` — yt-dlp channel resolution, enumeration, and downloads.
  - `workers.py` — child-process boundaries for network libraries; the parent owns bandwidth budgets via RPC.
  - `pipeline_health.py`, `healthcheck.py` — health snapshot and the container probe.
  - `config.py` — TOML (`config/config.toml`) and environment (`.env`) settings via environs.
- `tests/` — pytest suite, one or more files per module (`test_<module>.py`, `test_<module>_coverage.py`, plus behavior-focused files). Tests import shared fakes from each other (e.g. `from tests.test_archival import FakeGateway`).
- `.github/smoke_*.py` — image smoke tests run in CI against the built container.

## Conventions and invariants

- **Internet Archive access goes through Archivist only.** Saga must never communicate with the Internet Archive directly. All IA and archive.org interaction (S3, metadata, Tasks API, Wayback/SPN, and so on) goes through Archivist's **public** API. Don't import Archivist private modules (`archivist.*._*`), call underscore-prefixed methods, subclass Archivist clients to add requests, or send HTTP requests to archive.org hosts from Saga. If Saga needs IA functionality Archivist doesn't provide, stop and request an Archivist update that describes the needed API; don't work around the gap in Saga. `tests/test_archivist_adoption.py::test_saga_reaches_the_archive_only_through_public_archivist` enforces this.
- **Durability first.** Work state lives in SQLite and must survive restarts. Never delete local originals before the Archive upload acknowledgement is saved. Uncertain outcomes (lost receipts, ambiguous transfers) go to guarded reconciliation or operator review, never blind resubmission.
- **Schema changes are additive and automatic.** Add columns through the existing upgrade path in `state.py` (`ALTER TABLE ... ADD COLUMN` with defaults/nullable). Older rows must remain readable; users should never need a manual database edit. Migrations of legacy records run once at startup.
- **Health semantics.** The probe fails only on worker liveness problems (stale polling, stalled transfers, undispatched due work). External outages, backoff, credential rejection, and storage limits are *degraded* states that alert but must not trigger restarts.
- **`core/healthcheck.py` and `core/youtube_errors.py` must not import the downloader or database libraries**; they run in lightweight contexts.
- **Redaction.** Credentials and diagnostics are redacted and length-bounded consistently across SQLite, `transfers.json`, logs, and Discord alerts.
- **Config files stay in sync.** When adding or changing options, update `config.example.toml`, `config.schema.json`, and/or `.env.example` together; tests validate the example config against the schema and load `.env.example`.
- **Docs.** `README.md` is for people running Saga: features, setup, configuration options, and the few commands they use. Keep it short and plain. When adding or changing an option, update its README table row. Internal behavior, retry and alert mechanics, migrations, and diagnostics belong in the behavior reference below, not the README.
- Type-annotate everything; code must pass `ty check`. Match existing docstring style (single-sentence, imperative or descriptive).

## Behavior reference

Detailed runtime behavior the README intentionally leaves out. Keep this current when changing these mechanics.

### Discovery

- Recent scans run every `cooldown` seconds; full history scans every `reconciliation_interval` (default 21600). The last completed history scan is saved, so restarts wait out the remaining interval. A channel in several instances uses the shortest interval.
- History scans also read every playlist on the channel's Playlists tab to find unlisted uploads; playlist videos from other channels are ignored.
- The first scan archives existing videos without posting them to Discord.
- Channel scans are limited to two recent scans and one history scan at a time; upload and download worker limits are separate.
- Publication timestamps are resolved by a separate worker with a persistent SQLite queue, so lookups never delay startup or polling. Videos YouTube reports as unavailable, private, removed, or members-only are recorded and not looked up again. Other lookup failures (including offline scheduled livestreams) retry with backoff from 5 minutes to 1 hour; a livestream status change triggers the next lookup immediately.
- Livestreams wait until a replay is available.

### Child processes

YouTube, Archive, and webpage requests run in reusable child processes so yt-dlp is not reloaded per request. A process is replaced after a failed request, a forced stop, or 100 requests; idle processes exit after 10 minutes.

### Health and alerting

- Saga publishes a health snapshot every 30 seconds from a dedicated thread. Missing snapshots, snapshots older than two minutes, stale channel polling, stalled transfers, undispatched due work, or an active worker without progress beyond `SAGA_TRANSFER_STALL_TIMEOUT` fail the probe. Five minutes without startup progress (channel resolution, notification baselines) also fails it.
- Degraded, non-restarting conditions: active recovery, remote retry backoff, rejected credentials, storage limits, and jobs requiring operator review. Prolonged storage or auth failures and required-job failures emit ERROR while retries continue. Optional validation outcomes stay warnings. Failed, expired, or stalled webpage preservation produces an ERROR without making the container unhealthy.
- Archive cooldowns log once at WARNING. An outage escalates to ERROR after `SAGA_TRANSFER_STALL_TIMEOUT` without acknowledged uploads or advancing upload bytes, once a retry has also failed or a dispatched retry stopped progressing. A single failure followed by Saga's own retry delay stays WARNING. Healthy long transfers suppress service escalations and reminders.
- Confirmed authentication rejection or item-permission denial alerts immediately, even while another upload progresses.
- Unchanged escalated incidents repeat at most hourly. Failure counts, request IDs, phases, and retry timestamps do not reset the reminder limit. An acknowledged probe starts a 30-minute recovery-observation window; a new failure in that window continues the same incident. Incident identity, observation, escalation, and reminder timestamps persist in SQLite. Recovery of an escalated incident logs once at INFO (reaching Discord only if `LOG_DISCORD_LEVEL` allows INFO). Other pipeline conditions use 15-minute reminders and log recovery once.
- Webpage outcome notifications include video IDs and bounded, redacted reasons. Each changed set of failed, expired, or stalled jobs is announced once (fingerprint saved in SQLite); detail-only changes and restarts do not repeat it.

### Archive diagnostics

- Saga uses Archivist 1.6.0 response diagnostics: Archive/S3 error code, message, request ID, HTTP status, and request phase. `SlowDown` is throttling; explicit credential-rejection evidence produces authentication guidance; item access denials produce item-permission guidance. A generic HTTP 503 does not imply a quota or password problem. Generic HTML HTTP 400 login errors and malformed CSRF/login replies are service failures, not password rejections.
- Diagnostics are redacted and bounded: codes and request IDs to 256 characters, messages to 1,024, each header to 256. Only `content-type`, `retry-after`, and `x-amz-request-id` headers are kept. The same representation is used in SQLite, snapshots, logs, and Discord.
- `data/transfers.json` exposes the shared circuit (`archive_service`), `archive_diagnostics` (`error_kind` is Saga's classification; `error_code`, `error_message`, `error_request_id`, `error_headers`, and `error_queue` come from the Archive), incident progress fields, `retry_delay_source` (server `Retry-After` vs. local pacing), `storage`, `derives`, and `youtube_access`.

### Archive retries and recovery

- Transfer and recovery workers share a durable cooldown after network, service, or auth failures. One worker probes at a time; backoff is capped at 15 minutes unless `Retry-After` requires longer. Only an acknowledged upload or verified remote original releases queued service retries; metadata recovery alone does not. A metadata-only probe hands a missing original to one upload worker before reopening traffic.
- Before item creation, uploads, and recovery transfers, Saga calls IA-S3 `check_limit`. Over-limit defers the write as throttling; an unreadable check does not block and logs a warning. `check_limit` does not count the catalog task backlog, so `SAGA_ARCHIVE_TASK_QUEUE_LIMIT` reads the account task summary (shared for `SAGA_ARCHIVE_TASK_SUMMARY_TTL`) to fill that gap.
- A `SlowDown` reply to item creation proves the item was not created, so the upload retries with backoff. Empty item responses trigger a guarded create-only retry; existing items require matching Saga provenance before file recovery. Uncertain transfers keep their originals for guarded reconciliation.
- Upload recovery retries indefinitely; `SAGA_BACKGROUND_MAX_ATTEMPTS` and `SAGA_BACKGROUND_DEADLINE` apply only to optional background work.
- The upload-ready buffer counts due uploads and active downloads, not originals in retry backoff, so downloads continue within staging, buffer, and disk limits during an outage.
- Retries prefer the formats recorded for validated cached inputs. If formats disappear or change, only owned, validated inputs are reset.
- Upload display text drops control and surrogate characters the Archive rejects.
- `SAGA_ARCHIVE_INTERACTIVE_PRIORITY` is sent or omitted on every write to an item together, because mixed priorities can reorder writes.
- Multipart uploads save resume state in SQLite after each acknowledged part; originals needing more than 1000 parts use larger parts.

### Derives

- The derive option is stored on each item at its first write; resumed and recovery writes reuse it, so a setting change applies only to items not yet started.
- With `deferred`, one worker requests derives oldest-upload-first while queued + running tasks are below `SAGA_ARCHIVE_DERIVE_BACKLOG_TARGET`, `SAGA_ARCHIVE_TASK_QUEUE_LIMIT` is not deferring writes, and no upload is waiting for a worker.
- Requested derives are polled every `SAGA_ARCHIVE_DERIVE_POLL_INTERVAL` in small batches. A finished derive counts as done only when the item lists derivative files.
- Derives without progress fail after `SAGA_ARCHIVE_DERIVE_STALE_DAYS`: `DeriveStalled` (no task ID, or the Tasks API no longer tracks the task) or `DeriveNoOutput` (task finished without derivatives). A timeout happens only on a check where the Tasks API answered and a complete item read showed no derivatives; throttled, failed, invalid, or incomplete checks never time out a derive.
- Failed derives are never retried automatically; `saga.py retry-derives` queues exactly one new derive each. The failed task stays in the item's task list.
- Alerts fire when a derive is failed, unconfirmed, or held for more than 24 hours, rejected for credentials, or when the oldest pending derive exceeds `SAGA_ARCHIVE_DERIVE_MAX_LAG`.

### Storage

- `SAGA_MAX_DATA_BYTES` counts regular files under `SAGA_DATA_PATH` (downloads, partials, database, backups). Saga reserves estimated download and merge space across workers and rechecks during download callbacks and merge progress. It is an application budget, not a quota: database and backup writes and underestimated merges can exceed it; files outside the data path, symlink targets, and allocation overhead are not counted.
- `SAGA_MINIMUM_FREE_BYTES` is independent of the data budget.
- Partial cleanup removes unavailable-video inputs first, then the oldest retryable inputs older than `SAGA_PARTIAL_MIN_AGE`. Completed originals, active operations, unknown directories, and invalid manifests are protected.

### YouTube access

- Each extraction re-reads the cookie file; Saga checks for cookie changes every 30 seconds and reschedules failures from earlier sessions. Metadata rechecks get priority over the metadata buffer. Auth alerts count only failures under the current cookies; older ones appear under `youtube_access.awaiting_recheck`.
- Age checks, sign-in requirements, and bot challenges retry with backoff instead of marking videos unavailable.
- The PO token provider server and Python plugin versions must match (server `2.0.0` for native installs).

### Webpage capture

- Without both Ziggy variables, Saga submits watch pages to the Wayback Machine through Archivist. Ziggy needs no Internet Archive credentials.
- Ziggy connection refusal and DNS failures before the POST is sent retry with durable backoff. Lost or malformed receipts, read timeouts after a response, and redirects stay uncertain and require reconciliation. Saga never follows submission redirects.

### Startup migrations

One-time startup migrations restore work left by older releases without manual edits: expired recovery backlogs (legacy deadlines removed; attempt history, pending backoff, and ownership conflicts preserved), uploads rejected for control characters, auth-abandoned uploads and recovery checks, HTTP 400 login errors mislabeled as auth failures, access failures marked unavailable, format-change failures, legacy Ziggy jobs whose saved error proves the connection was refused before submission, and derive tracking for items uploaded before `SAGA_ARCHIVE_DERIVE` existed (recorded as `immediate`, then read once by a slow background pass).

## Docker

`Dockerfile` builds a multi-arch (amd64/arm64) Alpine image with uv-managed Python 3.15, ffmpeg, and Deno. `docker-entrypoint.sh` applies `PUID`/`PGID` and cache ownership. The build asserts the `urllib3`/`urllib3-future` split between yt-dlp and niquests; keep the `[tool.uv]` build settings in `pyproject.toml` intact.

## Git

- Work and commit on the current branch (`main`); do not create branches unless asked.
- Commit messages use Conventional Commits with a lowercase, outcome-focused summary: `fix: retry Ziggy submissions that never connected`, `feat: ...`, `test: ...`, `chore: ...`.
- Keep `uv.lock` committed and in sync (`uv sync --frozen` is used in CI and Docker).
