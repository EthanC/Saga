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
- **Docs.** User-visible behavior changes are documented in `README.md` in the same plain, declarative style.
- Type-annotate everything; code must pass `ty check`. Match existing docstring style (single-sentence, imperative or descriptive).

## Docker

`Dockerfile` builds a multi-arch (amd64/arm64) Alpine image with uv-managed Python 3.15, ffmpeg, and Deno. `docker-entrypoint.sh` applies `PUID`/`PGID` and cache ownership. The build asserts the `urllib3`/`urllib3-future` split between yt-dlp and niquests; keep the `[tool.uv]` build settings in `pyproject.toml` intact.

## Git

- Work and commit on the current branch (`main`); do not create branches unless asked.
- Commit messages use Conventional Commits with a lowercase, outcome-focused summary: `fix: retry Ziggy submissions that never connected`, `feat: ...`, `test: ...`, `chore: ...`.
- Keep `uv.lock` committed and in sync (`uv sync --frozen` is used in CI and Docker).
