# Production audit implementation roadmap

Status: repository implementation verified. Production configuration, measurements, image promotion, and acceptance remain operator work. See [the configuration handoff](OPERATIONS.md).

## Audit baseline

The read-only audit ran on October 1, 2026, approximately 19:24 to 19:31 America/Chicago, against `ethan@192.168.1.10`. Production configuration is in `/home/ethan/arcane/projects/saga`. Saga was running revision `7ac2fed4904976295e602aa51a0904d335589489`; the relevant deployed source files matched this checkout.

At that time, both containers were healthy. The queue contained 48 completed videos, 1,462 download-stage jobs, 1,130 metadata-stage jobs, 47 unavailable videos, one ready upload, and one active upload. The latest daily report recorded seven new uploads totaling 290.4 GB. Approximately 193 GB occupied staging, including 93.5 GB of inactive partial downloads. These are audit observations, not current-state guarantees. Storage quantities use decimal GB unless stated otherwise.

## Plan index

| Priority | Plan | Configuration work | Code work |
| --- | --- | --- | --- |
| P1 | [01: Recent-video scheduling](PLAN_01_RECENT_VIDEO_SCHEDULING.md) | Retain the small transfer buffer | Separate recent metadata admission from backfill admission |
| P1 | [02: Staging disk accounting](PLAN_02_STAGING_DISK_ACCOUNTING.md) | Preserve the staging mount and 25 GB floor | Check the correct filesystem and credit resumable bytes |
| P1 | [03: HTTP transport and retries](PLAN_03_HTTP_TRANSPORT_AND_RETRIES.md) | Build separate urllib3 namespaces | Configure and test bounded download retries |
| P1 | [04: Partial-download retention](PLAN_04_PARTIAL_DOWNLOAD_RETENTION.md) | Introduce an observation-first retention policy | Resume partials fairly, adopt legacy names, and reclaim eligible files |
| P2 | [05: Progress checkpointing](PLAN_05_PROGRESS_CHECKPOINTING.md) | Document the checkpoint policy | Reduce reservation commits without weakening disk guards |
| P2 | [06: Logging](PLAN_06_LOGGING.md) | Use INFO and bounded Docker logs | Suppress terminal progress and route redacted yt-dlp diagnostics |
| P2 | [07: Deployment configuration](PLAN_07_DEPLOYMENT_CONFIGURATION.md) | Make environment injection explicit and pin images | Update deployment documentation |
| P2 | [08: Graceful shutdown](PLAN_08_GRACEFUL_SHUTDOWN.md) | Set a grace period above the tested stop budget | Cancel blocking work cooperatively and persist resumable outcomes |
| P2 | [09: Pipeline health](PLAN_09_PIPELINE_HEALTH.md) | Define stage-age alerts and recovery behavior | Measure progress across lanes and suppress startup false alarms |
| P3 | [10: Upload concurrency](PLAN_10_UPLOAD_CONCURRENCY.md) | Trial two upload workers after prerequisites | No initial application change; fix defects only if the trial exposes them |
| P2 | [11: Backup and recovery](PLAN_11_BACKUP_RECOVERY.md) | Verify off-host coverage and rehearse restoration | Add application changes only if backup automation needs a supported interface |

P1 addresses observed preservation delays, failed work, or occupied staging. P2 covers operating cost, repeatable deployment, observability, and recovery. P3 is a measured throughput experiment.

## Configuration versus code

- **Deployment configuration** means files or settings on the production host or in Arcane: Compose, service environment files, logging limits, backup jobs, and image references. The repository does not currently contain production `compose.yaml`.
- **Build configuration** means repository inputs such as `pyproject.toml`, `uv.lock`, Docker build settings, and CI. These require a new image, even when application Python does not change.
- **Application code** means Python, the entrypoint, persistence models, and their regression tests. Any new environment variable named in a plan is proposed and requires parser support before deployment.
- **Documentation** means the example configuration, README, and operational procedures. Secret values belong in deployment storage, not in these plans or version control.

## Implementation sequence

1. Establish reproducible image/environment references through plan 07 and confirm a usable recovery copy through plan 11. This work can precede the code fixes.
2. Implement plans 01 and 02. They unblock recent discoveries and make admission decisions match staging capacity.
3. Implement plan 03 in a separately identifiable image so transport effects can be measured. Complete plan 04 after the reservation calculation and scheduling classes are defined.
4. Implement plan 05 against the corrected reservation model. Plan 06 can accompany these releases; its Docker log limits can be deployed independently.
5. Implement plan 08, then complete plan 09's shutdown/recovery integration. Additive health metrics can be developed alongside plan 05.
6. Run plan 10 only after disk accounting, resumable work, shutdown, and lane-level metrics pass their acceptance checks.

The disk reservation helper is owned by plan 02. Partial-file identity and inventory are owned by plan 04. Durable download progress fields and health-document versioning are owned by plan 09; plan 05 defines their write frequency. Use one agreed schema migration for shared fields rather than separate conflicting migrations.

## Verification policy

For application changes, start with the regression cases listed in the affected plan. Before publishing an image, run the checks already required by `.github/workflows/ci.yaml`:

```console
uv sync --frozen --all-groups
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest --cov=core --cov-branch --cov-report=term-missing --cov-report=xml --cov-report=html --cov-fail-under=100
```

The current Ruff configuration enables fixes. Run these implementation checks in the intended working tree and inspect any resulting edits. Configuration-only changes need rendered-Compose and runtime verification; they do not need artificial unit tests that repeat YAML values.

Persistence changes must also pass an idempotent migration rehearsal against a consistent SQLite snapshot. Existing `core.migration` supplies the rehearsal entrypoint. Use test storage and fake external clients when exercising a production-shaped queue, so the rehearsal cannot post notifications or create archive items.

## Release and rollback rules

- Record the current image digest, rendered service configuration, verified SQLite snapshot, and queue totals before each release.
- Keep one Saga writer for a data directory. The separate download mount is part of that directory's recovery mapping.
- Prefer additive schema changes. Rehearse both the new image and the intended rollback image against a snapshot before relying on backward compatibility.
- Revert image or configuration against current durable state when compatible. Restoring an old database after successful remote uploads loses acknowledgements; that operation belongs to the reconciliation procedure in plan 11.
- Measure each release against its own acceptance checks. Transport and concurrency improvements are experiments until observed, rather than assumed throughput gains.

## Completion record

For each implemented plan, record the commit, image digest, deployment time, verification results, and any revised operational values. Mark the plan complete only when its production acceptance checks pass.

### Repository implementation record

One dedicated commit was created for each numbered plan. The roadmap verification commit contains the final cross-plan acceptance fixes and handoff. No production acceptance is claimed by these repository records.

| Plan | Dedicated commit | Repository result |
| --- | --- | --- |
| 01 | `0271b82` | Independent recent metadata admission and successful-claim fairness |
| 02 | `92e8ac1` | Shared staging path and download/merge reservations |
| 03 | `330146b` | Separate HTTP namespaces and bounded media retries |
| 04 | `2740ea8` | Input identity, resume classes, adoption, and observe/enforce retention |
| 05 | `7ce1798` | Five-second routine checkpoints with immediate growth guards |
| 06 | `2678bbd` | Progress suppression, redacted diagnostics, and logging documentation |
| 07 | `6d18c1d` | Explicit service environment and digest promotion documentation |
| 08 | `82be693` | Owned blocking workers, cooperative stop, and resumable outcomes |
| 09 | `6b9f558` | Versioned lane health, preservation alerts, and per-channel freshness |
| 10 | `b6c15ec` | Two-worker claims, shared limiting, independent recovery/cleanup tests |
| 11 | `215c58e` | Off-host inputs, restoration, and reconciliation procedure |
| 00 | This document's commit | Acceptance fixes, image smoke checks, and configuration handoff |

Final acceptance tightened legacy identity validation before disk admission, native download-receipt promotion under disk pressure, ffmpeg `.temp.mkv` progress measurement, sidecar/output ownership checks, redaction of versioned webhooks/header dictionaries, and stop propagation after component failures. These fixes use the same additive schema migration (version 3); frozen metadata, operation IDs, attempts, notification facts, and upload receipts remain intact.

Verification completed on October 2, 2026 UTC (October 1 America/Chicago):

- `uv sync --frozen --all-groups`, Ruff format/lint, and `ty check` passed in a fresh dependency environment.
- The required coverage command passed: **619 tests, 100% statement and branch coverage**. XML and HTML reports were generated.
- Local HTTP acceptance exercised interrupted/range downloads, changed representations, legacy continuation at a nonzero offset, and Niquests single-attempt PUTs with bandwidth limiting. All external clients used fixtures.
- The migration CLI applied the new schema twice to a consistent 2,530-row production-shaped snapshot and passed integrity/foreign-key checks. Baseline source `7ac2fed` also opened and rehearsed a migrated copy twice; all 2,530 rows' statuses, attempts, frozen metadata, operation IDs, receipts, and acknowledgement times matched before and after. This checks the baseline reader, not a deployed rollback image.
- Docker builds and UID-1000 startup/health smoke checks passed for `linux/amd64` and `linux/arm64`, including separate client namespaces and ffmpeg/Deno execution. Local test image IDs were `sha256:992275d78394302e89c0619957eab38c2abbc03b9766cfb30056144968c2e091` and `sha256:35eb7df1aeb1ebd297c0a67b80879299b3a25d209af06225a1cac448d56f56a2`. These are local verification images, not production registry references.
- Signal, owned-child termination, shutdown receipt/reconciliation, checkpoint transaction-count, retention race/failure, lane-age, publisher interruption, and concurrent-upload regressions passed.

Production image digests, deployment times, off-host destination and restore timing, retention enforcement, throughput comparisons, and production acceptance results remain to be recorded after the operator applies [OPERATIONS.md](OPERATIONS.md).
