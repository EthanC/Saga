# Plan 01: Recent-video scheduling

Priority: P1. Primary change type: application code.

## Problem and target behavior

During the October 1 audit, `T6-QrUcHhv0` and `zuzBlGUDxx4` had been discovered approximately 57.5 hours earlier. Both had successful Discord deliveries but remained at `metadata` with zero archival attempts.

`StateStore.claim_archive()` blocks the metadata lane when the non-disk-blocked download count reaches `2 * upload_ready_buffer`. Production had 1,378 qualifying rows against a cutoff of four. This check runs before recent-video selection, so the current gate prevents those videos from progressing. The audit does not attribute their entire pre-deployment wait to this version.

Recent discoveries must receive prerequisite metadata capacity even while a migrated backfill exceeds the normal supply limit. Backfill work must continue receiving a bounded share of successful dispatches.

## Configuration changes

- Retain `SAGA_UPLOAD_READY_BUFFER=2`, one download worker, and one upload worker during this fix.
- Do not increase the transfer buffer to match the migrated queue length. It also controls staging admission.
- Derive the initial recent-metadata allowance from the existing small buffer. A new production environment variable is unnecessary for the first implementation.
- Document that recent-work priority uses durable notification eligibility, not a rolling age filter that would exclude already-delayed videos.

## Code changes

Primary files: `core/state.py`, `core/service.py`, and `tests/test_upload_state.py`.

1. Split metadata eligibility into recent and backfill classes. Use the persisted `Video.notification_candidate` flag for the recent class until the workflow completes or reaches a terminal state.
2. Apply the existing backfill supply limit only to backfill admission. Give recent metadata an independent allowance, initially equal to the ready-buffer count. Count claimed metadata and prepared recent download work atomically so multiple workers cannot exceed the allowance.
3. Define how retry-delayed and disk-blocked recent jobs affect supply. A job that cannot currently run must have an explicit waiting reason and must not permanently prevent later recent discoveries from receiving metadata.
4. Preserve the download lane's disk and ready-buffer checks. Metadata admission does not grant permission to download or upload.
5. Count the recent/backfill fairness cadence on successful claims. `_archive_worker_loop()` currently advances `claim_count` even when no job was claimed; empty polling cycles should not decide which class receives the next real dispatch.
6. Keep due recent downloads ahead of ordinary backfill while coordinating the resumable-partial class introduced by plan 04. Use bounded weighted dispatch rather than strict priority that can starve backfill.
7. Expose metadata pause reasons for plan 09, including separate recent and backfill supply limits.

Existing queue rows should be usable without resetting attempts, frozen metadata, notification state, or upload receipts. Implement this as a scheduling change unless the concurrency tests demonstrate a need for additional durable claim information.

## Regression cases

- Seed at least 1,400 download-stage backfill rows and a recent metadata row. The recent row is claimed while the backfill metadata gate remains closed.
- Seed the two recent classes represented by a normal video and a Short. Both advance when their prerequisite allowance becomes available.
- Repeated empty worker polls do not consume the fairness quota.
- Sustained recent arrivals still allow backfill dispatches at the defined quota.
- A recent job in retry backoff or disk wait does not monopolize prerequisite capacity indefinitely.
- Parallel connections cannot claim the same job or exceed the recent allowance.
- A full upload-ready buffer still stops additional downloads; ready uploads remain dispatchable.
- Already completed or unavailable videos are not reintroduced into the queue.

## Rollout

Repository implementation: independent transactional recent/backfill metadata allowances, explicit supply reasons, and a three-to-one cadence based on successful claims. Retry-delayed and disk-blocked prepared work is excluded from supply. Regression coverage includes 1,400 backfill rows, videos/Shorts, concurrent connections, and unchanged fairness across empty polls. Production advancement and sustained-arrival acceptance remain deployment checks.

Rehearse with a consistent, sanitized production-shaped snapshot. Publish a pinned image and deploy with the existing worker counts. Observe the two previously delayed video IDs and a newly discovered or test-seeded recent item.

The metadata attempt count should advance on the first available recent-metadata dispatch. If a subsequent stage waits, the reason must be visible and attributable to disk, buffer, retry, or worker occupancy. Existing upload work must continue progressing.

## Acceptance and rollback

Accept when the oversized-backfill regression passes, the recent queue advances in production, and backfill dispatches continue under sustained recent load. No exact end-to-end completion time is promised for videos whose download size or upstream availability differs.

For a scheduling-only implementation, rollback uses the previous image and the current database. If a schema change becomes necessary, apply the migration/rollback rehearsal policy in [the roadmap](PLAN_00_AUDIT_ROADMAP.md).

Dependencies: coordinate dispatch policy with [partial retention](PLAN_04_PARTIAL_DOWNLOAD_RETENTION.md) and waiting-reason metrics with [pipeline health](PLAN_09_PIPELINE_HEALTH.md).
