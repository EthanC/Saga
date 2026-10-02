# Plan 05: Download progress checkpointing

Priority: P2. Primary change type: application code.

## Cost observed during the audit

Each yt-dlp progress callback invokes `StateStore.guard_download_space()`. That method takes the shared write lock, reads other reservations, updates the current row, and commits. A ten-second production sample recorded 31 SQLite commits. The container had also accumulated approximately 2.7 GB of root-device writes since its current start; that figure includes more than progress bookkeeping.

Reduce progress-related transactions while preserving immediate reactions to a larger expected download and keeping completion receipts durable.

## Configuration changes

- Use a five-second maximum routine checkpoint interval initially, matching the existing upload progress cadence.
- Treat that interval as an internal policy for the first implementation. Add a runtime setting only if measurements show a need to tune it independently.
- Keep SQLite's existing durability settings. Reducing transaction frequency does not require lowering synchronous durability or disabling WAL.
- Document which health values are live estimates and which are durable checkpoints.

## Code changes

Primary files: `core/youtube.py`, `core/archival.py`, and `core/state.py`. Coordinate shared progress fields with plan 09.

1. Separate per-callback space validation from durable reservation checkpoints. Keep current stream sizes and byte progress in a worker-local accumulator.
2. Hold a conservative durable reservation between checkpoints. When the expected input size or required reservation grows beyond the allowance, perform the atomic guard and update immediately before continuing.
3. Check current filesystem headroom at a bounded cadence based on the reserved margin and maximum write increment. Throttling database commits must not also disable disk checks for five seconds of uncontrolled writes.
4. Persist shrinking reservations and ordinary byte progress at most once per five seconds while work proceeds. Update sooner at stream boundaries, before merge, when a disk pause occurs, or when a claim is released.
5. Persist download completion, upload acknowledgement, recovery outcome, and cleanup transitions immediately. These facts determine resumption and deletion and must not share a lossy progress buffer.
6. Record progress timestamps only when bytes advance. Repeated callbacks containing the same byte count must not make a stalled transfer look active.
7. Clear worker-local accounting when the operation changes. Restart recovery should rebuild conservative reservations from persisted facts and validated staging files.
8. Avoid querying and committing unchanged values. If a cached aggregate is introduced for other workers' reservations, keep the transactional database check authoritative for admission and increases.

If plan 09 adds durable download byte and phase fields, implement their migration once and use the same checkpoint boundary for those fields.

## Measurement and regression tests

Capture a baseline with upload and download both active: committed transactions per second, process CPU time, database/WAL I/O, write-lock wait time, and completed payload bytes. Distinguish progress transactions from other state changes when measuring the candidate build.

Use a fake clock and transaction instrumentation to verify:

- Thousands of unchanged or monotonic progress callbacks produce bounded routine checkpoints.
- A larger expected size triggers an immediate reservation decision even inside the checkpoint interval.
- Other download workers cannot use bytes still reserved by this operation.
- External disk depletion is detected without waiting for the next routine database checkpoint.
- Finalization and failure transitions flush required state exactly once.
- Repeated identical progress does not refresh the last-byte-progress timestamp.
- A process restart between checkpoints resumes safely with conservative space accounting.

Extend the reservation cases in `tests/test_upload_state.py`, progress cases in `tests/test_upload_failures.py`, and downloader tests as appropriate. Use a transaction counter tied to actual state changes rather than assertions about helper call names.

## Acceptance

Repository implementation: worker-local progress accumulator, five-second routine checkpoints, immediate reservation increases and boundary checks, per-callback filesystem/read-only budget checks, and failure flushing. The plan 04 schema carries durable download/merge facts. Production commit-rate and I/O comparison remains a deployment measurement.

Routine progress persistence occurs no more frequently than once per interval per worker, excluding estimate changes and required transitions. Steady-state progress-related commit frequency falls relative to the measured baseline. Disk-floor and concurrent-reservation tests continue to pass, and every completed upload retains its immediate durable acknowledgement.

Health byte counts may lag by up to the documented checkpoint interval. Completion counts and deletion eligibility must not lag for that reason.

## Rollout and rollback

Implement after [staging disk accounting](PLAN_02_STAGING_DISK_ACCOUNTING.md). Keep worker counts fixed during the first comparison. Observe a full large download, merge, upload, and cleanup cycle before combining the change with concurrency tuning.

Rollback can restore the earlier per-callback persistence policy through an image change. Any new progress columns should be additive and compatible with the rollback image. Current receipts and staging remain the source of truth.

Related plan: [pipeline health](PLAN_09_PIPELINE_HEALTH.md).
