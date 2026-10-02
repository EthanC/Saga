# Plan 09: Pipeline health and actionable alerts

Priority: P2. Change types: application code and operational configuration.

## Coverage gap

The audited deployment was healthy while recent videos remained blocked at metadata. Current health combines a polling heartbeat with upload progress. It does not establish whether metadata or downloads are advancing.

A startup also logged `Upload queue stalled` while an old ready item was being dispatched. The Docker start period suppresses failed healthchecks during startup, but it does not suppress the application's Discord-forwarded error.

The new model must distinguish worker failure, expected backpressure, slow but advancing work, and age-based preservation warnings.

## Configuration changes

Retain `SAGA_TRANSFER_STALL_TIMEOUT=900` as the initial no-byte-progress timeout for active transfers. Proposed additional policy values are:

- 120 seconds of application startup grace for dispatch-related stall evaluation.
- A warning after five minutes of due recent metadata work with no eligible dispatch and no documented reason.
- A warning after 15 minutes of recent download waiting, including the reason and current worker occupancy. A long earlier download can make this a queue-age warning without making the entire container unhealthy.

Expose new values through validated settings only where operators need tuning, then document them in `.env.example`. Begin with alert-only operational handling. Docker's `unless-stopped` restart policy does not react to an unhealthy state.

## Code changes

Primary files: `core/state.py`, `core/service.py`, `core/healthcheck.py`, `core/youtube.py`, and `core/reporting.py` where daily summaries need queue-age context.

1. Version the `transfers.json` document. Add lane records containing active count, due queue count, oldest due age, last byte/phase progress, and waiting reason. Keep the existing top-level `healthy` field during a compatibility period.
2. Add durable download byte-progress and phase facts where necessary. Separate downloading from merging, since network bytes stop advancing during a valid merge. Merge progress should use actual output growth or a controlled subprocess progress signal, not merely thread liveness.
3. Use plan 05's checkpoint policy. Update last-progress timestamps only when work advances; callbacks repeating the same byte count cannot renew health.
4. Track metadata admission reasons from plan 01 and the actual staging floor/reservations from plan 02. Include inactive-partial bytes and retention decisions from plan 04.
5. Calculate idle-upload stall time from current eligibility and available dispatch capacity. An old `ready_at` timestamp alone must not trigger an immediate startup alarm before the worker can claim the job.
6. Apply application startup grace to dispatch evaluation and logging. Continue validating the health document's freshness so grace cannot conceal a dead publisher indefinitely.
7. Expose recent-video preservation delay independently from upload throughput. A working upload lane must not erase a metadata-starvation warning.
8. Describe expected waits explicitly: retry deadline, disk floor, reserved merge space, full ready buffer, occupied worker, or dependency failure. Expected waits are degraded service information rather than evidence that a process restart will help.
9. Detect failed/expired recovery, validation, and capture jobs as operator-visible outcomes. Keep optional background failures from blocking acknowledged-transfer cleanup or marking all transfer capacity dead.
10. Emit alerts on transitions, recovery, and a bounded reminder interval. Consolidate repeated failures by lane/reason to avoid an error every 30 seconds.
11. Use each channel's own successful-poll timestamp if health is expanded to multiple channels. A healthy channel should not refresh another channel's failed polling state.

Coordinate additive persistence columns and their migration with plan 05. Do not create two competing migrations for the same progress fields.

## Health-state contract

| Condition | Expected classification |
| --- | --- |
| Download or upload bytes are advancing | Healthy transfer, even if slow |
| Merge output is advancing | Healthy merge phase |
| Work waits for disk, retry, or buffer capacity | Named backpressure/degraded condition |
| Recent metadata ages behind an incorrect gate | Preservation-delay alert |
| An active transfer has no byte/phase progress beyond its timeout | Unhealthy affected lane |
| Publisher or polling heartbeat is stale | Unhealthy service component |
| Optional validation/capture expires | Background-job alert; uploads remain independent |
| Old ready work is seen immediately after startup | Dispatch grace, followed by normal evaluation |

Define the aggregate Docker health rule from these classifications. Alert severity and process liveness must not be conflated.

## Verification and rollout

Extend `tests/test_healthcheck.py`, `tests/test_upload_state.py`, and service tests with fake-clock scenarios for every contract row. Include a large metadata backlog alongside an active upload, a retry deadline that becomes due after a long wait, an interrupted health-document write, identical byte callbacks, and a long valid merge.

Replay the startup sequence that produced the audit's false alarm. Require a named alert when metadata is starved, but no startup stall error before the grace and dispatch window have elapsed.

Publish the expanded document and alerts first. Observe a full day of backfill and at least one restart before considering automated recovery. Any later recovery action must preserve claims/receipts, have a cooldown, and avoid restarting a container solely for expected disk pressure or upstream slowness.

## Acceptance and rollback

Repository implementation: version 2 health documents with lane/worker counts, recent/backfill due counts, due age, download/merge progress, staging and retention facts, oldest recent items and reasons, and per-channel successful-poll timestamps. Dispatch stalls use current eligibility plus 120-second startup grace; preservation ages and optional job outcomes remain separate warnings. Alerts emit on transition/recovery with 15-minute reminders. The plan 04 migration supplies progress fields. Fake-clock contract, interrupted-publication, and per-channel freshness regressions pass; local checks reached 582 tests with 100% branch coverage. Production observation and alert-only response are operator checks.

The operator can identify the oldest recent item, its stage, and its waiting reason without reading the database. Injected lane stalls are detected, progressing transfers remain healthy, and recovered conditions generate one recovery event.

Rollback must preserve compatibility with the previous health-document consumer or revert producer and consumer together. New progress columns should remain harmless to a compatible older image.

Dependencies: [scheduling](PLAN_01_RECENT_VIDEO_SCHEDULING.md), [disk accounting](PLAN_02_STAGING_DISK_ACCOUNTING.md), [checkpointing](PLAN_05_PROGRESS_CHECKPOINTING.md), [logging](PLAN_06_LOGGING.md), and [shutdown](PLAN_08_GRACEFUL_SHUTDOWN.md).
