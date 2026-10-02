# Plan 08: Bounded graceful shutdown

Priority: P2. Change types: application code and deployment configuration.

## Observed stop behavior

At the audited restart, Saga logged a shutdown request at 06:00:02 and the container stopped approximately 30 seconds later. There was no normal `Saga stopped` message, and startup recovered one interrupted archive claim. This is consistent with exhausting `stop_grace_period: 30s`; retained Docker events did not establish the exit signal directly.

`SagaService` sets a stop event but waits for blocking archive operations. Cancellation shielding and `ThreadPoolExecutor` shutdown can wait for an upload or merge that lasts longer than the Docker deadline.

## Configuration changes

- Proposed application stop budget: 90 seconds, including checkpointing and client cleanup.
- Proposed Compose `stop_grace_period`: `120s`, leaving 30 seconds beyond that application budget.
- Keep per-request inactivity timeouts below the remaining stop budget. The existing archive timeout is 60 seconds; plan 03 proposes a 30-second downloader socket timeout.
- If the application budget becomes configurable, introduce a validated setting such as `SAGA_SHUTDOWN_TIMEOUT=90` and document its relationship to Compose's grace period.

The two-minute grace period is a target for a bounded implementation. Raising it alone does not allow an arbitrary multi-hour upload to finish.

## Code changes

Primary files: `saga.py`, `core/service.py`, `core/archival.py`, `core/youtube.py`, and `core/bandwidth.py`.

1. Create one stop controller visible to asyncio tasks and blocking workers, using a thread-safe event plus a monotonic deadline. The signal handler requests shutdown; it performs no database or network work itself.
2. Stop issuing new claims and new HTTP attempts once shutdown is requested. Retry sleeps and bandwidth waits must wake on the stop event rather than consume their full delay.
3. Check for cancellation in downloader progress hooks and upload body reads. Raise a typed cooperative-stop outcome, distinct from unavailable content and ordinary transient failures.
4. For a stopped download, flush any required progress state, preserve compatible `.part` files, release the reservation, and leave the workflow resumable. Do not create a completion receipt for an interrupted merge.
5. For an upload stopped after mutation could have begun, persist `reconcile_upload` with the original operation and file facts. An acknowledged upload must commit its receipt before shutdown proceeds to cleanup.
6. Bound ffmpeg and other child-process shutdown. Track only children owned by the current operation, send termination, wait for a short deadline, and escalate for those children if necessary. Avoid process-name-wide termination.
7. Verify the embedded downloader exposes enough control to implement that bound. If a native call or library-managed child cannot be interrupted reliably, place that blocking operation behind a managed worker-process boundary. Keep the parent responsible for durable transitions and communicate typed progress/results. Cancelling a Future does not terminate a running Python thread.
8. Bound metadata/history operations too. Existing retry loops must observe stop before beginning another attempt, and cancellation must not leave executor shutdown waiting indefinitely.
9. Drain or checkpoint active cleanup, backup, and report work within the remaining deadline. Preserve ambiguous notification/report outcomes according to their existing at-least-once behavior and document that behavior.
10. Close persistent state only after workers can no longer write. Flush queued logging within a bounded final interval and emit one stop summary with unfinished operation IDs and their saved stages.

## Failure-injection matrix

| Stop point | Required outcome |
| --- | --- |
| Before claiming a job | No new network work starts |
| Mid-download | Partial remains resumable; reservation and claim are released or recoverable |
| During retry or bandwidth sleep | Wait is interrupted promptly |
| During merge | Owned child exits within budget; no false completion receipt |
| Mid-upload | Remote outcome is reconciled before any replacement PUT |
| After remote acknowledgement | Receipt commits; cleanup may finish on the next start |
| During cleanup | Restart can finish cleanup idempotently |
| During metadata/history request | Request timeout or managed-process deadline bounds shutdown |
| During SQLite backup | Published snapshot is valid or temporary output is left for controlled cleanup |

Use a subprocess/container test harness with slow local HTTP fixtures and simulated child processes. Test the real signal path, rather than calling only `request_stop()` inside a unit test. Extend service and upload-failure tests and add an integration test for container termination if the current tests cannot exercise PID 1 behavior.

## Acceptance

Repository implementation: thread-safe stop controller, interruptible waits, typed resumable/uncertain stop outcomes, owned spawned-process boundaries for blocking HTTP and ffmpeg descendants, parent-side progress/receipts and aggregate limiting, cooperative SQLite backup cancellation, and shutdown summaries. README proposes a 90-second application budget (validated minimum 10 seconds) and 120-second Compose grace. Tests exercise the real entrypoint SIGTERM handler against a slow local HTTP mutation, retain unacknowledged staging, and persist reconciliation before exit within a 20-second harness limit. Owned descendant termination, retry/bandwidth waits, stopped downloads, accepted receipts, submitting jobs, and incomplete backup publication are covered. Local checks passed with 558 tests and 100% branch coverage. Production stop timing and container PID-1 verification remain release checks.

With each failure point injected, the application exits within the proposed 90-second budget and Docker does not need its 120-second deadline. The next start resumes or reconciles work without duplicated upload counts, deleted unacknowledged media, or orphaned owned children.

Normal idle shutdown should take seconds. Record the measured worst case and adjust the proposed budgets before deployment if the implementation cannot meet them.

## Rollout and rollback

Deploy the bounded-stop implementation and matching Compose grace together. Validate on test storage before an intentional production restart. Inspect the first production stop summary and recovery results.

Rollback can restore the prior image and timeout values, subject to schema compatibility. Current operation IDs and receipts must remain intact. A rollback image with the old blocking behavior again depends on forced termination when work exceeds its grace period.

Dependencies: [retry behavior](PLAN_03_HTTP_TRANSPORT_AND_RETRIES.md), [partial resumption](PLAN_04_PARTIAL_DOWNLOAD_RETENTION.md), and [progress checkpointing](PLAN_05_PROGRESS_CHECKPOINTING.md).
