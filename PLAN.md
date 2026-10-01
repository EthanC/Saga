# Upload-First Archival Plan

## Design

**Saga should consider a video uploaded when Internet Archive acknowledges the complete file transfer.** It should persist that result, delete local staging immediately, and move on. Validation and webpage capture become independent background jobs.

The main pipeline becomes:

```text
Metadata → Download → Ready to upload → Upload acknowledged → Delete staging
                                              │
                                              ├─ Optional validation
                                              └─ Webpage capture
```

## 1. Make Upload Acknowledgement the Completion Event

In `core/archival.py`:

- Change new uploads to Archivist's `upload(wait=False)`.
- Capture the returned result and require acknowledgement for the expected file.
- Persist `media_uploaded_at`, transferred size, item identifier, filename, and the returned receipt/ETag.
- Release the upload worker immediately after that transaction commits.
- Schedule staging cleanup immediately. Its eligibility depends on the persisted upload acknowledgement.
- Track webpage capture and validation independently, with their own statuses and retry schedules.

Archivist **1.3.0 already supports this for new uploads**. Saga currently discards the result and waits for additional processing.

Retain cheap operational checks: the downloader and merge completed successfully, the final file exists and is nonempty, and the upload received an acknowledgement.

## 2. Enforce Upload-First Scheduling

Replace the shared archive-worker queue in `core/service.py` and `core/state.py` with separate workers:

| Priority | Work | Scheduling policy |
| --- | --- | --- |
| **1** | Ready uploads and due upload retries | Reserved upload capacity; dispatch immediately when available |
| **2** | Downloads and prerequisite metadata | Maintain a bounded supply of upload-ready files |
| Background | Validation, webpage capture, uncertain-transfer checks | Separate capacity, deadlines, and retry budgets |

**Starting configuration:** one upload worker and one download worker. Downloads can run alongside uploads, but uploads always have reserved capacity.

Additional controls:

- Start with a buffer of **two upload-ready files**, subject to available disk space.
- Reserve space for active downloads and ffmpeg's temporary merge output.
- Pause new downloads when the buffer or disk budget is full; continue draining uploads.
- Keep metadata fetching available to supply downloads, including newly discovered videos.
- Apply exponential backoff with jitter to failed jobs. A failed upload waiting for its retry time releases its worker.
- Use independent, atomic job claims so background work cannot lock a video out of transfer processing.

This prevents a long download or a pending verification from occupying the only upload-capable worker.

## 3. Remove Validation from the Transfer Path

Changes in `core/youtube.py` and `core/archival.py`:

- Remove mandatory `ffprobe`, MD5, and SHA-1 validation before upload.
- Remove full-file hashing from upload preflight and reconciliation.
- Record download completion explicitly and finalize output atomically. An existing `.mkv` alone should not be treated as proof that an interrupted download finished.
- Make optional validation use remote metadata, saved receipts, file size, and existing checksums where available.
- Record outcomes such as `pending`, `passed`, `failed`, and `expired` independently of upload completion.

**Recommended default: optional validation disabled during the backfill.** When enabled, give it one background worker, bounded requests, increasing retry intervals, and a finite retry budget. Extra whole-file reads should require explicit opt-in.

Internet Archive's derivative tasks, unavailable replicas, missing checksums, or delayed public listings must have no effect on an acknowledged upload's completion, cleanup, or report count.

## 4. Handle Interrupted Uploads Without Recreating the Bottleneck

Separate **unknown transfer outcomes** from optional validation:

- **Acknowledged transfer:** record success and clean up.
- **Known failed transfer:** retry with backoff.
- **Connection lost before acknowledgement:** retain staging and schedule a bounded remote check.
- **Remote evidence confirms the expected original:** record recovered success.
- **Evidence establishes that a transfer is needed:** enqueue it at upload priority.
- **Remote state remains ambiguous:** defer that job while other transfers continue.

Persist the item ID, operation ID, and expected filename before sending bytes. Recovery writes should be scoped to that persisted Saga operation.

**This also requires an Archivist change.** Its existing `add_files()` hashes local files before network access and gates recovery on item readiness, even with `wait=False`. Add a transport-focused recovery API that returns acknowledged file-transfer results independently of derivative processing and checksum validation.

Also repair the missing-file retry path: an upload-ready job whose staging file disappeared should return to downloading instead of retrying the same impossible upload indefinitely.

## 5. Report Uploads Directly

Update `core/state.py` and `core/reporting.py` so the primary metrics use `media_uploaded_at`:

- **Videos uploaded**
- **Bytes uploaded**
- **Lifetime uploaded**
- Ready uploads, pending downloads, active transfers, and failed/uncertain transfers

Show optional validation and webpage-capture results separately.

Add operational visibility for:

- Last successful upload and current transfer progress.
- Oldest ready upload.
- Staging usage and download-pause reason.
- Worker time spent transferring versus background checks.

Alert when upload-ready work has neither transfer progress nor a recorded reason for waiting. The current polling-based healthcheck cannot detect this failure.

## 6. Migrate the Existing Queue

Implement an idempotent database migration and rehearse it against a consistent database backup.

At deployment:

1. Drain or stop the old workers and take a consistent SQLite backup.
2. Deploy a pinned, tested image and migrate with one Saga instance.
3. Carry existing verified successes into the new upload-completion model.
4. Recheck the three identified stuck originals using remote metadata and saved facts. Once confirmed, record recovered completion and release their roughly **121 GiB** of staging.
5. Reset the missing-file job, `0UCmCkIDANU`, to download.
6. Resume uploads and the bounded download pipeline.

Mark recovered historical uploads separately from newly transferred bytes so migration does not inflate the next daily report. Version the report semantics and preserve historical snapshots.

## 7. Prove the Failure Cannot Recur

Add regression tests around the actual failure modes:

- Hundreds of permanently pending validation jobs cannot delay a ready upload.
- An upload acknowledged while derivatives remain pending counts immediately and triggers cleanup.
- Upload and status-check paths perform no whole-file checksum reads.
- A failed upload releases capacity for another eligible upload.
- A long download leaves upload capacity available.
- Restarting during transfer, after acknowledgement, or during cleanup recovers without duplicate report counts.
- Missing local media returns to download.
- Disk reservations prevent downloads from exhausting staging.
- Unavailable webpage capture and disabled validation allow normal upload completion.

**Acceptance criterion:** with validation and Internet Archive derivative processing stalled indefinitely, Saga continues uploading available files, replenishing its download buffer, reclaiming acknowledged staging, and reporting successful transfers.
