# Plan 02: Staging disk accounting

Priority: P1. Primary change type: application code.

## Evidence

Production binds the database directory to `/data` and a separate SSD directory to `/data/downloads`. The initial audit snapshot showed approximately 189 GB available on the former and 141.5 GB on the latter.

`SagaService._archive_worker_loop()` measures `shutil.disk_usage(settings.data_path).free` for admission. `YouTubeClient.download_video()` measures its actual working directory afterward. The log for `CYEN7z-iMVU` records admission followed by rejection: 146.7 GB available on staging versus 160.7 GB required.

The scheduler and downloader must use the same filesystem and reservation calculation.

## Configuration changes

| Setting or mount | Planned treatment |
| --- | --- |
| `/mnt/ssd0/data/saga:/data/downloads` | Retain the separate staging volume |
| `SAGA_MINIMUM_FREE_BYTES` | Retain the production value of `25000000000` |
| Worker counts | Keep one download worker while validating the new calculation |
| Staging location | Use the existing `/data/downloads` convention; no new path variable is required |

Record filesystem identity and available bytes for both mounts during deployment verification. The free-space floor applies to staging, rather than being inferred from the database volume.

## Code changes

Primary files: `core/service.py`, `core/archival.py`, `core/state.py`, and `core/youtube.py`.

1. Establish one download-root value and use it for admission, runtime guards, cleanup, and transfer metrics. Ensure it exists before querying space; report an inaccessible staging path as an actionable startup or operational error.
2. Replace the scheduler's `/data` free-space measurement with the download root's available bytes.
3. Extract one reservation calculation shared by admission and progress guards. Let `E` be the conservative expected total input-stream bytes and `P` be validated, reusable input bytes already on disk. The additional download-and-merge reservation is `max(2 * E - P, E)`, plus any explicit estimate margin chosen by the implementation.
4. Admit work only when `available >= minimum_free + other_active_reservations + this_reservation`. On-disk files already reduce `available`; do not subtract their sizes again.
5. Credit partial bytes only after the operation and selected format are identified. Arbitrary files, unreceipted final MKVs, incompatible formats, and legacy names that have not been adopted receive no resume credit.
6. Increase reservations atomically when stream sizes become known. If the larger reservation cannot fit, stop the download at a resumable boundary and record a disk-wait reason.
7. Reconcile estimated size, selected formats, existing partial sizes, and the final merge requirement before starting ffmpeg. Recheck available space because other applications share the SSD.
8. Preserve the atomic download receipt and upload-acknowledgement rules. Releasing a reservation must occur on success, failure, shutdown, and restart recovery.

Plan 04 owns partial inventory and identity validation. Until that implementation is available, use `P=0` rather than giving unverified files credit. Plan 05 will later reduce checkpoint frequency while preserving these invariants.

## Verification

Extend `tests/test_upload_state.py` and `tests/test_upload_failures.py`, plus service tests for the mount selection:

- `/data` has ample space while `/data/downloads` does not: no download is claimed.
- The inverse mount capacities allow a staging download despite lower database-volume capacity.
- A valid resumed stream receives only its remaining download-and-merge reservation.
- Two download claims cannot each reserve the same free bytes.
- An upward size estimate pauses work before the larger write/merge allocation proceeds.
- A deleted or incompatible partial receives no credit.
- An unavailable staging path produces a named error rather than an unexplained worker failure.
- Ready uploads and acknowledged-file cleanup continue while downloads are disk-blocked.

Use a separate test filesystem or mocked per-path disk statistics. Run integration cases involving external space consumption on test storage, not on the production SSD.

## Release criteria

Repository implementation: one `Settings.download_root`, a named staging startup error, shared `download_reservation`, transactional reservation increases, and a pre-merge recheck. Admission measures the download filesystem. Resume credit remains zero until plan 04 validates input identity. Production filesystem/floor measurements remain operator checks.

During a representative large-video download and merge, available staging space remains above the 25 GB floor for Saga-controlled allocations. The health record identifies the staging path, available bytes, reserved bytes, and the blocking reason. Admission and downloader preflight agree for the same sampled state.

This guard cannot reserve space against unrelated host processes. Report external depletion separately and pause Saga when the floor is threatened.

Deploy the path correction before concurrency tuning. Roll back through a compatible pinned image if needed; retain partials and current acknowledgements. Returning to the older image also returns its known mount-accounting defect.

Related plans: [partial downloads](PLAN_04_PARTIAL_DOWNLOAD_RETENTION.md), [checkpointing](PLAN_05_PROGRESS_CHECKPOINTING.md), and [concurrency](PLAN_10_UPLOAD_CONCURRENCY.md).
