# Plan 10: Upload concurrency trial

Priority: P3. Primary change type: deployment configuration and measurement.

## Reason for the experiment

The audited completed uploads averaged approximately 3 to 13 MB/s each, below the configured aggregate allowance of 100 MB/s. The sampled host and Saga CPU usage did not indicate CPU saturation. A second upload worker could use otherwise idle capacity if the limit is per-connection throughput, but that gain has not been measured.

The host had no Saga-specific CPU, memory, or PID limits. Cgroup memory included a large filesystem cache, so a small Python RSS value alone is not enough to choose a hard memory limit.

## Prerequisites

Complete the acceptance checks for plans 01 through 05, 08, and 09 before this trial. Recent scheduling, correct staging reservations, resumable failures, bounded shutdown, and lane metrics are needed to interpret the results.

Use pinned images and explicit service settings from plan 07. Change concurrency separately from transport selection and bandwidth allowances.

## Configuration changes

| Setting | Baseline | Trial |
| --- | --- | --- |
| `SAGA_UPLOAD_WORKERS` | `1` | `2` |
| `SAGA_DOWNLOAD_WORKERS` | `1` | `1` |
| `SAGA_UPLOAD_READY_BUFFER` | `2` | `2` |
| `SAGA_UPLOAD_BYTES_PER_SECOND` | `100000000` | `100000000` |
| `SAGA_DOWNLOAD_BYTES_PER_SECOND` | `100000000` | `100000000` |
| `SAGA_MINIMUM_FREE_BYTES` | `25000000000` | `25000000000` |

Do not enlarge the ready buffer merely to keep two upload workers busy. If the single download worker cannot supply them, that is an experiment result. Uploading files continue occupying staging until acknowledged cleanup completes even when they no longer count as ready-buffer slots.

Measure ffmpeg/Deno process peaks and cgroup working-set/cache behavior before choosing service CPU, memory, and PID limits. Apply resource limits in a separate trial with documented headroom, so resource throttling does not confound the concurrency comparison. A hard memory cap must be tested during a large merge and two uploads.

## Code changes

Repository verification: a two-lane regression now exercises distinct simultaneous claims, one shared byte limiter, a lost acknowledgement alongside a successful upload, remote reconciliation under the original operation, and per-operation acknowledged cleanup. README records the controlled trial and rollback. The initial application worker count remains one; production throughput/host measurements and the trial decision are operator work.

No initial application code change is required: `Settings.upload_workers` and the service's independent upload lanes already exist.

Before the trial, confirm tests exercise two simultaneous uploads with distinct claims, shared aggregate bandwidth accounting, independent failure recovery, and per-operation staging cleanup. Add a regression test only for a missing concurrency behavior or a defect found by those checks.

If the second worker exposes a serialized code path, identify it from timing evidence and handle that as a separate implementation change. Avoid changing worker scheduling, retry policy, and transport in the same benchmark.

## Measurement design

1. Record a stable one-worker baseline across several large completed uploads. Prefer a full day; retain item sizes and content types so a day of Shorts is not compared with a day of livestreams.
2. Deploy the two-worker setting during a controlled restart using plan 08. Start the observation window after startup/recovery work has settled.
3. Measure acknowledged payload bytes per wall-clock hour, individual upload duration, upload-worker occupancy, download supply, staging peaks, disk-floor pauses, retry/recovery count, and upstream rate-limit responses.
4. Record host network utilization, Saga CPU, I/O latency, cgroup memory/OOM events, and effects on other workloads. Container network counters include downloads and protocol overhead; report acknowledgements are the payload-throughput reference.
5. Compare multiple representative completions. If network or upstream conditions differ enough to dominate the comparison, extend the observation or repeat the baseline rather than claiming a gain from one fast item.

## Decision criteria

The proposed retention threshold is at least a 20% gain in aggregate acknowledged payload per active wall-clock hour, without more transfer failures, disk-floor violations, or host contention. This is an acceptance target, not an expected result.

Keep two workers if the measured benefit meets that target and recovery/shutdown behavior remains correct. Return to one worker if the download lane cannot supply the second worker, upstream throttling increases, or contention consumes the gain. Document a neutral result.

## Rollback

Set `SAGA_UPLOAD_WORKERS=1` and redeploy the same pinned image using the bounded shutdown procedure. Reconcile interrupted uploads before replacement transfers. No database rollback or staging deletion is required.

After the trial, record the selected worker count, measurement window, payload totals, failure counts, peak staging usage, and any resource-limit values in the deployment record.
