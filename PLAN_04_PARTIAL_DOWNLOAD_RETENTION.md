# Plan 04: Partial-download resumption and retention

Priority: P1. Change types: application code and a new operational policy.

## Storage to recover

Five inactive partial downloads occupied 93.5 GB at the audit. Their retries were due, but their newer failure timestamps placed them behind older backfill jobs. One 25.9 GB partial used the legacy name `media.f315.webm.part`; current downloads use `merging.%(ext)s` and would not resume that file directly.

The ready-upload buffer limits complete pending files and active downloads. It does not bound accumulated failed partials. The first objective is to finish reusable work; retention handles leftovers that cannot be resumed within the storage budget.

## Configuration changes

Introduce the following settings only after parser and maintenance-worker support exists:

| Proposed setting | Initial value | Meaning |
| --- | --- | --- |
| `SAGA_PARTIAL_RETENTION_MODE` | `observe` | Inventory and report candidates; `enforce` enables eligible eviction |
| `SAGA_PARTIAL_MAX_BYTES` | `100000000000` | Initial 100 GB inactive-partial target, to be checked against staging peaks |
| `SAGA_PARTIAL_MIN_AGE` | `86400` | Minimum 24-hour inactivity before eviction eligibility |

The proposed 100 GB value is a trial policy, not measured spare capacity. The 25 GB staging floor and active download/merge reservations take precedence. If recent or protected files prevent reaching the target, pause additional downloads and report the reason instead of deleting protected work.

Document policy values in `.env.example` and README. Existing deployments should start in observation mode.

## Code changes

Implement a shared staging inventory helper, with a new `core/staging.py` if that keeps filesystem logic out of `state.py`. It must associate each directory with its persisted operation, list file roles, count allocated/logical sizes as appropriate, and tolerate files disappearing during normal cleanup.

### Resume scheduling

1. Add a due-resumable class to download scheduling. Balance it with the recent class from plan 01 and ordinary backfill using successful dispatch counts.
2. Prefer completing a compatible existing partial over starting another similar backfill download. Keep retry deadlines and recent-video service guarantees intact.
3. Supply validated reusable bytes to plan 02's admission calculation. Refresh the inventory when a job is claimed rather than trusting an old byte count.

### Legacy filenames

4. Before an unclaimed download starts, match legacy `media.f<format>.<ext>.part` files and any downloader sidecars to the current operation, video, and selected formats.
5. Adopt compatible files into the current naming convention atomically on the same filesystem. Handle sidecars as a set. If a destination exists or identity cannot be established, retain the files and report a conflict.
6. Make adoption restart-idempotent. It must never manufacture `download.json` or treat a partial as a completed MKV.

### Retention maintenance

7. Select only inactive, retryable download-stage partials meeting the policy. Exclude claimed work, ready/uploading/reconciliation workflows, completed media, and unknown directories. Files with unclear ownership require an inventory finding.
8. Acquire exclusive ownership of the transfer row before filesystem maintenance, using the same atomic exclusion that download claims obey. Persist maintenance ownership if work spans multiple transactions; restart recovery must release interrupted maintenance claims safely.
9. In observation mode, record the candidate operation, bytes, age, and reason. In enforcement mode, recheck identity and state after claiming, remove only recognized partial inputs, and leave the workflow eligible for a fresh download with an explicit eviction reason.
10. Schedule maintenance independently of uploads and acknowledged-file cleanup. Existing cleanup requires an upload receipt; partial eviction needs its own narrowly scoped operation.

## Verification

- A due resumed job receives a dispatch despite a large older backfill queue, while recent videos and backfill still receive their quotas.
- The audited legacy filename shape resumes at a nonzero byte offset without a full redownload when formats match.
- Conflicting destination names, changed formats, malformed sidecars, symlinks, and unknown operations remain unmodified and produce named findings.
- A crash between adoption steps can be retried without losing the reusable data or falsely completing a download.
- Concurrent download and eviction attempts cannot own the same operation.
- Observation mode performs no removal. Enforcement respects age, byte budget, ownership, and all protected states.
- Simulated removal failure releases or recovers maintenance ownership and does not block the upload lane.

Use fixture files and sparse test files for capacity tests. Preserve a manifest of the real audit-era partials before the first production adoption or enforcement run.

## Rollout and acceptance

Repository implementation: selected-format manifests, validated resume credit, legacy adoption, weighted download classes, minute-level inventory, and observe/enforce maintenance with transactional exclusion. One shared additive schema supplies maintenance ownership and the download/merge progress fields for plans 05 and 09. Interrupted maintenance uses the normal claim recovery. Production inventory review and enforcement acceptance remain operator checks.

Deploy resume prioritization and inventory first, after [disk accounting](PLAN_02_STAGING_DISK_ACCOUNTING.md). Run at least one full day in observation mode and record candidate decisions and peak staging occupancy. Enable enforcement only once those decisions match the intended policy and legacy adoption tests pass.

Accept when compatible partials are resumed, inactive bytes decline through completion or documented eviction, and staging pauses have an explicit explanation. The 93.5 GB audit figure is a baseline to reconcile, not a deletion target.

Rollback begins by returning retention to observation mode. Image rollback must account for adopted filenames and any maintenance-claim schema. Deleted partial inputs cannot be restored from a database rollback; those videos must redownload under their existing workflow identity.
