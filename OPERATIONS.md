# Production configuration handoff

Repository implementation and local checks are complete. Apply these settings with the tested new image and record the production checks in the roadmap's completion record.

## Deployment and initial settings

1. Save a verified SQLite snapshot and a private copy of the current deployment files. Record the current Saga/provider image digests and queue totals.
2. Put Saga's settings and secrets in `.env.saga`, referenced by Saga's `env_file`. Remove Saga credentials and webhooks from Arcane's project-wide environment. Recreate the provider and verify that it receives only its own settings. Keep the environment file private; a rendered Compose file also contains expanded secrets.
3. Pin both images to tested digests. The new Saga digest must come from the build containing these changes. The roadmap's audited Saga digest is a rollback reference, not the new release.
4. Preserve the existing config and database mounts and the SSD mount at `/data/downloads`. Keep the internal paths the same when restoring data.

Use these initial Saga values:

```dotenv
LOG_LEVEL=INFO
LOG_DISCORD_LEVEL=WARNING
SAGA_DOWNLOAD_WORKERS=1
SAGA_UPLOAD_WORKERS=1
SAGA_UPLOAD_READY_BUFFER=2
SAGA_MINIMUM_FREE_BYTES=25000000000
SAGA_DOWNLOAD_BYTES_PER_SECOND=100000000
SAGA_UPLOAD_BYTES_PER_SECOND=100000000
SAGA_TRANSFER_STALL_TIMEOUT=900
SAGA_PARTIAL_RETENTION_MODE=observe
SAGA_PARTIAL_MAX_BYTES=100000000000
SAGA_PARTIAL_MIN_AGE=86400
SAGA_SHUTDOWN_TIMEOUT=90
```

The free-space floor is 25 GB. The partial-input target is 100 GB, with a minimum inactivity age of 24 hours. Observation mode reports candidates without deleting inputs; compatible legacy filenames can still be adopted for resumption.

Add to Saga's Compose service:

```yaml
stop_grace_period: 120s
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

Give the provider the same logging limits. Recreate both containers, then inspect their effective image IDs, environment key names, mounts, and `HostConfig.LogConfig`. Confirm Arcane and the Compose CLI produce the same settings. Keep the provider private and retain its health-gated dependency when cookies are configured.

Provider 2.0.0 has no exposed quiet switch and prints generated tokens. Removing that output requires a separately reviewed provider update. Saga's INFO setting affects Saga, not the provider's stdout.

## Checks after deployment

- Recent videos and Shorts should advance at metadata even with the old download backlog. Inspect `transfers.json` for the oldest recent item and its waiting reason.
- Confirm that admission and merging use the SSD's available space and retain the 25 GB floor. Uploads and acknowledged cleanup should continue during disk waits.
- Review partial inventory and adoption findings for a full day. Enable `SAGA_PARTIAL_RETENTION_MODE=enforce` only after those decisions match the intended policy and staging peaks support the 100 GB target.
- Check one intentional stop: Saga should print its saved operations and stop before Compose's deadline. Interrupted uploads should reconcile before another PUT.
- Keep health handling alert-only initially. Disk/buffer/retry waits are backpressure; an unhealthy container does not restart automatically under `unless-stopped`.

Health publishes every 30 seconds; partial inventory refreshes once a minute. Routine byte checkpoints can lag by five seconds. The initial internal alert policy is 120 seconds of startup grace, five minutes for recent metadata waiting, and 15 minutes for recent download waiting. No new environment settings are needed for these alert ages, checkpoint frequency, or media retry counts.

For a transport comparison, build and record the plan 03 revision separately and keep worker counts, rates, and content selection fixed. Committing a revision does not publish an image for it.

## Backup and later trials

Verify the existing off-host backup tool's coverage and restore a copy using [RECOVERY.md](RECOVERY.md). Include published SQLite snapshots, Compose, `.env.saga`, config, cookies, image digests, and bind mappings. Preserve the daily 06:30 America/Chicago schedule and seven local snapshots until the off-host restore path works.

If you adopt the proposed one-hour lost-state target with a 15-minute copy allowance, change to:

```dotenv
SAGA_BACKUP_SCHEDULE=0,30 * * * *
SAGA_BACKUP_RETENTION_COUNT=336
SAGA_BACKUP_TIMEZONE=America/Chicago
```

Set remote retention and copy-failure alerts in the backup tool. Measure restoration against the proposed four-hour target.

After the earlier production checks pass, trial `SAGA_UPLOAD_WORKERS=2` on the same pinned image. Keep the download worker, buffer, rates, and floor above. Keep two upload workers only if acknowledged throughput improves by at least the proposed 20% without more failures or disk-floor violations. Measure CPU, memory/cache, and process peaks before a separate resource-limit trial.
