# Plan 11: Off-host backup and recovery verification

Priority: P2. Primary change type: operational configuration.

## Established facts and remaining question

Saga's live SQLite `quick_check` passed and no foreign-key violations were found. Four daily snapshots existed, and the newest snapshot also passed `quick_check`. The application stores those snapshots on the same filesystem as its primary database.

The audit did not establish whether another host backup system already copies them elsewhere. First verify that coverage. Avoid creating a second competing backup system if the existing one can meet the recovery requirements.

## Configuration changes

### Establish off-host coverage

1. Inventory existing backup jobs, destination, retention, last successful run, and restore credentials for the Saga project. Verify coverage by inspecting a restorable copy, not only by finding a configured path.
2. Include completed SQLite snapshots, Compose configuration, the service-scoped environment file, `config.toml`, required cookies, and a deployment manifest identifying image digests and bind mounts.
3. Exclude ordinary temporary downloads from the routine database/configuration backup. Treat acknowledged receipts and operation IDs as durable recovery facts. Document that lost staging can require redownload or remote reconciliation.
4. Copy only published snapshots matching the backup filename pattern. Ignore `.tmp` files. Use the existing backup tool's encryption and access controls for credentials contained in environment, cookie, configuration, and database files.
5. Store the recovery copy on a different failure domain from the primary host/filesystem. Record the actual destination and how to retrieve its keys after host loss.
6. Retry failed copies and expose last successful snapshot time, copy time, and age. Copying an old snapshot successfully must not make backup freshness appear current.

### Set cadence after the restore path works

Initially retain the working 06:30 America/Chicago daily schedule and seven local snapshots while validating replication. Proposed follow-up objectives are a maximum lost-state window of one hour (recovery point objective, RPO) and a restore time of four hours (recovery time objective, RTO). Confirm these operational targets before changing retention.

To meet the proposed one-hour RPO with a 15-minute copy allowance, use the existing scheduler's half-hourly cadence:

```dotenv
SAGA_BACKUP_SCHEDULE=0,30 * * * *
SAGA_BACKUP_RETENTION_COUNT=336
```

That retains seven days of half-hourly snapshots. At the audited database size of approximately 5.3 MB, the raw local snapshot budget would be about 1.8 GB. Recalculate as the database grows. Set remote retention through the backup tool and verify failed replication is alerted before unreplicated local snapshots age out.

The half-hourly values are proposed configuration, not settings currently in production.

## Code and repository changes

No application change is required for the first off-host-copy implementation or the proposed cron cadence. `core/backups.py` already creates online snapshots, verifies them, and publishes them atomically.

- Document the backup inputs, destination, cadence, retention, freshness alert, and restore procedure in the operational README or a dedicated recovery document.
- If reusable automation belongs in the repository, add a small wrapper around the selected backup tool with retry status and file selection. Keep destination credentials external.
- Add an application export manifest or completion hook only if the selected automation cannot consume the existing atomic snapshot filenames reliably. Test that interface's failure behavior if implemented.
- Ensure future schema migrations preserve a verified pre-migration snapshot and can be rehearsed through `core.migration`.

## Restore rehearsal

1. Retrieve a completed snapshot and matching deployment configuration into isolated test storage. Record its age and the elapsed restore time.
2. Verify SQLite integrity and foreign keys, then run the intended image's idempotent migration rehearsal on the copy. Also test the documented rollback image where schema compatibility is claimed.
3. Restore the bind-path mapping, including the separate staging root. Use fake external clients or blocked outbound access for the rehearsal so restored notification/report rows cannot send production messages.
4. Check channels, subscriptions, queue counts, frozen operation metadata, acknowledgements, report checkpoints, and outstanding background jobs against the backup manifest.
5. Exercise missing staging, interrupted download, uncertain upload, acknowledged-but-not-cleaned upload, and pending-report cases. A missing staging file must not be mistaken for a completed transfer.
6. For an actual recovery from an older snapshot, reconcile operations whose remote effects could have occurred after that snapshot. Restoring an old database can lose upload receipts and Discord delivery facts; define how those outcomes are checked before enabling external writes.
7. Record the measured RPO/RTO and any steps requiring a credential, image, or tool unavailable on a replacement host.

## Acceptance

Repository implementation: `RECOVERY.md` documents backup inputs, snapshot selection, freshness, proposed cadence, isolated restoration, and post-snapshot remote reconciliation. Existing `core.backups` and `core.migration` remain the supported interfaces. Verification: reviewed the procedures against those interfaces. Off-host destination, encryption keys, copy jobs, measured recovery times, and production acceptance are operator work.

A recovery copy exists outside the primary failure domain and has been restored successfully. Its credentials are retrievable, the restored state passes database checks, and a replacement instance can recover interrupted work under the documented procedure.

If the proposed one-hour RPO is adopted, completed remote snapshots remain within that age objective under normal operation and overdue copies trigger an actionable alert. The measured restore completes within the agreed RTO.

## Rollback and ownership

If increased backup frequency affects transfer latency or storage, return to the previous daily schedule while retaining established off-host coverage. Failed remote copies must not delete the last verified recovery copy. Database restoration is an incident-recovery operation, not the default rollback for an image deployment.

Record who owns backup alerts and restoration, the destination identifier, key-recovery location, last rehearsal date, and next rehearsal trigger. Rehearse again after a persistence migration or a change to image/runtime dependencies that affects opening the database.

Related plans: [deployment configuration](PLAN_07_DEPLOYMENT_CONFIGURATION.md), [shutdown](PLAN_08_GRACEFUL_SHUTDOWN.md), and the [roadmap's release rules](PLAN_00_AUDIT_ROADMAP.md).
