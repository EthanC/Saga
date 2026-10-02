# Backup and recovery

## Recovery copy

First check whether the host's existing backup tool already covers Saga. Retrieve and verify a copy before changing the backup schedule. Record the destination, retention, last successful copy, alert owner, and where restore keys can be obtained after host loss.

Back up these inputs together:

- Published `<database-name>-YYYYMMDDTHHMMSS.ffffffZ.backup.sqlite3` snapshots from the configured backup directory (normally `saga.sqlite-*.backup.sqlite3`). Ignore `.tmp` files and do not copy the live SQLite file without its WAL.
- Private Compose configuration, `.env.saga`, `config.toml`, and required cookies.
- A deployment manifest with both image digests, all bind mounts (including `/data/downloads`), snapshot creation time, and queue totals.

Use the selected backup tool's encryption and access controls. The destination must survive loss of the Saga host. Ordinary temporary downloads can be excluded; losing them requires redownloading or reconciling remote uploads using preserved operation IDs and receipts.

Track snapshot creation time and successful copy time separately. A successful copy of an old snapshot is still an old recovery point. Retry failed copies and alert before uncopied snapshots fall out of local retention.

Keep the existing daily schedule and seven snapshots while verifying restoration. If you adopt the proposed one-hour lost-state target with a 15-minute copy allowance, use:

```dotenv
SAGA_BACKUP_SCHEDULE=0,30 * * * *
SAGA_BACKUP_RETENTION_COUNT=336
SAGA_BACKUP_TIMEZONE=America/Chicago
```

This keeps seven days of half-hourly snapshots. Size the local and remote storage from current database size; configure remote retention in the backup tool. The proposed restore-time target is four hours.

## Restore rehearsal

1. Retrieve a published snapshot and its deployment manifest into isolated storage. Record the snapshot age and start time. Block external writes or use fake clients so notifications, reports, and archive mutations cannot reach production.
2. Check `PRAGMA integrity_check` and `PRAGMA foreign_key_check` with SQLite. Rehearse the intended version twice using:

   ```console
   uv run python -m core.migration /test/saga.sqlite3 --directory /test/rehearsal
   ```

   The command creates a consistent copy, applies migrations twice, and checks integrity and foreign keys. Also rehearse the recorded rollback image if relying on schema compatibility.
3. Restore config/data/staging bind mappings in test storage. Run one Saga writer. Compare channel/subscription counts, queue stages, frozen operation metadata, upload receipts, report checkpoints, and background jobs with the manifest.
4. Exercise missing staging, interrupted downloads, uncertain uploads, acknowledged uploads awaiting cleanup, and pending reports. Missing media must return to download; uncertain remote mutations must reconcile before another PUT; acknowledged transfers must keep their completion count.
5. Record elapsed restore time, measured lost-state window, unavailable credentials/tools/images, last rehearsal date, and the next rehearsal trigger. Repeat after schema or runtime changes.

## Restoring an older database

Stop the current writer and retain a verified copy of its state and staging before restoring. An older snapshot can lose acknowledgements and Discord delivery facts for remote effects after its creation. Build a list of affected operation IDs, item IDs, filenames, and notification/report windows from retained state and logs.

Before enabling external writes, compare each uncertain archive original with saved source, operation ID, filename, size, and available checksums. Route uncertain transfers through `reconcile_upload`; do not declare missing from an incomplete remote listing. Keep conflicts for operator review. Check post-snapshot Discord deliveries and reports to decide whether replay is acceptable. Their delivery is at least once when a response was lost, so restoration can resend a message.

An ordinary compatible image rollback uses current durable state. Restoring a database is an incident-recovery action, not an image rollback step. Failed copies must retain the last verified off-host recovery copy.
