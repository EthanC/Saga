"""Online SQLite snapshots and their serial cron scheduler."""

from __future__ import annotations

import asyncio
import re
import sqlite3
from contextlib import closing, suppress
from datetime import UTC, datetime, tzinfo
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, TypedDict

from cronsim import CronSim
from loguru import logger

from core.shutdown import CooperativeStop, StopController

if TYPE_CHECKING:
    from core.config import BackupSettings

_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S.%fZ"


class _BackupOptions(TypedDict, total=False):
    stop_controller: StopController


def next_backup_run(schedule: str, timezone: tzinfo, now: datetime) -> datetime:
    """Return the next cron occurrence after ``now`` in the configured timezone."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    occurrences = CronSim(schedule, now.astimezone(timezone))
    now_utc = now.astimezone(UTC)
    scheduled = next(occurrences)
    # Fixed-time jobs can resolve to the earlier fold during DST fallback.
    while scheduled.astimezone(UTC) <= now_utc:
        scheduled = next(occurrences)
    return scheduled


async def create_backup(
    database: Path,
    directory: Path,
    retention_count: int,
    *,
    stop_controller: StopController | None = None,
) -> Path:
    """Create, verify, publish, and retain an online SQLite snapshot."""
    options: _BackupOptions = (
        {"stop_controller": stop_controller} if stop_controller else {}
    )
    worker = asyncio.create_task(
        asyncio.to_thread(
            _create_backup_sync, database, directory, retention_count, **options
        )
    )
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        with suppress(Exception):
            await worker
        raise


def _create_backup_sync(
    database: Path,
    directory: Path,
    retention_count: int,
    *,
    timestamp: datetime | None = None,
    stop_controller: StopController | None = None,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        prefix=f".{database.name}.", suffix=".tmp", dir=directory, delete=False
    ) as temporary_file:
        temporary = Path(temporary_file.name)
    timestamp = timestamp or datetime.now(UTC)
    final = directory / (
        f"{database.name}-{timestamp.astimezone(UTC).strftime(_TIMESTAMP_FORMAT)}"
        ".backup.sqlite3"
    )
    source_uri = f"{database.resolve().as_uri()}?mode=ro"
    try:
        with (
            closing(sqlite3.connect(source_uri, uri=True)) as source,
            closing(sqlite3.connect(temporary)) as destination,
        ):
            if stop_controller:
                source.backup(
                    destination,
                    pages=256,
                    progress=lambda *args: stop_controller.check(),
                )
            else:
                source.backup(destination)
            result = destination.execute("PRAGMA quick_check").fetchone()
            if result != ("ok",):
                raise sqlite3.DatabaseError(f"backup quick check failed: {result!r}")
        temporary.replace(final)
        apply_retention(database, directory, retention_count)
        return final
    finally:
        temporary.unlink(missing_ok=True)


def apply_retention(database: Path, directory: Path, retention_count: int) -> None:
    """Keep only the newest matching snapshots, without following symlinks."""
    if retention_count == 0:
        return
    pattern = re.compile(
        rf"^{re.escape(database.name)}-\d{{8}}T\d{{6}}\.\d{{6}}Z\.backup\.sqlite3$"
    )
    snapshots = sorted(
        (
            path
            for path in directory.iterdir()
            if pattern.fullmatch(path.name) and path.is_file() and not path.is_symlink()
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    for snapshot in snapshots[retention_count:]:
        snapshot.unlink()


async def run_backup_scheduler(
    database: Path,
    settings: BackupSettings,
    stop: asyncio.Event,
    *,
    stop_controller: StopController | None = None,
) -> None:
    """Run successful backups serially and isolate per-run operational failures."""
    while not stop.is_set():
        now = datetime.now(UTC)
        scheduled = next_backup_run(settings.schedule, settings.timezone, now)
        logger.info("Next database backup scheduled for {}", scheduled.isoformat())
        await _wait(stop, max(0.0, (scheduled.astimezone(UTC) - now).total_seconds()))
        if stop.is_set():
            return
        try:
            snapshot = await create_backup(
                database,
                settings.directory,
                settings.retention_count,
                **({"stop_controller": stop_controller} if stop_controller else {}),
            )
        except CooperativeStop:
            return
        except Exception:
            logger.exception("Database backup failed")
        else:
            logger.info("Database backup completed: {}", snapshot)


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
