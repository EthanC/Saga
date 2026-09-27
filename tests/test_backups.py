from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from core import backups
from core.config import BackupSettings


def backup_settings(tmp_path, **changes):
    values = {
        "enabled": True,
        "schedule": "0 7 * * *",
        "directory": tmp_path / "backups",
        "retention_count": 7,
        "timezone": ZoneInfo("UTC"),
    }
    values.update(changes)
    return BackupSettings(**values)


def test_create_backup_captures_committed_wal_and_is_standalone(tmp_path):
    database = tmp_path / "saga.sqlite"
    directory = tmp_path / "backups"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("CREATE INDEX records_value ON records (value)")
    connection.execute("INSERT INTO records (value) VALUES ('committed')")
    connection.commit()
    connection.execute("INSERT INTO records (value) VALUES ('uncommitted')")
    assert database.with_name(f"{database.name}-wal").exists()

    snapshot = asyncio.run(backups.create_backup(database, directory, 7))

    with closing(sqlite3.connect(snapshot)) as restored:
        assert restored.execute("SELECT value FROM records").fetchall() == [
            ("committed",)
        ]
        assert restored.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("records_value",),
        ).fetchone() == ("records_value",)
        assert restored.execute("PRAGMA quick_check").fetchone() == ("ok",)
    assert not snapshot.with_name(f"{snapshot.name}-wal").exists()
    assert not snapshot.with_name(f"{snapshot.name}-shm").exists()
    connection.close()


def test_create_backup_opens_source_read_only(monkeypatch, tmp_path):
    database = tmp_path / "source.sqlite"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.commit()
    real_connect = sqlite3.connect
    calls = []

    def recording_connect(target, *args, **kwargs):
        calls.append((target, kwargs.copy()))
        return real_connect(target, *args, **kwargs)

    monkeypatch.setattr(backups.sqlite3, "connect", recording_connect)
    asyncio.run(backups.create_backup(database, tmp_path / "backups", 7))

    source, options = calls[0]
    assert source == f"{database.resolve().as_uri()}?mode=ro"
    assert options == {"uri": True}


def test_sync_backup_uses_utc_timestamp_and_applies_retention(monkeypatch, tmp_path):
    database = tmp_path / "saga.sqlite"
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("CREATE TABLE example (value INTEGER)")
        connection.commit()
    retain = MagicMock()
    monkeypatch.setattr(backups, "apply_retention", retain)

    snapshot = backups._create_backup_sync(
        database,
        tmp_path / "backups",
        3,
        timestamp=datetime(2026, 1, 2, 4, 5, 6, 7, tzinfo=ZoneInfo("Europe/Paris")),
    )

    assert snapshot.name == "saga.sqlite-20260102T030506.000007Z.backup.sqlite3"
    assert snapshot.is_file()
    assert not list(snapshot.parent.glob("*.tmp"))
    retain.assert_called_once_with(database, snapshot.parent, 3)


def test_backup_failure_cleans_temporary_file_without_retention(monkeypatch, tmp_path):
    retain = MagicMock()
    monkeypatch.setattr(backups, "apply_retention", retain)
    directory = tmp_path / "backups"

    with pytest.raises(sqlite3.OperationalError):
        backups._create_backup_sync(tmp_path / "missing.sqlite", directory, 2)

    assert list(directory.iterdir()) == []
    retain.assert_not_called()


def test_failed_quick_check_closes_connections_and_does_not_publish(
    monkeypatch, tmp_path
):
    connections = []

    class FakeConnection:
        def __init__(self, *, check=None):
            self.check = check
            self.closed = False

        def backup(self, destination):
            assert destination is connections[1]

        def execute(self, statement):
            assert statement == "PRAGMA quick_check"
            return MagicMock(fetchone=MagicMock(return_value=self.check))

        def close(self):
            self.closed = True

    def connect(*_args, **_kwargs):
        connection = FakeConnection(check=None if not connections else ("corrupt",))
        connections.append(connection)
        return connection

    retain = MagicMock()
    monkeypatch.setattr(backups.sqlite3, "connect", connect)
    monkeypatch.setattr(backups, "apply_retention", retain)
    directory = tmp_path / "backups"

    with pytest.raises(sqlite3.DatabaseError, match="quick check failed"):
        backups._create_backup_sync(tmp_path / "source.sqlite", directory, 7)

    assert all(connection.closed for connection in connections)
    assert list(directory.iterdir()) == []
    retain.assert_not_called()


def test_retention_keeps_newest_matching_regular_files(tmp_path):
    database = tmp_path / "saga.sqlite"
    directory = tmp_path / "backups"
    directory.mkdir()
    names = [
        f"saga.sqlite-2026010{day}T070000.000000Z.backup.sqlite3" for day in range(1, 6)
    ]
    for name in names:
        (directory / name).write_text(name, encoding="utf-8")
    unrelated = [
        directory / "other.sqlite-20260105T070000.000000Z.backup.sqlite3",
        directory / "saga.sqlite-invalid.backup.sqlite3",
        directory / ".saga.sqlite-temporary.tmp",
    ]
    for path in unrelated:
        path.write_text("keep", encoding="utf-8")
    matching_directory = (
        directory / "saga.sqlite-20260106T070000.000000Z.backup.sqlite3"
    )
    matching_directory.mkdir()

    backups.apply_retention(database, directory, 2)

    assert sorted(path.name for path in directory.iterdir()) == sorted(
        [*names[-2:], *(path.name for path in unrelated), matching_directory.name]
    )


def test_retention_ignores_symlinks(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    target = directory / "target"
    target.write_text("keep", encoding="utf-8")
    link = directory / "saga.sqlite-20260101T070000.000000Z.backup.sqlite3"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("creating symlinks is not permitted")

    backups.apply_retention(tmp_path / "saga.sqlite", directory, 1)

    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == "keep"


def test_zero_retention_keeps_all_without_reading_directory(tmp_path):
    backups.apply_retention(tmp_path / "saga.sqlite", tmp_path / "missing", 0)


@pytest.mark.parametrize(
    ("now", "expected", "utc_hours"),
    [
        (
            datetime(2026, 3, 7, 7, tzinfo=ZoneInfo("America/New_York")),
            datetime(2026, 3, 8, 7, tzinfo=ZoneInfo("America/New_York")),
            23,
        ),
        (
            datetime(2026, 10, 31, 7, tzinfo=ZoneInfo("America/New_York")),
            datetime(2026, 11, 1, 7, tzinfo=ZoneInfo("America/New_York")),
            25,
        ),
    ],
)
def test_next_backup_run_keeps_local_time_across_dst(now, expected, utc_hours):
    result = backups.next_backup_run("0 7 * * *", now.tzinfo, now)

    assert result == expected
    assert (result.astimezone(UTC) - now.astimezone(UTC)) == timedelta(hours=utc_hours)


def test_next_backup_run_is_strictly_after_boundary_and_requires_aware_time():
    timezone = ZoneInfo("UTC")
    now = datetime(2026, 1, 1, 7, tzinfo=timezone)

    assert backups.next_backup_run("0 7 * * *", timezone, now) == datetime(
        2026, 1, 2, 7, tzinfo=timezone
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        backups.next_backup_run("0 7 * * *", timezone, now.replace(tzinfo=None))


@pytest.mark.parametrize("now_timezone", [UTC, ZoneInfo("America/New_York")])
@pytest.mark.parametrize(
    ("schedule", "now", "expected"),
    [
        ("30 1 * * *", "2026-11-01T01:00:00-05:00", "2026-11-02T01:30:00-05:00"),
        ("30 * * * *", "2026-11-01T01:45:00-04:00", "2026-11-01T01:30:00-05:00"),
    ],
)
def test_next_backup_run_is_future_during_repeated_dst_hour(
    schedule, now, expected, now_timezone
):
    timezone = ZoneInfo("America/New_York")
    now = datetime.fromisoformat(now).astimezone(now_timezone)

    result = backups.next_backup_run(schedule, timezone, now)

    assert result.isoformat() == expected
    assert result.astimezone(UTC) > now.astimezone(UTC)


def test_create_backup_waits_for_worker_cleanup_when_cancelled(monkeypatch, tmp_path):
    started = threading.Event()
    release = threading.Event()

    def blocking_backup(*_args):
        started.set()
        assert release.wait(timeout=2)
        return tmp_path / "snapshot.sqlite"

    async def scenario():
        monkeypatch.setattr(backups, "_create_backup_sync", blocking_backup)
        task = asyncio.create_task(
            backups.create_backup(tmp_path / "source.sqlite", tmp_path / "backups", 7)
        )
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_scheduler_waits_until_next_run_and_stops(monkeypatch, tmp_path):
    stop = asyncio.Event()
    create = AsyncMock()
    delays = []

    def next_run(_schedule, _timezone, now):
        return now + timedelta(hours=1)

    async def wait(event, seconds):
        delays.append(seconds)
        event.set()

    monkeypatch.setattr(backups, "next_backup_run", next_run)
    monkeypatch.setattr(backups, "_wait", wait)
    monkeypatch.setattr(backups, "create_backup", create)

    asyncio.run(
        backups.run_backup_scheduler(
            tmp_path / "saga.sqlite", backup_settings(tmp_path), stop
        )
    )

    assert delays == [pytest.approx(3600)]
    create.assert_not_awaited()


def test_scheduler_recovers_after_failure_and_recalculates(monkeypatch, tmp_path):
    stop = asyncio.Event()
    attempts = 0

    def next_run(_schedule, _timezone, now):
        return now

    async def create(*_args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disk unavailable")
        stop.set()
        return tmp_path / "snapshot.sqlite"

    log_exception = MagicMock()
    log_info = MagicMock()
    next_backup = MagicMock(side_effect=next_run)
    monkeypatch.setattr(backups, "next_backup_run", next_backup)
    monkeypatch.setattr(backups, "_wait", AsyncMock())
    monkeypatch.setattr(backups, "create_backup", create)
    monkeypatch.setattr(backups.logger, "exception", log_exception)
    monkeypatch.setattr(backups.logger, "info", log_info)

    asyncio.run(
        backups.run_backup_scheduler(
            tmp_path / "saga.sqlite", backup_settings(tmp_path), stop
        )
    )

    assert attempts == 2
    assert next_backup.call_count == 2
    log_exception.assert_called_once_with("Database backup failed")
    assert log_info.call_count == 3


def test_wait_returns_for_stop_and_timeout():
    stop = asyncio.Event()
    stop.set()
    asyncio.run(backups._wait(stop, 10))
    asyncio.run(backups._wait(asyncio.Event(), 0))
