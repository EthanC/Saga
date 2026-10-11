import runpy
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlmodel import Session, select

from core import state as state_module
from core.archival import ArchiveProcessor, Inspection
from core.migration import rehearse_migration
from core.state import (
    ArchiveJob,
    ArchiveProgress,
    Report,
    SchemaMigration,
    StateStore,
    Video,
    utcnow,
)
from core.youtube import DiskSpaceError, YouTubeClient
from tests.test_archival import FakeGateway, run_job, saved, seed_upload, settings


@pytest.mark.parametrize("evidence", ["verified", "missing", "pending"])
def test_restart_reactivates_completed_recovery_after_new_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, evidence: str
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    video_id, media, _ = seed_upload(store, tmp_path)
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    store.defer_transfer(
        work, status="reconcile_upload", error="lost", delay=0, deadline=10
    )
    gateway = FakeGateway()
    gateway.inspection = Inspection("missing")
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    run_job(store, processor, "recovery", max_attempts=1)
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "passed" and job.attempts == 1
    retry = store.claim_archive(lane="upload", prefer_recent=False)
    assert retry is not None and retry.progress.upload_mode == "recover"
    store.update_archive(video_id, status="uploading", upload_started_at=utcnow())
    store.close()

    # The old budget and deadline are both exhausted when the process restarts.
    now = utcnow() + timedelta(days=1)
    monkeypatch.setattr(state_module, "utcnow", lambda: now)
    store = StateStore(path)
    store.reset_interrupted_claims(background_deadline=120)
    assert saved(store, video_id).status == "reconcile_upload"
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "pending" and job.attempts == 0
        assert job.claim_token is None and job.claimed_at is None
        assert job.deadline_at == now + timedelta(seconds=120)
    gateway.inspection = Inspection(evidence)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    run_job(store, processor, "recovery", max_attempts=1)
    assert gateway.upload_calls == 0
    if evidence == "verified":
        assert saved(store, video_id).media_uploaded_at
        run_job(store, processor, "cleanup")
        assert not media.exists()
    elif evidence == "missing":
        assert store.claim_archive(lane="upload", prefer_recent=False) is not None
    else:
        # Required recovery continues beyond the old attempt budget and deadline.
        store.reset_interrupted_claims(background_deadline=120)
        assert (
            store.claim_background(
                "recovery", now=now + timedelta(seconds=10), max_attempts=1
            )
            is not None
        )
        with Session(store.engine) as session:
            job = session.get(ArchiveJob, (video_id, "recovery"))
            assert job is not None and job.deadline_at is None and job.attempts == 2
    store.close()


def test_disk_reservations_and_buffer_pause_only_downloads(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    first_id, _, _ = seed_upload(store, tmp_path)
    second_id, _, _ = seed_upload(store, tmp_path, "SecondVid12")
    store.update_archive(first_id, status="download")
    store.update_archive(second_id, status="download")
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=7) is None
    )
    assert store.download_pause_reason == "disk budget full"
    first = store.claim_archive(lane="download", prefer_recent=False, free_bytes=15)
    assert first is not None
    assert first.progress.reserved_bytes == 8
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=15) is None
    )
    store.guard_download_space(first_id, expected=4, downloaded=4, free=10, minimum=1)
    with pytest.raises(DiskSpaceError):
        store.guard_download_space(
            first_id, expected=100, downloaded=4, free=10, minimum=1
        )
    store.update_archive(first_id, status="upload", release=True)
    store.update_archive(second_id, status="upload", release=True)
    third_id, _, _ = seed_upload(store, tmp_path, "ThirdVideo1")
    store.update_archive(third_id, status="download")
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=1000)
        is None
    )
    assert store.download_pause_reason == "upload-ready buffer full"
    assert store.claim_archive(lane="upload", prefer_recent=False) is not None
    store.update_archive(first_id, status="reconcile_upload", release=True)
    store.update_archive(second_id, status="reconcile_upload", release=True)
    # Retained ambiguous transfers consume disk, not slots in the ready buffer.
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=1000)
        is not None
    )
    store.close()


def test_claims_are_atomic_across_connections_and_job_kinds(tmp_path: Path) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    video_id, _, _ = seed_upload(store, tmp_path)
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="validation"))
        session.commit()
    other = StateStore(path)
    with ThreadPoolExecutor(2) as workers:
        claims = list(
            workers.map(
                lambda state: state.claim_archive(lane="upload", prefer_recent=False),
                (store, other),
            )
        )
    assert sum(work is not None for work in claims) == 1
    with ThreadPoolExecutor(2) as workers:
        jobs = list(
            workers.map(
                lambda state: state.claim_background("validation"), (store, other)
            )
        )
    assert sum(work is not None for work in jobs) == 1
    job = next(work for work in jobs if work is not None)
    store.reset_interrupted_claims()
    replacement = store.claim_background("validation")
    assert replacement is not None
    with pytest.raises(RuntimeError, match="claim"):
        store.update_job(job, status="passed")
    other.close()
    store.close()


def test_metadata_supply_is_bounded_but_ready_uploads_remain_claimable(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    for index in range(4):
        download_id, _, _ = seed_upload(store, tmp_path, f"{index:011d}")
        store.update_archive(download_id, status="download")
    metadata_id, _, _ = seed_upload(store, tmp_path, "Metadata123")
    store.update_archive(metadata_id, status="metadata")
    assert store.claim_archive(lane="metadata", prefer_recent=False, buffer=2) is None
    upload = store.claim_archive(lane="upload", prefer_recent=False)
    assert upload is not None and upload.video.id == video_id
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=1) is None
    )
    # Files too large for the disk budget must not block metadata for later videos.
    metadata = store.claim_archive(lane="metadata", prefer_recent=False, buffer=2)
    assert metadata is not None and metadata.video.id == metadata_id
    store.close()


def test_recent_metadata_has_independent_atomic_allowance(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    with Session(store.engine) as session:
        video = session.get(Video, video_id)
        assert video is not None
        channel_id = video.channel_id
        for index in range(1400):
            identifier = f"b{index:010d}"
            session.add(Video(id=identifier, channel_id=channel_id, webpage_url="test"))
            session.flush()
            session.add(ArchiveProgress(video_id=identifier, status="download"))
        for index, content_type in enumerate(("video", "short", "video")):
            identifier = f"r{index:010d}"
            session.add(
                Video(
                    id=identifier,
                    channel_id=channel_id,
                    webpage_url="test",
                    notification_candidate=True,
                    content_type=content_type,
                )
            )
            session.flush()
            session.add(ArchiveProgress(video_id=identifier))
        session.commit()
    other = StateStore(store.path)
    with ThreadPoolExecutor(2) as workers:
        claims = list(
            workers.map(
                lambda state: state.claim_archive(lane="metadata", prefer_recent=True),
                (store, other),
            )
        )
    assert {claim.video.content_type for claim in claims if claim} == {"video", "short"}
    assert store.claim_archive(lane="metadata", prefer_recent=True) is None
    assert store.metadata_pause_reasons == {
        "recent": "recent metadata supply full",
        "backfill": "backfill metadata supply full",
    }
    first = next(claim for claim in claims if claim)
    store.update_archive(
        first.video.id, status="download", release=True, retry_after=60
    )
    assert store.claim_archive(lane="metadata", prefer_recent=True) is not None
    other.close()
    store.close()


def test_recent_disk_wait_does_not_block_metadata_and_backfill_gets_turn(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    recent, _, _ = seed_upload(store, tmp_path)
    backfill, _, _ = seed_upload(store, tmp_path, "Backfill123")
    with Session(store.engine) as session:
        video = session.get(Video, recent)
        assert video is not None
        video.notification_candidate = True
        session.add(video)
        session.commit()
    store.update_archive(recent, status="download", failure_phase="disk")
    store.update_archive(backfill, status="metadata")
    assert store.claim_archive(lane="metadata", prefer_recent=True, buffer=1)
    store.update_archive(backfill, status="download", release=True)
    claim = store.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
    assert claim is not None and claim.video.id == backfill
    store.close()


def test_transfer_health_detects_idle_and_stalled_uploads(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    now = utcnow()
    store.update_archive(video_id, ready_at=now - timedelta(hours=1))
    assert store.transfer_health(stall_timeout=30, now=now)["healthy"] is True
    assert (
        store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=151))[
            "healthy"
        ]
        is False
    )
    store.update_archive(video_id, release=True, retry_after=60)
    health = store.transfer_health(stall_timeout=30)
    assert health["healthy"] and health["waiting_reason"] == "retry backoff"
    store.update_archive(
        video_id, status="uploading", upload_started_at=now - timedelta(hours=1)
    )
    assert not store.transfer_health(stall_timeout=30)["healthy"]
    store.update_archive(video_id, transfer_progress_at=utcnow(), transfer_bytes=2)
    health = store.transfer_health(stall_timeout=30)
    assert health["healthy"] and health["active"][0]["bytes"] == 2
    store.close()


OLD_ARCHIVE_FIELDS = {
    "video_id",
    "status",
    "attempts",
    "retry_at",
    "claimed_at",
    "working_directory",
    "media_path",
    "media_name",
    "media_size",
    "media_md5",
    "media_sha1",
    "frozen_metadata",
    "ia_identifier",
    "operation_id",
    "upload_started_at",
    "media_verified_at",
    "recovery_result",
    "failure_phase",
    "wayback_job_id",
    "webpage_status",
    "ziggy_receipt",
    "cleaned_at",
    "last_error",
    "updated_at",
}
OLD_REPORT_FIELDS = {
    "id",
    "window_start",
    "window_end",
    "generated_at",
    "discovered_videos",
    "discovered_shorts",
    "discovered_livestreams",
    "lifetime_discovered_videos",
    "lifetime_discovered_shorts",
    "lifetime_discovered_livestreams",
    "media_archived",
    "lifetime_media_archived",
    "media_bytes",
    "lifetime_media_bytes",
    "active_channels",
    "unfinished_workflows",
    "awaiting_replay",
    "unavailable",
    "operator_action",
    "status",
    "attempts",
    "retry_at",
    "message_id",
    "last_error",
    "completed_at",
}


def legacy_database(root: Path) -> Path:
    path = root / "legacy.sqlite"
    store = StateStore(path)
    for index, status in enumerate(
        (
            "complete",
            "webpage",
            "uploading",
            "reconcile_upload",
            "upload",
            "upload",
            "metadata",
            "wayback_submitting",
            "complete",
            "upload",
        )
    ):
        video_id = "0UCmCkIDANU" if index == 4 else f"{index:011d}"
        _, media, _ = seed_upload(store, root, video_id)
        values: dict[str, Any] = {}
        if index in {0, 1, 7, 8}:
            values["media_verified_at"] = utcnow() - timedelta(days=1)
        if index == 0:
            values["webpage_status"] = "captured"
            values["cleaned_at"] = utcnow()
            values["working_directory"] = None
        if index == 8:
            values["webpage_status"] = "queued"
        if index == 9:
            values["media_size"] = None
        store.update_archive(video_id, status=status, **values)
        if index == 4:
            media.unlink()
    with Session(store.engine) as session:
        session.add(
            Report(
                window_start=utcnow() - timedelta(days=2),
                window_end=utcnow() - timedelta(days=1),
                generated_at=utcnow(),
                media_archived=17,
            )
        )
        session.commit()
    store.close()
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("DROP TABLE schemamigration")
        connection.execute("DROP TABLE archivejob")
        connection.execute("DROP INDEX ix_archiveprogress_media_uploaded_at")
        for model, old in (
            (ArchiveProgress, OLD_ARCHIVE_FIELDS),
            (Report, OLD_REPORT_FIELDS),
        ):
            for name in set(model.model_fields) - old:
                connection.execute(
                    f"ALTER TABLE {model.__name__.lower()} DROP COLUMN {name}"
                )
    return path


def test_migration_rehearsal_is_consistent_idempotent_and_preserves_reports(
    tmp_path: Path,
) -> None:
    source = legacy_database(tmp_path)
    snapshot = rehearse_migration(source, tmp_path / "rehearsal")
    with closing(sqlite3.connect(source)) as connection:
        assert "media_uploaded_at" not in {
            row[1] for row in connection.execute("PRAGMA table_info(archiveprogress)")
        }
    assert source.with_name(source.name + ".pre-upload-first.sqlite3").exists() is False
    store = StateStore(snapshot)
    with Session(store.engine) as session:
        historical = session.get(ArchiveProgress, "00000000001")
        assert historical is not None and historical.status == "complete"
        assert historical.media_uploaded_at and historical.upload_origin == "historical"
        missing = session.get(ArchiveProgress, "0UCmCkIDANU")
        assert missing is not None and missing.status == "download"
        ready = session.get(ArchiveProgress, "00000000005")
        assert ready is not None and ready.download_completed_at
        interrupted = session.get(ArchiveProgress, "00000000002")
        assert interrupted is not None and interrupted.status == "reconcile_upload"
        assert interrupted.download_completed_at
        assert session.get(ArchiveJob, (interrupted.video_id, "recovery"))
        uncertain_page = session.get(ArchiveJob, ("00000000007", "webpage"))
        assert uncertain_page is not None and uncertain_page.status == "failed"
        report = session.exec(select(Report)).one()
        assert (report.media_archived, report.semantics_version) == (17, 1)
        assert session.get(SchemaMigration, 2)
    store.close()
    deployed = StateStore(source)
    deployed._backup_before_migration()
    deployed.close()
    backup = source.with_name(source.name + ".pre-upload-first.sqlite3")
    assert backup.exists()
    StateStore(source).close()


def test_migration_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = StateStore(tmp_path / "source.sqlite")
    store.close()
    monkeypatch.delitem(__import__("sys").modules, "core.migration", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["migration", str(store.path), "--directory", str(tmp_path / "rehearsal")],
    )
    runpy.run_module("core.migration", run_name="__main__")


@pytest.mark.parametrize(
    "old_status,phase,webpage_status,error,expected_status",
    [
        (
            "operator_action",
            "wayback",
            None,
            "submission outcome is uncertain; NetworkError: lost response",
            "failed",
        ),
        (
            "operator_action",
            "ziggy",
            None,
            "submission outcome is uncertain; receipt persistence failed",
            "failed",
        ),
        (
            "operator_action",
            "webpage_submission",
            None,
            "process stopped while webpage submission outcome was uncertain",
            "failed",
        ),
        ("operator_action", "wayback", "failed", "Wayback capture failed", "failed"),
        ("webpage", "ziggy", None, "Ziggy rejected the request", "pending"),
        ("webpage", None, None, None, "pending"),
        ("operator_action", "cleanup", None, "cleanup failed", "pending"),
    ],
)
def test_migration_preserves_legacy_capture_retry_policy(
    tmp_path: Path,
    old_status: str,
    phase: str | None,
    webpage_status: str | None,
    error: str | None,
    expected_status: str,
) -> None:
    source = legacy_database(tmp_path)
    video_id = "00000000001"
    with closing(sqlite3.connect(source)) as connection, connection:
        connection.execute(
            "UPDATE archiveprogress SET status=?, failure_phase=?, "
            "webpage_status=?, last_error=? WHERE video_id=?",
            (old_status, phase, webpage_status, error, video_id),
        )
    snapshot = rehearse_migration(source, tmp_path / "rehearsal")
    store = StateStore(snapshot)
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "webpage"))
        row = session.get(ArchiveProgress, video_id)
        assert job is not None and job.status == expected_status
        assert row is not None and row.media_uploaded_at and row.status == "complete"
        assert session.get(ArchiveJob, (video_id, "cleanup")) is not None
        if expected_status == "failed":
            assert job.last_error == error
            assert row.webpage_status == (webpage_status or "uncertain")
    claim = store.claim_background("webpage")
    if expected_status == "failed":
        assert claim is None
    else:
        assert claim is not None and claim.video.id == video_id
    store.close()
