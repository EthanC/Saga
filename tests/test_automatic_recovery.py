from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from archivist import (
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InvalidOptionError,
)
from sqlmodel import Session

from core.archival import ArchiveProcessor, Inspection, inspect_item
from core.state import ArchiveJob, StateStore, _as_utc, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, run_job, saved, seed_upload, settings


def test_expired_production_recovery_resumes_and_releases_storage(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="recovery",
                status="expired",
                attempts=12,
                deadline_at=utcnow() - timedelta(days=3),
                last_error="item metadata is unresolved",
            )
        )
        session.commit()
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    store.reset_interrupted_claims()
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "waiting" and job.deadline_at is None
        assert job.attempts == 12 and job.last_error == "item metadata is unresolved"
    gateway = FakeGateway()
    gateway.inspection = inspect_item(InternetArchiveItem(frozen.identifier), frozen, 4)
    assert gateway.inspection.state == "absent"
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    run_job(store, processor, "recovery")
    assert media.exists() and saved(store, video_id).upload_mode == "create"
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    run_job(store, processor, "cleanup")
    assert not media.exists() and saved(store, video_id).media_uploaded_at
    assert gateway.upload_calls == 1 and gateway.recovery_calls == 0
    store.close()


def test_pending_recovery_keeps_retrying_with_backoff_without_restart(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="reconcile_upload")
    store.reset_interrupted_claims()  # Repairs a missing historical recovery job.
    gateway = FakeGateway()
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    for _ in range(3):
        with Session(store.engine) as session:
            job = session.get(ArchiveJob, (video_id, "recovery"))
            assert job is not None
            due = job.retry_at or utcnow()
        work = store.claim_background("recovery", now=due, max_attempts=1)
        assert work is not None
        processor.process(work)
        with Session(store.engine) as session:
            job = session.get(ArchiveJob, (video_id, "recovery"))
            assert (
                job is not None and job.retry_at is not None and job.deadline_at is None
            )
            assert (
                store.claim_background(
                    "recovery", now=job.retry_at - timedelta(microseconds=1)
                )
                is None
            )
    assert media.exists() and gateway.upload_calls == 0
    assert saved(store, video_id).status == "reconcile_upload"
    store.close()


def test_startup_restores_entire_backlog_before_any_worker_claim(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    now = utcnow()
    statuses = ("expired", "pending", "waiting", "passed", "failed", "missing")
    for index, status in enumerate(statuses):
        video_id, _, _ = seed_upload(store, tmp_path, f"Recover{index:04}")
        store.update_archive(video_id, status="reconcile_upload")
        if status == "missing":
            continue
        with Session(store.engine) as session:
            session.add(
                ArchiveJob(
                    video_id=video_id,
                    kind="recovery",
                    status=status,
                    attempts=11,
                    deadline_at=now - timedelta(days=3),
                    retry_at=now + timedelta(hours=1),
                    last_error="ownership conflict"
                    if status == "failed"
                    else "item metadata is unresolved",
                )
            )
            session.commit()
    completed, _, _ = seed_upload(store, tmp_path, "Completed12")
    store.update_archive(completed, status="complete", media_uploaded_at=now)
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=completed, kind="recovery", status="passed"))
        session.commit()
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    store.reset_interrupted_claims()
    store.reset_interrupted_claims()  # A second startup must not reset retry backoff.
    with Session(store.engine) as session:
        for index, status in enumerate(statuses):
            job = session.get(ArchiveJob, (f"Recover{index:04}", "recovery"))
            assert job is not None and job.claim_token is None
            if status == "failed":
                assert job.status == "failed" and job.last_error == "ownership conflict"
            else:
                assert job.status in {"pending", "waiting"} and job.deadline_at is None
                assert job.attempts == (0 if status == "missing" else 11)
                assert (_as_utc(job.retry_at) if job.retry_at else None) == (
                    now + timedelta(hours=1)
                    if status in {"pending", "waiting"}
                    else None
                )
        job = session.get(ArchiveJob, (completed, "recovery"))
        assert job is not None and job.status == "passed"
    store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"server_unavailable": True},
        {"is_dark": True},
        {"has_redrow": True},
        {"nodownload": True},
        {"extended_error_code": "101"},
        {"unavailable_reason": "not found"},
        {"availability": InternetArchiveItemAvailability.PENDING},
    ],
)
def test_remote_uncertainty_does_not_trigger_creation(
    tmp_path: Path, changes: dict
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(store, tmp_path)
    item = replace(InternetArchiveItem(frozen.identifier), **changes)
    assert inspect_item(item, frozen, 4).state == "pending"
    store.close()


def test_delayed_creation_race_returns_to_reconciliation(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = InvalidOptionError(
        "Internet Archive item identifier is unavailable"
    )
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).status == "reconcile_upload" and media.exists()
    gateway.inspection = Inspection("verified", receipt="remote receipt")
    run_job(store, processor, "recovery", now=utcnow() + timedelta(minutes=1))
    assert saved(store, video_id).media_uploaded_at
    store.close()
