from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveUploadError,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
)
from sqlmodel import Session

from core.archival import ArchiveProcessor, Inspection, _error_text
from core.state import ArchiveJob, StateStore, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings


@pytest.mark.parametrize("stage", ["upload", "recovery"])
def test_credentials_failure_retries_and_recovers_when_access_returns(
    tmp_path: Path, stage: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)

    class Gateway(FakeGateway):
        def inspect(self, *args: Any, **kwargs: Any) -> Inspection:
            if self.upload_error:
                raise self.upload_error
            return Inspection("verified", receipt="receipt")

    gateway = Gateway()
    gateway.upload_error = AuthenticationError("login required", status_code=401)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    if stage == "recovery":
        store.update_archive(video_id, status="reconcile_upload")
        store.reset_interrupted_claims()
        work = store.claim_background(stage)
    else:
        work = store.claim_archive(lane=stage, prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert media.exists() and saved(store, video_id).status != "operator_action"
    now = utcnow()
    store.health_started_at = now - timedelta(hours=1)
    assert store.transfer_health(stall_timeout=30, now=now)["healthy"]
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))
    assert health["healthy"]
    assert any(a["key"] == "archive:access" for a in health["alerts"])
    gateway.upload_error = None
    if stage == "recovery":
        retry = store.claim_background(stage, now=now + timedelta(seconds=31))
    else:
        retry = store.claim_archive(
            lane=stage, prefer_recent=False, now=now + timedelta(seconds=31)
        )
    assert retry is not None
    processor.process(retry)
    assert saved(store, video_id).media_uploaded_at
    assert store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=32))[
        "healthy"
    ]
    store.close()


def test_startup_revives_legacy_credentials_failures(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(
        video_id,
        status="operator_action",
        failure_phase="upload",
        last_error="AuthenticationError: bad credentials",
    )
    recovery_id, _, _ = seed_upload(store, tmp_path, "Recover1234")
    store.update_archive(recovery_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=recovery_id,
                kind="recovery",
                status="failed",
                last_error="AuthenticationError: bad credentials",
            )
        )
        session.commit()
    store.reset_interrupted_claims()
    assert saved(store, video_id).status == "upload"
    assert store.claim_background("recovery")
    store.close()


def test_unexpected_preflight_authentication_failure_is_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="metadata")
    processor = ArchiveProcessor(
        settings(tmp_path), store, YouTubeClient(), FakeGateway()
    )

    def denied(*args: Any) -> None:
        raise AuthenticationError("credentials")

    monkeypatch.setattr(processor, "_metadata", denied)
    work = store.claim_archive(lane="metadata", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert (
        saved(store, video_id).status == "metadata" and saved(store, video_id).retry_at
    )
    store.close()


def test_upload_error_keeps_the_original_http_failure() -> None:
    error = InternetArchiveUploadError(
        InternetArchiveUploadResult("item", ()),
        InvalidServiceResponseError("service unavailable", status_code=503),
    )
    assert "HTTP 503" in _error_text(error)
    assert "InvalidServiceResponseError: service unavailable" in _error_text(error)


def test_unexpected_upload_service_response_retains_media_for_reconciliation(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = InvalidServiceResponseError(
        "unexpected rejection", status_code=400
    )
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).status == "reconcile_upload" and media.exists()
    assert saved(store, video_id).claimed_at is None
    store.close()
