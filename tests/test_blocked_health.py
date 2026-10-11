from datetime import timedelta
from pathlib import Path

import pytest
from sqlmodel import Session

from core.state import ArchiveJob
from tests.test_archival import seed_upload
from tests.test_pipeline_health import queue


def test_storage_pause_alerts_without_restarting_progressing_workers(
    tmp_path: Path,
) -> None:
    store, _, now = queue(tmp_path, "download", recent=True)
    store.download_pause_reason = "data budget full"
    assert store.transfer_health(stall_timeout=30, now=now)["healthy"]
    later = now + timedelta(seconds=31)
    health = store.transfer_health(stall_timeout=30, now=later)
    assert health["healthy"] and health["lanes"]["download"]["status"] == "degraded"
    assert any(
        a["key"] == "download:storage" and a["severity"] == "error"
        for a in health["alerts"]
    )
    upload, _, _ = seed_upload(store, tmp_path, "UploadVid12")
    store.update_archive(
        upload, status="uploading", transfer_progress_at=later, transfer_bytes=0
    )
    assert any(
        a["key"] == "download:storage"
        for a in store.transfer_health(stall_timeout=30, now=later)["alerts"]
    )
    store.update_archive(
        upload, status="uploading", transfer_progress_at=later, transfer_bytes=2
    )
    assert store.transfer_health(stall_timeout=30, now=later)["healthy"]
    assert not any(
        a["key"] == "download:storage"
        for a in store.transfer_health(stall_timeout=30, now=later)["alerts"]
    )
    store.download_pause_reason = None
    assert store.transfer_health(stall_timeout=30, now=later)["healthy"]
    assert "download:storage" not in store._eligible_since
    store.close()


@pytest.mark.parametrize("kind", ["recovery", "cleanup"])
def test_required_background_dispatch_cannot_wait_forever(
    tmp_path: Path, kind: str
) -> None:
    store, video_id, now = queue(tmp_path, "reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind=kind))
        session.commit()
    assert store.transfer_health(stall_timeout=30, now=now)["healthy"]
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))
    assert not health["healthy"] and health["lanes"][kind]["status"] == "unhealthy"
    assert store.claim_background(kind, now=now + timedelta(seconds=31))
    assert store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))[
        "healthy"
    ]
    store.close()


def test_persistent_access_failure_alert_clears_after_success(tmp_path: Path) -> None:
    store, video_id, now = queue(tmp_path, "metadata")
    store.update_archive(
        video_id, failure_phase="youtube_access", release=True, retry_after=3600
    )
    assert store.transfer_health(stall_timeout=30, now=now)["healthy"]
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))
    assert health["healthy"]
    assert any(a["key"] == "youtube:access" for a in health["alerts"])
    store.update_archive(video_id, status="complete", failure_phase=None)
    assert store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=32))[
        "healthy"
    ]
    assert "youtube:access" not in store._eligible_since
    store.close()
