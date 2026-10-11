from datetime import timedelta
from pathlib import Path

import pytest
from sqlmodel import Session

from core.state import ArchiveJob
from tests.test_archival import seed_upload
from tests.test_pipeline_health import queue


def test_active_recovery_does_not_fail_health_for_legacy_expired_backlog(
    tmp_path: Path,
) -> None:
    store, video_id, now = queue(tmp_path, "reconcile_upload")
    other, _, _ = seed_upload(store, tmp_path, "Expired1234")
    store.update_archive(other, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="recovery",
                status="waiting",
                claim_token="active",
                claimed_at=now,
            )
        )
        session.add(
            ArchiveJob(
                video_id=other,
                kind="recovery",
                status="expired",
                deadline_at=now - timedelta(days=3),
                last_error="item metadata is unresolved",
            )
        )
        session.commit()
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=20))
    assert health["healthy"] and not health["startup_grace"]
    recovery = health["lanes"]["recovery"]
    assert (
        recovery["status"] == "degraded"
        and recovery["waiting_reason"] == "worker occupied"
    )
    assert recovery["active_count"] == 1 and recovery["due_queue_count"] == 1
    assert not recovery["failed_or_expired"]
    assert not any(a["key"] == "recovery:outcome" for a in health["alerts"])
    store.close()


def test_remote_503_backoff_remains_degraded_past_startup_and_stall_timeout(
    tmp_path: Path,
) -> None:
    store, video_id, now = queue(tmp_path, "reconcile_upload")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="recovery",
                status="pending",
                retry_at=now + timedelta(hours=6),
                deadline_at=now - timedelta(days=1),
                last_error="Internet Archive returned HTTP 503",
            )
        )
        session.commit()
    for hours in (1, 5):
        health = store.transfer_health(
            stall_timeout=30, now=now + timedelta(hours=hours)
        )
        assert health["healthy"] and not health["startup_grace"]
        assert health["lanes"]["recovery"]["status"] == "degraded"
        assert health["lanes"]["recovery"]["waiting_reason"] == "retry backoff"
    store.close()


@pytest.mark.parametrize("kind", ["recovery", "cleanup", "validation", "webpage"])
def test_actual_background_worker_stalls_still_fail_required_lanes(
    tmp_path: Path, kind: str
) -> None:
    store, video_id, now = queue(tmp_path, "reconcile_upload")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind=kind,
                status="waiting",
                claim_token="active",
                claimed_at=now - timedelta(seconds=31),
            )
        )
        session.commit()
    health = store.transfer_health(stall_timeout=30, now=now)
    required = kind in {"recovery", "cleanup"}
    assert health["healthy"] != required
    assert health["lanes"][kind]["active_without_progress"] == [video_id]
    assert any(
        a["key"] == f"{kind}:outcome"
        and a["severity"] == ("error" if required else "warning")
        for a in health["alerts"]
    )
    store.close()


def test_conflict_requires_attention_without_restarting_healthy_workers(
    tmp_path: Path,
) -> None:
    store, video_id, now = queue(tmp_path, "operator_action")
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="recovery",
                status="failed",
                last_error="provenance mismatch",
            )
        )
        session.commit()
    health = store.transfer_health(stall_timeout=30, now=now)
    assert health["healthy"] and health["lanes"]["recovery"]["status"] == "degraded"
    assert health["lanes"]["recovery"]["waiting_reason"] == "operator review required"
    assert any(
        a["key"] == "recovery:outcome" and a["severity"] == "error"
        for a in health["alerts"]
    )
    store.close()
