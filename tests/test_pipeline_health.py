import asyncio
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import event
from sqlmodel import Session

from core import healthcheck, service
from core import state as state_module
from core.service import SagaService
from core.state import ArchiveJob, ArchiveProgress, Channel, StateStore, Video, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import seed_upload, settings


def queue(
    tmp_path: Path, stage: str, *, recent: bool = False
) -> tuple[StateStore, str, Any]:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    now = utcnow()
    state.health_started_at = now - timedelta(hours=2)
    with Session(state.engine) as session:
        row = session.get(ArchiveProgress, video_id)
        video = session.get(Video, video_id)
        assert row and video
        row.status = stage
        row.ready_at = None
        row.updated_at = now - timedelta(hours=1)
        video.discovered_at = row.updated_at
        video.notification_candidate = recent
        session.add_all((row, video))
        session.commit()
    return state, video_id, now


def test_old_ready_work_gets_grace_then_a_new_dispatch_window(tmp_path: Path) -> None:
    state, _, now = queue(tmp_path, "upload")
    first = state.transfer_health(stall_timeout=30, now=now, started_at=now)
    assert first["healthy"] and first["startup_grace"]
    assert first["lanes"]["upload"]["oldest_due_age_seconds"] >= 3600
    assert state.transfer_health(
        stall_timeout=30, now=now + timedelta(seconds=150), started_at=now
    )["healthy"]
    assert not state.transfer_health(
        stall_timeout=30, now=now + timedelta(seconds=151), started_at=now
    )["healthy"]
    state.close()


@pytest.mark.parametrize(
    "reason",
    [
        "disk budget full",
        "upload-ready buffer full",
        "protected partials exceed retention target",
    ],
)
def test_expected_download_wait_is_degraded_not_a_restart_trigger(
    tmp_path: Path, reason: str
) -> None:
    state, _, now = queue(tmp_path, "download", recent=True)
    state.download_pause_reason = reason
    health = state.transfer_health(stall_timeout=30, now=now)
    assert health["healthy"] and health["lanes"]["download"]["status"] == "degraded"
    assert health["recent"]["download"]["waiting_reason"] == reason
    assert any(alert["key"] == "download:recent-age" for alert in health["alerts"])
    assert state.transfer_health(stall_timeout=30, now=now + timedelta(hours=1))[
        "healthy"
    ]
    state.close()


def test_metadata_preservation_delay_survives_a_progressing_upload(
    tmp_path: Path,
) -> None:
    state, video_id, now = queue(tmp_path, "metadata", recent=True)
    upload_id, _, _ = seed_upload(state, tmp_path, "UploadVid12")
    state.update_archive(
        upload_id,
        status="uploading",
        upload_started_at=now,
        transfer_progress_at=now,
        transfer_bytes=2,
    )
    state.metadata_pause_reasons = {"recent": "recent metadata supply full"}
    health = state.transfer_health(stall_timeout=900, now=now)
    assert health["healthy"] and health["lanes"]["upload"]["status"] == "healthy"
    assert health["recent"]["metadata"]["video_id"] == video_id
    assert health["oldest_recent"]["video_id"] == video_id
    assert {alert["key"] for alert in health["alerts"]} == {"metadata:recent-age"}
    state.close()


def test_eligible_metadata_stall_and_resumed_claim_start_are_visible(
    tmp_path: Path,
) -> None:
    state, video_id, now = queue(tmp_path, "metadata", recent=True)
    first = state.transfer_health(stall_timeout=30, now=now)
    assert first["healthy"] and first["lanes"]["metadata"]["waiting_reason"] is None
    assert not state.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))[
        "healthy"
    ]
    state.update_archive(
        video_id,
        status="download",
        download_completed_at=None,
        download_progress_at=now - timedelta(hours=1),
        download_bytes=2,
    )
    claim = state.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
    assert claim
    health = state.transfer_health(stall_timeout=30)
    assert (
        health["healthy"]
        and health["oldest_recent"]["waiting_reason"] == "transfer in progress"
    )
    state.update_archive(video_id, release=True)
    assert claim.progress.operation_id
    assert state.claim_partial_maintenance(video_id, claim.progress.operation_id)
    health = state.transfer_health(stall_timeout=30)
    assert health["lanes"]["download"]["waiting_reason"] == "partial maintenance"
    state.close()


def test_polling_startup_grace_does_not_require_historical_freshness(
    tmp_path: Path,
) -> None:
    state, _, now = queue(tmp_path, "upload")
    health = state.transfer_health(
        stall_timeout=30, now=now, started_at=now, channel_intervals={"missing": 60}
    )
    assert health["healthy"] and health["polling"]["missing"]["healthy"]
    state.close()


@pytest.mark.parametrize(
    "phase,stalled", [("downloading", False), ("merging", False), ("merging", True)]
)
def test_download_and_merge_health_use_progress_not_thread_liveness(
    tmp_path: Path, phase: str, stalled: bool
) -> None:
    state, video_id, now = queue(tmp_path, "download")
    state.update_archive(
        video_id,
        download_phase=phase,
        download_bytes=4,
        merge_bytes=2,
        download_progress_at=now - timedelta(seconds=901 if stalled else 20),
    )
    with Session(state.engine) as session:
        row = session.get(ArchiveProgress, video_id)
        assert row
        row.claimed_at = now - timedelta(hours=1)
        session.add(row)
        session.commit()
    health = state.transfer_health(stall_timeout=900, now=now)
    assert health["healthy"] != stalled
    assert health["lanes"]["download"]["active"][0]["phase"] == phase
    state.close()


def test_retry_becoming_due_gets_its_own_dispatch_window(tmp_path: Path) -> None:
    state, video_id, now = queue(tmp_path, "upload", recent=True)
    state.update_archive(video_id, release=True, retry_after=60)
    health = state.transfer_health(stall_timeout=30, now=now)
    assert (
        health["healthy"]
        and health["lanes"]["upload"]["waiting_reason"] == "retry backoff"
    )
    assert health["oldest_recent"]["waiting_reason"] == "retry deadline"
    due = now + timedelta(seconds=61)
    assert state.transfer_health(stall_timeout=30, now=due)["healthy"]
    assert not state.transfer_health(stall_timeout=30, now=due + timedelta(seconds=31))[
        "healthy"
    ]
    state.update_archive(video_id, status="reconcile_upload", release=True)
    assert (
        state.transfer_health(stall_timeout=30)["oldest_recent"]["waiting_reason"]
        == "remote upload reconciliation"
    )
    state.close()


@pytest.mark.parametrize(
    "kind,status",
    [
        ("validation", "failed"),
        ("webpage", "expired"),
        ("recovery", "waiting"),
        ("cleanup", "pending"),
    ],
)
def test_background_outcomes_leave_upload_capacity_independent(
    tmp_path: Path, kind: str, status: str
) -> None:
    state, video_id, now = queue(tmp_path, "upload")
    with Session(state.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind=kind,
                status=status,
                deadline_at=now - timedelta(seconds=1),
                updated_at=now - timedelta(minutes=10),
                last_error="fixture",
            )
        )
        session.commit()
    health = state.transfer_health(stall_timeout=900, now=now)
    assert health["healthy"]
    assert bool(health["lanes"][kind]["failed_or_expired"]) == (kind != "cleanup")
    state.close()


def test_history_and_other_channels_cannot_refresh_failed_recent_poll(
    tmp_path: Path,
) -> None:
    state, _, now = queue(tmp_path, "upload")
    with Session(state.engine) as session:
        channel = session.get(Channel, "channel")
        assert channel
        channel.baseline_at = now - timedelta(hours=1)
        channel.last_polled_at = now - timedelta(minutes=10)
        session.add(channel)
        session.add(
            Channel(
                id="healthy", title="Healthy", canonical_url="test", last_polled_at=now
            )
        )
        session.commit()
    state.record_discoveries("channel", (), backfill=True)
    health = state.transfer_health(
        stall_timeout=900,
        now=now,
        channel_intervals={"channel": 60, "healthy": 60, "missing": 60},
    )
    assert not health["healthy"]
    assert health["polling"]["healthy"]["healthy"]
    assert not health["polling"]["channel"]["healthy"]
    state.close()


def test_poll_completed_during_health_reads_has_a_nonnegative_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, _, began = queue(tmp_path, "upload")
    state.establish_baseline("channel", ())
    clock = [began]
    completed = False
    monkeypatch.setattr(state_module, "utcnow", lambda: clock[0])

    def complete_poll(
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        nonlocal completed
        if not completed and "FROM archiveprogress JOIN video" in statement:
            completed = True
            clock[0] = began + timedelta(microseconds=701218)
            state.record_discoveries("channel", (), backfill=False)

    event.listen(state.engine, "after_cursor_execute", complete_poll)
    try:
        health = state.transfer_health(
            stall_timeout=900, channel_intervals={"channel": 60}
        )
        assert completed
        assert health["polling"]["channel"]["age_seconds"] == 0
        assert health["healthy"] and not health["startup_grace"]
        assert health["generated_at"] == clock[0].isoformat()
        assert not any(alert["key"].startswith("poll:") for alert in health["alerts"])
    finally:
        event.remove(state.engine, "after_cursor_execute", complete_poll)
        state.close()


def test_genuinely_future_poll_timestamp_still_fails_health(tmp_path: Path) -> None:
    state, _, now = queue(tmp_path, "upload")
    try:
        with Session(state.engine) as session:
            channel = session.get(Channel, "channel")
            assert channel
            channel.last_polled_at = now + timedelta(seconds=1)
            session.add(channel)
            session.commit()
        health = state.transfer_health(
            stall_timeout=900, now=now, channel_intervals={"channel": 60}
        )
        assert not health["healthy"]
        assert health["polling"]["channel"]["age_seconds"] == -1
        assert any(alert["key"] == "poll:channel" for alert in health["alerts"])
    finally:
        state.close()


def test_alert_transitions_reminders_and_recovery_are_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    saga = SagaService(settings(tmp_path), (), state, YouTubeClient())
    events: list[str] = []
    monkeypatch.setattr(
        service,
        "logger",
        SimpleNamespace(
            error=lambda *args: events.append("error"),
            warning=lambda *args: events.append("warning"),
            info=lambda *args: events.append("recovered"),
        ),
    )
    now = [0]
    monkeypatch.setattr(service.time, "monotonic", lambda: now[0])
    alert = {
        "key": "upload:stall",
        "severity": "error",
        "message": "stall",
        "details": {},
    }
    saga._emit_health_alerts([alert])
    now[0] = 30
    saga._emit_health_alerts([alert])
    now[0] = 900
    saga._emit_health_alerts([alert])
    saga._emit_health_alerts([dict(alert, severity="warning", message="different")])
    saga._emit_health_alerts([])
    saga._emit_health_alerts([])
    assert events == ["error", "error", "warning", "recovered"]
    state.close()


def test_interrupted_health_publication_keeps_previous_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    saga = SagaService(settings(tmp_path), (), state, YouTubeClient())
    target = tmp_path / "transfers.json"
    target.write_text('{"healthy":true}')
    original = Path.replace
    calls = 0

    def replace(path: Path, destination: Any) -> Path:
        nonlocal calls
        if Path(destination) == target and calls == 0:
            calls += 1
            raise OSError("fixture")
        return original(path, destination)

    async def wait(_: float) -> None:
        assert target.read_text() == '{"healthy":true}'
        saga.request_stop()

    monkeypatch.setattr(Path, "replace", replace)
    monkeypatch.setattr(saga, "_wait", wait)
    asyncio.run(saga._health_loop())
    assert calls == 1 and "publisher" in saga._health_alerts
    state.close()


@pytest.mark.parametrize(
    "stamp", ["now", "old", "future", "naive", "missing", "wrongtype"]
)
def test_versioned_health_checks_publisher_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stamp: str
) -> None:
    heartbeat = tmp_path / "healthy"
    now = utcnow()
    payload: dict[str, Any] = {"version": 1, "healthy": True}
    if stamp != "missing":
        payload["generated_at"] = {
            "now": now.isoformat(),
            "old": (now - timedelta(minutes=3)).isoformat(),
            "future": (now + timedelta(minutes=1)).isoformat(),
            "naive": now.replace(tzinfo=None).isoformat(),
            "wrongtype": None,
        }[stamp]
    heartbeat.write_text(json.dumps(payload))
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(heartbeat))
    assert healthcheck.is_healthy(tmp_path) == (stamp == "now")
