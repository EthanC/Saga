import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from loguru import logger
from sqlmodel import Session

from core import logging as logging_module
from core.config import LoggingSettings
from core.service import SagaService
from core.state import ArchiveJob, StateStore
from core.youtube import YouTubeClient
from tests.test_archival import seed_upload, settings
from tests.test_archive_retry import clock as clock


def failed_page(
    store: StateStore, root: Path, video_id: str, now: datetime, **values: Any
) -> None:
    seed_upload(store, root, video_id)
    store.update_archive(
        video_id, status="complete", media_uploaded_at=now, uploaded_bytes=4
    )
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="webpage",
                status="failed",
                last_error="SubmissionUncertainError: invalid receipt",
                **values,
            )
        )
        session.commit()


def test_webpage_errors_reach_discord_once_per_outcome_set_across_restarts(
    tmp_path: Path, clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    messages: list[logging.LogRecord] = []

    class Sink:
        def __init__(self, url: str) -> None:
            pass

        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record)

    monkeypatch.setattr(logging_module, "DiscordSink", Sink)
    monkeypatch.setattr(logging_module, "_STOP_CONTROLLER", None)
    console = StringIO()
    monkeypatch.setattr(logging_module, "stdout", console)
    logging_module.configure_logging(LoggingSettings("INFO", "fixture", "ERROR"))
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    failed_page(store, tmp_path, "FailedPage1", clock[0])
    saga = SagaService(settings(tmp_path), (), store, YouTubeClient())

    def emit() -> None:
        health = store.transfer_health(stall_timeout=30, now=clock[0])
        assert health["healthy"]
        saga._emit_health_alerts(health["alerts"])
        logger.complete()

    emit()
    assert len(messages) == 1 and messages[0].levelno == logging.ERROR
    assert "FailedPage1" in messages[0].getMessage()
    for _ in range(3):
        clock[0] += timedelta(hours=1)
        emit()
    store.close()
    store = StateStore(path)
    saga = SagaService(settings(tmp_path), (), store, YouTubeClient())
    emit()
    assert len(messages) == 1
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, ("FailedPage1", "webpage"))
        assert job is not None
        job.last_error = (
            "new detail; password=credential-secret "
            "https://example.com?sig=signed-secret " + "x" * 1000
        )
        job.updated_at = clock[0]
        session.add(job)
        session.commit()
    emit()
    assert len(messages) == 1
    failed_page(store, tmp_path, "FailedPage2", clock[0])
    emit()
    assert len(messages) == 2
    assert "credential-secret" not in messages[-1].getMessage()
    assert "signed-secret" not in messages[-1].getMessage()
    snapshot = store.transfer_health(stall_timeout=30)
    assert snapshot["webpage_notification"]["active"]
    assert snapshot["webpage_notification"]["outcome_count"] == 2
    saga._emit_health_alerts([], snapshot_complete=False)
    assert store.transfer_health(stall_timeout=30)["webpage_notification"]["active"]
    with Session(store.engine) as session:
        for video_id in ("FailedPage1", "FailedPage2"):
            job = session.get(ArchiveJob, (video_id, "webpage"))
            assert job is not None
            job.status = "passed"
            job.last_error = None
            session.add(job)
        session.commit()
    emit()
    emit()
    # The INFO recovery follows LOG_DISCORD_LEVEL=ERROR and stays off Discord.
    assert [message.levelno for message in messages] == [logging.ERROR, logging.ERROR]
    assert "condition cleared" in console.getvalue()
    assert not store.transfer_health(stall_timeout=30)["webpage_notification"]["active"]
    store.close()
    logger.remove()


def test_deadline_expiry_fingerprint_is_stable_when_worker_marks_it_expired(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_page(
        store,
        tmp_path,
        "ExpiredPage",
        clock[0],
        deadline_at=clock[0] - timedelta(seconds=1),
    )
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, ("ExpiredPage", "webpage"))
        assert job is not None
        job.status = "waiting"
        session.add(job)
        session.commit()
    alerts = store.transfer_health(stall_timeout=30)["alerts"]
    outcome = next(alert for alert in alerts if alert["key"] == "webpage:outcome")
    assert outcome["severity"] == "error"
    assert store.webpage_outcome_notifications(outcome)
    assert store.claim_background("webpage") is None
    alerts = store.transfer_health(stall_timeout=30)["alerts"]
    outcome = next(alert for alert in alerts if alert["key"] == "webpage:outcome")
    assert store.webpage_outcome_notifications(outcome) == []
    store.close()


def test_stalled_webpage_worker_is_actionable_but_does_not_restart_container(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_page(
        store,
        tmp_path,
        "StalledPage",
        clock[0],
        claim_token="claim",
        claimed_at=clock[0] - timedelta(minutes=10),
    )
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, ("StalledPage", "webpage"))
        assert job is not None
        job.status = "waiting"
        session.add(job)
        session.commit()
    health = store.transfer_health(stall_timeout=30)
    assert health["healthy"]
    outcome = next(
        alert for alert in health["alerts"] if alert["key"] == "webpage:outcome"
    )
    assert outcome["severity"] == "error"
    assert outcome["details"]["active_without_progress"] == ["StalledPage"]
    assert store.webpage_outcome_notifications(outcome)
    assert store.webpage_outcome_notifications(outcome) == []
    store.close()
