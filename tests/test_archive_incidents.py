from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from archivist import ServiceError
from sqlmodel import Session

from core import service
from core.archive_retry import INCIDENT_RECOVERY_DELAY, ArchiveServiceState
from core.service import SagaService
from core.state import StateStore, _as_utc, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, seed_upload, settings
from tests.test_archive_retry import clock as clock
from tests.test_archive_retry import gate, processor, upload


def outage(store: StateStore, root: Path, now: datetime) -> tuple[Any, FakeGateway]:
    gateway = FakeGateway()
    gateway.upload_error = ServiceError(
        "busy", status_code=503, request_phase="item-create"
    )
    work = upload(store, now)
    assert work is not None
    operation = processor(store, root, gateway)
    operation.process(work)
    return operation, gateway


def failed_retry(
    store: StateStore, operation: Any, clock: list[datetime], phase: str = "item-create"
) -> None:
    clock[0] = retry_at(store)
    work = upload(store, clock[0])
    assert work is not None and work.archive_probe_token
    operation.gateway.upload_error = ServiceError(
        "busy", status_code=503, request_phase=phase
    )
    operation.process(work)
    assert gate(store).failures == 2


def mutate(store: StateStore, **fields: Any) -> None:
    with Session(store.engine) as session:
        circuit = session.get(ArchiveServiceState, 1)
        assert circuit is not None
        for name, value in fields.items():
            setattr(circuit, name, value)
        session.add(circuit)
        session.commit()


def retry_at(store: StateStore) -> datetime:
    stamp = gate(store).retry_at
    assert stamp is not None
    return _as_utc(stamp)


def test_incident_escalation_reminders_detail_changes_and_restart(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    start = clock[0]
    first = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in first] == ["warning"]
    identity = first[0]["details"]["incident_id"]
    clock[0] += timedelta(seconds=31)
    # One failure followed by Saga's own backoff is not yet an escalation.
    assert store.archive_service_notifications(stall_timeout=30) == []
    assert (
        store.transfer_health(stall_timeout=30)["archive_service"]["severity"]
        == "warning"
    )
    failed_retry(store, operation, clock)
    escalation = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in escalation] == ["error"]
    escalated_at = clock[0]
    assert store.archive_service_notifications(stall_timeout=30) == []
    for index in range(3):
        clock[0] += timedelta(minutes=15)
        work = upload(store, clock[0])
        assert work is not None
        gateway.upload_error = ServiceError(
            "busy",
            status_code=503,
            request_phase="login",
            request_id=f"request-{index}",
        )
        operation.process(work)
        assert gate(store).incident_id == identity
        assert store.archive_service_notifications(stall_timeout=30) == []
    health = store.transfer_health(stall_timeout=30, now=clock[0])
    assert (
        health["archive_service"]["incident_age_seconds"]
        == (clock[0] - start).total_seconds()
    )
    assert health["archive_service"]["incident_escalated_at"]
    assert health["archive_service"]["incident_last_notified_at"]
    store.close()
    store = StateStore(path)
    store.reset_interrupted_claims()
    assert store.archive_service_notifications(stall_timeout=30) == []
    clock[0] = escalated_at + timedelta(seconds=3599)
    assert store.archive_service_notifications(stall_timeout=30) == []
    clock[0] += timedelta(seconds=1)
    reminders = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in reminders] == ["error"]
    assert reminders[0]["details"]["incident_id"] == identity
    assert store.archive_service_notifications(stall_timeout=30) == []
    store.close()


def test_actionable_category_changes_are_immediate_but_detail_changes_are_not(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    outage(store, tmp_path, clock[0])
    assert (
        store.archive_service_notifications(stall_timeout=3600)[0]["severity"]
        == "warning"
    )
    mutate(store, error_kind="rate_limit", error_code="SlowDown")
    assert store.archive_service_notifications(stall_timeout=3600) == []
    for kind, code, category in (
        ("authentication", "InvalidAccessKeyId", "authentication"),
        ("permission", "AccessDenied", "item-specific"),
        ("service", None, "transient"),
    ):
        mutate(store, error_kind=kind, error_code=code)
        events = store.archive_service_notifications(stall_timeout=3600)
        assert len(events) == 1 and events[0]["severity"] == (
            "warning" if kind == "service" else "error"
        )
        assert events[0]["details"]["incident_category"] == category
        mutate(
            store,
            generation=100,
            failures=70,
            error_request_id="different",
            error_phase="item-metadata",
            retry_at=clock[0] + timedelta(hours=1),
        )
        assert store.archive_service_notifications(stall_timeout=3600) == []
    store.close()


def test_recovery_is_once_persisted_and_next_outage_has_a_new_identity(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    # An incident first seen after a failed retry announces itself, then escalates.
    failed_retry(store, operation, clock)
    events = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in events] == ["warning", "error"]
    identity = gate(store).incident_id
    clock[0] = retry_at(store)
    probe = upload(store, clock[0])
    assert probe is not None
    gateway.upload_error = None
    operation.process(probe)
    assert gate(store).recovery_event is None
    assert store.archive_service_notifications(stall_timeout=30) == []
    assert (
        store.transfer_health(stall_timeout=30)["archive_service"][
            "incident_notification_state"
        ]
        == "observing"
    )
    clock[0] += timedelta(seconds=INCIDENT_RECOVERY_DELAY)
    store.close()
    store = StateStore(path)
    events = store.archive_service_notifications(stall_timeout=30)
    assert (
        len(events) == 1 and events[0]["severity"] == "info" and events[0]["recovery"]
    )
    assert events[0]["details"]["incident_id"] == identity
    assert gate(store).recovery_notified_at
    assert store.archive_service_notifications(stall_timeout=30) == []
    seed_upload(store, tmp_path, "NextVideo12")
    outage(store, tmp_path, clock[0])
    events = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in events] == ["warning"]
    assert (
        gate(store).incident_id != identity
        and gate(store).incident_escalated_at is None
    )
    store.close()


def test_brief_recovery_does_not_close_incident_or_reset_notification_budget(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    clock[0] += timedelta(seconds=31)
    store.archive_service_notifications(stall_timeout=30)
    old_id = gate(store).incident_id
    clock[0] = retry_at(store)
    probe = upload(store, clock[0])
    assert probe is not None
    gateway.upload_error = None
    operation.process(probe)
    seed_upload(store, tmp_path, "NextVideo12")
    outage(store, tmp_path, clock[0])
    events = store.archive_service_notifications(stall_timeout=30)
    assert events == []
    assert gate(store).incident_id == old_id
    assert gate(store).incident_healthy_since is None
    store.close()


def test_service_uses_durable_policy_and_marks_recovery_for_discord(
    tmp_path: Path, clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    saga = SagaService(settings(tmp_path), (), store, YouTubeClient())
    events: list[str] = []

    def bind(**fields: Any) -> Any:
        assert fields == {"archive_incident_recovery": True}
        return SimpleNamespace(info=lambda *args: events.append("info"))

    monkeypatch.setattr(
        service,
        "logger",
        SimpleNamespace(
            warning=lambda *args: events.append("warning"),
            error=lambda *args: events.append("error"),
            bind=bind,
        ),
    )
    alerts = [
        {
            "key": "archive:service",
            "severity": "warning",
            "message": "managed",
            "details": {},
        }
    ]
    saga._emit_health_alerts(alerts)
    failed_retry(store, operation, clock)
    clock[0] += timedelta(seconds=saga.settings.transfer_stall_timeout + 1)
    saga._emit_health_alerts(alerts)
    saga._emit_health_alerts(alerts)
    assert events == ["warning", "error"]
    assert "archive:service" not in saga._health_alerts
    clock[0] = max(clock[0], retry_at(store))
    probe = upload(store, clock[0])
    assert probe is not None
    gateway.upload_error = None
    operation.process(probe)
    saga._emit_health_alerts([])
    assert events == ["warning", "error"]
    clock[0] += timedelta(seconds=INCIDENT_RECOVERY_DELAY)
    saga._emit_health_alerts([])
    saga._emit_health_alerts([])
    assert events == ["warning", "error", "info"]
    store.close()


def test_legacy_incident_gets_identity_without_changing_retry_or_reason(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    now = utcnow()
    with Session(store.engine) as session:
        session.add(
            ArchiveServiceState(
                failures=2, first_failure_at=now, retry_at=now + timedelta(hours=2)
            )
        )
        session.commit()
    events = store.archive_service_notifications(stall_timeout=30, now=now)
    assert events[0]["details"]["incident_id"]
    assert events[0]["details"]["incident_category"] == "unclassified"
    assert events[0]["details"]["reason"] == "unknown"
    assert retry_at(store) == now + timedelta(hours=2)
    store.close()


def test_long_progressing_upload_suppresses_old_throttle_escalations_and_reminders(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    outage(store, tmp_path, clock[0])
    store.archive_service_notifications(stall_timeout=30)
    clock[0] = retry_at(store)
    probe = upload(store, clock[0])
    assert probe is not None and probe.archive_probe_token
    large = probe.video.id
    store.update_archive(large, status="uploading")
    for seconds in (31, 3600, 7200):
        clock[0] += timedelta(seconds=seconds)
        store.update_archive(
            large, transfer_bytes=seconds, transfer_progress_at=clock[0]
        )
        assert store.archive_service_notifications(stall_timeout=30) == []
        health = store.transfer_health(stall_timeout=30)
        assert health["healthy"]
        assert health["archive_service"]["severity"] == "warning"
        assert health["archive_service"]["active_upload_bytes"] == seconds
    clock[0] += timedelta(seconds=31)
    events = store.archive_service_notifications(stall_timeout=30)
    assert events[0]["severity"] == "error"
    assert not store.transfer_health(stall_timeout=30)["healthy"]
    clock[0] += timedelta(hours=1)
    store.update_archive(large, transfer_bytes=100000, transfer_progress_at=clock[0])
    assert store.archive_service_notifications(stall_timeout=30) == []
    mutate(store, error_kind="authentication", error_code="InvalidAccessKeyId")
    assert (
        store.archive_service_notifications(stall_timeout=30)[0]["severity"] == "error"
    )
    store.close()


def test_acknowledged_peer_progress_postpones_service_alarm_without_resetting_identity(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    outage(store, tmp_path, clock[0])
    identity = gate(store).incident_id
    peer, _, _ = seed_upload(store, tmp_path, "PeerVideo12")
    clock[0] += timedelta(seconds=60)
    store.update_archive(
        peer, status="complete", media_uploaded_at=clock[0], uploaded_bytes=4
    )
    events = store.archive_service_notifications(stall_timeout=30)
    assert [e["severity"] for e in events] == ["warning"]
    assert events[0]["details"]["acknowledged_uploads"] == 1
    assert events[0]["details"]["acknowledged_bytes"] == 4
    assert gate(store).incident_id == identity
    store.close()


def test_legacy_pending_recovery_is_forwarded_once(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    with Session(store.engine) as session:
        session.add(ArchiveServiceState(recovery_event='{"incident_id":"legacy"}'))
        session.commit()
    events = store.archive_service_notifications(stall_timeout=30)
    assert len(events) == 1 and events[0]["recovery"]
    assert store.archive_service_notifications(stall_timeout=30) == []
    store.close()


def test_legacy_active_incident_preserves_identity_on_its_next_probe_failure(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    with Session(store.engine) as session:
        session.add(
            ArchiveServiceState(
                failures=1,
                first_failure_at=clock[0],
                retry_at=clock[0],
                incident_id="legacy",
            )
        )
        session.commit()
    outage(store, tmp_path, clock[0])
    assert gate(store).incident_id == "legacy"
    assert gate(store).incident_started_at
    store.close()


def test_unescalated_incident_closes_quietly_after_stable_recovery(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    assert (
        store.archive_service_notifications(stall_timeout=3600)[0]["severity"]
        == "warning"
    )
    clock[0] = retry_at(store)
    probe = upload(store, clock[0])
    assert probe is not None
    gateway.upload_error = None
    operation.process(probe)
    clock[0] += timedelta(seconds=INCIDENT_RECOVERY_DELAY)
    assert store.archive_service_notifications(stall_timeout=3600) == []
    assert gate(store).incident_closed_at and gate(store).error_kind is None
    store.close()


def test_probe_permission_denial_updates_incident_without_password_guidance(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    operation, gateway = outage(store, tmp_path, clock[0])
    store.archive_service_notifications(stall_timeout=3600)
    clock[0] = retry_at(store)
    gateway.upload_error = ServiceError(
        "denied",
        status_code=403,
        service_code="AccessDenied",
        service_message="Item writes are restricted",
        request_phase="item-create",
    )
    probe = upload(store, clock[0])
    assert probe is not None and probe.archive_probe_token
    operation.process(probe)
    events = store.archive_service_notifications(stall_timeout=3600)
    assert len(events) == 1 and events[0]["severity"] == "error"
    assert events[0]["details"]["incident_category"] == "item-specific"
    assert "permissions" in events[0]["message"]
    assert "credentials" not in events[0]["message"]
    store.health_started_at = clock[0] - timedelta(hours=1)
    store.transfer_health(stall_timeout=30, now=clock[0])
    health = store.transfer_health(
        stall_timeout=30, now=clock[0] + timedelta(seconds=31)
    )
    assert not any(alert["key"] == "archive:permission" for alert in health["alerts"])
    assert any(alert["key"] == "archive:service" for alert in health["alerts"])
    store.close()
