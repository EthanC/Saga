from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from archivist import ServiceError
from sqlmodel import Session

from core import service
from core.archive_retry import ArchiveServiceState
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
    escalation = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in escalation] == ["error"]
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
    clock[0] = start + timedelta(seconds=3630)
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
        assert len(events) == 1 and events[0]["severity"] == "error"
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
    clock[0] += timedelta(seconds=31)
    events = store.archive_service_notifications(stall_timeout=30)
    assert [event["severity"] for event in events] == ["warning", "error"]
    identity = gate(store).incident_id
    clock[0] = retry_at(store)
    probe = upload(store, clock[0])
    assert probe is not None
    gateway.upload_error = None
    operation.process(probe)
    assert gate(store).recovery_event
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


def test_pending_recovery_survives_a_new_outage_before_notification(
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
    assert [event["severity"] for event in events] == ["info", "warning"]
    assert events[0]["details"]["incident_id"] == old_id
    assert events[1]["details"]["incident_id"] != old_id
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
