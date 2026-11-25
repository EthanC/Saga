import json
import pickle
from datetime import timedelta
from email.utils import format_datetime
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest
from archivist import (
    InternetArchiveAccount,
    InternetArchiveClient,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryPhase,
    InternetArchiveRecoveryResult,
    InternetArchiveUploadError,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    ServiceError,
)
from sqlmodel import Session

from core import archival, workers
from core import logging as logging_module
from core.archival import _error_text
from core.archive_errors import (
    diagnostic_snapshot,
    failure_cause,
    failure_facts,
    incident_category,
)
from core.archive_retry import ArchiveServiceState, retry_hint, retryable_error
from core.shutdown import StopController
from core.state import ArchiveJob, ArchiveProgress, StateStore, _as_utc, utcnow
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_archive_classification import wire
from tests.test_archive_retry import gate, processor, upload
from tests.test_worker_protocol import Connection


@pytest.mark.parametrize(
    "endpoint,status,payload,kind",
    [
        ("login", 400, "<html>Bad Request</html>", "service"),
        (
            "login",
            400,
            {"success": False, "error": "Incorrect password"},
            "authentication",
        ),
        ("login", 401, "Unauthorized", "authentication"),
        ("csrf", 403, "Forbidden", "service"),
        (
            "csrf",
            503,
            {
                "Code": "SlowDown",
                "Message": "Please slow down",
                "RequestId": "request-123",
            },
            "rate_limit",
        ),
    ],
)
def test_released_client_contract_replaces_saga_annotations(
    monkeypatch: pytest.MonkeyPatch, endpoint: str, status: int, payload: Any, kind: str
) -> None:
    wire(monkeypatch, endpoint, status, payload, retry_after="3600")
    with archival.Session(retries=0) as session:
        client = InternetArchiveClient(
            session=session, account=InternetArchiveAccount("user", "password")
        )
        with pytest.raises(ServiceError) as raised:
            client.login()
    error = pickle.loads(pickle.dumps(raised.value))
    facts = failure_facts(error)
    assert (facts["error_kind"], facts["error_status"], facts["error_phase"]) == (
        kind,
        status,
        endpoint,
    )
    assert error.retry_after == 3600 and error.retry_after_raw == "3600"
    assert json.loads(facts["error_headers"])["retry-after"] == "3600"
    if isinstance(payload, dict) and "Code" in payload:
        assert (error.service_code, error.service_message, error.request_id) == (
            "SlowDown",
            "Please slow down",
            "request-123",
        )


@pytest.mark.parametrize(
    "code,status,phase,kind,category,retryable",
    [
        ("SlowDown", 503, "file-upload", "rate_limit", "throttled", True),
        (None, 503, "file-upload", "service", "transient", True),
        (
            "InvalidAccessKeyId",
            403,
            "file-upload",
            "authentication",
            "authentication",
            True,
        ),
        ("AccessDenied", 403, "file-upload", "permission", "item-specific", False),
        ("NewArchiveCode", 503, "file-upload", "service", "unclassified", True),
        ("InternalError", 500, "item-create", "service", "transient", True),
    ],
)
def test_codes_are_evidence_and_messages_are_not_classification(
    code: str | None, status: int, phase: str, kind: str, category: str, retryable: bool
) -> None:
    error = ServiceError(
        "failed",
        status_code=status,
        service_code=code,
        service_message="quota exceeded; incorrect password",
        request_phase=phase,
    )
    facts = failure_facts(error)
    assert facts["error_kind"] == kind
    assert incident_category(kind, code) == category
    assert retryable_error(error) is retryable
    assert f"reason={code or 'unknown'}" in _error_text(error)
    assert "message=quota exceeded" in _error_text(error)


@pytest.mark.parametrize("wrapped", [False, True])
def test_wrapped_results_and_retry_hints_survive_worker_ipc(
    monkeypatch: pytest.MonkeyPatch, wrapped: bool
) -> None:
    cause = ServiceError(
        "failed",
        status_code=503,
        service_code="SlowDown",
        service_message="Busy",
        request_id="request-id",
        request_phase="file-upload",
        retry_after=7200,
        retry_after_raw="7200",
        diagnostic_headers={"x-amz-request-id": "request-id"},
    )
    result = InternetArchiveUploadResult(
        "item", (InternetArchiveUploadFileResult("media", 4, transferred=True),)
    )
    error = InternetArchiveUploadError(result, cause, failed_file="next")
    if wrapped:
        error = InternetArchiveRecoveryError(
            InternetArchiveRecoveryResult("item", ()),
            error,
            phase=InternetArchiveRecoveryPhase.TRANSFER,
            failed_file="next",
        )

    class PickledConnection(Connection):
        def send(self, value: Any) -> None:
            super().send(pickle.loads(pickle.dumps(value)))

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise error

    child = PickledConnection()
    monkeypatch.setattr(workers, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(logging_module, "configure_logging", lambda _: None)
    monkeypatch.setattr(
        archival, "ArchiveGateway", lambda *args, **kwargs: SimpleNamespace(upload=fail)
    )
    workers._execute(
        cast(Any, child), Event(), "gateway", "upload", {"settings": None}, (), {}
    )
    parent = Connection(child.messages)
    process = SimpleNamespace(
        start=lambda: None, join=lambda **kwargs: None, is_alive=lambda: False
    )
    context = SimpleNamespace(
        Pipe=lambda: (parent, Connection()),
        Event=Event,
        Process=lambda **kwargs: process,
    )
    monkeypatch.setattr(workers.multiprocessing, "get_context", lambda _: context)
    with pytest.raises(ServiceError) as raised:
        workers.run_managed("gateway", "upload", {}, (), {}, StopController())
    restored = raised.value
    assert failure_facts(restored) == failure_facts(cause)
    assert retry_hint(restored, utcnow()) == 7200
    if wrapped:
        assert isinstance(restored, InternetArchiveRecoveryError)
        upload_error = restored.cause
    else:
        upload_error = restored
    assert isinstance(upload_error, InternetArchiveUploadError)
    assert upload_error.result == result and upload_error.failed_file == "next"
    assert failure_cause(restored) is upload_error.cause


def test_diagnostics_are_bounded_redacted_and_persisted_in_every_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    now = utcnow()
    error = ServiceError(
        "Bearer message-secret",
        status_code=503,
        service_code="SlowDown",
        service_message="https://user:url-secret@example.com/file?sig=signed-secret "
        "password=password-secret secret_key=key-secret " + "x" * 2000,
        request_id="request\n-id",
        request_phase="item-create",
        retry_after_raw="7200",
        diagnostic_headers={
            "Authorization": "Bearer header-secret",
            "Content-Type": "application/xml",
            "x-amz-request-id": "r" * 1000,
        },
    )
    gateway = FakeGateway()
    gateway.upload_error = pickle.loads(pickle.dumps(error))
    work = upload(store, now)
    assert work is not None
    processor(store, tmp_path, gateway).process(work)
    row = saved(store, video_id)
    circuit = gate(store)
    assert row.retry_at is not None and circuit.retry_at is not None
    assert (_as_utc(row.retry_at) - now).total_seconds() >= 7200
    assert (_as_utc(circuit.retry_at) - now).total_seconds() >= 7200
    assert row.error_request_id == "request-id" and len(row.error_message or "") == 1024
    assert len(row.error_headers or "") < 1024
    store.update_archive(
        video_id, status="reconcile_upload", release=True, retry_after=0
    )
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    recovery = store.claim_background("recovery", now=now + timedelta(seconds=1))
    assert recovery is not None
    store.update_job(
        recovery, status="waiting", error=_error_text(error), **failure_facts(error)
    )
    health = store.transfer_health(stall_timeout=30)
    assert health["archive_service"]["error_code"] == "SlowDown"
    assert health["archive_diagnostics"]["jobs"][0]["error_code"] == "SlowDown"
    output: list[str] = []
    monkeypatch.setattr(logging_module, "stdout", SimpleNamespace(write=output.append))
    monkeypatch.setattr(
        logging_module,
        "DiscordSink",
        lambda _: SimpleNamespace(
            emit=lambda record: output.append(record.getMessage())
        ),
    )
    logging_module._safe_console(_error_text(error))
    import logging

    logging_module._SafeDiscordSink("fixture").emit(
        logging.LogRecord("test", logging.ERROR, "", 0, json.dumps(health), (), None)
    )
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None
        representations = json.dumps(
            [
                row.model_dump(mode="json"),
                circuit.model_dump(mode="json"),
                job.model_dump(mode="json"),
                health,
                output,
            ]
        )
    for secret in (
        "message-secret",
        "url-secret",
        "signed-secret",
        "password-secret",
        "key-secret",
        "header-secret",
    ):
        assert secret not in representations
    assert media.exists()
    store.close()


def test_additive_diagnostics_migration_keeps_old_rows_and_retry_deadlines(
    tmp_path: Path,
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    video_id, _, _ = seed_upload(store, tmp_path)
    deadline = utcnow() + timedelta(hours=2)
    store.update_archive(video_id, release=True, retry_after=7200)
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery", retry_at=deadline))
        session.add(ArchiveServiceState(failures=1, retry_at=deadline))
        session.commit()
    before = saved(store, video_id).retry_at
    with store.engine.begin() as connection:
        for table in ("archiveprogress", "archivejob", "archiveservicestate"):
            for name in (
                "error_code",
                "error_message",
                "error_request_id",
                "error_headers",
            ):
                connection.exec_driver_sql(f"ALTER TABLE {table} DROP COLUMN {name}")
    store.close()
    store = StateStore(path)
    with Session(store.engine) as session:
        for record in (
            session.get(ArchiveProgress, video_id),
            session.get(ArchiveJob, (video_id, "recovery")),
            session.get(ArchiveServiceState, 1),
        ):
            assert record is not None
            assert all(
                diagnostic_snapshot(record)[name] is None
                for name in (
                    "error_code",
                    "error_message",
                    "error_request_id",
                    "error_headers",
                )
            )
    assert saved(store, video_id).retry_at == before
    retry_at = gate(store).retry_at
    assert retry_at is not None and _as_utc(retry_at) == deadline
    store.close()


def test_s3_xml_diagnostics_and_retry_date_survive_released_upload_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    now = utcnow().replace(microsecond=0)
    retry_at = now + timedelta(hours=2)
    wire(
        monkeypatch,
        "file-upload",
        503,
        "<Error><Code>SlowDown</Code><Message>Reduce upload rate</Message>"
        "<RequestId>s3-request</RequestId></Error>",
        frozen.identifier,
        retry_after=format_datetime(retry_at, usegmt=True),
    )
    with pytest.raises(InternetArchiveUploadError) as raised:
        archival.ArchiveGateway(settings(tmp_path)).upload(
            frozen, media, progress=lambda _: None
        )
    error = pickle.loads(pickle.dumps(raised.value))
    facts = failure_facts(error)
    assert (
        facts["error_code"],
        facts["error_message"],
        facts["error_request_id"],
        facts["error_phase"],
    ) == ("SlowDown", "Reduce upload rate", "s3-request", "file-upload")
    assert retry_hint(error, now) == 7200
    assert error.result.files and not error.result.files[0].transferred
    store.close()
