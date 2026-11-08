import json
import pickle
from datetime import timedelta
from pathlib import Path
from typing import Any

import niquests
import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveApiKey,
    InternetArchiveUploadFile,
    InvalidServiceResponseError,
    NetworkError,
    ServiceError,
)
from niquests import Response
from sqlmodel import Session

from core import archival
from core.archival import ArchiveProcessor
from core.archive_errors import failure_facts
from core.state import ArchiveJob, StateStore, utcnow
from core.transport import TransportArchiveClient
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings


def wire(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    status: int,
    payload: Any,
    identifier: str = "item",
    retry_after: str | None = None,
) -> list[str]:
    puts: list[str] = []

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        response = Response()
        response.status_code = 200
        if url.endswith("csrf-token"):
            content: Any = {"success": True, "value": {"token": "fixture-token"}}
            phase = "csrf"
        elif url.endswith("login/"):
            content = {"success": True}
            phase = "login"
        elif url == "https://archive.org/upload":
            content = (
                '<input type="hidden" class="js-uploader-args" value=\''
                '{"s3user":{"s3accesskey":"key","s3secretkey":"secret"}}\'>'
            )
            phase = "upload-key"
        elif method == "POST":
            content = {"success": True, "identifier": identifier}
            phase = "identifier-check"
        else:
            puts.append(url)
            content = ""
            phase = "file-upload" if url.endswith(".mkv") else "item-create"
        if phase == endpoint:
            response.status_code = status
            content = payload
            if retry_after is not None:
                response.headers["Retry-After"] = retry_after
        response._content = (
            content.encode()
            if isinstance(content, str)
            else json.dumps(content).encode()
        )
        return response

    monkeypatch.setattr(archival.Session, "request", request)
    return puts


@pytest.mark.parametrize(
    "endpoint,status,payload,kind",
    [
        ("login", 400, "<html>Bad Request</html>", "service"),
        ("login", 400, {"success": True}, "service"),
        (
            "login",
            400,
            {"success": False, "error": "Incorrect password"},
            "authentication",
        ),
        ("login", 200, {"success": False}, "authentication"),
        ("login", 401, "Unauthorized", "authentication"),
        ("login", 403, "Forbidden", "authentication"),
        ("login", 502, "Server overloaded", "service"),
        ("login", 200, "<html>Gateway error</html>", "service"),
        ("login", 200, {}, "service"),
        ("csrf", 200, {"success": True, "value": {}}, "service"),
        ("csrf", 200, {"success": False}, "service"),
        ("csrf", 200, {"success": True, "value": {"token": ""}}, "service"),
        ("csrf", 503, "Unavailable", "service"),
        ("csrf", 401, "Unauthorized", "service"),
        ("csrf", 403, "Forbidden", "service"),
    ],
)
def test_login_replies_are_classified_and_persisted_without_false_password_alerts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    status: int,
    payload: Any,
    kind: str,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, endpoint, status, payload, frozen.identifier)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert row.status == "upload" and row.claimed_at is None and row.retry_at
    assert (row.error_kind, row.error_status, row.error_phase) == (
        kind,
        status,
        endpoint,
    )
    assert f"HTTP {status}" in (row.last_error or "") and f"phase={endpoint}" in (
        row.last_error or ""
    )
    assert media.exists() and not puts
    now = utcnow()
    store.health_started_at = now - timedelta(hours=1)
    store.transfer_health(stall_timeout=30, now=now)
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))
    assert any(a["key"] == "archive:access" for a in health["alerts"]) == (
        kind == "authentication"
    )
    store.close()


def test_generic_login_failure_keeps_phase_when_used_without_context_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wire(monkeypatch, "login", 400, "Bad Request")
    with archival.Session(retries=0) as session:
        client = TransportArchiveClient(
            session=session,
            account=InternetArchiveAccount("fixture@example.com", "fixture-password"),
        )
        with pytest.raises(InvalidServiceResponseError) as result:
            client.login()
    error = pickle.loads(pickle.dumps(result.value))
    assert (
        failure_facts(error).items()
        >= {
            "error_kind": "service",
            "error_status": 400,
            "error_phase": "login",
        }.items()
    )


def test_request_network_failures_keep_the_failed_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed(*args: Any, **kwargs: Any) -> Any:
        raise niquests.exceptions.ConnectionError("network failed")

    monkeypatch.setattr(archival.Session, "request", failed)
    with (
        archival.Session(retries=0) as session,
        TransportArchiveClient(
            session=session,
            account=InternetArchiveAccount("fixture@example.com", "password"),
        ) as client,
        pytest.raises(NetworkError) as result,
    ):
        client.login()
    assert (
        failure_facts(result.value).items()
        >= {
            "error_kind": "network",
            "error_status": None,
            "error_phase": "csrf",
            "error_code": "ConnectionError",
            "error_message": "network request to Internet Archive failed",
        }.items()
    )


def test_transfer_network_failures_name_the_transport_failure_in_alerts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, "item-create", 200, "", frozen.identifier)
    original = archival.Session.request

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        if url.endswith(".mkv"):
            raise niquests.exceptions.ReadTimeout("read timed out")
        return original(self, method, url, **kwargs)

    monkeypatch.setattr(archival.Session, "request", request)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert (row.error_kind, row.error_phase, row.error_code) == (
        "network",
        "file-upload",
        "ReadTimeout",
    )
    assert row.error_message == "network request to Internet Archive failed"
    assert "reason=ReadTimeout" in (row.last_error or "") and media.exists()
    store.close()


def test_network_failures_without_detail_keep_empty_diagnostics() -> None:
    error = NetworkError("")
    assert (
        failure_facts(error).items()
        >= {"error_kind": "network", "error_code": None, "error_message": None}.items()
    )


def test_upload_failures_keep_the_mutation_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, "file-upload", 503, "Unavailable", frozen.identifier)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert (
        row.status == "reconcile_upload"
        and row.error_phase == "file-upload"
        and row.error_status == 503
    )
    assert media.exists()
    store.close()


def test_resource_permission_failure_is_not_a_password_alert(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    error = AuthenticationError("resource denied", status_code=403)
    error.__dict__["request_phase"] = "file-upload"
    gateway = FakeGateway()
    gateway.upload_error = error
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).error_kind == "permission"
    now = utcnow()
    store.health_started_at = now - timedelta(hours=1)
    store.transfer_health(stall_timeout=30, now=now)
    health = store.transfer_health(stall_timeout=30, now=now + timedelta(seconds=31))
    assert any(a["key"] == "archive:permission" for a in health["alerts"])
    assert not any(a["key"] == "archive:access" for a in health["alerts"])
    store.close()


@pytest.mark.parametrize("original_status", ["upload", "operator_action"])
def test_startup_corrects_legacy_html_bad_request_records(
    tmp_path: Path, original_status: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    old = (
        "AuthenticationError: Bad Request Skip to main content "
        "The request had invalid syntax"
    )
    store.update_archive(
        video_id, status=original_status, failure_phase="upload", last_error=old
    )
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id, kind="recovery", status="failed", last_error=old
            )
        )
        session.commit()
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    store.reset_interrupted_claims()
    row = saved(store, video_id)
    assert (
        row.status == "upload"
        and row.error_kind == "service"
        and row.error_status == 400
        and row.error_phase == "login"
    )
    assert media.exists() and "AuthenticationError" not in (row.last_error or "")
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert (
            job is not None and job.status == "waiting" and job.error_kind == "service"
        )
    store.close()


@pytest.mark.parametrize("network", [False, True])
def test_recovery_preflight_failures_preserve_phase_and_do_not_send_media(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, network: bool
) -> None:
    media = tmp_path / "media.mkv"
    media.write_bytes(b"test")
    methods: list[str] = []

    def request(self: Any, request: Any, *args: Any, **kwargs: Any) -> Response:
        methods.append(request.method)
        if network:
            raise niquests.exceptions.ConnectionError("unavailable")
        response = Response()
        response.status_code = 503
        response._content = b"Unavailable"
        return response

    monkeypatch.setattr(archival.Session, "send", request)
    with (
        pytest.raises(ServiceError) as result,
        TransportArchiveClient(
            api_key=InternetArchiveApiKey("key", "secret")
        ) as client,
    ):
        client.add_file_transport(
            "item",
            InternetArchiveUploadFile(media),
            expected_metadata={
                "source": "https://example.com",
                "saga_operation_id": "operation",
            },
            expected_size=4,
        )
    assert (
        failure_facts(result.value).items()
        >= {
            "error_kind": "network" if network else "service",
            "error_status": None if network else 503,
            "error_phase": "identity",
        }.items()
    )
    assert methods == ["GET"] and media.read_bytes() == b"test"
