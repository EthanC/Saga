import asyncio
import json
import os
import sqlite3
from dataclasses import replace
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from typing import Any

import pytest
from archivist import (
    InternetArchiveApiKey,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
)
from niquests import Response
from sqlmodel import Session

from core import archival, healthcheck, migration, service, youtube
from core.archival import ArchiveProcessor, inspect_item
from core.state import ArchiveJob, StateStore, utcnow
from core.transport import RecoveryConflictError, TransportArchiveClient
from tests.test_archival import (
    FakeGateway,
    run_job,
    saved,
    seed_upload,
    settings,
    valid_item,
)


def test_truncated_ziggy_202_receipt_is_not_resubmitted(tmp_path: Path) -> None:
    submissions: list[bytes] = []

    class Ziggy(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            submissions.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.write(b'{"submissions":[')
            self.close_connection = True

        def log_message(self, format: str, *args: Any) -> None:
            pass

    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    with ThreadingHTTPServer(("127.0.0.1", 0), Ziggy) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            configured = replace(
                settings(tmp_path), ziggy_host=f"http://127.0.0.1:{server.server_port}"
            )
            processor = ArchiveProcessor(
                configured, state, youtube.YouTubeClient(), FakeGateway()
            )
            work = state.claim_archive(lane="upload", prefer_recent=False)
            assert work is not None
            processor.process(work)
            run_job(state, processor, "webpage")
            row = saved(state, video_id)
            assert row.webpage_status == "uncertain" and row.media_uploaded_at
            with Session(state.engine) as session:
                job = session.get(ArchiveJob, (video_id, "webpage"))
                assert job is not None and job.status == "failed"
                assert "IncompleteRead" in (job.last_error or "")
            assert (
                state.claim_background("webpage", now=utcnow() + timedelta(minutes=1))
                is None
            )
            state.close()
            state = StateStore(tmp_path / "saga.sqlite")
            state.reset_interrupted_claims()
            assert state.claim_background("webpage") is None
            processor = ArchiveProcessor(
                configured, state, youtube.YouTubeClient(), FakeGateway()
            )
            run_job(state, processor, "cleanup")
            assert not media.exists() and len(submissions) == 1
        finally:
            server.shutdown()
            thread.join()
            state.close()


@pytest.mark.parametrize(
    "error", [TimeoutError("receipt timed out"), ConnectionResetError("reset")]
)
def test_ziggy_receipt_read_errors_preserve_uncertain_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: OSError
) -> None:
    class Reply:
        status = 202

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def read(self) -> bytes:
            raise error

    monkeypatch.setattr(archival, "urlopen", lambda *args, **kwargs: Reply())
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    state.queue_webpage_only(video_id, deadline=60)
    processor = ArchiveProcessor(
        replace(settings(tmp_path), ziggy_host="http://ziggy"),
        state,
        youtube.YouTubeClient(),
        FakeGateway(),
    )
    run_job(state, processor, "webpage")
    assert saved(state, video_id).webpage_status == "uncertain"
    assert (
        state.claim_background("webpage", now=utcnow() + timedelta(seconds=10)) is None
    )
    state.close()


@pytest.mark.parametrize("endpoint", ["key", "availability"])
@pytest.mark.parametrize("status_code", [408, 425, 429, 500, 503])
def test_transient_upload_preflight_errors_retry_without_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, endpoint: str, status_code: int
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    failed = False
    puts: list[str] = []

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        nonlocal failed
        response = Response()
        response.status_code = 200
        response._content = b""
        if (
            not failed
            and url
            == {
                "key": "https://archive.org/upload",
                "availability": "https://archive.org/upload/app/upload_api.php",
            }[endpoint]
        ):
            failed = True
            response.status_code = status_code
            return response
        if url.endswith("csrf-token"):
            response._content = b'{"success":true,"value":{"token":"token"}}'
        elif url.endswith("login/"):
            response._content = b'{"success":true}'
        elif url == "https://archive.org/upload":
            response._content = (
                b'<input type="hidden" class="js-uploader-args" value=\''
                b'{"s3user":{"s3accesskey":"key","s3secretkey":"secret"}}\'>'
            )
        elif method == "POST":
            response._content = json.dumps(
                {"success": True, "identifier": frozen.identifier}
            ).encode()
        elif method == "PUT":
            puts.append(url)
            if url.endswith(frozen.media_name):
                assert b"".join(kwargs["data"]) == b"test"
        elif url == "https://s3.us.archive.org":
            response._content = b'{"over_limit":0}'
        else:
            pytest.fail(f"unexpected preflight recovery request: {method} {url}")
        return response

    monkeypatch.setattr(archival.Session, "request", request)
    processor = ArchiveProcessor(settings(tmp_path), state, youtube.YouTubeClient())
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)

    row = saved(state, video_id)
    assert row.status == "upload" and row.retry_at is not None
    assert row.claimed_at is None and row.media_uploaded_at is None
    assert puts == [] and media.exists()
    with Session(state.engine) as session:
        assert session.get(ArchiveJob, (video_id, "recovery")) is None
    assert (
        state.claim_archive(
            lane="upload",
            prefer_recent=False,
            now=row.retry_at - timedelta(microseconds=1),
        )
        is None
    )
    retry = state.claim_archive(lane="upload", prefer_recent=False, now=row.retry_at)
    assert retry is not None
    processor.process(retry)
    assert saved(state, video_id).media_uploaded_at is not None
    assert puts == [
        f"https://s3.us.archive.org/{frozen.identifier}",
        f"https://s3.us.archive.org/{frozen.identifier}/{frozen.media_name}",
    ]
    state.close()


@pytest.mark.parametrize(
    "payload", [None, "stale", "[]", "bad", '{"healthy":false}', '{"healthy":true}']
)
def test_healthcheck_requires_a_current_service_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: str | None
) -> None:
    heartbeat = tmp_path / "healthy"
    if payload:
        document = '{"healthy":true}' if payload == "stale" else payload
        if document.startswith("{"):
            value = json.loads(document)
            value.update(version=1, generated_at=utcnow().isoformat())
            document = json.dumps(value)
        heartbeat.write_text(document, encoding="utf-8")
        if payload == "stale":
            os.utime(heartbeat, (1, 1))
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(heartbeat))
    assert healthcheck.is_healthy(tmp_path) == (payload == '{"healthy":true}')


def test_transfer_health_publishes_stalls_and_survives_cleanup_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, ready_at=utcnow() - timedelta(hours=1))
    saga = service.SagaService(settings(tmp_path), (), state, youtube.YouTubeClient())
    saga.started_at = utcnow() - timedelta(hours=1)
    state._eligible_since["upload"] = saga.started_at

    async def wait(_: float) -> None:
        saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    saga._staging_bytes = service.staging_usage(saga.settings.download_root)
    asyncio.run(saga._health_loop())
    health = json.loads((tmp_path / "transfers.json").read_text(encoding="utf-8"))
    assert not health["healthy"] and health["staging_bytes"] == 4
    state.update_archive(video_id, ready_at=utcnow())
    saga = service.SagaService(settings(tmp_path), (), state, youtube.YouTubeClient())
    monkeypatch.setattr(saga, "_wait", wait)
    asyncio.run(saga._health_loop())
    assert json.loads((tmp_path / "transfers.json").read_text())["healthy"]
    gone = SimpleNamespace(
        is_file=lambda: True, stat=lambda: (_ for _ in ()).throw(FileNotFoundError())
    )
    remaining = SimpleNamespace(
        is_file=lambda: True, stat=lambda: SimpleNamespace(st_size=4)
    )
    directory = SimpleNamespace(is_file=lambda: False)
    assert (
        service.staging_usage(
            SimpleNamespace(rglob=lambda _: (gone, remaining, directory))
        )
        == 4
    )
    state.close()


@pytest.mark.parametrize("problem", ["operation", "metadata", "directory"])
def test_corrupt_persisted_contract_does_not_send_bytes(
    tmp_path: Path, problem: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    values: dict[str, Any] = (
        {"operation_id": "other"}
        if problem == "operation"
        else {"frozen_metadata": None}
        if problem == "metadata"
        else {"working_directory": None, "status": "download"}
    )
    state.update_archive(video_id, **values)
    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    gateway = FakeGateway()
    ArchiveProcessor(
        settings(tmp_path), state, youtube.YouTubeClient(), gateway
    ).process(work)
    assert gateway.upload_calls == 0
    assert saved(state, video_id).claimed_at is None
    state.close()


def test_unexpected_capture_status_retries_only_capture(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    processor = ArchiveProcessor(
        settings(tmp_path), state, youtube.YouTubeClient(), gateway
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    run_job(state, processor, "webpage")
    gateway.page_result = object()
    run_job(state, processor, "webpage", now=utcnow() + timedelta(minutes=1))
    assert saved(state, video_id).status == "complete"
    disabled = ArchiveProcessor(
        settings(tmp_path), state, youtube.YouTubeClient(), gateway
    )
    disabled.gateway = None
    with pytest.raises(RuntimeError, match="credentials"):
        disabled._gateway()
    state.close()


def test_ambiguous_duplicate_originals_cannot_establish_completion(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(state, tmp_path)
    item = valid_item(frozen)
    from dataclasses import replace

    assert (
        inspect_item(replace(item, files=item.files * 2), frozen, 4).state == "conflict"
    )
    state.close()


@pytest.mark.parametrize(
    "failure",
    ["bad_provenance", "changed_size", "http_error", "bad_status", "bad_body"],
)
def test_transport_errors_preserve_uncertainty_and_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    path = tmp_path / "media.mkv"
    path.write_bytes(b"test")
    expected = {"source": "https://example.com", "saga_operation_id": "operation"}

    def request(
        self: Any, method: str, url: str, *args: Any, **kwargs: Any
    ) -> Response:
        response = Response()
        response.status_code = 200
        if url.endswith("services/user.php"):
            response._content = b'{"success":true,"value":{"email":"owner"}}'
        elif method == "GET":
            response._content = json.dumps(
                {
                    "metadata": {"identifier": "item", "uploader": "owner", **expected},
                    "files": [],
                }
            ).encode()
        else:
            response.status_code = (
                403
                if failure == "http_error"
                else 202
                if failure == "bad_status"
                else 200
            )
            response._content = b"unexpected" if failure == "bad_body" else b""
        return response

    monkeypatch.setattr(TransportArchiveClient, "_recovery_request", request)
    with (
        TransportArchiveClient(
            api_key=InternetArchiveApiKey("key", "secret")
        ) as client,
        pytest.raises(
            RecoveryConflictError
            if failure in {"bad_provenance", "changed_size"}
            else InternetArchiveUploadError
        ),
    ):
        client.add_file_transport(
            "item",
            InternetArchiveUploadFile(path),
            expected_metadata={} if failure == "bad_provenance" else expected,
            expected_size=5 if failure == "changed_size" else 4,
        )
    assert path.read_bytes() == b"test"


def test_independent_claims_reject_stale_operations_and_late_failure_writes(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    with Session(state.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="validation"))
        session.commit()
    check = state.claim_background("validation")
    assert check is not None
    with pytest.raises(ValueError):
        state.update_job(check, status="passed", uploaded_bytes=4)
    state.update_archive(video_id, operation_id="replacement")
    with pytest.raises(RuntimeError, match="operation"):
        state.update_job(check, status="passed")
    with pytest.raises(RuntimeError, match="operation"):
        state.acknowledge_upload(work, receipt="old receipt", etag=None)
    with pytest.raises(RuntimeError, match="operation"):
        state.defer_transfer(
            work, status="upload", error="late error", delay=1, deadline=1
        )
    state.update_archive(video_id, operation_id=work.progress.operation_id)
    state.defer_transfer(
        work, status="reconcile_upload", error="lost", delay=0, deadline=100
    )
    state.defer_transfer(
        work, status="reconcile_upload", error="lost again", delay=0, deadline=100
    )
    state.acknowledge_upload(work, receipt="receipt", etag=None)
    state.defer_transfer(work, status="upload", error="late error", delay=1, deadline=1)
    assert saved(state, video_id).status == "complete"
    assert state.claim_background("recovery") is None
    with pytest.raises(LookupError):
        state.queue_webpage_only("missing", deadline=100)
    state.close()


def test_expired_jobs_and_interrupted_capture_do_not_lock_transfers(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    with Session(state.engine) as session:
        session.add(
            ArchiveJob(
                video_id=video_id,
                kind="validation",
                deadline_at=utcnow() - timedelta(seconds=1),
            )
        )
        session.add(ArchiveJob(video_id=video_id, kind="webpage"))
        session.commit()
    assert state.claim_background("validation") is None
    page = state.claim_background("webpage")
    assert page is not None
    state.update_job(page, status="submitting")
    state.reset_interrupted_claims()
    with Session(state.engine) as session:
        job = session.get(ArchiveJob, (video_id, "webpage"))
        assert job is not None and job.status == "failed"
    assert state.claim_archive(lane="upload", prefer_recent=False) is not None
    state.close()


@pytest.mark.parametrize("problem", ["integrity", "foreign_keys", "backup"])
def test_migration_checks_reject_failed_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    state.close()
    monkeypatch.setattr(
        migration, "_create_backup_sync", lambda *args, **kwargs: state.path
    )

    class Connection:
        def close(self) -> None:
            pass

        def backup(self, target: Any) -> None:
            pass

        def execute(self, query: str) -> Any:
            return SimpleNamespace(
                fetchone=lambda: ("bad",) if problem != "foreign_keys" else ("ok",),
                fetchall=lambda: [("foreign key error",)],
            )

    monkeypatch.setattr(sqlite3, "connect", lambda *args: Connection())
    with pytest.raises(sqlite3.DatabaseError):
        if problem == "backup":
            state._backup_before_migration()
        else:
            migration.rehearse_migration(state.path, tmp_path)


@pytest.mark.parametrize("marker", ["invalid", "wrong_size", "empty", "missing"])
def test_download_completion_marker_is_required_and_finalized_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marker: str
) -> None:
    output = tmp_path / "media.mkv"
    output.write_bytes(b"" if marker == "empty" else b"interrupted")
    (tmp_path / "merging.mkv").write_bytes(b"interrupted merge")
    if marker != "missing":
        (tmp_path / "download.json").write_text(
            "invalid"
            if marker == "invalid"
            else json.dumps({"video_id": "abcdefghijk", "size": 0}),
            encoding="utf-8",
        )

    class Downloader:
        def __init__(self, options: Any) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def download(self, urls: Any) -> int:
            assert not output.exists()
            assert not (tmp_path / "merging.mkv").exists()
            Path(
                self.options["outtmpl"]["default"].replace("%(ext)s", "mkv")
            ).write_bytes(b"complete")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", Downloader)
    path = youtube.YouTubeClient().download_video(
        "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=1
    )
    assert path.read_bytes() == b"complete"
    assert json.loads((tmp_path / "download.json").read_text()) == {
        "video_id": "abcdefghijk",
        "size": 8,
    }


@pytest.mark.parametrize("callback", [False, True])
def test_download_guard_reserves_merge_space_as_stream_sizes_grow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, callback: bool
) -> None:
    updates: list[tuple[int, int]] = []

    class Downloader:
        def __init__(self, options: Any) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def download(self, urls: Any) -> int:
            hook = self.options["progress_hooks"][0]
            hook({"filename": "video", "downloaded_bytes": 5, "total_bytes": 100})
            Path(
                self.options["outtmpl"]["default"].replace("%(ext)s", "mkv")
            ).write_bytes(b"complete")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", Downloader)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: SimpleNamespace(free=50)
    )
    client = youtube.YouTubeClient()
    if callback:
        client.download_video(
            "abcdefghijk",
            tmp_path,
            minimum_free_bytes=0,
            estimated_bytes=1,
            disk_guard=lambda expected, downloaded: updates.append(
                (expected, downloaded)
            ),
        )
        assert updates == [(100, 5)]
    else:
        with pytest.raises(youtube.DiskSpaceError):
            client.download_video(
                "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=1
            )
