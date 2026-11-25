import hashlib
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

import niquests
import pytest
from archivist.services.internet_archive import _multipart
from niquests import Response
from niquests.adapters import HTTPAdapter
from niquests.structures import CaseInsensitiveDict
from sqlmodel import Session

from core import archival, config
from core.archival import ArchiveProcessor, FrozenArchiveMetadata
from core.archive_errors import failure_facts
from core.state import ArchiveJob, ArchiveProgress, StateStore
from core.youtube import YouTubeClient
from tests.test_archival import run_job, saved, seed_upload, settings

OWNER = "owner@example.com"
MEDIA = b"abcdef"
STEPS = ["csrf", "login", "upload-key", "upload-limit", "item-create", "initiate"]


class Crash(BaseException):
    """Stop mid-request the way a killed worker would, skipping all handlers."""


class FakeArchive:
    """Keep IA-S3 items and open multipart uploads behind the HTTP adapter."""

    def __init__(self, frozen: FrozenArchiveMetadata) -> None:
        self.frozen = frozen
        self.items: dict[str, dict[str, Any]] = {}
        self.uploads: dict[str, dict[int, bytes]] = {}
        self.history: list[dict[str, Any]] = []
        self.calls: list[str] = []
        self.headers: dict[str, dict[str, str]] = {}
        self.faults: dict[str, Any] = {}
        self.issued = itertools.count(1)

    def fault(self, label: str, outcome: Any) -> None:
        """Fail the next request with this label once."""
        self.faults[label] = outcome

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        archive = self

        def send(self: Any, request: Any, *args: Any, **kwargs: Any) -> Response:
            return archive.send(request)

        monkeypatch.setattr(HTTPAdapter, "send", send)

    def send(self, request: Any) -> Response:
        url = urlsplit(request.url)
        params = dict(parse_qsl(url.query, keep_blank_values=True))
        label, handler = self.route(request.method, url.netloc, url.path, params)
        self.calls.append(label)
        self.headers[label] = dict(request.headers)
        fault = self.faults.pop(label, None)
        if fault == "network":
            raise niquests.exceptions.ConnectionError("connection reset")
        if fault == "crash":
            raise Crash
        if isinstance(fault, tuple):
            status, code = fault
            return self.error(request, status, code)
        if fault == "vanish":
            # The upload ID disappears without completing, and the reply is lost.
            self.uploads.pop(params["uploadId"])
            raise niquests.exceptions.ConnectionError("response lost")
        reply = handler(request, url.path, params)
        if fault == "lost":
            raise niquests.exceptions.ConnectionError("response lost")
        return reply

    def route(
        self, method: str, host: str, path: str, params: dict[str, str]
    ) -> tuple[str, Any]:
        if host == "s3.us.archive.org":
            names = [unquote(part) for part in path.strip("/").split("/", 1) if part]
            if not names:
                return "upload-limit", lambda *_: self.json(_[0], {"over_limit": 0})
            if len(names) == 1:
                return "item-create", self.create
            if "uploads" in params:
                return "initiate", self.initiate
            if "partNumber" in params:
                return f"part:{params['partNumber']}", self.part
            if "uploadId" not in params:
                return "file-upload", self.whole
            return (
                ("list", self.listing) if method == "GET" else ("complete", self.done)
            )
        if path.startswith("/metadata/"):
            return "metadata", self.metadata
        simple = {
            "/services/csrf-token": (
                "csrf",
                {"success": True, "value": {"token": "token"}},
            ),
            "/services/account/login/": ("login", {"success": True}),
            "/services/user.php": (
                "whoami",
                {"success": True, "value": {"email": OWNER}},
            ),
            "/services/tasks.php": (
                "tasks",
                {"success": True, "value": {"catalog": [], "history": self.history}},
            ),
            "/upload/app/upload_api.php": ("catalog", {"success": True, "rows": []}),
        }
        if path == "/upload":
            return "upload-key", lambda request, *_: self.reply(
                request,
                200,
                """<input type="hidden" class="js-uploader-args" value='"""
                """{"s3user":{"s3accesskey":"key","s3secretkey":"secret"}}'>""",
                "text/html",
            )
        if method == "POST" and path == "/upload/app/upload_api.php":
            return "identifier-check", lambda request, *_: self.json(
                request, {"success": True, "identifier": self.frozen.identifier}
            )
        label, payload = simple[path]
        return label, lambda request, *_: self.json(request, payload)

    def create(self, request: Any, path: str, params: dict[str, str]) -> Response:
        self.items[self.frozen.identifier] = {
            "metadata": {
                "identifier": self.frozen.identifier,
                "uploader": OWNER,
                "source": self.frozen.source_url,
                "saga_operation_id": self.frozen.operation_id,
            },
            "files": [],
        }
        return self.reply(request, 200, "")

    def initiate(self, request: Any, path: str, params: dict[str, str]) -> Response:
        if self.frozen.identifier not in self.items:
            return self.error(request, 404, "NoSuchBucket")
        upload_id = f"upload-{next(self.issued)}"
        self.uploads[upload_id] = {}
        return self.xml(
            request,
            "InitiateMultipartUploadResult",
            f"<UploadId>{upload_id}</UploadId>",
        )

    def part(self, request: Any, path: str, params: dict[str, str]) -> Response:
        parts = self.uploads.get(params["uploadId"])
        if parts is None:
            return self.error(request, 404, "NoSuchUpload")
        body = b"".join(request.body)
        parts[int(params["partNumber"])] = body
        reply = self.reply(request, 200, "")
        reply.headers["ETag"] = f'"{hashlib.md5(body).hexdigest()}"'
        return reply

    def whole(self, request: Any, path: str, params: dict[str, str]) -> Response:
        self.uploads["whole"] = {1: b"".join(request.body)}
        self.done(request, path, {"uploadId": "whole"})
        return self.reply(request, 200, "")

    def listing(self, request: Any, path: str, params: dict[str, str]) -> Response:
        parts = self.uploads.get(params["uploadId"])
        if parts is None:
            return self.error(request, 404, "NoSuchUpload")
        listed = "".join(
            f"<Part><PartNumber>{number}</PartNumber>"
            f'<ETag>"{hashlib.md5(body).hexdigest()}"</ETag>'
            f"<Size>{len(body)}</Size></Part>"
            for number, body in sorted(parts.items())
        )
        return self.xml(
            request,
            "ListPartsResult",
            f"<UploadId>{params['uploadId']}</UploadId>"
            f"<IsTruncated>false</IsTruncated>{listed}",
        )

    def done(self, request: Any, path: str, params: dict[str, str]) -> Response:
        upload_id = params["uploadId"]
        parts = self.uploads.pop(upload_id, None)
        if parts is None:
            return self.error(request, 404, "NoSuchUpload")
        data = b"".join(body for _, body in sorted(parts.items()))
        self.items[self.frozen.identifier]["files"].append(
            {
                "name": self.frozen.media_name,
                "source": "original",
                "size": str(len(data)),
                "md5": hashlib.md5(data).hexdigest(),
                "sha1": hashlib.sha1(data).hexdigest(),
            }
        )
        self.history.append(
            {
                "args": {
                    "comment": "s3-put-complete-multipart",
                    "complete_multipart_upload_id": upload_id,
                },
                "status": "done",
            }
        )
        return self.xml(request, "CompleteMultipartUploadResult", "")

    def metadata(self, request: Any, path: str, params: dict[str, str]) -> Response:
        return self.json(request, self.items.get(self.frozen.identifier, {}))

    def sent(self) -> list[str]:
        """Return the requests after the pre-write limit check."""
        return self.calls[self.calls.index("upload-limit") + 1 :]

    def stored(self) -> bytes | None:
        files = self.items.get(self.frozen.identifier, {}).get("files", [])
        return MEDIA if files and files[0]["size"] == str(len(MEDIA)) else None

    def xml(self, request: Any, root: str, inner: str) -> Response:
        return self.reply(
            request,
            200,
            f"<{root}><Bucket>{self.frozen.identifier}</Bucket>"
            f"<Key>{self.frozen.media_name}</Key>{inner}</{root}>",
            "application/xml",
        )

    def error(self, request: Any, status: int, code: str) -> Response:
        return self.reply(
            request,
            status,
            f"<Error><Code>{code}</Code><Message>{code} reply</Message>"
            "<RequestId>request</RequestId></Error>",
            "application/xml",
        )

    def json(self, request: Any, value: Any) -> Response:
        return self.reply(request, 200, json.dumps(value), "application/json")

    def reply(
        self, request: Any, status: int, body: str, kind: str = "text/plain"
    ) -> Response:
        response = Response()
        response.status_code = status
        response.headers = CaseInsensitiveDict({"Content-Type": kind})
        response.url = request.url
        response.request = request
        response.raw = SimpleNamespace(_original_response=None)
        response._content = body.encode()
        return response


def seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[StateStore, str, Path, FakeArchive]:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    media.write_bytes(MEDIA)
    store.update_archive(video_id, media_size=len(MEDIA))
    archive = FakeArchive(frozen)
    archive.install(monkeypatch)
    return store, video_id, media, archive


def attempt(store: StateStore, tmp_path: Path, **values: Any) -> None:
    """Claim the upload lane once with multipart enabled at two-byte parts."""
    options = {
        "archive_multipart_threshold": 1,
        "archive_multipart_part_size": 2,
        **values,
    }
    processor = ArchiveProcessor(settings(tmp_path, **options), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)


def resume_state(store: StateStore, video_id: str) -> dict[str, Any]:
    return json.loads(saved(store, video_id).multipart_state or "")


def test_large_original_uploads_in_parts_and_reports_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    clock = itertools.count(step=10)
    monkeypatch.setattr(
        archival, "time", SimpleNamespace(monotonic=lambda: next(clock))
    )
    updates: list[dict[str, Any]] = []
    original = store.update_archive

    def update(video: str, **values: Any) -> None:
        updates.append(values)
        original(video, **values)

    monkeypatch.setattr(store, "update_archive", update)
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert archive.calls == [*STEPS, "part:1", "part:2", "part:3", "complete"]
    assert archive.stored() == MEDIA and media.exists()
    assert row.status == "complete" and row.media_uploaded_at is not None
    assert row.multipart_state is None and row.upload_etag is None
    assert (row.media_md5, row.media_sha1) == (
        hashlib.md5(MEDIA).hexdigest(),
        hashlib.sha1(MEDIA).hexdigest(),
    )
    # Hashing refreshes liveness only; byte counts come from part reads.
    kinds = [
        "heartbeat"
        if set(values) == {"transfer_progress_at"}
        else "bytes"
        if "transfer_bytes" in values
        else "state"
        if "multipart_state" in values
        else "other"
        for values in updates
    ]
    first_state = kinds.index("state")
    assert "heartbeat" in kinds[:first_state]
    assert "bytes" not in kinds[kinds.index("heartbeat") : first_state]
    sent = [
        values["transfer_bytes"] for values in updates if "transfer_bytes" in values
    ]
    assert sent[0] == 0 and sent[1:] == sorted(sent[1:]) and 0 < max(sent) <= len(MEDIA)
    assert archive.headers["complete"]["x-archive-interactive-priority"] == "1"
    # The default defers the derive, so the completing write opts out.
    assert archive.headers["complete"]["x-archive-queue-derive"] == "0"
    assert row.derive_policy == "deferred" and row.derive_status == "pending"
    store.close()


def test_originals_below_the_threshold_keep_the_single_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    attempt(store, tmp_path, archive_multipart_threshold=len(MEDIA) + 1)
    assert archive.calls[-2:] == ["item-create", "file-upload"]
    row = saved(store, video_id)
    assert row.last_error is None, row.last_error
    assert row.media_uploaded_at is not None and row.media_md5 is None
    store.close()


@pytest.mark.parametrize("derive", ["deferred", "immediate", "never"])
def test_settings_reach_every_multipart_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, derive: str
) -> None:
    store, _, _, archive = seed(tmp_path, monkeypatch)
    attempt(store, tmp_path, archive_derive=derive, archive_interactive_priority=False)
    writes = ["item-create", "initiate", "part:1", "part:2", "part:3", "complete"]
    assert all(
        "x-archive-interactive-priority" not in archive.headers[label]
        for label in writes
    )
    if derive == "immediate":
        assert "x-archive-queue-derive" not in archive.headers["complete"]
    else:
        assert archive.headers["complete"]["x-archive-queue-derive"] == "0"
    store.close()


def test_large_originals_grow_parts_to_fit_the_part_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    monkeypatch.setattr(archival, "MULTIPART_MAX_PARTS", 2)
    attempt(store, tmp_path, archive_multipart_part_size=1)
    assert archive.calls[-3:] == ["part:1", "part:2", "complete"]
    assert saved(store, video_id).status == "complete"
    store.close()


def test_interruption_between_parts_resumes_without_resending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    archive.fault("part:2", "network")
    attempt(store, tmp_path)
    row = saved(store, video_id)
    state = resume_state(store, video_id)
    assert row.status == "upload" and row.media_uploaded_at is None and media.exists()
    assert state["phase"] == "initiated" and [part[0] for part in state["parts"]] == [1]
    assert (row.error_kind, row.error_phase) == ("network", "multipart-part")
    store.update_archive(video_id, release=True, retry_after=0)
    archive.calls.clear()
    # The saved state wins even after multipart is switched off.
    attempt(store, tmp_path, archive_multipart_threshold=0)
    assert archive.sent() == ["list", "part:2", "part:3", "complete"]
    assert saved(store, video_id).media_uploaded_at is not None
    assert archive.stored() == MEDIA
    store.close()


def test_lost_part_acknowledgement_is_reconciled_from_the_part_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    archive.fault("part:2", "lost")
    attempt(store, tmp_path)
    assert [part[0] for part in resume_state(store, video_id)["parts"]] == [1]
    store.update_archive(video_id, release=True, retry_after=0)
    archive.calls.clear()
    attempt(store, tmp_path)
    assert archive.sent() == ["list", "part:3", "complete"]
    assert saved(store, video_id).media_uploaded_at is not None
    store.close()


@pytest.mark.parametrize("fault", ["network", "lost"])
def test_completion_failure_retries_only_after_checking_the_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    archive.fault("complete", fault)
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert row.status == "upload" and row.media_uploaded_at is None and media.exists()
    assert resume_state(store, video_id)["phase"] == "completing"
    store.update_archive(video_id, release=True, retry_after=0)
    archive.calls.clear()
    attempt(store, tmp_path)
    if fault == "network":
        # The upload is still open, so completing it again is safe.
        assert archive.sent() == ["list", "complete"]
    else:
        # The upload ID is gone; the item listing proves the completion.
        assert archive.sent() == ["list", "metadata"]
    row = saved(store, video_id)
    assert row.media_uploaded_at is not None and row.multipart_state is None
    assert archive.calls.count("complete") <= 1 and archive.stored() == MEDIA
    store.close()


def test_unproven_completion_goes_to_guarded_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    archive.fault("complete", "vanish")
    attempt(store, tmp_path)
    store.update_archive(video_id, release=True, retry_after=0)
    archive.calls.clear()
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert archive.sent() == ["list", "metadata", "tasks"]
    assert row.status == "reconcile_upload" and row.multipart_state is None
    assert row.error_phase == "multipart-list" and media.exists()
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "pending"
    store.close()


def test_expired_upload_is_reconciled_instead_of_resumed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    archive.fault("part:2", "network")
    attempt(store, tmp_path)
    archive.uploads.clear()
    store.update_archive(video_id, release=True, retry_after=0)
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert row.status == "reconcile_upload" and row.multipart_state is None
    assert row.error_code == "NoSuchUpload"
    store.close()


@pytest.mark.parametrize(
    "label,status,saved_phase",
    [
        ("initiate", "reconcile_upload", None),
        ("part:2", "upload", "initiated"),
        ("complete", "upload", "completing"),
    ],
)
def test_slowdown_at_each_multipart_step_keeps_the_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    status: str,
    saved_phase: str | None,
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    archive.fault(label, (503, "SlowDown"))
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert row.status == status and media.exists() and row.retry_at is not None
    assert (row.error_kind, row.error_code, row.error_phase) == (
        "rate_limit",
        "SlowDown",
        "multipart-" + label.split(":")[0],
    )
    assert json.loads(row.error_queue or "") == {"over_limit": 0}
    state = json.loads(row.multipart_state) if row.multipart_state else None
    assert (state["phase"] if state else None) == saved_phase
    store.close()


@pytest.mark.parametrize(
    "status,expected", [(400, "upload"), (503, "reconcile_upload")]
)
def test_item_creation_failures_follow_the_single_transfer_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int, expected: str
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    archive.fault("item-create", (status, "InvalidArgument"))
    attempt(store, tmp_path)
    row = saved(store, video_id)
    assert row.status == expected and row.error_phase == "item-create"
    assert row.multipart_state is None and "initiate" not in archive.calls
    store.close()


def test_restart_mid_upload_resumes_parts_saved_in_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, media, archive = seed(tmp_path, monkeypatch)
    archive.fault("part:3", "crash")
    # A killed process never reaches the worker's own release step.
    monkeypatch.setattr(store, "finish_archive_request", lambda *_, **__: None)
    with pytest.raises(Crash):
        attempt(store, tmp_path)
    assert saved(store, video_id).status == "uploading"
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    assert [part[0] for part in resume_state(store, video_id)["parts"]] == [1, 2]
    processor = ArchiveProcessor(
        settings(
            tmp_path, archive_multipart_threshold=1, archive_multipart_part_size=2
        ),
        store,
        YouTubeClient(),
    )
    store.reset_interrupted_claims()
    # Startup sends the interrupted transfer through remote reconciliation;
    # open-upload spool parts are not originals, so the original is missing.
    assert saved(store, video_id).status == "reconcile_upload"
    run_job(store, processor, "recovery")
    row = saved(store, video_id)
    assert row.status == "upload" and row.upload_mode == "recover"
    archive.calls.clear()
    attempt(store, tmp_path)
    assert archive.sent() == ["list", "part:3", "complete"]
    row = saved(store, video_id)
    assert row.media_uploaded_at is not None and archive.stored() == MEDIA
    assert media.exists()
    store.close()


def test_recovery_mode_sends_a_fresh_multipart_upload_to_the_owned_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, archive = seed(tmp_path, monkeypatch)
    archive.fault("initiate", (503, "SlowDown"))
    attempt(store, tmp_path)
    store.update_archive(
        video_id, status="upload", upload_mode="recover", release=True, retry_after=0
    )
    archive.calls.clear()
    attempt(store, tmp_path)
    assert archive.sent()[:3] == ["whoami", "metadata", "catalog"]
    assert archive.calls[-4:] == ["part:1", "part:2", "part:3", "complete"]
    assert "item-create" not in archive.calls
    assert saved(store, video_id).media_uploaded_at is not None
    store.close()


def test_rows_from_before_multipart_load_and_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, video_id, _, _ = seed(tmp_path, monkeypatch)
    with store.engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE archiveprogress DROP COLUMN multipart_state"
        )
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    with Session(store.engine) as session:
        row = session.get(ArchiveProgress, video_id)
        assert row is not None and row.multipart_state is None
    attempt(store, tmp_path)
    assert saved(store, video_id).media_uploaded_at is not None
    store.close()


def test_multipart_failures_expose_their_cause_for_alerts() -> None:
    from archivist import InternetArchiveMultipartError, ServiceError

    cause = ServiceError(
        "denied",
        service="Internet Archive",
        status_code=403,
        service_code="AccessDenied",
        request_phase="multipart-part",
    )
    facts = failure_facts(InternetArchiveMultipartError(None, cause))
    assert (facts["error_kind"], facts["error_phase"]) == (
        "permission",
        "multipart-part",
    )


def test_multipart_limits_match_the_pinned_archivist() -> None:
    assert config.MULTIPART_MAX_PART_SIZE == _multipart.MAX_PART_SIZE
    assert config.MULTIPART_MAX_PARTS == _multipart.MAX_PARTS
