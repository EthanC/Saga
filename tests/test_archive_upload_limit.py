import json
import pickle
from pathlib import Path
from typing import Any

import pytest
from archivist import RateLimitError
from niquests import Response

from core import archival
from core.archival import ArchiveGateway, ArchiveProcessor
from core.archive_errors import failure_facts, queue_snapshot
from core.state import StateStore
from core.youtube import YouTubeClient
from tests.test_archival import route_sends, saved, seed_upload, settings

SLOW_DOWN = (
    "<Error><Code>SlowDown</Code><Message>Please reduce your request rate."
    "</Message><RequestId>request</RequestId></Error>"
)
QUEUE = {
    "accesskey_ration": 499,
    "accesskey_tasks_queued": 12,
    "bucket_limit": 999,
    "bucket_ration": 149,
    "bucket_tasks_queued": 0,
    "limit_reason": "",
    "rationing_engaged": 1,
    "rationing_level": 9999,
    "total_global_limit": 11999,
    "total_tasks_queued": 10202,
    "internal_unlisted": "dropped",
}


def wire(
    monkeypatch: pytest.MonkeyPatch,
    identifier: str,
    status: int,
    limit: Any,
    create_status: int = 200,
) -> list[str]:
    puts: list[str] = []

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        response = Response()
        response.status_code = 200
        content: Any = ""
        if url.endswith("csrf-token"):
            content = {"success": True, "value": {"token": "token"}}
        elif url.endswith("login/"):
            content = {"success": True}
        elif url == "https://archive.org/upload":
            content = (
                '<input type="hidden" class="js-uploader-args" value=\''
                '{"s3user":{"s3accesskey":"key","s3secretkey":"secret"}}\'>'
            )
        elif method == "POST":
            content = {"success": True, "identifier": identifier}
        elif url == "https://s3.us.archive.org":
            assert kwargs["params"] == {
                "check_limit": "1",
                "accesskey": "key",
                "bucket": identifier,
            }
            assert "Authorization" not in kwargs["headers"]
            response.status_code = status
            content = limit
        else:
            puts.append(url)
            response.headers["ETag"] = "etag"
            if url.endswith(identifier) and create_status != 200:
                response.status_code = create_status
                content = SLOW_DOWN
        response._content = (
            content.encode()
            if isinstance(content, str)
            else json.dumps(content).encode()
        )
        return response

    monkeypatch.setattr(archival.Session, "request", request)
    route_sends(monkeypatch, request)
    return puts


@pytest.mark.parametrize(
    "limit,reason",
    [
        ({"over_limit": 1, "detail": {"limit_reason": "tasks_queued"}}, "tasks_queued"),
        ({"over_limit": 1, "detail": {"limit_reason": ""}}, "over_limit"),
        ({"over_limit": 1}, "over_limit"),
    ],
)
def test_full_task_queue_defers_upload_before_creating_the_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: Any, reason: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, limit)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert puts == [] and media.exists()
    assert row.status == "upload" and row.retry_at and row.claimed_at is None
    assert (row.error_kind, row.error_phase, row.error_code) == (
        "rate_limit",
        "item-create",
        "SlowDown",
    )
    assert row.error_message == f"Upload deferred before sending: {reason}"
    store.close()


@pytest.mark.parametrize(
    "status,limit", [(200, {"over_limit": 0}), (200, {}), (200, "not json"), (503, "")]
)
def test_clear_or_unreadable_limit_check_lets_the_upload_proceed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int, limit: Any
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, status, limit)
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).upload_etag == "etag"
    assert puts == [
        f"https://s3.us.archive.org/{frozen.identifier}",
        f"https://s3.us.archive.org/{frozen.identifier}/{frozen.media_name}",
    ]
    store.close()


def test_full_task_queue_defers_recovery_transfer_before_any_metadata_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 1})
    gateway = ArchiveGateway(settings(tmp_path))
    with pytest.raises(RateLimitError) as result:
        gateway.recover(frozen, media, progress=lambda value: None)
    assert puts == [] and media.exists()
    assert failure_facts(result.value)["error_phase"] == "file-upload"
    store.close()


def test_rejected_write_keeps_the_queue_counters_seen_before_sending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    wire(
        monkeypatch,
        frozen.identifier,
        200,
        {"over_limit": 0, "detail": QUEUE},
        create_status=503,
    )
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert (row.error_kind, row.error_phase, row.error_code) == (
        "rate_limit",
        "item-create",
        "SlowDown",
    )
    expected = {
        key: value
        for key, value in QUEUE.items()
        if key not in {"limit_reason", "internal_unlisted"}
    }
    assert json.loads(row.error_queue or "") == {"over_limit": 0, **expected}
    assert media.exists()
    store.close()


def test_over_limit_deferral_keeps_reason_and_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    detail = {**QUEUE, "limit_reason": "accesskey"}
    wire(monkeypatch, frozen.identifier, 200, {"over_limit": 1, "detail": detail})
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    queue = json.loads(saved(store, video_id).error_queue or "")
    assert queue == {
        **{
            key: value
            for key, value in QUEUE.items()
            if key not in {"limit_reason", "internal_unlisted"}
        },
        "over_limit": 1,
        "limit_reason": "accesskey",
    }
    store.close()


def test_failed_limit_check_warns_and_leaves_no_queue_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, frozen.identifier, 503, "", create_status=503)
    messages: list[str] = []
    handler = archival.logger.add(messages.append, level="WARNING")
    try:
        processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
        work = store.claim_archive(lane="upload", prefer_recent=False)
        assert work is not None
        processor.process(work)
    finally:
        archival.logger.remove(handler)
    row = saved(store, video_id)
    assert row.error_code == "SlowDown" and row.error_queue is None
    assert any("upload-limit check failed" in message for message in messages)
    store.close()


def test_queue_snapshots_survive_worker_pickling_and_reject_unusable_detail() -> None:
    error = RateLimitError("slow down", service_code="SlowDown")
    error.__dict__["upload_queue"] = {"over_limit": 0, "total_tasks_queued": 5}
    restored = pickle.loads(pickle.dumps(error))
    assert json.loads(failure_facts(restored)["error_queue"]) == {
        "over_limit": 0,
        "total_tasks_queued": 5,
    }
    assert queue_snapshot([]) is None
    assert queue_snapshot({"over_limit": "0", "limit_reason": ""}) is None
