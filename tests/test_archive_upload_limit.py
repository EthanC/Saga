import json
from pathlib import Path
from typing import Any

import pytest
from archivist import RateLimitError
from niquests import Response

from core import archival
from core.archival import ArchiveGateway, ArchiveProcessor
from core.archive_errors import failure_facts
from core.state import StateStore
from core.youtube import YouTubeClient
from tests.test_archival import saved, seed_upload, settings


def wire(
    monkeypatch: pytest.MonkeyPatch, identifier: str, status: int, limit: Any
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
        response._content = (
            content.encode()
            if isinstance(content, str)
            else json.dumps(content).encode()
        )
        return response

    monkeypatch.setattr(archival.Session, "request", request)
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
    "status,limit", [(200, {"over_limit": 0}), (200, "not json"), (503, "")]
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
