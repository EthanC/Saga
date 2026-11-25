import json
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    InternetArchiveClient,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
)
from niquests import Response

from core import archival
from core.archival import ArchiveGateway, FrozenArchiveMetadata
from core.state import StateStore
from tests.test_archival import seed_upload, settings

S3 = "https://s3.us.archive.org"


def wire(
    monkeypatch: pytest.MonkeyPatch, frozen: FrozenArchiveMetadata
) -> list[tuple[str, dict[str, str]]]:
    """Answer every Archive request and record each PUT's headers."""
    writes: list[tuple[str, dict[str, str]]] = []

    def respond(method: str, url: str, headers: Any) -> Response:
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
        elif url == S3:
            content = {"over_limit": 0}
        elif url.endswith("services/user.php"):
            content = {"success": True, "value": {"email": "owner@example.com"}}
        elif url.startswith("https://archive.org/metadata/"):
            content = {
                "metadata": {
                    "identifier": frozen.identifier,
                    "uploader": "owner@example.com",
                    "source": frozen.source_url,
                    "saga_operation_id": frozen.operation_id,
                },
                "files": [],
            }
        elif method == "POST":
            content = {"success": True, "identifier": frozen.identifier}
        else:
            assert method == "PUT"
            writes.append((url, dict(headers)))
            response.headers["ETag"] = "etag"
        response._content = (
            content.encode()
            if isinstance(content, str)
            else json.dumps(content).encode()
        )
        return response

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        return respond(method, url, kwargs.get("headers") or {})

    def recover(
        self: Any, identifier: str, file: Any, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        # Archivist owns the guarded PUT; record the headers Saga asks it for.
        headers = {"If-None-Match": "*"}
        if kwargs["interactive_priority"]:
            headers["x-archive-interactive-priority"] = "1"
        if not kwargs["queue_derive"]:
            headers["x-archive-queue-derive"] = "0"
        assert kwargs["expected_metadata"] == {
            "source": frozen.source_url,
            "saga_operation_id": frozen.operation_id,
        }
        writes.append((f"{S3}/{identifier}/{file.name}", headers))
        return InternetArchiveUploadResult(
            identifier,
            (
                InternetArchiveUploadFileResult(
                    file.name, kwargs["expected_size"], transferred=True
                ),
            ),
        )

    monkeypatch.setattr(archival.Session, "request", request)
    monkeypatch.setattr(InternetArchiveClient, "transfer_missing_file", recover)
    return writes


def transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str, **values: Any
) -> tuple[FrozenArchiveMetadata, list[tuple[str, dict[str, str]]]]:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    writes = wire(monkeypatch, frozen)
    gateway = ArchiveGateway(settings(tmp_path, **values))
    result = getattr(gateway, operation)(frozen, media, progress=lambda value: None)
    assert result.files[-1].transferred
    store.close()
    return frozen, writes


def test_default_writes_keep_the_upload_and_recovery_derive_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    frozen, writes = transfer(tmp_path, monkeypatch, "upload")
    item, original = f"{S3}/{frozen.identifier}", f"{S3}/{frozen.identifier}/"
    assert [url for url, _ in writes] == [item, original + frozen.media_name]
    # Creation never derives; the single original queues the item's derive.
    assert writes[0][1]["x-archive-queue-derive"] == "0"
    assert "x-archive-queue-derive" not in writes[1][1]
    _, writes = transfer(tmp_path / "recovery", monkeypatch, "recover")
    assert [url for url, _ in writes] == [original + frozen.media_name]
    assert "x-archive-queue-derive" not in writes[0][1]


@pytest.mark.parametrize("operation", ["upload", "recover"])
def test_disabled_derive_marks_every_upload_and_recovery_put(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    _, writes = transfer(tmp_path, monkeypatch, operation, archive_queue_derive=False)
    assert writes and len(writes) == (2 if operation == "upload" else 1)
    assert all(headers["x-archive-queue-derive"] == "0" for _, headers in writes)


@pytest.mark.parametrize("priority", [True, False])
def test_create_upload_and_recovery_agree_on_interactive_priority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, priority: bool
) -> None:
    _, uploads = transfer(
        tmp_path, monkeypatch, "upload", archive_interactive_priority=priority
    )
    _, recoveries = transfer(
        tmp_path / "recovery",
        monkeypatch,
        "recover",
        archive_interactive_priority=priority,
    )
    writes = uploads + recoveries
    assert len(writes) == 3
    assert all(
        headers.get("x-archive-interactive-priority") == ("1" if priority else None)
        for _, headers in writes
    )
