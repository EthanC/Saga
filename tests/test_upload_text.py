from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    InternetArchiveClient,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadLimit,
    InternetArchiveUploadResult,
    InvalidOptionError,
)

from core.archival import ArchiveGateway, ArchiveProcessor
from core.state import StateStore
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings


@pytest.mark.parametrize("empty", [False, True])
def test_upload_cleans_invalid_display_text_without_changing_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    bad = "\x00\ud800"
    frozen = replace(
        frozen,
        subjects=(bad,) if empty else (bad, "tag\nvalue", "tagvalue"),
        title=bad if empty else "Title\x00",
        creator=bad if empty else "Name\x00",
        description=bad if empty else "First\x00\nSecond\tline",
    )
    store.update_archive(video_id, frozen_metadata=frozen.to_json())

    def upload(
        self: Any, files: Any, options: Any, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        assert options.identifier == frozen.identifier
        assert options.metadata["saga_operation_id"] == frozen.operation_id
        assert options.metadata["source"] == frozen.source_url
        assert options.subjects == (["youtube"] if empty else ["tagvalue"])
        assert options.title == (frozen.identifier if empty else "Title")
        assert options.creator == (frozen.channel_username if empty else "Name")
        assert options.description == (
            frozen.source_url if empty else "First\nSecond\tline"
        )
        return InternetArchiveUploadResult(
            frozen.identifier,
            (InternetArchiveUploadFileResult(frozen.media_name, 4, transferred=True),),
        )

    monkeypatch.setattr(InternetArchiveClient, "upload", upload)
    monkeypatch.setattr(
        InternetArchiveClient,
        "upload_limit",
        lambda *args: InternetArchiveUploadLimit(over_limit=False),
    )
    processor = ArchiveProcessor(
        settings(tmp_path), store, YouTubeClient(), ArchiveGateway(settings(tmp_path))
    )
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).media_uploaded_at and media.exists()
    store.close()


def test_startup_revives_upload_rejected_for_control_characters(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    store.update_archive(
        video_id,
        status="operator_action",
        failure_phase="upload",
        last_error="InvalidOptionError: subjects cannot contain controls or surrogates",
    )
    unrelated, _, _ = seed_upload(store, tmp_path, "Conflict123")
    store.update_archive(
        unrelated,
        status="operator_action",
        failure_phase="upload",
        last_error="RecoveryConflictError: ownership mismatch",
    )
    store.reset_interrupted_claims()
    row = saved(store, video_id)
    assert (
        row.status == "upload"
        and row.operation_id == frozen.operation_id
        and media.exists()
    )
    assert saved(store, unrelated).status == "operator_action"
    store.close()


def test_other_invalid_options_retain_original_for_review(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = InvalidOptionError("identifier is invalid")
    processor = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), gateway)
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(store, video_id).status == "operator_action" and media.exists()
    store.close()
