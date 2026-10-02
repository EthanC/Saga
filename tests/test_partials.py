import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlmodel import Session

from core.staging import (
    StagingIdentityError,
    inventory_inputs,
    maintain_partials,
    prepare_inputs,
)
from core.state import ArchiveProgress, StateStore, utcnow
from core.youtube import _staging_downloader
from tests.test_archival import saved, seed_upload, settings

FORMATS = [{"format_id": "315", "ext": "webm", "protocol": "https"}]


def partial(
    store: StateStore,
    root: Path,
    identifier: str = "abcdefghijk",
    *,
    legacy: bool = False,
) -> Path:
    video_id, media, frozen = seed_upload(store, root, identifier)
    media.unlink()
    directory = media.parent
    store.update_archive(
        video_id,
        status="download",
        download_completed_at=None,
        ready_at=None,
        updated_at=utcnow() - timedelta(days=2),
    )
    path = directory / ("media.f315.webm.part" if legacy else "merging.f315.webm.part")
    path.write_bytes(b"partial")
    if not legacy:
        prepare_inputs(directory, frozen.operation_id, video_id, FORMATS)
    os.utime(path, (1, 1))
    with Session(store.engine) as session:
        row = session.get(ArchiveProgress, video_id)
        assert row is not None
        row.updated_at = utcnow() - timedelta(days=2)
        session.add(row)
        session.commit()
    return path


def test_legacy_adoption_moves_sidecar_and_is_idempotent(tmp_path: Path) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    legacy = directory / "media.f315.webm.part"
    legacy.write_bytes(b"reusable")
    (directory / "media.f315.webm.ytdl").write_text(
        '{"downloader":{"current_fragment":{"index":2}}}'
    )
    assert inventory_inputs(directory, "operation", "video").reusable_bytes == 0
    inventory = prepare_inputs(directory, "operation", "video", FORMATS)
    assert inventory.reusable_bytes == 8
    assert (directory / "merging.f315.webm.ytdl").exists()
    assert not legacy.exists() and not (directory / "download.json").exists()
    assert prepare_inputs(directory, "operation", "video", FORMATS) == inventory


@pytest.mark.parametrize(
    "problem",
    ["destination", "format", "sidecar", "manifest", "identity", "invalidformat"],
)
def test_ambiguous_inputs_remain_untouched(tmp_path: Path, problem: str) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    path = directory / "media.f315.webm.part"
    path.write_bytes(b"retain")
    formats = FORMATS
    if problem == "destination":
        (directory / "merging.f315.webm.part").write_bytes(b"other")
    elif problem == "format":
        formats = [{"format_id": "314", "ext": "webm"}]
    elif problem == "sidecar":
        (directory / "media.f315.webm.ytdl").write_text("[]")
    elif problem == "manifest":
        (directory / "inputs.json").write_text("bad")
    elif problem == "invalidformat":
        formats = [{"format_id": "../", "ext": "webm"}]
    with pytest.raises(StagingIdentityError):
        prepare_inputs(
            directory,
            "different" if problem == "identity" else "operation",
            "video",
            formats,
        )
    assert path.read_bytes() == b"retain"


def test_adoption_recovers_crash_between_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    for name in ("media.f315.webm.part", "media.f315.webm.ytdl"):
        (directory / name).write_text('{"downloader":{}}')
    rename = Path.rename
    count = 0

    def interrupted(path: Path, destination: Any) -> Path:
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("crash")
        return rename(path, destination)

    monkeypatch.setattr(Path, "rename", interrupted)
    with pytest.raises(OSError):
        prepare_inputs(directory, "operation", "video", FORMATS)
    monkeypatch.setattr(Path, "rename", rename)
    assert prepare_inputs(directory, "operation", "video", FORMATS).reusable_bytes
    assert len(list(directory.glob("merging.*"))) == 2


def test_resume_dispatch_and_credit_are_refreshed_at_claim(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    identifier, _, _ = seed_upload(store, tmp_path, "Backfill123")
    store.update_archive(identifier, status="download")
    claim = store.claim_archive(
        lane="download",
        prefer_recent=False,
        dispatch_class="resume",
        download_root=tmp_path / "downloads",
        free_bytes=5,
    )
    assert claim is not None and claim.video.id == "abcdefghijk"
    assert claim.progress.reserved_bytes == 4
    store.update_archive(claim.video.id, release=True)
    path.unlink()
    assert (
        store.claim_archive(
            lane="download",
            prefer_recent=False,
            download_root=tmp_path / "downloads",
            free_bytes=5,
        )
        is None
    )
    store.close()


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_retention_observes_or_evicts_only_eligible_inputs(
    tmp_path: Path, mode: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    configured = replace(
        settings(tmp_path),
        partial_max_bytes=0,
        partial_retention_mode=mode,
        partial_min_age=1,
    )
    result = maintain_partials(store, configured)
    assert result["inactive_bytes"] == 7
    assert result["decisions"][0]["action"] == (
        "observe" if mode == "observe" else "evicted"
    )
    assert path.exists() == (mode == "observe")
    row = saved(store, "abcdefghijk")
    assert (
        row.claimed_at is None
        and row.maintenance_token is None
        and row.status == "download"
    )
    if mode == "enforce":
        assert "evicted" in (row.last_error or "")
    store.close()


def test_maintenance_claim_excludes_download_and_recovers_after_restart(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    row = saved(store, "abcdefghijk")
    assert row.operation_id is not None
    token = store.claim_partial_maintenance(row.video_id, row.operation_id)
    assert (
        token
        and store.claim_partial_maintenance(row.video_id, row.operation_id) is None
    )
    assert (
        store.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
        is None
    )
    with pytest.raises(RuntimeError, match="claim changed"):
        store.release_partial_maintenance(row.video_id, "wrong", error="error")
    store.reset_interrupted_claims()
    assert path.exists() and saved(store, row.video_id).maintenance_token is None
    assert store.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
    store.close()


def test_unknown_and_protected_inputs_report_wait_without_removal(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path, legacy=True)
    (tmp_path / "downloads" / "unknown").mkdir()
    result = maintain_partials(
        store,
        replace(
            settings(tmp_path), partial_max_bytes=0, partial_retention_mode="enforce"
        ),
    )
    assert result["findings"] and result["waiting_reason"]
    assert path.exists()
    store.close()


def test_staging_adapter_prepares_before_download() -> None:
    events: list[str] = []

    class Downloader:
        def process_info(self, info: Any) -> None:
            events.append("download")

    wrapped = _staging_downloader(Downloader, lambda info: events.append("prepare"))
    wrapped().process_info({})
    assert events == ["prepare", "download"]
