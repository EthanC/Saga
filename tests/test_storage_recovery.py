import json
import os
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from core import staging
from core.staging import maintain_partials
from core.state import StateStore
from core.storage import data_usage, remaining_data_bytes
from tests.test_archival import saved, seed_upload, settings
from tests.test_partials import partial


def test_default_cleanup_breaks_budget_deadlock_and_keeps_originals(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    terminal = partial(store, tmp_path, "GoneVideo12")
    store.update_archive(
        "GoneVideo12", status="unavailable", last_error="Video unavailable"
    )
    retryable = partial(store, tmp_path, "ResumeVid12")
    terminal.write_bytes(b"t" * 400000)
    retryable.write_bytes(b"r" * 400000)
    os.utime(terminal, (1, 1))
    os.utime(retryable, (1, 1))
    upload_id, media, _ = seed_upload(store, tmp_path, "UploadVid12")
    store.update_archive(upload_id, status="reconcile_upload")
    configured = replace(
        settings(tmp_path),
        partial_min_age=1,
        max_data_bytes=data_usage(tmp_path) - 500000,
    )
    assert configured.partial_retention_mode == "auto"
    assert (
        store.claim_archive(
            lane="download",
            prefer_recent=False,
            free_bytes=1000,
            data_budget_bytes=remaining_data_bytes(tmp_path, configured.max_data_bytes),
            download_root=configured.download_root,
        )
        is None
    )
    assert store.download_pause_reason == "data budget full"
    result = maintain_partials(store, configured, now=time.time() + 2)
    assert [d["video_id"] for d in result["decisions"]] == [
        "GoneVideo12",
        "ResumeVid12",
    ]
    assert result["reclaimed_bytes"] == 800000 and not result["waiting_reason"]
    assert (
        not terminal.exists()
        and not retryable.exists()
        and media.read_bytes() == b"test"
    )
    assert saved(store, "GoneVideo12").last_error == "Video unavailable"
    work = store.claim_archive(
        lane="download",
        prefer_recent=False,
        free_bytes=1000,
        data_budget_bytes=remaining_data_bytes(tmp_path, configured.max_data_bytes),
        download_root=configured.download_root,
    )
    assert work is not None and work.video.id == "ResumeVid12"
    store.close()


def test_filesystem_pressure_reclaims_below_partial_retention_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    configured = replace(settings(tmp_path), partial_min_age=1)
    monkeypatch.setattr(staging.shutil, "disk_usage", lambda _: SimpleNamespace(free=2))
    result = maintain_partials(store, configured)
    assert result["pressure_bytes"] == 6 and result["reclaimed_bytes"] == 7
    assert not path.exists()
    store.close()


@pytest.mark.parametrize("contract", ["invalid", "[]", '{"estimated_bytes":"bad"}'])
def test_pressure_uses_fallback_when_a_saved_estimate_is_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contract: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    store.update_archive(
        "abcdefghijk", frozen_metadata=contract, download_estimated_bytes=None
    )
    monkeypatch.setattr(staging.shutil, "disk_usage", lambda _: SimpleNamespace(free=2))
    result = maintain_partials(store, replace(settings(tmp_path), partial_min_age=0))
    assert result["pressure_bytes"] == 6 and result["reclaimed_bytes"] == 7
    assert not path.exists()
    store.close()


@pytest.mark.parametrize(
    "protection", ["active", "young", "manifest", "original", "observe"]
)
def test_pressure_does_not_bypass_input_protection(
    tmp_path: Path, protection: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    configured = replace(
        settings(tmp_path), max_data_bytes=1, partial_max_bytes=0, partial_min_age=1
    )
    if protection == "active":
        row = saved(store, "abcdefghijk")
        assert row.operation_id is not None
        assert store.claim_partial_maintenance(row.video_id, row.operation_id)
    elif protection == "young":
        os.utime(path, None)
    elif protection == "manifest":
        (path.parent / "inputs.json").write_text("{}")
    elif protection == "original":
        media = path.parent / "media.mkv"
        media.write_bytes(b"complete")
        (path.parent / "download.json").write_text(
            json.dumps({"video_id": "abcdefghijk", "size": 8})
        )
    else:
        configured = replace(configured, partial_retention_mode="observe")
    maintain_partials(store, configured)
    assert path.exists()
    store.close()
