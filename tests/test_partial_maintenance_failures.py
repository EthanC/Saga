from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from core.staging import (
    StagingIdentityError,
    inventory_inputs,
    maintain_partials,
    prepare_inputs,
)
from core.state import StateStore
from tests.test_archival import saved, settings
from tests.test_partials import FORMATS, partial


def test_failed_removal_releases_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    original = Path.unlink

    def denied(file: Path, *args: Any, **kwargs: Any) -> None:
        if file == path:
            raise PermissionError("fixture")
        original(file, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", denied)
    result = maintain_partials(
        store,
        replace(
            settings(tmp_path),
            partial_retention_mode="enforce",
            partial_max_bytes=0,
            partial_min_age=1,
        ),
    )
    assert (
        result["decisions"][0]["action"] == "partial eviction failed: PermissionError"
    )
    assert saved(store, "abcdefghijk").claimed_at is None
    assert path.exists()
    store.close()


def test_changed_selected_formats_get_no_credit(tmp_path: Path) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    prepare_inputs(directory, "operation", "video", FORMATS)
    (directory / "merging.f315.webm.part").write_bytes(b"preserve")
    with pytest.raises(StagingIdentityError, match="changed"):
        prepare_inputs(
            directory, "operation", "video", [{"format_id": "314", "ext": "webm"}]
        )
    (directory / "inputs.json").write_text(
        '{"operation_id":"other","video_id":"video","formats":{}}'
    )
    assert inventory_inputs(directory, "operation", "video").reusable_bytes == 0


@pytest.mark.parametrize("race", ["claim", "identity"])
def test_retention_rechecks_after_exclusive_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, race: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    original = store.claim_partial_maintenance

    def acquire(video_id: str, operation_id: str) -> str | None:
        if race == "claim":
            assert store.claim_archive(
                lane="download", prefer_recent=False, free_bytes=100
            )
            return None
        token = original(video_id, operation_id)
        (path.parent / "inputs.json").write_text("bad")
        return token

    monkeypatch.setattr(store, "claim_partial_maintenance", acquire)
    result = maintain_partials(
        store,
        replace(
            settings(tmp_path),
            partial_retention_mode="enforce",
            partial_max_bytes=0,
            partial_min_age=1,
        ),
    )
    assert result["decisions"][0]["action"] == (
        "claim conflict" if race == "claim" else "identity or age changed"
    )
    assert path.exists()
    store.close()


def test_retention_pressure_pauses_only_fresh_downloads(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    partial(store, tmp_path, legacy=True)
    assert (
        store.claim_archive(
            lane="download",
            prefer_recent=False,
            free_bytes=100,
            pause_fresh_downloads=True,
            download_root=tmp_path / "downloads",
        )
        is None
    )
    assert store.download_pause_reason == "protected partials exceed retention target"
    store.close()
