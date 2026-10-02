import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from core.service import SagaService
from core.staging import download_reservation, ensure_download_root
from core.state import StateStore
from core.youtube import YouTubeClient
from tests.test_archival import seed_upload, settings


@pytest.mark.parametrize(
    "expected,reusable,reservation",
    [(100, 0, 200), (100, 60, 140), (100, 100, 100), (100, 200, 100), (-1, -1, 0)],
)
def test_remaining_input_and_merge_budget(
    expected: int, reusable: int, reservation: int
) -> None:
    assert download_reservation(expected, reusable) == reservation


def test_staging_startup_names_inaccessible_path(tmp_path: Path) -> None:
    root = tmp_path / "downloads"
    assert ensure_download_root(root) == root
    assert ensure_download_root(root) == root
    blocked = tmp_path / "file"
    blocked.touch()
    with pytest.raises(RuntimeError, match="staging path is inaccessible"):
        ensure_download_root(blocked)


@pytest.mark.parametrize("staging_free", [1, 100])
def test_service_admission_uses_staging_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, staging_free: int
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="download")
    configured = settings(tmp_path)
    measured: list[Path] = []
    claimed: list[str] = []

    def disk_usage(path: Path) -> Any:
        measured.append(path)
        return SimpleNamespace(
            free=staging_free
            if path == configured.download_root
            else 100 - staging_free
        )

    def process(work: Any) -> None:
        claimed.append(work.video.id)
        saga.stop.set()

    saga = SagaService(
        configured,
        (),
        store,
        YouTubeClient(),
        archival=cast(Any, SimpleNamespace(process=process)),
    )
    monkeypatch.setattr("core.service.shutil.disk_usage", disk_usage)

    async def run() -> None:
        task = asyncio.create_task(saga._archive_loop(0, "download"))
        await asyncio.sleep(0.05)
        saga.request_stop()
        async with saga._work_changed:
            saga._work_changed.notify_all()
        await task

    asyncio.run(run())
    assert measured and set(measured) == {configured.download_root}
    assert claimed == ([video_id] if staging_free == 100 else [])
    store.close()
