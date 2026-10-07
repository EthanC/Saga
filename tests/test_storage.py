import asyncio
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from core import storage
from core.archival import ArchiveProcessor
from core.progress import DownloadCheckpoint
from core.service import SagaService
from core.state import StateStore
from core.storage import data_usage, remaining_data_bytes
from core.youtube import DiskSpaceError, YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings


def test_budget_counts_database_backups_and_all_download_states(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    backups = tmp_path / "backups"
    backups.mkdir()
    (tmp_path / "saga.sqlite").write_bytes(b"d" * 10)
    (backups / "snapshot.sqlite").write_bytes(b"b" * 20)
    (downloads / "active.part").write_bytes(b"a" * 30)
    (downloads / "upload.mkv").write_bytes(b"u" * 40)
    (downloads / "inactive.part").write_bytes(b"i" * 50)
    assert data_usage(tmp_path) == 150
    assert remaining_data_bytes(tmp_path, 250) == 100
    assert remaining_data_bytes(tmp_path, 100) == 0
    (downloads / "upload.mkv").unlink()
    assert remaining_data_bytes(tmp_path, 250) == 140


def test_unreadable_data_fails_closed_and_disabled_budget_does_not_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unreadable(_: Any) -> Any:
        raise PermissionError("unreadable subtree")

    monkeypatch.setattr(storage.os, "scandir", unreadable)
    assert remaining_data_bytes(tmp_path, 0) is None
    assert remaining_data_bytes(tmp_path, 250) == 0
    with pytest.raises(PermissionError):
        data_usage(tmp_path)


def test_scan_ignores_symlink_targets_and_concurrent_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def vanished() -> Any:
        raise FileNotFoundError("upload cleanup")

    entries = [
        SimpleNamespace(
            is_dir=lambda **_: False,
            is_file=lambda **_: True,
            stat=lambda **_: SimpleNamespace(st_size=7),
        ),
        # A symlink is neither a directory nor a regular file with no-follow.
        SimpleNamespace(is_dir=lambda **_: False, is_file=lambda **_: False),
        SimpleNamespace(
            is_dir=lambda **_: False,
            is_file=lambda **_: True,
            stat=lambda **_: vanished(),
        ),
        SimpleNamespace(is_dir=lambda **_: True, path=str(tmp_path / "gone")),
    ]
    monkeypatch.setattr(
        storage.os,
        "scandir",
        lambda path: nullcontext(iter(entries)) if path == tmp_path else vanished(),
    )
    assert data_usage(tmp_path) == 7


def test_admission_shares_budget_across_workers_and_keeps_uploads_eligible(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    first, _, _ = seed_upload(store, tmp_path, "aaaaaaaaaaa")
    second, _, _ = seed_upload(store, tmp_path, "bbbbbbbbbbb")
    upload, _, _ = seed_upload(store, tmp_path, "ccccccccccc")
    for video_id in (first, second):
        store.update_archive(video_id, status="download")
    options: dict[str, Any] = dict(
        lane="download", prefer_recent=False, free_bytes=1000, buffer=4
    )
    # Each 4-byte download needs 8 bytes for input streams and merge output.
    one = store.claim_archive(**options, data_budget_bytes=15)
    assert one and one.progress.reserved_bytes == 8
    assert store.claim_archive(**options, data_budget_bytes=15) is None
    assert store.download_pause_reason == "data budget full"
    ready = store.claim_archive(lane="upload", prefer_recent=False, data_budget_bytes=0)
    assert ready and ready.video.id == upload
    two = store.claim_archive(**options, data_budget_bytes=16)
    assert two and two.progress.reserved_bytes == 8
    assert store.download_pause_reason is None
    # The transactional guard also refuses reservation growth over the budget.
    with pytest.raises(DiskSpaceError, match="data reservations"):
        store.guard_download_space(one.video.id, 5, 0, 1000, 0, data_budget_bytes=17)
    store.guard_download_space(one.video.id, 5, 0, 1000, 0, data_budget_bytes=18)
    assert saved(store, one.video.id).reserved_bytes == 10
    store.close()


@pytest.mark.parametrize("phase", ["downloading", "merging"])
def test_active_guards_recheck_usage_before_checkpoint_interval(
    tmp_path: Path, phase: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="download", download_estimated_bytes=100)
    work = store.claim_archive(lane="download", prefer_recent=False, free_bytes=1000)
    assert work
    worker = DownloadCheckpoint(
        store,
        work,
        media.parent,
        0,
        data_path=tmp_path,
        max_data_bytes=data_usage(tmp_path) + 100_000,
        clock=lambda: 0,
    )
    worker.record(100, 100)
    if phase == "merging":
        worker.boundary("merging", 0)
    backup = tmp_path / "new-backup.sqlite"
    backup.write_bytes(b"b" * 200_000)
    with pytest.raises(DiskSpaceError, match="data-directory budget full"):
        if phase == "merging":
            worker.boundary("merging", 50)
        else:
            worker.record(100, 100)
    backup.unlink()
    worker.record(100, 100)
    store.close()


def test_archive_processor_wires_instance_budget_into_download_guard(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="download")
    work = store.claim_archive(lane="download", prefer_recent=False, free_bytes=1000)
    assert work

    class Youtube:
        def download_video(self, video_id: str, directory: Path, **kwargs: Any) -> Path:
            kwargs["disk_guard"](4, 0)
            raise AssertionError("budget should prevent download")

    processor = ArchiveProcessor(
        settings(tmp_path, max_data_bytes=1), store, cast(Any, Youtube()), FakeGateway()
    )
    processor.process(work)
    row = saved(store, video_id)
    assert row.status == "download" and row.claimed_at is None
    assert row.failure_phase == "disk" and row.reserved_bytes == 0
    assert "data-directory budget full" in (row.last_error or "")
    store.close()


def test_service_pauses_at_budget_and_resumes_after_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="download")
    configured = settings(tmp_path, max_data_bytes=data_usage(tmp_path) + 100_000)
    backup = tmp_path / "backup.sqlite"
    backup.write_bytes(b"b" * 200_000)
    claimed: list[str] = []

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

    async def wait_until(condition: Any) -> None:
        async with asyncio.timeout(5):
            while not condition():
                await asyncio.sleep(0.01)

    async def run() -> None:
        task = asyncio.create_task(saga._archive_loop(0, "download"))
        try:
            await wait_until(lambda: store.download_pause_reason == "data budget full")
            assert not claimed
            health = saga._health_snapshot()
            assert health["storage"]["used_bytes"] == data_usage(tmp_path)
            assert health["storage"]["max_bytes"] == configured.max_data_bytes
            assert health["storage"]["remaining_bytes"] == 0
            assert health["staging"]["waiting_reason"] == "data budget full"
            backup.unlink()
            health = saga._health_snapshot()
            assert health["storage"]["remaining_bytes"] > 0
            async with saga._work_changed:
                saga._work_changed.notify_all()
            await asyncio.wait_for(task, timeout=5)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    # Isolate host free space so this test exercises the instance budget.
    monkeypatch.setattr(
        "core.service.shutil.disk_usage", lambda _: SimpleNamespace(free=1_000_000)
    )
    asyncio.run(run())
    assert claimed == [video_id]
    assert store.download_pause_reason is None
    store.close()
