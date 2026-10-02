from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import event

from core import progress
from core.progress import DownloadCheckpoint
from core.state import StateStore
from core.youtube import DiskSpaceError
from tests.test_archival import saved, seed_upload


def checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[StateStore, DownloadCheckpoint, list[float]]:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="download", download_estimated_bytes=10000)
    work = store.claim_archive(lane="download", prefer_recent=False, free_bytes=100000)
    assert work
    now = [0.0]
    monkeypatch.setattr(
        progress.shutil, "disk_usage", lambda _: SimpleNamespace(free=100000)
    )
    monkeypatch.setattr(
        progress,
        "utcnow",
        lambda: datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=now[0]),
    )
    return (
        store,
        DownloadCheckpoint(store, work, media.parent, 0, clock=lambda: now[0]),
        now,
    )


def test_thousands_of_callbacks_have_bounded_commits_and_real_progress_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, worker, now = checkpoint(tmp_path, monkeypatch)
    commits: list[float] = []
    event.listen(store.engine, "commit", lambda _: commits.append(now[0]))
    for value in range(2000):
        now[0] = value / 100
        worker.record(10000, value)
        worker.record(10000, value)
    assert commits == [5.0, 10.0, 15.0]
    row = saved(store, worker.video_id)
    assert row.download_bytes == 1500
    worker.flush()
    assert saved(store, worker.video_id).download_bytes == 1999
    timestamp = saved(store, worker.video_id).download_progress_at
    now[0] = 30
    worker.record(10000, 1999)
    worker.boundary("stream_finished")
    worker.flush()
    assert saved(store, worker.video_id).download_progress_at == timestamp
    assert commits == [5, 10, 15, 19.99, 30]
    store.close()


def test_growth_is_immediate_and_external_depletion_does_not_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, worker, now = checkpoint(tmp_path, monkeypatch)
    worker.record(20000, 1)
    assert saved(store, worker.video_id).reserved_bytes == 39999
    monkeypatch.setattr(
        progress.shutil, "disk_usage", lambda _: SimpleNamespace(free=1)
    )
    with pytest.raises(DiskSpaceError):
        worker.record(20000, 2)
    assert saved(store, worker.video_id).download_bytes == 2
    assert now[0] == 0
    store.update_archive(worker.video_id, release=True)
    assert saved(store, worker.video_id).reserved_bytes == 0
    store.close()


def test_merge_boundaries_and_output_growth_checkpoint_actual_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, worker, now = checkpoint(tmp_path, monkeypatch)
    worker.record(10000, 10000)
    worker.boundary("merging", 0)
    now[0] = 5
    worker.boundary("merging", 500)
    row = saved(store, worker.video_id)
    assert row.download_phase == "merging" and row.merge_bytes == 500
    timestamp = row.download_progress_at
    now[0] = 6
    worker.boundary("merging", 500)
    assert saved(store, worker.video_id).download_progress_at == timestamp
    worker.boundary("merged", 1000)
    store.close()
