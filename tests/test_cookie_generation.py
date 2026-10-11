import asyncio
import pickle
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from yt_dlp.utils import DownloadError

from core import service, youtube
from core.archival import ArchiveProcessor
from core.service import SagaService
from core.state import StateStore
from core.youtube_errors import VideoAccessError, cookie_fingerprint
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_pipeline_health import queue


def test_cookie_replacement_clears_stale_alerts_until_a_new_failure(
    tmp_path: Path,
) -> None:
    store, video_id, now = queue(tmp_path, "metadata")
    old, fresh = cookie_fingerprint("old"), cookie_fingerprint("fresh")
    store.update_archive(
        video_id,
        failure_phase="youtube_access",
        youtube_access_generation=old,
        release=True,
        retry_after=3600,
    )
    store.transfer_health(stall_timeout=30, now=now, youtube_cookie_generation=old)
    assert any(
        a["key"] == "youtube:access"
        for a in store.transfer_health(
            stall_timeout=30,
            now=now + timedelta(seconds=31),
            youtube_cookie_generation=old,
        )["alerts"]
    )
    health = store.transfer_health(
        stall_timeout=30,
        now=now + timedelta(seconds=32),
        youtube_cookie_generation=fresh,
    )
    assert health["youtube_access"] == {"current_failures": 0, "awaiting_recheck": 1}
    assert not any(a["key"] == "youtube:access" for a in health["alerts"])
    store.update_archive(video_id, youtube_access_generation=fresh)
    store.transfer_health(
        stall_timeout=30,
        now=now + timedelta(seconds=33),
        youtube_cookie_generation=fresh,
    )
    health = store.transfer_health(
        stall_timeout=30,
        now=now + timedelta(seconds=64),
        youtube_cookie_generation=fresh,
    )
    assert health["youtube_access"] == {"current_failures": 1, "awaiting_recheck": 0}
    assert any(a["key"] == "youtube:access" for a in health["alerts"])
    store.close()


def test_failure_keeps_attempted_cookie_snapshot_across_file_changes_and_ipc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("old-session")

    class Downloader:
        def __init__(self, options: Any) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def extract_info(self, *args: Any, **kwargs: Any) -> Any:
            cookies.write_text("new-session")
            self.options["cookiefile"].seek(0)
            self.options["cookiefile"].write("server-updated-session")
            raise DownloadError("Sign in to confirm your age")

    monkeypatch.setattr(youtube, "YoutubeDL", Downloader)
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="metadata")
    client = youtube.YouTubeClient(cookies)
    with pytest.raises(VideoAccessError) as result:
        client.video_metadata(video_id)
    error = pickle.loads(pickle.dumps(result.value))
    assert error.cookie_generation == cookie_fingerprint("old-session")
    assert "old-session" not in str(error)
    cookies.write_text("old-session")
    work = store.claim_archive(lane="metadata", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(settings(tmp_path), store, client, FakeGateway()).process(work)
    assert saved(store, video_id).youtube_access_generation == cookie_fingerprint(
        "old-session"
    )
    store.close()


def test_rechecks_bypass_metadata_supply_but_not_download_admission(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    target, _, frozen = seed_upload(store, tmp_path, "AccessVid12")
    store.update_archive(
        target,
        status="metadata",
        failure_phase="youtube_access",
        youtube_access_generation="old",
        release=True,
        retry_after=21600,
    )
    for name in ("Backfill001", "Backfill002"):
        video_id, _, _ = seed_upload(store, tmp_path, name)
        store.update_archive(video_id, status="download", download_completed_at=None)
    assert store.claim_archive(lane="metadata", prefer_recent=False, buffer=1) is None
    assert store.refresh_youtube_access("fresh") == 1
    assert store.refresh_youtube_access("fresh") == 0
    work = store.claim_archive(
        lane="metadata",
        prefer_recent=False,
        buffer=1,
        youtube_cookie_generation="fresh",
    )
    assert work is not None and work.video.id == target
    store.update_archive(target, status="download", release=True)
    ready, _, _ = seed_upload(store, tmp_path, "ReadyUpload")
    assert (
        store.claim_archive(
            lane="download",
            prefer_recent=False,
            buffer=1,
            free_bytes=1000,
            youtube_cookie_generation="fresh",
        )
        is None
    )
    assert store.download_pause_reason == "upload-ready buffer full"
    store.update_archive(ready, status="complete")
    work = store.claim_archive(
        lane="download",
        prefer_recent=False,
        buffer=3,
        free_bytes=1000,
        youtube_cookie_generation="fresh",
        dispatch_class="backfill",
        download_root=tmp_path / "downloads",
    )
    assert (
        work is not None
        and work.video.id == target
        and work.progress.operation_id == frozen.operation_id
    )
    store.update_archive(
        target,
        release=True,
        retry_after=21600,
        youtube_access_generation="fresh",
        failure_phase="youtube_access",
    )
    assert store.refresh_youtube_access("fresh") == 0
    store.close()


@pytest.mark.parametrize("missing", [False, True])
def test_cookie_watcher_reschedules_legacy_failures_and_handles_read_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: bool
) -> None:
    cookies = tmp_path / "cookies.txt"
    if not missing:
        cookies.write_text("fresh")
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(
        video_id,
        status="metadata",
        failure_phase="youtube_access",
        release=True,
        retry_after=21600,
    )
    saga = SagaService(
        replace(settings(tmp_path), youtube_cookies_path=cookies),
        (),
        store,
        youtube.YouTubeClient(cookies),
    )
    messages: list[str] = []
    monkeypatch.setattr(
        service,
        "logger",
        SimpleNamespace(
            info=lambda *args: messages.append("scheduled"),
            warning=lambda *args: messages.append("read error"),
            debug=lambda *args: None,
        ),
    )

    async def wait(_: float) -> None:
        saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    asyncio.run(saga._cookie_watch_loop())
    assert messages == (["read error"] if missing else ["scheduled"])
    assert (saved(store, video_id).retry_at is None) != missing
    if not missing:
        assert saga._health_snapshot()["youtube_access"]["current_failures"] == 0
    store.close()
