from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from yt_dlp.utils import DownloadError

from core.archival import ArchiveProcessor
from core.state import StateStore, utcnow
from core.youtube import VideoUnavailableError, YouTubeClient, _classify_download_error
from core.youtube_errors import VideoAccessError
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_partials import partial
from tests.test_youtube_coverage import complete_metadata, fake_downloader


@pytest.mark.parametrize(
    "detail",
    [
        "Sign in to confirm your age",
        "Video unavailable. Sign in to confirm you’re not a bot",
        "This video is age-restricted",
        "This is members-only content",
        "Login required",
    ],
)
def test_access_errors_take_precedence_over_unavailable(detail: str) -> None:
    assert isinstance(_classify_download_error(DownloadError(detail)), VideoAccessError)
    assert isinstance(
        _classify_download_error(DownloadError("Private video")), VideoUnavailableError
    )


@pytest.mark.parametrize("stage", ["metadata", "download"])
def test_access_failure_retries_and_recovers_after_cookie_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("expired")
    client = YouTubeClient(cookies)
    processor = ArchiveProcessor(settings(tmp_path), store, client, FakeGateway())
    store.update_archive(video_id, status=stage, download_completed_at=None)
    fake_downloader(monkeypatch, DownloadError("Sign in to confirm your age"))
    if stage == "download":

        def denied(*args: Any, **kwargs: Any) -> Path:
            raise _classify_download_error(DownloadError("Sign in to confirm your age"))

        monkeypatch.setattr(client, "download_video", denied)
    work = store.claim_archive(lane=stage, prefer_recent=False, free_bytes=100)
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert row.status == stage and row.failure_phase == "youtube_access"
    assert row.retry_at is not None and row.claimed_at is None and media.exists()
    assert (
        store.claim_archive(
            lane=stage,
            prefer_recent=False,
            now=row.retry_at - timedelta(microseconds=1),
        )
        is None
    )
    cookies.write_text("fresh")
    assert client._options()["cookiefile"].read() == "fresh"
    if stage == "metadata":
        fake_downloader(monkeypatch, complete_metadata(id=video_id))
    else:
        monkeypatch.setattr(client, "download_video", lambda *args, **kwargs: media)
    retry = store.claim_archive(
        lane=stage, prefer_recent=False, free_bytes=100, now=row.retry_at
    )
    assert retry is not None
    processor.process(retry)
    assert saved(store, video_id).status == (
        "download" if stage == "metadata" else "upload"
    )
    if stage == "download":
        assert saved(store, video_id).operation_id == frozen.operation_id
    store.close()


@pytest.mark.parametrize("has_contract", [False, True])
def test_startup_repairs_legacy_authentication_failures_without_losing_partials(
    tmp_path: Path, has_contract: bool
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    path = partial(store, tmp_path)
    row = saved(store, "abcdefghijk")
    values: dict[str, Any] = {
        "status": "unavailable",
        "last_error": "VideoUnavailableError: Sign in to confirm your age",
    }
    if not has_contract:
        values["frozen_metadata"] = None
    store.update_archive(row.video_id, **values)
    other, _, _ = seed_upload(store, tmp_path, "GoneVideo12")
    store.update_archive(
        other,
        status="unavailable",
        last_error="VideoUnavailableError: has been removed",
    )
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    store.reset_interrupted_claims()
    recovered = saved(store, row.video_id)
    assert recovered.status == ("download" if has_contract else "metadata")
    assert (
        recovered.operation_id == row.operation_id and path.read_bytes() == b"partial"
    )
    assert saved(store, other).status == "unavailable"
    assert (
        store.claim_archive(
            lane=recovered.status, prefer_recent=False, free_bytes=100, now=utcnow()
        )
        is not None
    )
    store.close()
