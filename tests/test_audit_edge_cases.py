import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core import backups, service, youtube
from core.archival import ArchiveProcessor
from core.service import MonitoredChannel, SagaService
from core.shutdown import CooperativeStop, StopController
from core.staging import (
    StagingIdentityError,
    completed_download_size,
    maintain_partials,
    prepare_inputs,
)
from core.state import StateStore, utcnow
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_partials import FORMATS, partial
from tests.test_service_coverage import CHANNEL, VIDEO
from tests.test_worker_protocol import pickled
from tests.test_youtube_coverage import DownloadingYoutubeDL, fake_downloader


def test_atomic_completion_receipt_can_be_promoted_under_disk_pressure(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="download", download_completed_at=None)
    (media.parent / "download.json").write_text(
        json.dumps({"video_id": video_id, "size": media.stat().st_size})
    )
    claim = state.claim_archive(
        lane="download",
        prefer_recent=False,
        dispatch_class="recent",
        download_root=tmp_path / "downloads",
        free_bytes=0,
        minimum_free_bytes=25_000_000_000,
        pause_fresh_downloads=True,
    )
    assert claim and claim.progress.reserved_bytes == 0
    ArchiveProcessor(
        settings(tmp_path), state, youtube.YouTubeClient(), FakeGateway()
    ).process(claim)
    row = saved(state, video_id)
    assert (
        row.status == "upload"
        and row.download_completed_at
        and row.download_phase == "complete"
    )
    assert row.media_uploaded_at is None and media.exists()
    state.close()


def test_retention_protects_native_receipts_before_parent_completion_commit(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    path = partial(state, tmp_path)
    (path.parent / "media.mkv").write_bytes(b"complete")
    (path.parent / "download.json").write_text(
        json.dumps({"video_id": "abcdefghijk", "size": 8})
    )
    configured = settings(
        tmp_path,
        partial_max_bytes=0,
        partial_min_age=1,
        partial_retention_mode="enforce",
    )
    assert not maintain_partials(state, configured)["decisions"]
    assert path.exists()
    state.close()


@pytest.mark.parametrize(
    "problem", ["identity", "missing", "empty", "bad", "bool", "symlink"]
)
def test_unverified_outputs_never_receive_completion_credit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    media = directory / "media.mkv"
    media.write_bytes(b"x")
    marker = directory / "download.json"
    marker.write_text(json.dumps({"video_id": "video", "size": 1}))
    if problem == "missing":
        media.unlink()
    elif problem == "empty":
        media.write_bytes(b"")
    elif problem == "bad":
        marker.write_text("bad")
    elif problem == "bool":
        marker.write_text('{"video_id":"video","size":true}')
    elif problem == "symlink":
        original = Path.is_symlink
        monkeypatch.setattr(
            Path, "is_symlink", lambda file: file == media or original(file)
        )
    assert (
        completed_download_size(
            directory, "other" if problem == "identity" else "operation", "video"
        )
        is None
    )


@pytest.mark.parametrize("failure", ["contract", "request", "stop", "claim", "backoff"])
def test_legacy_validation_failures_release_ownership_and_preserve_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    path = partial(state, tmp_path, legacy=True)
    controller = StopController()
    if failure == "contract":
        state.update_archive("abcdefghijk", frozen_metadata="{}")
    elif failure == "backoff":
        state.update_archive("abcdefghijk", release=True, retry_after=60)
    elif failure == "claim":
        monkeypatch.setattr(state, "claim_partial_maintenance", lambda *args: None)
    calls: list[str] = []

    def select(video_id: str) -> Any:
        calls.append(video_id)
        if failure == "request":
            raise RuntimeError("https://user:secret@example.com/?sig=signed-secret")
        if failure == "stop":
            controller.request()
        return FORMATS, 7

    result = maintain_partials(
        state,
        settings(tmp_path),
        youtube=SimpleNamespace(selected_formats=select),
        controller=controller,
    )
    assert path.read_bytes() == b"partial"
    assert saved(state, "abcdefghijk").maintenance_token is None
    assert "signed-secret" not in str(result) and "user:secret" not in str(result)
    assert bool(calls) == (failure in {"request", "stop"})
    state.close()


@pytest.mark.parametrize("outcome", [None, "formats", "single", "error"])
def test_partial_identity_lookup_uses_selected_inputs_without_downloading(
    monkeypatch: pytest.MonkeyPatch, outcome: str | None
) -> None:
    info: Any = None
    if outcome == "formats":
        info = {
            "id": "video",
            "requested_formats": [
                {"format_id": "315", "ext": "webm", "filesize": 4},
                {"format_id": "251", "ext": "webm", "filesize_approx": 2},
            ],
        }
    elif outcome == "single":
        info = {"id": "video", "format_id": "315", "ext": "webm", "filesize": 4}
    elif outcome == "error":
        info = youtube.DownloadError("private video")
    fake_downloader(monkeypatch, info)
    client = youtube.YouTubeClient()
    if outcome is None:
        with pytest.raises(StagingIdentityError):
            client.selected_formats("video")
    elif outcome == "error":
        with pytest.raises(youtube.VideoUnavailableError):
            client.selected_formats("video")
    else:
        selected, expected = client.selected_formats("video")
        assert expected == (6 if outcome == "formats" else 4) and selected
    monkeypatch.setattr(youtube, "run_managed", pickled(lambda *args: (FORMATS, 7)))
    assert youtube.YouTubeClient(managed=True).selected_formats("video") == (FORMATS, 7)


def test_invalid_fragment_checkpoint_is_not_adopted(tmp_path: Path) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    legacy = directory / "media.f315.webm.part"
    legacy.write_bytes(b"preserve")
    (directory / "media.f315.webm.ytdl").write_text('{"downloader":{}}')
    with pytest.raises(StagingIdentityError, match="malformed"):
        prepare_inputs(directory, "operation", "video", FORMATS)
    assert legacy.read_bytes() == b"preserve"


@pytest.mark.parametrize("target", ["directory", "media.mkv", "download.tmp"])
def test_output_symlinks_are_retained_with_named_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    original = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda file: (
            file == directory
            if target == "directory"
            else file == directory / target or original(file)
        ),
    )
    with pytest.raises(StagingIdentityError, match="symlink"):
        youtube.YouTubeClient().download_video(
            "video", directory, estimated_bytes=4, minimum_free_bytes=0
        )


def test_incomplete_ffmpeg_temp_is_removed_before_new_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / "merging.temp.mkv"
    temporary.write_bytes(b"interrupted")
    monkeypatch.setattr(youtube, "YoutubeDL", DownloadingYoutubeDL)
    monkeypatch.setattr(DownloadingYoutubeDL, "create_output", True)
    monkeypatch.setattr(DownloadingYoutubeDL, "result", 0)
    monkeypatch.setattr(DownloadingYoutubeDL, "error", None)
    assert (
        youtube.YouTubeClient()
        .download_video("video", tmp_path, estimated_bytes=4, minimum_free_bytes=0)
        .exists()
    )
    assert not temporary.exists()


def test_upload_cannot_read_media_outside_the_owned_operation(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    outside = tmp_path / "outside.mkv"
    outside.write_bytes(b"test")
    state.update_archive(video_id, media_path=str(outside))
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work
    gateway = FakeGateway()
    ArchiveProcessor(
        settings(tmp_path), state, youtube.YouTubeClient(), gateway
    ).process(work)
    assert (
        gateway.upload_calls == 0 and saved(state, video_id).status == "operator_action"
    )
    assert media.exists() and outside.read_bytes() == b"test"
    state.close()


def test_fatal_component_requests_stop_before_draining_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    saga = SagaService(settings(tmp_path), (), state, youtube.YouTubeClient())

    async def fail() -> None:
        raise RuntimeError("fixture component failure")

    monkeypatch.setattr(saga, "_delivery_loop", fail)
    with pytest.raises(ExceptionGroup):
        asyncio.run(saga.run())
    assert saga.stop_controller.event.is_set()
    saga = SagaService(settings(tmp_path), (), state, youtube.YouTubeClient())
    monkeypatch.setattr(saga, "_initialize", fail)
    with pytest.raises(ExceptionGroup) as caught:
        asyncio.run(saga.run())
    assert caught.value.subgroup(RuntimeError) is not None
    assert saga.stop_controller.event.is_set()
    state.close()


def test_background_stop_outcomes_do_not_log_application_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    saga = SagaService(settings(tmp_path), (), state, youtube.YouTubeClient())

    async def stop(*args: Any, **kwargs: Any) -> Any:
        raise CooperativeStop("fixture")

    monkeypatch.setattr(saga, "_youtube_request", stop)
    monitored = MonitoredChannel(CHANNEL, 1, frozenset({"video"}))
    asyncio.run(saga._poll_loop(monitored))
    asyncio.run(saga._reconciliation_loop(monitored))
    monkeypatch.setattr(
        service,
        "maintain_partials",
        lambda *args, **kwargs: (_ for _ in ()).throw(CooperativeStop("fixture")),
    )
    asyncio.run(saga._partial_maintenance_loop())
    state.register_channel(CHANNEL.url, CHANNEL)
    state.establish_baseline(CHANNEL.id, ())
    from dataclasses import replace

    monkeypatch.setattr(
        saga,
        "_youtube_request_sync",
        lambda *args: (_ for _ in ()).throw(CooperativeStop("fixture")),
    )
    with pytest.raises(CooperativeStop):
        saga._resolve_publications(CHANNEL, (replace(VIDEO, published_at=None),))
    monkeypatch.setattr(backups, "next_backup_run", lambda *args: utcnow())
    monkeypatch.setattr(backups, "create_backup", stop)
    asyncio.run(
        backups.run_backup_scheduler(
            state.path, settings(tmp_path).backup, asyncio.Event()
        )
    )
    state.close()
