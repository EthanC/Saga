import json
from pathlib import Path
from typing import Any

import pytest
from yt_dlp import YoutubeDL

from core import youtube
from core.archival import ArchiveProcessor
from core.staging import StagingIdentityError, prepare_inputs, resume_format_selector
from core.state import StateStore
from tests.test_archival import FakeGateway, saved, seed_upload, settings

OLD = [
    {
        "format_id": "136",
        "ext": "mp4",
        "protocol": "https",
        "vcodec": "avc1",
        "acodec": "none",
        "width": 1280,
        "height": 720,
        "filesize": 4,
    },
    {
        "format_id": "251-12",
        "ext": "webm",
        "protocol": "https",
        "vcodec": "none",
        "acodec": "opus",
        "filesize": 3,
    },
]
NEW = [dict(OLD[0], format_id="137", height=1080), dict(OLD[1], format_id="251")]


def inputs(directory: Path, operation: str, video_id: str) -> tuple[Path, Path]:
    prepare_inputs(directory, operation, video_id, OLD)
    video, audio = directory / "merging.f136.mp4", directory / "merging.f251-12.webm"
    video.write_bytes(b"video")
    audio.write_bytes(b"audio")
    return video, audio


@pytest.mark.parametrize("selection", ["saved", "replacement", "descriptor"])
def test_download_finishes_after_cached_formats_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selection: str
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    media.unlink()
    video, audio = inputs(media.parent, frozen.operation_id, video_id)
    store.update_archive(
        video_id,
        status="download",
        download_completed_at=None,
        last_error=(
            "StagingIdentityError: selected formats changed; retained previous inputs"
        ),
        release=True,
        retry_after=21600,
    )
    store.reset_interrupted_claims()
    assert saved(store, video_id).retry_at is None
    selected = (
        OLD
        if selection == "saved"
        else NEW
        if selection == "replacement"
        else [dict(OLD[0], height=721), OLD[1]]
    )

    class Downloader:
        def __init__(self, options: Any) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def process_info(self, info: Any) -> None:
            if selection == "saved":
                assert video.read_bytes() == b"video" and audio.read_bytes() == b"audio"
            else:
                assert not video.exists() and not audio.exists()

        def download(self, urls: Any) -> int:
            assert self.options["format"] == "136+251-12/bestvideo+bestaudio"
            self.process_info({"id": video_id, "requested_formats": selected})
            (media.parent / "merging.mkv").write_bytes(b"merged")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", Downloader)
    processor = ArchiveProcessor(
        settings(tmp_path), store, youtube.YouTubeClient(), FakeGateway()
    )
    work = store.claim_archive(
        lane="download",
        prefer_recent=False,
        free_bytes=1000,
        download_root=tmp_path / "downloads",
    )
    assert work is not None
    processor.process(work)
    row = saved(store, video_id)
    assert row.status == "upload" and row.operation_id == frozen.operation_id
    assert media.read_bytes() == b"merged" and row.download_completed_at
    store.close()


@pytest.mark.parametrize(
    "problem",
    [
        "identity",
        "manifest",
        "unknown",
        "original",
        "symlink",
        "dangling_original",
        "invalid_selection",
    ],
)
def test_format_reset_preserves_unowned_or_ambiguous_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    video, audio = inputs(directory, "operation", "video")
    selected = NEW
    if problem == "identity":
        manifest = json.loads((directory / "inputs.json").read_text())
        manifest["video_id"] = "other"
        (directory / "inputs.json").write_text(json.dumps(manifest))
    elif problem == "manifest":
        (directory / "inputs.json").write_text("invalid")
    elif problem == "unknown":
        (directory / "unknown.part").write_bytes(b"unknown")
    elif problem == "original":
        (directory / "media.mkv").write_bytes(b"original")
    elif problem in {"symlink", "dangling_original"}:
        original = Path.is_symlink
        symlink = audio if problem == "symlink" else directory / "media.mkv"
        monkeypatch.setattr(
            Path, "is_symlink", lambda path: path == symlink or original(path)
        )
    else:
        selected = [dict(NEW[0], format_id="../invalid")]
    with pytest.raises(StagingIdentityError):
        prepare_inputs(
            directory, "operation", "video", selected, allow_reselection=True
        )
    assert video.read_bytes() == b"video" and audio.read_bytes() == b"audio"


def test_interrupted_input_reset_can_complete_without_mixing_formats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    video, audio = inputs(directory, "operation", "video")
    unlink = Path.unlink

    def interrupted(path: Path, *args: Any, **kwargs: Any) -> None:
        if path == audio:
            raise OSError("interrupted reset")
        unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupted)
    with pytest.raises(OSError):
        prepare_inputs(directory, "operation", "video", NEW, allow_reselection=True)
    assert audio.exists()
    assert "136" in json.loads((directory / "inputs.json").read_text())["formats"]
    monkeypatch.setattr(Path, "unlink", unlink)
    prepare_inputs(directory, "operation", "video", NEW, allow_reselection=True)
    assert not video.exists() and not audio.exists()
    assert "137" in json.loads((directory / "inputs.json").read_text())["formats"]


def test_resume_selector_only_pins_a_valid_separate_video_audio_pair(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    assert resume_format_selector(directory, "operation", "video") is None
    video, _ = inputs(directory, "operation", "video")
    assert (
        resume_format_selector(directory, "operation", "video")
        == "136+251-12/bestvideo+bestaudio"
    )
    manifest = json.loads((directory / "inputs.json").read_text())
    manifest["formats"]["136"]["vcodec"] = "none"
    (directory / "inputs.json").write_text(json.dumps(manifest))
    assert resume_format_selector(directory, "operation", "video") is None
    assert video.exists()


def test_incomplete_pair_is_not_pinned_and_empty_operation_can_change_selection(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    video, audio = inputs(directory, "operation", "video")
    audio.unlink()
    manifest = json.loads((directory / "inputs.json").read_text())
    del manifest["formats"]["251-12"]
    (directory / "inputs.json").write_text(json.dumps(manifest))
    assert resume_format_selector(directory, "operation", "video") is None
    assert video.read_bytes() == b"video"
    video.unlink()
    result = prepare_inputs(directory, "operation", "video", NEW)
    assert not result.files and result.partial_bytes == 0
    assert "137" in json.loads((directory / "inputs.json").read_text())["formats"]


@pytest.mark.parametrize("available", [True, False])
def test_real_downloader_selector_pins_saved_formats_or_uses_strict_fallback(
    available: bool,
) -> None:
    formats = [
        dict(item, url=f"https://example.com/{item['format_id']}")
        for item in ((OLD + NEW) if available else NEW)
    ]
    with YoutubeDL({"quiet": True, "cachedir": False}) as downloader:
        selector = downloader.build_format_selector("136+251-12/bestvideo+bestaudio")
        selected = list(
            selector(
                {
                    "formats": formats,
                    "has_merged_format": False,
                    "incomplete_formats": False,
                }
            )
        )
    assert [item["format_id"] for item in selected[0]["requested_formats"]] == (
        ["136", "251-12"] if available else ["137", "251"]
    )
