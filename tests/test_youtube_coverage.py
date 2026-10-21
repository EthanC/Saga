import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from yt_dlp.utils import DownloadError

from core import youtube
from core.state import DiscoveredVideo, ResolvedChannel

CHANNEL = ResolvedChannel(
    id="UCaaaaaaaaaaaaaaaaaaaaaa",
    title="Channel",
    handle="@channel",
    url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
)


def fake_downloader(
    monkeypatch: pytest.MonkeyPatch, outcome: Any
) -> list[dict[str, Any]]:
    options_seen: list[dict[str, Any]] = []

    class FakeYoutubeDL:
        def __init__(self, options: dict[str, Any]) -> None:
            options_seen.append(options)

        def __enter__(self) -> FakeYoutubeDL:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def extract_info(self, url: str, *, download: bool) -> Any:
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(youtube, "YoutubeDL", FakeYoutubeDL)
    return options_seen


@pytest.mark.parametrize(
    ("info", "expected", "message"),
    [
        (None, None, "no channel data"),
        ({}, None, "canonical channel ID"),
        (
            {
                "channel_id": CHANNEL.id,
                "channel": "  Display  ",
                "uploader_id": "@display",
            },
            ("Display", "@display"),
            None,
        ),
        (
            {"channel_id": CHANNEL.id, "uploader_id": "invalid"},
            (CHANNEL.id, None),
            None,
        ),
    ],
)
def test_resolve_channel_validates_metadata(
    monkeypatch: pytest.MonkeyPatch,
    info: Any,
    expected: tuple[str, str | None] | None,
    message: str | None,
) -> None:
    options = fake_downloader(monkeypatch, info)
    if message is not None:
        with pytest.raises(ValueError, match=message):
            youtube.YouTubeClient().resolve_channel("@channel")
        return

    result = youtube.YouTubeClient().resolve_channel("@channel")

    assert (result.title, result.handle) == expected
    assert result.url.endswith(CHANNEL.id)
    assert options[0]["extract_flat"] is True


def test_scan_history_batches_deduplicates_and_respects_enabled_tabs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "shorts": (video("aaaaaaaaaaa"), video("bbbbbbbbbbb")),
        "videos": (video("aaaaaaaaaaa"), video("ccccccccccc")),
    }

    def tabs(
        self: youtube.YouTubeClient,
        channel: ResolvedChannel,
        tab: str,
        content_type: str,
        limit: int | None,
    ) -> tuple[DiscoveredVideo, ...]:
        return values.get(tab, ())

    def playlists(
        self: youtube.YouTubeClient, channel: ResolvedChannel, enabled: Any
    ) -> tuple[DiscoveredVideo, ...]:
        assert enabled == {"short", "video"}
        return video("ccccccccccc"), video("ddddddddddd")

    monkeypatch.setattr(youtube.YouTubeClient, "_tab_videos", tabs)
    monkeypatch.setattr(youtube.YouTubeClient, "_playlist_videos", playlists)
    batches: list[tuple[str, ...]] = []
    client = youtube.YouTubeClient()

    client.scan_history(
        CHANNEL,
        lambda batch: batches.append(tuple(item.id for item in batch)),
        batch_size=2,
        content_types={"short", "video"},
    )
    client.scan_history(
        CHANNEL, lambda _: pytest.fail("unexpected batch"), content_types=()
    )

    assert batches == [("aaaaaaaaaaa", "bbbbbbbbbbb"), ("ccccccccccc", "ddddddddddd")]


def test_scan_history_flushes_final_partial_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        youtube.YouTubeClient,
        "_tab_videos",
        lambda self, channel, tab, content_type, limit: (video("aaaaaaaaaaa"),),
    )
    monkeypatch.setattr(
        youtube.YouTubeClient, "_playlist_videos", lambda self, channel, enabled: ()
    )
    batches: list[tuple[str, ...]] = []

    youtube.YouTubeClient().scan_history(
        CHANNEL,
        lambda batch: batches.append(tuple(item.id for item in batch)),
        content_types={"video"},
    )

    assert batches == [("aaaaaaaaaaa",)]


def playlist_downloader(
    monkeypatch: pytest.MonkeyPatch, pages: dict[str, Any]
) -> list[str]:
    requested: list[str] = []

    class FakeYoutubeDL:
        def __init__(self, options: dict[str, Any]) -> None:
            assert options["extract_flat"] is True

        def __enter__(self) -> FakeYoutubeDL:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def extract_info(self, url: str, *, download: bool) -> Any:
            requested.append(url)
            outcome = pages[url]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(youtube, "YoutubeDL", FakeYoutubeDL)
    return requested


def playlist_url(playlist_id: str) -> str:
    return f"https://www.youtube.com/playlist?list={playlist_id}"


def test_playlist_videos_find_channel_uploads_missing_from_tabs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def entry(video_id: str, **changes: Any) -> dict[str, Any]:
        return {
            "id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "channel_id": CHANNEL.id,
            **changes,
        }

    requested = playlist_downloader(
        monkeypatch,
        {
            f"{CHANNEL.url}/playlists": {
                "entries": [
                    None,
                    {"url": None},
                    {"url": "https://www.youtube.com/@other"},
                    {"url": playlist_url("PLfirst")},
                    {"url": playlist_url("PLbroken")},
                    {"url": playlist_url("PLfirst")},
                ]
            },
            playlist_url("PLfirst"): {
                "entries": [
                    entry("unlisted001", title="Unlisted"),
                    entry("foreign0001", channel_id="UCbbbbbbbbbbbbbbbbbbbbbb"),
                    entry("missingchan", channel_id=None),
                    entry("badid"),
                    entry(
                        "shortvideo1", url="https://www.youtube.com/shorts/shortvideo1"
                    ),
                    entry("pastlive001", live_status="was_live"),
                    "invalid",
                ]
            },
            playlist_url("PLbroken"): DownloadError("This playlist does not exist"),
        },
    )

    result = list(
        youtube.YouTubeClient()._playlist_videos(
            CHANNEL, frozenset({"video", "livestream"})
        )
    )

    assert [(item.id, item.content_type) for item in result] == [
        ("unlisted001", "video"),
        ("pastlive001", "livestream"),
    ]
    assert result[0].title == "Unlisted"
    assert requested == [
        f"{CHANNEL.url}/playlists",
        playlist_url("PLfirst"),
        playlist_url("PLbroken"),
    ]


def test_playlist_videos_propagate_access_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    playlist_downloader(
        monkeypatch,
        {
            f"{CHANNEL.url}/playlists": {"entries": [{"url": playlist_url("PLx")}]},
            playlist_url("PLx"): DownloadError("Sign in to confirm you're not a bot"),
        },
    )

    with pytest.raises(DownloadError, match="not a bot"):
        list(youtube.YouTubeClient()._playlist_videos(CHANNEL, frozenset({"video"})))


def test_playlist_videos_tolerate_channels_without_playlists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    playlist_downloader(
        monkeypatch,
        {
            f"{CHANNEL.url}/playlists": DownloadError(
                "This channel does not have a playlists tab"
            )
        },
    )

    assert not list(
        youtube.YouTubeClient()._playlist_videos(CHANNEL, frozenset({"video"}))
    )


def video(video_id: str) -> DiscoveredVideo:
    return DiscoveredVideo(video_id, video_id, None, None, True)


@pytest.mark.parametrize("info", [None, {}, {"entries": None}])
def test_tab_videos_rejects_missing_playlist_data(
    monkeypatch: pytest.MonkeyPatch, info: Any
) -> None:
    fake_downloader(monkeypatch, info)

    with pytest.raises(RuntimeError, match="no .*tab data"):
        list(youtube.YouTubeClient()._tab_videos(CHANNEL, "videos", "video", None))


def test_tab_videos_skips_invalid_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    options = fake_downloader(
        monkeypatch, {"entries": [None, {}, {"id": "abcdefghijk"}]}
    )

    result = list(youtube.YouTubeClient()._tab_videos(CHANNEL, "videos", "video", None))

    assert [item.id for item in result] == ["abcdefghijk"]
    assert "playlist_items" not in options[0]


def complete_metadata(**changes: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": "abcdefghijk",
        "title": "Video",
        "description": "Description",
        "upload_date": "20240102",
        "channel_id": CHANNEL.id,
        "channel": "Channel",
        "uploader_id": "@channel",
        "channel_url": CHANNEL.url,
        "filesize_approx": 12,
        "timestamp": 1_700_000_000,
        "live_status": "not_live",
    }
    values.update(changes)
    return values


def test_video_metadata_returns_complete_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_downloader(
        monkeypatch, complete_metadata(tags=[" Tag One ", "Tag Two", "Tag One", ""])
    )

    result = youtube.YouTubeClient().video_metadata("abcdefghijk")

    assert result.title == "Video"
    assert result.estimated_bytes == 12
    assert result.published_precise is True
    assert result.tags == ("Tag One", "Tag Two")


@pytest.mark.parametrize(
    ("outcome", "error"),
    [
        (None, RuntimeError),
        (DownloadError("Video unavailable"), youtube.VideoUnavailableError),
        (DownloadError("network broke"), RuntimeError),
    ],
)
def test_video_metadata_handles_downloader_failures(
    monkeypatch: pytest.MonkeyPatch, outcome: Any, error: type[Exception]
) -> None:
    fake_downloader(monkeypatch, outcome)

    with pytest.raises(error):
        youtube.YouTubeClient().video_metadata("abcdefghijk")


def test_full_metadata_uses_fallbacks_and_requested_format_sizes() -> None:
    result = youtube._full_metadata(
        complete_metadata(
            description=None,
            channel=None,
            uploader="Uploader",
            uploader_id=" ",
            channel_url=None,
            filesize_approx=None,
            requested_formats=[
                {"filesize": 4},
                {"filesize_approx": 5},
                {"filesize": True},
                "ignored",
            ],
            timestamp=None,
        ),
        "abcdefghijk",
    )

    assert result.channel_name == "Uploader"
    assert result.channel_handle == CHANNEL.id
    assert result.channel_url == CHANNEL.url
    assert result.description == ""
    assert result.tags == ()
    assert result.estimated_bytes == 9
    assert result.published_at == datetime(2024, 1, 2, tzinfo=UTC)
    assert result.published_precise is False


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"id": "wrong"}, "unexpected video ID"),
        ({"live_status": "is_live"}, "replay is not ready"),
        ({"title": None}, "has no title"),
        ({"channel": None, "uploader": None}, "has no channel"),
        ({"upload_date": "bad"}, "valid upload date"),
    ],
)
def test_full_metadata_rejects_invalid_data(
    changes: dict[str, Any], message: str
) -> None:
    with pytest.raises(RuntimeError, match=message):
        youtube._full_metadata(complete_metadata(**changes), "abcdefghijk")


class DownloadingYoutubeDL:
    result = 0
    error: Exception | None = None
    create_output = False
    options: dict[str, Any] = {}

    def __init__(self, options: dict[str, Any]) -> None:
        type(self).options = options

    def __enter__(self) -> DownloadingYoutubeDL:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def download(self, urls: list[str]) -> int:
        if self.error is not None:
            raise self.error
        if self.create_output:
            Path(
                self.options["outtmpl"]["default"].replace("%(ext)s", "mkv")
            ).write_bytes(b"media")
        return self.result


def test_download_video_handles_space_and_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(10, 0, 5)
    )
    with pytest.raises(youtube.DiskSpaceError, match="11 bytes"):
        youtube.YouTubeClient().download_video(
            "abcdefghijk", tmp_path, minimum_free_bytes=5, estimated_bytes=3
        )

    output = tmp_path / "media.mkv"
    output.write_bytes(b"existing")
    (tmp_path / "download.json").write_text(
        json.dumps({"video_id": "abcdefghijk", "size": 8}), encoding="utf-8"
    )
    assert (
        youtube.YouTubeClient().download_video(
            "abcdefghijk", tmp_path, minimum_free_bytes=5, estimated_bytes=3
        )
        == output
    )


@pytest.mark.parametrize("free_bytes", [15, 24, 25])
def test_download_reserves_space_for_streams_and_merged_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, free_bytes: int
) -> None:
    monkeypatch.setattr(youtube, "YoutubeDL", DownloadingYoutubeDL)
    monkeypatch.setattr(DownloadingYoutubeDL, "result", 0)
    monkeypatch.setattr(DownloadingYoutubeDL, "error", None)
    monkeypatch.setattr(DownloadingYoutubeDL, "create_output", True)
    monkeypatch.setattr(
        youtube.shutil,
        "disk_usage",
        lambda _: shutil._ntuple_diskusage(25, 25 - free_bytes, free_bytes),
    )
    client = youtube.YouTubeClient()

    if free_bytes < 25:
        with pytest.raises(youtube.DiskSpaceError, match="25 bytes"):
            client.download_video(
                "abcdefghijk", tmp_path, minimum_free_bytes=5, estimated_bytes=10
            )
        assert not (tmp_path / "media.mkv").exists()
    else:
        assert (
            client.download_video(
                "abcdefghijk", tmp_path, minimum_free_bytes=5, estimated_bytes=10
            )
            == tmp_path / "media.mkv"
        )


@pytest.mark.parametrize(
    ("result", "error", "create_output", "message", "expected_type"),
    [
        (
            0,
            DownloadError("private video"),
            False,
            "private video",
            youtube.VideoUnavailableError,
        ),
        (0, DownloadError("network broke"), False, "network broke", RuntimeError),
        (2, None, False, "status 2", RuntimeError),
        (0, None, False, "expected MKV", RuntimeError),
    ],
)
def test_download_video_handles_downloader_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    result: int,
    error: Exception | None,
    create_output: bool,
    message: str,
    expected_type: type[Exception],
) -> None:
    DownloadingYoutubeDL.result = result
    DownloadingYoutubeDL.error = error
    DownloadingYoutubeDL.create_output = create_output
    monkeypatch.setattr(youtube, "YoutubeDL", DownloadingYoutubeDL)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(10, 0, 10)
    )

    with pytest.raises(expected_type, match=message):
        youtube.YouTubeClient().download_video(
            "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=None
        )


def test_channel_and_video_normalization_edge_cases() -> None:
    assert youtube._channel_url(CHANNEL.id) == CHANNEL.url
    assert (
        youtube._channel_url(" https://example.com/channel/ ")
        == "https://example.com/channel"
    )
    assert youtube._video(None) is None
    assert youtube._video({}) is None
    normalized = youtube._video(
        {"id": "abcdefghijk", "title": " ", "live_status": 1, "upload_date": "20240102"}
    )
    assert normalized is not None
    assert normalized.title is None
    assert normalized.live_status is None
    assert normalized.published_precise is False
    assert youtube._published_at({"timestamp": True}) is None


@pytest.mark.parametrize(
    ("document", "content", "message"),
    [
        ({"streams": [{"codec_type": "video"}]}, b"data", "both audio and video"),
        (
            {"streams": [{"codec_type": "video"}, {"codec_type": "audio"}]},
            b"",
            "media file is empty",
        ),
    ],
)
def test_verify_media_rejects_invalid_media(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    document: Any,
    content: bytes,
    message: str,
) -> None:
    path = tmp_path / "media.mkv"
    path.write_bytes(content)
    monkeypatch.setattr(
        youtube.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(document)),
    )

    with pytest.raises(RuntimeError, match=message):
        youtube.verify_media(path)


def test_verify_media_hashes_valid_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "media.mkv"
    path.write_bytes(b"test")
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def run(*args: Any, **kwargs: Any) -> SimpleNamespace:
        calls.append((args, kwargs))
        return SimpleNamespace(
            stdout='{"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}'
        )

    monkeypatch.setattr(youtube.subprocess, "run", run)

    result = youtube.verify_media(path, timeout=2)

    assert result.size == 4
    assert result.md5 == "098f6bcd4621d373cade4e832627b4f6"
    assert calls[0][1]["timeout"] == 2
