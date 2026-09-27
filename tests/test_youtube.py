import shutil
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from yt_dlp.extractor.youtube import YoutubeIE, YoutubeTabIE
from yt_dlp.networking.common import Response
from yt_dlp.networking.exceptions import HTTPError
from yt_dlp.utils import DownloadError, ExtractorError

from core import youtube
from core.bandwidth import ByteLimiter
from core.state import ResolvedChannel
from core.youtube import YouTubeClient, _channel_url, _video


def test_channel_url_normalization() -> None:
    assert _channel_url("@Example") == "https://www.youtube.com/@Example"
    assert (
        _channel_url("https://www.youtube.com/@Example/shorts")
        == "https://www.youtube.com/@Example"
    )
    assert (
        _channel_url(
            "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa/featured"
        )
        == "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa"
    )
    assert _channel_url("not-a-channel-url") == "not-a-channel-url"


def test_video_normalization_defers_active_livestream() -> None:
    video = _video(
        {
            "id": "abcdefghijk",
            "title": "Live",
            "timestamp": 1_700_000_000,
            "live_status": "is_live",
        },
        "livestream",
    )

    assert video is not None
    assert video.published_at == datetime.fromtimestamp(1_700_000_000, UTC)
    assert video.replay_ready is False
    assert video.content_type == "livestream"


def test_recent_videos_only_scans_enabled_content_types(monkeypatch: Any) -> None:
    urls: list[str] = []

    class FakeYoutubeDL:
        def __init__(self, options: dict[str, Any]) -> None:
            pass

        def __enter__(self) -> FakeYoutubeDL:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def extract_info(self, url: str, *, download: bool) -> dict[str, Any]:
            urls.append(url)
            video_id = "sssssssssss" if url.endswith("/shorts") else "lllllllllll"
            return {"entries": [{"id": video_id, "title": video_id}]}

    monkeypatch.setattr(youtube, "YoutubeDL", FakeYoutubeDL)
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )

    videos = YouTubeClient().recent_videos(
        channel, content_types={"short", "livestream"}
    )

    assert all(not url.endswith("/videos") for url in urls)
    assert {(video.id, video.content_type) for video in videos} == {
        ("sssssssssss", "short"),
        ("lllllllllll", "livestream"),
    }


def test_download_uses_strict_separate_formats(
    tmp_path: Path, monkeypatch: Any
) -> None:
    options: dict[str, Any] = {}

    class FakeYoutubeDL:
        def __init__(self, values: dict[str, Any]) -> None:
            options.update(values)

        def __enter__(self) -> FakeYoutubeDL:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def download(self, urls: list[str]) -> int:
            output = Path(options["outtmpl"]["default"].replace("%(ext)s", "mkv"))
            output.write_bytes(b"media")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", FakeYoutubeDL)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda path: shutil._ntuple_diskusage(10, 0, 10)
    )

    path = YouTubeClient().download_video(
        "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=None
    )

    assert path == tmp_path / "media.mkv"
    assert options["format"] == "bestvideo+bestaudio"
    assert options["merge_output_format"] == "mkv"
    assert options["skip_unavailable_fragments"] is False
    assert "/best" not in options["format"]
    assert "ratelimit" not in options
    assert "external_downloader" not in options


def test_limited_download_uses_native_bounded_downloaders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options: dict[str, Any] = {}

    class FakeYoutubeDL:
        def __init__(self, values: dict[str, Any]) -> None:
            self.params = values
            options.update(values)

        def __enter__(self) -> FakeYoutubeDL:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def download(self, urls: list[str]) -> int:
            output = Path(options["outtmpl"]["default"].replace("%(ext)s", "mkv"))
            output.write_bytes(b"media")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", FakeYoutubeDL)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(10, 0, 10)
    )

    YouTubeClient(download_limiter=ByteLimiter(100)).download_video(
        "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=None
    )

    assert options["concurrent_fragment_downloads"] == 1
    assert options["external_downloader"] == {"default": "native"}
    assert options["hls_prefer_native"] is True
    assert options["skip_unavailable_fragments"] is False
    assert options["external_downloader_args"]["ffmpeg_i"] == [
        "-protocol_whitelist",
        "file,crypto,pipe",
    ]
    assert "ratelimit" not in options


@pytest.mark.parametrize("protocol", ["m3u8_native", "http_dash_segments"])
@pytest.mark.parametrize("bytes_per_second", [0, 1_000_000])
def test_missing_media_fragment_fails_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protocol: str,
    bytes_per_second: int,
) -> None:
    requests: list[str] = []

    def urlopen(self: Any, request: Any) -> Response:
        url = request.url
        requests.append(url)
        if url.endswith("/second"):
            raise HTTPError(Response(BytesIO(), url, {}, status=404))
        assert url.endswith("/first")
        return Response(BytesIO(b"fragment"), url, {"Content-Length": "8"})

    def download(self: Any, urls: list[str]) -> int:
        self.params["fragment_retries"] = 0
        success, _ = self.dl(
            str(tmp_path / "media.mkv"),
            {
                "id": "abcdefghijk",
                "url": "https://example.com/manifest.m3u8",
                "protocol": protocol,
                "ext": "mkv",
                "fragments": [
                    {"url": "https://example.com/first"},
                    {"url": "https://example.com/second"},
                ],
                "hls_media_playlist_data": (
                    "#EXTM3U\n#EXT-X-TARGETDURATION:1\n"
                    "#EXTINF:1,\nfirst\n#EXTINF:1,\nsecond\n#EXT-X-ENDLIST\n"
                ),
            },
        )
        return 0 if success else 1

    monkeypatch.setattr(youtube.YoutubeDL, "urlopen", urlopen)
    monkeypatch.setattr(youtube.YoutubeDL, "download", download)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(10, 0, 10)
    )

    with pytest.raises(RuntimeError, match="HTTP Error 404"):
        YouTubeClient(download_limiter=ByteLimiter(bytes_per_second)).download_video(
            "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=None
        )

    assert requests == ["https://example.com/first", "https://example.com/second"]
    assert not (tmp_path / "media.mkv").exists()


class RecordingStream(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.read_sizes: list[int | None] = []

    def read(self, size: int | None = -1) -> bytes:
        self.read_sizes.append(size)
        return super().read(size)


def test_limited_response_paces_bounded_and_unbounded_reads() -> None:
    stream = RecordingStream(b"abcdefghij")
    response = Response(
        stream,
        "https://example.com/media",
        {"X-Test": "value"},
        status=206,
        extensions={"test": True},
    )
    limited = youtube._LimitedResponse(response, ByteLimiter(4))

    assert limited.read(7) == b"abcdefg"
    assert limited.read() == b"hij"
    assert limited.read(2) == b""
    assert limited.read(0) == b""
    assert stream.read_sizes == [4, 3, 4, 4, 2, 0]
    assert limited.headers is response.headers
    assert limited.status == 206
    assert limited.url == response.url
    assert limited.extensions == {"test": True}
    limited.close()
    assert response.closed


def test_limited_response_closes_when_source_closes() -> None:
    class ClosingResponse(Response):
        def read(self, amt: int | None = None) -> bytes:
            chunk = super().read(amt)
            self.close()
            return chunk

    response = ClosingResponse(BytesIO(b"a"), "https://example.com/media", {})
    limited = youtube._LimitedResponse(response, ByteLimiter(1))

    assert limited.read(1) == b"a"
    assert limited.closed


def test_limited_downloader_shares_responses_and_rejects_ffmpeg_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = ByteLimiter(4)
    streams = [RecordingStream(b"abcd"), RecordingStream(b"efgh")]

    class Base:
        def __init__(self, options: dict[str, Any]) -> None:
            self.params = options
            self.download_calls = 0

        def urlopen(self, request: Any) -> Response:
            return Response(streams.pop(0), str(request), {})

        def dl(
            self,
            name: str,
            info: dict[str, Any],
            subtitle: bool = False,
            test: bool = False,
        ) -> bool:
            self.download_calls += 1
            return True

    downloader = youtube._limited_downloader(Base, limiter)({})
    first = downloader.urlopen("first")
    second = downloader.urlopen("second")
    assert first.read() == b"abcd"
    assert second.read() == b"efgh"

    monkeypatch.setattr(
        youtube, "get_suitable_downloader", lambda *args, **kwargs: object
    )
    assert downloader.dl("media", {"url": "https://example.com"}) is True
    assert downloader.download_calls == 1

    monkeypatch.setattr(
        youtube, "get_suitable_downloader", lambda *args, **kwargs: youtube.FFmpegFD
    )
    with pytest.raises(DownloadError, match="external network downloader"):
        downloader.dl("media", {"url": "https://example.com"}, test=True)


@pytest.mark.parametrize("fail_media", [False, True])
def test_limited_downloader_excludes_extraction_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_media: bool
) -> None:
    metadata_url = "https://www.youtube.com/watch?v=abcdefghijk"
    media_url = "https://example.com/media.mp4"
    bodies = {
        metadata_url: b"watch page",
        "https://www.youtube.com/youtubei/v1/player": b'{"videoDetails": {}}',
        "https://www.youtube.com/player.js": b"player javascript",
        media_url: b"media payload",
    }
    limiter = ByteLimiter(4)
    reservations: list[int] = []
    refunds: list[int] = []
    monkeypatch.setattr(limiter, "acquire", reservations.append)
    monkeypatch.setattr(limiter, "refund", refunds.append)

    def urlopen(self: Any, request: Any) -> Response:
        url = request if isinstance(request, str) else request.url
        if fail_media and url == media_url:
            raise HTTPError(Response(BytesIO(), url, {}, status=404))
        body = bodies[url]
        return Response(BytesIO(body), url, {"Content-Length": str(len(body))})

    def extract(self: YoutubeIE, url: str) -> dict[str, Any]:
        self._download_webpage(url, "abcdefghijk")
        self._download_json("https://www.youtube.com/youtubei/v1/player", "abcdefghijk")
        self._download_webpage("https://www.youtube.com/player.js", "abcdefghijk")
        return {"id": "abcdefghijk", "title": "Video", "url": media_url, "ext": "mp4"}

    monkeypatch.setattr(youtube.YoutubeDL, "urlopen", urlopen)
    monkeypatch.setattr(YoutubeIE, "_real_extract", extract)
    downloader_type = youtube._limited_downloader(youtube.YoutubeDL, limiter)
    with downloader_type(YouTubeClient()._options()) as downloader:
        info = downloader.extract_info(metadata_url, download=False)
        assert reservations == []
        path = tmp_path / "media.mp4"
        if fail_media:
            with pytest.raises(HTTPError, match="HTTP Error 404"):
                downloader.dl(str(path), info)
        else:
            assert downloader.dl(str(path), info) == (True, True)
            assert path.read_bytes() == bodies[media_url]
            assert sum(reservations) - sum(refunds) == len(bodies[media_url])
        transfer_reservations = reservations.copy()
        with downloader.urlopen(metadata_url) as response:
            assert response.read() == bodies[metadata_url]
        assert reservations == transfer_reservations


def test_read_only_cookies_are_loaded_without_modifying_the_export(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cookies.txt"
    contents = (
        "# Netscape HTTP Cookie File\n"
        ".youtube.com\tTRUE\t/\tTRUE\t0\tSAPISID\texported-token\n"
    )
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o400)
    client = YouTubeClient(path)

    try:
        with (
            youtube.YoutubeDL(client._options()) as first,
            youtube.YoutubeDL(client._options()) as second,
        ):
            assert first.cookiejar.get_cookie_header("https://www.youtube.com") == (
                "SAPISID=exported-token"
            )
            first.cookiejar.clear()
            assert second.cookiejar.get_cookie_header("https://www.youtube.com") == (
                "SAPISID=exported-token"
            )
        assert path.read_text(encoding="utf-8") == contents
    finally:
        path.chmod(0o600)


@pytest.mark.parametrize("download", [False, True])
def test_private_video_extractor_errors_are_classified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, download: bool
) -> None:
    def extract(self: YoutubeIE, url: str) -> None:
        raise ExtractorError("Private video", expected=True)

    monkeypatch.setattr(YoutubeIE, "_real_extract", extract)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(10, 0, 10)
    )

    with pytest.raises(youtube.VideoUnavailableError, match="Private video"):
        if download:
            YouTubeClient().download_video(
                "abcdefghijk", tmp_path, minimum_free_bytes=0, estimated_bytes=None
            )
        else:
            YouTubeClient().video_metadata("abcdefghijk")


@pytest.mark.parametrize("operation", ["resolve", "recent", "history"])
def test_channel_extractor_failures_propagate(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    def extract(self: YoutubeTabIE, url: str) -> None:
        raise ExtractorError("HTTP Error 503", expected=True)

    monkeypatch.setattr(YoutubeTabIE, "_real_extract", extract)
    channel = ResolvedChannel(
        "UCaaaaaaaaaaaaaaaaaaaaaa",
        "Channel",
        "@channel",
        "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    client = YouTubeClient()

    with pytest.raises(DownloadError, match="HTTP Error 503"):
        if operation == "resolve":
            client.resolve_channel("@channel")
        elif operation == "recent":
            client.recent_videos(channel)
        else:
            client.scan_history(channel, lambda _: pytest.fail("unexpected batch"))


@pytest.mark.parametrize("operation", ["recent", "history"])
def test_incomplete_channel_continuations_fail_scans(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    channel = ResolvedChannel(
        "UCaaaaaaaaaaaaaaaaaaaaaa",
        "Channel",
        "@channel",
        "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    tab = {
        "content": {
            "richGridRenderer": {
                "contents": [
                    {
                        "richItemRenderer": {
                            "content": {"videoRenderer": {"videoId": "abcdefghijk"}}
                        }
                    },
                    {
                        "continuationItemRenderer": {
                            "continuationEndpoint": {
                                "continuationCommand": {"token": "next-page"}
                            }
                        }
                    },
                ]
            }
        }
    }
    continuation_calls = 0

    def extract(self: YoutubeTabIE, url: str) -> dict[str, Any]:
        return self.playlist_result(
            self._entries(tab, channel.id, {}, None, None), channel.id
        )

    def incomplete_response(self: YoutubeTabIE, **kwargs: Any) -> dict[str, Any]:
        nonlocal continuation_calls
        assert kwargs["query"]["continuation"] == "next-page"
        continuation_calls += 1
        return {}

    monkeypatch.setattr(YoutubeTabIE, "_real_extract", extract)
    monkeypatch.setattr(YoutubeTabIE, "_call_api", incomplete_response)
    monkeypatch.setattr(
        youtube.YoutubeDL,
        "urlopen",
        lambda *args, **kwargs: pytest.fail("unexpected network request"),
    )
    client = YouTubeClient()

    with pytest.raises(DownloadError, match="Incomplete data received"):
        if operation == "recent":
            client.recent_videos(channel, content_types={"video"})
        else:
            client.scan_history(channel, lambda _: None, content_types={"video"})

    assert continuation_calls > 0


def test_missing_channel_tabs_are_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def extract(self: YoutubeTabIE, url: str) -> None:
        tab = url.rsplit("/", 1)[-1]
        raise ExtractorError(f"This channel does not have a {tab} tab", expected=True)

    monkeypatch.setattr(YoutubeTabIE, "_real_extract", extract)
    channel = ResolvedChannel(
        "UCaaaaaaaaaaaaaaaaaaaaaa",
        "Channel",
        "@channel",
        "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    client = YouTubeClient()

    assert client.recent_videos(channel) == ()
    client.scan_history(channel, lambda _: pytest.fail("unexpected batch"))
