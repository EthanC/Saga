"""Resolve YouTube channels and enumerate videos through yt-dlp."""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import md5, sha1
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from loguru import logger
from yt_dlp import YoutubeDL
from yt_dlp.downloader import FFmpegFD, get_suitable_downloader
from yt_dlp.networking.common import Response
from yt_dlp.utils import DownloadError

from core.bandwidth import ByteLimiter, Limiter
from core.config import ALL_CONTENT_TYPES, ContentType
from core.logging import DownloaderLogger, redact_diagnostics
from core.shutdown import StopController
from core.staging import (
    StagingIdentityError,
    completed_download_size,
    download_reservation,
    inventory_inputs,
    prepare_inputs,
)
from core.state import DiscoveredVideo, ResolvedChannel
from core.workers import run_managed
from core.youtube_errors import VideoAccessError, cookie_fingerprint, is_access_error

DEFERRED_LIVE_STATUSES = {"is_live", "is_upcoming", "post_live"}
CONTENT_TABS: tuple[tuple[ContentType, str], ...] = (
    ("livestream", "streams"),
    ("short", "shorts"),
    ("video", "videos"),
)


class VideoUnavailableError(RuntimeError):
    """Indicate that YouTube reports a video as inaccessible or unavailable."""


class DiskSpaceError(RuntimeError):
    """Indicate that the download volume lacks the configured free space."""


@dataclass(frozen=True, slots=True)
class VideoMetadata:
    """Contain full metadata needed to freeze an Archive.org item."""

    id: str
    title: str
    description: str
    upload_date: str
    channel_id: str
    channel_name: str
    channel_handle: str
    channel_url: str
    webpage_url: str
    estimated_bytes: int | None
    published_at: datetime | None = None
    published_precise: bool = False
    tags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MediaFile:
    """Contain the verified local media file and its checksums."""

    path: Path
    size: int
    md5: str
    sha1: str


class YouTubeClient:
    """Provide the blocking yt-dlp operations used by Saga workers."""

    def __init__(
        self,
        cookies_path: Path | None = None,
        download_limiter: Limiter | None = None,
        *,
        pot_provider_url: str = "http://127.0.0.1:4416",
        stop_controller: StopController | None = None,
        managed: bool = False,
    ) -> None:
        self.cookies_path = cookies_path
        self.download_limiter = download_limiter or ByteLimiter(0)
        self.pot_provider_url = pot_provider_url
        self.stop_controller = stop_controller or StopController()
        self.managed = managed
        logger.debug(
            "Initialized YouTube client (cookies {}, PO token provider {})",
            "enabled" if cookies_path is not None else "disabled",
            pot_provider_url,
        )

    def resolve_channel(self, reference: str) -> ResolvedChannel:
        """Resolve a configured handle or URL to a canonical channel ID."""
        self.stop_controller.check()
        if self.managed:
            return self._managed("resolve_channel", (reference,), {})
        base_url = _channel_url(reference)
        logger.debug("Requesting YouTube channel metadata for {}", reference)
        options = self._options()
        options.update({"extract_flat": True})
        with _limited_downloader(
            YoutubeDL, self.download_limiter, self.stop_controller
        )(options) as downloader:
            info: Any = downloader.extract_info(f"{base_url}/about", download=False)

        if not isinstance(info, dict):
            raise ValueError(f"yt-dlp returned no channel data for {reference!r}")
        channel_id = info.get("channel_id")
        if not isinstance(channel_id, str) or not channel_id.startswith("UC"):
            raise ValueError(
                f"Could not resolve a canonical channel ID for {reference!r}"
            )

        title = info.get("channel") or info.get("uploader") or info.get("title")
        if not isinstance(title, str) or not title.strip():
            title = channel_id
        handle = info.get("uploader_id")
        if not isinstance(handle, str) or not handle.startswith("@"):
            handle = None

        resolved = ResolvedChannel(
            id=channel_id,
            title=title.strip(),
            handle=handle,
            url=f"https://www.youtube.com/channel/{channel_id}",
        )
        logger.debug("Resolved YouTube channel {} to {}", reference, channel_id)
        return resolved

    def recent_videos(
        self,
        channel: ResolvedChannel,
        limit_per_tab: int = 30,
        *,
        content_types: Iterable[ContentType] = ALL_CONTENT_TYPES,
    ) -> tuple[DiscoveredVideo, ...]:
        """Return a deduplicated recent snapshot across enabled channel tabs."""
        self.stop_controller.check()
        if self.managed:
            return self._managed(
                "recent_videos",
                (channel, limit_per_tab),
                {"content_types": tuple(content_types)},
            )
        enabled = frozenset(content_types)
        logger.debug(
            "Fetching recent {} content for channel {}",
            ", ".join(sorted(enabled)),
            channel.id,
        )
        found: dict[str, DiscoveredVideo] = {}
        for content_type, tab in CONTENT_TABS:
            if content_type not in enabled:
                continue
            for video in self._tab_videos(channel, tab, content_type, limit_per_tab):
                found.setdefault(video.id, video)
        videos = tuple(found.values())
        logger.debug("Fetched {} recent video(s) for {}", len(videos), channel.id)
        return videos

    def scan_history(
        self,
        channel: ResolvedChannel,
        record_batch: Callable[[tuple[DiscoveredVideo, ...]], None],
        batch_size: int = 100,
        *,
        content_types: Iterable[ContentType] = ALL_CONTENT_TYPES,
    ) -> None:
        """Enumerate enabled tabs and persist bounded batches through a callback."""
        self.stop_controller.check()
        if self.managed:
            self._managed(
                "scan_history",
                (channel,),
                {"batch_size": batch_size, "content_types": tuple(content_types)},
                {"record_batch": record_batch},
            )
            return
        enabled = frozenset(content_types)
        logger.debug(
            "Starting full history scan for channel {} ({})",
            channel.id,
            ", ".join(sorted(enabled)),
        )
        seen: set[str] = set()
        batch: list[DiscoveredVideo] = []
        for content_type, tab in CONTENT_TABS:
            if content_type not in enabled:
                continue
            for video in self._tab_videos(channel, tab, content_type, None):
                if video.id in seen:
                    continue
                seen.add(video.id)
                batch.append(video)
                if len(batch) >= batch_size:
                    record_batch(tuple(batch))
                    batch.clear()
        if batch:
            record_batch(tuple(batch))
        logger.debug(
            "Completed history scan for {} with {} video(s)", channel.id, len(seen)
        )

    def video_metadata(self, video_id: str) -> VideoMetadata:
        """Extract full metadata for one replay without downloading it."""
        self.stop_controller.check()
        if self.managed:
            return self._managed("video_metadata", (video_id,), {})
        logger.debug("Fetching full metadata for video {}", video_id)
        options = self._options()
        options.update({"noplaylist": True})
        generation = _cookie_generation(options)
        try:
            with _limited_downloader(
                YoutubeDL, self.download_limiter, self.stop_controller
            )(options) as downloader:
                info: Any = downloader.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
        except DownloadError as error:
            raise _classify_download_error(
                error, cookie_generation=generation
            ) from error
        if not isinstance(info, dict):
            raise RuntimeError(f"yt-dlp returned no metadata for {video_id}")
        metadata = _full_metadata(info, video_id)
        logger.debug("Fetched full metadata for video {}", video_id)
        return metadata

    def download_video(
        self,
        video_id: str,
        directory: Path,
        *,
        minimum_free_bytes: int,
        estimated_bytes: int | None,
        disk_guard: Callable[[int, int], None] | None = None,
        operation_id: str | None = None,
        phase_hook: Callable[[str, int], None] | None = None,
    ) -> Path:
        """Resume a strict bestvideo+bestaudio download and return its MKV path."""
        self.stop_controller.check()
        if self.managed:
            merging_active = False

            def phase(phase: str, amount: int) -> None:
                nonlocal merging_active
                merging_active = phase == "merging"
                if phase_hook:
                    phase_hook(phase, amount)

            def probe() -> None:
                path = directory / "merging.temp.mkv"
                if merging_active and phase_hook and path.is_file():
                    phase_hook("merging", path.stat().st_size)

            return self._managed(
                "download_video",
                (video_id, directory),
                {
                    "minimum_free_bytes": minimum_free_bytes,
                    "estimated_bytes": estimated_bytes,
                    "operation_id": operation_id,
                    "use_parent_guard": disk_guard is not None,
                },
                {
                    "disk_guard": disk_guard or (lambda *args: None),
                    "phase_hook": phase,
                    "merge_probe": probe,
                },
            )
        if directory.is_symlink():
            raise StagingIdentityError("symlink download operation directory")
        directory.mkdir(parents=True, exist_ok=True)
        output = directory / "media.mkv"
        marker = directory / "download.json"
        if any(
            path.is_symlink()
            for path in (
                output,
                marker,
                directory / "merging.mkv",
                directory / "merging.temp.mkv",
                directory / "download.tmp",
            )
        ):
            raise StagingIdentityError("symlink in download output or receipt")
        if (
            completed_download_size(directory, operation_id or directory.name, video_id)
            is not None
        ):
            return output
        # A final filename without a completion receipt is not a completed download.
        output.unlink(missing_ok=True)
        marker.unlink(missing_ok=True)
        merging = directory / "merging.mkv"
        (directory / "merging.temp.mkv").unlink(missing_ok=True)
        if merging.exists():
            merging.unlink()
        # ffmpeg keeps both input streams while writing the merged output.
        reusable = (
            inventory_inputs(directory, operation_id, video_id).reusable_bytes
            if operation_id
            else 0
        )
        required = minimum_free_bytes + download_reservation(
            estimated_bytes or 0, reusable
        )
        free = shutil.disk_usage(directory).free
        if free < required:
            logger.warning(
                "Insufficient disk space for video {}: {} bytes free, {} required",
                video_id,
                free,
                required,
            )
            raise DiskSpaceError(f"download volume has less than {required} bytes free")

        logger.info("Downloading video {} to archive staging", video_id)

        options = self._options()
        streams: dict[str, tuple[int, int]] = {}
        generation = _cookie_generation(options)

        def guard(event: dict[str, Any]) -> None:
            self.stop_controller.check()
            downloaded = max(int(event.get("downloaded_bytes") or 0), 0)
            total = max(
                int(
                    event.get("total_bytes")
                    or event.get("total_bytes_estimate")
                    or downloaded
                ),
                downloaded,
            )
            streams[str(event.get("filename", "media")).removesuffix(".part")] = (
                total,
                downloaded,
            )
            expected = max(
                sum(value[0] for value in streams.values()), estimated_bytes or 0
            )
            if disk_guard:
                disk_guard(expected, sum(value[1] for value in streams.values()))
            elif shutil.disk_usage(
                directory
            ).free < minimum_free_bytes + download_reservation(
                expected, sum(value[1] for value in streams.values())
            ):
                raise DiskSpaceError("insufficient space for temporary merge output")
            if event.get("status") == "finished" and phase_hook:
                phase_hook("stream_finished", 0)

        def before_merge(event: dict[str, Any]) -> None:
            self.stop_controller.check()
            if event.get("status") != "started":
                if event.get("status") == "finished" and phase_hook:
                    phase_hook(
                        "merged", merging.stat().st_size if merging.exists() else 0
                    )
                return
            expected = max(
                sum(value[0] for value in streams.values()), estimated_bytes or 0
            )
            downloaded = sum(value[1] for value in streams.values())
            if disk_guard:
                disk_guard(expected, downloaded)
            elif shutil.disk_usage(
                directory
            ).free < minimum_free_bytes + download_reservation(expected, downloaded):
                raise DiskSpaceError("insufficient space before merge")
            if phase_hook:
                phase_hook("merging", 0)

        options.update(
            {
                **media_retry_options(self.stop_controller),
                "skip_download": False,
                "noplaylist": True,
                "format": "bestvideo+bestaudio",
                "merge_output_format": "mkv",
                "continuedl": True,
                "skip_unavailable_fragments": False,
                "overwrites": False,
                "progress_hooks": [guard],
                "postprocessor_hooks": [before_merge],
                "outtmpl": {"default": str(directory / "merging.%(ext)s")},
            }
        )
        downloader_type: type[Any] = YoutubeDL
        if self.download_limiter.enabled:
            options.update(
                {
                    "concurrent_fragment_downloads": 1,
                    "external_downloader": {"default": "native"},
                    "external_downloader_args": {
                        "ffmpeg_i": ["-protocol_whitelist", "file,crypto,pipe"]
                    },
                    "hls_prefer_native": True,
                }
            )
        downloader_type = _limited_downloader(
            YoutubeDL, self.download_limiter, self.stop_controller
        )
        if operation_id:

            def prepare(info: dict[str, Any]) -> None:
                if info.get("id") != video_id:
                    raise RuntimeError("selected video does not match operation")
                selected = info.get("requested_formats") or [info]
                inventory = prepare_inputs(directory, operation_id, video_id, selected)
                for path in inventory.files:
                    if (
                        path.name.startswith("merging.")
                        and not path.name.endswith(".ytdl")
                        and "-Frag" not in path.name
                    ):
                        size = path.stat().st_size
                        streams[str(path).removesuffix(".part")] = size, size
                expected = max(
                    estimated_bytes or 0,
                    inventory.reusable_bytes,
                    sum(
                        item.get("filesize") or item.get("filesize_approx") or 0
                        for item in selected
                    ),
                )
                if disk_guard:
                    disk_guard(expected, inventory.reusable_bytes)
                elif shutil.disk_usage(
                    directory
                ).free < minimum_free_bytes + download_reservation(
                    expected, inventory.reusable_bytes
                ):
                    raise DiskSpaceError("selected input and merge disk budget full")

            downloader_type = _staging_downloader(downloader_type, prepare)
        try:
            with downloader_type(options) as downloader:
                result = downloader.download(
                    [f"https://www.youtube.com/watch?v={video_id}"]
                )
        except DownloadError as error:
            raise _classify_download_error(
                error, cookie_generation=generation
            ) from error
        if result != 0:
            raise RuntimeError(f"yt-dlp exited with status {result} for {video_id}")
        if not merging.is_file() or merging.stat().st_size == 0:
            raise RuntimeError(
                f"yt-dlp did not produce the expected MKV for {video_id}"
            )
        merging.replace(output)
        temporary_marker = marker.with_suffix(".tmp")
        temporary_marker.write_text(
            json.dumps({"video_id": video_id, "size": output.stat().st_size}),
            encoding="utf-8",
        )
        temporary_marker.replace(marker)
        logger.info("Downloaded video {}", video_id)
        return output

    def selected_formats(self, video_id: str) -> tuple[list[dict[str, Any]], int]:
        """Resolve strict input identity for inactive partials without writing media."""
        self.stop_controller.check()
        if self.managed:
            return self._managed("selected_formats", (video_id,), {})
        options = {
            **self._options(),
            "noplaylist": True,
            "format": "bestvideo+bestaudio",
        }
        generation = _cookie_generation(options)
        try:
            with _limited_downloader(
                YoutubeDL, self.download_limiter, self.stop_controller
            )(options) as downloader:
                info: Any = downloader.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
        except DownloadError as error:
            raise _classify_download_error(
                error, cookie_generation=generation
            ) from error
        if not isinstance(info, dict) or info.get("id") != video_id:
            raise StagingIdentityError(
                "selected video does not match partial operation"
            )
        selected = info.get("requested_formats") or [info]
        expected = sum(
            item.get("filesize") or item.get("filesize_approx") or 0
            for item in selected
        )
        return selected, expected

    def _managed(
        self,
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        callbacks: dict[str, Callable[..., Any]] | None = None,
    ) -> Any:
        return run_managed(
            "youtube",
            method,
            {
                "cookies_path": self.cookies_path,
                "pot_provider_url": self.pot_provider_url,
                "limiter_enabled": self.download_limiter.enabled,
                "chunk_size": self.download_limiter.chunk_size,
            },
            args,
            kwargs,
            self.stop_controller,
            {
                "acquire": self.download_limiter.acquire,
                "refund": self.download_limiter.refund,
                **(callbacks or {}),
            },
        )

    def _tab_videos(
        self,
        channel: ResolvedChannel,
        tab: str,
        content_type: ContentType,
        limit: int | None,
    ) -> Iterable[DiscoveredVideo]:
        options = self._options()
        options.update({"extract_flat": True, "lazy_playlist": True})
        options.setdefault("extractor_args", {})["youtube"] = {
            "raise_incomplete_data": ["true"]
        }
        if limit is not None:
            options["playlist_items"] = f"1:{limit}"

        logger.trace("Reading YouTube {} tab for channel {}", tab, channel.id)
        with _limited_downloader(
            YoutubeDL, self.download_limiter, self.stop_controller
        )(options) as downloader:
            try:
                info: Any = downloader.extract_info(
                    f"{channel.url}/{tab}", download=False
                )
            except DownloadError as error:
                if f"This channel does not have a {tab} tab" in str(error):
                    logger.debug("Channel {} has no {} tab", channel.id, tab)
                    return
                raise
            if not isinstance(info, dict) or info.get("entries") is None:
                raise RuntimeError(
                    f"yt-dlp returned no {tab} tab data for {channel.id}"
                )
            for entry in info["entries"]:
                self.stop_controller.check()
                video = _video(entry, content_type)
                if video is not None:
                    yield video

    def _options(self) -> dict[str, Any]:
        options: dict[str, Any] = {
            "quiet": True,
            "no_warnings": False,
            "noprogress": True,
            "logger": DownloaderLogger(),
            "skip_download": True,
            "ignoreerrors": False,
            "socket_timeout": 30,
        }
        if self.cookies_path is not None:
            # yt-dlp saves cookies on close; keep each worker's updates in memory.
            options["cookiefile"] = StringIO(
                self.cookies_path.read_text(encoding="utf-8")
            )
        options["extractor_args"] = {
            "youtubepot-bgutilhttp": {"base_url": [self.pot_provider_url]}
        }
        return options


class _LimitedResponse(Response):
    def __init__(self, response: Response, limiter: Limiter) -> None:
        super().__init__(
            response,
            response.url,
            {},
            response.status,
            response.reason,
            response.extensions,
        )
        self.headers = response.headers
        self.limiter = limiter

    def read(self, amt: int | None = None) -> bytes:
        if amt is not None and amt >= 0:
            if amt == 0:
                return self._read_once(0)
            chunks: list[bytes] = []
            remaining = amt
            while remaining:
                requested = min(remaining, self.limiter.chunk_size)
                chunk = self._read_once(requested)
                chunks.append(chunk)
                remaining -= len(chunk)
                if len(chunk) < requested:
                    break
            return b"".join(chunks)

        chunks = []
        while chunk := self._read_once(self.limiter.chunk_size):
            chunks.append(chunk)
        return b"".join(chunks)

    def _read_once(self, amount: int) -> bytes:
        if amount == 0:
            return self.fp.read(0)
        self.limiter.acquire(amount)
        try:
            chunk = self.fp.read(amount)
        except Exception:
            self.limiter.refund(amount)
            raise
        self.limiter.refund(amount - len(chunk))
        if self.fp.closed:
            self.close()
        return chunk


def _limited_downloader(
    downloader_type: type[Any],
    limiter: Limiter,
    controller: StopController | None = None,
) -> type[Any]:
    class LimitedYoutubeDL(downloader_type):
        _media_download_active = False

        def urlopen(self, request: Any) -> Response:
            if controller:
                controller.check()
            response = super().urlopen(request)
            if self._media_download_active:
                return _LimitedResponse(response, limiter)
            return response

        def dl(
            self,
            name: str,
            info: dict[str, Any],
            subtitle: bool = False,
            test: bool = False,
        ) -> bool:
            params = self.params if not test else {**self.params, "test": True}
            if controller:
                controller.check()
            if (
                limiter.enabled
                and get_suitable_downloader(info, params, to_stdout=name == "-")
                is FFmpegFD
            ):
                raise DownloadError(
                    "selected formats require an external network downloader, "
                    "which is disabled while bandwidth limiting is enabled"
                )
            previous = self._media_download_active
            self._media_download_active = True
            try:
                return super().dl(name, info, subtitle, test)
            finally:
                self._media_download_active = previous

    return LimitedYoutubeDL


def media_retry_options(controller: StopController | None = None) -> dict[str, Any]:
    """Bound per-request retries without changing metadata's outer retry policy."""

    def retry_sleep(n: int) -> float:
        seconds = min(2 ** max(n, 0), 8)
        if controller:
            controller.wait(seconds)
            return 0
        return seconds

    return {
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "retry_sleep_functions": {kind: retry_sleep for kind in ("http", "fragment")},
    }


def _staging_downloader(
    downloader_type: type[Any], prepare: Callable[[dict[str, Any]], None]
) -> type[Any]:
    class StagingYoutubeDL(downloader_type):
        def process_info(self, info: dict[str, Any]) -> Any:
            prepare(info)
            return super().process_info(info)

    return StagingYoutubeDL


def _channel_url(reference: str) -> str:
    value = reference.strip().rstrip("/")
    if value.startswith("UC") and len(value) == 24:
        return f"https://www.youtube.com/channel/{value}"
    if value.startswith("@"):
        return f"https://www.youtube.com/{value}"

    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc:
        segments = [segment for segment in parsed.path.split("/") if segment]
        keep = 1 if segments and segments[0].startswith("@") else 2
        if segments and segments[0] in {"channel", "c", "user"}:
            keep = 2
        path = "/" + "/".join(segments[:keep])
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return value


def _video(value: Any, content_type: ContentType = "video") -> DiscoveredVideo | None:
    if not isinstance(value, dict):
        return None
    video_id = value.get("id")
    if not isinstance(video_id, str) or len(video_id) != 11:
        return None

    title = value.get("title")
    if not isinstance(title, str) or not title.strip():
        title = None
    live_status = value.get("live_status")
    if not isinstance(live_status, str):
        live_status = None

    published_at, published_precise = _publication(value)
    return DiscoveredVideo(
        id=video_id,
        title=title,
        published_at=published_at,
        live_status=live_status,
        replay_ready=live_status not in DEFERRED_LIVE_STATUSES,
        published_precise=published_precise,
        content_type=content_type,
    )


def _published_at(value: dict[str, Any]) -> datetime | None:
    return _publication(value)[0]


def _publication(value: dict[str, Any]) -> tuple[datetime | None, bool]:
    timestamp = value.get("release_timestamp") or value.get("timestamp")
    if isinstance(timestamp, int | float) and not isinstance(timestamp, bool):
        return datetime.fromtimestamp(timestamp, UTC), True

    upload_date = value.get("upload_date")
    if isinstance(upload_date, str) and len(upload_date) == 8 and upload_date.isdigit():
        return datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=UTC), False
    return None, False


def verify_media(path: Path, *, timeout: float = 120.0) -> MediaFile:
    """Require audio and video streams, then hash the unchanged local file."""
    logger.debug("Verifying downloaded media at {}", path)
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "json",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    document: Any = json.loads(result.stdout)
    streams = document.get("streams", []) if isinstance(document, dict) else []
    kinds = {stream.get("codec_type") for stream in streams if isinstance(stream, dict)}
    if not {"audio", "video"}.issubset(kinds):
        raise RuntimeError("ffprobe did not find both audio and video streams")

    md5_hash = md5(usedforsecurity=False)
    sha1_hash = sha1(usedforsecurity=False)
    size = 0
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            size += len(chunk)
            md5_hash.update(chunk)
            sha1_hash.update(chunk)
    if size == 0:
        raise RuntimeError("downloaded media file is empty")
    media = MediaFile(path, size, md5_hash.hexdigest(), sha1_hash.hexdigest())
    logger.info("Verified downloaded media at {} ({} bytes)", path, size)
    return media


def _full_metadata(info: dict[str, Any], expected_id: str) -> VideoMetadata:
    video_id = info.get("id")
    if video_id != expected_id:
        raise RuntimeError(f"yt-dlp returned unexpected video ID {video_id!r}")
    live_status = info.get("live_status")
    if live_status in DEFERRED_LIVE_STATUSES:
        raise RuntimeError(f"video replay is not ready ({live_status})")

    title = _required_text(info, "title")
    channel_id = _required_text(info, "channel_id")
    channel_name = _required_text(info, "channel", fallback="uploader")
    handle = info.get("uploader_id")
    if not isinstance(handle, str) or not handle.strip():
        handle = channel_id
    channel_url = info.get("channel_url")
    if not isinstance(channel_url, str) or not channel_url.strip():
        channel_url = f"https://www.youtube.com/channel/{channel_id}"
    upload_date = info.get("upload_date")
    if (
        not isinstance(upload_date, str)
        or len(upload_date) != 8
        or not upload_date.isdigit()
    ):
        raise RuntimeError("yt-dlp metadata has no valid upload date")
    description = info.get("description")
    if not isinstance(description, str):
        description = ""
    raw_tags = info.get("tags")
    tags = (
        tuple(
            dict.fromkeys(
                tag.strip() for tag in raw_tags if isinstance(tag, str) and tag.strip()
            )
        )
        if isinstance(raw_tags, list)
        else ()
    )

    estimated = info.get("filesize_approx") or info.get("filesize")
    if not isinstance(estimated, int) or isinstance(estimated, bool) or estimated <= 0:
        requested = info.get("requested_formats")
        sizes = (
            [
                item.get("filesize") or item.get("filesize_approx")
                for item in requested
                if isinstance(item, dict)
            ]
            if isinstance(requested, list)
            else []
        )
        estimated = (
            sum(
                size
                for size in sizes
                if isinstance(size, int) and not isinstance(size, bool)
            )
            or None
        )

    published_at, published_precise = _publication(info)
    return VideoMetadata(
        id=video_id,
        title=title,
        description=description,
        upload_date=upload_date,
        channel_id=channel_id,
        channel_name=channel_name,
        channel_handle=handle,
        channel_url=channel_url,
        webpage_url=f"https://www.youtube.com/watch?v={video_id}",
        estimated_bytes=estimated,
        published_at=published_at,
        published_precise=published_precise,
        tags=tags,
    )


def _required_text(
    info: dict[str, Any], name: str, *, fallback: str | None = None
) -> str:
    value = info.get(name)
    if (not isinstance(value, str) or not value.strip()) and fallback is not None:
        value = info.get(fallback)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"yt-dlp metadata has no {name}")
    return value.strip()


def _cookie_generation(options: dict[str, Any]) -> str:
    cookies = options.get("cookiefile")
    return cookie_fingerprint(cookies.getvalue() if cookies is not None else None)


def _classify_download_error(
    error: DownloadError, *, cookie_generation: str | None = None
) -> RuntimeError:
    detail = redact_diagnostics(str(error))
    if is_access_error(detail):
        result = VideoAccessError(detail)
        result.cookie_generation = cookie_generation
        return result
    unavailable_markers = (
        "private video",
        "video unavailable",
        "this video is unavailable",
        "has been removed",
    )
    if any(marker in detail.casefold() for marker in unavailable_markers):
        return VideoUnavailableError(detail)
    return RuntimeError(detail)
