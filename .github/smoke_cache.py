"""Verify downloader caches under both image and remapped runtime identities."""

import os
from pathlib import Path

from yt_dlp import YoutubeDL

from core.youtube import YouTubeClient


def main() -> None:
    assert vars(os)["getuid"]() == int(os.environ["PUID"])
    assert vars(os)["getgid"]() == int(os.environ["PGID"])
    assert Path.home().is_dir()
    marker = {"cache": "writable"}
    with YoutubeDL(YouTubeClient()._options()) as downloader:
        downloader.cache.store("youtube-sigfuncs", "saga-smoke", marker)
        assert downloader.cache.load("youtube-sigfuncs", "saga-smoke") == marker
    deno_marker = Path(os.environ["DENO_DIR"]) / "saga-smoke"
    deno_marker.write_text("writable", encoding="utf-8")
    assert deno_marker.read_text(encoding="utf-8") == "writable"
    deno_marker.unlink()


if __name__ == "__main__":
    main()
