"""Check image transports, media tools, UID, startup, and health on fixture data."""

import asyncio
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import niquests
import requests
import urllib3
import urllib3_future
from yt_dlp.networking import _requests

from core.config import load_settings, load_youtube_configs
from core.healthcheck import is_healthy
from core.service import SagaService
from core.state import DiscoveredVideo, ResolvedChannel, StateStore
from core.youtube import YouTubeClient


class FixtureYouTube(YouTubeClient):
    def resolve_channel(self, reference: str) -> ResolvedChannel:
        return ResolvedChannel(
            "UC" + "A" * 22, "Fixture", "@SmokeChannel", "https://example.com/channel"
        )

    def recent_videos(self, *args: Any, **kwargs: Any) -> tuple[DiscoveredVideo, ...]:
        return ()

    def scan_history(self, *args: Any, **kwargs: Any) -> None:
        pass


async def main() -> None:
    assert vars(os)["getuid"]() == 1000
    assert vars(requests.packages)["urllib3"] is urllib3
    assert _requests.urllib3 is urllib3
    assert niquests.packages.urllib3 is urllib3_future
    assert urllib3.__file__ != urllib3_future.__file__
    for tool in ("ffmpeg", "deno"):
        subprocess.run(
            [tool, "-version" if tool == "ffmpeg" else "--version"],
            check=True,
            capture_output=True,
            timeout=10,
        )
    config = Path("/config/config.toml")
    config.write_text(
        '[[instances.youtube]]\nname = "fixture"\n'
        '[[instances.youtube.channels]]\nreference = "@SmokeChannel"\n'
        'discord_webhook_urls = ["https://discord.com/api/webhooks/1/fixture"]\n',
        encoding="utf-8",
    )
    os.environ["INTERNET_ARCHIVE_EMAIL"] = "smoke@example.com"
    os.environ["INTERNET_ARCHIVE_PASSWORD"] = "fixture-password"
    root = Path("/app")
    settings = load_settings(root)
    state = StateStore(settings.data_path / "saga.sqlite")
    service = SagaService(
        settings, load_youtube_configs(config), state, FixtureYouTube()
    )
    task = asyncio.create_task(service.run())
    try:
        for _ in range(200):
            if is_healthy(root):
                break
            await asyncio.sleep(0.05)
        assert is_healthy(root)
        document = json.loads((settings.data_path / "transfers.json").read_text())
        assert (
            document["version"] == 2
            and document["lanes"]["download"]["active_count"] == 0
        )
    finally:
        service.request_stop()
        await task
        state.close()


if __name__ == "__main__":
    asyncio.run(main())
