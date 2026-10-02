from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any

import niquests
import pytest
import requests
import urllib3
import urllib3_future
from yt_dlp import YoutubeDL
from yt_dlp.networking import _requests
from yt_dlp.utils import DownloadError

from core.bandwidth import ByteLimiter
from core.youtube import _limited_downloader, media_retry_options


def test_clients_use_separate_transport_namespaces() -> None:
    assert getattr(requests.packages, "urllib3") is urllib3
    assert niquests.packages.urllib3 is urllib3_future
    assert _requests.urllib3 is urllib3
    assert not hasattr(urllib3, "AsyncPoolManager")
    assert hasattr(urllib3_future, "AsyncPoolManager")
    assert urllib3.__file__ != urllib3_future.__file__


@pytest.mark.parametrize("limited", [False, True])
@pytest.mark.parametrize("always_fail", [False, True])
def test_real_disconnect_retries_keep_payload_and_resume_offset(
    tmp_path: Path, limited: bool, always_fail: bool
) -> None:
    payload = bytes(range(256)) * 1024
    offsets: list[int] = []

    class Server(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            offset = int(
                self.headers.get("Range", "bytes=0-").split("=")[1].split("-")[0]
            )
            offsets.append(offset)
            self.send_response(206 if offset else 200)
            self.send_header("Content-Length", str(len(payload) - offset))
            if offset:
                self.send_header(
                    "Content-Range", f"bytes {offset}-{len(payload) - 1}/{len(payload)}"
                )
            self.end_headers()
            end = (
                min(offset + 32768, len(payload) - 1)
                if always_fail or len(offsets) == 1
                else len(payload)
            )
            self.wfile.write(payload[offset:end])
            self.close_connection = True

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Server) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        options = {
            **media_retry_options(),
            "quiet": True,
            "noprogress": True,
            "outtmpl": str(tmp_path / "media.bin"),
        }
        # Exercise retries immediately; the separately checked policy supplies sleeps.
        options["retry_sleep_functions"] = {"http": lambda n: 0}
        downloader_type = (
            _limited_downloader(YoutubeDL, ByteLimiter(100_000_000))
            if limited
            else YoutubeDL
        )
        try:
            with downloader_type(options) as downloader:
                info = {
                    "id": "fixture",
                    "title": "fixture",
                    "url": f"http://127.0.0.1:{server.server_port}/media",
                    "ext": "bin",
                }
                if always_fail:
                    with pytest.raises(DownloadError):
                        downloader.process_info(info)
                    assert len(offsets) == 4
                    assert not (tmp_path / "media.bin").exists()
                else:
                    downloader.process_info(info)
                    assert (tmp_path / "media.bin").read_bytes() == payload
                    assert offsets[0] == 0 and offsets[1] > 0
            assert media_retry_options()["retry_sleep_functions"]["http"](4) == 8
        finally:
            server.shutdown()
            thread.join()
