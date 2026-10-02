import json
import sqlite3
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Thread
from typing import Any

import pytest
from niquests import Session as HttpSession
from niquests.exceptions import ConnectionError as HttpConnectionError
from sqlmodel import Session, select
from yt_dlp import YoutubeDL

from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter
from core.migration import rehearse_migration
from core.staging import maintain_partials, prepare_inputs
from core.state import ArchiveProgress, Channel, StateStore, Video, utcnow
from core.youtube import YouTubeClient, _limited_downloader, _staging_downloader
from tests.test_archival import saved, settings
from tests.test_partials import FORMATS, partial


@pytest.mark.parametrize("limited", [False, True])
@pytest.mark.parametrize("changed", [False, True])
def test_adopted_legacy_input_resumes_or_restarts_changed_http_representation(
    tmp_path: Path, limited: bool, changed: bool
) -> None:
    original = bytes(range(256)) * 100
    representation = bytes(reversed(range(256))) * 100 if changed else original
    offsets: list[int] = []

    class Server(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            offset = int(
                self.headers.get("Range", "bytes=0-").split("=")[1].split("-")[0]
            )
            offsets.append(offset)
            resume = offset if not changed else 0
            self.send_response(206 if resume else 200)
            self.send_header("Content-Length", str(len(representation) - resume))
            if resume:
                self.send_header(
                    "Content-Range",
                    f"bytes {resume}-{len(representation) - 1}/{len(representation)}",
                )
            self.end_headers()
            self.wfile.write(representation[resume:])

        def log_message(self, format: str, *args: Any) -> None:
            pass

    directory = tmp_path / "operation"
    directory.mkdir()
    (directory / "media.f315.bin.part").write_bytes(original[:4096])
    with ThreadingHTTPServer(("127.0.0.1", 0), Server) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = (
            _limited_downloader(YoutubeDL, ByteLimiter(100_000_000))
            if limited
            else YoutubeDL
        )

        def prepare(info: dict[str, Any]) -> None:
            prepare_inputs(directory, "operation", "abcdefghijk", [info])

        downloader_type = _staging_downloader(base, prepare)
        try:
            with downloader_type(
                {
                    **YouTubeClient()._options(),
                    "skip_download": False,
                    "outtmpl": str(directory / "merging.f%(format_id)s.%(ext)s"),
                }
            ) as downloader:
                downloader.process_info(
                    {
                        "id": "abcdefghijk",
                        "title": "fixture",
                        "format_id": "315",
                        "ext": "bin",
                        "url": f"http://127.0.0.1:{server.server_port}/media",
                        "protocol": "http",
                    }
                )
            assert offsets[0] == 4096
            assert (directory / "merging.f315.bin").read_bytes() == representation
            assert not (directory / "download.json").exists()
        finally:
            server.shutdown()
            thread.join()


def test_production_shaped_snapshot_rehearsal_preserves_durable_facts(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite"
    state = StateStore(source)
    with Session(state.engine) as session:
        session.add(
            Channel(id="fixture", title="Fixture", canonical_url="https://example.com")
        )
        session.commit()
        for index in range(2530):
            identifier = f"{index:011d}"
            session.add(
                Video(
                    id=identifier,
                    channel_id="fixture",
                    notification_candidate=index >= 2528,
                    webpage_url="https://example.com",
                )
            )
        session.commit()
        for index in range(2530):
            session.add(
                ArchiveProgress(
                    video_id=f"{index:011d}",
                    status="download" if index < 1400 else "metadata",
                    attempts=3,
                    frozen_metadata=json.dumps({"estimated_bytes": 100}),
                    operation_id=f"operation-{index}" if index < 1400 else None,
                )
            )
        session.commit()
        uploaded = session.get(ArchiveProgress, "00000000000")
        assert uploaded
        uploaded.status = "complete"
        uploaded.media_uploaded_at = utcnow()
        uploaded.uploaded_bytes = 100
        uploaded.upload_receipt = "fixture-receipt"
        session.add(uploaded)
        session.commit()
    state.close()
    with closing(sqlite3.connect(source)) as connection, connection:
        for column in (
            "download_bytes",
            "download_progress_at",
            "download_phase",
            "merge_bytes",
            "maintenance_token",
        ):
            connection.execute(f"ALTER TABLE archiveprogress DROP COLUMN {column}")
        connection.execute("DELETE FROM schemamigration WHERE version=3")
    snapshot = rehearse_migration(source, tmp_path / "rehearsal")
    migrated = StateStore(snapshot)
    with Session(migrated.engine) as session:
        rows = session.exec(select(ArchiveProgress)).all()
        assert len(rows) == 2530 and all(row.attempts == 3 for row in rows)
        uploaded = session.get(ArchiveProgress, "00000000000")
        assert (
            uploaded
            and uploaded.upload_receipt == "fixture-receipt"
            and uploaded.media_uploaded_at
        )
        assert uploaded.operation_id == "operation-0"
    claim = migrated.claim_archive(lane="metadata", prefer_recent=True)
    assert claim and claim.video.notification_candidate
    migrated.close()


def test_legacy_inventory_can_validate_identity_before_disk_admission(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    path = partial(state, tmp_path, legacy=True)
    configured = settings(tmp_path)
    assert (
        state.claim_archive(
            lane="download",
            prefer_recent=False,
            free_bytes=5,
            download_root=configured.download_root,
        )
        is None
    )
    client = type(
        "Formats", (), {"selected_formats": lambda self, video_id: (FORMATS, 7)}
    )()
    result = maintain_partials(state, configured, youtube=client)
    assert not result["findings"]
    assert not path.exists() and (path.parent / "merging.f315.webm.part").exists()
    assert saved(state, "abcdefghijk").attempts == 0
    claim = state.claim_archive(
        lane="download",
        prefer_recent=False,
        free_bytes=8,
        minimum_free_bytes=1,
        download_root=configured.download_root,
        dispatch_class="resume",
    )
    assert claim and claim.progress.reserved_bytes == 7
    assert not (path.parent / "download.json").exists()
    state.close()


@pytest.mark.parametrize("disconnect", [False, True])
def test_isolated_niquests_put_adapter_preserves_payload_and_single_attempt(
    disconnect: bool,
) -> None:
    payload = bytes(range(256)) * 1024
    received: list[bytes] = []

    class Server(BaseHTTPRequestHandler):
        def do_PUT(self) -> None:
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            if disconnect:
                self.close_connection = True
                return
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Server) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with HttpSession(retries=0) as client:
                client.mount(
                    "http://",
                    BandwidthHTTPAdapter(ByteLimiter(100_000_000), max_retries=0),
                )

                def put() -> Any:
                    return client.put(
                        f"http://127.0.0.1:{server.server_port}/media",
                        data=BytesIO(payload),
                        headers={
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(len(payload)),
                        },
                        timeout=5,
                    )

                if disconnect:
                    with pytest.raises(HttpConnectionError):
                        put()
                else:
                    assert put().status_code == 200
            assert received == [payload]
        finally:
            server.shutdown()
            thread.join()
