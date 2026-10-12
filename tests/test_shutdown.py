import asyncio
import multiprocessing
import os
import socket
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from typing import Any

import pytest

from core.archival import ArchiveProcessor, _ProgressStream
from core.backups import _create_backup_sync
from core.bandwidth import ByteLimiter
from core.service import SagaService
from core.shutdown import CooperativeStop, StopController
from core.state import StateStore
from core.workers import run_managed, terminate_owned_process
from core.youtube import YouTubeClient, media_retry_options
from tests.test_archival import FakeGateway, saved, seed_upload, settings


def test_stop_interrupts_retry_bandwidth_and_upload_body_reads() -> None:
    controller = StopController()
    limiter = ByteLimiter(1, stop_controller=controller)
    limiter.acquire(1)
    controller.request()
    deadline = controller.deadline
    controller.request()
    assert controller.deadline == deadline and 0 < controller.remaining() <= 90
    with pytest.raises(CooperativeStop):
        limiter.acquire(1)
    with pytest.raises(CooperativeStop):
        media_retry_options(controller)["retry_sleep_functions"]["http"](3)
    with pytest.raises(CooperativeStop):
        _ProgressStream(BytesIO(b"media"), lambda _: None, controller).read(1)


def test_limiter_wait_is_interrupted_and_idle_wait_returns() -> None:
    controller = StopController()
    controller.wait(0)
    limiter = ByteLimiter(1, stop_controller=controller)
    limiter.acquire(1)

    def stop() -> None:
        time.sleep(0.05)
        controller.request()

    thread = Thread(target=stop)
    thread.start()
    with pytest.raises(CooperativeStop):
        limiter.acquire(1)
    thread.join()


def test_retry_sleep_occurs_once_and_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    controller = StopController()
    waits: list[float] = []
    monkeypatch.setattr(controller, "wait", waits.append)
    retry = media_retry_options(controller)["retry_sleep_functions"]["http"]
    assert [retry(n=index) for index in range(6)] == [0] * 6
    assert waits == [1, 2, 4, 8, 8, 8]


def test_corrupt_download_directory_is_retained(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="download", working_directory=str(tmp_path))
    work = state.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
    assert work
    ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), FakeGateway()).process(
        work
    )
    assert media.exists() and saved(state, video_id).status == "operator_action"
    state.close()


def test_typed_stop_is_not_retried(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    service = SagaService(settings(tmp_path), (), state, YouTubeClient())

    def stop() -> None:
        raise CooperativeStop("fixture")

    with pytest.raises(CooperativeStop):
        asyncio.run(service._youtube_request(stop, 3, 0))
    with pytest.raises(CooperativeStop):
        service._youtube_request_sync(stop, 3, 0)
    state.close()


def test_undersized_shutdown_budget_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.config import load_settings

    monkeypatch.setenv("SAGA_SHUTDOWN_TIMEOUT", "1")
    with pytest.raises(ValueError, match="at least 10"):
        load_settings(tmp_path)


def test_stopped_download_releases_claim_and_preserves_inputs(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="download")
    work = state.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
    assert work
    controller = StopController()
    controller.request()
    ArchiveProcessor(
        settings(tmp_path),
        state,
        YouTubeClient(),
        FakeGateway(),
        stop_controller=controller,
    ).process(work)
    row = saved(state, video_id)
    assert (
        row.claimed_at is None and row.reserved_bytes == 0 and row.status == "download"
    )
    assert media.exists() and state.shutdown_summary()[0]["video_id"] == video_id
    state.close()


def test_backup_cancellation_never_publishes_incomplete_snapshot(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    state.close()
    controller = StopController()
    controller.request()
    with pytest.raises(CooperativeStop):
        _create_backup_sync(
            state.path, tmp_path / "backups", 7, stop_controller=controller
        )
    assert list((tmp_path / "backups").iterdir()) == []


def test_service_does_not_start_a_retry_after_stop(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    service = SagaService(settings(tmp_path), (), state, YouTubeClient())
    calls: list[int] = []

    def operation() -> None:
        calls.append(1)
        service.request_stop()
        raise OSError("fixture")

    with pytest.raises(CooperativeStop):
        asyncio.run(service._youtube_request(operation, 3, 0))
    assert calls == [1]
    with pytest.raises(CooperativeStop):
        service._youtube_request_sync(operation, 3, 0)
    assert calls == [1]
    state.close()


def test_real_managed_http_stop_is_bounded(tmp_path: Path) -> None:
    requested = Event()
    closed = Event()

    class SlowResponse(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.rfile.read(int(self.headers["Content-Length"]))
            requested.set()
            self.send_response(202)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            try:
                while not closed.wait(0.1):
                    self.wfile.write(b" ")
                    self.wfile.flush()
            except OSError:
                pass

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), SlowResponse) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        controller = StopController(10)

        def stop() -> None:
            assert requested.wait(10)
            controller.request()

        stopper = Thread(target=stop)
        stopper.start()
        try:
            with pytest.raises(CooperativeStop):
                run_managed(
                    "ziggy",
                    "submit",
                    {},
                    (
                        f"http://127.0.0.1:{server.server_port}",
                        "fixture",
                        "https://example.com/video",
                        60,
                    ),
                    {},
                    controller,
                )
            assert controller.remaining() > 0
        finally:
            closed.set()
            server.shutdown()
            thread.join()
            stopper.join()


def test_real_entrypoint_sigterm_saves_uncertain_upload(tmp_path: Path) -> None:
    requested: list[bytes] = []
    closed = Event()

    class SlowResponse(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requested.append(self.rfile.read(int(self.headers["Content-Length"])))
            (tmp_path / "request-started").write_text("received", encoding="utf-8")
            self.send_response(202)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            try:
                while not closed.wait(0.1):
                    self.wfile.write(b" ")
                    self.wfile.flush()
            except OSError:
                pass

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), SlowResponse) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.shutdown_fixture",
                    str(tmp_path),
                    f"http://127.0.0.1:{server.server_port}",
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert result.returncode == 0, result.stderr
            assert (
                "Saga stopped" in result.stdout and "reconcile_upload" in result.stdout
            )
            state = StateStore(tmp_path / "saga.sqlite")
            row = saved(state, "abcdefghijk")
            assert row.status == "reconcile_upload" and row.claimed_at is None
            assert (
                row.media_path
                and row.media_uploaded_at is None
                and Path(row.media_path).exists()
            )
            assert len(requested) == 1 and state.claim_background("recovery")
            state.close()
        finally:
            closed.set()
            server.shutdown()
            thread.join()


def owned_child(connection: Any) -> None:
    if os.name != "nt":
        os.setsid()
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import socket,time; s=socket.socket(); s.bind(('127.0.0.1',0)); "
            "s.listen(); print(s.getsockname()[1],flush=True); time.sleep(1000)",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None
    connection.send(int(child.stdout.readline()))
    time.sleep(1000)


def test_owned_descendants_are_terminated_without_process_name_matching() -> None:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=owned_child, args=(child,))
    process.start()
    child.close()
    assert parent.poll(10)
    port = parent.recv()
    with socket.create_connection(("127.0.0.1", port), timeout=1):
        pass
    terminate_owned_process(process)
    assert not process.is_alive()
    with (
        pytest.raises(OSError),
        socket.create_connection(("127.0.0.1", port), timeout=1),
    ):
        pass
    parent.close()


@pytest.mark.parametrize(
    "point", ["pending_job", "submitting", "download", "upload", "acknowledged"]
)
def test_stop_outcomes_preserve_receipts_and_independent_claims(
    tmp_path: Path, point: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    controller = StopController()

    class Gateway(FakeGateway):
        def upload(self, *args: Any, **kwargs: Any) -> Any:
            if point == "upload":
                raise CooperativeStop("upload stopped")
            result = super().upload(*args, **kwargs)
            kwargs["progress"](4)
            controller.request()
            return result

    gateway = Gateway()

    class Youtube(YouTubeClient):
        def download_video(self, video_id: str, directory: Path, **kwargs: Any) -> Path:
            (directory / "merging.f315.webm.part").write_bytes(b"part")
            kwargs["disk_guard"](4, 2)
            raise CooperativeStop("download stopped")

    processor = ArchiveProcessor(
        settings(tmp_path), state, Youtube(), gateway, stop_controller=controller
    )
    if point in {"pending_job", "submitting"}:
        state.queue_webpage_only(video_id, deadline=60)
        work = state.claim_background("webpage")
        if point == "pending_job":
            controller.request()
        else:
            gateway.page_error = CooperativeStop("submission stopped")
    else:
        if point == "download":
            state.update_archive(video_id, status="download")
        work = state.claim_archive(
            lane="download" if point == "download" else "upload",
            prefer_recent=False,
            free_bytes=100,
        )
    assert work
    processor.process(work)
    row = saved(state, video_id)
    if point == "acknowledged":
        assert row.media_uploaded_at and row.upload_receipt
    elif point == "upload":
        assert row.status == "reconcile_upload" and state.claim_background("recovery")
    elif point == "download":
        assert (
            row.download_bytes == 2
            and row.reserved_bytes == 0
            and row.claimed_at is None
        )
        assert (media.parent / "merging.f315.webm.part").exists()
    else:
        from sqlmodel import Session

        from core.state import ArchiveJob

        with Session(state.engine) as session:
            job = session.get(ArchiveJob, (video_id, "webpage"))
            assert job and job.status == (
                "waiting" if point == "pending_job" else "failed"
            )
    assert media.exists()
    state.close()
