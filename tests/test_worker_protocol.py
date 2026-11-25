from collections import deque
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest
from archivist import (
    InternetArchiveUploadError,
    InternetArchiveUploadResult,
    NetworkError,
)

from core import archival, workers, youtube
from core import logging as logging_module
from core.shutdown import CooperativeStop, StopController
from tests.test_archival import settings


class Connection:
    def __init__(self, replies: list[Any] | None = None) -> None:
        self.replies = deque(replies or [])
        self.messages: list[Any] = []
        self.closed = False

    def send(self, value: Any) -> None:
        self.messages.append(value)

    def recv(self) -> Any:
        value = self.replies.popleft() if self.replies else ("ok", None)
        if isinstance(value, Exception):
            raise value
        return value

    def close(self) -> None:
        self.closed = True

    def poll(self, seconds: float) -> bool:
        return bool(self.replies)


@pytest.mark.parametrize(
    "category,method",
    [
        ("youtube", "download_video"),
        ("youtube", "scan_history"),
        ("youtube", "video_metadata"),
        ("gateway", "upload"),
        ("gateway", "upload_multipart"),
        ("gateway", "inspect"),
        ("ziggy", "submit"),
        ("log", "emit"),
        ("unknown", "unknown"),
        ("failure", "failure"),
    ],
)
def test_child_protocol_dispatches_callbacks_and_serializes_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, category: str, method: str
) -> None:
    connection = Connection()

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.limiter = kwargs.get("download_limiter", kwargs.get("upload_limiter"))

        def download_video(self, *args: Any, **kwargs: Any) -> str:
            self.limiter.acquire(4)
            self.limiter.refund(1)
            kwargs["disk_guard"](4, 3)
            kwargs["phase_hook"]("merging", 0)
            return "media"

        def scan_history(self, *args: Any, **kwargs: Any) -> str:
            kwargs["record_batch"](("video",))
            return "history"

        def video_metadata(self, *args: Any, **kwargs: Any) -> str:
            return "metadata"

        def upload(self, *args: Any, **kwargs: Any) -> None:
            kwargs["progress"](4)
            raise InternetArchiveUploadError(
                InternetArchiveUploadResult("item", ()), NetworkError("lost")
            )

        def upload_multipart(self, *args: Any, **kwargs: Any) -> str:
            kwargs["progress"](4)
            kwargs["heartbeat"]()
            kwargs["on_state"]("{}")
            return "multipart"

        def inspect(self, *args: Any, **kwargs: Any) -> str:
            return "inspection"

    monkeypatch.setattr(youtube, "YouTubeClient", Client)
    monkeypatch.setattr(archival, "ArchiveGateway", Client)
    monkeypatch.setattr(archival, "submit_ziggy", lambda *args, **kwargs: "receipt")
    emitted: list[str] = []
    monkeypatch.setattr(
        "loguru_discord.DiscordSink",
        lambda url: SimpleNamespace(
            emit=lambda record: emitted.append(record.getMessage())
        ),
    )
    stop = Event()
    if category == "failure":
        stop.set()
    workers._execute(
        cast(Any, connection),
        stop,
        category,
        method,
        {"settings": settings(tmp_path)}
        if category == "gateway"
        else {"webhook_url": "url", "message": "safe"}
        if category == "log"
        else {},
        (),
        {},
    )
    if category == "gateway" and method == "upload":
        assert connection.messages[-1][0] == "upload_failure"
    elif method == "upload_multipart":
        assert [message[0] for message in connection.messages] == [
            "progress",
            "heartbeat",
            "on_state",
            "result",
        ]
    elif category in {"unknown", "failure"}:
        assert connection.messages[-1][0] == "failure"
    else:
        assert connection.messages[-1][0] == "result"


def test_remote_callback_error_propagates_as_typed_stop() -> None:
    connection = Connection([("error", CooperativeStop("stop"))])
    with pytest.raises(CooperativeStop):
        workers.RemoteLimiter(cast(Any, connection), True, 64).acquire(10)


def test_managed_download_without_parent_guard_keeps_native_space_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Connection()
    monkeypatch.setattr(logging_module, "configure_logging", lambda settings: None)

    def download(*args: Any, **kwargs: Any) -> str:
        assert "disk_guard" not in kwargs
        kwargs["phase_hook"]("merging", 0)
        return "native guard"

    monkeypatch.setattr(
        youtube,
        "YouTubeClient",
        lambda **kwargs: SimpleNamespace(download_video=download),
    )
    workers._execute(
        cast(Any, connection),
        Event(),
        "youtube",
        "download_video",
        {},
        (),
        {"use_parent_guard": False},
    )
    assert connection.messages[-1] == ("result", "native guard")


@pytest.mark.parametrize(
    "outcome",
    [
        "result",
        "failure",
        "upload_failure",
        "callback",
        "callback_error",
        "eof",
        "dead",
    ],
)
def test_parent_keeps_progress_durable_and_handles_lost_worker_outcomes(
    monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    upload = InternetArchiveUploadResult("item", ())
    replies = {
        "result": [("result", "receipt")],
        "failure": [("failure", ValueError("fixture"))],
        "upload_failure": [("upload_failure", (upload, NetworkError("lost"), "media"))],
        "callback": [("progress", (4,)), ("result", "receipt")],
        "callback_error": [("progress", (4,)), ("result", "receipt")],
        "eof": [EOFError()],
        "dead": [],
    }[outcome]
    parent, child = Connection(replies), Connection()
    process = SimpleNamespace(
        start=lambda: None, join=lambda **kwargs: None, is_alive=lambda: False
    )
    context = SimpleNamespace(
        Pipe=lambda: (parent, child), Event=Event, Process=lambda **kwargs: process
    )
    monkeypatch.setattr(workers.multiprocessing, "get_context", lambda _: context)

    def progress(value: int) -> None:
        if outcome == "callback_error":
            raise CooperativeStop("stop")

    def run() -> Any:
        return workers.run_managed(
            "gateway",
            "upload",
            {},
            (),
            {},
            StopController(),
            {"progress": progress, "merge_probe": lambda: None},
        )

    if outcome in {"result", "callback", "callback_error"}:
        assert run() == "receipt"
    else:
        with pytest.raises((ValueError, RuntimeError, InternetArchiveUploadError)):
            run()
    assert parent.closed and child.closed
    if outcome == "callback_error":
        assert parent.messages[1][0] == "error"
    assert parent.messages[0][:2] == ("gateway", "upload")


@pytest.mark.parametrize(
    "platform,missing,escalate",
    [
        ("posix", False, False),
        ("posix", False, True),
        ("posix", True, True),
        ("nt", False, True),
    ],
)
def test_owned_termination_escalates_only_for_its_pid(
    monkeypatch: pytest.MonkeyPatch, platform: str, missing: bool, escalate: bool
) -> None:
    signals: list[tuple[int, int]] = []
    killed: list[str] = []

    def killpg(pid: int, sig: int) -> None:
        signals.append((pid, sig))
        if missing:
            raise ProcessLookupError()

    monkeypatch.setattr(workers, "os", SimpleNamespace(name=platform, killpg=killpg))
    monkeypatch.setattr(workers, "signal", SimpleNamespace(SIGTERM=15, SIGKILL=9))
    monkeypatch.setattr(workers.subprocess, "run", lambda *args, **kwargs: None)
    process = SimpleNamespace(
        pid=1234,
        join=lambda **kwargs: None,
        is_alive=lambda: escalate,
        terminate=lambda: killed.append("terminate"),
        kill=lambda: killed.append("kill"),
    )
    workers.terminate_owned_process(process)
    assert all(pid == 1234 for pid, _ in signals)
    if escalate:
        assert (
            ("kill" in killed)
            if missing or platform == "nt"
            else signals[-1] == (1234, 9)
        )


def test_managed_youtube_dispatch_and_merge_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = youtube.YouTubeClient(managed=True)
    seen: list[str] = []

    def run(
        category: str,
        method: str,
        options: Any,
        args: Any,
        kwargs: Any,
        controller: Any,
        callbacks: Any,
    ) -> str:
        seen.append(method)
        if method == "scan_history":
            callbacks["record_batch"](())
        if method == "download_video":
            callbacks["merge_probe"]()
            callbacks["phase_hook"]("merging", 0)
            (tmp_path / "merging.temp.mkv").write_bytes(b"merge")
            callbacks["merge_probe"]()
            callbacks["phase_hook"]("merged", 5)
        return "result"

    monkeypatch.setattr(youtube, "run_managed", run)
    client.resolve_channel("@handle")
    client.recent_videos(cast(Any, "channel"))
    client.scan_history(cast(Any, "channel"), lambda batch: None)
    client.video_metadata("video")
    client.download_video(
        "video",
        tmp_path,
        minimum_free_bytes=0,
        estimated_bytes=4,
        phase_hook=lambda *args: None,
    )
    client.download_video("video", tmp_path, minimum_free_bytes=0, estimated_bytes=4)
    assert set(seen) == {
        "resolve_channel",
        "recent_videos",
        "scan_history",
        "video_metadata",
        "download_video",
    }


def test_managed_gateway_dispatch_preserves_upload_callbacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(archival, "run_managed", lambda *args, **kwargs: "result")
    gateway = archival.ArchiveGateway(settings(tmp_path), managed=True)
    gateway.upload(cast(Any, "metadata"), tmp_path, progress=lambda _: None)
    gateway.recover(cast(Any, "metadata"), tmp_path, progress=lambda _: None)
    gateway.inspect(cast(Any, "metadata"), size=4)
    gateway.submit_page("url")
    gateway.page_status("job")


def test_managed_multipart_keeps_resume_callbacks_in_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        archival, "run_managed", lambda *args, **kwargs: calls.append(args)
    )
    saved: list[str] = []
    gateway = archival.ArchiveGateway(settings(tmp_path), managed=True)
    gateway.upload_multipart(
        cast(Any, "metadata"),
        tmp_path,
        create=False,
        state="saved",
        progress=lambda _: None,
        heartbeat=lambda: None,
        on_state=saved.append,
    )
    (_, method, _, args, kwargs, _, callbacks) = calls[0]
    assert (method, args[1], kwargs) == (
        "upload_multipart",
        tmp_path,
        {"create": False, "state": "saved", "derive": "deferred"},
    )
    assert {"progress", "heartbeat", "on_state"} <= set(callbacks)
    callbacks["on_state"]("next")
    assert saved == ["next"]


def test_log_forwarding_uses_bounded_process_with_redacted_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(logging_module, "_STOP_CONTROLLER", None)
    monkeypatch.setattr(
        logging_module, "DiscordSink", lambda url: SimpleNamespace(webhook_url=url)
    )
    calls: list[Any] = []
    monkeypatch.setattr(workers, "run_managed", lambda *args: calls.append(args))
    logging_module.bind_stop_controller(StopController())
    import logging

    sink = logging_module._SafeDiscordSink("url")
    sink.emit(
        logging.LogRecord("saga", 30, "", 0, "Authorization: Bearer secret", (), None)
    )
    assert len(calls) == 1 and "secret" not in calls[0][2]["message"]
