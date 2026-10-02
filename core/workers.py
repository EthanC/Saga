"""Owned process boundaries for network libraries and their child processes."""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

from core.shutdown import CooperativeStop, StopController


class RemoteLimiter:
    """Ask the parent for its shared aggregate byte budget before every read."""

    def __init__(self, connection: Connection, enabled: bool, chunk_size: int) -> None:
        self.connection = connection
        self.enabled = enabled
        self.chunk_size = chunk_size

    def acquire(self, byte_count: int) -> None:
        _rpc(self.connection, "acquire", (byte_count,))

    def refund(self, byte_count: int) -> None:
        _rpc(self.connection, "refund", (byte_count,))


def _rpc(connection: Connection, kind: str, args: tuple[Any, ...]) -> Any:
    connection.send((kind, args))
    outcome, result = connection.recv()
    if outcome == "error":
        raise result
    return result


def _worker(
    connection: Connection,
    stop: Any,
    category: str,
    method: str,
    options: dict[str, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    if os.name != "nt":
        os.setsid()
    from core.config import LoggingSettings
    from core.logging import configure_logging

    configure_logging(LoggingSettings())
    controller = StopController(event=stop)
    limiter = RemoteLimiter(
        connection,
        options.pop("limiter_enabled", False),
        options.pop("chunk_size", 65536),
    )
    try:
        controller.check()
        if category == "youtube":
            from core.youtube import YouTubeClient

            client = YouTubeClient(
                **options, download_limiter=limiter, stop_controller=controller
            )
            if method == "download_video":
                if kwargs.pop("use_parent_guard", True):
                    kwargs["disk_guard"] = lambda *values: _rpc(
                        connection, "disk_guard", values
                    )
                kwargs["phase_hook"] = lambda *values: _rpc(
                    connection, "phase_hook", values
                )
            elif method == "scan_history":
                kwargs["record_batch"] = lambda *values: _rpc(
                    connection, "record_batch", values
                )
            result = getattr(client, method)(*args, **kwargs)
        elif category == "gateway":
            from core.archival import ArchiveGateway

            client = ArchiveGateway(
                options["settings"], upload_limiter=limiter, stop_controller=controller
            )
            if method in {"upload", "recover"}:
                kwargs["progress"] = lambda *values: _rpc(
                    connection, "progress", values
                )
            result = getattr(client, method)(*args, **kwargs)
        elif category == "ziggy":
            from core.archival import submit_ziggy

            result = submit_ziggy(*args, **kwargs)
        elif category == "log":
            import logging

            from loguru_discord import DiscordSink

            sink = DiscordSink(options["webhook_url"])
            sink.emit(
                logging.LogRecord(
                    "saga", logging.WARNING, "", 0, options["message"], (), None
                )
            )
            result = None
        else:
            raise ValueError("unknown managed operation")
        connection.send(("result", result))
    except BaseException as error:
        from archivist import InternetArchiveUploadError

        if isinstance(error, InternetArchiveUploadError):
            connection.send(
                ("upload_failure", (error.result, error.cause, error.failed_file))
            )
        else:
            connection.send(("failure", error))
    finally:
        connection.close()


def terminate_owned_process(process: Any) -> None:
    """Terminate only this worker and its descendants, escalating within seconds."""
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            timeout=5,
            check=False,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.terminate()
    process.join(timeout=2)
    if process.is_alive():
        if os.name != "nt":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                process.kill()
        else:
            process.kill()
        process.join(timeout=2)


def run_managed(
    category: str,
    method: str,
    options: dict[str, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    controller: StopController,
    callbacks: dict[str, Callable[..., Any]] | None = None,
) -> Any:
    """Keep durable callbacks in the parent; cap cancellation of blocking calls."""
    controller.check()
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    stop = context.Event()
    process = context.Process(
        target=_worker,
        args=(child, stop, category, method, options, args, kwargs),
        daemon=False,
    )
    process.start()
    child.close()
    stopping_at: float | None = None
    callbacks = callbacks or {}
    try:
        while True:
            if "merge_probe" in callbacks:
                callbacks["merge_probe"]()
            if controller.event.is_set():
                stop.set()
                if stopping_at is None:
                    stopping_at = time.monotonic()
                if time.monotonic() - stopping_at >= min(
                    5, max(0, controller.remaining() - (9 if os.name == "nt" else 4))
                ):
                    raise CooperativeStop("managed operation stopped before deadline")
            if not parent.poll(0.1):
                if not process.is_alive():
                    controller.check()
                    raise RuntimeError(
                        f"managed {category}/{method} exited without an outcome"
                    )
                continue
            kind, values = parent.recv()
            if kind == "result":
                return values
            if kind == "failure":
                raise values
            if kind == "upload_failure":
                from archivist import InternetArchiveUploadError

                result, cause, failed_file = values
                raise InternetArchiveUploadError(result, cause, failed_file=failed_file)
            try:
                controller.check()
                result = callbacks[kind](*values)
            except Exception as error:
                parent.send(("error", error))
            else:
                parent.send(("ok", result))
    except EOFError as error:
        controller.check()
        raise RuntimeError(f"managed {category}/{method} lost its result") from error
    finally:
        parent.close()
        process.join(timeout=0.2)
        if process.is_alive():
            terminate_owned_process(process)
