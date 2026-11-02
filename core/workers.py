"""Owned process boundaries for network libraries and their child processes."""

from __future__ import annotations

import atexit
import multiprocessing
import multiprocessing.util
import os
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing.connection import Connection
from threading import Lock
from typing import Any

from core.shutdown import CooperativeStop, StopController

WORKER_MAX_CALLS = 100
WORKER_MAX_IDLE = 4
WORKER_IDLE_TIMEOUT = 600.0


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


def _serve(connection: Connection, stop: Any) -> None:
    """Run managed requests in one owned process group until the parent hangs up."""
    if os.name != "nt":
        os.setsid()
    from core.config import LoggingSettings
    from core.logging import configure_logging

    configure_logging(LoggingSettings())
    try:
        while True:
            try:
                request = connection.recv()
            except EOFError:
                return
            _execute(connection, stop, *request)
    finally:
        connection.close()


def _execute(
    connection: Connection,
    stop: Any,
    category: str,
    method: str,
    options: dict[str, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
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


@dataclass(eq=False, slots=True)
class ManagedWorker:
    """Track one reusable child process and its private request channel."""

    process: Any
    connection: Any
    stop: Any
    calls: int = 0
    idle_since: float = 0.0


class WorkerPool:
    """Reuse warm child processes and retire any that fail, stall, or age out."""

    def __init__(
        self,
        *,
        max_calls: int = WORKER_MAX_CALLS,
        max_idle: int = WORKER_MAX_IDLE,
        idle_timeout: float = WORKER_IDLE_TIMEOUT,
    ) -> None:
        self.max_calls = max_calls
        self.max_idle = max_idle
        self.idle_timeout = idle_timeout
        self.spawned = 0
        self._idle: list[ManagedWorker] = []
        self._lock = Lock()
        self._closed = False

    def acquire(self) -> ManagedWorker:
        """Return the most recently used live worker, or start a new one."""
        now = time.monotonic()
        with self._lock:
            usable = [
                worker
                for worker in self._idle
                if worker.process.is_alive()
                and now - worker.idle_since < self.idle_timeout
            ]
            expired = [worker for worker in self._idle if worker not in usable]
            selected = usable.pop() if usable else None
            self._idle = usable
        for worker in expired:
            _retire(worker)
        return selected or self._spawn()

    def release(self, worker: ManagedWorker, *, reusable: bool) -> None:
        """Keep a cleanly finished worker warm; retire every other outcome."""
        worker.calls += 1
        with self._lock:
            if (
                reusable
                and not self._closed
                and worker.calls < self.max_calls
                and len(self._idle) < self.max_idle
                and worker.process.is_alive()
            ):
                worker.idle_since = time.monotonic()
                self._idle.append(worker)
                return
        _retire(worker)

    def shutdown(self) -> None:
        """Stop idle workers and retire busy ones as soon as they are released."""
        with self._lock:
            self._closed = True
            idle, self._idle = self._idle, []
        for worker in idle:
            _retire(worker)

    def _spawn(self) -> ManagedWorker:
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        stop = context.Event()
        process = context.Process(target=_serve, args=(child, stop), daemon=False)
        process.start()
        child.close()
        self.spawned += 1
        return ManagedWorker(process, parent, stop)


def _retire(worker: ManagedWorker) -> None:
    worker.connection.close()
    worker.process.join(timeout=0.2)
    if worker.process.is_alive():
        terminate_owned_process(worker.process)


POOL = WorkerPool()


def shutdown_workers() -> None:
    """Stop idle managed workers before multiprocessing joins its children."""
    POOL.shutdown()


# multiprocessing.util registered its child-joining exit hook on import, so this
# later hook runs first and idle workers cannot hold interpreter shutdown open.
atexit.register(shutdown_workers)


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
    worker = POOL.acquire()
    parent, process, stop = worker.connection, worker.process, worker.stop
    reusable = False
    stopping_at: float | None = None
    callbacks = callbacks or {}
    try:
        stop.clear()
        try:
            parent.send((category, method, options, args, kwargs))
        except OSError as error:
            raise RuntimeError(
                f"managed {category}/{method} could not reach its worker"
            ) from error
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
                reusable = True
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
        POOL.release(worker, reusable=reusable)
