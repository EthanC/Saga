"""Bound repeated YouTube work: dead videos, worker spawns, and history rescans."""

import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlmodel import Session

from core import workers
from core.config import YouTubeChannelConfig, YouTubeConfig
from core.service import MonitoredChannel
from core.shutdown import CooperativeStop, StopController
from core.state import PUBLICATION_RETRY_CAP, Channel, StateStore, Video, utcnow
from core.youtube import VideoUnavailableError
from core.youtube_errors import is_permanent_video_error
from tests.test_service_coverage import CHANNEL, VIDEO, metadata, service_with
from tests.test_service_liveness import notification_service

UNRESOLVED = replace(VIDEO, published_at=None, published_precise=False)


@pytest.mark.parametrize(
    "detail,permanent",
    [
        ("ERROR: [youtube] 7ff_7MJjF1U: Video unavailable", True),
        ("ERROR: [youtube] abc: Private video. Sign in if you've been granted", True),
        ("ERROR: [youtube] abc: This video has been removed by the uploader", True),
        ("ERROR: [youtube] abc: Join this channel to get access to members-only", True),
        ("This video is available to this channel’s members on level: Tier", True),
        ("ERROR: [youtube] hv32dw3gNNE: Offline.", False),
        ("ERROR: [youtube] abc: This live event will begin in 3 hours.", False),
        ("Video unavailable. This content isn't available, try again later.", False),
        ("ERROR: [youtube] abc: Sign in to confirm you're not a bot", False),
        ("HTTP Error 429: Too Many Requests", False),
        ("<urlopen error timed out>", False),
    ],
)
def test_video_errors_are_classified_permanent_or_transient(
    detail: str, permanent: bool
) -> None:
    assert is_permanent_video_error(detail) is permanent


def run_publication(saga: Any, monkeypatch: pytest.MonkeyPatch, retries: int) -> None:
    async def wait(_: float) -> None:
        saga.stop.set()

    monkeypatch.setattr(saga, "_wait", wait)
    saga.stop.clear()
    asyncio.run(
        saga._publication_loop(
            (MonitoredChannel(CHANNEL, 60, frozenset({"video"}), retries=retries),)
        )
    )


def video_row(state: StateStore, video_id: str = VIDEO.id) -> Video:
    with Session(state.engine) as session:
        row = session.get(Video, video_id)
        assert row is not None
        return row


def test_permanently_unavailable_video_is_recorded_and_never_requested_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        calls = 0

        def video_metadata(self, video_id: str) -> Any:
            self.calls += 1
            raise VideoUnavailableError(
                f"ERROR: [youtube] {video_id}: Video unavailable"
            )

    youtube = Youtube()
    state, saga = notification_service(tmp_path, youtube)
    try:
        state.record_discoveries(CHANNEL.id, (UNRESOLVED,), backfill=True)
        run_publication(saga, monkeypatch, retries=3)
        # A permanent outcome is not retried within the request either.
        assert youtube.calls == 1
        row = video_row(state)
        assert row.publication_failure == "permanent"
        assert row.publication_attempts == 1
        assert row.publication_retry_at is None
        assert row.publication_error and "Video unavailable" in row.publication_error

        later = utcnow() + timedelta(days=30)
        assert state.claim_publication((CHANNEL.id,), now=later) is None
        assert state.publication_detail_ids((VIDEO.id,)) == set()
        for _ in range(3):
            run_publication(saga, monkeypatch, retries=3)
        assert youtube.calls == 1
    finally:
        state.close()


def test_transient_failures_back_off_exponentially_with_a_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        calls = 0

        def video_metadata(self, video_id: str) -> Any:
            self.calls += 1
            raise RuntimeError(f"ERROR: [youtube] {video_id}: Offline.")

    youtube = Youtube()
    state, saga = notification_service(tmp_path, youtube)
    try:
        state.record_discoveries(CHANNEL.id, (UNRESOLVED,), backfill=True)
        before = utcnow()
        run_publication(saga, monkeypatch, retries=0)
        row = video_row(state)
        assert row.publication_failure == "transient"
        assert row.publication_retry_at is not None
        first_delay = (
            row.publication_retry_at.replace(tzinfo=before.tzinfo) - before
        ).total_seconds()
        assert 240 <= first_delay <= 361
        # The next scan pass does not retry before the backoff expires.
        run_publication(saga, monkeypatch, retries=0)
        assert youtube.calls == 1

        delays = []
        now = utcnow()
        for _ in range(10):
            retry_at = state.record_publication_failure(
                VIDEO.id, "RuntimeError: Offline.", permanent=False, now=now
            )
            assert retry_at is not None
            delays.append((retry_at - now).total_seconds())
        assert delays[1] > delays[0] * 1.3
        assert max(delays) == PUBLICATION_RETRY_CAP
        assert video_row(state).publication_attempts == 11
        assert state.claim_publication((CHANNEL.id,), now=now) is None
        claimed = state.claim_publication(
            (CHANNEL.id,), now=now + timedelta(seconds=PUBLICATION_RETRY_CAP)
        )
        assert claimed and claimed[1].id == VIDEO.id
    finally:
        state.close()


def test_offline_livestream_is_retried_as_soon_as_it_goes_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        live = False

        def video_metadata(self, video_id: str) -> Any:
            if not self.live:
                raise RuntimeError(f"ERROR: [youtube] {video_id}: Offline.")
            return metadata(utcnow(), True)

    youtube = Youtube()
    state, saga = notification_service(tmp_path, youtube)
    # A date-only listing on the baseline day stays ambiguous during recent scans.
    baseline = state.notification_baseline(CHANNEL.id)
    upcoming = replace(
        UNRESOLVED,
        published_at=baseline.replace(hour=0, minute=0, second=0, microsecond=0),
        live_status="is_upcoming",
    )
    try:
        state.record_discoveries(CHANNEL.id, (upcoming,), backfill=True)
        run_publication(saga, monkeypatch, retries=0)
        assert video_row(state).publication_failure == "transient"
        # An unchanged recent scan keeps the backoff.
        state.record_discoveries(CHANNEL.id, (upcoming,), backfill=False)
        assert state.claim_publication((CHANNEL.id,)) is None

        youtube.live = True
        state.record_discoveries(
            CHANNEL.id, (replace(upcoming, live_status="is_live"),), backfill=False
        )
        run_publication(saga, monkeypatch, retries=0)
        row = video_row(state)
        assert row.notification_eligible and not row.notification_ambiguous
        assert row.publication_failure is None and row.publication_attempts == 0
        assert row.publication_error is None
    finally:
        state.close()


def test_publication_failure_requires_a_known_video(tmp_path: Path) -> None:
    state, _ = notification_service(tmp_path, object())
    try:
        with pytest.raises(LookupError):
            state.record_publication_failure("missing", "error", permanent=True)
    finally:
        state.close()


def test_failure_and_completion_columns_are_added_to_existing_database(
    tmp_path: Path,
) -> None:
    state, _ = notification_service(tmp_path, object())
    state.record_discoveries(CHANNEL.id, (UNRESOLVED,), backfill=True)
    state.close()
    with closing(sqlite3.connect(tmp_path / "saga.sqlite")) as connection, connection:
        for name in (
            "publication_attempts",
            "publication_failure",
            "publication_error",
        ):
            connection.execute(f"ALTER TABLE video DROP COLUMN {name}")
        connection.execute("ALTER TABLE channel DROP COLUMN backfill_completed_at")
    reopened = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    try:
        assert reopened.backfill_completed_at(CHANNEL.id) is None
        row = video_row(reopened)
        assert row.publication_attempts == 0 and row.publication_failure is None
        reopened.mark_backfill_complete(CHANNEL.id)
        completed = reopened.backfill_completed_at(CHANNEL.id)
        assert completed and utcnow() - completed < timedelta(minutes=1)
        assert reopened.backfill_completed_at("UCmissing") is None
    finally:
        reopened.close()


def history_service(tmp_path: Path, completed_at: Any) -> tuple[Any, list[str]]:
    scans: list[str] = []

    class State:
        def backfill_completed_at(self, channel_id: str) -> Any:
            return completed_at

        def mark_backfill_complete(self, channel_id: str) -> None:
            scans.append("complete")

    class Youtube:
        def scan_history(self, channel: Any, callback: Any, **kwargs: Any) -> None:
            scans.append("scan")

    return service_with(tmp_path, State(), Youtube()), scans


def test_recent_reconciliation_is_not_repeated_on_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, scans = history_service(tmp_path, utcnow() - timedelta(hours=1))
    waits: list[float] = []

    async def wait(seconds: float) -> None:
        waits.append(seconds)
        if len(waits) == 2:
            service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    monitored = MonitoredChannel(
        CHANNEL, 60, frozenset({"video"}), reconciliation_interval=21_600
    )
    asyncio.run(service._reconciliation_loop(monitored))
    # The first wait covers the remaining five hours before any scan starts.
    assert 17_900 <= waits[0] <= 18_000
    assert scans == ["scan", "complete"]
    assert waits[1] == 21_600


def test_startup_stop_during_remaining_interval_skips_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, scans = history_service(tmp_path, utcnow() - timedelta(minutes=5))

    async def wait(seconds: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    asyncio.run(
        service._reconciliation_loop(MonitoredChannel(CHANNEL, 60, frozenset()))
    )
    assert scans == []


@pytest.mark.parametrize("age", [None, timedelta(hours=7)])
def test_missing_or_stale_completion_reconciles_on_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age: timedelta | None
) -> None:
    service, scans = history_service(tmp_path, None if age is None else utcnow() - age)
    waits: list[float] = []

    async def wait(seconds: float) -> None:
        waits.append(seconds)
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    asyncio.run(
        service._reconciliation_loop(
            MonitoredChannel(
                CHANNEL, 60, frozenset({"video"}), reconciliation_interval=900
            )
        )
    )
    assert scans == ["scan", "complete"]
    assert waits == [900]


def test_reconciliation_interval_uses_shortest_instance_setting(tmp_path: Path) -> None:
    class Youtube:
        def resolve_channel(self, reference: str) -> Any:
            return CHANNEL

    route = YouTubeChannelConfig("@channel", ("https://discord.com/api/webhooks/1/a",))
    state = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    try:
        service = service_with(tmp_path, state, Youtube())
        service.configurations = (
            YouTubeConfig("slow", (route,), reconciliation_interval=86_400),
            YouTubeConfig("fast", (route,), reconciliation_interval=3_600),
        )
        (monitored,) = asyncio.run(service._initialize())
        assert monitored.reconciliation_interval == 3_600
        default = YouTubeConfig("default", (route,))
        assert default.reconciliation_interval == 21_600
    finally:
        state.close()


def test_backfill_completion_is_persisted_across_restarts(tmp_path: Path) -> None:
    state, _ = notification_service(tmp_path, object())
    assert state.backfill_completed_at(CHANNEL.id) is None
    state.mark_backfill_complete(CHANNEL.id)
    state.close()
    reopened = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    try:
        with Session(reopened.engine) as session:
            channel = session.get(Channel, CHANNEL.id)
            assert channel is not None and channel.backfill_complete
        completed = reopened.backfill_completed_at(CHANNEL.id)
        assert completed is not None and completed.tzinfo is not None
    finally:
        reopened.close()


# Worker reuse ---------------------------------------------------------------


class Ziggy(BaseHTTPRequestHandler):
    """Answer quickly with a receipt, or hold the response open to simulate a hang."""

    released = Event()
    requested = Event()

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if body["identifier"] == "stuck":
            Ziggy.requested.set()
            self.send_response(202)
            self.send_header("Content-Length", "100000")
            self.end_headers()
            try:
                while not Ziggy.released.wait(0.1):
                    self.wfile.write(b" ")
                    self.wfile.flush()
            except OSError:
                pass
            return
        payload = json.dumps(
            {"submissions": [{"url": body["urls"][0], "receipt_id": "receipt"}]}
        ).encode()
        self.send_response(202)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture
def ziggy() -> Any:
    Ziggy.released.clear()
    Ziggy.requested.clear()
    with ThreadingHTTPServer(("127.0.0.1", 0), Ziggy) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            Ziggy.released.set()
            server.shutdown()
            thread.join()


@pytest.fixture
def pool(monkeypatch: pytest.MonkeyPatch) -> Any:
    pool = workers.WorkerPool()
    monkeypatch.setattr(workers, "POOL", pool)
    yield pool
    pool.shutdown()


def submit(host: str, identifier: str, controller: StopController | None = None) -> Any:
    return workers.run_managed(
        "ziggy",
        "submit",
        {},
        (host, identifier, "https://example.com/video", 30),
        {},
        controller or StopController(),
    )


def test_real_worker_is_reused_across_calls_and_stopped_on_shutdown(
    pool: workers.WorkerPool, ziggy: str
) -> None:
    for _ in range(3):
        assert submit(ziggy, "fixture")["receipt_id"] == "receipt"
    assert pool.spawned == 1
    (worker,) = pool._idle
    assert worker.calls == 3 and worker.process.is_alive()

    pool.shutdown()
    assert pool._idle == []
    worker.process.join(timeout=5)
    assert not worker.process.is_alive()
    # Calls after shutdown still complete in a one-off process.
    assert submit(ziggy, "fixture")["receipt_id"] == "receipt"
    assert pool.spawned == 2 and pool._idle == []


def test_real_stuck_call_is_killed_and_its_worker_replaced(
    pool: workers.WorkerPool, ziggy: str
) -> None:
    assert submit(ziggy, "fixture")
    (warm,) = pool._idle
    controller = StopController(10)

    def stop() -> None:
        assert Ziggy.requested.wait(10)
        controller.request()

    stopper = Thread(target=stop)
    stopper.start()
    try:
        with pytest.raises(CooperativeStop):
            submit(ziggy, "stuck", controller)
    finally:
        stopper.join()
    assert controller.remaining() > 0
    assert not warm.process.is_alive()
    assert pool._idle == []
    assert submit(ziggy, "fixture")["receipt_id"] == "receipt"
    assert pool.spawned == 2


class FakeConnection:
    def __init__(self, replies: list[Any] | None = None) -> None:
        self.replies = list(replies or [])
        self.sent: list[Any] = []
        self.closed = False
        self.fail_send = False

    def send(self, value: Any) -> None:
        if self.fail_send:
            raise BrokenPipeError("worker exited")
        self.sent.append(value)

    def recv(self) -> Any:
        if not self.replies:
            raise EOFError()
        return self.replies.pop(0)

    def poll(self, seconds: float) -> bool:
        return bool(self.replies)

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    def __init__(self) -> None:
        self.alive = True
        self.pid = 1234

    def start(self) -> None:
        pass

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        pass


def fake_pool(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> Any:
    spawned: list[tuple[FakeConnection, FakeProcess]] = []
    replies: list[Any] = []

    def get_context(method: str) -> Any:
        assert method == "spawn"

        def pipe() -> tuple[FakeConnection, FakeConnection]:
            parent = FakeConnection()
            parent.replies = replies
            spawned.append((parent, FakeProcess()))
            return parent, FakeConnection()

        return SimpleNamespace(
            Pipe=pipe, Event=Event, Process=lambda **_: spawned[-1][1]
        )

    def terminate(process: FakeProcess) -> None:
        process.alive = False

    monkeypatch.setattr(workers.multiprocessing, "get_context", get_context)
    monkeypatch.setattr(workers, "terminate_owned_process", terminate)
    pool = workers.WorkerPool(**kwargs)
    monkeypatch.setattr(workers, "POOL", pool)
    return pool, spawned, replies


def call(replies: list[Any], reply: Any) -> Any:
    replies.append(reply)
    return workers.run_managed(
        "youtube", "video_metadata", {}, (), {}, StopController()
    )


def test_pool_replaces_workers_after_failure_or_call_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool, spawned, replies = fake_pool(monkeypatch, max_calls=3)
    for _ in range(3):
        assert call(replies, ("result", "metadata")) == "metadata"
    # The third call reached the limit, so its worker was retired.
    assert pool.spawned == 1 and pool._idle == []
    assert spawned[0][0].closed and not spawned[0][1].alive

    assert call(replies, ("result", "metadata")) == "metadata"
    assert pool.spawned == 2 and len(pool._idle) == 1
    with pytest.raises(VideoUnavailableError):
        call(replies, ("failure", VideoUnavailableError("Video unavailable")))
    assert pool._idle == [] and not spawned[1][1].alive
    assert spawned[1][0].sent[-1][:2] == ("youtube", "video_metadata")


def test_pool_skips_dead_or_expired_idle_workers_and_bounds_idle_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool, _, _ = fake_pool(monkeypatch, max_idle=1, idle_timeout=60)
    first, second = pool.acquire(), pool.acquire()
    pool.release(first, reusable=True)
    pool.release(second, reusable=True)
    assert pool._idle == [first] and not second.process.alive

    first.process.alive = False
    third = pool.acquire()
    assert third is not first and pool.spawned == 3
    pool.release(third, reusable=True)
    third.idle_since -= 61
    fourth = pool.acquire()
    assert fourth is not third and not third.process.alive
    assert pool._idle == []

    fourth.process.alive = False
    pool.release(fourth, reusable=True)
    assert pool._idle == []


def test_unreachable_worker_reports_a_runtime_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool, _, _ = fake_pool(monkeypatch)
    worker = pool.acquire()
    worker.connection.fail_send = True
    pool.release(worker, reusable=True)
    with pytest.raises(RuntimeError, match="could not reach its worker"):
        workers.run_managed("youtube", "video_metadata", {}, (), {}, StopController())
    assert pool._idle == [] and pool.spawned == 1


@pytest.mark.parametrize("platform", ["posix", "nt"])
def test_served_worker_handles_requests_until_parent_hangs_up(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    sessions: list[bool] = []
    monkeypatch.setattr(
        workers,
        "os",
        SimpleNamespace(name=platform, setsid=lambda: sessions.append(True)),
    )
    monkeypatch.setattr("core.logging.configure_logging", lambda settings: None)
    handled: list[str] = []
    monkeypatch.setattr(
        workers,
        "_execute",
        lambda connection, stop, category, method, *rest: handled.append(method),
    )
    connection = FakeConnection(
        [
            ("youtube", "recent_videos", {}, (), {}),
            ("youtube", "scan_history", {}, (), {}),
        ]
    )
    workers._serve(cast(Any, connection), Event())
    assert handled == ["recent_videos", "scan_history"]
    assert connection.closed
    assert sessions == ([True] if platform == "posix" else [])


def test_shutdown_hook_stops_the_shared_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        workers, "POOL", SimpleNamespace(shutdown=lambda: calls.append("shutdown"))
    )
    workers.shutdown_workers()
    assert calls == ["shutdown"]
