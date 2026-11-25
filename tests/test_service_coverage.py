import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlmodel import Session, select

from core import service as service_module
from core.config import BackupSettings, Settings, YouTubeChannelConfig, YouTubeConfig
from core.service import MonitoredChannel, SagaService
from core.state import (
    DiscoveredVideo,
    NotificationDelivery,
    Report,
    ResolvedChannel,
    StateStore,
    Video,
)
from core.youtube import VideoMetadata, YouTubeClient

CHANNEL = ResolvedChannel(
    "UCaaaaaaaaaaaaaaaaaaaaaa",
    "Channel",
    "@channel",
    "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
)
VIDEO = DiscoveredVideo(
    "abcdefghijk", "Video", datetime(2024, 1, 1, tzinfo=UTC), "not_live", True
)


def settings(
    tmp_path: Path, *, healthcheck: bool = True, backups: bool = False
) -> Settings:
    return Settings(
        config_path=tmp_path / "config.toml",
        data_path=tmp_path,
        healthcheck_file=tmp_path / "healthy" if healthcheck else None,
        youtube_cookies_path=None,
        internet_archive_email="user@example.com",
        internet_archive_password="password",
        ziggy_host=None,
        ziggy_identifier=None,
        log_level="INFO",
        backup=BackupSettings(enabled=backups, directory=tmp_path / "backups"),
        archive_retry_delay=0.01,
    )


def service_with(
    tmp_path: Path, state: Any, youtube: Any, *, healthcheck: bool = False
) -> SagaService:
    return SagaService(
        settings(tmp_path, healthcheck=healthcheck),
        (),
        state,
        youtube,
        archival=cast(Any, SimpleNamespace(process=lambda _: None)),
    )


def frozen_report(*, active: bool = False) -> Report:
    return Report(
        id=1,
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
        generated_at=datetime(2026, 1, 2, 0, 5, tzinfo=UTC),
        discovered_videos=1 if active else 0,
    )


def test_run_starts_named_channel_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    service = SagaService(
        settings(tmp_path, healthcheck=True, backups=True),
        (),
        store,
        YouTubeClient(),
        archival=cast(Any, SimpleNamespace(process=lambda _: None, derives=object())),
    )
    monitored = MonitoredChannel(CHANNEL, 1, frozenset({"video"}))
    names: list[str | None] = []

    async def initialize() -> tuple[MonitoredChannel, ...]:
        return (monitored,)

    async def worker(*args: Any, **kwargs: Any) -> None:
        task = asyncio.current_task()
        assert task is not None
        names.append(task.get_name())

    monkeypatch.setattr(service, "_initialize", initialize)
    monkeypatch.setattr(service, "_poll_loop", worker)
    monkeypatch.setattr(service, "_reconciliation_loop", worker)
    monkeypatch.setattr(service, "_delivery_loop", worker)
    monkeypatch.setattr(service, "_report_loop", worker)
    monkeypatch.setattr(service, "_archive_loop", worker)
    monkeypatch.setattr(service, "_health_loop", worker)
    monkeypatch.setattr(service, "_publication_loop", worker)
    monkeypatch.setattr(service, "_partial_maintenance_loop", worker)
    monkeypatch.setattr(service, "_derive_loop", worker)
    monkeypatch.setattr(service_module, "run_backup_scheduler", worker)
    service.stop.set()

    asyncio.run(service.run())

    assert set(names) == {
        "derive",
        f"poll:{CHANNEL.id}",
        f"history:{CHANNEL.id}",
        "discord",
        "reporting",
        "backup-scheduler",
        "upload:0",
        "download:0",
        "metadata:0",
        "cleanup:0",
        "webpage:0",
        "recovery:0",
        "health",
        "publication",
        "partial-maintenance",
    }
    assert not (tmp_path / "healthy").exists()
    store.close()


def test_run_skips_archive_workers_when_archival_is_disabled(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite", archival_enabled=False)
    disabled = replace(
        settings(tmp_path), internet_archive_email=None, internet_archive_password=None
    )
    service = SagaService(disabled, (), store, YouTubeClient())
    service.stop.set()

    asyncio.run(service.run())

    assert service.archival is None
    store.close()


def test_youtube_request_helpers_retry_configured_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = service_with(tmp_path, SimpleNamespace(), SimpleNamespace())
    outcomes: list[Any] = [RuntimeError("temporary"), "result"]
    waits: list[float] = []

    def operation() -> str:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def wait(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(service, "_wait", wait)
    assert asyncio.run(service._youtube_request(operation, 1, 2.5)) == "result"
    assert waits == [2.5]

    outcomes[:] = [RuntimeError("temporary"), "sync-result"]
    sleeps: list[float] = []
    monkeypatch.setattr(service.stop_controller, "wait", sleeps.append)
    assert service._youtube_request_sync(operation, 1, 3) == "sync-result"
    assert sleeps == [3]

    with pytest.raises(RuntimeError, match="final"):
        asyncio.run(
            service._youtube_request(
                lambda: (_ for _ in ()).throw(RuntimeError("final")), 0, 0
            )
        )


def test_report_loop_initializes_without_historical_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), True

        def claim_report(self, now: datetime) -> None:
            return None

    service = service_with(tmp_path, State(), SimpleNamespace())

    async def wait(_: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    asyncio.run(service._report_loop())


def test_report_loop_generates_and_suppresses_empty_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    completed: list[tuple[int, str, str | None]] = []
    item = frozen_report()

    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), False

        def create_report_snapshot(self, *args: Any, **kwargs: Any) -> Report:
            return item

        def claim_report(self, now: datetime) -> Report:
            return item

        def complete_report(
            self, report_id: int, status: str, message_id: str | None
        ) -> None:
            completed.append((report_id, status, message_id))
            service.stop.set()

    service = service_with(tmp_path, State(), SimpleNamespace())
    monkeypatch.setattr(
        service_module,
        "next_report_window",
        lambda *_: SimpleNamespace(start=item.window_start, end=item.window_end),
    )

    asyncio.run(service._report_loop())

    assert completed == [(1, "suppressed", None)]


def test_report_loop_waits_when_no_window_or_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), False

        def claim_report(self, now: datetime) -> None:
            return None

    service = service_with(tmp_path, State(), SimpleNamespace())
    monkeypatch.setattr(service_module, "next_report_window", lambda *_: None)

    async def wait(_: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    asyncio.run(service._report_loop())


def test_report_loop_logs_without_webhook(tmp_path: Path) -> None:
    item = frozen_report(active=True)
    completed: list[str] = []

    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), True

        def claim_report(self, now: datetime) -> Report:
            return item

        def complete_report(self, report_id: int, status: str, message_id: Any) -> None:
            completed.append(status)
            service.stop.set()

    service = service_with(tmp_path, State(), SimpleNamespace())
    asyncio.run(service._report_loop())
    assert completed == ["logged"]


@pytest.mark.parametrize("failure", [False, True])
def test_report_loop_delivers_or_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: bool
) -> None:
    item = frozen_report(active=True)
    events: list[tuple[Any, ...]] = []

    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), True

        def claim_report(self, now: datetime) -> Report:
            return item

        def complete_report(self, *args: Any) -> None:
            events.append(("complete", *args))
            service.stop.set()

        def fail_report(self, report_id: int, error: str) -> float:
            events.append(("fail", report_id, error))
            service.stop.set()
            return 2

    configured = replace(
        settings(tmp_path),
        report_discord_webhook_url="https://discord.com/api/webhooks/1/secret",
    )
    service = SagaService(
        configured,
        (),
        cast(Any, State()),
        cast(Any, SimpleNamespace()),
        archival=cast(Any, SimpleNamespace(process=lambda _: None)),
    )

    async def send(*_: Any) -> str:
        if failure:
            raise RuntimeError("https://discord.com/api/webhooks/1/secret failed")
        return "message-id"

    monkeypatch.setattr(service_module, "send_report", send)
    asyncio.run(service._report_loop())

    if failure:
        assert events[0][:2] == ("fail", 1)
        assert "secret" not in events[0][2]
    else:
        assert events == [("complete", 1, "sent", "message-id")]


def test_report_loop_contains_state_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class State:
        def initialize_report_checkpoint(self, *_: Any) -> None:
            raise RuntimeError("database failed")

    service = service_with(tmp_path, State(), SimpleNamespace())

    async def wait(_: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    asyncio.run(service._report_loop())


def test_report_loop_propagates_delivery_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    item = frozen_report(active=True)

    class State:
        def initialize_report_checkpoint(self, end: datetime, schedule: str) -> Any:
            return SimpleNamespace(last_window_end=end), True

        def claim_report(self, now: datetime) -> Report:
            return item

    configured = replace(
        settings(tmp_path),
        report_discord_webhook_url="https://discord.com/api/webhooks/1/token",
    )
    service = SagaService(
        configured,
        (),
        cast(Any, State()),
        cast(Any, SimpleNamespace()),
        archival=cast(Any, SimpleNamespace(process=lambda _: None)),
    )

    async def cancel(*_: Any) -> str:
        raise asyncio.CancelledError

    monkeypatch.setattr(service_module, "send_report", cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service._report_loop())


def test_initialize_reuses_resolution_and_merges_channel_policy(tmp_path: Path) -> None:
    class Youtube:
        def __init__(self) -> None:
            self.resolve_calls = 0
            self.recent_calls = 0

        def resolve_channel(self, reference: str) -> ResolvedChannel:
            self.resolve_calls += 1
            return CHANNEL

        def recent_videos(self, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
            self.recent_calls += 1
            return ()

    first_channel = YouTubeChannelConfig(
        "@channel",
        ("https://discord.com/api/webhooks/1/first",),
        shorts=False,
        livestreams=False,
    )
    second_channel = YouTubeChannelConfig(
        "@channel",
        ("https://discord.com/api/webhooks/2/second",),
        videos=False,
        shorts=False,
    )
    configurations = (
        YouTubeConfig("first", (first_channel,), cooldown=60),
        YouTubeConfig("second", (second_channel,), cooldown=10),
    )
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("seed", CHANNEL)
    store.establish_baseline(CHANNEL.id, [])
    client = Youtube()
    service = SagaService(settings(tmp_path), configurations, store, cast(Any, client))

    result = asyncio.run(service._initialize())

    assert client.resolve_calls == 1
    assert client.recent_calls == 0
    assert result == (
        MonitoredChannel(CHANNEL, 10, frozenset({"video", "livestream"}), 3, 5),
    )
    store.close()


def test_poll_loop_records_successes_and_survives_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        outcomes: list[Any] = [(VIDEO,), (VIDEO,), RuntimeError("scan failed")]

        def recent_videos(
            self, *args: Any, **kwargs: Any
        ) -> tuple[DiscoveredVideo, ...]:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    class State:
        additions = [1, 0]
        recorded: list[tuple[Any, ...]] = []

        def record_discoveries(self, *args: Any, **kwargs: Any) -> int:
            assert kwargs == {"backfill": False}
            self.recorded.append(args)
            return self.additions.pop(0)

    state = State()
    service = service_with(tmp_path, state, Youtube())
    waits = 0

    async def wait(_: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 3:
            service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)

    asyncio.run(service._poll_loop(MonitoredChannel(CHANNEL, 1, frozenset({"video"}))))

    assert state.recorded == [(CHANNEL.id, (VIDEO,)), (CHANNEL.id, (VIDEO,))]


def test_poll_loop_propagates_cancellation(tmp_path: Path) -> None:
    class Youtube:
        def recent_videos(self, *args: Any, **kwargs: Any) -> None:
            raise asyncio.CancelledError

    service = service_with(tmp_path, SimpleNamespace(), Youtube())
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            service._poll_loop(MonitoredChannel(CHANNEL, 1, frozenset({"video"})))
        )


def test_reconciliation_records_batches_and_survives_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        calls = 0

        def scan_history(self, channel: Any, callback: Any, **kwargs: Any) -> None:
            self.calls += 1
            if self.calls == 1:
                callback((VIDEO,))
            else:
                raise RuntimeError("history failed")

    class State:
        recorded: list[tuple[Any, ...]] = []
        completed = 0

        def publication_detail_ids(self, ids: Any) -> set[str]:
            return set(ids)

        def record_discoveries(
            self, channel_id: str, batch: Any, *, backfill: bool
        ) -> None:
            self.recorded.append((channel_id, batch, backfill))

        def mark_backfill_complete(self, channel_id: str) -> None:
            self.completed += 1

        def backfill_completed_at(self, channel_id: str) -> None:
            return None

    state = State()
    service = service_with(tmp_path, state, Youtube())
    waits = 0

    async def wait(_: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 2:
            service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)

    asyncio.run(
        service._reconciliation_loop(MonitoredChannel(CHANNEL, 1, frozenset({"video"})))
    )

    assert state.completed == 1
    assert state.recorded[0][1][0].title == "Video"
    assert len(state.recorded) == 1
    assert all(item[2] is True for item in state.recorded)


def test_reconciliation_queues_archive_before_resolving_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    route = YouTubeChannelConfig(
        "@channel", ("https://discord.com/api/webhooks/1/token",)
    )
    store.synchronize_subscriptions(
        [(YouTubeConfig("primary", (route,)), ((CHANNEL.id, route),))]
    )
    baseline = store.establish_baseline(CHANNEL.id, ())
    video = replace(
        VIDEO, published_at=None, live_status="is_live", published_precise=False
    )
    claimed: list[str] = []

    class Youtube:
        def scan_history(self, channel: Any, callback: Any, **kwargs: Any) -> None:
            callback((video,))

        def video_metadata(self, video_id: str) -> VideoMetadata:
            work = store.claim_archive(prefer_recent=False)
            assert work is not None
            claimed.append(work.video.id)
            with Session(store.engine) as session:
                assert session.exec(select(NotificationDelivery)).first() is None
            return replace(metadata(baseline + timedelta(seconds=1), True), id=video_id)

    service = service_with(tmp_path, store, Youtube())

    async def wait(_: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    try:
        asyncio.run(
            service._reconciliation_loop(
                MonitoredChannel(CHANNEL, 1, frozenset({"video"}))
            )
        )
        assert claimed == []
        service.stop.clear()
        asyncio.run(
            service._publication_loop(
                (MonitoredChannel(CHANNEL, 1, frozenset({"video"})),)
            )
        )
        assert claimed == [video.id]
    finally:
        store.close()


def test_reconciliation_cancellation_waits_for_thread(tmp_path: Path) -> None:
    started = Event()
    finish = Event()

    class Youtube:
        def scan_history(self, *args: Any, **kwargs: Any) -> None:
            started.set()
            finish.wait(timeout=5)
            raise RuntimeError("late failure")

    service = service_with(
        tmp_path, SimpleNamespace(backfill_completed_at=lambda _: None), Youtube()
    )

    async def scenario() -> None:
        task = asyncio.create_task(
            service._reconciliation_loop(
                MonitoredChannel(CHANNEL, 1, frozenset({"video"}))
            )
        )
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        asyncio.run(scenario())
    finally:
        finish.set()


def metadata(published_at: datetime | None, precise: bool) -> VideoMetadata:
    return VideoMetadata(
        VIDEO.id,
        "Video",
        "Description",
        "20240101",
        CHANNEL.id,
        CHANNEL.title,
        CHANNEL.handle or CHANNEL.id,
        CHANNEL.url,
        "https://www.youtube.com/watch?v=abcdefghijk",
        None,
        published_at,
        precise,
    )


def test_history_resolves_uploads_beyond_the_recent_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    route = YouTubeChannelConfig(
        "@channel", ("https://discord.com/api/webhooks/1/token",)
    )
    store.synchronize_subscriptions(
        [(YouTubeConfig("primary", (route,)), ((CHANNEL.id, route),))]
    )
    videos = tuple(
        replace(VIDEO, id=f"{index:011d}", published_at=None, published_precise=False)
        for index in range(33)
    )
    baseline = store.establish_baseline(CHANNEL.id, videos[:1])
    store.record_discoveries(
        CHANNEL.id,
        [
            replace(
                video,
                published_at=baseline + timedelta(seconds=1),
                published_precise=True,
            )
            for video in videos[2:32]
        ],
        backfill=False,
    )
    detail_calls: list[str] = []

    class Youtube:
        scans = 0

        def scan_history(self, channel: Any, callback: Any, **kwargs: Any) -> None:
            self.scans += 1
            callback(videos)

        def video_metadata(self, video_id: str) -> VideoMetadata:
            detail_calls.append(video_id)
            if video_id == videos[1].id and detail_calls.count(video_id) == 1:
                raise RuntimeError("temporary metadata failure")
            published = baseline + timedelta(
                days=-1 if video_id == videos[-1].id else 1
            )
            return replace(metadata(published, True), id=video_id)

    youtube = Youtube()
    service = service_with(tmp_path, store, youtube)

    async def wait(_: float) -> None:
        service.stop.set()

    monkeypatch.setattr(service, "_wait", wait)
    try:
        asyncio.run(
            service._reconciliation_loop(
                MonitoredChannel(CHANNEL, 1, frozenset({"video"}))
            )
        )
        for _ in range(2):
            service.stop.clear()
            with Session(store.engine) as session:
                for row in session.exec(select(Video)).all():
                    row.publication_retry_at = None
                    session.add(row)
                session.commit()
            asyncio.run(
                service._publication_loop(
                    (MonitoredChannel(CHANNEL, 1, frozenset({"video"}), retries=0),)
                )
            )

        with Session(store.engine) as session:
            deliveries = session.exec(select(NotificationDelivery)).all()
            assert sorted(item.video_id for item in deliveries) == [
                video.id for video in videos[1:32]
            ]
        assert detail_calls == [videos[1].id, videos[-1].id, videos[1].id]
        assert store.publication_detail_ids(video.id for video in videos) == set()
    finally:
        store.close()


def test_resolve_publications_selects_replaces_and_preserves(tmp_path: Path) -> None:
    baseline = datetime(2024, 1, 1, 12, tzinfo=UTC)
    ambiguous = replace(
        VIDEO, published_at=baseline.replace(hour=0), published_precise=False
    )
    missing = replace(VIDEO, id="bbbbbbbbbbb", published_at=None)
    precise = replace(VIDEO, id="ccccccccccc", published_precise=True)
    failed = replace(VIDEO, id="ddddddddddd", published_at=None)

    class State:
        def notification_baseline(self, channel_id: str) -> datetime:
            return baseline

        def publication_detail_ids(self, ids: Any) -> set[str]:
            return set(ids)

        def record_publication_failure(
            self, video_id: str, error: str, *, permanent: bool
        ) -> datetime:
            assert (video_id, error, permanent) == (
                failed.id,
                "RuntimeError: failed",
                False,
            )
            return baseline

    class Youtube:
        def video_metadata(self, video_id: str) -> VideoMetadata:
            if video_id == failed.id:
                raise RuntimeError("failed")
            if video_id == ambiguous.id:
                return metadata(baseline + timedelta(hours=1), True)
            return replace(metadata(None, False), id=video_id)

    service = service_with(tmp_path, State(), Youtube())

    result = service._resolve_publications(
        CHANNEL,
        (ambiguous, missing, precise, failed),
        {ambiguous.id, missing.id, failed.id},
    )
    all_result = service._resolve_publications(CHANNEL, (ambiguous,))

    assert result[0].published_at == baseline + timedelta(hours=1)
    assert result[1].published_at is None
    assert result[2] is precise
    assert result[3] is failed
    assert all_result[0].published_precise is True


def test_delivery_loop_handles_idle_success_failures_and_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "https://discord.com/api/webhooks/1/secret"
    deliveries = [
        None,
        SimpleNamespace(id=1, webhook_url=secret, video_id="aaaaaaaaaaa"),
        SimpleNamespace(id=2, webhook_url=secret, video_id="bbbbbbbbbbb"),
        SimpleNamespace(id=3, webhook_url=secret, video_id="ccccccccccc"),
        SimpleNamespace(id=4, webhook_url=secret, video_id="ddddddddddd"),
    ]

    class State:
        completed: list[tuple[int, str]] = []
        failures: list[tuple[int, str]] = []

        def claim_delivery(self) -> Any:
            return deliveries.pop(0)

        def complete_delivery(self, delivery_id: int, message_id: str) -> None:
            self.completed.append((delivery_id, message_id))

        def fail_delivery(self, delivery_id: int, detail: str) -> bool:
            self.failures.append((delivery_id, detail))
            return delivery_id == 2

    state = State()
    service = service_with(tmp_path, state, SimpleNamespace())

    async def send(webhook: str, video_id: str, timeout: float) -> str:
        if video_id == "aaaaaaaaaaa":
            return "message"
        if video_id == "ddddddddddd":
            raise asyncio.CancelledError
        raise RuntimeError(secret + " " + "x" * 2100)

    async def wait(_: float) -> None:
        pass

    monkeypatch.setattr(service_module, "send_video", send)
    monkeypatch.setattr(service, "_wait", wait)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service._delivery_loop())

    assert state.completed == [(1, "message")]
    assert [item[0] for item in state.failures] == [2, 3]
    assert secret not in state.failures[0][1]
    assert len(state.failures[0][1]) == 2000


def test_archive_loop_rotates_preference_and_handles_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = SimpleNamespace(video=SimpleNamespace(id="abcdefghijk"))

    class State:
        preferences: list[bool] = []

        def claim_archive(self, *, prefer_recent: bool, **kwargs: Any) -> Any:
            self.preferences.append(prefer_recent)
            return None if len(self.preferences) == 1 else work

        def record_worker_time(self, *args: Any) -> None:
            pass

    state = State()
    service = service_with(tmp_path, state, SimpleNamespace())

    def process(_: Any) -> None:
        service.stop.set()
        raise RuntimeError("worker failed")

    async def wait(_: float) -> None:
        pass

    monkeypatch.setattr(service.archival, "process", process)
    monkeypatch.setattr(service, "_wait", wait)

    asyncio.run(service._archive_loop(2))

    assert state.preferences == [True, True]


def test_wait_and_healthcheck_noop_paths(tmp_path: Path) -> None:
    service = service_with(tmp_path, SimpleNamespace(), SimpleNamespace())
    asyncio.run(service._wait(0))
    service.stop.set()
    asyncio.run(service._wait(1))
    service._write_healthcheck()
    service._remove_healthcheck()
