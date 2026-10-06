import asyncio
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlmodel import Session, select

from core import healthcheck
from core import service as service_module
from core.config import YouTubeChannelConfig, YouTubeConfig
from core.service import MonitoredChannel, SagaService
from core.shutdown import CooperativeStop
from core.state import Channel, NotificationDelivery, StateStore, Video, utcnow
from tests.test_service import settings
from tests.test_service_coverage import CHANNEL, VIDEO, metadata


def notification_service(
    tmp_path: Path, youtube: Any
) -> tuple[StateStore, SagaService]:
    state = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    configured = replace(
        settings(tmp_path), internet_archive_email=None, internet_archive_password=None
    )
    route = YouTubeChannelConfig(
        "@channel", ("https://discord.com/api/webhooks/1/token",)
    )
    config = YouTubeConfig("primary", (route,), retries=0)
    state.register_channel(route.reference, CHANNEL)
    state.synchronize_subscriptions([(config, ((CHANNEL.id, route),))])
    state.establish_baseline(CHANNEL.id, ())
    saga = SagaService(configured, (config,), state, youtube)
    saga._channel_intervals = {CHANNEL.id: 60}
    saga.started_at = utcnow() - timedelta(minutes=10)
    return state, saga


def test_nine_channel_startup_does_not_fetch_video_metadata(tmp_path: Path) -> None:
    class Youtube:
        calls = 0

        def resolve_channel(self, reference: str) -> Any:
            self.calls += 1
            return replace(CHANNEL, id=f"UC{self.calls:022d}", handle=reference)

        def recent_videos(self, *args: Any, **kwargs: Any) -> None:
            pytest.fail("onboarding must not scan before workers start")

        def video_metadata(self, *args: Any) -> None:
            pytest.fail("onboarding must not enrich metadata")

    state = StateStore(tmp_path / "saga.sqlite")
    routes = tuple(
        YouTubeChannelConfig(
            f"@channel{index}", ("https://discord.com/api/webhooks/1/token",)
        )
        for index in range(9)
    )
    youtube = Youtube()
    saga = SagaService(
        settings(tmp_path),
        (YouTubeConfig("primary", routes),),
        state,
        cast(Any, youtube),
    )
    try:
        monitored = asyncio.run(saga._initialize())
        assert len(monitored) == youtube.calls == 9
        assert all(state.notification_baseline(item.channel.id) for item in monitored)
        assert saga._startup_progress <= time.monotonic()
    finally:
        state.close()


def test_startup_health_requires_progress_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, saga = notification_service(tmp_path, object())
    saga._initializing = True
    saga._startup_progress = 0
    clock = [0.0]
    monkeypatch.setattr(service_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(tmp_path / "healthy"))
    verdicts = []

    async def wait(_: float) -> None:
        verdicts.append(healthcheck.is_healthy(tmp_path))
        assert json.loads((tmp_path / "healthy").read_text())["phase"] == "initializing"
        if len(verdicts) == 1:
            clock[0] = 301
        elif len(verdicts) == 2:
            saga._startup_progress = clock[0]
        else:
            saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    try:
        asyncio.run(saga._health_loop())
        assert verdicts == [True, False, True]
    finally:
        state.close()


def test_service_publishes_during_initialization_and_stops_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, saga = notification_service(tmp_path, object())
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(tmp_path / "healthy"))

    async def scenario() -> None:
        release = asyncio.Event()

        async def initialize() -> Any:
            await release.wait()
            saga.stop_controller.check()

        monkeypatch.setattr(saga, "_initialize", initialize)
        task = asyncio.create_task(saga.run())
        try:
            async with asyncio.timeout(3):
                while not (tmp_path / "healthy").exists():
                    await asyncio.sleep(0.005)
            assert healthcheck.is_healthy(tmp_path)
            assert (
                json.loads((tmp_path / "healthy").read_text())["phase"]
                == "initializing"
            )
        finally:
            saga.request_stop()
            release.set()
            await task
        assert not (tmp_path / "healthy").exists()

    try:
        asyncio.run(scenario())
    finally:
        state.close()


@pytest.mark.parametrize("new_upload", [False, True])
def test_polling_continues_while_notification_metadata_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, new_upload: bool
) -> None:
    started, release = Event(), Event()

    class Youtube:
        polls = 0

        def recent_videos(self, *args: Any, **kwargs: Any) -> Any:
            self.polls += 1
            if started.is_set() and self.polls >= 3:
                saga.stop.set()
            return (replace(VIDEO, published_at=None, published_precise=False),)

        def video_metadata(self, video_id: str) -> Any:
            started.set()
            assert release.wait(5)
            baseline = state.notification_baseline(CHANNEL.id)
            return metadata(baseline + timedelta(days=1 if new_upload else -1), True)

    youtube = Youtube()
    state, saga = notification_service(tmp_path, youtube)
    monitored = MonitoredChannel(CHANNEL, 60, frozenset({"video"}))

    async def wait(_: float) -> None:
        await asyncio.sleep(0.005)

    monkeypatch.setattr(saga, "_wait", wait)

    async def scenario() -> None:
        polling = asyncio.create_task(saga._poll_loop(monitored))
        enrichment = asyncio.create_task(saga._publication_loop((monitored,)))
        try:
            await asyncio.wait_for(polling, timeout=3)
            assert started.is_set() and not enrichment.done()
            snapshot = saga._health_snapshot()
            assert snapshot["healthy"] and not snapshot["startup_grace"]
            assert snapshot["polling"][CHANNEL.id]["age_seconds"] < 5
            with Session(state.engine) as session:
                assert session.exec(select(NotificationDelivery)).first() is None
            release.set()
            await asyncio.wait_for(enrichment, timeout=3)
            with Session(state.engine) as session:
                deliveries = session.exec(select(NotificationDelivery)).all()
                assert len(deliveries) == int(new_upload)
                video = session.get(Video, VIDEO.id)
                assert video and video.notification_silent != new_upload
                stamp = session.get(Channel, CHANNEL.id)
                assert stamp and stamp.last_polled_at
        finally:
            release.set()
            saga.request_stop()
            await asyncio.gather(polling, enrichment)

    try:
        asyncio.run(scenario())
        assert youtube.polls >= 3
    finally:
        state.close()


def test_health_publication_bypasses_saturated_default_executor_and_disk_walks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, saga = notification_service(tmp_path, object())
    saga.archival = cast(Any, object())
    saga.settings.download_root.mkdir()
    saga._staging_bytes = 42
    started, release = Event(), Event()
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(tmp_path / "healthy"))

    def block() -> None:
        started.set()
        assert release.wait(5)

    def unexpected_walk(*args: Any) -> None:
        pytest.fail("health publication must not walk the staging tree")

    async def stop_after_publish(_: float) -> None:
        assert healthcheck.is_healthy(tmp_path)
        assert not release.is_set()
        saga.request_stop()

    monkeypatch.setattr(service_module, "staging_usage", unexpected_walk)
    monkeypatch.setattr(saga, "_wait", stop_after_publish)

    async def scenario() -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        asyncio.get_running_loop().set_default_executor(executor)
        occupied = asyncio.create_task(asyncio.to_thread(block))
        try:
            async with asyncio.timeout(3):
                while not started.is_set():
                    await asyncio.sleep(0.005)
                await saga._health_loop()
        finally:
            release.set()
            await occupied

    try:
        asyncio.run(scenario())
        document = json.loads((tmp_path / "transfers.json").read_text())
        assert document["healthy"] and not document["startup_grace"]
        assert document["staging_bytes"] == 42
    finally:
        state.close()


def test_delayed_health_snapshot_keeps_its_original_timestamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, saga = notification_service(tmp_path, object())
    snapshot = saga._health_snapshot()
    snapshot["generated_at"] = (utcnow() - timedelta(minutes=3)).isoformat()
    monkeypatch.setenv("SAGA_HEALTHCHECK_FILE", str(tmp_path / "healthy"))
    monkeypatch.setattr(saga, "_health_snapshot", lambda: snapshot)

    async def wait(_: float) -> None:
        saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    try:
        asyncio.run(saga._health_loop())
        assert not healthcheck.is_healthy(tmp_path)
        assert (
            json.loads((tmp_path / "healthy").read_text())["generated_at"]
            == snapshot["generated_at"]
        )
    finally:
        state.close()


def test_publication_retry_survives_restart_and_prioritizes_new_work(
    tmp_path: Path,
) -> None:
    state, _ = notification_service(tmp_path, object())
    now = utcnow()
    unknown = replace(VIDEO, published_at=None, published_precise=False)
    state.record_discoveries(CHANNEL.id, (unknown,), backfill=True)
    first = state.claim_publication((CHANNEL.id,), now=now)
    assert first and first[1].id == VIDEO.id
    state.close()
    state = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    try:
        assert (
            state.claim_publication((CHANNEL.id,), now=now + timedelta(seconds=299))
            is None
        )
        newer = replace(unknown, id="new-video-id")
        state.record_discoveries(CHANNEL.id, (newer,), backfill=True)
        first = state.claim_publication((CHANNEL.id,), now=now + timedelta(minutes=5))
        second = state.claim_publication((CHANNEL.id,), now=now + timedelta(minutes=5))
        assert first and first[1].id == newer.id
        assert second and second[1].id == unknown.id
        assert state.claim_publication((), now=now + timedelta(days=1)) is None
    finally:
        state.close()


def test_publication_retry_column_is_added_to_existing_database(tmp_path: Path) -> None:
    state, _ = notification_service(tmp_path, object())
    state.close()
    with closing(sqlite3.connect(tmp_path / "saga.sqlite")) as connection, connection:
        connection.execute("ALTER TABLE video DROP COLUMN publication_retry_at")
    reopened = StateStore(tmp_path / "saga.sqlite", archival_enabled=False)
    try:
        assert reopened.claim_publication((CHANNEL.id,)) is None
        assert reopened.notification_baseline(CHANNEL.id)
    finally:
        reopened.close()


def test_notification_metadata_stall_is_unhealthy_and_cooperative_stop_is_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Youtube:
        def video_metadata(self, video_id: str) -> None:
            raise CooperativeStop("fixture")

    state, saga = notification_service(tmp_path, Youtube())
    saga._publication_started = time.monotonic() - 901
    assert not saga._health_snapshot()["healthy"]
    state.record_discoveries(
        CHANNEL.id, (replace(VIDEO, published_at=None),), backfill=True
    )
    try:
        asyncio.run(
            saga._publication_loop(
                (MonitoredChannel(CHANNEL, 60, frozenset({"video"})),)
            )
        )
        assert saga._publication_started is None
        assert saga._health_snapshot()["healthy"]
    finally:
        state.close()


def test_scan_concurrency_is_bounded_and_history_cannot_take_poll_slots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, saga = notification_service(tmp_path, object())
    active = {"recent": 0, "history": 0}
    peak = dict(active)
    entered: list[str] = []

    async def scenario() -> None:
        release = asyncio.Event()

        async def request(operation: str, *args: Any, **kwargs: Any) -> Any:
            active[operation] += 1
            peak[operation] = max(peak[operation], active[operation])
            entered.append(operation)
            await release.wait()
            active[operation] -= 1
            return ()

        async def wait(_: float) -> None:
            saga.stop.set()

        monkeypatch.setattr(saga, "_youtube_request", request)
        monkeypatch.setattr(saga, "_wait", wait)
        monkeypatch.setattr(
            saga,
            "youtube",
            SimpleNamespace(recent_videos="recent", scan_history="history"),
        )
        monitored = MonitoredChannel(CHANNEL, 60, frozenset({"video"}))
        tasks = [
            asyncio.create_task(saga._scan_history(monitored, lambda _: None))
            for _ in range(9)
        ]
        tasks += [asyncio.create_task(saga._poll_loop(monitored)) for _ in range(9)]
        try:
            for _ in range(20):
                if len(entered) >= 3:
                    break
                await asyncio.sleep(0)
            assert sorted(entered) == ["history", "recent", "recent"]
        finally:
            release.set()
            await asyncio.gather(*tasks)
        assert peak == {"recent": 2, "history": 1}

    try:
        asyncio.run(scenario())
    finally:
        state.close()
