import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from sqlmodel import Session, select

from core.config import BackupSettings, Settings, YouTubeChannelConfig, YouTubeConfig
from core.service import MonitoredChannel, SagaService
from core.state import (
    DiscoveredVideo,
    NotificationDelivery,
    ResolvedChannel,
    StateStore,
)
from core.youtube import YouTubeClient


def settings(tmp_path: Path) -> Settings:
    return Settings(
        config_path=tmp_path / "config.toml",
        data_path=tmp_path,
        healthcheck_file=tmp_path / "healthy",
        youtube_cookies_path=None,
        internet_archive_email="user@example.com",
        internet_archive_password="password",
        ziggy_host=None,
        ziggy_identifier=None,
        log_level="INFO",
        backup=BackupSettings(enabled=False, directory=tmp_path / "backups"),
        archive_retry_delay=0.01,
    )


class EmptyService(SagaService):
    async def _initialize(self) -> tuple[MonitoredChannel, ...]:
        return ()


class ResolvingYouTube:
    def __init__(self, channels: dict[str, ResolvedChannel]) -> None:
        self.channels = channels

    def resolve_channel(self, reference: str) -> ResolvedChannel:
        return self.channels[reference]

    def recent_videos(
        self, channel: ResolvedChannel, content_types: object
    ) -> tuple[DiscoveredVideo, ...]:
        return ()


def test_initialize_preserves_per_channel_webhook_routes(tmp_path: Path) -> None:
    first = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="First",
        handle="@first",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    second = ResolvedChannel(
        id="UCbbbbbbbbbbbbbbbbbbbbbb",
        title="Second",
        handle="@second",
        url="https://www.youtube.com/channel/UCbbbbbbbbbbbbbbbbbbbbbb",
    )
    first_webhook = "https://discord.com/api/webhooks/123/first"
    second_webhook = "https://discord.com/api/webhooks/456/second"
    config = YouTubeConfig(
        name="primary",
        channels=(
            YouTubeChannelConfig("@first", (first_webhook,)),
            YouTubeChannelConfig("@second", (second_webhook,)),
        ),
        cooldown=45,
        retries=4,
        retry_delay=10,
    )
    state = StateStore(tmp_path / "saga.sqlite")
    youtube: Any = ResolvingYouTube({"@first": first, "@second": second})
    service = SagaService(settings(tmp_path), (config,), state, youtube)

    monitored = asyncio.run(service._initialize())

    assert {(item.channel.id, item.cooldown) for item in monitored} == {
        (first.id, 45),
        (second.id, 45),
    }
    assert all(
        item.content_types == {"video", "short", "livestream"} for item in monitored
    )
    for channel, video_id in ((first, "aaaaaaaaaaa"), (second, "bbbbbbbbbbb")):
        baseline = state.notification_baseline(channel.id)
        state.record_discoveries(
            channel.id,
            [
                DiscoveredVideo(
                    id=video_id,
                    title=video_id,
                    published_at=baseline + timedelta(seconds=1),
                    live_status="not_live",
                    replay_ready=True,
                )
            ],
            backfill=False,
        )

    with Session(state.engine) as session:
        deliveries = session.exec(
            select(NotificationDelivery).order_by(NotificationDelivery.video_id)
        ).all()
        assert [
            (item.video_id, item.webhook_url, item.max_attempts, item.retry_delay)
            for item in deliveries
        ] == [
            ("aaaaaaaaaaa", first_webhook, 5, 10),
            ("bbbbbbbbbbb", second_webhook, 5, 10),
        ]
    state.close()


def test_initialize_persists_baseline_before_recent_request(tmp_path: Path) -> None:
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state = StateStore(tmp_path / "saga.sqlite")

    class Youtube:
        def resolve_channel(self, reference: str) -> ResolvedChannel:
            return channel

        def recent_videos(self, *_: Any, **__: Any) -> tuple[Any, ...]:
            assert state.notification_baseline(channel.id)
            return ()

    config = YouTubeConfig(
        name="primary",
        channels=(
            YouTubeChannelConfig(
                "@channel", ("https://discord.com/api/webhooks/123/token",)
            ),
        ),
    )
    youtube: Any = Youtube()
    service = SagaService(settings(tmp_path), (config,), state, youtube)

    asyncio.run(service._initialize())

    state.close()


def test_service_stops_cooperatively_and_removes_healthcheck(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    service = EmptyService(settings(tmp_path), (), state, YouTubeClient())
    service.request_stop()

    asyncio.run(service.run())

    assert not (tmp_path / "healthy").exists()
    state.close()


def test_service_clears_previous_heartbeat_before_initializing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    service = SagaService(settings(tmp_path), (), state, YouTubeClient())
    heartbeat = tmp_path / "healthy"
    heartbeat.touch()

    async def initialize() -> tuple[MonitoredChannel, ...]:
        assert not heartbeat.exists()
        raise RuntimeError("initialization failed")

    monkeypatch.setattr(service, "_initialize", initialize)
    try:
        with pytest.raises(RuntimeError, match="initialization failed"):
            asyncio.run(service.run())
        assert not heartbeat.exists()
    finally:
        state.close()


def test_worker_failure_is_supervised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    service = EmptyService(settings(tmp_path), (), state, YouTubeClient())

    def fail_claim(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("database failed")

    monkeypatch.setattr(state, "claim_delivery", fail_claim)

    with pytest.raises(ExceptionGroup) as caught:
        asyncio.run(service.run())

    assert caught.value.subgroup(RuntimeError) is not None
    assert not (tmp_path / "healthy").exists()
    state.close()


def test_cancellation_waits_for_active_archive_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    channel_config = YouTubeChannelConfig(
        "@channel", ("https://discord.com/api/webhooks/123/token",)
    )
    state.synchronize_subscriptions(
        [
            (
                YouTubeConfig(name="primary", channels=(channel_config,)),
                ((channel.id, channel_config),),
            )
        ]
    )
    state.establish_baseline(
        channel.id,
        [
            DiscoveredVideo(
                id="abcdefghijk",
                title="Video",
                published_at=datetime(2020, 1, 1, tzinfo=UTC),
                live_status="not_live",
                replay_ready=True,
            )
        ],
    )
    service = EmptyService(settings(tmp_path), (), state, YouTubeClient())
    started = Event()
    finish = Event()

    def block_archive(work: Any) -> None:
        started.set()
        finish.wait(timeout=5)

    monkeypatch.setattr(service.archival, "process", block_archive)

    async def scenario() -> None:
        task = asyncio.create_task(service.run())
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        finish.set()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert not (tmp_path / "healthy").exists()
    state.close()
