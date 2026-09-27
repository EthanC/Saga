import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlmodel import Session, select

from core.config import ContentType, YouTubeChannelConfig, YouTubeConfig
from core.state import (
    ArchiveProgress,
    DiscoveredVideo,
    NotificationDelivery,
    ResolvedChannel,
    StateStore,
)

WEBHOOK_1 = "https://discord.com/api/webhooks/123/first"
WEBHOOK_2 = "https://discord.com/api/webhooks/456/second"
WEBHOOK_1_ROTATED = "https://discord.com/api/webhooks/123/rotated"


def discovery(
    video_id: str,
    published_at: datetime,
    *,
    ready: bool = True,
    content_type: ContentType = "video",
) -> DiscoveredVideo:
    return DiscoveredVideo(
        id=video_id,
        title=video_id,
        published_at=published_at,
        live_status="not_live" if ready else "is_live",
        replay_ready=ready,
        content_type=content_type,
    )


def configured(webhook: str) -> YouTubeConfig:
    return YouTubeConfig(
        name="primary", channels=(YouTubeChannelConfig("@channel", (webhook,)),)
    )


def subscriptions(
    config: YouTubeConfig, channel_id: str
) -> list[tuple[YouTubeConfig, tuple[tuple[str, YouTubeChannelConfig], ...]]]:
    return [(config, ((channel_id, config.channels[0]),))]


def test_existing_database_gets_new_schema_columns(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE subscription (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO subscription (id) VALUES (1)")
        connection.execute("CREATE TABLE video (id VARCHAR PRIMARY KEY)")
        connection.execute("INSERT INTO video (id) VALUES ('abcdefghijk')")
        connection.execute(
            "CREATE TABLE reportcheckpoint ("
            "id INTEGER PRIMARY KEY, last_window_end DATETIME NOT NULL, "
            "interval_seconds FLOAT NOT NULL, initialized_at DATETIME NOT NULL, "
            "updated_at DATETIME NOT NULL)"
        )
        connection.execute(
            "INSERT INTO reportcheckpoint VALUES "
            "(1, '2026-01-01 00:00:00', 86400, '2026-01-01 00:00:00', "
            "'2026-01-01 00:00:00')"
        )
        connection.commit()

    state = StateStore(path)

    with state.engine.connect() as connection:
        policy = connection.exec_driver_sql(
            "SELECT videos, shorts, livestreams FROM subscription"
        ).one()
        content_type = connection.exec_driver_sql(
            "SELECT content_type FROM video"
        ).scalar_one()
    assert policy == (1, 1, 1)
    assert content_type == "video"
    checkpoint, initialized = state.initialize_report_checkpoint(
        datetime(2026, 1, 2, tzinfo=UTC), "0 0 * * *"
    )
    assert initialized is True
    assert checkpoint.schedule == "0 0 * * *"
    assert checkpoint.last_window_end.replace(tzinfo=UTC) == datetime(
        2026, 1, 2, tzinfo=UTC
    )
    state.close()


def test_baseline_is_silent_and_new_video_is_queued(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    old = discovery("aaaaaaaaaaa", datetime(2020, 1, 1, tzinfo=UTC))

    baseline = state.establish_baseline(channel.id, [old])

    with Session(state.engine) as session:
        assert session.exec(select(NotificationDelivery)).all() == []
        assert session.get(ArchiveProgress, old.id) is not None

    new = discovery("bbbbbbbbbbb", baseline + timedelta(seconds=1))
    assert state.record_discoveries(channel.id, [new], backfill=False) == 1

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == new.id
    assert delivery.attempts == 1
    assert delivery.max_attempts == 4
    assert delivery.id is not None
    state.complete_delivery(delivery.id, "message-id")

    with Session(state.engine) as session:
        saved = session.get(NotificationDelivery, delivery.id)
        assert saved is not None
        assert saved.status == "sent"
        assert saved.message_id == "message-id"
    state.close()


def test_disabled_archival_does_not_queue_videos(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite", archival_enabled=False)
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))

    state.establish_baseline(
        channel.id, [discovery("aaaaaaaaaaa", datetime(2020, 1, 1, tzinfo=UTC))]
    )

    with Session(state.engine) as session:
        assert session.exec(select(ArchiveProgress)).all() == []
    state.close()


def test_channel_content_toggles_filter_delivery_and_archival(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    config = YouTubeConfig(
        name="primary",
        channels=(
            YouTubeChannelConfig(
                "@channel", (WEBHOOK_1,), videos=False, shorts=True, livestreams=False
            ),
        ),
    )
    state.synchronize_subscriptions(subscriptions(config, channel.id))
    baseline = state.establish_baseline(channel.id, [])

    videos = (
        discovery("vvvvvvvvvvv", baseline + timedelta(seconds=1)),
        discovery("sssssssssss", baseline + timedelta(seconds=2), content_type="short"),
        discovery(
            "lllllllllll", baseline + timedelta(seconds=3), content_type="livestream"
        ),
    )
    state.record_discoveries(channel.id, videos, backfill=False)

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == "sssssssssss"
    assert state.claim_delivery() is None
    with Session(state.engine) as session:
        archived = session.exec(select(ArchiveProgress)).all()
        assert [item.video_id for item in archived] == ["sssssssssss"]
    state.close()


def test_livestream_notifies_before_replay_is_ready(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    initial_live = discovery(
        "ccccccccccc", datetime(2020, 1, 1, tzinfo=UTC), ready=False
    )
    baseline = state.establish_baseline(channel.id, [initial_live])

    with Session(state.engine) as session:
        assert session.get(ArchiveProgress, initial_live.id) is None

    state.record_discoveries(
        channel.id,
        [discovery(initial_live.id, datetime(2020, 1, 1, tzinfo=UTC), ready=True)],
        backfill=False,
    )
    assert state.claim_delivery() is None
    with Session(state.engine) as session:
        assert session.get(ArchiveProgress, initial_live.id) is not None

    future_live = discovery("ddddddddddd", baseline + timedelta(seconds=1), ready=False)
    state.record_discoveries(channel.id, [future_live], backfill=False)
    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == future_live.id
    assert delivery.id is not None
    state.complete_delivery(delivery.id, "message-id")

    with Session(state.engine) as session:
        assert session.get(ArchiveProgress, future_live.id) is None

    state.record_discoveries(channel.id, [future_live], backfill=False)
    state.record_discoveries(
        channel.id,
        [discovery(future_live.id, baseline + timedelta(seconds=1), ready=True)],
        backfill=False,
    )

    assert state.claim_delivery() is None
    with Session(state.engine) as session:
        deliveries = session.exec(
            select(NotificationDelivery).where(
                NotificationDelivery.video_id == future_live.id
            )
        ).all()
        assert len(deliveries) == 1
        assert session.get(ArchiveProgress, future_live.id) is not None
    state.close()


def test_webhook_rotation_does_not_requeue_existing_videos(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])
    first = discovery("eeeeeeeeeee", baseline + timedelta(seconds=1))
    state.record_discoveries(channel.id, [first], backfill=False)

    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_2), channel.id))
    second = discovery("fffffffffff", baseline + timedelta(seconds=2))
    state.record_discoveries(channel.id, [second], backfill=False)

    with Session(state.engine) as session:
        deliveries = session.exec(
            select(NotificationDelivery).order_by(NotificationDelivery.video_id)
        ).all()
        assert [(item.video_id, item.webhook_url) for item in deliveries] == [
            (first.id, WEBHOOK_1),
            (second.id, WEBHOOK_2),
        ]
    state.close()


def test_token_rotation_updates_pending_delivery_without_replay(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])
    video = discovery("kkkkkkkkkkk", baseline + timedelta(seconds=1))
    state.record_discoveries(channel.id, [video], backfill=False)

    state.synchronize_subscriptions(
        subscriptions(configured(WEBHOOK_1_ROTATED), channel.id)
    )
    state.record_discoveries(channel.id, [video], backfill=False)

    with Session(state.engine) as session:
        deliveries = session.exec(select(NotificationDelivery)).all()
        assert len(deliveries) == 1
        assert deliveries[0].webhook_url == WEBHOOK_1_ROTATED
    state.close()


def test_unknown_backfill_date_can_be_promoted_by_recent_scan(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    state.establish_baseline(channel.id, [])
    video = DiscoveredVideo(
        id="lllllllllll",
        title="unknown date",
        published_at=None,
        live_status="not_live",
        replay_ready=True,
    )

    state.record_discoveries(channel.id, [video], backfill=True)
    assert state.claim_delivery() is None
    resolved = DiscoveredVideo(
        id=video.id,
        title=video.title,
        published_at=state.notification_baseline(channel.id) + timedelta(seconds=1),
        live_status=video.live_status,
        replay_ready=True,
        published_precise=True,
    )
    state.record_discoveries(channel.id, [resolved], backfill=False)

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == video.id
    state.close()


def test_upcoming_livestream_found_by_history_is_promoted_by_recent_scan(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    state.establish_baseline(channel.id, [])
    upcoming = DiscoveredVideo(
        id="ooooooooooo",
        title="Upcoming",
        published_at=None,
        live_status="is_upcoming",
        replay_ready=False,
        published_precise=False,
        content_type="livestream",
    )

    state.record_discoveries(channel.id, [upcoming], backfill=True)
    assert state.claim_delivery() is None
    state.record_discoveries(channel.id, [upcoming], backfill=False)

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == upcoming.id
    with Session(state.engine) as session:
        assert session.get(ArchiveProgress, upcoming.id) is None
    state.close()


def test_adding_webhook_does_not_replay_existing_video(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])
    video = discovery("mmmmmmmmmmm", baseline + timedelta(seconds=1))
    state.record_discoveries(channel.id, [video], backfill=False)

    config = YouTubeConfig(
        name="primary",
        channels=(YouTubeChannelConfig("@channel", (WEBHOOK_1, WEBHOOK_2)),),
    )
    state.synchronize_subscriptions(subscriptions(config, channel.id))
    state.record_discoveries(channel.id, [video], backfill=False)

    with Session(state.engine) as session:
        deliveries = session.exec(select(NotificationDelivery)).all()
        assert len(deliveries) == 1
        assert deliveries[0].webhook_url == WEBHOOK_1
    state.close()


def test_date_only_same_day_backfill_waits_for_exact_timestamp(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])
    date_only = DiscoveredVideo(
        id="nnnnnnnnnnn",
        title="same day",
        published_at=baseline.replace(hour=0, minute=0, second=0, microsecond=0),
        live_status="not_live",
        replay_ready=True,
        published_precise=False,
    )
    state.record_discoveries(channel.id, [date_only], backfill=True)
    assert state.claim_delivery() is None

    exact = DiscoveredVideo(
        id=date_only.id,
        title=date_only.title,
        published_at=baseline + timedelta(seconds=1),
        live_status="not_live",
        replay_ready=True,
        published_precise=True,
    )
    state.record_discoveries(channel.id, [exact], backfill=False)

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == date_only.id
    state.close()


def test_backfill_notifies_only_uploads_after_baseline(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])

    state.record_discoveries(
        channel.id,
        [
            discovery("ggggggggggg", baseline - timedelta(days=1)),
            discovery("hhhhhhhhhhh", baseline + timedelta(seconds=1)),
        ],
        backfill=True,
    )

    delivery = state.claim_delivery()
    assert delivery is not None
    assert delivery.video_id == "hhhhhhhhhhh"
    assert state.claim_delivery() is None
    state.close()


def test_webhook_failures_are_independent(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    config = YouTubeConfig(
        name="primary",
        channels=(YouTubeChannelConfig("@channel", (WEBHOOK_1, WEBHOOK_2)),),
        retries=0,
    )
    state.synchronize_subscriptions(subscriptions(config, channel.id))
    baseline = state.establish_baseline(channel.id, [])
    state.record_discoveries(
        channel.id,
        [discovery("iiiiiiiiiii", baseline + timedelta(seconds=1))],
        backfill=False,
    )

    first = state.claim_delivery()
    assert first is not None and first.id is not None
    state.complete_delivery(first.id, "message-id")
    second = state.claim_delivery()
    assert second is not None and second.id is not None
    assert state.fail_delivery(second.id, "failure") is False

    with Session(state.engine) as session:
        statuses = {
            item.status for item in session.exec(select(NotificationDelivery)).all()
        }
        assert statuses == {"sent", "failed"}
    state.close()


def test_overlapping_scans_deduplicate_without_aborting(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle=None,
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@channel", channel)
    state.synchronize_subscriptions(subscriptions(configured(WEBHOOK_1), channel.id))
    baseline = state.establish_baseline(channel.id, [])
    video = discovery("jjjjjjjjjjj", baseline + timedelta(seconds=1))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: state.record_discoveries(channel.id, [video], backfill=False),
                range(2),
            )
        )

    assert sorted(results) == [0, 1]
    with Session(state.engine) as session:
        assert len(session.exec(select(NotificationDelivery)).all()) == 1
    state.close()
