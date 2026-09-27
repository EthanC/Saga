from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlmodel import Session, select

from core import state as state_module
from core.config import YouTubeChannelConfig, YouTubeConfig
from core.state import (
    ArchiveProgress,
    Channel,
    ChannelReference,
    DiscoveredVideo,
    NotificationDelivery,
    ResolvedChannel,
    StateStore,
    Video,
    webhook_key,
)

WEBHOOK = "https://discord.com/api/webhooks/123/token"
ROTATED_WEBHOOK = "https://discord.com/api/webhooks/123/rotated"
CHANNEL = ResolvedChannel(
    id="UCaaaaaaaaaaaaaaaaaaaaaa",
    title="Channel",
    handle="@channel",
    url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
)


def configured(name: str = "primary", webhook: str = WEBHOOK) -> YouTubeConfig:
    return YouTubeConfig(
        name=name, channels=(YouTubeChannelConfig("@channel", (webhook,)),)
    )


def prepare(tmp_path: Path) -> tuple[StateStore, datetime]:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    channel_config = configured().channels[0]
    store.synchronize_subscriptions([(configured(), ((CHANNEL.id, channel_config),))])
    return store, store.establish_baseline(CHANNEL.id, [])


def discovered(video_id: str, published_at: datetime | None) -> DiscoveredVideo:
    return DiscoveredVideo(video_id, video_id, published_at, "not_live", True)


def test_channel_registration_remaps_and_lifecycle_errors(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    updated = ResolvedChannel(CHANNEL.id, "Updated", None, CHANNEL.url)
    saved, onboarding = store.register_channel("@channel", updated)
    assert saved.title == "Updated"
    assert onboarding is True

    other = ResolvedChannel(
        "UCbbbbbbbbbbbbbbbbbbbbbb",
        "Other",
        None,
        "https://www.youtube.com/channel/UCbbbbbbbbbbbbbbbbbbbbbb",
    )
    store.register_channel("other", other)
    store.register_channel("@channel", other)
    with Session(store.engine) as session:
        mapping = session.get(ChannelReference, "@channel")
        assert mapping is not None and mapping.channel_id == other.id

    baseline = store.establish_baseline(CHANNEL.id, [])
    assert store.establish_baseline(CHANNEL.id, []) == baseline
    with pytest.raises(LookupError, match="Unknown channel missing"):
        store.establish_baseline("missing", [])
    with pytest.raises(LookupError, match="no notification baseline"):
        store.record_discoveries(other.id, [], backfill=False)
    with pytest.raises(LookupError, match="no notification baseline"):
        store.notification_baseline(other.id)

    store.mark_backfill_complete(CHANNEL.id)
    with Session(store.engine) as session:
        channel = session.get(Channel, CHANNEL.id)
        assert channel is not None and channel.backfill_complete is True
    with pytest.raises(LookupError, match="Unknown channel missing"):
        store.mark_backfill_complete("missing")
    store.close()


def test_publication_details_include_unseen_and_ambiguous_videos(
    tmp_path: Path,
) -> None:
    store, baseline = prepare(tmp_path)
    unknown = discovered("uuuuuuuuuuu", None)
    assert store.record_discoveries(CHANNEL.id, [unknown], backfill=False) == 1
    assert store.claim_delivery() is None

    ambiguous = DiscoveredVideo(
        "aaaaaaaaaaa",
        "ambiguous",
        baseline.replace(hour=0, minute=0, second=0, microsecond=0),
        "not_live",
        True,
        published_precise=False,
    )
    store.record_discoveries(CHANNEL.id, [ambiguous], backfill=True)
    store.record_discoveries(CHANNEL.id, [ambiguous], backfill=True)

    assert store.publication_detail_ids([]) == set()
    assert store.publication_detail_ids(
        [unknown.id, ambiguous.id, "zzzzzzzzzzz", "zzzzzzzzzzz"]
    ) == {unknown.id, ambiguous.id, "zzzzzzzzzzz"}
    store.close()


def test_shared_webhook_policy_and_conflicting_tokens(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    first = configured("first")
    second = configured("second")
    store.synchronize_subscriptions(
        [
            (first, ((CHANNEL.id, first.channels[0]),)),
            (second, ((CHANNEL.id, second.channels[0]),)),
        ]
    )
    baseline = store.establish_baseline(CHANNEL.id, [])
    store.record_discoveries(
        CHANNEL.id,
        [discovered("bbbbbbbbbbb", baseline + timedelta(seconds=1))],
        backfill=False,
    )
    with Session(store.engine) as session:
        deliveries = session.exec(select(NotificationDelivery)).all()
        assert len(deliveries) == 1

    rotated = configured("second", ROTATED_WEBHOOK)
    with pytest.raises(ValueError, match="Conflicting tokens"):
        store.synchronize_subscriptions(
            [
                (first, ((CHANNEL.id, first.channels[0]),)),
                (rotated, ((CHANNEL.id, rotated.channels[0]),)),
            ]
        )
    store.close()


def test_existing_delivery_is_not_duplicated_when_video_becomes_eligible(
    tmp_path: Path,
) -> None:
    store, baseline = prepare(tmp_path)
    video = discovered("ccccccccccc", None)
    store.record_discoveries(CHANNEL.id, [video], backfill=True)
    with Session(store.engine) as session:
        session.add(
            NotificationDelivery(
                video_id=video.id,
                webhook_key=webhook_key(WEBHOOK),
                webhook_url=WEBHOOK,
                max_attempts=4,
                retry_delay=5,
            )
        )
        session.commit()

    promoted = discovered(video.id, baseline + timedelta(seconds=1))
    store.record_discoveries(CHANNEL.id, [promoted], backfill=False)

    with Session(store.engine) as session:
        assert len(session.exec(select(NotificationDelivery)).all()) == 1
    store.close()


def test_archive_validation_interrupted_claims_and_delivery_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixed_now = datetime(2030, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(state_module, "utcnow", lambda: fixed_now)
    store, baseline = prepare(tmp_path)
    videos = [
        discovered("ddddddddddd", baseline + timedelta(seconds=1)),
        discovered("eeeeeeeeeee", baseline + timedelta(seconds=2)),
    ]
    store.record_discoveries(CHANNEL.id, videos, backfill=False)

    with pytest.raises(ValueError, match="Unknown archive fields"):
        store.update_archive("anything", unexpected=True)
    with pytest.raises(ValueError, match="retry_after must be non-negative"):
        store.update_archive("anything", retry_after=-1)
    with pytest.raises(LookupError, match="Unknown archive progress"):
        store.update_archive("missing", status="metadata")

    store.update_archive(videos[0].id, last_error="note")
    first = store.claim_archive(prefer_recent=False)
    second = store.claim_archive(prefer_recent=False)
    assert first is not None and second is not None
    store.update_archive(first.video.id, status="uploading")
    delivery = store.claim_delivery()
    assert delivery is not None and delivery.id is not None

    store.reset_interrupted_claims()

    with Session(store.engine) as session:
        first_progress = session.get(ArchiveProgress, first.video.id)
        second_progress = session.get(ArchiveProgress, second.video.id)
        saved_delivery = session.get(NotificationDelivery, delivery.id)
        assert (
            first_progress is not None and first_progress.status == "reconcile_upload"
        )
        assert second_progress is not None and second_progress.status == "metadata"
        assert first_progress.claimed_at is None and second_progress.claimed_at is None
        assert saved_delivery is not None and saved_delivery.status == "pending"

    with pytest.raises(LookupError, match="Unknown notification delivery"):
        store.complete_delivery(999_999, "message")
    with pytest.raises(LookupError, match="Unknown notification delivery"):
        store.fail_delivery(999_999, "error")
    assert store.fail_delivery(delivery.id, "temporary") is True
    with Session(store.engine) as session:
        saved_delivery = session.get(NotificationDelivery, delivery.id)
        assert saved_delivery is not None
        assert saved_delivery.retry_at == fixed_now + timedelta(seconds=5)
    store.close()


def test_archive_claim_falls_back_and_reset_marks_submission_uncertain(
    tmp_path: Path,
) -> None:
    store = StateStore(tmp_path / "state.sqlite")
    store.register_channel("@channel", CHANNEL)
    channel_config = configured().channels[0]
    store.synchronize_subscriptions([(configured(), ((CHANNEL.id, channel_config),))])
    old = discovered("fffffffffff", datetime(2020, 1, 1, tzinfo=UTC))
    store.establish_baseline(CHANNEL.id, [old])

    work = store.claim_archive(prefer_recent=True)
    assert work is not None
    store.update_archive(work.video.id, release=True)
    work = store.claim_archive(prefer_recent=True)
    assert work is not None
    store.update_archive(work.video.id, status="ziggy_submitting")
    store.reset_interrupted_claims()

    with Session(store.engine) as session:
        progress = session.get(ArchiveProgress, old.id)
        assert progress is not None
        assert progress.status == "operator_action"
        assert progress.failure_phase == "webpage_submission"
    assert store.claim_archive(prefer_recent=True) is None
    store.close()


def test_archive_claim_leaves_disk_blocked_video_at_back(tmp_path: Path) -> None:
    store, baseline = prepare(tmp_path)
    videos = [
        discovered("ddddddddddd", baseline + timedelta(seconds=1)),
        discovered("eeeeeeeeeee", baseline + timedelta(seconds=2)),
    ]
    store.record_discoveries(CHANNEL.id, videos, backfill=False)

    blocked = store.claim_archive(prefer_recent=False)
    assert blocked is not None
    store.update_archive(blocked.video.id, release=True, failure_phase="disk")
    viable_id = next(video.id for video in videos if video.id != blocked.video.id)
    with Session(store.engine) as session:
        viable_video = session.get(Video, viable_id)
        assert viable_video is not None
        viable_video.notification_candidate = False
        session.add(viable_video)
        session.commit()

    viable = store.claim_archive(prefer_recent=True)
    assert viable is not None and viable.video.id == viable_id
    store.update_archive(viable.video.id, release=True)

    next_work = store.claim_archive(prefer_recent=True)
    assert next_work is not None and next_work.video.id == viable.video.id
    store.close()
