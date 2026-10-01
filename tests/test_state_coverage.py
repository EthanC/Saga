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


def seed_archive_queue(
    store: StateStore, baseline: datetime, rows: list[tuple[str, str, datetime]]
) -> None:
    store.record_discoveries(
        CHANNEL.id,
        [
            discovered(video_id, baseline + timedelta(seconds=index + 1))
            for index, (video_id, _, _) in enumerate(rows)
        ],
        backfill=True,
    )
    with Session(store.engine) as session:
        for video_id, status, updated_at in rows:
            progress = session.get(ArchiveProgress, video_id)
            assert progress is not None
            progress.status = status
            progress.updated_at = updated_at
            session.add(progress)
        session.commit()


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


def test_archive_claim_prioritizes_upload_over_older_upstream_work(
    tmp_path: Path,
) -> None:
    store, baseline = prepare(tmp_path)
    oldest = datetime(2020, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("mmmmmmmmmmm", "metadata", oldest),
            ("ddddddddddd", "download", oldest + timedelta(days=1)),
            ("uuuuuuuuuuu", "upload", oldest + timedelta(days=2)),
        ],
    )

    work = store.claim_archive(prefer_recent=False)

    assert work is not None
    assert work.video.id == "uuuuuuuuuuu"
    store.close()


@pytest.mark.parametrize("prefer_recent", [False, True])
def test_archive_claim_prioritizes_downstream_stages(
    tmp_path: Path, prefer_recent: bool
) -> None:
    store, baseline = prepare(tmp_path)
    oldest = datetime(2020, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("mmmmmmmmmmm", "metadata", oldest),
            ("ddddddddddd", "download", oldest + timedelta(days=1)),
            ("uuuuuuuuuuu", "upload", oldest + timedelta(days=2)),
            ("wwwwwwwwwww", "webpage", oldest + timedelta(days=3)),
            ("ppppppppppp", "wayback_wait", oldest + timedelta(days=4)),
            ("rrrrrrrrrrr", "reconcile_upload", oldest + timedelta(days=5)),
            ("iiiiiiiiiii", "uploading", oldest + timedelta(days=6)),
            ("ccccccccccc", "cleanup", oldest + timedelta(days=7)),
        ],
    )

    claimed = [
        work.video.id
        for _ in range(8)
        if (work := store.claim_archive(prefer_recent=prefer_recent)) is not None
    ]

    assert claimed == ["uuuuuuuuuuu", "ddddddddddd", "mmmmmmmmmmm"]
    store.close()


@pytest.mark.parametrize("status", ["upload", "download"])
def test_archive_claim_skips_high_priority_future_retry(
    tmp_path: Path, status: str
) -> None:
    store, baseline = prepare(tmp_path)
    now = datetime(2030, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("ccccccccccc", status, datetime(2020, 1, 1, tzinfo=UTC)),
            ("mmmmmmmmmmm", "metadata", datetime(2020, 1, 2, tzinfo=UTC)),
        ],
    )
    with Session(store.engine) as session:
        deferred = session.get(ArchiveProgress, "ccccccccccc")
        assert deferred is not None
        deferred.retry_at = now + timedelta(seconds=1)
        session.add(deferred)
        session.commit()

    work = store.claim_archive(prefer_recent=False, now=now)

    assert work is not None
    assert work.video.id == "mmmmmmmmmmm"
    assert store.claim_archive(prefer_recent=False, now=now) is None
    retry = store.claim_archive(prefer_recent=False, now=now + timedelta(seconds=1))
    assert retry is not None
    assert retry.video.id == "ccccccccccc"
    store.close()


@pytest.mark.parametrize("prefer_recent", [False, True])
def test_archive_notification_preference_is_limited_to_stage_class(
    tmp_path: Path, prefer_recent: bool
) -> None:
    store, baseline = prepare(tmp_path)
    oldest = datetime(2020, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("ooooooooooo", "download", oldest),
            ("nnnnnnnnnnn", "download", oldest + timedelta(days=1)),
            ("mmmmmmmmmmm", "metadata", oldest),
            ("uuuuuuuuuuu", "upload", oldest + timedelta(days=2)),
        ],
    )
    with Session(store.engine) as session:
        for video_id in ("ooooooooooo", "nnnnnnnnnnn", "mmmmmmmmmmm", "uuuuuuuuuuu"):
            video = session.get(Video, video_id)
            assert video is not None
            video.notification_candidate = video_id in {"nnnnnnnnnnn", "mmmmmmmmmmm"}
            session.add(video)
        session.commit()

    claimed = [
        work.video.id
        for _ in range(4)
        if (work := store.claim_archive(prefer_recent=prefer_recent)) is not None
    ]

    downloads = (
        ["nnnnnnnnnnn", "ooooooooooo"]
        if prefer_recent
        else ["ooooooooooo", "nnnnnnnnnnn"]
    )
    assert claimed == ["uuuuuuuuuuu", *downloads, "mmmmmmmmmmm"]
    store.close()


@pytest.mark.parametrize("prefer_recent", [False, True])
@pytest.mark.parametrize(
    ("older_stage", "newer_stage"),
    [("upload", "upload"), ("download", "download"), ("metadata", "metadata")],
)
def test_archive_claim_keeps_oldest_first_within_stage_class(
    tmp_path: Path, prefer_recent: bool, older_stage: str, newer_stage: str
) -> None:
    store, baseline = prepare(tmp_path)
    oldest = datetime(2020, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("ooooooooooo", older_stage, oldest),
            ("nnnnnnnnnnn", newer_stage, oldest + timedelta(days=1)),
        ],
    )

    work = store.claim_archive(prefer_recent=prefer_recent)

    assert work is not None
    assert work.video.id == "ooooooooooo"
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


@pytest.mark.parametrize("prefer_recent", [False, True])
@pytest.mark.parametrize("status", ["metadata", "download"])
def test_archive_claim_leaves_disk_blocked_video_at_back(
    tmp_path: Path, prefer_recent: bool, status: str
) -> None:
    store, baseline = prepare(tmp_path)
    oldest = datetime(2020, 1, 1, tzinfo=UTC)
    seed_archive_queue(
        store,
        baseline,
        [
            ("ddddddddddd", status, oldest),
            ("eeeeeeeeeee", status, oldest + timedelta(days=1)),
        ],
    )
    with Session(store.engine) as session:
        blocked = session.get(ArchiveProgress, "ddddddddddd")
        assert blocked is not None
        blocked.failure_phase = "disk"
        blocked.retry_at = oldest
        session.add(blocked)
        viable_video = session.get(Video, "eeeeeeeeeee")
        assert viable_video is not None
        viable_video.notification_candidate = False
        session.add(viable_video)
        session.commit()

    viable = store.claim_archive(prefer_recent=prefer_recent)
    assert viable is not None and viable.video.id == "eeeeeeeeeee"
    store.update_archive(viable.video.id, release=True)

    next_work = store.claim_archive(prefer_recent=prefer_recent)
    assert next_work is not None and next_work.video.id == viable.video.id
    deferred = store.claim_archive(prefer_recent=prefer_recent)
    assert deferred is not None and deferred.video.id == "ddddddddddd"
    store.close()
