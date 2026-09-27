from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlmodel import Session, select

from core.state import ArchiveProgress, Channel, Report, StateStore, Subscription, Video


def add_source_data(state: StateStore, start: datetime, end: datetime) -> None:
    with Session(state.engine) as session:
        session.add(
            Channel(
                id="UCaaaaaaaaaaaaaaaaaaaaaa",
                title="Channel",
                canonical_url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
            )
        )
        session.add(
            Subscription(
                instance_name="primary",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                webhook_key="key",
                webhook_url="https://discord.com/api/webhooks/1/token",
                retries=3,
                retry_delay=5,
            )
        )
        session.add(
            Subscription(
                instance_name="secondary",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                webhook_key="other-key",
                webhook_url="https://discord.com/api/webhooks/2/token",
                retries=3,
                retry_delay=5,
            )
        )
        videos = (
            Video(
                id="aaaaaaaaaaa",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                content_type="video",
                webpage_url="https://youtu.be/aaaaaaaaaaa",
                discovered_at=start,
            ),
            Video(
                id="bbbbbbbbbbb",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                content_type="short",
                webpage_url="https://youtu.be/bbbbbbbbbbb",
                discovered_at=end - timedelta(seconds=1),
            ),
            Video(
                id="ccccccccccc",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                content_type="livestream",
                webpage_url="https://youtu.be/ccccccccccc",
                discovered_at=end,
                replay_ready=False,
            ),
            Video(
                id="eeeeeeeeeee",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                content_type="unknown",
                webpage_url="https://youtu.be/eeeeeeeeeee",
                discovered_at=start,
            ),
            Video(
                id="fffffffffff",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                webpage_url="https://youtu.be/fffffffffff",
                discovered_at=end,
            ),
            Video(
                id="ggggggggggg",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                webpage_url="https://youtu.be/ggggggggggg",
                discovered_at=end,
            ),
        )
        session.add_all(videos)
        session.commit()
        session.add(
            ArchiveProgress(
                video_id="aaaaaaaaaaa",
                status="complete",
                media_size=1024,
                media_verified_at=start,
            )
        )
        session.add(ArchiveProgress(video_id="bbbbbbbbbbb", status="operator_action"))
        session.add(ArchiveProgress(video_id="fffffffffff", status="unavailable"))
        session.add(ArchiveProgress(video_id="ggggggggggg", status="metadata"))
        session.commit()


def test_report_snapshot_uses_half_open_windows_and_freezes_metrics(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    add_source_data(state, start, end)
    checkpoint, initialized = state.initialize_report_checkpoint(start, 86_400)
    assert initialized is True
    assert checkpoint.last_window_end.replace(tzinfo=UTC) == start
    _, initialized = state.initialize_report_checkpoint(start, 86_400)
    assert initialized is False

    first = state.create_report_snapshot(start, end, end + timedelta(minutes=5))

    assert (
        first.discovered_videos,
        first.discovered_shorts,
        first.discovered_livestreams,
    ) == (1, 1, 0)
    assert first.lifetime_discovered_videos == 1
    assert first.lifetime_discovered_shorts == 1
    assert first.lifetime_discovered_livestreams == 0
    assert first.media_archived == 1
    assert first.media_bytes == 1024
    assert first.active_channels == 1
    assert first.unfinished_workflows == 1
    assert first.awaiting_replay == 1
    assert first.unavailable == 1
    assert first.operator_action == 1

    with Session(state.engine) as session:
        session.add(
            Video(
                id="ddddddddddd",
                channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
                webpage_url="https://youtu.be/ddddddddddd",
                discovered_at=start + timedelta(hours=1),
            )
        )
        session.commit()

    repeated = state.create_report_snapshot(start, end, end + timedelta(hours=1))
    assert repeated.id == first.id
    assert repeated.discovered_videos == 1
    assert repeated.generated_at.replace(tzinfo=UTC) == end + timedelta(minutes=5)
    checkpoint = state.report_checkpoint()
    assert checkpoint is not None
    assert checkpoint.last_window_end.replace(tzinfo=UTC) == end
    state.close()


def test_report_claim_retry_completion_and_restart_recovery(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    state.initialize_report_checkpoint(start, 86_400)
    created = state.create_report_snapshot(start, end, end)

    claimed = state.claim_report(end)
    assert claimed is not None and claimed.id == created.id
    assert claimed.status == "sending"
    assert claimed.attempts == 1
    state.reset_interrupted_claims()
    claimed = state.claim_report(end)
    assert claimed is not None and claimed.id is not None
    assert claimed.attempts == 2
    assert state.fail_report(claimed.id, "token secret") == 4

    with Session(state.engine) as session:
        saved = session.get(Report, claimed.id)
        assert saved is not None
        saved.retry_at = end
        session.add(saved)
        session.commit()

    claimed = state.claim_report(end)
    assert claimed is not None and claimed.id is not None
    state.complete_report(claimed.id, "sent", "message-id")
    with Session(state.engine) as session:
        saved = session.exec(select(Report)).one()
        assert saved.status == "sent"
        assert saved.message_id == "message-id"
        assert saved.last_error is None
        saved.attempts = 20
        session.add(saved)
        session.commit()
    assert state.fail_report(claimed.id, "retry") == 3_600
    state.close()


def test_report_checkpoint_realigns_after_interval_change(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    first_end = datetime(2026, 1, 1, tzinfo=UTC)
    state.initialize_report_checkpoint(first_end, 86_400)

    checkpoint, initialized = state.initialize_report_checkpoint(
        first_end + timedelta(hours=1), 3_600
    )

    assert initialized is True
    assert checkpoint.interval_seconds == 3_600
    assert checkpoint.last_window_end.replace(tzinfo=UTC) == first_end + timedelta(
        hours=1
    )
    state.close()


def test_report_state_rejects_invalid_transitions(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "state.sqlite")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    assert state.report_checkpoint() is None
    with pytest.raises(RuntimeError, match="checkpoint"):
        state.create_report_snapshot(start, end, end)
    with pytest.raises(ValueError, match="invalid completed"):
        state.complete_report(1, "pending", None)
    with pytest.raises(ValueError, match="message ID"):
        state.complete_report(1, "sent", None)
    with pytest.raises(LookupError, match="Unknown report"):
        state.complete_report(1, "logged", None)
    with pytest.raises(LookupError, match="Unknown report"):
        state.fail_report(1, "error")
    state.close()
