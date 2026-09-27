"""SQLite models and short transactional workflow operations."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlsplit

from loguru import logger
from sqlalchemy import event, func, inspect, text
from sqlmodel import (
    Field,
    Session,
    SQLModel,
    UniqueConstraint,
    col,
    create_engine,
    select,
)

from core.config import ContentType, YouTubeChannelConfig, YouTubeConfig


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(UTC)


class Channel(SQLModel, table=True):
    """Represent one canonical YouTube channel."""

    id: str = Field(primary_key=True)
    title: str
    handle: str | None = None
    canonical_url: str
    baseline_at: datetime | None = Field(default=None, index=True)
    backfill_complete: bool = False
    last_polled_at: datetime | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class ChannelReference(SQLModel, table=True):
    """Map a configured reference to its canonical channel."""

    reference: str = Field(primary_key=True)
    channel_id: str = Field(foreign_key="channel.id", index=True)


class Subscription(SQLModel, table=True):
    """Associate one named instance with a channel/webhook pair."""

    __table_args__ = (
        UniqueConstraint(
            "instance_name", "channel_id", "webhook_key", name="uq_subscription"
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    instance_name: str = Field(index=True)
    channel_id: str = Field(foreign_key="channel.id", index=True)
    webhook_key: str = Field(index=True)
    webhook_url: str
    retries: int
    retry_delay: float
    videos: bool = True
    shorts: bool = True
    livestreams: bool = True
    active: bool = Field(default=True, index=True)


class Video(SQLModel, table=True):
    """Represent a discovered YouTube video."""

    id: str = Field(primary_key=True)
    channel_id: str = Field(foreign_key="channel.id", index=True)
    title: str | None = None
    published_at: datetime | None = Field(default=None, index=True)
    discovered_at: datetime = Field(default_factory=utcnow, index=True)
    live_status: str | None = None
    content_type: str = Field(default="video", index=True)
    replay_ready: bool = Field(default=True, index=True)
    notification_candidate: bool = Field(default=False, index=True)
    notification_eligible: bool = Field(default=False, index=True)
    notification_silent: bool = False
    notification_ambiguous: bool = False
    webpage_url: str


class NotificationDelivery(SQLModel, table=True):
    """Track delivery of one video to one Discord webhook."""

    __table_args__ = (
        UniqueConstraint("video_id", "webhook_key", name="uq_video_webhook_delivery"),
    )

    id: int | None = Field(default=None, primary_key=True)
    video_id: str = Field(foreign_key="video.id", index=True)
    webhook_key: str = Field(index=True)
    webhook_url: str
    status: str = Field(default="pending", index=True)
    attempts: int = 0
    max_attempts: int
    retry_delay: float
    retry_at: datetime | None = Field(default=None, index=True)
    message_id: str | None = None
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    completed_at: datetime | None = None


class ArchiveProgress(SQLModel, table=True):
    """Track preservation state independently from Discord delivery."""

    video_id: str = Field(foreign_key="video.id", primary_key=True)
    status: str = Field(default="metadata", index=True)
    attempts: int = 0
    retry_at: datetime | None = Field(default=None, index=True)
    claimed_at: datetime | None = Field(default=None, index=True)
    working_directory: str | None = None
    media_path: str | None = None
    media_name: str | None = None
    media_size: int | None = None
    media_md5: str | None = None
    media_sha1: str | None = None
    frozen_metadata: str | None = None
    ia_identifier: str | None = Field(default=None, index=True)
    operation_id: str | None = Field(default=None, index=True)
    upload_started_at: datetime | None = None
    media_verified_at: datetime | None = None
    recovery_result: str | None = None
    failure_phase: str | None = None
    wayback_job_id: str | None = None
    webpage_status: str | None = None
    ziggy_receipt: str | None = None
    cleaned_at: datetime | None = None
    last_error: str | None = None
    updated_at: datetime = Field(default_factory=utcnow)


class ReportCheckpoint(SQLModel, table=True):
    """Store the end of the last generated fixed reporting window."""

    id: int = Field(default=1, primary_key=True)
    last_window_end: datetime
    interval_seconds: float
    initialized_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class Report(SQLModel, table=True):
    """Store one immutable metric snapshot and its delivery state."""

    __table_args__ = (
        UniqueConstraint("window_start", "window_end", name="uq_report_window"),
    )

    id: int | None = Field(default=None, primary_key=True)
    window_start: datetime = Field(index=True)
    window_end: datetime = Field(index=True)
    generated_at: datetime
    discovered_videos: int = 0
    discovered_shorts: int = 0
    discovered_livestreams: int = 0
    lifetime_discovered_videos: int = 0
    lifetime_discovered_shorts: int = 0
    lifetime_discovered_livestreams: int = 0
    media_archived: int = 0
    lifetime_media_archived: int = 0
    media_bytes: int = 0
    lifetime_media_bytes: int = 0
    active_channels: int = 0
    unfinished_workflows: int = 0
    awaiting_replay: int = 0
    unavailable: int = 0
    operator_action: int = 0
    status: str = Field(default="pending", index=True)
    attempts: int = 0
    retry_at: datetime | None = Field(default=None, index=True)
    message_id: str | None = None
    last_error: str | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ResolvedChannel:
    """Contain canonical channel information returned by yt-dlp."""

    id: str
    title: str
    handle: str | None
    url: str


@dataclass(frozen=True, slots=True)
class DiscoveredVideo:
    """Contain normalized playlist metadata for one video."""

    id: str
    title: str | None
    published_at: datetime | None
    live_status: str | None
    replay_ready: bool
    published_precise: bool = True
    content_type: ContentType = "video"

    @property
    def webpage_url(self) -> str:
        """Return the stable short YouTube URL."""
        return f"https://youtu.be/{self.id}"


@dataclass(frozen=True, slots=True)
class ArchiveWork:
    """Contain detached records needed by one archive worker claim."""

    progress: ArchiveProgress
    video: Video
    channel: Channel


class StateStore:
    """Own Saga's SQLite engine and transactional state transitions."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{path}", connect_args={"check_same_thread": False}
        )
        self._write_lock = Lock()
        event.listen(self.engine, "connect", _configure_sqlite)
        SQLModel.metadata.create_all(self.engine)
        self._migrate_content_type_columns()
        logger.debug("Initialized SQLite state at {}", self.path)

    def close(self) -> None:
        """Dispose database connections."""
        self.engine.dispose()
        logger.debug("Closed SQLite state at {}", self.path)

    def register_channel(
        self, reference: str, resolved: ResolvedChannel
    ) -> tuple[Channel, bool]:
        """Upsert canonical metadata and return whether onboarding is required."""
        now = utcnow()
        with self._write_lock, Session(self.engine) as session:
            channel = session.get(Channel, resolved.id)
            if channel is None:
                logger.info(
                    "Registering new YouTube channel {} ({})",
                    resolved.title,
                    resolved.id,
                )
                channel = Channel(
                    id=resolved.id,
                    title=resolved.title,
                    handle=resolved.handle,
                    canonical_url=resolved.url,
                    created_at=now,
                    updated_at=now,
                )
                session.add(channel)
            else:
                logger.trace("Refreshing channel metadata for {}", resolved.id)
                channel.title = resolved.title
                channel.handle = resolved.handle
                channel.canonical_url = resolved.url
                channel.updated_at = now
                session.add(channel)

            mapping = session.get(ChannelReference, reference)
            if mapping is None:
                session.add(
                    ChannelReference(reference=reference, channel_id=resolved.id)
                )
            elif mapping.channel_id != resolved.id:
                mapping.channel_id = resolved.id
                session.add(mapping)

            session.commit()
            session.refresh(channel)
            return channel, channel.baseline_at is None

    def synchronize_subscriptions(
        self,
        configurations: Sequence[
            tuple[YouTubeConfig, Sequence[tuple[str, YouTubeChannelConfig]]]
        ],
    ) -> None:
        """Activate configured subscriptions without changing historic deliveries."""
        desired: dict[
            tuple[str, str, str], tuple[str, int, float, bool, bool, bool]
        ] = {}
        for config, routes in configurations:
            for channel_id, channel in routes:
                for webhook_url in channel.discord_webhook_urls:
                    key = webhook_key(webhook_url)
                    identity = (config.name, channel_id, key)
                    previous = desired.get(identity)
                    desired[identity] = (
                        webhook_url,
                        config.retries,
                        config.retry_delay,
                        channel.videos or previous is not None and previous[3],
                        channel.shorts or previous is not None and previous[4],
                        channel.livestreams or previous is not None and previous[5],
                    )

        with self._write_lock, Session(self.engine) as session:
            subscriptions = session.exec(select(Subscription)).all()
            existing = {
                (item.instance_name, item.channel_id, item.webhook_key): item
                for item in subscriptions
            }
            for item in subscriptions:
                item.active = False
                session.add(item)

            for identity, values in desired.items():
                webhook_url, retries, retry_delay, videos, shorts, livestreams = values
                item = existing.get(identity)
                if item is None:
                    item = Subscription(
                        instance_name=identity[0],
                        channel_id=identity[1],
                        webhook_key=identity[2],
                        webhook_url=webhook_url,
                        retries=retries,
                        retry_delay=retry_delay,
                        videos=videos,
                        shorts=shorts,
                        livestreams=livestreams,
                    )
                else:
                    item.webhook_url = webhook_url
                    item.retries = retries
                    item.retry_delay = retry_delay
                    item.videos = videos
                    item.shorts = shorts
                    item.livestreams = livestreams
                    item.active = True
                session.add(item)
            active_urls: dict[str, str] = {}
            for identity, values in desired.items():
                previous_url = active_urls.setdefault(identity[2], values[0])
                if previous_url != values[0]:
                    raise ValueError(
                        "Conflicting tokens were configured for one Discord webhook ID"
                    )
            pending_deliveries = session.exec(
                select(NotificationDelivery).where(
                    col(NotificationDelivery.status).in_(("pending", "sending"))
                )
            ).all()
            for delivery in pending_deliveries:
                current_url = active_urls.get(delivery.webhook_key)
                if current_url is not None:
                    delivery.webhook_url = current_url
                    session.add(delivery)
            session.commit()
            logger.debug("Synchronized {} active subscription route(s)", len(desired))

    def establish_baseline(
        self, channel_id: str, videos: Iterable[DiscoveredVideo]
    ) -> datetime:
        """Record an initial snapshot and baseline in one transaction."""
        now = utcnow()
        with self._write_lock, Session(self.engine) as session:
            channel = session.get(Channel, channel_id)
            if channel is None:
                raise LookupError(f"Unknown channel {channel_id}")
            if channel.baseline_at is not None:
                return channel.baseline_at

            for video in videos:
                self._upsert_video(
                    session,
                    channel,
                    video,
                    notify=False,
                    silent=True,
                    ambiguous=False,
                    now=now,
                )
            channel.baseline_at = now
            channel.last_polled_at = now
            channel.updated_at = now
            session.add(channel)
            session.commit()
            logger.debug("Stored notification baseline for channel {}", channel_id)
            return now

    def record_discoveries(
        self, channel_id: str, videos: Iterable[DiscoveredVideo], *, backfill: bool
    ) -> int:
        """Persist discoveries and enqueue eligible independent workflows."""
        now = utcnow()
        added = 0
        with self._write_lock, Session(self.engine) as session:
            channel = session.get(Channel, channel_id)
            if channel is None or channel.baseline_at is None:
                raise LookupError(f"Channel {channel_id} has no notification baseline")

            for video in videos:
                existing_video = session.get(Video, video.id)
                existed = existing_video is not None
                baseline = _as_utc(channel.baseline_at)
                ambiguous = video.published_at is None or (
                    video.published_at is not None
                    and not video.published_precise
                    and _as_utc(video.published_at).date() == baseline.date()
                )
                if video.published_at is not None and (
                    video.published_precise
                    or _as_utc(video.published_at).date() != baseline.date()
                ):
                    should_notify = _as_utc(video.published_at) > baseline
                    silent = not should_notify
                elif not backfill and video.published_at is None:
                    should_notify = video.live_status in {"is_live", "is_upcoming"}
                    silent = False
                else:
                    should_notify = False
                    silent = False
                self._upsert_video(
                    session,
                    channel,
                    video,
                    notify=should_notify,
                    silent=silent,
                    ambiguous=ambiguous,
                    now=now,
                )
                added += not existed

            channel.last_polled_at = now
            channel.updated_at = now
            session.add(channel)
            session.commit()
        logger.debug(
            "Recorded channel scan for {}: {} new video(s), backfill={}",
            channel_id,
            added,
            backfill,
        )
        return added

    def mark_backfill_complete(self, channel_id: str) -> None:
        """Record completion of a full channel enumeration."""
        with self._write_lock, Session(self.engine) as session:
            channel = session.get(Channel, channel_id)
            if channel is None:
                raise LookupError(f"Unknown channel {channel_id}")
            channel.backfill_complete = True
            channel.updated_at = utcnow()
            session.add(channel)
            session.commit()
        logger.debug("Marked history backfill complete for channel {}", channel_id)

    def notification_baseline(self, channel_id: str) -> datetime:
        """Return one channel's established notification boundary in UTC."""
        with Session(self.engine) as session:
            channel = session.get(Channel, channel_id)
            if channel is None or channel.baseline_at is None:
                raise LookupError(f"Channel {channel_id} has no notification baseline")
            return _as_utc(channel.baseline_at)

    def publication_detail_ids(self, video_ids: Iterable[str]) -> set[str]:
        """Return unseen or undecided videos that need an exact publication time."""
        identifiers = tuple(dict.fromkeys(video_ids))
        if not identifiers:
            return set()
        with Session(self.engine) as session:
            known = session.exec(
                select(Video).where(col(Video.id).in_(identifiers))
            ).all()
            known_ids = {video.id for video in known}
            ambiguous = {
                video.id
                for video in known
                if video.notification_ambiguous
                and not video.notification_silent
                and not video.notification_eligible
            }
            return set(identifiers) - known_ids | ambiguous

    def reset_interrupted_claims(self) -> None:
        """Return work interrupted by a prior process to safe persisted stages."""
        with self._write_lock, Session(self.engine) as session:
            deliveries = session.exec(
                select(NotificationDelivery).where(
                    NotificationDelivery.status == "sending"
                )
            ).all()
            for delivery in deliveries:
                delivery.status = "pending"
                delivery.retry_at = None
                session.add(delivery)
            archive_rows = session.exec(
                select(ArchiveProgress).where(
                    col(ArchiveProgress.claimed_at).is_not(None)
                )
            ).all()
            for progress in archive_rows:
                if progress.status == "uploading":
                    progress.status = "reconcile_upload"
                elif progress.status in {"wayback_submitting", "ziggy_submitting"}:
                    progress.status = "operator_action"
                    progress.failure_phase = "webpage_submission"
                    progress.last_error = (
                        "process stopped while webpage submission outcome was uncertain"
                    )
                progress.claimed_at = None
                progress.retry_at = None
                progress.updated_at = utcnow()
                session.add(progress)
            reports = session.exec(
                select(Report).where(Report.status == "sending")
            ).all()
            for report in reports:
                report.status = "pending"
                report.retry_at = None
                session.add(report)
            session.commit()
        interrupted = len(deliveries) + len(archive_rows) + len(reports)
        if interrupted:
            logger.warning(
                "Recovered {} interrupted workflow claim(s): {} delivery, {} archive, "
                "{} report",
                interrupted,
                len(deliveries),
                len(archive_rows),
                len(reports),
            )
        else:
            logger.debug("No interrupted workflow claims required recovery")

    def claim_archive(
        self, *, prefer_recent: bool, now: datetime | None = None
    ) -> ArchiveWork | None:
        """Claim one due archive row, optionally favoring notification candidates."""
        current = now or utcnow()
        terminal = ("complete", "unavailable", "operator_action")
        with self._write_lock, Session(self.engine) as session:
            statement = (
                select(ArchiveProgress, Video, Channel)
                .join(Video, col(ArchiveProgress.video_id) == col(Video.id))
                .join(Channel, col(Video.channel_id) == col(Channel.id))
                .where(
                    col(ArchiveProgress.status).not_in(terminal),
                    col(ArchiveProgress.claimed_at).is_(None),
                    col(ArchiveProgress.retry_at).is_(None)
                    | (col(ArchiveProgress.retry_at) <= current),
                )
            )
            if prefer_recent:
                statement = statement.where(col(Video.notification_candidate).is_(True))
            statement = statement.order_by(
                col(ArchiveProgress.updated_at), col(ArchiveProgress.video_id)
            )
            row = session.exec(statement).first()
            if row is None and prefer_recent:
                row = session.exec(
                    select(ArchiveProgress, Video, Channel)
                    .join(Video, col(ArchiveProgress.video_id) == col(Video.id))
                    .join(Channel, col(Video.channel_id) == col(Channel.id))
                    .where(
                        col(ArchiveProgress.status).not_in(terminal),
                        col(ArchiveProgress.claimed_at).is_(None),
                        col(ArchiveProgress.retry_at).is_(None)
                        | (col(ArchiveProgress.retry_at) <= current),
                    )
                    .order_by(
                        col(ArchiveProgress.updated_at), col(ArchiveProgress.video_id)
                    )
                ).first()
            if row is None:
                return None

            progress, video, channel = row
            progress.claimed_at = current
            progress.attempts += 1
            progress.updated_at = current
            session.add(progress)
            session.commit()
            for item in (progress, video, channel):
                session.refresh(item)
            for item in (progress, video, channel):
                session.expunge(item)
            logger.trace(
                "Claimed archive row for video {} at stage {}",
                video.id,
                progress.status,
            )
            return ArchiveWork(progress=progress, video=video, channel=channel)

    def update_archive(
        self,
        video_id: str,
        *,
        status: str | None = None,
        release: bool = False,
        retry_after: float | None = None,
        **values: Any,
    ) -> None:
        """Persist an archive transition without holding a network transaction."""
        allowed = {
            "working_directory",
            "media_path",
            "media_name",
            "media_size",
            "media_md5",
            "media_sha1",
            "frozen_metadata",
            "ia_identifier",
            "operation_id",
            "upload_started_at",
            "media_verified_at",
            "recovery_result",
            "failure_phase",
            "wayback_job_id",
            "webpage_status",
            "ziggy_receipt",
            "cleaned_at",
            "last_error",
        }
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"Unknown archive fields: {sorted(unknown)}")
        if retry_after is not None and retry_after < 0:
            raise ValueError("retry_after must be non-negative")

        with self._write_lock, Session(self.engine) as session:
            progress = session.get(ArchiveProgress, video_id)
            if progress is None:
                raise LookupError(f"Unknown archive progress for {video_id}")
            previous_status = progress.status
            if status is not None:
                progress.status = status
            for name, value in values.items():
                setattr(progress, name, value)
            if release:
                progress.claimed_at = None
                progress.retry_at = (
                    utcnow() + timedelta(seconds=retry_after)
                    if retry_after is not None
                    else None
                )
            progress.updated_at = utcnow()
            session.add(progress)
            session.commit()
        logger.trace(
            "Persisted archive transition for {}: {} -> {}, release={}",
            video_id,
            previous_status,
            status or previous_status,
            release,
        )

    def claim_delivery(
        self, now: datetime | None = None
    ) -> NotificationDelivery | None:
        """Claim the oldest due Discord delivery for the single delivery worker."""
        current = now or utcnow()
        with self._write_lock, Session(self.engine) as session:
            delivery = session.exec(
                select(NotificationDelivery)
                .where(
                    NotificationDelivery.status == "pending",
                    col(NotificationDelivery.retry_at).is_(None)
                    | (col(NotificationDelivery.retry_at) <= current),
                )
                .order_by(
                    col(NotificationDelivery.created_at), col(NotificationDelivery.id)
                )
            ).first()
            if delivery is None:
                return None
            delivery.status = "sending"
            delivery.attempts += 1
            session.add(delivery)
            session.commit()
            session.refresh(delivery)
            session.expunge(delivery)
            logger.trace(
                "Claimed delivery {} for video {}", delivery.id, delivery.video_id
            )
            return delivery

    def complete_delivery(self, delivery_id: int, message_id: str) -> None:
        """Persist a waited Discord response as successful."""
        with self._write_lock, Session(self.engine) as session:
            delivery = session.get(NotificationDelivery, delivery_id)
            if delivery is None:
                raise LookupError(f"Unknown notification delivery {delivery_id}")
            delivery.status = "sent"
            delivery.message_id = message_id
            delivery.last_error = None
            delivery.retry_at = None
            delivery.completed_at = utcnow()
            session.add(delivery)
            session.commit()
        logger.debug("Completed delivery {} with message {}", delivery_id, message_id)

    def fail_delivery(self, delivery_id: int, error: str) -> bool:
        """Persist a failure and return whether another attempt was scheduled."""
        with self._write_lock, Session(self.engine) as session:
            delivery = session.get(NotificationDelivery, delivery_id)
            if delivery is None:
                raise LookupError(f"Unknown notification delivery {delivery_id}")
            delivery.last_error = error
            if delivery.attempts >= delivery.max_attempts:
                delivery.status = "failed"
                delivery.retry_at = None
                retrying = False
            else:
                delivery.status = "pending"
                delivery.retry_at = utcnow() + timedelta(seconds=delivery.retry_delay)
                retrying = True
            session.add(delivery)
            session.commit()
            logger.debug(
                "Recorded delivery {} failure; retrying={}", delivery_id, retrying
            )
            return retrying

    def initialize_report_checkpoint(
        self, initial_end: datetime, interval_seconds: float
    ) -> tuple[ReportCheckpoint, bool]:
        """Create or realign the singleton report checkpoint."""
        current = utcnow()
        with self._write_lock, Session(self.engine) as session:
            checkpoint = session.get(ReportCheckpoint, 1)
            initialized = checkpoint is None
            if checkpoint is None:
                checkpoint = ReportCheckpoint(
                    last_window_end=initial_end,
                    interval_seconds=interval_seconds,
                    initialized_at=current,
                    updated_at=current,
                )
            elif checkpoint.interval_seconds != interval_seconds:
                checkpoint.last_window_end = initial_end
                checkpoint.interval_seconds = interval_seconds
                checkpoint.updated_at = current
                initialized = True
            session.add(checkpoint)
            session.commit()
            session.refresh(checkpoint)
            session.expunge(checkpoint)
            return checkpoint, initialized

    def report_checkpoint(self) -> ReportCheckpoint | None:
        """Return the detached report scheduling checkpoint."""
        with Session(self.engine) as session:
            checkpoint = session.get(ReportCheckpoint, 1)
            if checkpoint is not None:
                session.expunge(checkpoint)
            return checkpoint

    def create_report_snapshot(
        self, window_start: datetime, window_end: datetime, generated_at: datetime
    ) -> Report:
        """Aggregate and commit one immutable report before any delivery attempt."""
        with self._write_lock, Session(self.engine) as session:
            existing = session.exec(
                select(Report).where(
                    Report.window_start == window_start, Report.window_end == window_end
                )
            ).first()
            if existing is None:
                period_discovered = self._discovery_counts(
                    session, start=window_start, end=window_end
                )
                lifetime_discovered = self._discovery_counts(
                    session, start=None, end=window_end
                )
                media_period = session.exec(
                    select(
                        func.count(col(ArchiveProgress.video_id)),
                        func.coalesce(func.sum(col(ArchiveProgress.media_size)), 0),
                    ).where(
                        col(ArchiveProgress.media_verified_at).is_not(None),
                        col(ArchiveProgress.media_verified_at) >= window_start,
                        col(ArchiveProgress.media_verified_at) < window_end,
                    )
                ).one()
                media_lifetime = session.exec(
                    select(
                        func.count(col(ArchiveProgress.video_id)),
                        func.coalesce(func.sum(col(ArchiveProgress.media_size)), 0),
                    ).where(
                        col(ArchiveProgress.media_verified_at).is_not(None),
                        col(ArchiveProgress.media_verified_at) < window_end,
                    )
                ).one()
                active_channels = session.exec(
                    select(func.count(func.distinct(Subscription.channel_id))).where(
                        col(Subscription.active).is_(True)
                    )
                ).one()
                unfinished = session.exec(
                    select(func.count(col(ArchiveProgress.video_id))).where(
                        col(ArchiveProgress.status).not_in(
                            ("complete", "unavailable", "operator_action")
                        )
                    )
                ).one()
                awaiting_replay = session.exec(
                    select(func.count(col(Video.id))).where(
                        col(Video.replay_ready).is_(False)
                    )
                ).one()
                unavailable = session.exec(
                    select(func.count(col(ArchiveProgress.video_id))).where(
                        ArchiveProgress.status == "unavailable"
                    )
                ).one()
                operator_action = session.exec(
                    select(func.count(col(ArchiveProgress.video_id))).where(
                        ArchiveProgress.status == "operator_action"
                    )
                ).one()
                existing = Report(
                    window_start=window_start,
                    window_end=window_end,
                    generated_at=generated_at,
                    discovered_videos=period_discovered["video"],
                    discovered_shorts=period_discovered["short"],
                    discovered_livestreams=period_discovered["livestream"],
                    lifetime_discovered_videos=lifetime_discovered["video"],
                    lifetime_discovered_shorts=lifetime_discovered["short"],
                    lifetime_discovered_livestreams=lifetime_discovered["livestream"],
                    media_archived=int(media_period[0]),
                    lifetime_media_archived=int(media_lifetime[0]),
                    media_bytes=int(media_period[1] or 0),
                    lifetime_media_bytes=int(media_lifetime[1] or 0),
                    active_channels=int(active_channels),
                    unfinished_workflows=int(unfinished),
                    awaiting_replay=int(awaiting_replay),
                    unavailable=int(unavailable),
                    operator_action=int(operator_action),
                )
                session.add(existing)
            checkpoint = session.get(ReportCheckpoint, 1)
            if checkpoint is None:
                raise RuntimeError("report checkpoint was not initialized")
            if _as_utc(checkpoint.last_window_end) < _as_utc(window_end):
                checkpoint.last_window_end = window_end
                checkpoint.updated_at = generated_at
                session.add(checkpoint)
            session.commit()
            session.refresh(existing)
            session.expunge(existing)
            return existing

    def claim_report(self, now: datetime | None = None) -> Report | None:
        """Claim the oldest report whose delivery is due."""
        current = now or utcnow()
        with self._write_lock, Session(self.engine) as session:
            report = session.exec(
                select(Report)
                .where(
                    Report.status == "pending",
                    col(Report.retry_at).is_(None) | (col(Report.retry_at) <= current),
                )
                .order_by(col(Report.window_start), col(Report.id))
            ).first()
            if report is None:
                return None
            report.status = "sending"
            report.attempts += 1
            session.add(report)
            session.commit()
            session.refresh(report)
            session.expunge(report)
            return report

    def complete_report(
        self, report_id: int, status: str, message_id: str | None
    ) -> None:
        """Persist a delivered, logged, or suppressed report outcome."""
        if status not in {"sent", "logged", "suppressed"}:
            raise ValueError("invalid completed report status")
        if status == "sent" and not message_id:
            raise ValueError("sent reports require a Discord message ID")
        with self._write_lock, Session(self.engine) as session:
            report = session.get(Report, report_id)
            if report is None:
                raise LookupError(f"Unknown report {report_id}")
            report.status = status
            report.message_id = message_id
            report.last_error = None
            report.retry_at = None
            report.completed_at = utcnow()
            session.add(report)
            session.commit()

    def fail_report(self, report_id: int, error: str) -> float:
        """Persist a report failure with exponential backoff capped at one hour."""
        with self._write_lock, Session(self.engine) as session:
            report = session.get(Report, report_id)
            if report is None:
                raise LookupError(f"Unknown report {report_id}")
            delay = float(min(3_600, 2 ** min(report.attempts, 12)))
            report.status = "pending"
            report.retry_at = utcnow() + timedelta(seconds=delay)
            report.last_error = error[:2000]
            session.add(report)
            session.commit()
            return delay

    @staticmethod
    def _discovery_counts(
        session: Session, *, start: datetime | None, end: datetime
    ) -> dict[str, int]:
        statement = select(Video.content_type, func.count(col(Video.id))).where(
            Video.discovered_at < end
        )
        if start is not None:
            statement = statement.where(Video.discovered_at >= start)
        rows = session.exec(statement.group_by(Video.content_type)).all()
        counts = {"video": 0, "short": 0, "livestream": 0}
        for content_type, count in rows:
            if content_type in counts:
                counts[content_type] = int(count)
        return counts

    def _upsert_video(
        self,
        session: Session,
        channel: Channel,
        discovery: DiscoveredVideo,
        *,
        notify: bool,
        silent: bool,
        ambiguous: bool,
        now: datetime,
    ) -> None:
        video = session.get(Video, discovery.id)
        was_eligible = video.notification_eligible if video is not None else False
        if video is None:
            video = Video(
                id=discovery.id,
                channel_id=channel.id,
                title=discovery.title,
                published_at=discovery.published_at,
                live_status=discovery.live_status,
                content_type=discovery.content_type,
                replay_ready=discovery.replay_ready,
                notification_candidate=notify,
                notification_eligible=notify,
                notification_silent=silent,
                notification_ambiguous=ambiguous,
                webpage_url=discovery.webpage_url,
                discovered_at=now,
            )
        else:
            video.title = discovery.title or video.title
            video.published_at = discovery.published_at or video.published_at
            video.live_status = discovery.live_status
            video.content_type = discovery.content_type
            video.replay_ready = discovery.replay_ready
            video.notification_silent = video.notification_silent or silent
            if discovery.published_precise:
                video.notification_ambiguous = False
            else:
                video.notification_ambiguous = video.notification_ambiguous or ambiguous
            video.notification_candidate = video.notification_candidate or (
                notify and not video.notification_silent
            )
            video.notification_eligible = (
                video.notification_eligible or video.notification_candidate
            )
        session.add(video)

        subscriptions = session.exec(
            select(Subscription).where(
                Subscription.channel_id == channel.id,
                col(Subscription.active).is_(True),
            )
        ).all()
        enabled_subscriptions = [
            subscription
            for subscription in subscriptions
            if _subscription_allows(subscription, discovery.content_type)
        ]

        if (
            discovery.replay_ready
            and enabled_subscriptions
            and session.get(ArchiveProgress, discovery.id) is None
        ):
            session.add(ArchiveProgress(video_id=discovery.id, updated_at=now))
            logger.debug("Queued video {} for archival", discovery.id)

        if not video.notification_eligible or was_eligible:
            return

        policies: dict[str, Subscription] = {}
        for subscription in enabled_subscriptions:
            current = policies.get(subscription.webhook_key)
            if current is None or subscription.retries > current.retries:
                policies[subscription.webhook_key] = subscription

        for subscription in policies.values():
            existing = session.exec(
                select(NotificationDelivery).where(
                    NotificationDelivery.video_id == discovery.id,
                    NotificationDelivery.webhook_key == subscription.webhook_key,
                )
            ).first()
            if existing is None:
                session.add(
                    NotificationDelivery(
                        video_id=discovery.id,
                        webhook_key=subscription.webhook_key,
                        webhook_url=subscription.webhook_url,
                        max_attempts=subscription.retries + 1,
                        retry_delay=subscription.retry_delay,
                        created_at=now,
                    )
                )
                logger.debug("Queued Discord delivery for video {}", discovery.id)

    def _migrate_content_type_columns(self) -> None:
        """Add default-on content policy columns to databases from older releases."""
        schema = inspect(self.engine)
        subscription_columns = {
            column["name"] for column in schema.get_columns("subscription")
        }
        video_columns = {column["name"] for column in schema.get_columns("video")}
        statements = [
            f"ALTER TABLE subscription ADD COLUMN {name} BOOLEAN NOT NULL DEFAULT 1"
            for name in ("videos", "shorts", "livestreams")
            if name not in subscription_columns
        ]
        if "content_type" not in video_columns:
            statements.append(
                "ALTER TABLE video ADD COLUMN content_type VARCHAR NOT NULL "
                "DEFAULT 'video'"
            )
        if not statements:
            return
        logger.info("Applying {} SQLite schema migration(s)", len(statements))
        with self.engine.begin() as connection:
            for statement in statements:
                connection.execute(text(statement))


def webhook_key(url: str) -> str:
    """Return an opaque identity that stays stable when Discord rotates a token."""
    path = urlsplit(url).path.split("/")
    webhook_id = path[3] if len(path) > 3 else url
    return sha256(webhook_id.encode()).hexdigest()


def _subscription_allows(subscription: Subscription, content_type: ContentType) -> bool:
    return {
        "video": subscription.videos,
        "short": subscription.shorts,
        "livestream": subscription.livestreams,
    }[content_type]


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _configure_sqlite(connection: Any, _: Any) -> None:
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()
