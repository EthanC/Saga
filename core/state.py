"""SQLite models and short transactional workflow operations."""

from __future__ import annotations

import json
import random
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import uuid4

from loguru import logger
from sqlalchemy import case, event, false, func, inspect, text
from sqlmodel import (
    Field,
    Session,
    SQLModel,
    UniqueConstraint,
    col,
    create_engine,
    select,
)

from core.archive_errors import (
    DIAGNOSTIC_FIELDS,
    apply_diagnostics,
    diagnostic_fields,
    diagnostic_snapshot,
    legacy_bad_login,
)
from core.archive_incidents import incident_facts, incident_open
from core.archive_retry import (
    INCIDENT_RECOVERY_DELAY,
    INCIDENT_REMINDER_DELAY,
    MAX_PROBE_DELAY,
    MIN_WRITE_INTERVAL,
    RELEASE_WRITE_INTERVAL,
    THROTTLE_PROBATION,
    ArchiveServiceState,
    retryable_record,
)
from core.config import ContentType, YouTubeChannelConfig, YouTubeConfig
from core.logging import redact_diagnostics
from core.staging import completed_download_size, download_reservation, inventory_inputs
from core.webpage_errors import legacy_ziggy_not_sent
from core.youtube_errors import is_access_error

PUBLICATION_RETRY_BASE = 300.0
PUBLICATION_RETRY_CAP = 3600.0


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
    backfill_completed_at: datetime | None = None
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
    publication_retry_at: datetime | None = None
    publication_attempts: int = 0
    publication_failure: str | None = None
    publication_error: str | None = None
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
    download_completed_at: datetime | None = None
    ready_at: datetime | None = None
    media_uploaded_at: datetime | None = Field(default=None, index=True)
    uploaded_bytes: int | None = None
    upload_receipt: str | None = None
    upload_etag: str | None = None
    upload_origin: str | None = None
    upload_mode: str = "create"
    transfer_bytes: int = 0
    transfer_progress_at: datetime | None = None
    retry_attempts: int = 0
    reserved_bytes: int = 0
    download_estimated_bytes: int | None = None
    download_bytes: int = 0
    download_progress_at: datetime | None = None
    download_phase: str | None = None
    merge_bytes: int = 0
    maintenance_token: str | None = None
    media_verified_at: datetime | None = None
    recovery_result: str | None = None
    failure_phase: str | None = None
    youtube_access_generation: str | None = None
    wayback_job_id: str | None = None
    webpage_status: str | None = None
    ziggy_receipt: str | None = None
    cleaned_at: datetime | None = None
    last_error: str | None = None
    updated_at: datetime = Field(default_factory=utcnow)
    error_kind: str | None = None
    error_status: int | None = None
    error_phase: str | None = None
    archive_generation: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_request_id: str | None = None
    error_headers: str | None = None
    error_queue: str | None = None
    multipart_state: str | None = None


class ArchiveJob(SQLModel, table=True):
    """Claim background work independently of a video's transfer claim."""

    video_id: str = Field(foreign_key="archiveprogress.video_id", primary_key=True)
    kind: str = Field(primary_key=True)
    status: str = Field(default="pending", index=True)
    attempts: int = 0
    retry_at: datetime | None = Field(default=None, index=True)
    deadline_at: datetime | None = None
    claim_token: str | None = None
    claimed_at: datetime | None = None
    last_error: str | None = None
    updated_at: datetime = Field(default_factory=utcnow)
    error_kind: str | None = None
    error_status: int | None = None
    error_phase: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_request_id: str | None = None
    error_headers: str | None = None
    error_queue: str | None = None


class WorkerTiming(SQLModel, table=True):
    """Persist cumulative worker occupancy by job kind."""

    kind: str = Field(primary_key=True)
    seconds: float = 0.0


class PreservationAlertState(SQLModel, table=True):
    """Remember which actionable webpage outcomes have already been announced."""

    key: str = Field(primary_key=True)
    fingerprint: str | None = None
    notified_at: datetime | None = None
    outcome_count: int = 0


class SchemaMigration(SQLModel, table=True):
    """Record one-time data migrations independently of additive columns."""

    version: int = Field(primary_key=True)
    applied_at: datetime = Field(default_factory=utcnow)


class ReportCheckpoint(SQLModel, table=True):
    """Store the end of the last generated scheduled reporting window."""

    id: int = Field(default=1, primary_key=True)
    last_window_end: datetime
    schedule: str
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
    semantics_version: int = 2
    ready_uploads: int = 0
    pending_downloads: int = 0
    active_transfers: int = 0
    failed_transfers: int = 0
    uncertain_transfers: int = 0
    validation_pending: int = 0
    validation_passed: int = 0
    validation_failed: int = 0
    validation_expired: int = 0
    webpage_pending: int = 0
    webpage_completed: int = 0
    webpage_failed: int = 0
    recovered_uploads: int = 0
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
    job: ArchiveJob | None = None
    archive_generation: int | None = None
    archive_probe_token: str | None = None


class StateStore:
    """Own Saga's SQLite engine and transactional state transitions."""

    def __init__(self, path: Path, *, archival_enabled: bool = True) -> None:
        self.path = path
        self.archival_enabled = archival_enabled
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{path}", connect_args={"check_same_thread": False}
        )
        self._write_lock = Lock()
        self.download_pause_reason: str | None = None
        self.metadata_pause_reasons: dict[str, str | None] = {}
        self.health_started_at = utcnow()
        self._eligible_since: dict[str, datetime] = {}
        self.lane_progress: dict[str, datetime] = {}
        event.listen(self.engine, "connect", _configure_sqlite)
        if "archiveprogress" in inspect(self.engine).get_table_names() and (
            "media_uploaded_at"
            not in {
                column["name"]
                for column in inspect(self.engine).get_columns("archiveprogress")
            }
        ):
            self._backup_before_migration()
        elif "archiveprogress" in inspect(self.engine).get_table_names() and (
            "download_phase"
            not in {
                column["name"]
                for column in inspect(self.engine).get_columns("archiveprogress")
            }
        ):
            self._backup_before_migration("pre-staging-audit")
        SQLModel.metadata.create_all(self.engine)
        self._migrate_schema()
        logger.debug("Initialized SQLite state at {}", self.path)

    def close(self) -> None:
        """Dispose database connections."""
        self.engine.dispose()
        logger.debug("Closed SQLite state at {}", self.path)

    def _backup_before_migration(self, label: str = "pre-upload-first") -> None:
        backup = self.path.with_name(f"{self.path.name}.{label}.sqlite3")
        if not backup.exists():
            with (
                closing(sqlite3.connect(self.path)) as source,
                closing(sqlite3.connect(backup)) as target,
            ):
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise sqlite3.DatabaseError("migration backup failed quick_check")
            logger.info("Saved pre-migration SQLite backup at {}", backup)

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

            if not backfill:
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
            channel.backfill_completed_at = channel.updated_at = utcnow()
            session.add(channel)
            session.commit()
        logger.debug("Marked history backfill complete for channel {}", channel_id)

    def backfill_completed_at(self, channel_id: str) -> datetime | None:
        """Return when one channel's last full enumeration finished, if recorded."""
        with Session(self.engine) as session:
            channel = session.get(Channel, channel_id)
            if channel is None or channel.backfill_completed_at is None:
                return None
            return _as_utc(channel.backfill_completed_at)

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
                and video.publication_failure != "permanent"
            }
            return set(identifiers) - known_ids | ambiguous

    def claim_publication(
        self, channel_ids: Iterable[str], *, now: datetime | None = None
    ) -> tuple[str, DiscoveredVideo] | None:
        """Reserve one unresolved notification timestamp for the metadata worker.

        New discoveries take precedence over retries. Persisting the retry time
        prevents inaccessible videos from occupying the worker after a restart.
        """
        current = now or utcnow()
        with self._write_lock, Session(self.engine) as session:
            video = session.exec(
                select(Video)
                .where(
                    col(Video.channel_id).in_(tuple(channel_ids)),
                    col(Video.notification_ambiguous).is_(True),
                    col(Video.notification_silent).is_(False),
                    col(Video.notification_eligible).is_(False),
                    col(Video.publication_failure).is_(None)
                    | (col(Video.publication_failure) != "permanent"),
                    col(Video.publication_retry_at).is_(None)
                    | (col(Video.publication_retry_at) <= current),
                )
                .order_by(
                    col(Video.publication_retry_at).is_not(None),
                    col(Video.discovered_at).desc(),
                    col(Video.id),
                )
                .limit(1)
            ).first()
            if video is None:
                return None
            video.publication_retry_at = current + timedelta(minutes=5)
            session.add(video)
            session.commit()
            return video.channel_id, DiscoveredVideo(
                id=video.id,
                title=video.title,
                published_at=_as_utc(video.published_at)
                if video.published_at
                else None,
                live_status=video.live_status,
                replay_ready=video.replay_ready,
                published_precise=False,
                content_type=cast(ContentType, video.content_type),
            )

    def record_publication_failure(
        self,
        video_id: str,
        error: str,
        *,
        permanent: bool,
        base_delay: float = PUBLICATION_RETRY_BASE,
        now: datetime | None = None,
    ) -> datetime | None:
        """Stop resolving gone videos and back off retries for unready ones."""
        current = now or utcnow()
        with self._write_lock, Session(self.engine) as session:
            video = session.get(Video, video_id)
            if video is None:
                raise LookupError(f"Unknown video {video_id}")
            video.publication_attempts += 1
            video.publication_failure = "permanent" if permanent else "transient"
            video.publication_error = redact_diagnostics(error)[:2000]
            video.publication_retry_at = (
                None
                if permanent
                else current
                + timedelta(
                    seconds=min(
                        PUBLICATION_RETRY_CAP,
                        retry_delay(base_delay, video.publication_attempts),
                    )
                )
            )
            session.add(video)
            session.commit()
            return (
                None
                if video.publication_retry_at is None
                else _as_utc(video.publication_retry_at)
            )

    def reset_interrupted_claims(self, *, background_deadline: float = 86_400) -> None:
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
            for model in (ArchiveProgress, ArchiveJob):
                for row in session.exec(
                    select(model).where(
                        col(model.error_kind).is_(None),
                        col(model.last_error).is_not(None),
                    )
                ).all():
                    if legacy_bad_login(row.last_error):
                        row.error_kind = "service"
                        row.error_status = 400
                        row.error_phase = "login"
                        row.last_error = (
                            "InvalidServiceResponseError: unexpected Archive login "
                            "response (HTTP 400; phase=login)"
                        )
                        if (
                            isinstance(row, ArchiveProgress)
                            and row.status == "operator_action"
                            and row.failure_phase == "upload"
                            and row.media_uploaded_at is None
                        ):
                            row.status = "upload"
                            row.retry_at = None
                        elif isinstance(row, ArchiveJob) and row.status == "failed":
                            row.status = "waiting"
                        session.add(row)
            for progress in session.exec(
                select(ArchiveProgress).where(
                    ArchiveProgress.status == "operator_action",
                    ArchiveProgress.failure_phase == "upload",
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                )
            ).all():
                if (progress.last_error or "").startswith(
                    "AuthenticationError:"
                ) or progress.last_error in {
                    f"InvalidOptionError: {field} cannot contain controls or surrogates"
                    for field in ("title", "description", "subjects", "creator")
                }:
                    progress.status = "upload"
                    progress.retry_at = None
                    progress.retry_attempts = 0
                    progress.updated_at = utcnow()
                    session.add(progress)
            for progress in session.exec(
                select(ArchiveProgress).where(
                    ArchiveProgress.status == "unavailable",
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                )
            ).all():
                if is_access_error(progress.last_error or ""):
                    progress.status = (
                        "download" if progress.frozen_metadata else "metadata"
                    )
                    progress.failure_phase = "youtube_access"
                    progress.retry_at = None
                    progress.retry_attempts = 0
                    progress.updated_at = utcnow()
                    session.add(progress)
            for progress in session.exec(
                select(ArchiveProgress).where(
                    ArchiveProgress.status == "download",
                    ArchiveProgress.last_error
                    == (
                        "StagingIdentityError: selected formats changed; "
                        "retained previous inputs"
                    ),
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                )
            ).all():
                progress.retry_at = None
                progress.retry_attempts = 0
                session.add(progress)
            archive_rows = session.exec(
                select(ArchiveProgress).where(
                    col(ArchiveProgress.claimed_at).is_not(None)
                    | col(ArchiveProgress.maintenance_token).is_not(None)
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
                progress.maintenance_token = None
                progress.reserved_bytes = 0
                progress.archive_generation = None
                progress.retry_at = None
                progress.updated_at = utcnow()
                session.add(progress)
                if progress.status == "reconcile_upload":
                    self._restart_recovery_job(session, progress.video_id, delay=0)
            jobs = session.exec(
                select(ArchiveJob).where(col(ArchiveJob.claim_token).is_not(None))
            ).all()
            for job in jobs:
                if job.status == "submitting":
                    job.status = "failed"
                    job.last_error = "process stopped before webpage receipt was saved"
                job.claim_token = None
                job.claimed_at = None
                session.add(job)
            for job in session.exec(
                select(ArchiveJob).where(
                    ArchiveJob.kind == "recovery",
                    ArchiveJob.status == "failed",
                    col(ArchiveJob.last_error).like("AuthenticationError:%"),
                )
            ).all():
                job.status = "waiting"
                job.retry_at = None
                job.deadline_at = None
                session.add(job)
            # Older releases expired recovery checks while retaining the media.
            # Rebuild missing jobs as well; recovery is required for preservation.
            for progress in session.exec(
                select(ArchiveProgress).where(
                    ArchiveProgress.status == "reconcile_upload",
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                )
            ).all():
                self._queue_job(session, progress.video_id, "recovery")
                session.flush()
                job = session.get(ArchiveJob, (progress.video_id, "recovery"))
                assert job is not None
                if job.status in {"expired", "passed"}:
                    job.status = "waiting"
                    job.retry_at = None
                if job.status in {"pending", "waiting"}:
                    job.deadline_at = None
                    session.add(job)
            reports = session.exec(
                select(Report).where(Report.status == "sending")
            ).all()
            for report in reports:
                report.status = "pending"
                report.retry_at = None
                session.add(report)
            gate = session.get(ArchiveServiceState, 1)
            if gate is not None:
                self._clear_archive_probe(gate)
                gate.generation += 1
                session.add(gate)
            session.commit()
        interrupted = len(deliveries) + len(archive_rows) + len(reports)
        if interrupted:
            logger.info(
                "Recovered {} interrupted workflow claim(s): {} delivery, {} archive, "
                "{} report",
                interrupted,
                len(deliveries),
                len(archive_rows),
                len(reports),
            )
        else:
            logger.debug("No interrupted workflow claims required recovery")

    @staticmethod
    def _archive_due(record: Any, now: datetime) -> bool:
        return record.retry_at is None or _as_utc(record.retry_at) <= _as_utc(now)

    @staticmethod
    def _clear_archive_probe(gate: ArchiveServiceState) -> None:
        gate.probe_token = gate.probe_video_id = gate.probe_kind = None
        gate.probe_claimed_at = None

    def _archive_gate(self, session: Session, now: datetime) -> ArchiveServiceState:
        gate = session.get(ArchiveServiceState, 1)
        if gate is None:
            gate = ArchiveServiceState()
            # Legacy per-item delays must not postpone the first recovery probe.
            pending = session.exec(
                select(ArchiveProgress).where(
                    col(ArchiveProgress.status).in_(("upload", "reconcile_upload")),
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                )
            ).all()
            jobs = session.exec(
                select(ArchiveJob).where(
                    ArchiveJob.kind == "recovery",
                    col(ArchiveJob.status).in_(("pending", "waiting", "expired")),
                )
            ).all()
            if any(retryable_record(row) for row in [*pending, *jobs]):
                gate.failures = 1
                gate.retry_at = gate.first_failure_at = now
                gate.incident_id = str(uuid4())
                gate.incident_started_at = now
            session.add(gate)
            session.flush()
        return gate

    def _archive_admission(
        self, session: Session, now: datetime, lane: str
    ) -> ArchiveServiceState | None:
        gate = self._archive_gate(session, now)
        if (
            gate.failures == 0
            and gate.probation_until is not None
            and _as_utc(gate.probation_until) <= now
            and not gate.probe_token
            and not gate.probe_video_id
        ):
            # Restoring full concurrency at once re-triggers throttling, so a
            # clean probation only halves the spacing until it is negligible.
            interval = gate.write_interval_seconds / 2
            if interval < RELEASE_WRITE_INTERVAL:
                gate.probation_until = gate.retry_at = None
                gate.write_interval_seconds = 0
            else:
                gate.write_interval_seconds = interval
                gate.probation_until = now + timedelta(seconds=THROTTLE_PROBATION)
            session.add(gate)
        if not self._archive_paced(gate):
            return gate
        if gate.probe_token or not self._archive_due(gate, now):
            return None
        if gate.probe_video_id:
            progress = session.get(ArchiveProgress, gate.probe_video_id)
            if (
                progress is None
                or progress.status != "upload"
                or progress.media_uploaded_at
            ):
                self._clear_archive_probe(gate)
                session.add(gate)
            elif lane != "upload":
                return None
        if lane == "recovery":
            uploads = session.exec(
                select(ArchiveProgress).where(
                    ArchiveProgress.status == "upload",
                    col(ArchiveProgress.claimed_at).is_(None),
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                    col(ArchiveProgress.maintenance_token).is_(None),
                )
            ).all()
            if any(
                self._archive_due(row, now) or retryable_record(row) for row in uploads
            ):
                return None
        return gate

    @staticmethod
    def _archive_paced(gate: ArchiveServiceState) -> bool:
        return gate.failures > 0 or gate.probation_until is not None

    @staticmethod
    def _claim_archive_probe(
        session: Session,
        gate: ArchiveServiceState | None,
        video_id: str,
        kind: str,
        now: datetime,
    ) -> str | None:
        if gate is None or not StateStore._archive_paced(gate):
            return None
        token = str(uuid4())
        gate.probe_token, gate.probe_video_id, gate.probe_kind = token, video_id, kind
        gate.probe_claimed_at = gate.last_probe_at = now
        session.add(gate)
        return token

    def record_archive_failure(
        self,
        work: ArchiveWork,
        *,
        base_delay: float,
        hint: float,
        error: str,
        error_kind: str | None,
        error_status: int | None,
        error_phase: str | None,
        error_code: str | None = None,
        error_message: str | None = None,
        error_request_id: str | None = None,
        error_headers: str | None = None,
        error_queue: str | None = None,
    ) -> float:
        now = utcnow()
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            progress = session.get(ArchiveProgress, work.video.id)
            if progress is None or progress.operation_id != work.progress.operation_id:
                raise RuntimeError("Archive failure belongs to a different operation")
            if progress.media_uploaded_at is not None:
                return 0
            if work.job:
                job = session.get(ArchiveJob, (work.video.id, work.job.kind))
                if job is None or job.claim_token != work.job.claim_token:
                    return 0
            elif (
                work.archive_generation is not None
                and progress.archive_generation != work.archive_generation
            ):
                return 0
            gate = self._archive_gate(session, now)
            hinted = now + timedelta(seconds=hint)
            if work.archive_generation != gate.generation:
                # In-flight peers cannot extend an outage or reset its probes.
                # A longer explicit Retry-After still takes precedence.
                if hint and (gate.retry_at is None or _as_utc(gate.retry_at) < hinted):
                    gate.retry_at = hinted
                    gate.generation += 1
                    self._clear_archive_probe(gate)
                    session.add(gate)
                    session.commit()
                return (
                    max(0.0, (_as_utc(gate.retry_at) - now).total_seconds())
                    if gate.retry_at
                    else 0
                )
            if gate.incident_started_at is None and gate.first_failure_at is not None:
                gate.incident_started_at = gate.first_failure_at
            if not incident_open(gate):
                gate.incident_id = str(uuid4())
                gate.incident_started_at = now
                gate.incident_closed_at = None
                gate.incident_notified_category = None
                gate.incident_escalated_at = gate.incident_last_notified_at = None
            gate.incident_healthy_since = None
            gate.failures += 1
            gate.generation += 1
            gate.first_failure_at = gate.first_failure_at or now
            if error_kind == "rate_limit":
                gate.write_interval_seconds = min(
                    MAX_PROBE_DELAY,
                    max(MIN_WRITE_INTERVAL, gate.write_interval_seconds * 2),
                )
                gate.probation_until = now + timedelta(seconds=THROTTLE_PROBATION)
            delay = max(
                hint,
                gate.write_interval_seconds,
                min(MAX_PROBE_DELAY, retry_delay(base_delay, gate.failures)),
            )
            gate.retry_at = now + timedelta(seconds=delay)
            gate.error_kind, gate.error_status, gate.error_phase = (
                error_kind,
                error_status,
                error_phase,
            )
            gate.last_error = redact_diagnostics(error)[:2000]
            apply_diagnostics(
                gate,
                error_code=error_code,
                error_message=error_message,
                error_request_id=error_request_id,
                error_headers=error_headers,
                error_queue=error_queue,
            )
            self._clear_archive_probe(gate)
            session.add(gate)
            session.commit()
            return delay

    def _release_archive_deferrals(self, session: Session) -> None:
        rows = session.exec(
            select(ArchiveProgress).where(
                col(ArchiveProgress.status).in_(("upload", "reconcile_upload")),
                col(ArchiveProgress.claimed_at).is_(None),
                col(ArchiveProgress.media_uploaded_at).is_(None),
            )
        ).all()
        deferred = {row.video_id for row in rows if retryable_record(row)}
        for row in rows:
            if row.video_id in deferred:
                row.retry_at = None
                session.add(row)
        jobs = session.exec(
            select(ArchiveJob).where(
                ArchiveJob.kind == "recovery",
                col(ArchiveJob.status).in_(("pending", "waiting", "expired")),
                col(ArchiveJob.claim_token).is_(None),
            )
        ).all()
        for job in jobs:
            if (
                retryable_record(job)
                or job.error_kind is None
                and job.video_id in deferred
            ):
                job.retry_at = None
                session.add(job)

    def finish_archive_request(self, work: ArchiveWork, *, base_delay: float) -> None:
        if work.archive_generation is None:
            return
        now = utcnow()
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            gate = session.get(ArchiveServiceState, 1)
            progress = session.get(ArchiveProgress, work.video.id)
            if (
                gate is None
                or progress is None
                or progress.operation_id != work.progress.operation_id
                or work.archive_generation != gate.generation
            ):
                return
            if work.archive_probe_token is None:
                if gate.failures == 0 and progress.media_uploaded_at:
                    gate.last_success_at = now
                    session.add(gate)
                    session.commit()
                return
            if gate.probe_token != work.archive_probe_token:
                return
            if progress.media_uploaded_at:
                was_failing = gate.failures > 0
                gate.incident_healthy_since = gate.incident_healthy_since or now
                gate.failures = 0
                gate.generation += 1
                gate.retry_at = gate.first_failure_at = None
                gate.last_success_at = now
                if gate.probation_until is not None:
                    if was_failing:
                        gate.probation_until = now + timedelta(
                            seconds=THROTTLE_PROBATION
                        )
                    gate.retry_at = now + timedelta(seconds=gate.write_interval_seconds)
                self._clear_archive_probe(gate)
                self._release_archive_deferrals(session)
            elif work.job and progress.status == "upload":
                # A metadata read is not a successful write probe. Hand its
                # identified original to one upload worker while peers wait.
                gate.probe_token = gate.probe_claimed_at = None
                gate.probe_kind = "upload"
                gate.retry_at = now
            else:
                self._clear_archive_probe(gate)
                gate.retry_at = (
                    now + timedelta(seconds=min(MAX_PROBE_DELAY, max(1.0, base_delay)))
                    if progress.status == "reconcile_upload"
                    else now
                )
            session.add(gate)
            session.commit()

    def refresh_youtube_access(self, cookie_generation: str) -> int:
        """Make failures from an earlier cookie session due without touching claims."""
        with self._write_lock, Session(self.engine) as session:
            rows = session.exec(
                select(ArchiveProgress).where(
                    col(ArchiveProgress.status).in_(("metadata", "download")),
                    ArchiveProgress.failure_phase == "youtube_access",
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                    col(ArchiveProgress.claimed_at).is_(None),
                    col(ArchiveProgress.maintenance_token).is_(None),
                    col(ArchiveProgress.retry_at).is_not(None),
                    col(ArchiveProgress.youtube_access_generation).is_(None)
                    | (ArchiveProgress.youtube_access_generation != cookie_generation),
                )
            ).all()
            for row in rows:
                row.retry_at = None
                session.add(row)
            session.commit()
            return len(rows)

    def _archive_service_health(
        self,
        gate: ArchiveServiceState | None,
        rows: list[tuple[ArchiveProgress, Video]],
        jobs: list[ArchiveJob],
        current: datetime,
        started_at: datetime,
        stall_timeout: float,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        alerts: list[dict[str, Any]] = []
        if gate is None:
            self._eligible_since.pop("archive:probe", None)
            return None, alerts
        status = (
            "available"
            if not self._archive_paced(gate)
            else "probing"
            if gate.probe_token
            else ("probation" if gate.failures == 0 else "cooldown")
            if not self._archive_due(gate, current)
            else "probe ready"
        )
        snapshot: dict[str, Any] = {
            "status": status,
            "failure_count": gate.failures,
            "probation_until": _as_utc(gate.probation_until).isoformat()
            if gate.probation_until
            else None,
            "write_interval_seconds": gate.write_interval_seconds,
            "next_retry_at": _as_utc(gate.retry_at).isoformat()
            if gate.retry_at
            else None,
            "last_success_at": _as_utc(gate.last_success_at).isoformat()
            if gate.last_success_at
            else None,
            "last_probe_at": _as_utc(gate.last_probe_at).isoformat()
            if gate.last_probe_at
            else None,
            "probe_kind": gate.probe_kind,
            "probe_video_id": gate.probe_video_id,
            "error_kind": gate.error_kind,
            "error_status": gate.error_status,
            "error_phase": gate.error_phase,
            "reason": gate.error_code or "unknown",
            **diagnostic_snapshot(gate),
            "incident_id": gate.incident_id,
            "incident_age_seconds": max(
                0.0, (current - _as_utc(gate.first_failure_at)).total_seconds()
            )
            if gate.first_failure_at
            else None,
            "incident_escalated_at": _as_utc(gate.incident_escalated_at).isoformat()
            if gate.incident_escalated_at
            else None,
            "incident_last_notified_at": _as_utc(
                gate.incident_last_notified_at
            ).isoformat()
            if gate.incident_last_notified_at
            else None,
            "incident_notified_category": gate.incident_notified_category,
            "recovery_pending": gate.recovery_event is not None,
            "recovery_notified_at": _as_utc(gate.recovery_notified_at).isoformat()
            if gate.recovery_notified_at
            else None,
            "incident_healthy_since": _as_utc(gate.incident_healthy_since).isoformat()
            if gate.incident_healthy_since
            else None,
            "incident_closed_at": _as_utc(gate.incident_closed_at).isoformat()
            if gate.incident_closed_at
            else None,
            **incident_facts(gate, [row for row, _ in rows], current, stall_timeout),
        }
        by_id = {row.video_id: row for row, _ in rows}
        pending = any(
            row.status == "upload"
            and not row.claimed_at
            and not row.maintenance_token
            and row.media_uploaded_at is None
            and (self._archive_due(row, current) or retryable_record(row))
            for row, _ in rows
        ) or any(
            job.kind == "recovery"
            and job.status in {"pending", "waiting", "expired"}
            and not job.claim_token
            and job.video_id in by_id
            and by_id[job.video_id].media_uploaded_at is None
            and (
                self._archive_due(job, current)
                or retryable_record(job)
                or job.error_kind is None
                and retryable_record(by_id[job.video_id])
            )
            for job in jobs
        )
        if status == "probe ready" and pending:
            self._eligible_since.setdefault("archive:probe", current)
            if (
                current
                - max(
                    self._eligible_since["archive:probe"],
                    _as_utc(started_at) + timedelta(seconds=120),
                    _as_utc(gate.last_probe_at or started_at),
                )
            ).total_seconds() > stall_timeout:
                alerts.append(
                    {
                        "key": "archive:probe-stall",
                        "severity": "error",
                        "message": "Archive retry probe was not dispatched",
                        "details": snapshot,
                    }
                )
        else:
            self._eligible_since.pop("archive:probe", None)
        if gate.failures or incident_open(gate):
            alerts.append(
                {
                    "key": "archive:service",
                    "severity": snapshot["severity"],
                    "message": _archive_incident_message(snapshot["incident_category"]),
                    "details": snapshot,
                }
            )
        return snapshot, alerts

    def archive_service_notifications(
        self, *, stall_timeout: float, now: datetime | None = None
    ) -> list[dict[str, Any]]:
        """Reserve incident events durably before forwarding them to log sinks."""
        current = now or utcnow()
        events: list[dict[str, Any]] = []
        with self._write_lock, Session(self.engine) as session:
            gate = session.get(ArchiveServiceState, 1)
            if gate is None:
                return events
            if gate.recovery_event:
                events.append(
                    {
                        "severity": "info",
                        "recovery": True,
                        "message": (
                            "Archive service recovered; automatic preservation resumed"
                        ),
                        "details": json.loads(gate.recovery_event),
                    }
                )
                gate.recovery_event = None
                gate.recovery_notified_at = current
            if gate.failures:
                gate.incident_id = gate.incident_id or str(uuid4())
                gate.incident_started_at = (
                    gate.incident_started_at or gate.first_failure_at or current
                )
            if (
                incident_open(gate)
                and gate.failures == 0
                and gate.incident_healthy_since is not None
                and (current - _as_utc(gate.incident_healthy_since)).total_seconds()
                >= INCIDENT_RECOVERY_DELAY
            ):
                gate.incident_closed_at = current
                if gate.incident_escalated_at is not None:
                    events.append(
                        {
                            "severity": "info",
                            "recovery": True,
                            "message": (
                                "Archive service recovered after "
                                "sustained healthy writes"
                            ),
                            "details": {
                                "incident_id": gate.incident_id,
                                "incident_category": gate.incident_notified_category,
                                "recovered_at": current.isoformat(),
                            },
                        }
                    )
                    gate.recovery_notified_at = current
                gate.error_kind = gate.error_phase = gate.last_error = None
                gate.error_status = None
                apply_diagnostics(gate)
            if gate.failures:
                rows = session.exec(
                    select(ArchiveProgress).where(
                        (col(ArchiveProgress.status) == "uploading")
                        | (
                            col(ArchiveProgress.media_uploaded_at)
                            >= gate.incident_started_at
                        )
                    )
                ).all()
                facts = incident_facts(gate, rows, current, stall_timeout)
                category = facts["incident_category"]
                escalated = gate.incident_escalated_at is not None
                actionable = category in {"authentication", "item-specific"}
                error = facts["severity"] == "error"
                notify = (
                    gate.incident_last_notified_at is None
                    or error
                    and not escalated
                    or category != gate.incident_notified_category
                    and (
                        actionable
                        or gate.incident_notified_category
                        in {"authentication", "item-specific"}
                    )
                    or error
                    and escalated
                    and (
                        current - _as_utc(gate.incident_last_notified_at)
                    ).total_seconds()
                    >= INCIDENT_REMINDER_DELAY
                )
                if notify:
                    starting = gate.incident_last_notified_at is None
                    gate.incident_notified_category = category
                    gate.incident_last_notified_at = current
                    if error and not escalated:
                        gate.incident_escalated_at = current
                    events.append(
                        {
                            "severity": "error" if error else "warning",
                            "message": _archive_incident_message(category),
                            "details": {
                                "incident_id": gate.incident_id,
                                **facts,
                                "failure_count": gate.failures,
                                "next_retry_at": _as_utc(gate.retry_at).isoformat()
                                if gate.retry_at
                                else None,
                                "error_kind": gate.error_kind,
                                "error_status": gate.error_status,
                                "error_phase": gate.error_phase,
                                "reason": gate.error_code or "unknown",
                                **diagnostic_snapshot(gate),
                            },
                        }
                    )
                    if starting and error:
                        events.insert(
                            len(events) - 1, {**events[-1], "severity": "warning"}
                        )
            session.add(gate)
            session.commit()
        return events

    def webpage_outcome_notifications(
        self, alert: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        """Announce a changed failed-job set once, including across restarts."""
        with self._write_lock, Session(self.engine) as session:
            record = session.get(PreservationAlertState, "webpage:outcome")
            if alert is None:
                if record is None or record.fingerprint is None:
                    return []
                record.fingerprint = None
                record.notified_at = utcnow()
                session.add(record)
                session.commit()
                return [
                    {
                        "severity": "info",
                        "recovery": True,
                        "message": "Webpage preservation condition cleared",
                        "details": {"previous_count": record.outcome_count},
                    }
                ]
            fingerprint = alert["incident_fingerprint"]
            if record is not None and record.fingerprint == fingerprint:
                return []
            record = record or PreservationAlertState(key="webpage:outcome")
            record.fingerprint = fingerprint
            record.notified_at = utcnow()
            record.outcome_count = alert["details"]["count"] + len(
                alert["details"]["active_without_progress"]
            )
            session.add(record)
            session.commit()
            return [alert]

    def claim_archive(
        self,
        *,
        prefer_recent: bool,
        now: datetime | None = None,
        lane: str = "transfer",
        free_bytes: int | None = None,
        minimum_free_bytes: int = 0,
        data_budget_bytes: int | None = None,
        buffer: int = 2,
        unknown_download_bytes: int = 10_000_000_000,
        download_root: Path | None = None,
        dispatch_class: str | None = None,
        pause_fresh_downloads: bool = False,
        youtube_cookie_generation: str | None = None,
        archive_retry_base: float | None = None,
    ) -> ArchiveWork | None:
        """Atomically claim due transfer work within its reserved worker lane."""
        current = now or utcnow()
        stages = {
            "upload": ("upload",),
            "download": ("download",),
            "metadata": ("metadata",),
            "transfer": ("upload", "download", "metadata"),
        }[lane]
        access_recheck = (
            (col(ArchiveProgress.failure_phase) == "youtube_access")
            & (
                col(ArchiveProgress.youtube_access_generation).is_(None)
                | (
                    ArchiveProgress.youtube_access_generation
                    != youtube_cookie_generation
                )
            )
            if youtube_cookie_generation is not None
            else false()
        )
        queue_order = (
            case(
                (col(ArchiveProgress.status) == "upload", 0),
                (col(ArchiveProgress.status) == "download", 1),
                else_=2,
            ),
            case((access_recheck, 0), else_=1),
            case((col(ArchiveProgress.failure_phase) == "disk", 1), else_=0),
            case((col(Video.notification_candidate).is_(prefer_recent), 0), else_=1),
            col(ArchiveProgress.updated_at),
            col(ArchiveProgress.video_id),
        )
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            gate = (
                self._archive_admission(session, current, "upload")
                if lane == "upload" and archive_retry_base is not None
                else None
            )
            if lane == "upload" and archive_retry_base is not None and gate is None:
                session.commit()
                return None
            probe = gate is not None and self._archive_paced(gate)
            if lane == "download":
                staged_rows = session.exec(
                    select(ArchiveProgress).where(
                        col(ArchiveProgress.media_uploaded_at).is_(None),
                        ArchiveProgress.status == "upload",
                        col(ArchiveProgress.retry_at).is_(None)
                        | (col(ArchiveProgress.retry_at) <= current),
                    )
                ).all()
                service_gate = (
                    session.get(ArchiveServiceState, 1)
                    if archive_retry_base is not None
                    else None
                )
                staged = sum(
                    service_gate is None
                    or not self._archive_paced(service_gate)
                    or not retryable_record(row)
                    for row in staged_rows
                )
                active = session.exec(
                    select(func.count(col(ArchiveProgress.video_id))).where(
                        ArchiveProgress.status == "download",
                        col(ArchiveProgress.claimed_at).is_not(None),
                        col(ArchiveProgress.maintenance_token).is_(None),
                    )
                ).one()
                if staged + active >= buffer:
                    self.download_pause_reason = "upload-ready buffer full"
                    return None
                reserved = session.exec(
                    select(
                        func.coalesce(func.sum(col(ArchiveProgress.reserved_bytes)), 0)
                    )
                ).one()
            else:
                reserved = 0
            metadata_classes: list[bool] = []
            if lane == "metadata":
                for recent, allowance in ((True, buffer), (False, 2 * buffer)):
                    supply = session.exec(
                        select(func.count(col(ArchiveProgress.video_id)))
                        .join(Video, col(ArchiveProgress.video_id) == col(Video.id))
                        .where(
                            col(Video.notification_candidate).is_(recent),
                            (col(ArchiveProgress.status) == "metadata")
                            & col(ArchiveProgress.claimed_at).is_not(None)
                            | (col(ArchiveProgress.status) == "download")
                            & (
                                col(ArchiveProgress.retry_at).is_(None)
                                | (col(ArchiveProgress.retry_at) <= current)
                            )
                            & (
                                col(ArchiveProgress.failure_phase).is_(None)
                                | (ArchiveProgress.failure_phase != "disk")
                            ),
                        )
                    ).one()
                    key = "recent" if recent else "backfill"
                    self.metadata_pause_reasons[key] = (
                        f"{key} metadata supply full" if supply >= allowance else None
                    )
                    if supply < allowance:
                        metadata_classes.append(recent)
            statement = (
                select(ArchiveProgress, Video, Channel)
                .join(Video, col(ArchiveProgress.video_id) == col(Video.id))
                .join(Channel, col(Video.channel_id) == col(Channel.id))
                .where(
                    col(ArchiveProgress.status).in_(stages),
                    col(ArchiveProgress.media_uploaded_at).is_(None),
                    col(ArchiveProgress.claimed_at).is_(None),
                    col(ArchiveProgress.maintenance_token).is_(None),
                )
            )
            if not probe:
                statement = statement.where(
                    col(ArchiveProgress.retry_at).is_(None)
                    | (col(ArchiveProgress.retry_at) <= current)
                )
            if lane == "metadata":
                statement = statement.where(
                    col(Video.notification_candidate).in_(metadata_classes)
                    | access_recheck
                )
            statement = statement.order_by(*queue_order)
            rows = (
                list(session.exec(statement).all())
                if lane == "download" or probe
                else (
                    [row]
                    if (row := session.exec(statement).first()) is not None
                    else []
                )
            )
            if probe:
                assert gate is not None
                rows = [
                    row
                    for row in rows
                    if (self._archive_due(row[0], current) or retryable_record(row[0]))
                    and (
                        gate.probe_video_id is None
                        or row[0].video_id == gate.probe_video_id
                    )
                ]
                rows.sort(key=lambda row: row[0].media_size or float("inf"))
            selected = None
            inventories = {
                progress.video_id: inventory_inputs(
                    Path(progress.working_directory), progress.operation_id, video.id
                )
                for progress, video, _ in rows
                if lane == "download"
                and download_root is not None
                and progress.working_directory
                and progress.operation_id
                and Path(progress.working_directory).parent.resolve()
                == download_root.resolve()
            }
            completed = {
                progress.video_id: completed_download_size(
                    Path(progress.working_directory),
                    progress.operation_id,
                    progress.video_id,
                )
                for progress, _, _ in rows
                if progress.video_id in inventories
                and progress.working_directory
                and progress.operation_id
            }
            if lane == "download" and dispatch_class:

                def priority(row: Any) -> tuple[int, int, int, datetime, str]:
                    progress, video, _ = row
                    inventory = inventories.get(video.id)
                    category = (
                        "recent"
                        if video.notification_candidate
                        else "resume"
                        if inventory and inventory.partial_bytes
                        else "backfill"
                    )
                    order = [
                        dispatch_class,
                        *(
                            kind
                            for kind in ("recent", "resume", "backfill")
                            if kind != dispatch_class
                        ),
                    ]
                    return (
                        int(completed.get(video.id) is None),
                        int(
                            not (
                                youtube_cookie_generation is not None
                                and progress.failure_phase == "youtube_access"
                                and progress.youtube_access_generation
                                != youtube_cookie_generation
                            )
                        ),
                        order.index(category),
                        progress.updated_at,
                        video.id,
                    )

                rows.sort(key=priority)
            for progress, video, channel in rows:
                reservation = 0
                if lane == "download":
                    if (
                        pause_fresh_downloads
                        and completed.get(video.id) is None
                        and not (
                            video.id in inventories
                            and inventories[video.id].reusable_bytes
                        )
                    ):
                        self.download_pause_reason = (
                            "protected partials exceed retention target"
                        )
                        continue
                    metadata = json.loads(progress.frozen_metadata or "{}")
                    reservation = download_reservation(
                        max(
                            progress.download_estimated_bytes
                            or metadata.get("estimated_bytes")
                            or unknown_download_bytes,
                            1,
                        ),
                        inventories[video.id].reusable_bytes
                        if video.id in inventories
                        else 0,
                    )
                    if completed.get(video.id) is not None:
                        reservation = 0
                    if reservation and (
                        free_bytes is None
                        or free_bytes < minimum_free_bytes + reserved + reservation
                    ):
                        self.download_pause_reason = "disk budget full"
                        progress.failure_phase = "disk"
                        progress.last_error = (
                            "waiting for download and merge disk budget"
                        )
                        session.add(progress)
                        continue
                    if reservation and (
                        data_budget_bytes is not None
                        and data_budget_bytes < reserved + reservation
                    ):
                        self.download_pause_reason = "data budget full"
                        progress.failure_phase = "disk"
                        progress.last_error = "waiting for Saga data-directory budget"
                        session.add(progress)
                        continue
                selected = progress, video, channel
                progress.reserved_bytes = reservation
                break
            if selected is None:
                if not rows and lane == "download":
                    self.download_pause_reason = "no eligible download"
                session.commit()
                return None
            progress, video, channel = selected
            if lane == "download":
                self.download_pause_reason = None
                progress.failure_phase = None
                progress.last_error = None
            progress.claimed_at = current
            progress.attempts += 1
            progress.updated_at = current
            generation = gate.generation if gate is not None else None
            progress.archive_generation = generation
            probe_token = self._claim_archive_probe(
                session, gate, progress.video_id, "upload", current
            )
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
            return ArchiveWork(
                progress=progress,
                video=video,
                channel=channel,
                archive_generation=generation,
                archive_probe_token=probe_token,
            )

    def staging_rows(self) -> list[ArchiveProgress]:
        """Read detached operation facts for inventory and narrow maintenance."""
        with Session(self.engine) as session:
            rows = list(
                session.exec(
                    select(ArchiveProgress).where(
                        col(ArchiveProgress.working_directory).is_not(None)
                    )
                ).all()
            )
            for row in rows:
                session.expunge(row)
            return rows

    def shutdown_summary(self) -> list[dict[str, str | None]]:
        """Identify stopped and uncertain operations before closing durable state."""
        with Session(self.engine) as session:
            rows = session.exec(
                select(ArchiveProgress).where(
                    (col(ArchiveProgress.failure_phase) == "shutdown")
                    | (col(ArchiveProgress.status) == "reconcile_upload")
                )
            ).all()
            return [
                {
                    "video_id": row.video_id,
                    "operation_id": row.operation_id,
                    "stage": row.status,
                }
                for row in rows
            ]

    def claim_partial_maintenance(self, video_id: str, operation_id: str) -> str | None:
        """Exclude download claims while deleting one inactive operation's inputs."""
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            row = session.get(ArchiveProgress, video_id)
            if (
                row is None
                or row.operation_id != operation_id
                or row.status not in {"download", "unavailable"}
                or row.claimed_at
                or row.maintenance_token
                or row.media_uploaded_at
                or row.download_completed_at
            ):
                return None
            token = str(uuid4())
            row.maintenance_token = token
            row.claimed_at = utcnow()
            session.add(row)
            session.commit()
            return token

    def release_partial_maintenance(
        self, video_id: str, token: str, *, error: str
    ) -> None:
        """Release maintenance without changing retry deadlines or attempt counts."""
        with self._write_lock, Session(self.engine) as session:
            row = session.get(ArchiveProgress, video_id)
            if row is None or row.maintenance_token != token:
                raise RuntimeError("partial maintenance claim changed")
            row.maintenance_token = None
            row.claimed_at = None
            row.reserved_bytes = 0
            if row.status != "unavailable":
                row.last_error = error
            if error.startswith("partial inputs evicted"):
                if row.status != "unavailable":
                    row.failure_phase = "partial_evicted"
                row.download_bytes = 0
                row.download_progress_at = None
                row.download_phase = None
                row.merge_bytes = 0
            session.add(row)
            session.commit()

    @staticmethod
    def _queue_job(
        session: Session, video_id: str, kind: str, *, deadline: float | None = None
    ) -> None:
        if session.get(ArchiveJob, (video_id, kind)) is None:
            session.add(
                ArchiveJob(
                    video_id=video_id,
                    kind=kind,
                    deadline_at=utcnow() + timedelta(seconds=deadline)
                    if deadline and kind != "recovery"
                    else None,
                )
            )

    def claim_background(
        self,
        kind: str,
        *,
        now: datetime | None = None,
        max_attempts: int = 12,
        archive_retry_base: float | None = None,
    ) -> ArchiveWork | None:
        """Claim background work; required upload recovery has no expiry budget."""
        current = now or utcnow()
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            gate = (
                self._archive_admission(session, current, "recovery")
                if kind == "recovery" and archive_retry_base is not None
                else None
            )
            if kind == "recovery" and archive_retry_base is not None and gate is None:
                session.commit()
                return None
            probe = gate is not None and self._archive_paced(gate)
            statement = select(ArchiveJob)
            jobs = session.exec(
                statement.where(
                    ArchiveJob.kind == kind,
                    col(ArchiveJob.status).in_(
                        ("pending", "waiting", "expired")
                        if kind == "recovery"
                        else ("pending", "waiting")
                    ),
                    col(ArchiveJob.claim_token).is_(None),
                ).order_by(col(ArchiveJob.updated_at), col(ArchiveJob.video_id))
            ).all()
            if probe:
                sizes = dict(
                    session.exec(
                        select(
                            col(ArchiveProgress.video_id),
                            col(ArchiveProgress.media_size),
                        )
                    ).all()
                )
                jobs = sorted(
                    jobs, key=lambda job: sizes.get(job.video_id) or float("inf")
                )
            for job in jobs:
                if not probe and not self._archive_due(job, current):
                    continue
                if kind not in {"cleanup", "recovery"} and (
                    job.attempts >= max_attempts
                    or (
                        job.deadline_at and _as_utc(job.deadline_at) <= _as_utc(current)
                    )
                ):
                    job.status = "expired"
                    session.add(job)
                    continue
                progress = session.get(ArchiveProgress, job.video_id)
                assert progress is not None
                if probe and not (
                    self._archive_due(job, current)
                    or retryable_record(job)
                    or job.error_kind is None
                    and retryable_record(progress)
                ):
                    continue
                if kind == "recovery" and progress.media_uploaded_at is not None:
                    job.status = "passed"
                    session.add(job)
                    continue
                if kind == "recovery":
                    job.status = "waiting"
                    job.deadline_at = None
                video = session.get(Video, job.video_id)
                assert video is not None
                channel = session.get(Channel, video.channel_id)
                assert channel is not None
                job.claim_token = str(uuid4())
                job.claimed_at = current
                job.attempts += 1
                probe_token = self._claim_archive_probe(
                    session, gate, job.video_id, kind, current
                )
                generation = gate.generation if gate is not None else None
                session.add(job)
                session.commit()
                for item in (job, progress, video, channel):
                    session.refresh(item)
                    session.expunge(item)
                return ArchiveWork(
                    progress,
                    video,
                    channel,
                    job,
                    archive_generation=generation,
                    archive_probe_token=probe_token,
                )
            session.commit()
            return None

    @staticmethod
    def _restart_recovery_job(session: Session, video_id: str, *, delay: float) -> None:
        """Reset recovery for a newly uncertain transfer without an expiry deadline."""
        job = session.get(ArchiveJob, (video_id, "recovery"))
        if job is None:
            job = ArchiveJob(video_id=video_id, kind="recovery")
        now = utcnow()
        job.status = "pending"
        job.attempts = 0
        job.retry_at = now + timedelta(seconds=delay)
        job.deadline_at = None
        job.claim_token = None
        job.claimed_at = None
        job.last_error = None
        job.updated_at = now
        session.add(job)

    def update_job(
        self,
        work: ArchiveWork,
        *,
        status: str,
        retry_after: float | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        error_status: int | None = None,
        error_phase: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        error_request_id: str | None = None,
        error_headers: str | None = None,
        error_queue: str | None = None,
        **facts: Any,
    ) -> None:
        """Commit background facts only for the current independent claim."""
        assert work.job is not None
        with self._write_lock, Session(self.engine) as session:
            job = session.get(ArchiveJob, (work.video.id, work.job.kind))
            if job is None or job.claim_token != work.job.claim_token:
                raise RuntimeError("background claim no longer belongs to this worker")
            progress = session.get(ArchiveProgress, work.video.id)
            assert progress is not None
            if progress.operation_id != work.progress.operation_id:
                raise RuntimeError(
                    "background job belongs to a different Saga operation"
                )
            for name, value in facts.items():
                if name not in {
                    "webpage_status",
                    "wayback_job_id",
                    "ziggy_receipt",
                    "media_verified_at",
                    "cleaned_at",
                    "media_path",
                    "working_directory",
                }:
                    raise ValueError(f"invalid background fact {name}")
                setattr(progress, name, value)
            job.status = status
            job.last_error = (
                redact_diagnostics(error)[:2000] if error is not None else None
            )
            apply_diagnostics(
                job,
                error_code=error_code,
                error_message=error_message,
                error_request_id=error_request_id,
                error_headers=error_headers,
                error_queue=error_queue,
            )
            job.error_kind, job.error_status, job.error_phase = (
                error_kind,
                error_status,
                error_phase,
            )
            job.updated_at = utcnow()
            if status != "submitting":
                job.claim_token = None
                job.claimed_at = None
            job.retry_at = (
                utcnow() + timedelta(seconds=retry_after)
                if retry_after is not None
                else None
            )
            session.add_all((job, progress))
            session.commit()

    def acknowledge_upload(
        self,
        work: ArchiveWork,
        *,
        receipt: str,
        etag: str | None,
        recovered: bool = False,
        validation: bool = False,
        deadline: float = 86_400,
    ) -> None:
        """Commit upload completion and enqueue cleanup in the same transaction."""
        with self._write_lock, Session(self.engine) as session:
            progress = session.get(ArchiveProgress, work.video.id)
            if (
                progress is None
                or not progress.operation_id
                or progress.operation_id != work.progress.operation_id
            ):
                raise RuntimeError(
                    "upload receipt does not belong to this Saga operation"
                )
            if progress.media_uploaded_at is None:
                progress.media_uploaded_at = utcnow()
                progress.uploaded_bytes = progress.media_size
                progress.upload_receipt = receipt
                progress.upload_etag = etag
                progress.upload_origin = "recovered" if recovered else "transferred"
            progress.multipart_state = None
            progress.status = "complete"
            progress.claimed_at = None
            progress.retry_at = None
            progress.reserved_bytes = 0
            progress.last_error = None
            progress.error_kind = progress.error_status = progress.error_phase = None
            apply_diagnostics(progress)
            progress.failure_phase = None
            progress.updated_at = utcnow()
            session.add(progress)
            self._queue_job(session, work.video.id, "cleanup")
            self._queue_job(session, work.video.id, "webpage", deadline=deadline)
            if validation:
                self._queue_job(session, work.video.id, "validation", deadline=deadline)
            session.commit()

    def defer_transfer(
        self,
        work: ArchiveWork,
        *,
        status: str,
        error: str,
        delay: float,
        deadline: float,
        error_kind: str | None = None,
        error_status: int | None = None,
        error_phase: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        error_request_id: str | None = None,
        error_headers: str | None = None,
        error_queue: str | None = None,
    ) -> None:
        """Release failed or uncertain transfers and give checks their own queue."""
        with self._write_lock, Session(self.engine) as session:
            progress = session.get(ArchiveProgress, work.video.id)
            if progress is None or progress.operation_id != work.progress.operation_id:
                raise RuntimeError("transfer belongs to a different Saga operation")
            if progress.media_uploaded_at is not None:
                return
            if (
                work.archive_generation is not None
                and progress.archive_generation != work.archive_generation
            ):
                return
            progress.status = status
            progress.claimed_at = None
            progress.retry_at = utcnow() + timedelta(seconds=delay)
            progress.last_error = redact_diagnostics(error)[:2000]
            apply_diagnostics(
                progress,
                error_code=error_code,
                error_message=error_message,
                error_request_id=error_request_id,
                error_headers=error_headers,
                error_queue=error_queue,
            )
            progress.error_kind, progress.error_status, progress.error_phase = (
                error_kind,
                error_status,
                error_phase,
            )
            progress.failure_phase = "initial_upload"
            progress.retry_attempts += 1
            progress.updated_at = utcnow()
            session.add(progress)
            if status == "reconcile_upload":
                self._restart_recovery_job(session, work.video.id, delay=delay)
            session.commit()

    def queue_webpage_only(self, video_id: str, *, deadline: float) -> None:
        """Finish Ziggy-only prerequisites and schedule its capture separately."""
        with self._write_lock, Session(self.engine) as session:
            progress = session.get(ArchiveProgress, video_id)
            if progress is None:
                raise LookupError(f"Unknown archive progress for {video_id}")
            progress.status = "complete"
            progress.claimed_at = None
            session.add(progress)
            self._queue_job(session, video_id, "webpage", deadline=deadline)
            session.commit()

    def record_worker_time(self, kind: str, seconds: float) -> None:
        """Accumulate worker occupancy without changing report snapshots."""
        with self._write_lock, Session(self.engine) as session:
            timing = session.get(WorkerTiming, kind) or WorkerTiming(kind=kind)
            timing.seconds += seconds
            session.add(timing)
            session.commit()

    def other_download_reservations(self, video_id: str) -> int:
        """Read other workers' conservative budgets without a write transaction."""
        with Session(self.engine) as session:
            return session.exec(
                select(
                    func.coalesce(func.sum(col(ArchiveProgress.reserved_bytes)), 0)
                ).where(ArchiveProgress.video_id != video_id)
            ).one()

    def guard_download_space(
        self,
        video_id: str,
        expected: int,
        downloaded: int,
        free: int,
        minimum: int,
        data_budget_bytes: int | None = None,
        **facts: Any,
    ) -> None:
        """Adjust a download/merge reservation as yt-dlp learns stream sizes."""
        with self._write_lock, Session(self.engine) as session:
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            progress = session.get(ArchiveProgress, video_id)
            assert progress is not None
            others = session.exec(
                select(
                    func.coalesce(func.sum(col(ArchiveProgress.reserved_bytes)), 0)
                ).where(ArchiveProgress.video_id != video_id)
            ).one()
            remaining = download_reservation(expected, downloaded)
            if facts.get("download_phase") in {"merging", "merged"}:
                remaining = max(expected - facts.get("merge_bytes", 0), 0)
            progress.download_estimated_bytes = max(
                expected, progress.download_estimated_bytes or 0
            )
            permitted = free >= minimum + others + remaining and (
                data_budget_bytes is None or data_budget_bytes >= others + remaining
            )
            if permitted:
                progress.reserved_bytes = remaining
            for name, value in facts.items():
                setattr(progress, name, value)
            if session.is_modified(progress):
                session.add(progress)
                session.commit()
            if not permitted:
                from core.youtube import DiskSpaceError

                raise DiskSpaceError(
                    "download and merge exceed available disk or data reservations"
                )

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
        allowed = set(ArchiveProgress.model_fields) - {
            "video_id",
            "status",
            "claimed_at",
            "retry_at",
        }
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"Unknown archive fields: {sorted(unknown)}")
        if retry_after is not None and retry_after < 0:
            raise ValueError("retry_after must be non-negative")

        diagnostics = {
            name: values[name] for name in DIAGNOSTIC_FIELDS if name in values
        }
        if diagnostics:
            values.update(diagnostic_fields(**diagnostics))
        if values.get("last_error") is not None:
            values["last_error"] = redact_diagnostics(values["last_error"])[:2000]

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
                progress.reserved_bytes = 0
                progress.retry_at = (
                    utcnow() + timedelta(seconds=retry_after)
                    if retry_after is not None
                    else None
                )
            progress.updated_at = utcnow()
            session.add(progress)
            session.commit()
            if previous_status != progress.status:
                self.lane_progress[previous_status] = utcnow()
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
        self, initial_end: datetime, schedule: str
    ) -> tuple[ReportCheckpoint, bool]:
        """Create or realign the singleton report checkpoint."""
        current = utcnow()
        with self._write_lock, Session(self.engine) as session:
            checkpoint = session.get(ReportCheckpoint, 1)
            initialized = checkpoint is None
            if checkpoint is None:
                checkpoint = ReportCheckpoint(
                    last_window_end=initial_end,
                    schedule=schedule,
                    initialized_at=current,
                    updated_at=current,
                )
            elif checkpoint.schedule != schedule:
                checkpoint.last_window_end = initial_end
                checkpoint.schedule = schedule
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
                        func.coalesce(func.sum(col(ArchiveProgress.uploaded_bytes)), 0),
                    ).where(
                        col(ArchiveProgress.media_uploaded_at).is_not(None),
                        col(ArchiveProgress.media_uploaded_at) >= window_start,
                        col(ArchiveProgress.media_uploaded_at) < window_end,
                        ArchiveProgress.upload_origin == "transferred",
                    )
                ).one()
                media_lifetime = session.exec(
                    select(
                        func.count(col(ArchiveProgress.video_id)),
                        func.coalesce(func.sum(col(ArchiveProgress.uploaded_bytes)), 0),
                    ).where(
                        col(ArchiveProgress.media_uploaded_at).is_not(None),
                        col(ArchiveProgress.media_uploaded_at) < window_end,
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
                    **self._queue_counts(session),
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

    @staticmethod
    def _queue_counts(session: Session) -> dict[str, Any]:
        statuses = dict(
            session.exec(
                select(
                    ArchiveProgress.status, func.count(col(ArchiveProgress.video_id))
                ).group_by(ArchiveProgress.status)
            ).all()
        )
        jobs = {
            (kind, status): count
            for kind, status, count in session.exec(
                select(
                    ArchiveJob.kind,
                    ArchiveJob.status,
                    func.count(col(ArchiveJob.video_id)),
                ).group_by(ArchiveJob.kind, ArchiveJob.status)
            ).all()
        }
        return {
            "ready_uploads": statuses.get("upload", 0),
            "pending_downloads": statuses.get("download", 0)
            + statuses.get("metadata", 0),
            "active_transfers": statuses.get("uploading", 0),
            "failed_transfers": session.exec(
                select(func.count(col(ArchiveProgress.video_id))).where(
                    ArchiveProgress.status == "upload",
                    col(ArchiveProgress.last_error).is_not(None),
                )
            ).one(),
            "uncertain_transfers": statuses.get("reconcile_upload", 0),
            "recovered_uploads": session.exec(
                select(func.count(col(ArchiveProgress.video_id))).where(
                    col(ArchiveProgress.upload_origin).in_(("recovered", "historical"))
                )
            ).one(),
            **{
                f"validation_{status}": jobs.get(("validation", status), 0)
                + (jobs.get(("validation", "waiting"), 0) if status == "pending" else 0)
                for status in ("pending", "passed", "failed", "expired")
            },
            "webpage_pending": sum(
                jobs.get(("webpage", status), 0)
                for status in ("pending", "waiting", "submitting")
            ),
            "webpage_completed": jobs.get(("webpage", "passed"), 0),
            "webpage_failed": jobs.get(("webpage", "failed"), 0)
            + jobs.get(("webpage", "expired"), 0),
        }

    def transfer_health(
        self,
        *,
        stall_timeout: float,
        now: datetime | None = None,
        started_at: datetime | None = None,
        upload_workers: int = 1,
        download_workers: int = 1,
        channel_intervals: dict[str, float] | None = None,
        include_archive: bool = True,
        youtube_cookie_generation: str | None = None,
    ) -> dict[str, Any]:
        """Measure progress, due age, and backpressure independently across lanes."""
        from core.pipeline_health import build_pipeline_health

        intervals = channel_intervals or {}
        with Session(self.engine) as session:
            rows = list(
                session.exec(
                    select(ArchiveProgress, Video).join(
                        Video, col(ArchiveProgress.video_id) == col(Video.id)
                    )
                ).all()
            )
            jobs = (
                list(session.exec(select(ArchiveJob)).all()) if include_archive else []
            )
            poll_stamps = dict(
                session.exec(
                    select(Channel.id, Channel.last_polled_at).where(
                        col(Channel.id).in_(tuple(intervals))
                    )
                ).all()
            )
            gate = session.get(ArchiveServiceState, 1) if include_archive else None
            # Polls can commit during these reads. Observe time after fetching
            # their timestamps so a newly completed poll cannot have a negative age.
            current = now or utcnow()
            health = build_pipeline_health(
                rows if include_archive else [],
                jobs,
                now=current,
                started_at=started_at or self.health_started_at,
                stall_timeout=stall_timeout,
                capacities={
                    "upload": upload_workers,
                    "download": download_workers,
                    "metadata": 1,
                },
                eligible_since=self._eligible_since,
                metadata_reasons=self.metadata_pause_reasons,
                download_reason=self.download_pause_reason,
                lane_progress=self.lane_progress,
                youtube_cookie_generation=youtube_cookie_generation,
                archive_pause_reason=(
                    "Archive retry probe in progress"
                    if gate and self._archive_paced(gate) and gate.probe_token
                    else "Archive service cooldown"
                    if gate
                    and self._archive_paced(gate)
                    and not self._archive_due(gate, current)
                    else "Archive retry probe required"
                    if gate and self._archive_paced(gate)
                    else None
                ),
            )
            service_state, service_alerts = self._archive_service_health(
                gate,
                rows,
                jobs,
                current,
                started_at or self.health_started_at,
                stall_timeout,
            )
            health["archive_service"] = service_state
            outcome = session.get(PreservationAlertState, "webpage:outcome")
            health["webpage_notification"] = {
                "active": bool(outcome and outcome.fingerprint),
                "last_notified_at": _as_utc(outcome.notified_at).isoformat()
                if outcome and outcome.notified_at
                else None,
                "outcome_count": outcome.outcome_count if outcome else 0,
            }
            if gate and gate.failures:
                health["alerts"] = [
                    alert
                    for alert in health["alerts"]
                    if alert["key"] != "archive:access"
                    and not (
                        alert["key"] == "archive:permission"
                        and gate.error_kind == "permission"
                    )
                ]
            health["alerts"].extend(service_alerts)
            health["archive_diagnostics"] = {
                "transfers": [
                    {
                        "video_id": row.video_id,
                        "error_kind": row.error_kind,
                        "error_status": row.error_status,
                        "error_phase": row.error_phase,
                        **diagnostic_snapshot(row),
                    }
                    for row, _ in rows
                    if row.last_error
                ],
                "jobs": [
                    {
                        "video_id": job.video_id,
                        "kind": job.kind,
                        "error_kind": job.error_kind,
                        "error_status": job.error_status,
                        "error_phase": job.error_phase,
                        **diagnostic_snapshot(job),
                    }
                    for job in jobs
                    if job.last_error
                ],
            }
            if any(alert["key"] == "archive:probe-stall" for alert in service_alerts):
                health["healthy"] = False
            polling: dict[str, Any] = {}
            for channel_id, interval in intervals.items():
                stamp = poll_stamps.get(channel_id)
                age = (
                    (_as_utc(current) - _as_utc(stamp)).total_seconds()
                    if stamp
                    else None
                )
                healthy = age is not None and 0 <= age <= max(300, 3 * interval)
                if (
                    _as_utc(current) - _as_utc(started_at or self.health_started_at)
                ).total_seconds() < 120:
                    healthy = True
                polling[channel_id] = {
                    "healthy": healthy,
                    "last_success_at": _as_utc(stamp).isoformat() if stamp else None,
                    "age_seconds": age,
                }
                if not healthy:
                    health["healthy"] = False
                    health["alerts"].append(
                        {
                            "key": f"poll:{channel_id}",
                            "severity": "error",
                            "message": "channel polling heartbeat stale",
                            "details": {"channel_id": channel_id, "age_seconds": age},
                        }
                    )
            health["polling"] = polling
            last = session.exec(
                select(func.max(col(ArchiveProgress.media_uploaded_at)))
            ).one()
            health.update(
                {
                    "last_upload": str(last) if last else None,
                    "oldest_ready": min(
                        (
                            _as_utc(row.ready_at or row.updated_at).isoformat()
                            for row, _ in rows
                            if row.status == "upload"
                            and (
                                not row.retry_at
                                or _as_utc(row.retry_at) <= _as_utc(current)
                            )
                        ),
                        default=None,
                    ),
                    "waiting_reason": "transfer in progress"
                    if health["lanes"]["upload"]["active_count"]
                    else health["lanes"]["upload"]["waiting_reason"],
                    "active": health["lanes"]["upload"]["active"],
                    "metadata_pause_reasons": self.metadata_pause_reasons,
                    "download_pause_reason": self.download_pause_reason,
                    "reserved_bytes": session.exec(
                        select(
                            func.coalesce(
                                func.sum(col(ArchiveProgress.reserved_bytes)), 0
                            )
                        )
                    ).one(),
                    "worker_seconds": {
                        row.kind: row.seconds
                        for row in session.exec(select(WorkerTiming)).all()
                    },
                }
            )
            return health

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
            if (
                video.publication_failure == "transient"
                and discovery.live_status != video.live_status
            ):
                # A scheduled stream that starts or ends may now have metadata.
                video.publication_retry_at = None
            video.live_status = discovery.live_status
            video.content_type = discovery.content_type
            video.replay_ready = discovery.replay_ready
            video.notification_silent = video.notification_silent or silent
            if discovery.published_precise:
                video.notification_ambiguous = False
                video.publication_attempts = 0
                video.publication_failure = video.publication_error = None
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
            self.archival_enabled
            and discovery.replay_ready
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

    def _migrate_schema(self) -> None:
        """Add columns introduced after the initial SQLite schema."""
        schema = inspect(self.engine)
        subscription_columns = {
            column["name"] for column in schema.get_columns("subscription")
        }
        video_columns = {column["name"] for column in schema.get_columns("video")}
        channel_columns = {column["name"] for column in schema.get_columns("channel")}
        report_checkpoint_columns = {
            column["name"] for column in schema.get_columns("reportcheckpoint")
        }
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
        if "publication_retry_at" not in video_columns:
            statements.append(
                "ALTER TABLE video ADD COLUMN publication_retry_at DATETIME"
            )
        if "publication_attempts" not in video_columns:
            statements.append(
                "ALTER TABLE video ADD COLUMN publication_attempts INTEGER NOT NULL "
                "DEFAULT 0"
            )
        statements.extend(
            f"ALTER TABLE video ADD COLUMN {name} VARCHAR"
            for name in ("publication_failure", "publication_error")
            if name not in video_columns
        )
        if "backfill_completed_at" not in channel_columns:
            statements.append(
                "ALTER TABLE channel ADD COLUMN backfill_completed_at DATETIME"
            )
        if "schedule" not in report_checkpoint_columns:
            statements.append(
                "ALTER TABLE reportcheckpoint ADD COLUMN schedule VARCHAR NOT NULL "
                "DEFAULT ''"
            )
        for model in (ArchiveProgress, ArchiveJob, ArchiveServiceState, Report):
            table = cast(Any, model).__table__
            existing = {column["name"] for column in schema.get_columns(table.name)}
            for column in table.columns:
                if column.name in existing:
                    continue
                default = model.model_fields[column.name].default
                definition = column.type.compile(dialect=self.engine.dialect)
                if column.name == "semantics_version":
                    default = 1
                if default is not None:
                    literal = (
                        f"'{default}'"
                        if isinstance(default, str)
                        else str(int(default))
                    )
                    definition += f" NOT NULL DEFAULT {literal}"
                statements.append(
                    f"ALTER TABLE {table.name} ADD COLUMN {column.name} {definition}"
                )
        with self.engine.begin() as connection:
            for statement in statements:
                connection.execute(text(statement))
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_archiveprogress_media_uploaded_at "
                    "ON archiveprogress(media_uploaded_at)"
                )
            )
        with self._write_lock, Session(self.engine) as session:
            if session.get(SchemaMigration, 2) is not None:
                self._recover_unsubmitted_ziggy(session)
                self._recheck_throttled_item_creation(session)
                if session.get(SchemaMigration, 3) is None:
                    session.add(SchemaMigration(version=3))
                    session.commit()
                return
            for progress in session.exec(select(ArchiveProgress)).all():
                old_status = progress.status
                if progress.media_size and progress.status in {
                    "upload",
                    "uploading",
                    "reconcile_upload",
                }:
                    progress.download_completed_at = progress.updated_at
                    progress.ready_at = progress.updated_at
                if progress.media_verified_at is not None:
                    progress.media_uploaded_at = progress.media_verified_at
                    progress.uploaded_bytes = progress.media_size
                    progress.upload_origin = "historical"
                    progress.upload_receipt = json.dumps(
                        {"migration": 2, "verified_at": str(progress.media_verified_at)}
                    )
                    progress.status = "complete"
                    if progress.working_directory and not progress.cleaned_at:
                        self._queue_job(session, progress.video_id, "cleanup")
                    if progress.webpage_status not in {"queued", "captured"}:
                        self._queue_job(
                            session, progress.video_id, "webpage", deadline=86_400
                        )
                        if old_status in {"ziggy_submitting", "wayback_submitting"} or (
                            old_status == "operator_action"
                            and progress.failure_phase
                            in {"ziggy", "wayback", "webpage_submission"}
                        ):
                            session.flush()
                            job = session.get(
                                ArchiveJob, (progress.video_id, "webpage")
                            )
                            assert job is not None
                            job.status = "failed"
                            job.last_error = progress.last_error or (
                                "historical webpage submission outcome is uncertain"
                            )
                            progress.webpage_status = (
                                progress.webpage_status or "uncertain"
                            )
                            session.add(job)
                elif progress.status in {"uploading", "reconcile_upload"}:
                    progress.status = "reconcile_upload"
                    self._queue_job(
                        session, progress.video_id, "recovery", deadline=86_400
                    )
                elif progress.status == "upload":
                    if (
                        not progress.media_path
                        or not Path(progress.media_path).is_file()
                    ):
                        progress.status = (
                            "download" if progress.frozen_metadata else "metadata"
                        )
                        progress.media_path = None
                    elif progress.media_size:
                        progress.download_completed_at = progress.updated_at
                        progress.ready_at = progress.updated_at
                progress.claimed_at = None
                progress.reserved_bytes = 0
                session.add(progress)
            session.add(SchemaMigration(version=2))
            session.add(SchemaMigration(version=3))
            session.commit()
            self._recover_unsubmitted_ziggy(session)
            self._recheck_throttled_item_creation(session)

    @staticmethod
    def _recover_unsubmitted_ziggy(session: Session) -> None:
        """Restore only legacy failures that prove the TCP connection was refused."""
        if session.get(SchemaMigration, 4) is not None:
            return
        for job in session.exec(
            select(ArchiveJob).where(
                ArchiveJob.kind == "webpage",
                ArchiveJob.status == "failed",
                col(ArchiveJob.claim_token).is_(None),
            )
        ).all():
            progress = session.get(ArchiveProgress, job.video_id)
            assert progress is not None
            if (
                not legacy_ziggy_not_sent(job.last_error)
                or progress.ziggy_receipt
                or progress.wayback_job_id
            ):
                continue
            now = utcnow()
            job.status = "waiting"
            job.attempts = 0
            job.retry_at = job.updated_at = now
            job.deadline_at = now + timedelta(days=1)
            job.error_kind = "network"
            job.error_phase = "webpage-connect"
            job.last_error = (
                "SubmissionNotSentError: Ziggy connection refused before submission"
            )
            progress.webpage_status = "pending"
            session.add_all((job, progress))
        session.add(SchemaMigration(version=4))
        session.commit()

    @classmethod
    def _recheck_throttled_item_creation(cls, session: Session) -> None:
        """Recheck transfers held for reconciliation by a throttled item creation.

        Older releases did not treat IA-S3 ``SlowDown`` at item creation as a
        known failure. The recovery check reads the item and returns only an
        absent item to ``upload``; any remote evidence keeps reconciliation.
        """
        if session.get(SchemaMigration, 5) is not None:
            return
        for progress in session.exec(
            select(ArchiveProgress).where(
                ArchiveProgress.status == "reconcile_upload",
                ArchiveProgress.error_code == "SlowDown",
                ArchiveProgress.error_phase == "item-create",
                col(ArchiveProgress.media_uploaded_at).is_(None),
            )
        ).all():
            cls._restart_recovery_job(session, progress.video_id, delay=0)
        session.add(SchemaMigration(version=5))
        session.commit()


def retry_delay(base: float, attempts: int) -> float:
    """Use increasing retry intervals with bounded jitter and a six-hour cap."""
    return min(
        21_600.0, base * 2 ** min(max(attempts - 1, 0), 12) * random.uniform(0.8, 1.2)
    )


def _archive_incident_message(category: str) -> str:
    return {
        "transient": "Archive service unavailable; retries are coordinated",
        "throttled": "Archive throttled requests; retries are coordinated",
        "authentication": "Archive rejected authentication; check account credentials",
        "item-specific": "Archive denied item access; review item permissions",
        "unclassified": (
            "Archive failure reason is unclassified; retries are coordinated"
        ),
    }[category]


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
