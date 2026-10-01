"""Coordinate discovery and notification workers."""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from core.archival import ArchiveProcessor
from core.backups import run_backup_scheduler
from core.bandwidth import ByteLimiter
from core.config import ContentType, Settings, YouTubeChannelConfig, YouTubeConfig
from core.logging import redact_discord_webhooks
from core.notifications import send_video
from core.reporting import (
    latest_report_boundary,
    next_report_window,
    report_has_activity,
    send_report,
)
from core.state import DiscoveredVideo, ResolvedChannel, StateStore
from core.youtube import YouTubeClient


@dataclass(frozen=True, slots=True)
class MonitoredChannel:
    """Contain the effective monitoring policy for one canonical channel."""

    channel: ResolvedChannel
    cooldown: float
    content_types: frozenset[ContentType]
    retries: int = 0
    retry_delay: float = 0.0


class SagaService:
    """Run Saga's bounded background workflows."""

    def __init__(
        self,
        settings: Settings,
        configurations: Sequence[YouTubeConfig],
        state: StateStore,
        youtube: YouTubeClient,
        archival: ArchiveProcessor | None = None,
        upload_limiter: ByteLimiter | None = None,
    ) -> None:
        self.settings = settings
        self.configurations = configurations
        self.state = state
        self.youtube = youtube
        self.archival = archival
        if self.archival is None and settings.archival_enabled:
            self.archival = ArchiveProcessor(
                settings, state, youtube, upload_limiter=upload_limiter
            )
        self.stop = asyncio.Event()
        self._work_changed = asyncio.Condition()

    async def run(self) -> None:
        """Initialize durable state, run workers, and stop them cooperatively."""
        logger.info("Initializing Saga service")
        self._remove_healthcheck()
        monitored = await self._initialize()
        self.state.reset_interrupted_claims(
            background_deadline=self.settings.background_deadline
        )
        self._write_healthcheck()
        logger.info("Starting workers for {} channel(s)", len(monitored))

        try:
            async with asyncio.TaskGroup() as workers:
                for item in monitored:
                    workers.create_task(
                        self._poll_loop(item), name=f"poll:{item.channel.id}"
                    )
                    workers.create_task(
                        self._reconciliation_loop(item),
                        name=f"history:{item.channel.id}",
                    )
                workers.create_task(self._delivery_loop(), name="discord")
                workers.create_task(self._report_loop(), name="reporting")
                if self.settings.backup.enabled:
                    workers.create_task(
                        run_backup_scheduler(
                            self.state.path, self.settings.backup, self.stop
                        ),
                        name="backup-scheduler",
                    )
                if self.archival is not None:
                    for lane, count in (
                        ("upload", self.settings.upload_workers),
                        ("download", self.settings.download_workers),
                        ("metadata", 1),
                        ("cleanup", 1),
                        ("recovery", 1),
                        ("webpage", 1),
                        ("validation", int(self.settings.validation_enabled)),
                    ):
                        for index in range(count):
                            workers.create_task(
                                self._archive_loop(index, lane), name=f"{lane}:{index}"
                            )
                    workers.create_task(
                        self._transfer_health_loop(), name="transfer-health"
                    )
                await self.stop.wait()
        finally:
            logger.debug("Worker group is stopping")
            self._remove_healthcheck()

    async def _initialize(self) -> tuple[MonitoredChannel, ...]:
        resolved_by_reference: dict[str, ResolvedChannel] = {}
        routes_by_instance: list[
            tuple[YouTubeConfig, tuple[tuple[str, YouTubeChannelConfig], ...]]
        ] = []
        cooldown_by_channel: dict[str, float] = {}
        retries_by_channel: dict[str, int] = {}
        retry_delay_by_channel: dict[str, float] = {}
        content_types_by_channel: dict[str, set[ContentType]] = {}

        for config in self.configurations:
            logger.debug(
                "Initializing YouTube instance {} with {} configured channel(s)",
                config.name,
                len(config.channels),
            )
            routes: list[tuple[str, YouTubeChannelConfig]] = []
            for channel in config.channels:
                reference = channel.reference
                resolved = resolved_by_reference.get(reference)
                if resolved is None:
                    logger.info("Resolving YouTube channel {}", reference)
                    resolved = await self._youtube_request(
                        self.youtube.resolve_channel,
                        config.retries,
                        config.retry_delay,
                        reference,
                    )
                    logger.debug(
                        "Resolved {} to channel {} ({})",
                        reference,
                        resolved.title,
                        resolved.id,
                    )
                    resolved_by_reference[reference] = resolved
                self.state.register_channel(reference, resolved)
                routes.append((resolved.id, channel))
                content_types_by_channel.setdefault(resolved.id, set()).update(
                    channel.content_types
                )
                previous = cooldown_by_channel.get(resolved.id)
                cooldown_by_channel[resolved.id] = (
                    config.cooldown
                    if previous is None
                    else min(previous, config.cooldown)
                )
                retries_by_channel[resolved.id] = max(
                    retries_by_channel.get(resolved.id, 0), config.retries
                )
                previous_delay = retry_delay_by_channel.get(resolved.id)
                retry_delay_by_channel[resolved.id] = (
                    config.retry_delay
                    if previous_delay is None
                    else min(previous_delay, config.retry_delay)
                )
            routes_by_instance.append((config, tuple(routes)))

        self.state.synchronize_subscriptions(routes_by_instance)

        channels = {item.id: item for item in resolved_by_reference.values()}
        for channel in channels.values():
            _, onboarding = self.state.register_channel(channel.url, channel)
            if onboarding:
                logger.info("Establishing notification baseline for {}", channel.title)
                self.state.establish_baseline(channel.id, ())
                recent = await self._youtube_request(
                    self.youtube.recent_videos,
                    retries_by_channel[channel.id],
                    retry_delay_by_channel[channel.id],
                    channel,
                    content_types=content_types_by_channel[channel.id],
                )
                recent = await asyncio.to_thread(
                    self._resolve_publications,
                    channel,
                    recent,
                    None,
                    retries_by_channel[channel.id],
                    retry_delay_by_channel[channel.id],
                )
                self.state.record_discoveries(channel.id, recent, backfill=False)
                logger.info(
                    "Established notification baseline for {} with {} video(s)",
                    channel.title,
                    len(recent),
                )

        return tuple(
            MonitoredChannel(
                channels[channel_id],
                cooldown,
                frozenset(content_types_by_channel[channel_id]),
                retries_by_channel[channel_id],
                retry_delay_by_channel[channel_id],
            )
            for channel_id, cooldown in cooldown_by_channel.items()
        )

    async def _poll_loop(self, monitored: MonitoredChannel) -> None:
        logger.debug(
            "Recent-scan worker started for {} with a {} second interval",
            monitored.channel.title,
            monitored.cooldown,
        )
        while not self.stop.is_set():
            try:
                logger.debug("Scanning recent videos for {}", monitored.channel.title)
                recent = await self._youtube_request(
                    self.youtube.recent_videos,
                    monitored.retries,
                    monitored.retry_delay,
                    monitored.channel,
                    content_types=monitored.content_types,
                )
                added = await asyncio.to_thread(
                    self._record_videos,
                    monitored.channel,
                    recent,
                    False,
                    monitored.retries,
                    monitored.retry_delay,
                )
                if added:
                    logger.info(
                        "Discovered {} new video(s) for {}",
                        added,
                        monitored.channel.title,
                    )
                else:
                    logger.debug(
                        "Recent scan found no new videos for {} ({} inspected)",
                        monitored.channel.title,
                        len(recent),
                    )
                self._write_healthcheck()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Recent scan failed for {}", monitored.channel.title)
            await self._wait(monitored.cooldown)

    async def _reconciliation_loop(self, monitored: MonitoredChannel) -> None:
        channel = monitored.channel
        logger.debug("History worker started for {}", channel.title)
        while not self.stop.is_set():
            try:
                logger.info("Reconciling full history for {}", channel.title)
                discovered = 0

                def record_batch(batch: tuple[DiscoveredVideo, ...]) -> None:
                    nonlocal discovered
                    self._record_videos(
                        channel, batch, True, monitored.retries, monitored.retry_delay
                    )
                    discovered += len(batch)
                    logger.debug(
                        "Reconciled a batch of {} video(s) for {}",
                        len(batch),
                        channel.title,
                    )

                operation = asyncio.create_task(
                    self._youtube_request(
                        self.youtube.scan_history,
                        monitored.retries,
                        monitored.retry_delay,
                        channel,
                        record_batch,
                        content_types=monitored.content_types,
                    ),
                    name=f"history-operation:{channel.id}",
                )
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError:
                    with suppress(Exception):
                        await operation
                    raise
                self.state.mark_backfill_complete(channel.id)
                logger.info(
                    "Completed full history reconciliation for {} ({} video(s))",
                    channel.title,
                    discovered,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("History reconciliation failed for {}", channel.title)
            await self._wait(21_600)

    def _record_videos(
        self,
        channel: ResolvedChannel,
        videos: tuple[DiscoveredVideo, ...],
        backfill: bool,
        retries: int,
        retry_delay: float,
    ) -> int:
        """Queue discoveries before resolving metadata needed only for notifications."""
        needs_detail = self.state.publication_detail_ids(video.id for video in videos)
        if not needs_detail:
            return self.state.record_discoveries(channel.id, videos, backfill=backfill)

        added = self.state.record_discoveries(channel.id, videos, backfill=True)
        resolved = self._resolve_publications(
            channel, videos, needs_detail, retries, retry_delay
        )
        self.state.record_discoveries(channel.id, resolved, backfill=backfill)
        return added

    def _resolve_publications(
        self,
        channel: ResolvedChannel,
        videos: tuple[DiscoveredVideo, ...],
        only: set[str] | None = None,
        retries: int = 0,
        retry_delay: float = 0,
    ) -> tuple[DiscoveredVideo, ...]:
        baseline = self.state.notification_baseline(channel.id)
        if only is None:
            only = self.state.publication_detail_ids(video.id for video in videos)
        resolved: list[DiscoveredVideo] = []
        for video in videos:
            ambiguous_date = (
                video.published_at is not None
                and not video.published_precise
                and video.published_at.date() == baseline.date()
            )
            needs_detail = video.id in only and (
                ambiguous_date or video.published_at is None
            )
            if not needs_detail:
                resolved.append(video)
                continue
            try:
                logger.debug("Resolving exact publication time for video {}", video.id)
                details = self._youtube_request_sync(
                    self.youtube.video_metadata, retries, retry_delay, video.id
                )
            except Exception as error:
                logger.warning(
                    "Could not resolve an exact publication time for {}: {}: {}",
                    video.id,
                    type(error).__name__,
                    error,
                )
                resolved.append(video)
                continue
            resolved.append(
                replace(
                    video,
                    published_at=details.published_at or video.published_at,
                    published_precise=details.published_precise,
                )
            )
        return tuple(resolved)

    async def _youtube_request(
        self,
        operation: Any,
        retries: int,
        retry_delay: float,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run one blocking YouTube request with its configured retry policy."""
        for attempt in range(retries + 1):
            try:
                return await asyncio.to_thread(operation, *args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= retries:
                    raise
                logger.warning(
                    "YouTube request failed; retrying attempt {} of {} in {} seconds",
                    attempt + 2,
                    retries + 1,
                    retry_delay,
                )
                await self._wait(retry_delay)
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _youtube_request_sync(
        operation: Any, retries: int, retry_delay: float, *args: Any
    ) -> Any:
        for attempt in range(retries + 1):
            try:
                return operation(*args)
            except Exception:
                if attempt >= retries:
                    raise
                time.sleep(retry_delay)
        raise AssertionError("unreachable")  # pragma: no cover

    async def _delivery_loop(self) -> None:
        while not self.stop.is_set():
            delivery = self.state.claim_delivery()
            if delivery is None:
                await self._wait(1)
                continue
            assert delivery.id is not None
            logger.debug(
                "Claimed Discord delivery {} for video {}",
                delivery.id,
                delivery.video_id,
            )
            try:
                message_id = await send_video(
                    delivery.webhook_url,
                    delivery.video_id,
                    self.settings.notification_timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                detail = _safe_error(error, delivery.webhook_url)
                retrying = self.state.fail_delivery(delivery.id, detail)
                log = logger.warning if retrying else logger.error
                log(
                    "Discord delivery {} for video {}: {}",
                    "will retry" if retrying else "failed permanently",
                    delivery.video_id,
                    detail,
                )
            else:
                self.state.complete_delivery(delivery.id, message_id)
                logger.info("Delivered video {} to Discord", delivery.video_id)

    async def _archive_loop(self, worker_index: int, lane: str = "metadata") -> None:
        # Discovery and history use asyncio's default pool. Each archive worker
        # owns a thread so those requests cannot consume upload capacity.
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"saga-{lane}-{worker_index}"
        ) as executor:
            await self._archive_worker_loop(worker_index, lane, executor)

    async def _archive_worker_loop(
        self, worker_index: int, lane: str, executor: ThreadPoolExecutor
    ) -> None:
        assert self.archival is not None
        claim_count = worker_index
        while not self.stop.is_set():
            if lane in {"upload", "download", "metadata"}:
                work = self.state.claim_archive(
                    prefer_recent=claim_count % 4 != 3,
                    lane=lane,
                    buffer=self.settings.upload_ready_buffer,
                    free_bytes=shutil.disk_usage(self.settings.data_path).free
                    if lane == "download"
                    else None,
                    minimum_free_bytes=self.settings.minimum_free_bytes,
                    unknown_download_bytes=self.settings.unknown_download_bytes,
                )
            else:
                work = self.state.claim_background(
                    lane, max_attempts=self.settings.background_max_attempts
                )
            claim_count += 1
            if work is None:
                async with self._work_changed:
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._work_changed.wait(), timeout=1)
                continue
            logger.debug(
                "Archive worker {} claimed video {}", worker_index, work.video.id
            )
            operation = asyncio.create_task(
                self._archive_operation(executor, work),
                name=f"archive-operation:{work.video.id}",
            )
            started = time.monotonic()
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                with suppress(Exception):
                    await operation
                raise
            except Exception:
                logger.exception("Archive worker failed for {}", work.video.id)
            finally:
                self.state.record_worker_time(lane, time.monotonic() - started)
                async with self._work_changed:
                    self._work_changed.notify_all()

    async def _archive_operation(self, executor: ThreadPoolExecutor, work: Any) -> None:
        assert self.archival is not None
        await asyncio.get_running_loop().run_in_executor(
            executor, self.archival.process, work
        )

    async def _transfer_health_loop(self) -> None:
        """Publish transfer progress separately from the polling heartbeat."""
        while not self.stop.is_set():
            health = self.state.transfer_health(
                stall_timeout=self.settings.transfer_stall_timeout
            )
            root = self.settings.data_path / "downloads"
            health["staging_bytes"] = await asyncio.to_thread(staging_usage, root)
            path = self.settings.data_path / "transfers.json"
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(health, sort_keys=True), encoding="utf-8")
            temporary.replace(path)
            if not health["healthy"]:
                logger.error("Upload queue stalled: {}", health)
            await self._wait(30)

    async def _report_loop(self) -> None:
        logger.debug(
            "Reporting worker started with cron schedule {} UTC",
            self.settings.report_schedule,
        )
        while not self.stop.is_set():
            try:
                now = datetime.now(UTC)
                initial_end = latest_report_boundary(
                    now,
                    self.settings.report_schedule,
                    self.settings.report_finalization_grace,
                )
                checkpoint, initialized = self.state.initialize_report_checkpoint(
                    initial_end, self.settings.report_schedule
                )
                if initialized:
                    logger.info(
                        "Initialized report schedule at {} without sending history",
                        initial_end,
                    )
                else:
                    window = next_report_window(
                        checkpoint.last_window_end,
                        now,
                        self.settings.report_schedule,
                        self.settings.report_finalization_grace,
                    )
                    if window is not None:
                        report = self.state.create_report_snapshot(
                            window.start, window.end, now
                        )
                        logger.info(
                            "Generated archival report for {} to {}",
                            window.start,
                            window.end,
                        )

                report = self.state.claim_report(now)
                if report is None:
                    await self._wait(1)
                    continue
                assert report.id is not None
                if not report_has_activity(report):
                    self.state.complete_report(report.id, "suppressed", None)
                    logger.info(
                        "Archival report {} to {} had no activity or outstanding work",
                        report.window_start,
                        report.window_end,
                    )
                    continue
                webhook_url = self.settings.report_discord_webhook_url
                if webhook_url is None:
                    logger.info(
                        "YouTube archival report {} to {}: {} discovered, {} media "
                        "uploaded, {} unfinished, {} awaiting replay, {} unavailable, "
                        "{} requiring operator action",
                        report.window_start,
                        report.window_end,
                        report.discovered_videos
                        + report.discovered_shorts
                        + report.discovered_livestreams,
                        report.media_archived,
                        report.unfinished_workflows,
                        report.awaiting_replay,
                        report.unavailable,
                        report.operator_action,
                    )
                    self.state.complete_report(report.id, "logged", None)
                    continue
                try:
                    message_id = await send_report(
                        report, webhook_url, self.settings.notification_timeout
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    detail = _safe_error(error, webhook_url)
                    delay = self.state.fail_report(report.id, detail)
                    logger.warning(
                        "Archival report delivery will retry in {} seconds: {}",
                        delay,
                        detail,
                    )
                else:
                    self.state.complete_report(report.id, "sent", message_id)
                    logger.info(
                        "Delivered archival report {} to {}",
                        report.window_start,
                        report.window_end,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Reporting worker failed; continuing independently")
                await self._wait(1)

    async def _wait(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)

    def request_stop(self) -> None:
        """Request cooperative service shutdown."""
        logger.debug("Setting the cooperative shutdown event")
        self.stop.set()

    def _write_healthcheck(self) -> None:
        path = self.settings.healthcheck_file
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        logger.trace("Refreshed healthcheck file {}", path)

    def _remove_healthcheck(self) -> None:
        path = self.settings.healthcheck_file
        if path is not None:
            path.unlink(missing_ok=True)
            logger.trace("Removed healthcheck file {}", path)


def _safe_error(error: Exception, secret: str) -> str:
    detail = f"{type(error).__name__}: {error}"
    return redact_discord_webhooks(detail.replace(secret, "<discord webhook>"))[:2000]


def staging_usage(root: Any) -> int:
    """Measure staging while allowing the cleanup worker to remove directories."""
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total
