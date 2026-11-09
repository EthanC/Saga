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
from functools import partial
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
from core.shutdown import CooperativeStop, StopController
from core.staging import maintain_partials
from core.state import DiscoveredVideo, ResolvedChannel, StateStore, utcnow
from core.storage import data_usage, remaining_data_bytes
from core.youtube import YouTubeClient
from core.youtube_errors import cookie_fingerprint, is_permanent_video_error

STARTUP_STALL_TIMEOUT = 300


@dataclass(frozen=True, slots=True)
class MonitoredChannel:
    """Contain the effective monitoring policy for one canonical channel."""

    channel: ResolvedChannel
    cooldown: float
    content_types: frozenset[ContentType]
    retries: int = 0
    retry_delay: float = 0.0
    reconciliation_interval: float = 21_600.0


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
        stop_controller: StopController | None = None,
    ) -> None:
        self.settings = settings
        self.configurations = configurations
        self.state = state
        self.youtube = youtube
        self.stop_controller = stop_controller or StopController(
            settings.shutdown_timeout
        )
        if isinstance(youtube, YouTubeClient):
            youtube.stop_controller = self.stop_controller
        self.archival = archival
        if self.archival is None and settings.archival_enabled:
            self.archival = ArchiveProcessor(
                settings,
                state,
                youtube,
                upload_limiter=upload_limiter,
                stop_controller=self.stop_controller,
            )
        self.stop = asyncio.Event()
        self._work_changed = asyncio.Condition()
        self._partial_health: dict[str, Any] = {}
        self.started_at = utcnow()
        self._channel_intervals: dict[str, float] = {}
        self._health_alerts: dict[str, tuple[str, str, float]] = {}
        self._initializing = False
        self._startup_progress = time.monotonic()
        self._publication_started: float | None = None
        self._poll_slots = asyncio.Semaphore(2)
        self._history_slots = asyncio.Semaphore(1)
        self._staging_bytes: int | None = None
        self._youtube_cookie_generation: str | None = None

    async def run(self) -> None:
        """Initialize durable state, run workers, and stop them cooperatively."""
        logger.info("Initializing Saga service")
        self._remove_healthcheck()
        self._initializing = True
        self._startup_progress = time.monotonic()
        try:
            async with asyncio.TaskGroup() as workers:
                workers.create_task(self._health_loop(), name="health")
                try:
                    monitored = await self._initialize()
                    self.started_at = utcnow()
                    self._channel_intervals = {
                        item.channel.id: item.cooldown for item in monitored
                    }
                    self.state.reset_interrupted_claims(
                        background_deadline=self.settings.background_deadline
                    )
                    self._initializing = False
                    logger.info("Starting workers for {} channel(s)", len(monitored))

                    for item in monitored:
                        workers.create_task(
                            self._poll_loop(item), name=f"poll:{item.channel.id}"
                        )
                        workers.create_task(
                            self._reconciliation_loop(item),
                            name=f"history:{item.channel.id}",
                        )
                    workers.create_task(
                        self._publication_loop(monitored), name="publication"
                    )
                    workers.create_task(self._delivery_loop(), name="discord")
                    workers.create_task(self._report_loop(), name="reporting")
                    if self.settings.backup.enabled:
                        workers.create_task(
                            run_backup_scheduler(
                                self.state.path,
                                self.settings.backup,
                                self.stop,
                                stop_controller=self.stop_controller,
                            ),
                            name="backup-scheduler",
                        )
                    if self.archival is not None:
                        workers.create_task(
                            self._cookie_watch_loop(), name="cookie-watch"
                        )
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
                                    self._archive_loop(index, lane),
                                    name=f"{lane}:{index}",
                                )
                        workers.create_task(
                            self._partial_maintenance_loop(), name="partial-maintenance"
                        )
                    await self.stop.wait()
                finally:
                    self.request_stop()
        except* CooperativeStop:
            pass
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
        reconciliation_by_channel: dict[str, float] = {}
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
                self._startup_progress = time.monotonic()
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
                reconciliation_by_channel[resolved.id] = min(
                    reconciliation_by_channel.get(
                        resolved.id, config.reconciliation_interval
                    ),
                    config.reconciliation_interval,
                )
            routes_by_instance.append((config, tuple(routes)))

        self.state.synchronize_subscriptions(routes_by_instance)

        channels = {item.id: item for item in resolved_by_reference.values()}
        for channel in channels.values():
            _, onboarding = self.state.register_channel(channel.url, channel)
            if onboarding:
                logger.info("Establishing notification baseline for {}", channel.title)
                self.state.establish_baseline(channel.id, ())

        return tuple(
            MonitoredChannel(
                channels[channel_id],
                cooldown,
                frozenset(content_types_by_channel[channel_id]),
                retries_by_channel[channel_id],
                retry_delay_by_channel[channel_id],
                reconciliation_by_channel[channel_id],
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
                async with self._poll_slots:
                    recent = await self._youtube_request(
                        self.youtube.recent_videos,
                        monitored.retries,
                        monitored.retry_delay,
                        monitored.channel,
                        content_types=monitored.content_types,
                    )
                    added = self.state.record_discoveries(
                        monitored.channel.id, recent, backfill=False
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
            except asyncio.CancelledError:
                raise
            except CooperativeStop:
                return
            except Exception:
                logger.exception("Recent scan failed for {}", monitored.channel.title)
            await self._wait(monitored.cooldown)

    async def _reconciliation_loop(self, monitored: MonitoredChannel) -> None:
        channel = monitored.channel
        interval = monitored.reconciliation_interval
        logger.debug(
            "History worker started for {} with a {} second interval",
            channel.title,
            interval,
        )
        completed_at = self.state.backfill_completed_at(channel.id)
        if completed_at is not None:
            remaining = interval - (utcnow() - completed_at).total_seconds()
            if remaining > 0:
                logger.info(
                    "Full history for {} was reconciled at {}; next reconciliation "
                    "in {:.0f} seconds",
                    channel.title,
                    completed_at,
                    remaining,
                )
                await self._wait(remaining)
        while not self.stop.is_set():
            try:
                logger.info("Reconciling full history for {}", channel.title)
                discovered = 0

                def record_batch(batch: tuple[DiscoveredVideo, ...]) -> None:
                    nonlocal discovered
                    self.state.record_discoveries(channel.id, batch, backfill=True)
                    discovered += len(batch)
                    logger.debug(
                        "Reconciled a batch of {} video(s) for {}",
                        len(batch),
                        channel.title,
                    )

                operation = asyncio.create_task(
                    self._scan_history(monitored, record_batch),
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
            except CooperativeStop:
                return
            except Exception:
                logger.exception("History reconciliation failed for {}", channel.title)
            await self._wait(interval)

    async def _scan_history(
        self, monitored: MonitoredChannel, record_batch: Any
    ) -> None:
        async with self._history_slots:
            await self._youtube_request(
                self.youtube.scan_history,
                monitored.retries,
                monitored.retry_delay,
                monitored.channel,
                record_batch,
                content_types=monitored.content_types,
            )

    async def _publication_loop(self, monitored: Sequence[MonitoredChannel]) -> None:
        """Resolve durable notification work without delaying channel polling."""
        channels = {item.channel.id: item for item in monitored}
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="saga-publication"
        ) as executor:
            while not self.stop.is_set():
                work = self.state.claim_publication(channels)
                if work is None:
                    await self._wait(1)
                    continue
                channel_id, video = work
                item = channels[channel_id]
                self._publication_started = time.monotonic()
                try:
                    resolved = await self._run_blocking(
                        executor,
                        self._resolve_publications,
                        item.channel,
                        (video,),
                        None,
                        item.retries,
                        item.retry_delay,
                    )
                    self.state.record_discoveries(channel_id, resolved, backfill=True)
                except CooperativeStop:
                    return
                finally:
                    self._publication_started = None

    async def _run_blocking(
        self,
        executor: ThreadPoolExecutor | None,
        operation: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        future = asyncio.get_running_loop().run_in_executor(
            executor, partial(operation, *args, **kwargs)
        )
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            with suppress(Exception):
                await future
            raise

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
            self.stop_controller.check()
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
            except CooperativeStop:
                raise
            except Exception as error:
                detail = f"{type(error).__name__}: {error}"
                permanent = is_permanent_video_error(str(error))
                retry_at = self.state.record_publication_failure(
                    video.id, detail, permanent=permanent
                )
                if permanent:
                    logger.warning(
                        "Video {} is permanently unavailable; no longer resolving "
                        "its publication time: {}",
                        video.id,
                        detail,
                    )
                else:
                    logger.warning(
                        "Could not resolve an exact publication time for {}; "
                        "retrying after {}: {}",
                        video.id,
                        retry_at,
                        detail,
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
            self.stop_controller.check()
            try:
                return await self._run_blocking(None, operation, *args, **kwargs)
            except asyncio.CancelledError:
                raise
            except CooperativeStop:
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

    def _youtube_request_sync(
        self, operation: Any, retries: int, retry_delay: float, *args: Any
    ) -> Any:
        for attempt in range(retries + 1):
            self.stop_controller.check()
            try:
                return operation(*args)
            except CooperativeStop:
                raise
            except Exception as error:
                if attempt >= retries or is_permanent_video_error(str(error)):
                    raise
                self.stop_controller.wait(retry_delay)
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
                    min(
                        self.settings.notification_timeout,
                        self.settings.shutdown_timeout - 5,
                    ),
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
                data_budget = (
                    await self._run_blocking(
                        executor,
                        remaining_data_bytes,
                        self.settings.data_path,
                        self.settings.max_data_bytes,
                    )
                    if lane == "download" and self.settings.max_data_bytes
                    else None
                )
                work = self.state.claim_archive(
                    prefer_recent=claim_count % 4 != 3,
                    lane=lane,
                    archive_retry_base=self.settings.archive_retry_delay,
                    youtube_cookie_generation=self._youtube_cookie_generation,
                    buffer=self.settings.upload_ready_buffer,
                    free_bytes=shutil.disk_usage(self.settings.download_root).free
                    if lane == "download"
                    else None,
                    minimum_free_bytes=self.settings.minimum_free_bytes,
                    data_budget_bytes=data_budget,
                    unknown_download_bytes=self.settings.unknown_download_bytes,
                    download_root=self.settings.download_root,
                    pause_fresh_downloads=self.settings.partial_retention_mode
                    != "observe"
                    and self._partial_health.get("waiting_reason")
                    == "protected partials exceed retention target",
                    dispatch_class=("recent", "resume", "recent", "backfill")[
                        claim_count % 4
                    ]
                    if lane == "download"
                    else None,
                )
            else:
                work = self.state.claim_background(
                    lane,
                    max_attempts=self.settings.background_max_attempts,
                    archive_retry_base=self.settings.archive_retry_delay,
                )
            if work is None:
                async with self._work_changed:
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._work_changed.wait(), timeout=1)
                continue
            claim_count += 1
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

    async def _health_loop(self) -> None:
        """Publish a bounded startup or pipeline verdict independently of scans."""
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="saga-health"
        ) as executor:
            while not self.stop.is_set():
                try:
                    generated_at = None
                    if self._initializing:
                        healthy = (
                            time.monotonic() - self._startup_progress
                            <= STARTUP_STALL_TIMEOUT
                        )
                    else:
                        health = await self._run_blocking(
                            executor, self._health_snapshot
                        )
                        path = self.settings.data_path / "transfers.json"
                        temporary = path.with_suffix(".tmp")
                        temporary.write_text(
                            json.dumps(health, sort_keys=True), encoding="utf-8"
                        )
                        temporary.replace(path)
                        self._emit_health_alerts(health["alerts"])
                        healthy = health["healthy"]
                        generated_at = health["generated_at"]
                    self._write_healthcheck(healthy=healthy, generated_at=generated_at)
                except OSError as error:
                    self._emit_health_alerts(
                        [
                            {
                                "key": "publisher",
                                "severity": "error",
                                "message": "health publisher failed",
                                "details": {"cause": type(error).__name__},
                            }
                        ],
                        snapshot_complete=False,
                    )
                await self._wait(30)

    def _health_snapshot(self) -> dict[str, Any]:
        health = self.state.transfer_health(
            stall_timeout=self.settings.transfer_stall_timeout,
            started_at=self.started_at,
            upload_workers=self.settings.upload_workers,
            download_workers=self.settings.download_workers,
            channel_intervals=self._channel_intervals,
            include_archive=self.archival is not None,
            youtube_cookie_generation=self._cookie_generation(),
        )
        publication_started = self._publication_started
        if publication_started is not None and (
            time.monotonic() - publication_started
            > self.settings.transfer_stall_timeout
        ):
            health["healthy"] = False
            health["alerts"].append(
                {
                    "key": "publication:stall",
                    "severity": "error",
                    "message": "notification metadata worker stalled",
                    "details": {},
                }
            )
        if self.archival is not None:
            root = self.settings.download_root
            used = (
                data_usage(self.settings.data_path)
                if self.settings.max_data_bytes
                else None
            )
            health["storage"] = {
                "path": str(self.settings.data_path),
                "used_bytes": used,
                "max_bytes": self.settings.max_data_bytes,
                "remaining_bytes": max(self.settings.max_data_bytes - used, 0)
                if used is not None
                else None,
            }
            health["staging_bytes"] = self._staging_bytes
            health["staging"] = {
                "path": str(root),
                "filesystem_id": root.stat().st_dev,
                "available_bytes": shutil.disk_usage(root).free,
                "reserved_bytes": health["reserved_bytes"],
                "minimum_free_bytes": self.settings.minimum_free_bytes,
                "waiting_reason": health["download_pause_reason"],
            }
            health["partials"] = self._partial_health
        return health

    def _emit_health_alerts(
        self, alerts: list[dict[str, Any]], *, snapshot_complete: bool = True
    ) -> None:
        if snapshot_complete and self.settings.archival_enabled:
            outcome = next(
                (alert for alert in alerts if alert["key"] == "webpage:outcome"), None
            )
            for event in self.state.webpage_outcome_notifications(outcome):
                if event.get("recovery"):
                    logger.info("{}: {}", event["message"], event["details"])
                else:
                    logger.error("{}: {}", event["message"], event["details"])
        for event in self.state.archive_service_notifications(
            stall_timeout=self.settings.transfer_stall_timeout
        ):
            if event.get("recovery"):
                logger.info("{}: {}", event["message"], event["details"])
            else:
                log = logger.error if event["severity"] == "error" else logger.warning
                log("{}: {}", event["message"], event["details"])
        now = time.monotonic()
        active = {alert["key"] for alert in alerts}
        for alert in alerts:
            key = alert["key"]
            if key in {"archive:service", "webpage:outcome"}:
                continue
            message = alert["message"]
            severity = alert["severity"]
            previous = self._health_alerts.get(key)
            if (
                previous is None
                or previous[0] != message
                or previous[1] != severity
                or now - previous[2] >= 900
            ):
                log = logger.error if severity == "error" else logger.warning
                log("{}: {}", message, alert["details"])
                self._health_alerts[key] = (message, severity, now)
        for key in tuple(self._health_alerts):
            if key not in active:
                message, _, _ = self._health_alerts.pop(key)
                logger.info("Pipeline condition recovered: {}", message)

    def _cookie_generation(self) -> str:
        path = self.settings.youtube_cookies_path
        return cookie_fingerprint(path.read_text(encoding="utf-8") if path else None)

    async def _cookie_watch_loop(self) -> None:
        while not self.stop.is_set():
            try:
                generation = await asyncio.to_thread(self._cookie_generation)
                self._youtube_cookie_generation = generation
                count = await asyncio.to_thread(
                    self.state.refresh_youtube_access, generation
                )
                if count:
                    logger.info(
                        "Scheduled {} earlier YouTube access failures for recheck",
                        count,
                    )
                    async with self._work_changed:
                        self._work_changed.notify_all()
            except OSError as error:
                logger.warning(
                    "YouTube cookie watcher could not read the session ({})",
                    type(error).__name__,
                )
            await self._wait(30)

    async def _partial_maintenance_loop(self) -> None:
        while not self.stop.is_set():
            try:
                self._partial_health = await asyncio.to_thread(
                    maintain_partials,
                    self.state,
                    self.settings,
                    youtube=self.youtube,
                    controller=self.stop_controller,
                )
                self._staging_bytes = await asyncio.to_thread(
                    staging_usage, self.settings.download_root
                )
            except CooperativeStop:
                return
            except Exception:
                logger.exception("Partial inventory maintenance failed")
            await self._wait(60)

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
                        report,
                        webhook_url,
                        min(
                            self.settings.notification_timeout,
                            self.settings.shutdown_timeout - 5,
                        ),
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
        self.stop_controller.request()

    def _write_healthcheck(
        self, *, healthy: bool = True, generated_at: str | None = None
    ) -> None:
        path = self.settings.healthcheck_file
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(
            json.dumps(
                {
                    "version": 1,
                    "generated_at": generated_at or utcnow().isoformat(),
                    "healthy": healthy,
                    "phase": "initializing" if self._initializing else "running",
                }
            ),
            encoding="utf-8",
        )
        temporary.replace(path)
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
