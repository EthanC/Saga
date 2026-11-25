"""Shared, cached Archive task-backlog readings and the retry pacing they imply."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock

from archivist import (
    AuthenticationError,
    InternetArchiveTaskSummary,
    RateLimitError,
    ServiceError,
)
from loguru import logger

from core.archive_retry import retry_hint

MAX_BACKLOG_DELAY = 3600.0
DRAIN_WINDOW = 3600.0
CREDENTIAL_PHASES = frozenset({"csrf", "login", "upload-key"})


@dataclass(frozen=True, slots=True)
class TaskBacklog:
    """Describe one account task summary and the retry delay it implies."""

    queued: int
    running: int
    error: int
    paused: int
    sampled_at: datetime
    retry_seconds: float

    @property
    def outstanding(self) -> int:
        """Return tasks that have not finished: queued plus running."""
        return self.queued + self.running


class TaskSource:
    """Supply no backlog reading and ignore write outcomes."""

    def read(
        self,
        fetch: Callable[[], InternetArchiveTaskSummary],
        *,
        limit: int,
        base_delay: float,
    ) -> TaskBacklog | None:
        return None

    def latest(self) -> TaskBacklog | None:
        return None

    def note_write(self, tasks: int) -> None:
        return None

    def throttle_hint(self) -> float:
        return 0.0


class PresetTasks(TaskSource):
    """Return a reading taken by the managing process without another request."""

    def __init__(self, backlog: TaskBacklog | None) -> None:
        self.backlog = backlog

    def read(
        self,
        fetch: Callable[[], InternetArchiveTaskSummary],
        *,
        limit: int,
        base_delay: float,
    ) -> TaskBacklog | None:
        return self.backlog


@dataclass(frozen=True, slots=True)
class _Sample:
    at: float
    outstanding: int
    added: int


class TaskSummaryCache(TaskSource):
    """Share one task-summary request per TTL across every worker in a process."""

    def __init__(
        self,
        ttl: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        listener: Callable[[TaskBacklog], None] | None = None,
    ) -> None:
        self.ttl = ttl
        self.clock = clock
        self.listener = listener
        self._lock = Lock()
        self._backlog: TaskBacklog | None = None
        self._read_at = 0.0
        self._failed_at: float | None = None
        self._samples: deque[_Sample] = deque()
        self._added = 0
        self._throttled_until: datetime | None = None
        self._errors = 0

    def read(
        self,
        fetch: Callable[[], InternetArchiveTaskSummary],
        *,
        limit: int,
        base_delay: float,
    ) -> TaskBacklog | None:
        """Return a reading no older than the TTL, refreshing it at most once."""
        with self._lock:
            now = self.clock()
            if self._backlog is not None and now - self._read_at < self.ttl:
                return self._backlog
            if self._failed_at is not None and now - self._failed_at < self.ttl:
                return self._stale(now)
            try:
                summary = fetch()
            except ServiceError as error:
                if isinstance(error, AuthenticationError) or (
                    error.request_phase in CREDENTIAL_PHASES
                ):
                    raise
                self._failed_at = now
                if isinstance(error, RateLimitError):
                    self._throttle(error)
                logger.warning(
                    "Archive task-summary read failed; sending without it: {}: {}",
                    type(error).__name__,
                    error,
                )
                return self._stale(now)
            self._failed_at = None
            self._samples.append(_Sample(now, summary.outstanding, self._added))
            while len(self._samples) > 2 and now - self._samples[1].at >= DRAIN_WINDOW:
                self._samples.popleft()
            self._read_at = now
            self._backlog = TaskBacklog(
                summary.queued,
                summary.running,
                summary.error,
                summary.paused,
                datetime.now(UTC),
                self._retry_seconds(summary.outstanding, limit, base_delay),
            )
            if summary.error != self._errors:
                if summary.error:
                    logger.warning(
                        "Internet Archive reports {} failed account tasks; "
                        "they need attention on the Archive",
                        summary.error,
                    )
                self._errors = summary.error
            if self.listener is not None:
                self.listener(self._backlog)
            return self._backlog

    def latest(self) -> TaskBacklog | None:
        with self._lock:
            return self._backlog

    def note_write(self, tasks: int) -> None:
        """Count tasks Saga queued so the drain rate excludes its own additions."""
        with self._lock:
            self._added += tasks

    def throttle_hint(self) -> float:
        """Return the remaining Retry-After from a throttled task-summary read."""
        with self._lock:
            if self._throttled_until is None:
                return 0.0
            return max(0.0, (self._throttled_until - datetime.now(UTC)).total_seconds())

    def _throttle(self, error: RateLimitError) -> None:
        now = datetime.now(UTC)
        hint = retry_hint(error, now)
        if hint:
            self._throttled_until = datetime.fromtimestamp(now.timestamp() + hint, UTC)

    def _stale(self, now: float) -> TaskBacklog | None:
        if self._backlog is not None and now - self._read_at < 2 * self.ttl:
            return self._backlog
        return None

    def _retry_seconds(self, outstanding: int, limit: int, base_delay: float) -> float:
        """Estimate how long the backlog above the limit takes to drain."""
        if len(self._samples) < 2:
            return base_delay
        first, last = self._samples[0], self._samples[-1]
        drained = first.outstanding + (last.added - first.added) - last.outstanding
        rate = drained / (last.at - first.at) if last.at > first.at else 0.0
        if rate <= 0:
            return MAX_BACKLOG_DELAY
        excess = max(1, outstanding - limit + 1)
        return min(MAX_BACKLOG_DELAY, max(base_delay, excess / rate))
