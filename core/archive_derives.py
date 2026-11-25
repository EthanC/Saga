"""Paced derive requests and their confirmation for items uploaded without one."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol

from archivist import (
    AuthenticationError,
    InternetArchiveItem,
    InternetArchiveItemFileSource,
    InternetArchiveTaskNotFoundError,
    InternetArchiveTaskState,
    InternetArchiveTaskStatus,
    InternetArchiveTaskSubmission,
    InternetArchiveTaskSummary,
    RateLimitError,
    ServiceError,
)
from loguru import logger

from core.archive_errors import failure_facts
from core.archive_retry import retry_hint
from core.archive_tasks import TaskBacklog, TaskSource
from core.config import Settings
from core.logging import redact_diagnostics
from core.shutdown import CooperativeStop, StopController
from core.state import (
    ArchiveProgress,
    ArchiveWork,
    StateStore,
    _as_utc,
    retry_delay,
    utcnow,
)

DERIVE_COMMAND = "derive.php"
# Catalog reads per worker pass, so confirmation adds little Archive load.
CHECK_BATCH = 10
RECONCILE_BATCH = 5
# Catalog ``wait_admin`` values for a running and a staff-held task.
WAIT_RUNNING = 1
WAIT_PAUSED = 9
# Derive states that wait on the Archive and can stall without a task ID.
WATCHED = frozenset({"requested", "running", "unknown"})
# Why a derive stalled: no task ID, a task the Tasks API lost, or a task
# that finished without derivative files.
ITEM_ONLY = "item"
LOST = "lost"
NO_OUTPUT = "no_output"
LIVE_STATES = {
    InternetArchiveTaskState.QUEUED: "requested",
    InternetArchiveTaskState.RUNNING: "running",
    InternetArchiveTaskState.PAUSED: "paused",
}


class DeriveBackend(Protocol):
    """Reach the Archive's derive, task, and item APIs through Archivist."""

    tasks: TaskSource

    def task_summary(self) -> InternetArchiveTaskSummary: ...
    def queue_derive(self, identifier: str) -> InternetArchiveTaskSubmission: ...
    def task_status(self, task_id: int) -> InternetArchiveTaskStatus: ...
    def get_item(self, identifier: str) -> InternetArchiveItem: ...


class DeriveDispatcher:
    """Request derives while the account backlog has room, then confirm them."""

    def __init__(
        self,
        settings: Settings,
        state: StateStore,
        gateway: DeriveBackend,
        stop_controller: StopController | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.gateway = gateway
        self.stop_controller = stop_controller or StopController()
        self._settled: bool | None = None
        # When the backlog was last seen falling to the target or below. It
        # lives in memory, so a restart restarts it at the first reading.
        self._settled_since: datetime | None = None
        self._above_target = False

    def room(self) -> int:
        """Return how many derives fit under the backlog target right now."""
        target = self.settings.archive_derive_backlog_target
        if self.settings.archive_derive_paused or target <= 0:
            return 0
        backlog = self._read_backlog()
        if backlog is None:
            return 0
        limit = self.settings.archive_task_queue_limit
        if 0 < limit <= backlog.outstanding:
            return 0
        return max(0, target - backlog.outstanding)

    def submit_round(self) -> int:
        """Request derives, oldest upload first, until the room is used."""
        room = self.room()
        calls = 0
        while calls < room:
            if calls:
                self.stop_controller.wait(self.settings.archive_derive_submit_interval)
            self.stop_controller.check()
            # Preservation comes first: a due upload keeps derives waiting.
            if self.state.uploads_due():
                break
            work = self.state.claim_derive("submit")
            if work is None:
                break
            calls += 1
            if not self._guarded(work, self._submit):
                break
        return calls

    def _read_backlog(self) -> TaskBacklog | None:
        """Read the shared backlog and note when it falls to the target."""
        target = self.settings.archive_derive_backlog_target
        backlog = self.gateway.tasks.read(
            self.gateway.task_summary,
            limit=target,
            base_delay=self.settings.archive_retry_delay,
        )
        if backlog is not None:
            settled = backlog.outstanding <= target
            if settled and (self._above_target or self._settled_since is None):
                self._settled_since = utcnow()
            self._above_target = not settled
        return backlog

    def check_round(self) -> None:
        """Confirm a bounded batch of requested derives, then adopt older items."""
        self._settled = None
        for mode, batch in (("check", CHECK_BATCH), ("reconcile", RECONCILE_BATCH)):
            for _ in range(batch):
                self.stop_controller.check()
                work = self.state.claim_derive(mode)
                if work is None:
                    break
                if not self._guarded(work, self._check):
                    return

    def _guarded(self, work: ArchiveWork, operation: Any) -> bool:
        """Release the claim unchanged if the operation stops or fails unexpectedly."""
        try:
            return operation(work)
        except CooperativeStop:
            self.state.finish_derive(work, job_status="waiting", retry_at=None)
            raise
        except Exception as error:
            logger.exception(
                "Derive work for {} failed unexpectedly", work.progress.ia_identifier
            )
            self.state.finish_derive(
                work,
                job_status="waiting",
                retry_at=self._backoff(work, error),
                error=_error_text(error),
                **failure_facts(error),
            )
            return False

    def _submit(self, work: ArchiveWork) -> bool:
        identifier = work.progress.ia_identifier
        assert identifier is not None
        try:
            submission = self.gateway.queue_derive(identifier)
        except AuthenticationError as error:
            logger.error(
                "Archive rejected the derive request for {}; check account "
                "credentials: {}",
                identifier,
                _error_text(error),
            )
            self.state.finish_derive(
                work,
                job_status="failed",
                retry_at=None,
                derive_status="failed",
                error=_error_text(error),
                **failure_facts(error),
            )
            return False
        except RateLimitError as error:
            logger.warning(
                "Archive throttled the derive request for {}: {}",
                identifier,
                _error_text(error),
            )
            self.state.finish_derive(
                work,
                job_status="waiting",
                retry_at=utcnow()
                + timedelta(
                    seconds=max(
                        retry_hint(error, utcnow()), self.settings.archive_retry_delay
                    )
                ),
                error=_error_text(error),
                **failure_facts(error),
            )
            return False
        except ServiceError as error:
            # A lost reply is safe: the next request finds the derive it posted.
            logger.warning(
                "Derive request for {} will retry: {}", identifier, _error_text(error)
            )
            self.state.finish_derive(
                work,
                job_status="waiting",
                retry_at=self._backoff(work, error),
                error=_error_text(error),
                **failure_facts(error),
            )
            return False
        now = utcnow()
        facts: dict[str, Any] = {
            "derive_task_id": submission.task_id,
            "derive_requested_at": now,
        }
        status = "requested"
        if submission.already_pending:
            # Nothing was posted, so reading the task first loses no receipt.
            try:
                observed = self.gateway.task_status(submission.task_id)
            except Exception as error:
                logger.debug(
                    "Could not read pending derive {} for {}: {}",
                    submission.task_id,
                    identifier,
                    _error_text(error),
                )
            else:
                facts["derive_requested_at"] = observed.submitted_at
                facts["derive_raw_state"] = observed.raw_state
                status = LIVE_STATES.get(observed.state, "requested")
        self.state.finish_derive(
            work,
            job_status="waiting",
            retry_at=now
            + timedelta(seconds=self.settings.archive_derive_poll_interval),
            derive_status=status,
            reset_attempts=True,
            **facts,
        )
        logger.info(
            "{} derive {} for {}",
            "Found pending" if submission.already_pending else "Requested",
            submission.task_id,
            identifier,
        )
        return True

    def _check(self, work: ArchiveWork) -> bool:
        progress = work.progress
        next_check = utcnow() + timedelta(
            seconds=self.settings.archive_derive_poll_interval
        )
        try:
            # Saga's own task decides the outcome; the item's task list keeps
            # older failed derives that say nothing about this one.
            if progress.derive_task_id is None:
                outcome = self._from_item(work)
            else:
                try:
                    status = self.gateway.task_status(progress.derive_task_id)
                except InternetArchiveTaskNotFoundError:
                    logger.warning(
                        "Archive has no task {} for {}; reading the item instead",
                        progress.derive_task_id,
                        progress.ia_identifier,
                    )
                    outcome = self._from_item(work, cause=LOST)
                else:
                    outcome = self._from_status(work, status)
        except ServiceError as error:
            # Every row waits for the next pass with its state unchanged.
            logger.warning(
                "Derive confirmation paused until the next pass: {}", _error_text(error)
            )
            self.state.finish_derive(
                work,
                job_status="waiting",
                retry_at=next_check,
                error=_error_text(error),
                **failure_facts(error),
            )
            return False
        if outcome is None:
            self.state.finish_derive(work, job_status="waiting", retry_at=next_check)
            return True
        error = None
        cause = self._stale_cause(progress, outcome.pop("_cause", None), outcome)
        if cause is not None:
            error = self._report_stale(progress, cause)
            raw = outcome.get("derive_raw_state", progress.derive_raw_state)
            outcome.clear()
            outcome.update(derive_status="failed", derive_raw_state=raw)
        status = outcome.pop("derive_status")
        job_status = (
            "passed"
            if status == "done"
            else "failed"
            if status == "failed"
            else "pending"
            if status == "pending"
            else "waiting"
        )
        self.state.finish_derive(
            work,
            job_status=job_status,
            retry_at=next_check if job_status == "waiting" else None,
            derive_status=status,
            error=error,
            **outcome,
        )
        if error is None and status != progress.derive_status:
            log = logger.warning if status in {"failed", "unknown"} else logger.info
            log(
                "Derive for {} is {} (was {})",
                progress.ia_identifier,
                status,
                progress.derive_status,
            )
        return True

    def _stale_cause(
        self, progress: ArchiveProgress, cause: str | None, outcome: dict[str, Any]
    ) -> str | None:
        """Return why a derive that made no progress has waited too long.

        A derive known only from item reads stalls while it stays requested,
        running, or unknown. A derive with a task ID stalls only while it
        stays unknown because this check found the task lost or finished,
        and the item read in this same check was complete and showed no
        derivative files. Checks that fail or read an incomplete item never
        reach here, and paused derives are never stalled.
        """
        if progress.derive_task_id is None:
            if (
                progress.derive_requested_at is None
                or progress.derive_status not in WATCHED
                or outcome["derive_status"] not in WATCHED
            ):
                return None
            cause = ITEM_ONLY
        elif cause is None or progress.derive_status != "unknown":
            return None
        return cause if self._timed_out(progress) else None

    def _timed_out(self, progress: ArchiveProgress) -> bool:
        """Return whether the stale period has passed on the shared clock.

        The clock starts at the later of ``derive_status_at`` and the last
        time the account backlog fell to the target or below, and a derive
        fails only while the backlog stays there. A derive held up by a full
        Archive queue gets the whole stale period again once the queue
        drains, and an unreadable backlog fails nothing.
        """
        stamp = progress.derive_status_at
        limit = self.settings.archive_derive_stale_days * 86_400
        if stamp is None or (utcnow() - _as_utc(stamp)).total_seconds() <= limit:
            return False
        if self._settled is None:
            backlog = self._read_backlog()
            target = self.settings.archive_derive_backlog_target
            self._settled = backlog is not None and backlog.outstanding <= target
        since = self._settled_since
        return (
            self._settled
            and since is not None
            and (utcnow() - max(_as_utc(stamp), since)).total_seconds() > limit
        )

    def _report_stale(self, progress: ArchiveProgress, cause: str) -> str:
        """Log the single error for a stalled derive and return its reason."""
        days = self.settings.archive_derive_stale_days
        identifier = progress.ia_identifier
        task_id = progress.derive_task_id
        if cause == NO_OUTPUT:
            logger.error(
                "Derive task {} for {} finished without derivative files and none "
                "appeared for {:g} days; marked failed. A retry probably won't help; "
                "check the item on the Archive by hand",
                task_id,
                identifier,
                days,
            )
            return f"DeriveNoOutput: task {task_id} finished without derivatives"
        if cause == LOST:
            message = (
                f"DeriveStalled: task {task_id} no longer tracked by the Tasks API"
            )
            detail = f"task {task_id} is no longer tracked by the Archive's Tasks API"
        else:
            message = (
                f"DeriveStalled: no derivative files after {days:g} days "
                "without an Archive task ID"
            )
            detail = "it showed no derivative files"
        logger.error(
            "Derive for {} stalled: {} for {:g} days; marked failed. "
            "Retry it with: python saga.py retry-derives {}",
            identifier,
            detail,
            days,
            identifier,
        )
        return message

    def _from_status(
        self, work: ArchiveWork, status: InternetArchiveTaskStatus
    ) -> dict[str, Any] | None:
        raw = {"derive_raw_state": status.raw_state}
        if status.state in LIVE_STATES:
            return {"derive_status": LIVE_STATES[status.state], **raw}
        if status.state is InternetArchiveTaskState.ERROR:
            return {"derive_status": "failed", **raw}
        if status.state is InternetArchiveTaskState.FINISHED:
            item = self.gateway.get_item(_identifier(work))
            if not _resolved(item):
                return None
            if _derived(item):
                # The Archive reports no finish time, so record when Saga saw it.
                return {"derive_status": "done", "derive_completed_at": utcnow(), **raw}
            logger.warning(
                "Derive {} for {} finished without derivative files",
                status.task_id,
                _identifier(work),
            )
            return {"derive_status": "unknown", "_cause": NO_OUTPUT, **raw}
        logger.warning(
            "Derive {} for {} has an unrecognized state {!r}; reading the item",
            status.task_id,
            _identifier(work),
            status.raw_state,
        )
        return self._from_item(work, cause=LOST)

    def _from_item(
        self, work: ArchiveWork, *, cause: str | None = None
    ) -> dict[str, Any] | None:
        """Read derive state from the item's files and outstanding tasks.

        ``cause`` records why a derive with a task ID was read from its item,
        so an ``unknown`` result can stall on the shared clock.
        """
        item = self.gateway.get_item(_identifier(work))
        if not _resolved(item):
            return None
        if _derived(item):
            return {"derive_status": "done", "derive_completed_at": utcnow()}
        pending = [
            task for task in item.pending_tasks if task.command == DERIVE_COMMAND
        ]
        if pending:
            task = pending[0]
            return {
                "derive_status": "running"
                if task.wait_admin == WAIT_RUNNING
                else "paused"
                if task.wait_admin == WAIT_PAUSED
                else "requested",
                "derive_requested_at": work.progress.derive_requested_at or utcnow(),
                "derive_raw_state": task.raw_state,
            }
        failed = [task for task in item.error_tasks if task.command == DERIVE_COMMAND]
        # Item tasks carry no task ID or submit time. With a task ID of its own,
        # Saga learns of a failure only from the task API, so a derive that
        # failed before a retry cannot fail the retried one.
        if failed and work.progress.derive_task_id is None:
            return {
                "derive_status": "failed",
                "derive_raw_state": failed[0].raw_state or failed[0].error,
            }
        if work.progress.derive_task_id is None and (
            work.progress.derive_requested_at is None
        ):
            # No derive was ever seen for this item, so it still needs one.
            return {"derive_status": "pending"}
        return {"derive_status": "unknown", "_cause": cause}

    def _backoff(self, work: ArchiveWork, error: Exception) -> datetime:
        assert work.job is not None
        delay = max(
            retry_hint(error, utcnow()),
            retry_delay(self.settings.archive_retry_delay, work.job.attempts),
        )
        return utcnow() + timedelta(seconds=delay)


def _identifier(work: ArchiveWork) -> str:
    assert work.progress.ia_identifier is not None
    return work.progress.ia_identifier


def _resolved(item: InternetArchiveItem) -> bool:
    """Return whether an item read is complete enough to judge its files."""
    return bool(item.metadata) and not item.server_unavailable and item.listing_complete


def _derived(item: InternetArchiveItem) -> bool:
    return any(
        file.source is InternetArchiveItemFileSource.DERIVATIVE for file in item.files
    )


def _error_text(error: Exception) -> str:
    detail = f"{type(error).__name__}: {error}"
    phase = getattr(error, "request_phase", None)
    if phase:
        detail += f" (phase={phase})"
    return redact_diagnostics(detail)[:2000]
