import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemFile,
    InternetArchiveItemFileSource,
    InternetArchiveItemTask,
    InternetArchiveItemTaskState,
    InternetArchiveTaskNotFoundError,
    InternetArchiveTaskState,
    InternetArchiveTaskStatus,
    InternetArchiveTaskSubmission,
    InternetArchiveTaskSummary,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from sqlmodel import Session, select

from core import archival, maintenance, reporting
from core.archival import ArchiveGateway, ArchiveProcessor
from core.archive_derives import DeriveDispatcher, _error_text
from core.archive_tasks import TaskBacklog, TaskSource, TaskSummaryCache
from core.service import SagaService
from core.shutdown import CooperativeStop, StopController
from core.state import (
    ArchiveJob,
    ArchiveProgress,
    Report,
    SchemaMigration,
    StateStore,
    _as_utc,
    utcnow,
)
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings

KNOWN = (
    "riyadhfalcons-vs-royalravens-major-i-tournament-elimination-round-1-1jd2l2k6ZQY"
)
SUBMITTED = datetime(2026, 11, 20, 12, tzinfo=UTC)


class Backlog(TaskSource):
    """Return a fixed account backlog, or none, after reading the summary."""

    def __init__(self, outstanding: int | None) -> None:
        self.outstanding = outstanding
        self.reads = 0

    def read(self, fetch: Any, *, limit: int, base_delay: float) -> TaskBacklog | None:
        self.reads += 1
        if self.outstanding is None:
            return None
        return TaskBacklog(self.outstanding, 0, 0, 0, datetime.now(UTC), 1.0)


class Archive:
    """Answer derive, task, and item reads from scripted results."""

    def __init__(self, outstanding: int | None = 0) -> None:
        self.tasks: TaskSource = Backlog(outstanding)
        self.submissions: list[Any] = []
        self.statuses: list[Any] = []
        self.items: list[Any] = []
        self.calls: list[tuple[str, Any]] = []
        self.next_task = 100

    def task_summary(self) -> InternetArchiveTaskSummary:
        return InternetArchiveTaskSummary(0, 0, 0, 0)

    def queue_derive(self, identifier: str) -> InternetArchiveTaskSubmission:
        self.calls.append(("queue", identifier))
        value = self.submissions.pop(0) if self.submissions else None
        if isinstance(value, BaseException):
            raise value
        if value is None:
            self.next_task += 1
            return InternetArchiveTaskSubmission(
                self.next_task, identifier, None, already_pending=False
            )
        return value

    def task_status(self, task_id: int) -> InternetArchiveTaskStatus:
        self.calls.append(("status", task_id))
        value = self.statuses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def get_item(self, identifier: str) -> InternetArchiveItem:
        self.calls.append(("item", identifier))
        value = self.items.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


def status(
    state: InternetArchiveTaskState,
    task_id: int = 101,
    raw: str | None = None,
    marker: int | None = None,
) -> InternetArchiveTaskStatus:
    return InternetArchiveTaskStatus(
        task_id, "item", "derive.php", state, SUBMITTED, None, None, None, raw, marker
    )


def task(
    state: InternetArchiveItemTaskState,
    wait_admin: int | None = 0,
    command: str = "derive.php",
    raw: str | None = None,
    error: str | None = None,
) -> InternetArchiveItemTask:
    return InternetArchiveItemTask(command, state, raw, wait_admin, error)


def item(
    identifier: str = "item",
    *,
    derived: bool = False,
    tasks: tuple[InternetArchiveItemTask, ...] = (),
    metadata: bool = True,
    complete: bool = True,
    unavailable: bool = False,
) -> InternetArchiveItem:
    files = [
        InternetArchiveItemFile(
            "media.mkv", 4, None, None, InternetArchiveItemFileSource.ORIGINAL, None
        )
    ]
    if derived:
        files.append(
            InternetArchiveItemFile(
                "media.mp4",
                3,
                None,
                None,
                InternetArchiveItemFileSource.DERIVATIVE,
                "derivative",
            )
        )
    return InternetArchiveItem(
        identifier,
        {"identifier": identifier} if metadata else {},
        None,
        tuple(files),
        tasks,
        InternetArchiveItemAvailability.AVAILABLE,
        complete,
        False,
        False,
        False,
        unavailable,
        (),
        None,
        None,
    )


def acknowledge(store: StateStore, video_id: str) -> None:
    work = SimpleNamespace(
        video=SimpleNamespace(id=video_id), progress=saved(store, video_id)
    )
    store.acknowledge_upload(cast(Any, work), receipt="{}", etag=None)


def uploaded(
    store: StateStore, root: Path, video_id: str, policy: str = "deferred"
) -> str:
    """Seed one acknowledged upload written with the given derive option."""
    video_id, _, _ = seed_upload(store, root / video_id, video_id)
    store.update_archive(video_id, derive_policy=policy)
    acknowledge(store, video_id)
    return video_id


def job(store: StateStore, video_id: str) -> ArchiveJob | None:
    with Session(store.engine) as session:
        result = session.get(ArchiveJob, (video_id, "derive"))
        if result is not None:
            session.expunge(result)
        return result


def due(store: StateStore, video_id: str) -> None:
    """Make a derive job due now without changing its state."""
    with Session(store.engine) as session:
        row = session.get(ArchiveJob, (video_id, "derive"))
        assert row is not None
        row.retry_at = None
        session.add(row)
        session.commit()


def set_progress(store: StateStore, video_id: str, **values: Any) -> None:
    with Session(store.engine) as session:
        row = session.get(ArchiveProgress, video_id)
        assert row is not None
        for name, value in values.items():
            setattr(row, name, value)
        session.add(row)
        session.commit()


def dispatcher(
    store: StateStore, root: Path, archive: Archive, **values: Any
) -> DeriveDispatcher:
    values.setdefault("archive_derive_submit_interval", 0)
    return DeriveDispatcher(settings(root, **values), store, cast(Any, archive))


def near(value: datetime | None, expected: datetime, slack: float = 5) -> bool:
    assert value is not None
    return abs((_as_utc(value) - expected).total_seconds()) <= slack


@pytest.fixture
def store(tmp_path: Path) -> Any:
    state = StateStore(tmp_path / "saga.sqlite")
    yield state
    state.close()


# Upload options and derive jobs


def test_upload_option_decides_whether_an_item_gets_a_derive_job(
    store: StateStore, tmp_path: Path
) -> None:
    deferred = uploaded(store, tmp_path, "deferred000")
    immediate = uploaded(store, tmp_path, "immediate00", "immediate")
    never = uploaded(store, tmp_path, "never000000", "never")
    assert saved(store, deferred).derive_status == "pending"
    assert saved(store, deferred).derive_status_at is not None
    assert job(store, deferred) is not None
    assert saved(store, immediate).derive_status is None
    assert job(store, immediate) is None
    assert saved(store, never).derive_status == "not_needed"
    assert job(store, never) is None
    # Acknowledging again leaves the recorded derive state alone.
    acknowledge(store, deferred)
    assert saved(store, deferred).derive_status == "pending"


@pytest.mark.parametrize("multipart", [False, True])
@pytest.mark.parametrize("stored", [None, "immediate", "never"])
def test_transfers_send_the_stored_option_after_the_setting_changes(
    store: StateStore, tmp_path: Path, multipart: bool, stored: str | None
) -> None:
    video_id, _, _ = seed_upload(store, tmp_path)
    if multipart:
        store.update_archive(video_id, multipart_state="{}")
    if stored:
        store.update_archive(video_id, derive_policy=stored, upload_mode="recover")
    seen: list[Any] = []

    class Gateway(FakeGateway):
        def recover(self, *args: Any, **kwargs: Any) -> Any:
            seen.append(("recover", kwargs["derive"]))
            return FakeGateway.upload(self, *args, progress=kwargs["progress"])

        def upload(self, *args: Any, **kwargs: Any) -> Any:
            seen.append(("upload", kwargs.get("derive")))
            return super().upload(*args, progress=kwargs["progress"])

        def upload_multipart(self, *args: Any, **kwargs: Any) -> Any:
            seen.append(("multipart", kwargs["derive"]))
            return super().upload(*args, progress=kwargs["progress"])

    ArchiveProcessor(
        settings(tmp_path, archive_derive="deferred"), store, YouTubeClient(), Gateway()
    )._upload(cast(Any, SimpleNamespace(**_work(store, video_id))))
    expected = stored or "deferred"
    assert seen[-1][1] == expected
    assert seen[-1][0] == (
        "multipart" if multipart else "recover" if stored else "upload"
    )
    assert saved(store, video_id).derive_policy == expected


def _work(store: StateStore, video_id: str) -> dict[str, Any]:
    return {
        "video": SimpleNamespace(id=video_id),
        "progress": saved(store, video_id),
        "job": None,
        "archive_generation": None,
    }


def test_processor_runs_derives_only_with_a_real_gateway(
    store: StateStore, tmp_path: Path
) -> None:
    real = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    assert isinstance(real.derives, DeriveDispatcher)
    fake = ArchiveProcessor(settings(tmp_path), store, YouTubeClient(), FakeGateway())
    assert fake.derives is None


# Gateway calls through Archivist


def test_gateway_reaches_derive_task_and_item_apis_through_archivist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submissions = [
        InternetArchiveTaskSubmission(1, "item", None, already_pending=False),
        InternetArchiveTaskSubmission(1, "item", None, already_pending=True),
    ]
    monkeypatch.setattr(
        archival.InternetArchiveClient,
        "queue_derive",
        lambda self, identifier: submissions.pop(0),
    )
    monkeypatch.setattr(
        archival.InternetArchiveClient,
        "task_status",
        lambda self, task_id: status(InternetArchiveTaskState.QUEUED, task_id),
    )
    monkeypatch.setattr(
        archival.InternetArchiveClient,
        "get_item",
        lambda self, identifier: item(identifier),
    )
    gateway = ArchiveGateway(settings(tmp_path))
    assert isinstance(gateway.tasks, TaskSummaryCache)
    assert not gateway.queue_derive("item").already_pending
    assert gateway.queue_derive("item").already_pending
    # Only a new submission adds to the backlog Saga measures its drain against.
    assert gateway.tasks._added == 1
    assert gateway.task_status(5).task_id == 5
    assert gateway.get_item("item").identifier == "item"


def test_managed_gateway_sends_derive_reads_to_a_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, tuple[Any, ...]]] = []
    results = {
        "queue_derive": InternetArchiveTaskSubmission(
            9, "item", None, already_pending=False
        ),
        "task_status": status(InternetArchiveTaskState.RUNNING, 9),
        "get_item": item(),
    }

    def run(category: str, method: str, options: Any, args: Any, *rest: Any) -> Any:
        calls.append((method, args))
        return results[method]

    monkeypatch.setattr(archival, "run_managed", run)
    gateway = ArchiveGateway(settings(tmp_path), managed=True)
    assert gateway.queue_derive("item").task_id == 9
    assert gateway.task_status(9).state is InternetArchiveTaskState.RUNNING
    assert gateway.get_item("item").identifier == "item"
    assert calls == [
        ("queue_derive", ("item",)),
        ("task_status", (9,)),
        ("get_item", ("item",)),
    ]
    assert cast(TaskSummaryCache, gateway.tasks)._added == 1


# Submission


def test_submissions_fill_the_room_oldest_upload_first(
    store: StateStore, tmp_path: Path
) -> None:
    first = uploaded(store, tmp_path, "first000000")
    second = uploaded(store, tmp_path, "second00000")
    third = uploaded(store, tmp_path, "third000000")
    archive = Archive(outstanding=18)
    waits: list[float] = []
    derives = dispatcher(store, tmp_path, archive, archive_derive_submit_interval=2)

    def wait(seconds: float) -> None:
        waits.append(seconds)

    derives.stop_controller.wait = wait  # type: ignore[method-assign]
    assert derives.submit_round() == 2
    identifiers = [saved(store, video).ia_identifier for video in (first, second)]
    assert archive.calls == [("queue", value) for value in identifiers]
    assert waits == [2]
    row = saved(store, first)
    assert (row.derive_status, row.derive_task_id) == ("requested", 101)
    assert near(row.derive_requested_at, datetime.now(UTC))
    claim = job(store, first)
    assert claim is not None and claim.status == "waiting" and claim.attempts == 0
    assert claim.claim_token is None
    assert near(claim.retry_at, datetime.now(UTC) + timedelta(seconds=900))
    assert saved(store, third).derive_status == "pending"


@pytest.mark.parametrize(
    "outstanding,values",
    [
        (0, {"archive_derive_backlog_target": 0}),
        (None, {}),
        (20, {}),
        (25, {}),
        (12, {"archive_task_queue_limit": 10}),
    ],
)
def test_no_derive_is_requested_without_room(
    store: StateStore, tmp_path: Path, outstanding: int | None, values: dict[str, Any]
) -> None:
    uploaded(store, tmp_path, "waiting0000")
    archive = Archive(outstanding)
    assert dispatcher(store, tmp_path, archive, **values).submit_round() == 0
    assert archive.calls == []
    reads = cast(Backlog, archive.tasks).reads
    assert reads == (0 if values.get("archive_derive_backlog_target") == 0 else 1)


def test_due_uploads_keep_derives_waiting(store: StateStore, tmp_path: Path) -> None:
    derived = uploaded(store, tmp_path, "derived0000")
    seed_upload(store, tmp_path / "next", "nextupload0")
    archive = Archive()
    assert dispatcher(store, tmp_path, archive).submit_round() == 0
    assert archive.calls == []
    assert store.uploads_due()
    store.update_archive("nextupload0", retry_after=600, release=True)
    assert not store.uploads_due()
    assert dispatcher(store, tmp_path, archive).submit_round() == 1
    assert saved(store, derived).derive_status == "requested"


@pytest.mark.parametrize(
    "observed,expected,requested",
    [
        (status(InternetArchiveTaskState.PAUSED, 77, "9"), "paused", SUBMITTED),
        (status(InternetArchiveTaskState.RUNNING, 77), "running", SUBMITTED),
        (status(InternetArchiveTaskState.QUEUED, 77), "requested", SUBMITTED),
        (status(InternetArchiveTaskState.FINISHED, 77), "requested", SUBMITTED),
        (NetworkError("lost"), "requested", None),
    ],
)
def test_a_pending_derive_is_adopted_without_a_new_request(
    store: StateStore,
    tmp_path: Path,
    observed: Any,
    expected: str,
    requested: datetime | None,
) -> None:
    video_id = uploaded(store, tmp_path, "pending0000")
    archive = Archive()
    archive.submissions = [
        InternetArchiveTaskSubmission(77, "item", None, already_pending=True)
    ]
    archive.statuses = [observed]
    assert dispatcher(store, tmp_path, archive).submit_round() == 1
    row = saved(store, video_id)
    assert (row.derive_status, row.derive_task_id) == (expected, 77)
    assert near(row.derive_requested_at, requested or datetime.now(UTC))
    assert [name for name, _ in archive.calls] == ["queue", "status"]


def test_throttled_request_waits_for_the_retry_hint_and_ends_the_round(
    store: StateStore, tmp_path: Path
) -> None:
    first = uploaded(store, tmp_path, "first000000")
    uploaded(store, tmp_path, "second00000")
    archive = Archive()
    archive.submissions = [
        RateLimitError(
            "slow",
            service_code="SlowDown",
            retry_after=600,
            request_phase="task-submit",
        )
    ]
    assert dispatcher(store, tmp_path, archive).submit_round() == 1
    assert len(archive.calls) == 1
    row, claim = saved(store, first), job(store, first)
    assert claim is not None and row.derive_status == "pending"
    assert claim.status == "waiting" and claim.error_kind == "rate_limit"
    assert near(claim.retry_at, datetime.now(UTC) + timedelta(seconds=600))
    assert claim.last_error is not None and "phase=task-submit" in claim.last_error


@pytest.mark.parametrize(
    "error",
    [
        NetworkError("reset", request_phase="task-submit"),
        InvalidServiceResponseError("bad reply", request_phase="task-submit"),
        ServiceError("unavailable", status_code=503, request_phase="task-submit"),
    ],
)
def test_a_lost_reply_backs_off_and_the_retry_finds_the_posted_derive(
    store: StateStore, tmp_path: Path, error: ServiceError
) -> None:
    video_id = uploaded(store, tmp_path, "lostreply00")
    archive = Archive()
    archive.submissions = [
        error,
        InternetArchiveTaskSubmission(55, "item", None, already_pending=True),
    ]
    archive.statuses = [status(InternetArchiveTaskState.QUEUED, 55)]
    derives = dispatcher(store, tmp_path, archive)
    assert derives.submit_round() == 1
    claim = job(store, video_id)
    assert claim is not None and claim.status == "waiting" and claim.attempts == 1
    assert claim.retry_at is not None and _as_utc(claim.retry_at) > datetime.now(UTC)
    assert saved(store, video_id).derive_status == "pending"
    assert derives.submit_round() == 0
    due(store, video_id)
    assert derives.submit_round() == 1
    row = saved(store, video_id)
    assert (row.derive_status, row.derive_task_id) == ("requested", 55)
    assert [name for name, _ in archive.calls] == ["queue", "queue", "status"]


def test_rejected_credentials_fail_the_derive_and_alert(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = uploaded(store, tmp_path, "rejected000")
    uploaded(store, tmp_path, "untouched00")
    archive = Archive()
    archive.submissions = [AuthenticationError("denied", status_code=403)]
    assert dispatcher(store, tmp_path, archive).submit_round() == 1
    claim = job(store, video_id)
    assert claim is not None and claim.status == "failed"
    assert claim.error_kind == "authentication"
    assert saved(store, video_id).derive_status == "failed"
    health = store.transfer_health(stall_timeout=900)
    alert = next(a for a in health["alerts"] if a["key"] == "derive:authentication")
    assert alert["severity"] == "error"
    assert alert["details"]["derives"][0]["video_id"] == video_id
    assert health["healthy"]


def test_failed_derives_are_never_claimed_or_resubmitted(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = uploaded(store, tmp_path, "failed00000")
    set_progress(store, video_id, derive_status="failed", derive_task_id=7)
    archive = Archive()
    derives = dispatcher(store, tmp_path, archive)
    assert derives.submit_round() == 0
    derives.check_round()
    assert archive.calls == []
    for mode in ("submit", "check", "reconcile"):
        assert store.claim_derive(mode) is None


def test_shutdown_releases_the_claim_unchanged(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = uploaded(store, tmp_path, "stopping000")
    archive = Archive()
    archive.submissions = [CooperativeStop("stop")]
    with pytest.raises(CooperativeStop):
        dispatcher(store, tmp_path, archive).submit_round()
    claim = job(store, video_id)
    assert claim is not None and claim.claim_token is None
    assert claim.status == "waiting" and claim.retry_at is None
    assert saved(store, video_id).derive_status == "pending"


def test_shutdown_during_the_submit_interval_keeps_the_saved_request(
    store: StateStore, tmp_path: Path
) -> None:
    first = uploaded(store, tmp_path, "first000000")
    second = uploaded(store, tmp_path, "second00000")
    controller = StopController()
    derives = DeriveDispatcher(
        settings(tmp_path, archive_derive_submit_interval=30),
        store,
        cast(Any, Archive()),
        controller,
    )
    original = derives._submit

    def submit(work: Any) -> bool:
        result = original(work)
        controller.request()
        return result

    derives._submit = submit  # type: ignore[method-assign]
    with pytest.raises(CooperativeStop):
        derives.submit_round()
    assert saved(store, first).derive_status == "requested"
    assert saved(store, second).derive_status == "pending"
    claim = job(store, second)
    assert claim is not None and claim.claim_token is None


def test_unexpected_errors_back_off_and_end_the_round(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = uploaded(store, tmp_path, "unexpected0")
    archive = Archive()
    archive.submissions = [ValueError("bad identifier")]
    assert dispatcher(store, tmp_path, archive).submit_round() == 1
    claim = job(store, video_id)
    assert claim is not None and claim.status == "waiting"
    assert claim.last_error == "ValueError: bad identifier"
    assert claim.retry_at is not None and _as_utc(claim.retry_at) > datetime.now(UTC)


# Confirmation


def requested(
    store: StateStore, root: Path, video_id: str = "requested00", **values: Any
) -> str:
    video_id = uploaded(store, root, video_id)
    values = {
        "derive_status": "requested",
        "derive_task_id": 101,
        "derive_requested_at": utcnow(),
        **values,
    }
    set_progress(store, video_id, **values)
    return video_id


@pytest.mark.parametrize(
    "state,expected",
    [
        (InternetArchiveTaskState.QUEUED, "requested"),
        (InternetArchiveTaskState.RUNNING, "running"),
        (InternetArchiveTaskState.PAUSED, "paused"),
    ],
)
def test_live_derives_are_followed_without_resubmission(
    store: StateStore, tmp_path: Path, state: InternetArchiveTaskState, expected: str
) -> None:
    video_id = requested(store, tmp_path)
    archive = Archive()
    archive.statuses = [status(state, raw="x")]
    dispatcher(store, tmp_path, archive).check_round()
    row, claim = saved(store, video_id), job(store, video_id)
    assert (row.derive_status, row.derive_raw_state) == (expected, "x")
    assert claim is not None and claim.status == "waiting"
    assert near(claim.retry_at, datetime.now(UTC) + timedelta(seconds=900))
    assert archive.calls == [("status", 101)]
    # A checked derive waits for the next poll interval.
    dispatcher(store, tmp_path, archive).check_round()
    assert archive.calls == [("status", 101)]


def test_failed_derive_keeps_its_task_for_follow_up(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = requested(store, tmp_path)
    archive = Archive()
    archive.statuses = [status(InternetArchiveTaskState.ERROR, raw="2")]
    dispatcher(store, tmp_path, archive).check_round()
    row, claim = saved(store, video_id), job(store, video_id)
    assert (row.derive_status, row.derive_task_id, row.derive_raw_state) == (
        "failed",
        101,
        "2",
    )
    assert claim is not None and claim.status == "failed" and claim.retry_at is None


def test_finished_derive_is_done_only_with_derivative_files(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = requested(store, tmp_path)
    identifier = saved(store, video_id).ia_identifier
    archive = Archive()
    archive.statuses = [status(InternetArchiveTaskState.FINISHED, marker=1_700_000_000)]
    archive.items = [item(), item(derived=True)]
    derives = dispatcher(store, tmp_path, archive)
    derives.check_round()
    row = saved(store, video_id)
    assert (row.derive_status, row.derive_task_id) == ("unknown", 101)
    assert row.derive_completed_at is None
    due(store, video_id)
    derives.check_round()
    row, claim = saved(store, video_id), job(store, video_id)
    assert row.derive_status == "done"
    # The finish marker is opaque; completion is when Saga saw the files.
    assert near(row.derive_completed_at, datetime.now(UTC))
    assert claim is not None and claim.status == "passed"
    assert archive.calls == [
        ("status", 101),
        ("item", identifier),
        ("item", identifier),
    ]


def test_finished_derive_with_derivatives_completes_at_observation(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = requested(store, tmp_path)
    archive = Archive()
    archive.statuses = [status(InternetArchiveTaskState.FINISHED, marker=5)]
    archive.items = [item(derived=True)]
    dispatcher(store, tmp_path, archive).check_round()
    row = saved(store, video_id)
    assert row.derive_status == "done"
    assert near(row.derive_completed_at, datetime.now(UTC))


@pytest.mark.parametrize(
    "answer",
    [
        status(InternetArchiveTaskState.UNKNOWN, raw="7"),
        InternetArchiveTaskNotFoundError(101),
    ],
)
@pytest.mark.parametrize(
    "found,expected,job_status",
    [
        (item(derived=True), "done", "passed"),
        (
            item(tasks=(task(InternetArchiveItemTaskState.PENDING, 1),)),
            "running",
            "waiting",
        ),
        (
            item(tasks=(task(InternetArchiveItemTaskState.PENDING, 9),)),
            "paused",
            "waiting",
        ),
        (
            item(tasks=(task(InternetArchiveItemTaskState.UNKNOWN, None),)),
            "requested",
            "waiting",
        ),
        (
            item(tasks=(task(InternetArchiveItemTaskState.ERROR, 2, error="boom"),)),
            "failed",
            "failed",
        ),
        (
            item(tasks=(task(InternetArchiveItemTaskState.PENDING, 0, "other.php"),)),
            "unknown",
            "waiting",
        ),
    ],
)
def test_unrecognized_or_missing_tasks_are_read_from_the_item(
    store: StateStore,
    tmp_path: Path,
    answer: Any,
    found: InternetArchiveItem,
    expected: str,
    job_status: str,
) -> None:
    video_id = requested(store, tmp_path)
    archive = Archive()
    archive.statuses = [answer]
    archive.items = [found]
    dispatcher(store, tmp_path, archive).check_round()
    row, claim = saved(store, video_id), job(store, video_id)
    assert row.derive_status == expected
    assert row.derive_task_id == 101
    # The missing task was handled before generic service failures.
    assert claim is not None and claim.status == job_status
    assert claim.last_error is None
    assert not any(name == "queue" for name, _ in archive.calls)


@pytest.mark.parametrize(
    "error",
    [
        RateLimitError("slow", retry_after=30),
        NetworkError("reset"),
        AuthenticationError("denied"),
        InvalidServiceResponseError("bad"),
    ],
)
@pytest.mark.parametrize("on_item", [False, True])
def test_read_failures_end_the_pass_with_state_unchanged(
    store: StateStore, tmp_path: Path, error: ServiceError, on_item: bool
) -> None:
    first = requested(store, tmp_path, "first000000")
    second = requested(store, tmp_path, "second00000")
    archive = Archive()
    if on_item:
        archive.statuses = [status(InternetArchiveTaskState.FINISHED)]
        archive.items = [error]
    else:
        archive.statuses = [error]
    dispatcher(store, tmp_path, archive).check_round()
    for video_id in (first, second):
        assert saved(store, video_id).derive_status == "requested"
    claim = job(store, first)
    assert claim is not None and claim.status == "waiting"
    assert near(claim.retry_at, datetime.now(UTC) + timedelta(seconds=900))
    assert claim.last_error is not None
    assert len([name for name, _ in archive.calls if name == "status"]) == 1


@pytest.mark.parametrize(
    "unresolved", [item(metadata=False), item(complete=False), item(unavailable=True)]
)
def test_unresolved_item_reads_change_nothing(
    store: StateStore, tmp_path: Path, unresolved: InternetArchiveItem
) -> None:
    video_id = requested(store, tmp_path, derive_status="unknown")
    archive = Archive()
    archive.items = [unresolved]
    dispatcher(store, tmp_path, archive).check_round()
    claim = job(store, video_id)
    assert saved(store, video_id).derive_status == "unknown"
    assert claim is not None and claim.last_error is None
    assert near(claim.retry_at, datetime.now(UTC) + timedelta(seconds=900))


def test_untracked_requested_derive_is_read_from_the_item(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = requested(store, tmp_path, derive_task_id=None)
    archive = Archive()
    archive.items = [item()]
    dispatcher(store, tmp_path, archive).check_round()
    # It was seen before, so it never returns to the submission queue.
    assert saved(store, video_id).derive_status == "unknown"
    assert [name for name, _ in archive.calls] == ["item"]


def test_unexpected_check_errors_release_the_claim(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = requested(store, tmp_path)
    archive = Archive()
    archive.statuses = [KeyError("odd")]
    dispatcher(store, tmp_path, archive).check_round()
    claim = job(store, video_id)
    assert claim is not None and claim.claim_token is None
    assert claim.status == "waiting" and claim.last_error is not None
    assert saved(store, video_id).derive_status == "requested"


def test_check_round_stops_cooperatively(store: StateStore, tmp_path: Path) -> None:
    requested(store, tmp_path)
    controller = StopController()
    controller.request()
    derives = DeriveDispatcher(
        settings(tmp_path), store, cast(Any, Archive()), controller
    )
    with pytest.raises(CooperativeStop):
        derives.check_round()


def test_claim_rejects_another_workers_claim(store: StateStore, tmp_path: Path) -> None:
    video_id = uploaded(store, tmp_path, "claimed0000")
    work = store.claim_derive("submit")
    assert work is not None and work.job is not None
    stale = SimpleNamespace(video=work.video, job=SimpleNamespace(claim_token="old"))
    with pytest.raises(RuntimeError, match="no longer belongs"):
        store.finish_derive(cast(Any, stale), job_status="waiting", retry_at=None)
    with pytest.raises(ValueError, match="Unknown derive fields"):
        store.finish_derive(work, job_status="waiting", retry_at=None, status="done")
    assert job(store, video_id) is not None


# Existing items


def legacy_store(tmp_path: Path) -> StateStore:
    """Build a database as an older release left it, then reopen it."""
    store = StateStore(tmp_path / "saga.sqlite")
    for name in ("uploaded000", "uploading00", "reconcile00", "upload00000"):
        seed_upload(store, tmp_path / name, name)
    seed_upload(store, tmp_path / "known", "1jd2l2k6ZQY")
    seed_upload(store, tmp_path / "metadata", "metadata000")
    with Session(store.engine) as session:
        for row in session.exec(select(ArchiveProgress)).all():
            row.derive_policy = row.derive_status = None
            if row.video_id in {"uploaded000", "1jd2l2k6ZQY"}:
                row.media_uploaded_at = utcnow()
                row.status = "complete"
            elif row.video_id == "uploading00":
                row.status = "uploading"
            elif row.video_id == "reconcile00":
                row.status = "reconcile_upload"
            elif row.video_id == "metadata000":
                row.status = "metadata"
            if row.video_id == "1jd2l2k6ZQY":
                row.ia_identifier = KNOWN
            session.add(row)
        migration = session.get(SchemaMigration, 6)
        assert migration is not None
        session.delete(migration)
        session.commit()
    store.close()
    return StateStore(tmp_path / "saga.sqlite")


def test_upgrade_records_older_items_as_immediate_once(tmp_path: Path) -> None:
    store = legacy_store(tmp_path)
    rows = {
        video: saved(store, video)
        for video in (
            "uploaded000",
            "uploading00",
            "reconcile00",
            "upload00000",
            "1jd2l2k6ZQY",
            "metadata000",
        )
    }
    assert {video: row.derive_policy for video, row in rows.items()} == {
        "uploaded000": "immediate",
        "uploading00": "immediate",
        "reconcile00": "immediate",
        "upload00000": "immediate",
        "1jd2l2k6ZQY": "immediate",
        "metadata000": None,
    }
    assert rows["uploaded000"].derive_status == "unknown"
    assert rows["upload00000"].derive_status is None
    assert rows["1jd2l2k6ZQY"].derive_task_id == 5765169292
    assert job(store, "uploaded000") is not None
    assert job(store, "upload00000") is None
    set_progress(store, "metadata000", derive_policy=None)
    store.close()
    # The one-time migration does not run again.
    store = StateStore(tmp_path / "saga.sqlite")
    assert saved(store, "1jd2l2k6ZQY").derive_task_id == 5765169292
    assert saved(store, "uploaded000").derive_status == "unknown"
    store.close()


def test_reconciliation_reads_each_older_item_once_and_resumes(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    videos = [f"legacy{index:05d}" for index in range(7)]
    for video in videos:
        uploaded(store, tmp_path, video, "immediate")
        set_progress(store, video, derive_policy="immediate", derive_status="unknown")
    with Session(store.engine) as session:
        for video in videos:
            session.add(ArchiveJob(video_id=video, kind="derive"))
        session.commit()
    archive = Archive()
    archive.items = [
        item(derived=True),
        item(tasks=(task(InternetArchiveItemTaskState.PENDING, 0),)),
        item(tasks=(task(InternetArchiveItemTaskState.PENDING, 1),)),
        item(tasks=(task(InternetArchiveItemTaskState.PENDING, 9),)),
        item(tasks=(task(InternetArchiveItemTaskState.ERROR, 2, raw="2"),)),
        item(),
        item(metadata=False),
    ]
    derives = dispatcher(store, tmp_path, archive, archive_derive_backlog_target=0)
    derives.check_round()
    statuses = [saved(store, video).derive_status for video in videos]
    # One pass reads at most five older items.
    assert statuses[:5] == ["done", "requested", "running", "paused", "failed"]
    assert statuses[5:] == ["unknown", "unknown"]
    assert len(archive.calls) == 5
    derives.check_round()
    assert saved(store, videos[5]).derive_status == "pending"
    assert saved(store, videos[6]).derive_status == "unknown"
    assert saved(store, videos[1]).derive_requested_at is not None
    # Only the item that never got a derive waits for the dispatcher.
    pending = job(store, videos[5])
    assert pending is not None and pending.status == "pending"
    assert store.claim_derive("submit") is not None
    store.close()


def test_known_failed_derive_reaches_the_first_report(tmp_path: Path) -> None:
    store = legacy_store(tmp_path)
    archive = Archive()
    archive.items = [
        item(KNOWN, tasks=(task(InternetArchiveItemTaskState.ERROR, 2, raw="2"),))
    ]
    with Session(store.engine) as session:
        for row in session.exec(select(ArchiveJob)).all():
            if row.kind == "derive" and row.video_id != "1jd2l2k6ZQY":
                row.status = "passed"
                session.add(row)
        session.commit()
    dispatcher(store, tmp_path, archive).check_round()
    assert archive.calls == [("item", KNOWN)]
    row = saved(store, "1jd2l2k6ZQY")
    assert (row.derive_status, row.derive_task_id) == ("failed", 5765169292)
    now = datetime.now(UTC)
    store.initialize_report_checkpoint(now - timedelta(days=1), "0 0 * * *")
    report = store.create_report_snapshot(
        now - timedelta(days=1), now, now, derive_policy="deferred"
    )
    assert report.derives_failed == 1
    assert json.loads(report.derive_failures or "[]") == [
        {"identifier": KNOWN, "task_id": 5765169292}
    ]
    text = reporting_text(report)
    assert f"Failed derive: `{KNOWN}` (task 5765169292)" in text
    store.close()


# Operator retry


def test_operator_retry_queues_exactly_one_new_derive(
    store: StateStore, tmp_path: Path
) -> None:
    failed = uploaded(store, tmp_path, "failed00000")
    other = uploaded(store, tmp_path, "otherfail00")
    claimed = uploaded(store, tmp_path, "claimed0000")
    for video in (failed, other, claimed):
        set_progress(
            store, video, derive_status="failed", derive_task_id=5, derive_raw_state="2"
        )
    with Session(store.engine) as session:
        row = session.get(ArchiveJob, (claimed, "derive"))
        assert row is not None
        row.claim_token = "busy"
        session.add(row)
        session.delete(session.get(ArchiveJob, (other, "derive")))
        session.commit()
    identifier = saved(store, failed).ia_identifier
    assert identifier is not None
    assert store.reset_failed_derives([identifier]) == [identifier]
    row = saved(store, failed)
    assert (row.derive_status, row.derive_task_id, row.derive_raw_state) == (
        "pending",
        None,
        None,
    )
    archive = Archive()
    derives = dispatcher(store, tmp_path, archive)
    assert derives.submit_round() == 1
    assert archive.calls == [("queue", identifier)]
    assert derives.submit_round() == 0
    # Every failed derive: one rebuilt job, and a claimed one is left alone.
    assert store.reset_failed_derives() == [saved(store, other).ia_identifier]
    assert saved(store, claimed).derive_status == "failed"
    assert store.reset_failed_derives(["missing"]) == []


def write_env(root: Path) -> None:
    (root / ".env").write_text(f"SAGA_DATA_PATH={root / 'data'}\n", encoding="utf-8")


def test_retry_command_resets_named_or_all_failed_derives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in (
        "SAGA_DATA_PATH",
        "INTERNET_ARCHIVE_EMAIL",
        "INTERNET_ARCHIVE_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    write_env(tmp_path)
    store = StateStore(tmp_path / "data" / "saga.sqlite")
    first = uploaded(store, tmp_path, "first000000")
    second = uploaded(store, tmp_path, "second00000")
    for video in (first, second):
        set_progress(store, video, derive_status="failed")
    store.close()
    output: list[str] = []
    assert (
        maintenance.run_command(tmp_path, ["retry-derives", first], output.append) == 0
    )
    assert (
        maintenance.run_command(tmp_path, ["retry-derives", first], output.append) == 1
    )
    assert (
        maintenance.run_command(tmp_path, ["retry-derives", "--all"], output.append)
        == 0
    )
    assert output[1] == "No failed derives matched"
    assert output[0].startswith("Queued a new derive for a-title-")
    store = StateStore(tmp_path / "data" / "saga.sqlite")
    assert saved(store, second).derive_status == "pending"
    store.close()
    for arguments in (["retry-derives"], ["retry-derives", "x", "--all"], []):
        with pytest.raises(SystemExit):
            maintenance.run_command(tmp_path, arguments, output.append)


# Reports and health


def reporting_text(report: Report) -> str:
    webhook = reporting.build_report_webhook(
        report, "https://discord.com/api/webhooks/1/t"
    )
    components = cast(Any, webhook.components)
    return components[0].components[1].content


def test_report_counts_derives_and_their_lag(store: StateStore, tmp_path: Path) -> None:
    pending = uploaded(store, tmp_path, "pending0000")
    set_progress(store, pending, derive_status_at=utcnow() - timedelta(days=3))
    for video, value in (
        ("requested00", "requested"),
        ("running0000", "running"),
        ("paused00000", "paused"),
        ("done0000000", "done"),
    ):
        uploaded(store, tmp_path, video)
        set_progress(store, video, derive_status=value)
    now = datetime.now(UTC)
    store.initialize_report_checkpoint(now - timedelta(days=1), "0 0 * * *")
    report = store.create_report_snapshot(
        now - timedelta(days=1), now, now, derive_policy="deferred"
    )
    counts = (
        report.derives_pending,
        report.derives_requested,
        report.derives_running,
        report.derives_paused,
        report.derives_done,
        report.derives_failed,
    )
    assert counts == (1, 1, 1, 1, 1, 0)
    assert report.derive_lag_seconds is not None
    assert report.derive_lag_seconds >= 3 * 86_400 - 5
    assert report.derive_failures is None
    assert reporting.report_has_activity(report)
    text = reporting_text(report)
    assert "Derives (deferred): 1 pending, 1 requested, 1 running, 1 paused" in text
    assert "oldest pending 3d" in text


def test_report_omits_derives_without_media_archival() -> None:
    report = Report(
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
        generated_at=datetime(2026, 1, 2, tzinfo=UTC),
        derives_failed=1,
        derive_failures=json.dumps([{"identifier": "item", "task_id": None}]),
    )
    assert reporting.report_has_activity(report)
    assert "Derives" not in reporting_text(report)
    report.derive_policy = "never"
    text = reporting_text(report)
    assert text.endswith("Failed derive: `item`")
    assert "oldest pending" not in text


@pytest.mark.parametrize(
    "seconds,expected", [(59, "0m"), (61, "1m"), (7200, "2h"), (90_000, "1d")]
)
def test_ages_use_their_largest_whole_unit(seconds: float, expected: str) -> None:
    assert reporting.format_age(seconds) == expected


def test_health_alerts_on_stale_held_and_lagging_derives(
    store: StateStore, tmp_path: Path
) -> None:
    old = utcnow() - timedelta(days=2)
    for video, value in (
        ("failed00000", "failed"),
        ("unknown0000", "unknown"),
        ("paused00000", "paused"),
        ("pending0000", "pending"),
        ("recent00000", "failed"),
    ):
        uploaded(store, tmp_path, video)
        set_progress(
            store,
            video,
            derive_status=value,
            derive_status_at=utcnow() if video == "recent00000" else old,
        )
    set_progress(store, "unknown0000", derive_status_at=None)
    health = store.transfer_health(stall_timeout=900, derive_max_lag=86_400)
    alerts = {alert["key"]: alert for alert in health["alerts"]}
    assert alerts["derive:failed"]["details"]["count"] == 1
    assert alerts["derive:paused"]["details"]["derives"][0]["video_id"] == "paused00000"
    assert alerts["derive:lag"]["details"]["pending"] == 1
    assert "derive:authentication" not in alerts
    assert health["derives"]["failed"] == 2
    assert health["healthy"]
    quiet = store.transfer_health(stall_timeout=900)
    assert "derive:lag" not in {alert["key"] for alert in quiet["alerts"]}
    assert "derives" not in store.transfer_health(
        stall_timeout=900, include_archive=False
    )


# Service lane


def test_service_logs_the_option_and_runs_derive_rounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    calls: list[str] = []

    class Derives:
        def submit_round(self) -> int:
            calls.append("submit")
            if len(calls) == 1:
                raise RuntimeError("transient")
            return 0

        def check_round(self) -> None:
            calls.append("check")
            if calls.count("check") == 2:
                raise CooperativeStop("stop")

    service = SagaService(
        settings(tmp_path, archive_task_summary_ttl=0.01),
        (),
        store,
        YouTubeClient(),
        archival=cast(Any, SimpleNamespace(process=lambda _: None, derives=Derives())),
    )
    asyncio.run(service._derive_loop())
    assert calls == ["submit", "submit", "check", "submit", "check"]
    service.stop.set()
    asyncio.run(service._derive_loop())
    store.close()


def test_health_snapshot_and_reports_name_the_derive_option(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    service = SagaService(
        settings(tmp_path, archive_derive="never", archive_derive_backlog_target=5),
        (),
        store,
        YouTubeClient(),
        archival=cast(Any, SimpleNamespace(process=lambda _: None, derives=None)),
    )
    settings(tmp_path).download_root.mkdir()
    health = service._health_snapshot()
    assert health["derives"]["policy"] == "never"
    assert health["derives"]["backlog_target"] == 5
    store.close()


def test_error_text_names_the_request_phase() -> None:
    assert _error_text(NetworkError("x", request_phase="task-status")).endswith(
        "(phase=task-status)"
    )
    assert _error_text(ValueError("y")) == "ValueError: y"


# Follow-ups: pausing requests, stalled item-read derives, state timestamps


def test_paused_requests_leave_checks_running(
    store: StateStore, tmp_path: Path
) -> None:
    pending = uploaded(store, tmp_path, "pending0000")
    tracked = requested(store, tmp_path, "tracked0000")
    archive = Archive()
    archive.statuses = [status(InternetArchiveTaskState.RUNNING)]
    derives = dispatcher(store, tmp_path, archive, archive_derive_paused=True)
    assert derives.submit_round() == 0
    assert cast(Backlog, archive.tasks).reads == 0
    derives.check_round()
    assert saved(store, pending).derive_status == "pending"
    assert saved(store, tracked).derive_status == "running"
    assert archive.calls == [("status", 101)]


def item_tracked(
    store: StateStore, root: Path, video_id: str, days: float, **values: Any
) -> str:
    """Seed a derive seen only as a pending item task, unchanged for some days."""
    values = {
        "derive_task_id": None,
        "derive_status_at": utcnow() - timedelta(days=days),
        **values,
    }
    return requested(store, root, video_id, **values)


@pytest.mark.parametrize("state", ["requested", "running", "unknown"])
def test_item_read_derives_without_files_fail_after_the_stale_days(
    store: StateStore, tmp_path: Path, state: str
) -> None:
    first = item_tracked(store, tmp_path, "first000000", 8, derive_status=state)
    second = item_tracked(store, tmp_path, "second00000", 8, derive_status=state)
    archive = Archive(outstanding=20)
    pending = (task(InternetArchiveItemTaskState.PENDING, 1, raw="1"),)
    archive.items = [item(tasks=pending), item(tasks=pending)]
    messages: list[str] = []
    handler = archival.logger.add(messages.append, level="ERROR", format="{message}")
    try:
        derives = dispatcher(store, tmp_path, archive)
        # The backlog fell to the target before these derives went stale.
        derives._settled_since = datetime.now(UTC) - timedelta(days=8)
        derives.check_round()
        derives.check_round()
    finally:
        archival.logger.remove(handler)
    for video_id in (first, second):
        row, claim = saved(store, video_id), job(store, video_id)
        assert row.derive_status == "failed"
        assert near(row.derive_status_at, datetime.now(UTC))
        assert claim is not None and claim.status == "failed"
        assert claim.last_error is not None
        assert claim.last_error.startswith("DeriveStalled: no derivative files after 7")
    # One error per row, and one backlog read per pass.
    assert len(messages) == 2 and "retry-derives" in messages[0]
    assert cast(Backlog, archive.tasks).reads == 1
    identifier = saved(store, first).ia_identifier
    assert identifier is not None
    assert store.reset_failed_derives([identifier]) == [identifier]
    assert store.claim_derive("submit") is not None


@pytest.mark.parametrize(
    "values,outstanding,found,expected",
    [
        ({}, 21, item(), "unknown"),
        ({}, None, item(), "unknown"),
        ({"days": 6}, 0, item(), "unknown"),
        ({}, 0, item(derived=True), "done"),
        ({"derive_status": "paused"}, 0, item(), "unknown"),
        (
            {"derive_status": "paused"},
            0,
            item(tasks=(task(InternetArchiveItemTaskState.PENDING, 9),)),
            "paused",
        ),
        ({"derive_status_at": None}, 0, item(), "unknown"),
        ({}, 0, item(metadata=False), "requested"),
    ],
)
def test_stalled_timeout_needs_a_settled_backlog_and_no_files(
    store: StateStore,
    tmp_path: Path,
    values: dict[str, Any],
    outstanding: int | None,
    found: InternetArchiveItem,
    expected: str,
) -> None:
    values = dict(values)
    days = values.pop("days", 8)
    video_id = item_tracked(store, tmp_path, "stalled0000", days, **values)
    archive = Archive(outstanding)
    archive.items = [found]
    dispatcher(store, tmp_path, archive).check_round()
    assert saved(store, video_id).derive_status == expected


def test_stalled_timeout_skips_task_ids_and_unread_items(
    store: StateStore, tmp_path: Path
) -> None:
    tracked = requested(
        store,
        tmp_path,
        "tracked0000",
        derive_status="unknown",
        derive_status_at=utcnow() - timedelta(days=30),
    )
    unread = uploaded(store, tmp_path, "unread00000")
    set_progress(
        store,
        unread,
        derive_status="unknown",
        derive_status_at=utcnow() - timedelta(days=30),
    )
    archive = Archive(outstanding=0)
    archive.items = [
        item(),
        item(tasks=(task(InternetArchiveItemTaskState.PENDING, 0),)),
    ]
    dispatcher(store, tmp_path, archive).check_round()
    assert saved(store, tracked).derive_status == "unknown"
    # A first read starts the clock instead of failing an old item at once.
    row = saved(store, unread)
    assert row.derive_status == "requested"
    assert near(row.derive_status_at, datetime.now(UTC))
    assert cast(Backlog, archive.tasks).reads == 0


def test_derive_state_changes_only_with_its_timestamp(
    store: StateStore, tmp_path: Path
) -> None:
    video_id = uploaded(store, tmp_path, "guarded0000")
    for name in ("derive_status", "derive_status_at"):
        with pytest.raises(ValueError, match="Unknown archive fields"):
            store.update_archive(video_id, **cast(dict[str, Any], {name: None}))


def test_upgrade_dates_derive_states_from_the_upload(tmp_path: Path) -> None:
    store = legacy_store(tmp_path)
    row = saved(store, "uploaded000")
    assert row.media_uploaded_at is not None
    assert row.derive_status_at == row.media_uploaded_at
    uploaded_at = utcnow() - timedelta(days=40)
    set_progress(
        store, "uploaded000", derive_status_at=None, media_uploaded_at=uploaded_at
    )
    set_progress(store, "metadata000", derive_status="pending", derive_status_at=None)
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    assert near(saved(store, "uploaded000").derive_status_at, uploaded_at)
    assert near(saved(store, "metadata000").derive_status_at, datetime.now(UTC))
    # Never-read older items wait for reconciliation without a 24-hour alert.
    health = store.transfer_health(stall_timeout=900)
    assert health["derives"]["unread"] == 1
    assert "derive:failed" not in {alert["key"] for alert in health["alerts"]}
    store.close()


def test_startup_log_names_paused_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    service = SagaService(
        settings(tmp_path, archive_derive_paused=True),
        (),
        store,
        YouTubeClient(),
        archival=cast(Any, SimpleNamespace(process=lambda _: None, derives=object())),
    )

    async def idle(*args: Any, **kwargs: Any) -> None:
        return None

    async def initialize() -> tuple[()]:
        return ()

    for name in (
        "_delivery_loop",
        "_report_loop",
        "_archive_loop",
        "_health_loop",
        "_publication_loop",
        "_partial_maintenance_loop",
        "_derive_loop",
        "_cookie_watch_loop",
    ):
        monkeypatch.setattr(service, name, idle)
    monkeypatch.setattr(service, "_initialize", initialize)
    messages: list[str] = []
    handler = archival.logger.add(messages.append, format="{message}")
    service.stop.set()
    try:
        asyncio.run(service.run())
    finally:
        archival.logger.remove(handler)
    expected = "Archive derive option: deferred (backlog target 20; requests paused)"
    assert any(message.startswith(expected) for message in messages)
    settings(tmp_path).download_root.mkdir(exist_ok=True)
    assert service._health_snapshot()["derives"]["requests_paused"] is True
    store.close()


def test_a_long_backlog_restarts_the_stale_clock_when_it_drains(
    store: StateStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core import archive_derives

    start = datetime.now(UTC)
    clock = {"now": start}
    monkeypatch.setattr(archive_derives, "utcnow", lambda: clock["now"])
    video_id = item_tracked(store, tmp_path, "queued00000", 0)
    pending = (task(InternetArchiveItemTaskState.PENDING, 0),)
    archive = Archive(outstanding=25)
    archive.items = [item(tasks=pending) for _ in range(6)]
    derives = dispatcher(store, tmp_path, archive)

    def check(days: float, outstanding: int) -> str | None:
        clock["now"] = start + timedelta(days=days)
        cast(Backlog, archive.tasks).outstanding = outstanding
        due(store, video_id)
        derives.check_round()
        return saved(store, video_id).derive_status

    # The derive waits in a backlog above the target for longer than 7 days.
    assert check(3, 25) == "requested"
    assert check(10, 25) == "requested"
    # Saga sees the backlog drained on day 10.5 and allows 7 more days.
    assert check(10.5, 5) == "requested"
    assert check(17.4, 5) == "requested"
    assert check(17.6, 5) == "failed"


def test_after_a_restart_the_stale_clock_starts_at_the_first_reading(
    store: StateStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core import archive_derives

    start = datetime.now(UTC)
    clock = {"now": start}
    monkeypatch.setattr(archive_derives, "utcnow", lambda: clock["now"])
    video_id = item_tracked(store, tmp_path, "restart0000", 30)
    archive = Archive(outstanding=0)
    archive.items = [item(), item()]
    derives = dispatcher(store, tmp_path, archive)
    # The first submission round reads the backlog and starts the clock.
    assert derives.submit_round() == 0
    derives.check_round()
    assert saved(store, video_id).derive_status == "unknown"
    clock["now"] = start + timedelta(days=7, hours=1)
    due(store, video_id)
    derives.check_round()
    assert saved(store, video_id).derive_status == "failed"
