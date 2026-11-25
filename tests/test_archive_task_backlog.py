import json
import pickle
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveTaskSummary,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadLimit,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from clyde.components import Container, TextDisplay
from sqlmodel import Session

from core import archival
from core.archival import ArchiveGateway, ArchiveProcessor
from core.archive_errors import failure_facts, incident_category, queue_snapshot
from core.archive_retry import ArchiveServiceState, retryable_error
from core.archive_tasks import (
    MAX_BACKLOG_DELAY,
    PresetTasks,
    TaskBacklog,
    TaskSource,
    TaskSummaryCache,
)
from core.reporting import build_report_webhook
from core.state import StateStore, _archive_incident_message, _as_utc
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_archive_retry import clock as clock
from tests.test_archive_retry import gate, processor, upload
from tests.test_archive_upload_limit import wire
from tests.test_reporting import WEBHOOK, report


def summary(
    queued: int, running: int = 0, error: int = 0
) -> InternetArchiveTaskSummary:
    return InternetArchiveTaskSummary(queued, running, error, 0)


def backlog(queued: int, running: int = 0, retry: float = 1.0) -> TaskBacklog:
    return TaskBacklog(queued, running, 0, 0, datetime.now(UTC), retry)


class Clock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value


class Fetches:
    def __init__(self, *values: InternetArchiveTaskSummary | Exception) -> None:
        self.values = list(values)
        self.calls = 0

    def __call__(self) -> InternetArchiveTaskSummary:
        self.calls += 1
        value = self.values.pop(0) if len(self.values) > 1 else self.values[0]
        if isinstance(value, Exception):
            raise value
        return value


def read(cache: TaskSummaryCache, fetch: Any, limit: int = 400) -> TaskBacklog | None:
    return cache.read(fetch, limit=limit, base_delay=300)


def got(value: TaskBacklog | None) -> TaskBacklog:
    assert value is not None
    return value


def patch_summary(
    monkeypatch: pytest.MonkeyPatch, *values: InternetArchiveTaskSummary | Exception
) -> Fetches:
    fetches = Fetches(*values)
    monkeypatch.setattr(
        archival.InternetArchiveClient, "task_summary", lambda self: fetches()
    )
    return fetches


def logs(level: str = "INFO") -> tuple[list[str], int]:
    messages: list[str] = []
    return messages, archival.logger.add(messages.append, level=level)


# Cache


def test_one_summary_request_per_ttl_is_shared_by_concurrent_workers() -> None:
    cache = TaskSummaryCache(60)
    calls: list[int] = []

    def fetch() -> InternetArchiveTaskSummary:
        calls.append(1)
        time.sleep(0.05)
        return summary(10)

    results: list[TaskBacklog | None] = []
    threads = [
        threading.Thread(target=lambda: results.append(read(cache, fetch)))
        for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(calls) == 1
    assert {result.outstanding for result in results if result} == {10}
    assert len(results) == 4


def test_summary_refreshes_after_ttl_and_reports_each_reading() -> None:
    clock = Clock()
    seen: list[TaskBacklog] = []
    cache = TaskSummaryCache(60, clock=clock, listener=seen.append)
    fetch = Fetches(summary(10), summary(5, 2))
    assert cache.latest() is None
    assert got(read(cache, fetch)).outstanding == 10
    clock.value += 59
    assert got(read(cache, fetch)).outstanding == 10
    clock.value += 1
    assert got(read(cache, fetch)).outstanding == 7
    assert fetch.calls == 2
    assert [value.outstanding for value in seen] == [10, 7]
    assert cache.latest() is seen[-1]


@pytest.mark.parametrize(
    "error",
    [
        NetworkError("down"),
        InvalidServiceResponseError("bad counts"),
        RateLimitError("slow", status_code=429),
        ServiceError("busy", status_code=500),
    ],
)
def test_failed_refresh_keeps_a_young_reading_then_skips_the_check(
    error: Exception,
) -> None:
    clock = Clock()
    cache = TaskSummaryCache(60, clock=clock)
    fetch = Fetches(summary(10), error)
    messages, handler = logs("WARNING")
    try:
        assert got(read(cache, fetch)).outstanding == 10
        clock.value += 61
        assert got(read(cache, fetch)).outstanding == 10
        # A failed refresh is not retried by every worker within the TTL.
        clock.value += 30
        assert got(read(cache, fetch)).outstanding == 10
        assert fetch.calls == 2
        clock.value += 30
        assert read(cache, fetch) is None
        assert fetch.calls == 3
    finally:
        archival.logger.remove(handler)
    assert sum("task-summary read failed" in message for message in messages) == 2


def test_failed_first_refresh_skips_the_check() -> None:
    cache = TaskSummaryCache(60)
    assert read(cache, Fetches(NetworkError("down"))) is None


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationError("rejected", status_code=401),
        ServiceError("login", request_phase="login"),
    ],
)
def test_credential_failures_reading_the_summary_are_final(error: Exception) -> None:
    cache = TaskSummaryCache(60)
    with pytest.raises(type(error)):
        read(cache, Fetches(error))


def test_throttled_summary_read_supplies_a_retry_hint() -> None:
    cache = TaskSummaryCache(60)
    assert cache.throttle_hint() == 0
    read(cache, Fetches(RateLimitError("slow", status_code=429)))
    assert cache.throttle_hint() == 0
    cache = TaskSummaryCache(60)
    read(cache, Fetches(RateLimitError("slow", status_code=503, retry_after=120)))
    assert 110 < cache.throttle_hint() <= 120


def test_failed_task_count_is_logged_once_per_change() -> None:
    clock = Clock()
    cache = TaskSummaryCache(1, clock=clock)
    fetch = Fetches(
        summary(1, error=1),
        summary(1, error=1),
        summary(1, error=2),
        summary(1),
        summary(1, error=2),
    )
    messages, handler = logs("WARNING")
    try:
        for _ in range(5):
            read(cache, fetch)
            clock.value += 1
    finally:
        archival.logger.remove(handler)
    failed = [message for message in messages if "failed account tasks" in message]
    assert [message.split("reports ")[1][0] for message in failed] == ["1", "2", "2"]


def test_retry_hint_follows_the_measured_drain_rate() -> None:
    clock = Clock()
    cache = TaskSummaryCache(60, clock=clock)
    # One reading cannot measure a drain rate.
    assert got(read(cache, Fetches(summary(500)), 400)).retry_seconds == 300
    # 500 + 10 added - 450 = 60 drained in 600 s: 0.1 task/s, 51 excess.
    clock.value += 600
    cache.note_write(10)
    hint = got(read(cache, Fetches(summary(450)), 400)).retry_seconds
    assert hint == pytest.approx(510)
    # A larger backlog waits longer, within the upper bound.
    clock.value += 60
    larger = got(read(cache, Fetches(summary(444)), 300)).retry_seconds
    assert hint < larger <= MAX_BACKLOG_DELAY
    # A backlog that does not drain waits the longest.
    clock.value += 60
    assert (
        got(read(cache, Fetches(summary(900)), 400)).retry_seconds == MAX_BACKLOG_DELAY
    )
    # A fast drain never retries sooner than the configured delay.
    clock.value += 60
    assert got(read(cache, Fetches(summary(0)), 1)).retry_seconds == 300


def test_drain_rate_uses_a_bounded_window_of_readings() -> None:
    clock = Clock()
    cache = TaskSummaryCache(1, clock=clock)
    for outstanding in (900, 800, 700):
        read(cache, Fetches(summary(outstanding)))
        clock.value += 3600
    # Older readings leave the window: 100 tasks drained over the last hour.
    assert got(read(cache, Fetches(summary(600)), 599)).retry_seconds == 300
    assert [sample.outstanding for sample in cache._samples] == [700, 600]


def test_simultaneous_readings_have_no_drain_rate() -> None:
    cache = TaskSummaryCache(0, clock=lambda: 5.0)
    read(cache, Fetches(summary(500)))
    assert got(read(cache, Fetches(summary(400)))).retry_seconds == MAX_BACKLOG_DELAY


def test_task_sources_without_a_cache_never_request_a_summary() -> None:
    fetch = Fetches(NetworkError("unused"))
    preset = backlog(5)
    for source, expected in ((TaskSource(), None), (PresetTasks(preset), preset)):
        assert source.read(fetch, limit=1, base_delay=1) is expected
        assert source.latest() is None and source.throttle_hint() == 0
        assert source.note_write(1) is None
    assert fetch.calls == 0
    assert pickle.loads(pickle.dumps(preset)) == preset


# Gate


def test_writes_below_the_limit_are_sent_with_the_task_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    fetches = patch_summary(monkeypatch, summary(398, 1, 1))
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    messages, handler = logs()
    try:
        ArchiveProcessor(
            settings(tmp_path, archive_task_queue_limit=400), store, YouTubeClient()
        ).process(work)
    finally:
        archival.logger.remove(handler)
    assert len(puts) == 2 and fetches.calls == 1
    assert saved(store, video_id).upload_etag == "etag"
    assert any("created an item with 399 outstanding" in m for m in messages)
    assert gate(store).task_queued == 398
    store.close()


def test_backlog_at_the_limit_defers_before_sending_and_keeps_the_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    patch_summary(monkeypatch, summary(423, 3, 1))
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(
        settings(tmp_path, archive_task_queue_limit=426), store, YouTubeClient()
    ).process(work)
    row = saved(store, video_id)
    assert puts == [] and media.exists()
    assert row.status == "upload" and row.claimed_at is None and row.retry_at
    assert (row.error_kind, row.error_phase, row.error_code) == (
        "deferred",
        "item-create",
        "SlowDown",
    )
    assert row.error_message == (
        "Upload deferred before sending: account task backlog 426 >= 426"
    )
    assert json.loads(row.error_queue or "") == {
        "over_limit": 0,
        "task_queued": 423,
        "task_running": 3,
        "task_error": 1,
        "task_limit": 426,
    }
    store.close()


def test_gated_recovery_transfer_stays_pending_without_touching_the_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(store, tmp_path)
    store.update_archive(video_id, upload_mode="recover")
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    patch_summary(monkeypatch, summary(500))
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(
        settings(tmp_path, archive_task_queue_limit=400), store, YouTubeClient()
    ).process(work)
    row = saved(store, video_id)
    assert puts == [] and media.exists()
    assert (row.status, row.upload_mode, row.error_phase) == (
        "upload",
        "recover",
        "file-upload",
    )
    store.close()


@pytest.mark.parametrize(
    "create,state,phase", [(True, None, "item-create"), (False, None, "file-upload")]
)
def test_gated_multipart_writes_defer_before_sending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    create: bool,
    state: str | None,
    phase: str,
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    patch_summary(monkeypatch, summary(500))
    gateway = ArchiveGateway(settings(tmp_path, archive_task_queue_limit=400))
    with pytest.raises(RateLimitError) as result:
        gateway.upload_multipart(
            frozen,
            media,
            create=create,
            state=state,
            progress=lambda value: None,
            heartbeat=lambda: None,
            on_state=lambda value: None,
        )
    assert puts == []
    assert failure_facts(result.value)["error_phase"] == phase
    store.close()


def test_disabled_limit_never_reads_the_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    fetches = patch_summary(monkeypatch, NetworkError("unused"))
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(settings(tmp_path), store, YouTubeClient()).process(work)
    assert fetches.calls == 0
    assert saved(store, video_id).upload_etag == "etag"
    with Session(store.engine) as session:
        assert session.get(ArchiveServiceState, 1) is None
    store.close()


def test_failed_summary_read_sends_the_write_with_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    patch_summary(monkeypatch, NetworkError("down"))
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    messages, handler = logs("WARNING")
    try:
        ArchiveProcessor(
            settings(tmp_path, archive_task_queue_limit=1), store, YouTubeClient()
        ).process(work)
    finally:
        archival.logger.remove(handler)
    assert len(puts) == 2 and saved(store, video_id).upload_etag == "etag"
    assert any("task-summary read failed" in message for message in messages)
    store.close()


def test_rejected_credentials_on_the_summary_read_stop_the_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    puts = wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0})
    patch_summary(monkeypatch, AuthenticationError("rejected", status_code=401))
    gateway = ArchiveGateway(settings(tmp_path, archive_task_queue_limit=1))
    with pytest.raises(AuthenticationError):
        gateway.upload(frozen, media, progress=lambda value: None)
    assert puts == []
    store.close()


def test_throttled_summary_read_paces_a_failed_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0}, create_status=503)
    patch_summary(monkeypatch, RateLimitError("slow", status_code=503, retry_after=900))
    gateway = ArchiveGateway(settings(tmp_path, archive_task_queue_limit=1))
    with pytest.raises(Exception) as result:
        gateway.upload(frozen, media, progress=lambda value: None)
    cause = archival.failure_cause(result.value)
    assert isinstance(cause, RateLimitError)
    assert isinstance(cause.retry_after, float) and 890 < cause.retry_after <= 900
    store.close()


def test_real_slowdown_logs_the_backlog_seen_before_sending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    wire(monkeypatch, frozen.identifier, 200, {"over_limit": 0}, create_status=503)
    patch_summary(monkeypatch, summary(42))
    gateway = ArchiveGateway(settings(tmp_path, archive_task_queue_limit=400))
    messages, handler = logs("WARNING")
    try:
        with pytest.raises(Exception) as result:
            gateway.upload(frozen, media, progress=lambda value: None)
    finally:
        archival.logger.remove(handler)
    cause = archival.failure_cause(result.value)
    assert isinstance(cause, RateLimitError) and cause.retry_after is None
    assert any("SlowDown at item-create with 42 outstanding" in m for m in messages)
    store.close()


def test_write_outcomes_without_a_backlog_or_service_cause_are_not_logged(
    tmp_path: Path,
) -> None:
    gateway = ArchiveGateway(settings(tmp_path, archive_task_queue_limit=400))
    messages, handler = logs()
    try:
        with pytest.raises(OSError):
            gateway._observed(
                "item-create", lambda: (_ for _ in ()).throw(OSError()), "deferred"
            )
        with pytest.raises(RateLimitError):
            gateway._observed(
                "item-create",
                lambda: (_ for _ in ()).throw(
                    RateLimitError("slow", service_code="SlowDown", retry_after=5)
                ),
                "deferred",
            )
        assert gateway._observed("item-create", lambda: 1, "immediate") == 1
        gateway.tasks.read(lambda: summary(3), limit=400, base_delay=1)
        assert gateway._observed("file-upload", lambda: 2, "deferred") == 2
        error = RateLimitError(
            "deferred",
            service_code="SlowDown",
            service_message=archival.DEFERRED_PREFIX + "account task backlog",
        )
        with pytest.raises(RateLimitError):
            gateway._observed(
                "item-create", lambda: (_ for _ in ()).throw(error), "deferred"
            )
        with pytest.raises(ServiceError):
            gateway._observed(
                "item-create",
                lambda: (_ for _ in ()).throw(ServiceError("x")),
                "deferred",
            )
    finally:
        archival.logger.remove(handler)
    assert messages == []
    assert error.retry_after is None


def test_managed_writes_read_the_shared_summary_in_the_managing_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, media, frozen = seed_upload(store, tmp_path)
    store.close()
    calls: list[tuple[str, Any]] = []
    result = InternetArchiveUploadResult(
        frozen.identifier,
        (InternetArchiveUploadFileResult(frozen.media_name, 4, transferred=True),),
    )

    def run(category: str, method: str, options: dict[str, Any], *args: Any) -> Any:
        calls.append((method, options.get("task_backlog")))
        return summary(10) if method == "task_summary" else result

    monkeypatch.setattr(archival, "run_managed", run)
    gateway = ArchiveGateway(
        settings(tmp_path, archive_task_queue_limit=400), managed=True
    )
    gateway.upload(frozen, media, progress=lambda value: None)
    gateway.recover(frozen, media, progress=lambda value: None)
    gateway.upload_multipart(
        frozen,
        media,
        create=True,
        state="{}",
        progress=lambda value: None,
        heartbeat=lambda: None,
        on_state=lambda value: None,
    )
    assert [method for method, _ in calls] == [
        "task_summary",
        "upload",
        "recover",
        "upload_multipart",
    ]
    assert {value.outstanding for _, value in calls[1:]} == {10}
    assert isinstance(gateway.tasks, TaskSummaryCache)
    assert gateway.tasks._added == 3


def test_child_gateway_uses_the_preset_reading_and_reads_summaries_directly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetches = patch_summary(monkeypatch, summary(7))
    preset = backlog(6)
    gateway = ArchiveGateway(
        settings(tmp_path, archive_task_queue_limit=5), tasks=PresetTasks(preset)
    )
    monkeypatch.setattr(
        archival.InternetArchiveClient,
        "upload_limit",
        lambda self, identifier: InternetArchiveUploadLimit(over_limit=False),
    )
    with pytest.raises(RateLimitError) as result:
        gateway._respect_upload_limit(
            gateway._client(gateway._session()), "item", "file-upload"
        )
    assert str(result.value.service_message).endswith("backlog 6 >= 5")
    assert fetches.calls == 0
    assert gateway.task_summary().outstanding == 7
    assert gateway._backlog("item-create") is preset
    assert ArchiveGateway(settings(tmp_path))._backlog("item-create") is None


# Classification, incidents, and observability


def test_backlog_deferrals_survive_pickling_and_classify_separately() -> None:
    error = RateLimitError("deferred", service_code="SlowDown")
    error.__dict__["deferred_backlog"] = True
    error.__dict__["upload_queue"] = {"task_queued": 5, "task_limit": 4, "x": 1}
    facts = failure_facts(pickle.loads(pickle.dumps(error)))
    assert facts["error_kind"] == "deferred"
    assert json.loads(facts["error_queue"]) == {"task_queued": 5, "task_limit": 4}
    assert retryable_error(error)
    assert incident_category("deferred", "SlowDown") == "deferred-backlog"
    assert queue_snapshot({"task_error": 1}) == '{"task_error": 1}'


def test_deferral_messages_name_the_backlog() -> None:
    assert _archive_incident_message(
        "deferred-backlog", {"task_queued": 1200, "task_running": 3, "task_limit": 1000}
    ) == ("Deferred: IA task backlog 1,203 ≥ 1,000")
    assert _archive_incident_message("deferred-backlog", None) == (
        "Deferred: IA task backlog is at its limit"
    )
    assert _archive_incident_message("throttled").startswith("Archive throttled")


def deferral() -> RateLimitError:
    error = RateLimitError(
        "deferred",
        service_code="SlowDown",
        service_message="Upload deferred before sending: account task backlog 9 >= 5",
        request_phase="item-create",
        retry_after=600,
    )
    error.__dict__["deferred_backlog"] = True
    error.__dict__["upload_queue"] = {
        "task_queued": 8,
        "task_running": 1,
        "task_error": 0,
        "task_limit": 5,
    }
    return error


def test_local_deferrals_alert_without_escalating(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = deferral()
    operation = processor(store, tmp_path, gateway)
    for attempt in range(3):
        work = upload(store, clock[0])
        assert work is not None
        operation.process(work)
        assert saved(store, video_id).status == "upload"
        events = store.archive_service_notifications(stall_timeout=30)
        if attempt == 0:
            assert [event["severity"] for event in events] == ["warning"]
            assert events[0]["message"] == "Deferred: IA task backlog 9 ≥ 5"
            assert events[0]["details"]["incident_category"] == "deferred-backlog"
        else:
            assert events == []
        service = gate(store)
        assert service.retry_at is not None
        assert (_as_utc(service.retry_at) - clock[0]).total_seconds() >= 600
        clock[0] = _as_utc(service.retry_at) + timedelta(hours=1)
    service = gate(store)
    assert service.failures == 3 and service.incident_escalated_at is None
    # Only real Archive throttling widens the shared write spacing.
    assert service.write_interval_seconds == 0
    health = store.transfer_health(
        stall_timeout=30, started_at=clock[0] - timedelta(days=1), now=clock[0]
    )
    archive = health["archive_service"]
    assert archive["severity"] == "warning"
    assert archive["account_tasks"] is None
    alert = next(a for a in health["alerts"] if a["key"] == "archive:service")
    assert alert["message"] == "Deferred: IA task backlog 9 ≥ 5"
    store.close()


def test_latest_task_summary_reaches_health_and_reports(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    store.initialize_report_checkpoint(start, "0 0 * * *")
    empty = store.create_report_snapshot(start, end, end)
    assert empty.ia_tasks_outstanding is None and empty.ia_tasks_error is None
    sampled = datetime(2026, 1, 2, 12, tzinfo=UTC)
    store.record_archive_tasks(TaskBacklog(423, 3, 1, 2, sampled, 300))
    tasks = store.transfer_health(stall_timeout=30)["archive_service"]["account_tasks"]
    assert tasks == {
        "queued": 423,
        "running": 3,
        "error": 1,
        "paused": 2,
        "outstanding": 426,
        "sampled_at": sampled.isoformat(),
    }
    later = store.create_report_snapshot(end, end + timedelta(days=1), end)
    assert (later.ia_tasks_outstanding, later.ia_tasks_error) == (426, 1)
    with Session(store.engine) as session:
        assert session.get(ArchiveServiceState, 1) is not None
    store.close()


def test_report_lists_task_counts_only_when_sampled() -> None:
    def text(value: Any) -> str:
        components = build_report_webhook(value, WEBHOOK).components
        assert isinstance(components, list)
        container = components[0]
        assert isinstance(container, Container)
        counts = container.components[1]
        assert isinstance(counts, TextDisplay)
        return counts.content

    assert "IA tasks" not in text(report())
    assert "IA tasks: 1,203 outstanding | 4 failed" in text(
        report(ia_tasks_outstanding=1203, ia_tasks_error=4)
    )
    assert "| 0 failed" in text(report(ia_tasks_outstanding=1))


def test_settings_default_to_an_unenforced_limit(tmp_path: Path) -> None:
    value = settings(tmp_path)
    assert (value.archive_task_queue_limit, value.archive_task_summary_ttl) == (0, 60)
    assert replace(value, archive_task_queue_limit=5).archive_task_queue_limit == 5
