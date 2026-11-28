from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from archivist import InvalidServiceResponseError, NetworkError, RateLimitError
from sqlalchemy import event, text
from sqlmodel import Session

from core import archival
from core import state as state_module
from core.archival import ArchiveProcessor, Inspection
from core.archive_retry import (
    MAX_PROBE_DELAY,
    ArchiveServiceState,
    retry_hint,
    retryable_fields,
)
from core.shutdown import StopController
from core.state import ArchiveJob, StateStore, _as_utc, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_archive_classification import wire


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    value = [utcnow()]
    monkeypatch.setattr(state_module, "utcnow", lambda: value[0])
    monkeypatch.setattr(archival, "utcnow", lambda: value[0])
    monkeypatch.setattr(state_module.random, "uniform", lambda *args: 1.0)
    return value


def gate(store: StateStore) -> ArchiveServiceState:
    with Session(store.engine) as session:
        value = session.get(ArchiveServiceState, 1)
        assert value is not None
        session.expunge(value)
        return value


def failed_upload(store: StateStore, root: Path, video_id: str, now: datetime) -> str:
    identifier, _, _ = seed_upload(store, root, video_id)
    store.update_archive(
        identifier,
        status="upload",
        release=True,
        retry_after=21600,
        last_error="InvalidServiceResponseError: Internet Archive returned HTTP 502",
    )
    return identifier


def processor(
    store: StateStore, root: Path, gateway: FakeGateway, base: float = 300
) -> ArchiveProcessor:
    return ArchiveProcessor(
        replace(settings(root), archive_retry_delay=base),
        store,
        YouTubeClient(),
        gateway,
    )


def upload(store: StateStore, now: datetime) -> Any:
    return store.claim_archive(
        lane="upload", prefer_recent=False, now=now, archive_retry_base=300
    )


def test_production_backlog_prefetches_and_uses_one_probe_before_releasing_retries(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(8):
        failed_upload(store, tmp_path, f"Legacy{index:05}", clock[0])
    fresh, _, _ = seed_upload(store, tmp_path, "FreshVideo1")
    store.update_archive(fresh, status="download", download_completed_at=None)
    work = store.claim_archive(
        lane="download",
        prefer_recent=False,
        now=clock[0],
        free_bytes=1000,
        buffer=1,
        archive_retry_base=300,
    )
    assert work is not None and work.video.id == fresh
    store.update_archive(
        fresh,
        status="upload",
        release=True,
        download_completed_at=clock[0],
        ready_at=clock[0],
    )
    assert store.claim_background("recovery") is None
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: upload(store, clock[0]), range(4)))
    probes = [claim for claim in claims if claim is not None]
    assert len(probes) == 1 and probes[0].archive_probe_token
    gateway = FakeGateway()
    gateway.upload_error = InvalidServiceResponseError("overloaded", status_code=503)
    operation = processor(store, tmp_path, gateway)
    operation.process(probes[0])
    cooldown = gate(store)
    assert cooldown.retry_at is not None
    assert (
        0 < (_as_utc(cooldown.retry_at) - clock[0]).total_seconds() <= MAX_PROBE_DELAY
    )
    assert upload(store, clock[0]) is None
    another, _, _ = seed_upload(store, tmp_path, "NextVideo12")
    store.update_archive(another, status="download", download_completed_at=None)
    prefetch = store.claim_archive(
        lane="download",
        prefer_recent=False,
        now=clock[0],
        free_bytes=1000,
        buffer=1,
        archive_retry_base=300,
    )
    assert prefetch is not None
    store.update_archive(
        another,
        status="upload",
        release=True,
        download_completed_at=clock[0],
        ready_at=clock[0],
    )
    more, _, _ = seed_upload(store, tmp_path, "MoreVideo12")
    store.update_archive(more, status="download", download_completed_at=None)
    assert (
        store.claim_archive(
            lane="download",
            prefer_recent=False,
            free_bytes=1000,
            buffer=1,
            archive_retry_base=300,
        )
        is None
    )
    clock[0] = _as_utc(cooldown.retry_at)
    gateway.upload_error = None
    probe = upload(store, clock[0])
    assert probe is not None and probe.archive_probe_token
    operation.process(probe)
    assert gate(store).failures == 0
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: upload(store, clock[0]), range(4)))
    assert all(
        claim is not None and claim.archive_probe_token is None for claim in claims
    )
    assert gateway.upload_calls == 2
    store.close()


def paced(store: StateStore, now: datetime, **values: Any) -> None:
    with Session(store.engine) as session:
        session.add(
            ArchiveServiceState(
                failures=1, retry_at=now, first_failure_at=now, **values
            )
        )
        session.commit()


def test_recovery_read_ignores_the_write_gate_and_its_transfer_takes_a_probe(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = failed_upload(store, tmp_path, "Recovery123", clock[0])
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    # A due upload used to hold every recovery read behind the paced gate.
    seed_upload(store, tmp_path, "ACompetitor")
    paced(store, clock[0])
    gateway = FakeGateway()
    gateway.inspection = Inspection("missing")
    operation = processor(store, tmp_path, gateway)
    read = store.claim_background("recovery", now=clock[0])
    assert read is not None and read.archive_probe_token is None
    assert read.archive_generation is None
    operation.process(read)
    held = gate(store)
    assert held.failures == 1 and held.probe_token is None
    assert held.retry_at and _as_utc(held.retry_at) == clock[0]
    assert saved(store, video_id).status == "upload"
    transfer = upload(store, clock[0])
    assert (
        transfer is not None
        and transfer.video.id == video_id
        and transfer.archive_probe_token
    )
    assert upload(store, clock[0]) is None
    operation.process(transfer)
    assert saved(store, video_id).media_uploaded_at and gate(store).failures == 0
    assert gateway.recovery_calls == 1
    store.close()


def test_pending_recovery_read_backs_off_on_its_own_schedule(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = failed_upload(store, tmp_path, "Recovery123", clock[0])
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    paced(store, clock[0])
    operation = processor(store, tmp_path, FakeGateway())
    work = store.claim_background("recovery", now=clock[0])
    assert work is not None
    operation.process(work)
    state = gate(store)
    assert state.failures == 1 and state.retry_at
    assert _as_utc(state.retry_at) == clock[0]
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "waiting" and job.retry_at
        assert (_as_utc(job.retry_at) - clock[0]).total_seconds() == 300
    assert store.claim_background("recovery", now=clock[0]) is None
    store.close()


def test_restart_fences_old_claims_and_cancellation_does_not_leak_a_probe(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    identifier = failed_upload(store, tmp_path, "Recovery123", clock[0])
    old = upload(store, clock[0])
    assert old is not None
    store.close()
    store = StateStore(path)
    store.reset_interrupted_claims()
    fresh = upload(store, clock[0])
    assert fresh is not None and fresh.archive_probe_token != old.archive_probe_token
    store.finish_archive_request(old, base_delay=300)
    store.defer_transfer(
        old, status="reconcile_upload", error="late failure", delay=0, deadline=1
    )
    assert gate(store).probe_token == fresh.archive_probe_token
    assert saved(store, identifier).claimed_at is not None
    controller = StopController()
    controller.request()
    operation = ArchiveProcessor(
        settings(tmp_path),
        store,
        YouTubeClient(),
        FakeGateway(),
        stop_controller=controller,
    )
    operation.process(fresh)
    assert (
        gate(store).probe_token is None and saved(store, identifier).claimed_at is None
    )
    assert upload(store, clock[0]) is not None
    store.close()


def test_failed_probes_cap_backoff_and_respect_retry_after(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_upload(store, tmp_path, "Recovery123", clock[0])
    gateway = FakeGateway()
    gateway.upload_error = InvalidServiceResponseError("overloaded", status_code=503)
    operation = processor(store, tmp_path, gateway)
    for _ in range(5):
        work = upload(store, clock[0])
        assert work is not None
        operation.process(work)
        state = gate(store)
        assert state.retry_at is not None
        assert (
            0 < (_as_utc(state.retry_at) - clock[0]).total_seconds() <= MAX_PROBE_DELAY
        )
        clock[0] = _as_utc(state.retry_at)
    gateway.upload_error = RateLimitError(
        "rate limited", status_code=429, retry_after=3600
    )
    work = upload(store, clock[0])
    assert work is not None
    operation.process(work)
    state = gate(store)
    assert (
        state.retry_at is not None
        and (_as_utc(state.retry_at) - clock[0]).total_seconds() == 3600
    )
    assert upload(store, clock[0] + timedelta(seconds=3599)) is None
    store.close()


def test_late_peer_results_do_not_reset_cooldown_and_longer_hints_are_honored(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(3):
        seed_upload(store, tmp_path, f"Normal{index:05}")
    peers = [upload(store, clock[0]) for _ in range(3)]
    assert all(work is not None for work in peers)
    gateway = FakeGateway()
    gateway.upload_error = NetworkError("offline")
    operation = processor(store, tmp_path, gateway)
    operation.process(peers[0])
    original = gate(store)
    operation.process(peers[1])
    assert (
        gate(store).failures == original.failures
        and gate(store).retry_at == original.retry_at
    )
    gateway.upload_error = None
    operation.process(peers[2])
    assert gate(store).failures == original.failures
    store.close()


@pytest.mark.parametrize("status", [429, 503])
def test_real_http_retry_after_paces_the_shared_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: list[datetime], status: int
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(store, tmp_path)
    wire(
        monkeypatch,
        "file-upload",
        status,
        "Unavailable",
        frozen.identifier,
        retry_after="3600",
    )
    work = upload(store, clock[0])
    assert work is not None
    operation = ArchiveProcessor(settings(tmp_path), store, YouTubeClient())
    operation.process(work)
    retry_at = gate(store).retry_at
    assert (
        retry_at is not None and (_as_utc(retry_at) - clock[0]).total_seconds() == 3600
    )
    store.close()


def test_late_peer_retry_after_invalidates_an_in_flight_probe(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(2):
        seed_upload(store, tmp_path, f"Normal{index:05}")
    first, late = upload(store, clock[0]), upload(store, clock[0])
    assert first is not None and late is not None
    gateway = FakeGateway()
    gateway.upload_error = NetworkError("offline")
    operation = processor(store, tmp_path, gateway)
    operation.process(first)
    due = gate(store).retry_at
    assert due is not None
    clock[0] = _as_utc(due)
    probe = upload(store, clock[0])
    assert probe is not None and probe.archive_probe_token
    gateway.upload_error = RateLimitError("limited", retry_after=3600)
    operation.process(late)
    assert gate(store).probe_token is None
    gateway.upload_error = None
    operation.process(probe)
    assert gate(store).failures > 0
    retry_at = gate(store).retry_at
    assert (
        retry_at is not None and (_as_utc(retry_at) - clock[0]).total_seconds() == 3600
    )
    store.close()


def test_success_releases_service_jobs_but_preserves_item_local_backoff(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(3):
        video_id = failed_upload(store, tmp_path, f"Recovery{index:03}", clock[0])
        store.update_archive(video_id, status="reconcile_upload")
        with Session(store.engine) as session:
            session.add(
                ArchiveJob(
                    video_id=video_id,
                    kind="recovery",
                    status="waiting",
                    retry_at=clock[0] + timedelta(hours=6),
                    last_error="denied"
                    if index == 2
                    else "NetworkError: offline"
                    if index == 1
                    else None,
                    error_kind="permission"
                    if index == 2
                    else "network"
                    if index == 1
                    else None,
                )
            )
            session.commit()
    failed_upload(store, tmp_path, "UploadFirst", clock[0])
    work = upload(store, clock[0])
    assert work is not None
    processor(store, tmp_path, FakeGateway()).process(work)
    with Session(store.engine) as session:
        for index in range(3):
            job = session.get(ArchiveJob, (f"Recovery{index:03}", "recovery"))
            assert job is not None
            assert (job.retry_at is None) == (index != 2)
    store.close()


def test_recovery_reads_wait_for_their_own_retry_time_even_while_paced(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    later, _, _ = seed_upload(store, tmp_path, "LaterVideo1")
    due = failed_upload(store, tmp_path, "DueVideo123", clock[0])
    for name, retry_at in ((later, clock[0] + timedelta(hours=6)), (due, None)):
        store.update_archive(name, status="reconcile_upload")
        with Session(store.engine) as session:
            session.add(ArchiveJob(video_id=name, kind="recovery", retry_at=retry_at))
            session.commit()
    paced(store, clock[0] + timedelta(hours=1))
    work = store.claim_background("recovery", now=clock[0])
    assert work is not None and work.video.id == due
    assert store.claim_background("recovery", now=clock[0]) is None
    store.close()


def test_stale_operations_cannot_change_service_state_and_acknowledgement_wins(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_upload(store, tmp_path, "Recovery123", clock[0])
    work = upload(store, clock[0])
    assert work is not None
    facts: dict[str, Any] = dict(
        base_delay=300,
        hint=0,
        error="offline",
        error_kind="network",
        error_status=None,
        error_phase="login",
    )
    altered = replace(
        work, progress=work.progress.model_copy(update={"operation_id": "other"})
    )
    with pytest.raises(RuntimeError, match="different operation"):
        store.record_archive_failure(altered, **facts)
    store.reset_interrupted_claims()
    fresh = upload(store, clock[0])
    assert fresh is not None
    assert store.record_archive_failure(work, **facts).delay == 0
    assert gate(store).probe_token == fresh.archive_probe_token
    store.acknowledge_upload(fresh, receipt="ack", etag=None)
    assert store.record_archive_failure(fresh, **facts).delay == 0
    store.finish_archive_request(fresh, base_delay=300)
    assert gate(store).failures == 0
    store.close()


def test_recovery_read_failures_back_off_the_job_without_failing_the_gate(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = failed_upload(store, tmp_path, "Recovery123", clock[0])
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    paced(store, clock[0])
    old = store.claim_background("recovery", now=clock[0])
    assert old is not None
    store.reset_interrupted_claims()
    fresh = store.claim_background("recovery", now=clock[0])
    assert fresh is not None
    with pytest.raises(RuntimeError, match="no longer belongs"):
        store.update_job(old, status="passed")

    class FailingGateway(FakeGateway):
        def inspect(self, *args: Any, **kwargs: Any) -> Inspection:
            raise NetworkError("offline")

    before = gate(store)
    processor(store, tmp_path, FailingGateway()).process(fresh)
    after = gate(store)
    assert after.failures == before.failures and after.retry_at == before.retry_at
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        assert job is not None and job.status == "waiting"
        assert job.error_kind == "network" and job.retry_at is not None
    store.close()


def test_claim_returns_its_allocated_generation_even_when_a_peer_changes_the_gate(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    seed_upload(store, tmp_path)
    changed = False

    def change_gate(session: Session) -> None:
        nonlocal changed
        if changed:
            return
        changed = True
        with store.engine.begin() as connection:
            connection.execute(
                text("UPDATE archiveservicestate SET generation = generation + 1")
            )

    event.listen(Session, "after_commit", change_gate)
    try:
        work = upload(store, clock[0])
        assert work is not None and work.archive_generation == 0
        assert work.progress.archive_generation == 0 and gate(store).generation == 1
    finally:
        event.remove(Session, "after_commit", change_gate)
        store.close()


def test_intentional_cooldown_does_not_turn_prefetched_work_into_a_dispatch_stall(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_upload(store, tmp_path, "RetryVideo1", clock[0])
    work = upload(store, clock[0])
    assert work is not None
    gateway = FakeGateway()
    gateway.upload_error = RateLimitError("limited", retry_after=3600)
    processor(store, tmp_path, gateway).process(work)
    seed_upload(store, tmp_path, "NextReady12")
    store.health_started_at = clock[0] - timedelta(hours=1)
    store.transfer_health(stall_timeout=30, now=clock[0])
    health = store.transfer_health(
        stall_timeout=30, now=clock[0] + timedelta(seconds=31)
    )
    assert health["healthy"]
    assert health["lanes"]["upload"]["waiting_reason"] == "Archive service cooldown"
    store.close()


def test_cooldown_does_not_pause_due_recovery_reads(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = failed_upload(store, tmp_path, "Recovery123", clock[0])
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    paced(store, clock[0] + timedelta(hours=1))
    store.health_started_at = clock[0] - timedelta(hours=1)
    health = store.transfer_health(stall_timeout=30, now=clock[0])
    assert health["lanes"]["recovery"]["waiting_reason"] == "awaiting worker dispatch"
    later = store.transfer_health(
        stall_timeout=30, now=clock[0] + timedelta(seconds=31)
    )
    # An undispatched read is now a worker problem, not an expected cooldown.
    assert not later["healthy"]
    assert not any(alert["key"] == "archive:probe-stall" for alert in later["alerts"])
    store.close()


def test_healthy_long_probe_keeps_peers_paused_but_a_stalled_probe_still_fails_health(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_upload(store, tmp_path, "RetryVideo1", clock[0])
    work = upload(store, clock[0])
    assert work is not None
    store.update_archive(
        work.video.id,
        status="uploading",
        transfer_bytes=4,
        transfer_progress_at=clock[0],
    )
    seed_upload(store, tmp_path, "NextReady12")
    store.health_started_at = clock[0] - timedelta(hours=1)
    store.transfer_health(stall_timeout=30, now=clock[0])
    progressed = clock[0] + timedelta(seconds=31)
    store.update_archive(
        work.video.id, transfer_bytes=8, transfer_progress_at=progressed
    )
    assert store.transfer_health(stall_timeout=30, now=progressed)["healthy"]
    assert not store.transfer_health(
        stall_timeout=30, now=progressed + timedelta(seconds=31)
    )["healthy"]
    store.close()


def test_service_probe_prefers_a_small_original_for_prompt_recovery(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    large = failed_upload(store, tmp_path, "ALargeVideo", clock[0])
    small = failed_upload(store, tmp_path, "ZSmallVideo", clock[0])
    store.update_archive(large, media_size=64000)
    store.update_archive(small, media_size=4)
    work = upload(store, clock[0])
    assert work is not None and work.video.id == small
    store.close()


def test_health_reports_cooldown_but_detects_a_missing_retry_dispatch(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    failed_upload(store, tmp_path, "Recovery123", clock[0])
    work = upload(store, clock[0])
    assert work is not None
    gateway = FakeGateway()
    gateway.upload_error = NetworkError("offline")
    operation = processor(store, tmp_path, gateway)
    operation.process(work)
    store.health_started_at = clock[0] - timedelta(hours=1)
    assert store.transfer_health(stall_timeout=30, now=clock[0])["healthy"]
    retry_at = gate(store).retry_at
    assert retry_at is not None
    due = _as_utc(retry_at)
    assert store.transfer_health(stall_timeout=30, now=due)["healthy"]
    stalled = store.transfer_health(stall_timeout=30, now=due + timedelta(seconds=31))
    assert not stalled["healthy"] and any(
        a["key"] == "archive:probe-stall" for a in stalled["alerts"]
    )
    assert stalled["archive_service"]["status"] == "probe ready"
    store.close()


@pytest.mark.parametrize(
    "kind,status,phase,detail,expected",
    [
        ("permission", 403, "file-upload", "denied", False),
        ("service", 400, "item-metadata", "invalid item", False),
        ("service", 400, "login", "bad gateway", True),
        ("service", 408, "file-upload", "timeout", True),
        (None, None, None, "NetworkError: offline", True),
        (None, None, None, "InvalidServiceResponseError: HTTP 503", True),
        (None, None, None, "item metadata is unresolved", False),
    ],
)
def test_retry_policy_does_not_make_local_errors_global(
    kind: str | None, status: int | None, phase: str | None, detail: str, expected: bool
) -> None:
    assert retryable_fields(kind, status, phase, detail) == expected


@pytest.mark.parametrize("value", [0, 3600, "date", "naive", None])
def test_retry_after_supports_relative_and_absolute_hints(
    value: Any, clock: list[datetime]
) -> None:
    now = clock[0]
    hint = (
        now + timedelta(seconds=3600)
        if value == "date"
        else (now + timedelta(seconds=3600)).replace(tzinfo=None)
        if value == "naive"
        else value
    )
    error = RateLimitError("limited", retry_after=hint)
    assert retry_hint(error, now) == (3600 if value in {3600, "date", "naive"} else 0)
