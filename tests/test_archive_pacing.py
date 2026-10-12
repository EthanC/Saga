from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from archivist import RateLimitError
from sqlmodel import Session

from core.archival import Inspection
from core.archive_retry import MAX_PROBE_DELAY, THROTTLE_PROBATION, ArchiveServiceState
from core.state import ArchiveJob, StateStore, _as_utc
from tests.test_archival import FakeGateway, seed_upload
from tests.test_archive_retry import clock as clock
from tests.test_archive_retry import gate, processor, upload


def due(store: StateStore) -> datetime:
    stamp = gate(store).retry_at
    assert stamp is not None
    return _as_utc(stamp)


def test_successful_throttle_probe_resumes_one_paced_write_not_four_workers(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(8):
        seed_upload(store, tmp_path, f"Paced{index:06}")
    gateway = FakeGateway()
    gateway.upload_error = RateLimitError("slow down", retry_after=7200)
    operation = processor(store, tmp_path, gateway)
    first = upload(store, clock[0])
    assert first is not None
    operation.process(first)
    assert due(store) == clock[0] + timedelta(seconds=7200)
    clock[0] = due(store)
    gateway.upload_error = None
    probe = upload(store, clock[0])
    assert probe is not None and probe.archive_probe_token
    operation.process(probe)
    state = gate(store)
    assert state.failures == 0 and state.probation_until
    assert state.write_interval_seconds == 60
    assert upload(store, clock[0]) is None
    health = store.transfer_health(stall_timeout=30, now=clock[0])
    assert health["healthy"] and health["archive_service"]["status"] == "probation"
    deadline = _as_utc(state.probation_until)
    store.close()
    store = StateStore(tmp_path / "saga.sqlite")
    store.reset_interrupted_claims()
    assert upload(store, clock[0]) is None
    clock[0] = due(store)
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: upload(store, clock[0]), range(4)))
    admitted = [work for work in claims if work is not None]
    assert len(admitted) == 1 and admitted[0].archive_probe_token
    # A long-running paced write cannot be bypassed when the probation timer ends.
    clock[0] = deadline
    assert upload(store, clock[0]) is None
    processor(store, tmp_path, gateway).process(admitted[0])
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: upload(store, clock[0]), range(4)))
    assert all(work is not None and work.archive_probe_token is None for work in claims)
    assert (
        gate(store).probation_until is None and gate(store).write_interval_seconds == 0
    )
    store.close()


def test_recurrent_throttling_increases_pacing_until_stable_recovery(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    for index in range(12):
        seed_upload(store, tmp_path, f"Repeat{index:05}")
    gateway = FakeGateway()
    operation = processor(store, tmp_path, gateway)
    for expected in (60, 120, 240, 480, 900, 900):
        gateway.upload_error = RateLimitError("slow down")
        work = upload(store, clock[0])
        assert work is not None
        operation.process(work)
        assert gate(store).write_interval_seconds == expected
        assert (due(store) - clock[0]).total_seconds() <= MAX_PROBE_DELAY
        clock[0] = due(store)
        gateway.upload_error = None
        work = upload(store, clock[0])
        assert work is not None
        operation.process(work)
        clock[0] = due(store)
    store.close()


def test_probation_metadata_handoff_is_fenced_and_does_not_bypass_write_spacing(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(store, tmp_path)
    store.update_archive(video_id, status="reconcile_upload")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.add(
            ArchiveServiceState(
                probation_until=clock[0] + timedelta(seconds=THROTTLE_PROBATION),
                write_interval_seconds=60,
                retry_at=clock[0],
            )
        )
        session.commit()
    gateway = FakeGateway()
    gateway.inspection = Inspection("missing")
    operation = processor(store, tmp_path, gateway)
    work = store.claim_background("recovery", archive_retry_base=300)
    assert work is not None and work.archive_probe_token
    operation.process(work)
    assert store.claim_background("recovery", archive_retry_base=300) is None
    transfer = upload(store, clock[0])
    assert transfer is not None and transfer.archive_probe_token
    operation.process(transfer)
    assert gate(store).failures == 0 and due(store) == clock[0] + timedelta(seconds=60)
    store.close()
