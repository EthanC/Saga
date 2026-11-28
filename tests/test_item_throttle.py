import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    InternetArchiveUploadError,
    InternetArchiveUploadResult,
    RateLimitError,
)
from clyde.components import Container, TextDisplay
from loguru import logger
from sqlmodel import Session

from core import reporting
from core import service as service_module
from core.archival import FrozenArchiveMetadata, Inspection
from core.archive_errors import DEFERRED_PREFIX
from core.archive_retry import (
    ITEM_THROTTLE_BASE,
    ITEM_THROTTLE_CAP,
    ITEM_THROTTLED,
    ArchiveServiceState,
)
from core.service import RECOVERY_READ_INTERVAL, SagaService
from core.state import ArchiveJob, ArchiveProgress, SchemaMigration, StateStore, _as_utc
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, saved, seed_upload, settings
from tests.test_archive_retry import clock as clock
from tests.test_archive_retry import gate, paced, processor, upload

SLOWDOWN = "Please reduce your request rate."
REQUEST = "b37de890-17b9-4fde-b2f2-3209263b5643"
SLOWDOWN_TEXT = (
    "InternetArchiveUploadError: Internet Archive item upload failed; "
    "RateLimitError: Internet Archive item request returned HTTP 503 "
    "(HTTP 503; phase=item-create; reason=SlowDown)"
)


def slowdown(identifier: str) -> InternetArchiveUploadError:
    return InternetArchiveUploadError(
        InternetArchiveUploadResult(identifier, ()),
        RateLimitError(
            SLOWDOWN,
            service="Internet Archive",
            status_code=503,
            service_code="SlowDown",
            service_message=SLOWDOWN,
            request_id=REQUEST,
            request_phase="item-create",
        ),
    )


class PoisonGateway(FakeGateway):
    """Throttle creation of one item while the Archive accepts every other write."""

    def __init__(self, poison: str | None = None, *, everything: bool = False) -> None:
        super().__init__()
        self.poison = poison
        self.everything = everything
        self.poison_calls = 0
        self.inspection = Inspection("absent")

    def upload(
        self, metadata: FrozenArchiveMetadata, media_path: Path, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        if self.everything or metadata.identifier == self.poison:
            self.upload_calls += 1
            self.poison_calls += metadata.identifier == self.poison
            raise slowdown(metadata.identifier)
        return super().upload(metadata, media_path, **kwargs)


def ready(
    store: StateStore, root: Path, video_id: str, size: int = 8, **values: Any
) -> str:
    identifier, media, _ = seed_upload(store, root, video_id)
    media.write_bytes(b"x" * size)
    store.update_archive(identifier, media_size=size, **values)
    return identifier


def uncertain(store: StateStore, root: Path, video_id: str) -> str:
    """Seed an upload held for reconciliation after a throttled item creation."""
    ready(
        store,
        root,
        video_id,
        status="reconcile_upload",
        error_kind="rate_limit",
        error_status=503,
        error_phase="item-create",
        error_code="SlowDown",
        last_error=SLOWDOWN_TEXT,
        retry_attempts=1,
    )
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    return video_id


def identifier(store: StateStore, video_id: str) -> str:
    value = saved(store, video_id).ia_identifier
    assert value is not None
    return value


def job(store: StateStore, video_id: str) -> ArchiveJob:
    with Session(store.engine) as session:
        value = session.get(ArchiveJob, (video_id, "recovery"))
        assert value is not None
        session.expunge(value)
        return value


def warnings() -> tuple[list[str], int]:
    messages: list[str] = []
    return messages, logger.add(messages.append, level="WARNING", format="{message}")


def throttled(messages: list[str], video_id: str) -> int:
    return sum(
        f"Archive throttled item creation for video {video_id} only" in message
        for message in messages
    )


def test_poison_item_backs_off_alone_while_uploads_and_recovery_reads_continue(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    # The poison item is the smallest original, which the old probe always chose.
    poison = ready(store, tmp_path, "PoisonVid01", size=4)
    clock[0] += timedelta(seconds=1)
    others = [ready(store, tmp_path, f"Ready{index:06}") for index in range(2)]
    recovering = [
        uncertain(store, tmp_path, f"Uncertain{index:02}") for index in range(3)
    ]
    gateway = PoisonGateway(identifier(store, poison))
    operation = processor(store, tmp_path, gateway)
    messages, handler = warnings()
    reads_while_paced = 0
    recovered = False
    try:
        for step in range(240):
            work = upload(store, clock[0])
            if work is not None:
                operation.process(work)
            read = store.claim_background("recovery", now=clock[0])
            if read is not None:
                assert read.archive_probe_token is None
                reads_while_paced += gate(store).failures > 0
                operation.process(read)
            failures = gate(store).failures
            # After the first acknowledged probe, the poison never fails the gate.
            assert not (recovered and failures)
            recovered = recovered or failures == 0
            if step % 5 == 4 and step < 200:
                others.append(ready(store, tmp_path, f"Arrival{step:05}"))
            clock[0] += timedelta(seconds=60)
    finally:
        logger.remove(handler)
    # Every recovery read ran while the shared gate was still paced.
    assert reads_while_paced == len(recovering)
    assert all(job(store, video_id).status == "passed" for video_id in recovering)
    assert all(saved(store, video_id).media_uploaded_at for video_id in others)
    assert all(saved(store, video_id).media_uploaded_at for video_id in recovering)
    state = gate(store)
    assert state.failures == 0 and state.probation_until is None
    row = saved(store, poison)
    assert row.media_uploaded_at is None and row.error_kind == ITEM_THROTTLED
    assert row.item_throttle_steps >= 2
    # One shared failure at first, then one write per backoff step.
    assert gateway.poison_calls == row.item_throttle_steps + 1
    assert throttled(messages, poison) == row.item_throttle_steps
    assert row.retry_at is not None
    backoff = min(
        ITEM_THROTTLE_CAP, ITEM_THROTTLE_BASE * 2 ** (row.item_throttle_steps - 1)
    )
    assert row.item_slowdown_at is not None
    assert _as_utc(row.retry_at) == _as_utc(row.item_slowdown_at) + timedelta(
        seconds=backoff
    )
    health = store.transfer_health(stall_timeout=900, now=clock[0])
    assert [item["video_id"] for item in health["item_throttled"]] == [poison]
    assert health["item_throttled"][0]["backoff_steps"] == row.item_throttle_steps
    assert health["healthy"]
    # Once the Archive accepts the item, its throttle history is cleared.
    gateway.poison = None
    clock[0] = _as_utc(row.retry_at)
    work = upload(store, clock[0])
    assert work is not None and work.video.id == poison
    operation.process(work)
    done = saved(store, poison)
    assert done.media_uploaded_at and done.error_kind is None
    assert done.item_slowdown_at is None
    assert done.item_slowdown_probes == done.item_throttle_steps == 0
    store.close()


def test_item_backoff_doubles_to_a_daily_cap(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    poison = ready(store, tmp_path, "PoisonVid01")
    witness = ready(store, tmp_path, "Witness0001")
    store.update_archive(
        witness,
        status="complete",
        media_uploaded_at=clock[0],
        upload_origin="transferred",
    )
    gateway = PoisonGateway(identifier(store, poison))
    operation = processor(store, tmp_path, gateway)
    delays = []
    for _ in range(8):
        # Another item was created after the previous SlowDown each time.
        store.update_archive(witness, media_uploaded_at=clock[0] + timedelta(seconds=1))
        work = upload(store, clock[0] + timedelta(seconds=2))
        assert work is not None and work.video.id == poison
        clock[0] += timedelta(seconds=2)
        operation.process(work)
        row = saved(store, poison)
        assert row.retry_at is not None
        delays.append((_as_utc(row.retry_at) - clock[0]).total_seconds())
        clock[0] = _as_utc(row.retry_at)
    # The very first SlowDown has no earlier one to compare with.
    assert delays[0] <= 900
    assert delays[1:] == [3600, 7200, 14400, 28800, 57600, 86400, 86400]
    assert gate(store).failures == 1
    store.close()


def upload_work(store: StateStore, video_id: str, now: datetime) -> Any:
    work = upload(store, now)
    assert work is not None and work.video.id == video_id
    return work


def test_account_wide_throttling_still_paces_the_shared_gate(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    first = ready(store, tmp_path, "FirstVideo1", size=4)
    second = ready(store, tmp_path, "SecondVide1")
    gateway = PoisonGateway(everything=True)
    operation = processor(store, tmp_path, gateway)
    claimed = []
    for _ in range(6):
        work = upload(store, clock[0])
        assert work is not None
        claimed.append(work.video.id)
        operation.process(work)
        state = gate(store)
        assert state.retry_at is not None
        clock[0] = _as_utc(state.retry_at)
    # Probes alternate between items, and no SlowDown is blamed on one item.
    assert claimed == [first, second] * 3
    assert gate(store).failures == 6
    assert saved(store, first).error_kind == saved(store, second).error_kind
    assert saved(store, first).error_kind == "rate_limit"
    store.close()


def test_saga_deferrals_never_count_as_item_throttling(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = ready(store, tmp_path, "Deferred001")
    witness = ready(store, tmp_path, "Witness0001")
    store.update_archive(video_id, item_slowdown_at=clock[0] - timedelta(hours=1))
    store.update_archive(
        witness, media_uploaded_at=clock[0], upload_origin="transferred"
    )
    work = upload_work(store, video_id, clock[0])
    verdict = store.record_archive_failure(
        work,
        base_delay=300,
        hint=0,
        error="Upload deferred before sending",
        error_kind="rate_limit",
        error_status=None,
        error_phase="item-create",
        error_code="SlowDown",
        error_message=DEFERRED_PREFIX + "over_limit",
    )
    assert not verdict.item_throttled and gate(store).failures == 1
    store.close()


def test_a_recovered_original_does_not_prove_the_archive_accepts_writes(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    poison = ready(store, tmp_path, "PoisonVid01")
    found = ready(store, tmp_path, "FoundVideo1")
    store.update_archive(poison, item_slowdown_at=clock[0] - timedelta(hours=1))
    # A recovery read found this original from an earlier write.
    store.update_archive(
        found, status="complete", media_uploaded_at=clock[0], upload_origin="recovered"
    )
    gateway = PoisonGateway(identifier(store, poison))
    processor(store, tmp_path, gateway).process(upload_work(store, poison, clock[0]))
    assert saved(store, poison).error_kind == "rate_limit"
    assert gate(store).failures == 1
    store.close()


def test_paced_probes_rotate_by_least_recent_attempt_and_skip_the_failed_probe(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    fresh = ready(store, tmp_path, "FreshVideo1", size=4)
    older = ready(
        store,
        tmp_path,
        "OlderTry001",
        size=64,
        upload_started_at=clock[0] - timedelta(hours=2),
    )
    newer = ready(
        store,
        tmp_path,
        "NewerTry001",
        size=4,
        upload_started_at=clock[0] - timedelta(hours=1),
    )
    large = ready(store, tmp_path, "LargeFresh1", size=64)
    paced(store, clock[0], failed_probe_video_id=fresh)
    order = []
    for _ in range(4):
        work = upload(store, clock[0])
        assert work is not None and work.archive_probe_token
        order.append(work.video.id)
        store.finish_archive_request(work, base_delay=300)
    # Never-attempted first (smallest breaks the tie), then the oldest attempt;
    # the item whose probe just failed waits until nothing else is left.
    assert order == [large, older, newer, fresh]
    store.close()


def test_paced_probes_alternate_reconciled_transfers_and_new_uploads(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    plain = [ready(store, tmp_path, f"Plain{index:06}", size=4) for index in range(2)]
    reconciled = [
        ready(store, tmp_path, f"Recon{index:06}", size=64) for index in range(2)
    ]
    with Session(store.engine) as session:
        session.add(
            ArchiveJob(video_id=reconciled[0], kind="recovery", status="passed")
        )
        session.commit()
    # A transfer resuming into an existing item is a recovery write too.
    store.update_archive(reconciled[1], upload_mode="recover")
    paced(store, clock[0])
    order = []
    classes = []
    for _ in range(4):
        work = upload(store, clock[0])
        assert work is not None
        order.append(work.video.id)
        classes.append(gate(store).last_probe_class)
        store.finish_archive_request(work, base_delay=300)
    assert order == [reconciled[0], plain[0], reconciled[1], plain[1]]
    assert classes == ["recovery", "upload", "recovery", "upload"]
    store.close()


def test_one_item_has_one_transfer_until_its_claim_is_released(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = ready(store, tmp_path, "OnlyVideo01")
    with Session(store.engine) as session:
        session.add(ArchiveJob(video_id=video_id, kind="recovery"))
        session.commit()
    with ThreadPoolExecutor(max_workers=4) as workers:
        claims = list(workers.map(lambda _: upload(store, clock[0]), range(4)))
    held = [work for work in claims if work is not None]
    assert len(held) == 1
    # Neither another upload worker nor a recovery read can take the held item.
    assert upload(store, clock[0]) is None
    assert store.claim_background("recovery", now=clock[0]) is None
    store.defer_transfer(
        held[0], status="upload", error="deferred", delay=0, deadline=60
    )
    again = upload(store, clock[0])
    assert again is not None and again.video.id == video_id
    assert again.progress.attempts == held[0].progress.attempts + 1
    store.close()


def production_state(root: Path, now: datetime, *, competitor: bool) -> Path:
    """Rebuild the stalled production database from before this release."""
    path = root / "saga.sqlite"
    store = StateStore(path)
    poison = ready(
        store,
        root,
        "j33Zgb-P_X0",
        size=4,
        attempts=992,
        retry_attempts=990,
        error_kind="rate_limit",
        error_status=503,
        error_phase="item-create",
        error_code="SlowDown",
        error_message=SLOWDOWN,
        error_request_id=REQUEST,
        error_headers="{}",
        last_error=SLOWDOWN_TEXT,
        failure_phase="initial_upload",
        upload_started_at=now - timedelta(seconds=25),
        ready_at=now - timedelta(hours=13),
    )
    store.update_archive(poison, release=True, retry_after=900)
    if competitor:
        ready(store, root, "CYlAN4SrAws")
    for index in range(57):
        uncertain(store, root, f"Recover{index:04}")
    with Session(store.engine) as session:
        session.add(
            ArchiveServiceState(
                generation=3140,
                failures=53,
                retry_at=now + timedelta(seconds=900),
                first_failure_at=now - timedelta(hours=13),
                last_success_at=now - timedelta(hours=13),
                last_probe_at=now,
                error_kind="rate_limit",
                error_status=503,
                error_phase="item-create",
                error_code="SlowDown",
                error_message=SLOWDOWN,
                error_request_id=REQUEST,
                last_error=SLOWDOWN_TEXT,
                incident_id="56b45df8-aa21-488f-9b44-46b5c3fc73f2",
                incident_started_at=now - timedelta(hours=13),
                incident_escalated_at=now - timedelta(hours=13),
                incident_notified_category="throttled",
                probation_until=now + timedelta(seconds=1800),
                write_interval_seconds=900,
            )
        )
        forget_item_throttle_migration(session)
    store.close()
    return path


def forget_item_throttle_migration(session: Session) -> None:
    migration = session.get(SchemaMigration, 7)
    assert migration is not None
    session.delete(migration)
    session.commit()


def deployed(path: Path) -> StateStore:
    store = StateStore(path)
    store.reset_interrupted_claims()
    return store


def test_deploy_recovers_the_stalled_production_state(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = production_state(tmp_path, clock[0], competitor=True)
    store = deployed(path)
    poison = saved(store, "j33Zgb-P_X0")
    assert poison.item_slowdown_probes == 990
    assert poison.item_slowdown_at == poison.updated_at
    assert gate(store).failed_probe_video_id == "j33Zgb-P_X0"
    gateway = PoisonGateway(identifier(store, "j33Zgb-P_X0"))
    operation = processor(store, tmp_path, gateway)
    # All 57 item reads run at once, before the paced gate allows any write.
    reads = 0
    while (read := store.claim_background("recovery", now=clock[0])) is not None:
        operation.process(read)
        reads += 1
    assert reads == 57 and gate(store).failures == 53
    assert all(
        saved(store, f"Recover{index:04}").status == "upload" for index in range(57)
    )
    assert upload(store, clock[0]) is None
    messages, handler = warnings()
    try:
        probes = []
        while gateway.poison_calls == 0 and len(probes) < 120:
            state = gate(store)
            if state.retry_at is not None:
                clock[0] = max(clock[0], _as_utc(state.retry_at))
            work = upload(store, clock[0])
            assert work is not None
            probes.append(work.video.id)
            operation.process(work)
    finally:
        logger.remove(handler)
    # The poison goes last, and its first failure after deploy backs it off alone.
    # Reconciled transfers and new uploads alternate; the poison goes last.
    assert probes == ["Recover0000", "CYlAN4SrAws", "Recover0001", "j33Zgb-P_X0"]
    assert gateway.poison_calls == 1
    poison = saved(store, "j33Zgb-P_X0")
    assert poison.error_kind == ITEM_THROTTLED and poison.item_throttle_steps == 1
    assert poison.retry_at is not None
    assert _as_utc(poison.retry_at) == clock[0] + timedelta(seconds=ITEM_THROTTLE_BASE)
    assert gate(store).failures == 0
    assert throttled(messages, "j33Zgb-P_X0") == 1
    store.close()


def test_deploy_backs_off_a_lone_poison_probe_while_recovery_waits(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = production_state(tmp_path, clock[0], competitor=False)
    store = deployed(path)
    gateway = PoisonGateway(identifier(store, "j33Zgb-P_X0"))
    clock[0] += timedelta(seconds=900)
    work = upload(store, clock[0])
    assert work is not None and work.video.id == "j33Zgb-P_X0"
    processor(store, tmp_path, gateway).process(work)
    poison = saved(store, "j33Zgb-P_X0")
    assert poison.error_kind == ITEM_THROTTLED and poison.item_throttle_steps == 1
    state = gate(store)
    # The item's own failure neither counts against the gate nor holds its probe.
    assert state.failures == 53 and state.probe_token is None
    assert state.retry_at is not None
    assert _as_utc(state.retry_at) == clock[0] + timedelta(seconds=900)
    assert state.incident_id == "56b45df8-aa21-488f-9b44-46b5c3fc73f2"
    store.close()


def test_migration_seeds_slowdown_history_without_a_shared_gate(
    tmp_path: Path, clock: list[datetime]
) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    video_id = ready(
        store,
        tmp_path,
        "LegacyVideo",
        retry_attempts=4,
        error_kind="rate_limit",
        error_phase="item-create",
        error_code="SlowDown",
        last_error=SLOWDOWN_TEXT,
    )
    other = ready(
        store,
        tmp_path,
        "OtherVideo1",
        retry_attempts=2,
        error_kind="rate_limit",
        error_phase="item-create",
        error_code="SlowDown",
        error_request_id="other-request",
        last_error=SLOWDOWN_TEXT,
    )
    with Session(store.engine) as session:
        forget_item_throttle_migration(session)
    store.close()
    store = StateStore(path)
    assert saved(store, video_id).item_slowdown_probes == 4
    assert saved(store, other).item_slowdown_probes == 2
    with Session(store.engine) as session:
        assert session.get(ArchiveServiceState, 1) is None
    store.close()
    # The migration runs once; later restarts leave the history alone.
    store = StateStore(path)
    assert saved(store, video_id).item_slowdown_probes == 4
    store.close()


def test_transfer_report_lists_item_throttled_uploads(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = ready(store, tmp_path, "PoisonVid01", error_kind=ITEM_THROTTLED)
    store.update_archive(video_id, release=True, retry_after=3600)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    store.initialize_report_checkpoint(start, "0 0 * * *")
    report = store.create_report_snapshot(start, start + timedelta(days=1), clock[0])
    assert report.item_throttled == 1
    assert report.item_throttled_items == f'["{identifier(store, video_id)}"]'
    webhook = reporting.build_report_webhook(
        report, "https://discord.com/api/webhooks/1/token"
    )
    assert isinstance(webhook.components, list)
    container = webhook.components[0]
    assert isinstance(container, Container)
    counts = container.components[1]
    assert isinstance(counts, TextDisplay)
    content = counts.content
    assert "Item-throttled uploads: 1" in content
    assert f"Item throttled: `{identifier(store, video_id)}`" in content
    store.close()


def test_recovery_worker_paces_its_reads_and_names_its_lane(
    tmp_path: Path, clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = uncertain(store, tmp_path, "Recovery123")
    gateway = PoisonGateway()
    saga = SagaService(
        settings(tmp_path),
        (),
        store,
        YouTubeClient(),
        archival=processor(store, tmp_path, gateway),
    )
    waits: list[float] = []

    async def wait(seconds: float) -> None:
        waits.append(seconds)
        saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    messages: list[str] = []
    handler = logger.add(messages.append, level="DEBUG", format="{message}")
    try:
        asyncio.run(saga._archive_loop(0, "recovery"))
    finally:
        logger.remove(handler)
    assert waits == [RECOVERY_READ_INTERVAL]
    assert f"Archive recovery worker 0 claimed video {video_id}\n" in messages
    assert saved(store, video_id).status == "upload"
    store.close()


def test_standing_warnings_repeat_only_when_items_change_or_hours_pass(
    tmp_path: Path, clock: list[datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    saga = SagaService(settings(tmp_path), (), store, YouTubeClient())
    now = [0.0]
    monkeypatch.setattr(service_module.time, "monotonic", lambda: now[0])

    def alerts(*affected: str) -> list[dict[str, Any]]:
        return [
            {
                "key": "derive:failed",
                "severity": "warning",
                "message": "derives failed or unconfirmed for more than 24 hours",
                "details": {},
                "affected": list(affected),
                "reminder_seconds": 21_600,
            },
            {
                "key": "download:stall",
                "severity": "warning",
                "message": "download lane stalled",
                "details": {},
            },
        ]

    messages: list[str] = []
    handler = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        logged = []
        for stamp, affected in (
            (0, ("a",)),
            (900, ("a",)),
            (1000, ("a", "b")),
            (1000 + 21_599, ("a", "b")),
            (1000 + 21_600, ("a", "b")),
        ):
            now[0] = stamp
            before = len(messages)
            saga._emit_health_alerts(alerts(*affected))
            logged.append([message.split(":")[0] for message in messages[before:]])
    finally:
        logger.remove(handler)
    standing = "derives failed or unconfirmed for more than 24 hours"
    stall = "download lane stalled"
    assert logged == [[standing, stall], [stall], [standing], [stall], [standing]]
    store.close()


def test_health_alerts_name_the_items_behind_standing_warnings(
    tmp_path: Path, clock: list[datetime]
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    video_id = ready(store, tmp_path, "DeriveVideo")
    store.acknowledge_upload(
        upload_work(store, video_id, clock[0]), receipt="ack", etag=None
    )
    with Session(store.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        progress.derive_status = "failed"
        progress.derive_status_at = clock[0] - timedelta(days=2)
        progress.derive_task_id = 5765169292
        session.add(progress)
        session.commit()
    health = store.transfer_health(stall_timeout=900, now=clock[0])
    alert = next(item for item in health["alerts"] if item["key"] == "derive:failed")
    assert alert["affected"] == [video_id]
    assert alert["reminder_seconds"] == 21_600
    store.close()
