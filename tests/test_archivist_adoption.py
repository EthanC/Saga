import ast
import pickle
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from archivist import (
    InternetArchiveRecoveryConflictError,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryReason,
    InternetArchiveUploadError,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
    RateLimitError,
)
from sqlmodel import Session, select

from core.archival import ArchiveProcessor, FrozenArchiveMetadata, Inspection
from core.archive_errors import failure_facts
from core.archive_retry import retryable_error
from core.state import ArchiveJob, ArchiveWork, SchemaMigration, StateStore, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, refusal, saved, seed_upload, settings

ARCHIVE_HOST = re.compile(r"(?:^|//|\.)archive\.org", re.IGNORECASE)
CORE = Path(__file__).resolve().parents[1] / "core"
DEFERRED = ("metadata_unresolved", "slot_occupied", "listing_not_authoritative")
CONFLICTS = (
    "ownership_mismatch",
    "item_restricted",
    "size_changed",
    "provenance_missing",
)


class RecoveryGateway(FakeGateway):
    def __init__(self, error: Exception | None = None) -> None:
        super().__init__()
        self.recovery_error = error

    def recover(
        self, metadata: FrozenArchiveMetadata, media_path: Path, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        self.recovery_calls += 1
        if self.recovery_error:
            raise self.recovery_error
        return super().upload(metadata, media_path, **kwargs)


def recovering(
    tmp_path: Path, error: Exception | None
) -> tuple[
    StateStore,
    str,
    Path,
    FrozenArchiveMetadata,
    RecoveryGateway,
    ArchiveProcessor,
    ArchiveWork,
]:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    state.update_archive(video_id, upload_mode="recover")
    gateway = RecoveryGateway(error)
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    return state, video_id, media, frozen, gateway, processor, work


def recovery_job(state: StateStore, video_id: str) -> ArchiveJob | None:
    with Session(state.engine) as session:
        job = session.get(ArchiveJob, (video_id, "recovery"))
        if job is not None:
            session.expunge(job)
        return job


def test_every_reason_is_mapped_to_a_deferral_or_a_conflict() -> None:
    assert {reason.value for reason in InternetArchiveRecoveryReason} == {
        *DEFERRED,
        *CONFLICTS,
    }
    assert all(
        isinstance(refusal(reason), InternetArchiveRecoveryDeferredError)
        for reason in DEFERRED
    )
    assert all(
        isinstance(refusal(reason), InternetArchiveRecoveryConflictError)
        for reason in CONFLICTS
    )


def test_transferred_recovery_is_acknowledged(tmp_path: Path) -> None:
    state, video_id, media, *_, gateway, processor, work = recovering(tmp_path, None)
    processor.process(work)
    row = saved(state, video_id)
    assert gateway.recovery_calls == 1 and gateway.upload_calls == 1
    assert row.media_uploaded_at is not None and row.status == "complete"
    state.close()


@pytest.mark.parametrize("reason", DEFERRED)
def test_deferred_recovery_reaches_reconciliation_not_the_generic_handler(
    tmp_path: Path, reason: str
) -> None:
    error = refusal(reason)
    # Managed workers return failures by pickling them.
    assert pickle.loads(pickle.dumps(error)).reason == error.reason
    state, video_id, media, _, _, processor, work = recovering(tmp_path, error)
    # The dedicated branch handles it; the generic path would re-raise here.
    processor._upload(work)
    state.finish_archive_request(work, base_delay=1)
    row = saved(state, video_id)
    job = recovery_job(state, video_id)
    assert row.status == "reconcile_upload" and media.exists()
    assert (row.error_kind, row.error_status, row.error_phase) == (None, None, None)
    assert row.error_code == reason and row.error_message == f"refused: {reason}"
    assert row.last_error and f"reason={reason}" in row.last_error
    assert row.last_error.startswith("InternetArchiveRecoveryDeferredError:")
    assert job is not None and job.status == "pending"
    assert not retryable_error(error)
    state.close()


@pytest.mark.parametrize("reason", CONFLICTS)
def test_conflicting_recovery_holds_the_original_for_review(
    tmp_path: Path, reason: str
) -> None:
    error = refusal(reason)
    state, video_id, media, _, _, processor, work = recovering(tmp_path, error)
    # The generic handler would hold this transfer for reconciliation instead.
    processor.process(work)
    row = saved(state, video_id)
    assert row.status == "operator_action" and row.failure_phase == "upload"
    assert row.error_code == reason and row.error_kind is None
    assert row.media_uploaded_at is None and media.exists()
    assert recovery_job(state, video_id) is None
    state.close()


@pytest.mark.parametrize("status", [400, 503])
def test_recovery_put_failures_keep_their_upload_error_handling(
    tmp_path: Path, status: int
) -> None:
    state, video_id, media, frozen, gateway, processor, work = recovering(
        tmp_path, None
    )
    gateway.recovery_error = InternetArchiveUploadError(
        InternetArchiveUploadResult(
            frozen.identifier, (InternetArchiveUploadFileResult(frozen.media_name, 4),)
        ),
        InvalidServiceResponseError(
            "rejected",
            service="Internet Archive",
            status_code=status,
            request_phase="file-upload",
        ),
        failed_file=frozen.media_name,
    )
    processor.process(work)
    row = saved(state, video_id)
    assert row.status == ("upload" if status == 400 else "reconcile_upload")
    assert row.upload_mode == "recover" and row.error_phase == "file-upload"
    assert row.error_kind == "service" and row.error_status == status
    assert media.exists()
    state.close()


def slow_down(phase: str) -> RateLimitError:
    return RateLimitError(
        "Please slow down",
        service="Internet Archive",
        status_code=503,
        service_code="SlowDown",
        service_message="Please reduce your request rate.",
        request_phase=phase,
    )


@pytest.mark.parametrize(
    "phase,status", [("item-create", "upload"), ("file-upload", "reconcile_upload")]
)
def test_slow_down_is_known_only_before_the_item_exists(
    tmp_path: Path, phase: str, status: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = InternetArchiveUploadError(
        InternetArchiveUploadResult(frozen.identifier, ()), slow_down(phase)
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway).process(work)
    row = saved(state, video_id)
    assert row.status == status and row.upload_mode == "create" and media.exists()
    assert (row.error_kind, row.error_status, row.error_phase, row.error_code) == (
        "rate_limit",
        503,
        phase,
        "SlowDown",
    )
    assert (recovery_job(state, video_id) is not None) is (status != "upload")
    state.close()


def test_unwrapped_slow_down_stays_throttling(tmp_path: Path) -> None:
    error = slow_down("csrf")
    assert failure_facts(error)["error_kind"] == "rate_limit"
    assert retryable_error(error)
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    gateway.upload_error = error
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway).process(work)
    assert saved(state, video_id).status == "upload"
    state.close()


def test_startup_rechecks_throttled_item_creation_once(tmp_path: Path) -> None:
    path = tmp_path / "saga.sqlite"
    state = StateStore(path)
    throttled, media, frozen = seed_upload(state, tmp_path)
    other, _, other_frozen = seed_upload(state, tmp_path, "ZyXwVuTsR98")
    # Archivist 1.5.2 reported SlowDown as an invalid service response.
    for video_id, identifier, phase in (
        (throttled, frozen.identifier, "item-create"),
        (other, other_frozen.identifier, "file-upload"),
    ):
        gateway = FakeGateway()
        gateway.upload_error = InternetArchiveUploadError(
            InternetArchiveUploadResult(identifier, ()),
            InvalidServiceResponseError(
                "SlowDown",
                service="Internet Archive",
                status_code=503,
                service_code="SlowDown",
                request_phase=phase,
            ),
        )
        work = state.claim_archive(lane="upload", prefer_recent=False)
        assert work is not None and work.video.id == video_id
        ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway).process(
            work
        )
        assert saved(state, video_id).status == "reconcile_upload"
    later = utcnow() + timedelta(hours=6)
    with Session(state.engine) as session:
        for job in session.exec(select(ArchiveJob)).all():
            job.status, job.retry_at = "waiting", later
            session.add(job)
        migration = session.get(SchemaMigration, 5)
        assert migration is not None
        session.delete(migration)
        session.commit()
    state.close()

    state = StateStore(path)
    rechecked = recovery_job(state, throttled)
    untouched = recovery_job(state, other)
    assert rechecked and rechecked.status == "pending"
    assert rechecked.retry_at and rechecked.retry_at.replace(
        tzinfo=None
    ) <= utcnow().replace(tzinfo=None)
    assert untouched and untouched.status == "waiting"
    with Session(state.engine) as session:
        assert session.get(SchemaMigration, 5) is not None
        job = session.get(ArchiveJob, (throttled, "recovery"))
        assert job is not None
        job.status, job.retry_at = "waiting", later
        session.add(job)
        session.commit()
    state.close()

    state = StateStore(path)
    again = recovery_job(state, throttled)
    assert again and again.status == "waiting"
    # Only the item read decides: an absent item goes back to guarded creation.
    with Session(state.engine) as session:
        job = session.get(ArchiveJob, (throttled, "recovery"))
        assert job is not None
        job.retry_at = None
        session.add(job)
        session.commit()
    gateway = FakeGateway()
    gateway.inspection = Inspection("absent", "empty item response")
    work = state.claim_background("recovery")
    assert work is not None and work.video.id == throttled
    ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway).process(work)
    row = saved(state, throttled)
    assert row.status == "upload" and row.upload_mode == "create" and media.exists()
    state.close()


def private_archivist_imports(tree: ast.AST) -> list[str]:
    names = [
        alias.name
        if isinstance(node, ast.Import)
        else f"{node.module or ''}.{alias.name}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for alias in node.names
    ]
    return [
        name
        for name in names
        if name.split(".")[0] == "archivist"
        and any(part.startswith("_") for part in name.split("."))
    ]


def archive_host_literals(tree: ast.AST) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and ARCHIVE_HOST.search(node.value)
    ]


def archivist_client_subclasses(tree: ast.AST) -> list[str]:
    imported = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and (node.module or "").split(".")[0] == "archivist"
        for alias in node.names
    }
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
        and any(
            isinstance(base, ast.Name)
            and base.id in imported
            and base.id.endswith("Client")
            for base in node.bases
        )
    ]


def test_saga_reaches_the_archive_only_through_public_archivist() -> None:
    """Enforce the AGENTS.md rule against direct Internet Archive access."""
    violations: list[str] = []
    for path in sorted(CORE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        violations += [
            f"{path.name}: private import {name}"
            for name in private_archivist_imports(tree)
        ]
        violations += [
            f"{path.name}: Archive host {value!r}"
            for value in archive_host_literals(tree)
        ]
        violations += [
            f"{path.name}: client subclass {name}"
            for name in archivist_client_subclasses(tree)
        ]
    assert not violations, "; ".join(violations)


def test_guardrail_detects_each_kind_of_direct_archive_access() -> None:
    source = (
        "import archivist.core._http",
        "from archivist.services.internet_archive import _items",
        "from archivist import InternetArchiveClient, ServiceError",
        "import niquests",
        "niquests.get('https://s3.us.archive.org/item')",
        "class Extended(InternetArchiveClient): pass",
        "class Failure(ServiceError): pass",
    )
    tree = ast.parse(chr(10).join(source))
    assert private_archivist_imports(tree) == [
        "archivist.core._http",
        "archivist.services.internet_archive._items",
    ]
    assert archive_host_literals(tree) == ["https://s3.us.archive.org/item"]
    assert archivist_client_subclasses(tree) == ["Extended"]
