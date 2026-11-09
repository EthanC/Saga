import asyncio
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast
from urllib.error import HTTPError, URLError

import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveApiKey,
    InternetArchiveFailedStatus,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemFile,
    InternetArchiveItemFileSource,
    InternetArchiveItemTask,
    InternetArchiveItemTaskState,
    InternetArchivePendingStatus,
    InternetArchiveSuccessStatus,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
    NetworkError,
)
from niquests import Response
from sqlmodel import Session

from core import archival
from core.archival import (
    ArchiveGateway,
    ArchiveProcessor,
    FrozenArchiveMetadata,
    Inspection,
    archive_identifier,
    freeze_metadata,
    inspect_item,
    submit_ziggy,
)
from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter
from core.config import Settings
from core.service import SagaService
from core.state import ArchiveJob, ArchiveProgress, Channel, StateStore, Video, utcnow
from core.transport import (
    RecoveryConflictError,
    RecoveryDeferredError,
    TransportArchiveClient,
)
from core.youtube import VideoMetadata, YouTubeClient


@pytest.mark.parametrize(
    "outcome", ["success", "disk", "unavailable", "empty", "network"]
)
def test_metadata_and_download_record_completion_without_validation(
    tmp_path: Path, outcome: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="metadata")

    class Youtube:
        def video_metadata(self, video_id: str) -> VideoMetadata:
            if outcome == "unavailable":
                raise archival.VideoUnavailableError("gone")
            return VideoMetadata(
                video_id,
                "Title",
                "Description",
                "20200102",
                "channel",
                "Channel",
                "@Channel",
                "https://example.com/channel",
                frozen.source_url,
                4,
            )

        def download_video(self, video_id: str, directory: Path, **kwargs: Any) -> Path:
            if outcome == "disk":
                raise archival.DiskSpaceError("full")
            if outcome == "network":
                raise RuntimeError("downloader failed")
            directory.mkdir(parents=True)
            kwargs["disk_guard"](4, 4)
            path = directory / "media.mkv"
            path.write_bytes(b"" if outcome == "empty" else b"test")
            return path

    processor = ArchiveProcessor(
        settings(tmp_path), state, cast(Any, Youtube()), FakeGateway()
    )
    work = state.claim_archive(lane="metadata", prefer_recent=False)
    assert work is not None
    processor.process(work)
    if outcome != "unavailable":
        work = state.claim_archive(lane="download", prefer_recent=False, free_bytes=100)
        assert work is not None
        processor.process(work)
    result = saved(state, video_id)
    assert result.claimed_at is None
    if outcome == "success":
        assert result.status == "upload" and result.download_completed_at
        assert result.media_md5 is None and result.media_sha1 is None
    elif outcome == "unavailable":
        assert result.status == "unavailable"
    else:
        assert result.status == "download" and result.retry_at
    state.close()


@pytest.mark.parametrize(
    "outcome",
    [
        "create_rejected",
        "file_rejected",
        "acknowledged_error",
        "missing_ack",
        "recovery_deferred",
    ],
)
def test_transfer_receipts_and_rejections_are_classified(
    tmp_path: Path, outcome: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(state, tmp_path)
    file = InternetArchiveUploadFileResult(
        frozen.media_name, 4, transferred=outcome == "acknowledged_error"
    )
    result = InternetArchiveUploadResult(frozen.identifier, (file,))

    class Gateway(FakeGateway):
        def upload(self, *args: Any, **kwargs: Any) -> InternetArchiveUploadResult:
            if outcome == "missing_ack":
                return result
            if outcome == "recovery_deferred":
                raise RecoveryDeferredError("listing delayed")
            raise InternetArchiveUploadError(
                result,
                InvalidServiceResponseError("status", status_code=400),
                failed_file=frozen.media_name if outcome == "file_rejected" else None,
            )

    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), Gateway()).process(
        work
    )
    row = saved(state, video_id)
    if outcome == "acknowledged_error":
        assert row.media_uploaded_at and row.status == "complete"
    elif outcome in {"missing_ack", "recovery_deferred"}:
        assert row.status == "reconcile_upload"
    else:
        assert row.status == "upload" and row.retry_at
        assert row.upload_mode == (
            "recover" if outcome == "file_rejected" else "create"
        )
        if outcome == "file_rejected":
            gateway = FakeGateway()
            work = state.claim_archive(
                lane="upload", prefer_recent=False, now=utcnow() + timedelta(seconds=10)
            )
            assert work is not None
            ArchiveProcessor(
                settings(tmp_path), state, YouTubeClient(), gateway
            ).process(work)
            assert gateway.recovery_calls == 1
    state.close()


@pytest.mark.parametrize(
    "outcome", ["verified", "conflict", "missing", "disabled", "error", "no_size"]
)
def test_validation_cannot_revoke_acknowledgement(tmp_path: Path, outcome: str) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    processor = ArchiveProcessor(
        settings(tmp_path, validation_enabled=True), state, YouTubeClient(), gateway
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    if outcome == "disabled":
        processor.settings = replace(processor.settings, validation_enabled=False)
    elif outcome == "no_size":
        state.update_archive(video_id, media_size=None)
    elif outcome == "error":
        gateway.inspect = cast(
            Any, lambda *args, **kwargs: (_ for _ in ()).throw(NetworkError("offline"))
        )
    else:
        gateway.inspection = Inspection(outcome)
    run_job(state, processor, "validation")
    assert saved(state, video_id).status == "complete"
    assert saved(state, video_id).media_uploaded_at
    state.close()


@pytest.mark.parametrize("provider", ["ziggy", "wayback", "ziggy_only"])
@pytest.mark.parametrize("commit_failure", [False, True])
def test_webpage_receipt_commit_failure_is_not_resubmitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str, commit_failure: bool
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    configured = settings(tmp_path)
    if provider.startswith("ziggy"):
        configured = replace(configured, ziggy_host="http://ziggy")
    if provider == "ziggy_only":
        configured = replace(
            configured, internet_archive_email=None, internet_archive_password=None
        )
        state.update_archive(video_id, status="metadata", frozen_metadata=None)
        work = state.claim_archive(lane="metadata", prefer_recent=False)
        assert work is not None
        ArchiveProcessor(configured, state, YouTubeClient()).process(work)
    else:
        work = state.claim_archive(lane="upload", prefer_recent=False)
        assert work is not None
        ArchiveProcessor(configured, state, YouTubeClient(), FakeGateway()).process(
            work
        )
    monkeypatch.setattr(
        archival,
        "submit_ziggy",
        lambda *args: {"url": args[2], "receipt_id": "receipt"},
    )
    original = state.update_job

    def update(work: Any, **kwargs: Any) -> None:
        if commit_failure and (
            kwargs.get("wayback_job_id") or kwargs.get("ziggy_receipt")
        ):
            raise RuntimeError("receipt commit failed")
        original(work, **kwargs)

    monkeypatch.setattr(state, "update_job", update)
    processor = ArchiveProcessor(
        configured,
        state,
        YouTubeClient(),
        FakeGateway() if provider != "ziggy_only" else None,
    )
    run_job(state, processor, "webpage")
    with Session(state.engine) as session:
        job = session.get(ArchiveJob, (video_id, "webpage"))
        assert job is not None
        assert job.status == (
            "failed"
            if commit_failure
            else "waiting"
            if provider == "wayback"
            else "passed"
        )
    state.close()


@pytest.mark.parametrize(
    "problem",
    [
        "unacknowledged",
        "outside",
        "media_outside",
        "media_missing_path",
        "wrong_size",
        "unknown_stage",
    ],
)
def test_staging_cleanup_and_upload_invariants(tmp_path: Path, problem: str) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), FakeGateway()
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    if problem == "wrong_size":
        media.write_bytes(b"changed")
        processor.process(work)
        assert saved(state, video_id).status == "operator_action"
    elif problem == "unknown_stage":
        work.progress.status = "unknown"
        processor.process(work)
        assert saved(state, video_id).status == "operator_action"
    else:
        if problem != "unacknowledged":
            processor.process(work)
        else:
            with Session(state.engine) as session:
                session.add(ArchiveJob(video_id=video_id, kind="cleanup"))
                session.commit()
        if problem == "outside":
            state.update_archive(video_id, working_directory=str(tmp_path / "outside"))
        elif problem == "media_outside":
            state.update_archive(video_id, media_path=str(tmp_path / "elsewhere.mkv"))
        elif problem == "media_missing_path":
            state.update_archive(video_id, media_path=None)
        run_job(state, processor, "cleanup")
        assert media.exists() is (problem != "media_missing_path")
    state.close()


def test_gateway_status_and_transport_recovery_delegation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    _, path, frozen = seed_upload(state, tmp_path)
    monkeypatch.setattr(
        TransportArchiveClient, "get_item", lambda *args: valid_item(frozen)
    )
    monkeypatch.setattr(
        TransportArchiveClient, "upload_limit", lambda *args: {"over_limit": 0}
    )
    monkeypatch.setattr(
        TransportArchiveClient,
        "add_file_transport",
        lambda *args, **kwargs: InternetArchiveUploadResult(frozen.identifier, ()),
    )
    monkeypatch.setattr(
        TransportArchiveClient, "submit", lambda *args: SimpleNamespace(job_id="job")
    )
    monkeypatch.setattr(
        TransportArchiveClient,
        "status",
        lambda *args: InternetArchivePendingStatus("job"),
    )
    gateway = ArchiveGateway(settings(tmp_path))
    assert gateway.inspect(frozen, size=4).state == "verified"
    assert (
        gateway.recover(frozen, path, progress=lambda _: None).identifier
        == frozen.identifier
    )
    assert gateway.submit_page(frozen.source_url) == "job"
    assert isinstance(gateway.page_status("job"), InternetArchivePendingStatus)
    clock = iter((0.0, 6.0, 6.0))
    monkeypatch.setattr(archival.time, "monotonic", lambda: next(clock))
    updates: list[int] = []
    with path.open("rb") as file:
        assert archival._ProgressStream(file, updates.append).read(4) == b"test"
    assert updates == [4]
    state.close()


@pytest.mark.parametrize(
    "changes, expected",
    [
        ({"identifier": "wrong"}, "conflict"),
        ({"metadata": {}}, "pending"),
        ({"metadata": {"source": "wrong"}}, "conflict"),
        ({"files": ()}, "missing"),
        ({"files": (), "listing_complete": False}, "pending"),
        ({"files": (), "server_unavailable": True}, "pending"),
        (
            {"files": (), "availability": InternetArchiveItemAvailability.PENDING},
            "pending",
        ),
    ],
)
def test_remote_evidence_requires_the_persisted_original(
    tmp_path: Path, changes: dict[str, Any], expected: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(state, tmp_path)
    item = replace(valid_item(frozen), **changes)
    assert inspect_item(item, frozen, 4).state == expected
    state.close()


@pytest.mark.parametrize(
    "size, md5, sha1, expected",
    [
        (None, None, None, "pending"),
        (4, "wrong", None, "conflict"),
        (4, None, "wrong", "conflict"),
        (4, "md5", "sha1", "verified"),
        (4, None, None, "verified"),
    ],
)
def test_recovery_uses_existing_checksums_without_reading_local_media(
    tmp_path: Path, size: int | None, md5: str | None, sha1: str | None, expected: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(state, tmp_path)
    item = valid_item(frozen)
    item = replace(
        item,
        files=(replace(item.files[0], size=size, md5=md5, sha1=sha1),),
        metadata={
            "source": (frozen.source_url,),
            "saga_operation_id": (frozen.operation_id,),
        },
    )
    assert inspect_item(item, frozen, 4, "md5", "sha1").state == expected
    state.close()


def settings(root: Path, **values: Any) -> Settings:
    return Settings(
        config_path=root / "config.toml",
        data_path=root,
        healthcheck_file=None,
        youtube_cookies_path=None,
        internet_archive_email="user@example.com",
        internet_archive_password="password",
        ziggy_host=None,
        ziggy_identifier=None,
        log_level="INFO",
        minimum_free_bytes=0,
        archive_retry_delay=1,
        unknown_download_bytes=4,
        **values,
    )


def seed_upload(
    state: StateStore, root: Path, video_id: str = "AbCdEfGhI12"
) -> tuple[str, Path, FrozenArchiveMetadata]:
    frozen = freeze_metadata(
        VideoMetadata(
            video_id,
            "A title",
            "Description",
            "20200102",
            "channel",
            "Channel",
            "@Channel",
            "https://youtube.com/channel/channel",
            f"https://www.youtube.com/watch?v={video_id}",
            4,
        )
    )
    directory = root / "downloads" / frozen.operation_id
    directory.mkdir(parents=True)
    media = directory / "media.mkv"
    media.write_bytes(b"test")
    with Session(state.engine) as session:
        if session.get(Channel, "channel") is None:
            session.add(
                Channel(
                    id="channel",
                    title="Channel",
                    canonical_url="https://youtube.com/channel/channel",
                )
            )
            session.commit()
        session.add(
            Video(id=video_id, channel_id="channel", webpage_url=frozen.source_url)
        )
        session.commit()
        session.add(
            ArchiveProgress(
                video_id=video_id,
                status="upload",
                media_path=str(media),
                working_directory=str(directory),
                media_name=frozen.media_name,
                media_size=4,
                frozen_metadata=frozen.to_json(),
                ia_identifier=frozen.identifier,
                operation_id=frozen.operation_id,
                download_completed_at=utcnow(),
                ready_at=utcnow(),
            )
        )
        session.commit()
    return video_id, media, frozen


class FakeGateway:
    def __init__(self) -> None:
        self.upload_calls = 0
        self.recovery_calls = 0
        self.inspect_calls = 0
        self.upload_error: Exception | None = None
        self.inspection = Inspection("pending", "not visible")
        self.page_result: Any = InternetArchivePendingStatus("job")
        self.page_error: Exception | None = None

    def upload(
        self, metadata: FrozenArchiveMetadata, media_path: Path, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        self.upload_calls += 1
        if self.upload_error:
            raise self.upload_error
        kwargs["progress"](media_path.stat().st_size)
        return InternetArchiveUploadResult(
            metadata.identifier,
            (
                InternetArchiveUploadFileResult(
                    metadata.media_name,
                    media_path.stat().st_size,
                    transferred=True,
                    etag="receipt-etag",
                ),
            ),
        )

    def recover(
        self, metadata: FrozenArchiveMetadata, media_path: Path, **kwargs: Any
    ) -> InternetArchiveUploadResult:
        self.recovery_calls += 1
        return self.upload(metadata, media_path, **kwargs)

    def inspect(self, *args: Any, **kwargs: Any) -> Inspection:
        self.inspect_calls += 1
        return self.inspection

    def submit_page(self, url: str) -> str:
        if self.page_error:
            raise self.page_error
        return "job"

    def page_status(self, job_id: str) -> Any:
        return self.page_result


def test_upload_thread_is_reserved_when_discovery_pool_is_busy(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), FakeGateway()
    )
    service = SagaService(
        settings(tmp_path), (), state, YouTubeClient(), archival=processor
    )
    busy, finish = Event(), Event()

    def discovery() -> None:
        busy.set()
        assert finish.wait(5)

    async def scenario() -> None:
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=1)
        )
        polling = asyncio.create_task(asyncio.to_thread(discovery))
        upload = asyncio.create_task(service._archive_loop(0, "upload"))
        try:
            for _ in range(200):
                if busy.is_set() and saved(state, video_id).media_uploaded_at:
                    break
                await asyncio.sleep(0.01)
            assert busy.is_set() and saved(state, video_id).media_uploaded_at
            assert not finish.is_set()
        finally:
            service.request_stop()
            finish.set()
            await asyncio.gather(polling, upload)

    asyncio.run(scenario())
    state.close()


def run_job(
    state: StateStore, processor: ArchiveProcessor, kind: str, **kwargs: Any
) -> None:
    work = state.claim_background(kind, **kwargs)
    assert work is not None
    processor.process(work)


def saved(state: StateStore, video_id: str) -> ArchiveProgress:
    with Session(state.engine) as session:
        result = session.get(ArchiveProgress, video_id)
        assert result is not None
        session.expunge(result)
        return result


def test_acknowledgement_counts_and_cleans_before_derivatives_or_capture(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    start = utcnow() - timedelta(minutes=1)
    state.initialize_report_checkpoint(start, "0 0 * * *")
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    progress = saved(state, video_id)
    assert progress.media_uploaded_at is not None
    assert progress.claimed_at is None
    assert progress.status == "complete"
    assert progress.uploaded_bytes == 4
    assert progress.upload_etag == "receipt-etag"
    assert progress.media_verified_at is None
    assert gateway.inspect_calls == 0
    report = state.create_report_snapshot(
        start, utcnow() + timedelta(seconds=1), utcnow()
    )
    assert (report.media_archived, report.media_bytes, report.semantics_version) == (
        1,
        4,
        2,
    )
    with Session(state.engine) as session:
        assert session.get(ArchiveJob, (video_id, "validation")) is None
    gateway.page_error = NetworkError("capture unavailable")
    run_job(state, processor, "webpage")
    assert saved(state, video_id).status == "complete"
    run_job(state, processor, "cleanup")
    assert not media.parent.exists()
    state.close()


def test_hundreds_of_pending_jobs_have_independent_claims(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    with Session(state.engine) as session:
        for index in range(300):
            session.add(
                Video(
                    id=f"{index:011d}",
                    channel_id="channel",
                    webpage_url="https://example.com",
                )
            )
        session.commit()
        for index in range(300):
            session.add(ArchiveProgress(video_id=f"{index:011d}", status="complete"))
        session.commit()
        for index in range(300):
            session.add(ArchiveJob(video_id=f"{index:011d}", kind="validation"))
        session.add(ArchiveJob(video_id=video_id, kind="validation"))
        session.commit()
    check = state.claim_background("validation")
    assert check is not None
    upload = state.claim_archive(lane="upload", prefer_recent=False)
    assert upload is not None and upload.video.id == video_id
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), FakeGateway()
    )
    processor.process(upload)
    assert saved(state, video_id).media_uploaded_at
    state.close()


@pytest.mark.parametrize(
    "error",
    [
        NetworkError("preflight failed"),
        AuthenticationError("credentials"),
        InternetArchiveUploadError(
            InternetArchiveUploadResult("item", ()), NetworkError("lost response")
        ),
    ],
)
def test_failed_upload_releases_worker_for_next_file(
    tmp_path: Path, error: Exception
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    first_id, _, _ = seed_upload(state, tmp_path)
    second_id, _, _ = seed_upload(state, tmp_path, "SecondVid12")
    gateway = FakeGateway()
    gateway.upload_error = error
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    first = state.claim_archive(lane="upload", prefer_recent=False)
    assert first is not None
    processor.process(first)
    assert saved(state, first_id).claimed_at is None
    second = state.claim_archive(lane="upload", prefer_recent=False)
    assert second is not None and second.video.id == second_id
    state.close()


@pytest.mark.parametrize(
    "moment", ["during_transfer", "after_acknowledgement", "during_cleanup"]
)
def test_restart_recovers_without_duplicate_counts(tmp_path: Path, moment: str) -> None:
    path = tmp_path / "saga.sqlite"
    state = StateStore(path)
    video_id, media, _ = seed_upload(state, tmp_path)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    gateway = FakeGateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    if moment == "during_transfer":
        state.update_archive(video_id, status="uploading", upload_started_at=utcnow())
    else:
        processor.process(work)
        if moment == "during_cleanup":
            cleanup = state.claim_background("cleanup")
            assert cleanup is not None
            shutil.rmtree(media.parent)
    state.close()
    state = StateStore(path)
    state.reset_interrupted_claims()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    if moment == "during_transfer":
        gateway.inspection = Inspection("verified", receipt='{"original":"confirmed"}')
        run_job(state, processor, "recovery")
    run_job(state, processor, "cleanup")
    progress = saved(state, video_id)
    assert progress.media_uploaded_at and progress.cleaned_at
    state.acknowledge_upload(work, receipt="duplicate", etag="duplicate")
    assert saved(state, video_id).upload_receipt == progress.upload_receipt
    start = utcnow() - timedelta(days=1)
    state.initialize_report_checkpoint(start, "0 0 * * *")
    report = state.create_report_snapshot(
        start, utcnow() + timedelta(seconds=1), utcnow()
    )
    assert report.lifetime_media_archived == 1
    assert report.media_archived == (0 if moment == "during_transfer" else 1)
    state.close()


def test_missing_staging_and_unmarked_download_return_to_download(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    media.unlink()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), FakeGateway()
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(state, video_id).status == "download"
    state.update_archive(
        video_id, status="upload", media_path=str(media), download_completed_at=None
    )
    media.write_bytes(b"test")
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(state, video_id).status == "download"
    state.close()


def test_acknowledgement_commit_failure_recovers_saved_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    original = state.acknowledge_upload
    monkeypatch.setattr(
        state,
        "acknowledge_upload",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("commit failed")),
    )
    gateway = FakeGateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(state, video_id).status == "reconcile_upload"
    assert media.exists()
    monkeypatch.setattr(state, "acknowledge_upload", original)
    gateway.inspection = Inspection("verified")
    run_job(state, processor, "recovery", now=utcnow() + timedelta(seconds=10))
    assert saved(state, video_id).media_uploaded_at
    assert gateway.upload_calls == 1
    state.close()


@pytest.mark.parametrize("evidence", ["missing", "pending", "conflict"])
def test_remote_checks_never_read_staging_and_only_enqueue_transfers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, evidence: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    state.defer_transfer(
        work, status="reconcile_upload", error="lost", delay=0, deadline=100
    )
    gateway = FakeGateway()
    gateway.inspection = Inspection(evidence, "detail")
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    monkeypatch.setattr(
        Path,
        "open",
        lambda *args, **kwargs: pytest.fail("status check read a local file"),
    )
    run_job(state, processor, "recovery")
    assert gateway.recovery_calls == 0
    assert (
        saved(state, video_id).status
        == {
            "missing": "upload",
            "pending": "reconcile_upload",
            "conflict": "operator_action",
        }[evidence]
    )
    state.close()


def test_long_download_does_not_take_upload_capacity(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    upload_id, _, _ = seed_upload(state, tmp_path)
    download_id, media, _ = seed_upload(state, tmp_path, "Download123")
    state.update_archive(download_id, status="download")
    started, finish = Event(), Event()

    class Youtube:
        def download_video(self, *args: Any, **kwargs: Any) -> Path:
            started.set()
            assert finish.wait(5)
            return media

    processor = ArchiveProcessor(
        settings(tmp_path), state, cast(Any, Youtube()), FakeGateway()
    )
    service = SagaService(
        settings(tmp_path), (), state, cast(Any, Youtube()), archival=processor
    )

    async def scenario() -> None:
        download = asyncio.create_task(service._archive_loop(0, "download"))
        upload = asyncio.create_task(service._archive_loop(0, "upload"))
        try:
            for _ in range(200):
                if started.is_set() and saved(state, upload_id).media_uploaded_at:
                    break
                await asyncio.sleep(0.01)
            assert started.is_set()
            assert saved(state, upload_id).media_uploaded_at
            assert not finish.is_set()
        finally:
            service.request_stop()
            finish.set()
            await asyncio.gather(download, upload)

    asyncio.run(scenario())
    state.close()


def valid_item(frozen: FrozenArchiveMetadata) -> InternetArchiveItem:
    return InternetArchiveItem(
        frozen.identifier,
        metadata={
            "source": frozen.source_url,
            "saga_operation_id": frozen.operation_id,
        },
        files=(
            InternetArchiveItemFile(
                frozen.media_name, 4, source=InternetArchiveItemFileSource.ORIGINAL
            ),
        ),
        availability=InternetArchiveItemAvailability.AVAILABLE,
        listing_complete=True,
    )


@pytest.mark.parametrize("state", list(InternetArchiveItemTaskState))
def test_original_evidence_ignores_derivative_state(
    tmp_path: Path, state: InternetArchiveItemTaskState
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    _, _, frozen = seed_upload(store, tmp_path)
    item = replace(
        valid_item(frozen),
        tasks=(InternetArchiveItemTask("derive.php", state),),
        server_unavailable=True,
        availability=InternetArchiveItemAvailability.PENDING,
        listing_complete=False,
    )
    assert inspect_item(item, frozen, 4).state == "verified"
    assert inspect_item(item, frozen, 5).state == "conflict"
    assert inspect_item(item, frozen, 4, "md5", validation=True).state == "pending"
    store.close()


@pytest.mark.parametrize(
    "result",
    [
        InternetArchivePendingStatus("job"),
        InternetArchiveFailedStatus("job", "failed"),
        InternetArchiveSuccessStatus(
            "job", "https://example.com", datetime(2020, 1, 1, tzinfo=UTC)
        ),
    ],
)
def test_capture_outcomes_do_not_change_upload_completion(
    tmp_path: Path, result: Any
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    gateway.page_result = result
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    run_job(state, processor, "webpage")
    run_job(state, processor, "webpage", now=utcnow() + timedelta(seconds=10))
    assert saved(state, video_id).status == "complete"
    state.close()


def test_optional_validation_has_finite_budget_and_works_after_cleanup(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    gateway = FakeGateway()
    processor = ArchiveProcessor(
        settings(tmp_path, validation_enabled=True), state, YouTubeClient(), gateway
    )
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    run_job(state, processor, "cleanup")
    run_job(state, processor, "validation")
    assert (
        state.claim_background(
            "validation", now=utcnow() + timedelta(hours=1), max_attempts=1
        )
        is None
    )
    with Session(state.engine) as session:
        job = session.get(ArchiveJob, (video_id, "validation"))
        assert job is not None and job.status == "expired"
    assert saved(state, video_id).media_uploaded_at
    state.close()


def test_metadata_contract_and_identifier() -> None:
    details = VideoMetadata(
        "AbCdEfGhI12",
        "Café: A / Test?",
        "Original text",
        "20260203",
        "channel",
        "Display Name",
        "@Handle",
        "https://youtube.com/channel/channel",
        "https://youtube.com/watch?v=AbCdEfGhI12",
        None,
        datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC),
        True,
        ("video essay",),
    )
    frozen = freeze_metadata(details)
    assert frozen.identifier == "cafe-a-test-AbCdEfGhI12"
    assert "Published: 2026-02-03 04:05:06 UTC" in frozen.description
    assert "Tags: video essay" in frozen.description
    assert FrozenArchiveMetadata.from_json(frozen.to_json()) == frozen
    assert len(archive_identifier("x" * 200, details.id)) == 100
    assert archive_identifier("🎬", details.id) == "video-AbCdEfGhI12"
    with pytest.raises(ValueError):
        FrozenArchiveMetadata.from_json("[]")


def test_gateway_http_upload_reads_media_only_for_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    calls: list[tuple[str, str]] = []
    transferred: list[bytes] = []

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        calls.append((method, url))
        response = Response()
        response.status_code = 200
        response._content = b""
        if url.endswith("csrf-token"):
            response._content = b'{"success":true,"value":{"token":"token"}}'
        elif url.endswith("login/"):
            response._content = b'{"success":true}'
        elif url == "https://archive.org/upload":
            response._content = (
                b'<input type="hidden" class="js-uploader-args" value=\''
                b'{"s3user":{"s3accesskey":"key","s3secretkey":"secret"}}\'>'
            )
        elif method == "POST":
            response._content = json.dumps(
                {"success": True, "identifier": frozen.identifier}
            ).encode()
        elif method == "PUT" and url.endswith(frozen.media_name):
            transferred.append(b"".join(kwargs["data"]))
            response.headers["ETag"] = "etag"
        elif url == "https://s3.us.archive.org":
            assert kwargs["params"]["check_limit"] == "1"
            response._content = b'{"over_limit":0}'
        elif method != "PUT":
            pytest.fail("upload waited for background processing")
        return response

    monkeypatch.setattr(archival.Session, "request", request)
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient())
    work = state.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert saved(state, video_id).upload_etag == "etag"
    assert transferred == [b"test"]
    assert media.exists()
    assert not any("catalogRows" in url for _, url in calls)
    state.close()


@pytest.mark.parametrize("limiting", [False, True])
def test_gateway_sessions_disable_retries(tmp_path: Path, limiting: bool) -> None:
    limiter = ByteLimiter(10 if limiting else 0)
    gateway = ArchiveGateway(settings(tmp_path), limiter)
    with gateway._session() as session:
        for prefix in ("https://", "http://"):
            adapter = session.get_adapter(prefix)
            assert getattr(getattr(adapter, "max_retries", None), "total", None) == 0
            if limiting:
                assert (
                    isinstance(adapter, BandwidthHTTPAdapter)
                    and adapter.limiter is limiter
                )


@pytest.mark.parametrize(
    "remote",
    [
        "missing",
        "pending_task",
        "occupied",
        "wrong_owner",
        "wrong_operation",
        "unresolved",
        "blocked",
        "incomplete",
    ],
)
def test_transport_recovery_is_scoped_and_never_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, remote: str
) -> None:
    path = tmp_path / "media.mkv"
    path.write_bytes(b"test")
    calls: list[str] = []
    expected = {
        "source": "https://youtu.be/AbCdEfGhI12",
        "saga_operation_id": "operation",
    }
    metadata = {"identifier": "item", "uploader": "owner@example.com", **expected}
    document: dict[str, Any] = {"metadata": metadata, "files": []}
    if remote == "pending_task":
        document["tasks"] = [{"cmd": "derive.php", "state": "pending"}]
    elif remote == "occupied":
        document["files"] = [{"name": "media.mkv", "source": "original", "size": "4"}]
    elif remote == "wrong_owner":
        metadata["uploader"] = "someone@example.com"
    elif remote == "wrong_operation":
        metadata["saga_operation_id"] = "other"
    elif remote == "unresolved":
        document = {}
    elif remote == "blocked":
        document["is_dark"] = True
    elif remote == "incomplete":
        document["files_count"] = 3

    def request(
        self: Any, method: str, url: str, *args: Any, **kwargs: Any
    ) -> Response:
        calls.append(method)
        response = Response()
        response.status_code = 200
        if url.endswith("services/user.php"):
            response._content = (
                b'{"success":true,"value":{"email":"owner@example.com"}}'
            )
        elif method == "GET":
            response._content = json.dumps(document).encode()
        else:
            assert b"".join(kwargs["data"]) == b"test"
            assert kwargs["headers"]["If-None-Match"] == "*"
            assert kwargs["headers"]["x-archive-interactive-priority"] == "1"
            response._content = b""
            response.headers["ETag"] = "etag"
        return response

    monkeypatch.setattr(TransportArchiveClient, "_recovery_request", request)
    monkeypatch.setattr(
        "archivist.services.internet_archive._recovery.hash_prepared_files",
        lambda *args: pytest.fail("whole-file checksum read"),
    )
    with TransportArchiveClient(
        api_key=InternetArchiveApiKey("key", "secret")
    ) as client:
        if remote in {"missing", "pending_task"}:
            result = client.add_file_transport(
                "item",
                InternetArchiveUploadFile(path),
                expected_metadata=expected,
                expected_size=4,
            )
            assert result.files[0].transferred and result.files[0].etag == "etag"
            assert calls == ["GET", "GET", "PUT"]
        else:
            with pytest.raises((RecoveryConflictError, RecoveryDeferredError)):
                client.add_file_transport(
                    "item",
                    InternetArchiveUploadFile(path),
                    expected_metadata=expected,
                    expected_size=4,
                )
            assert "PUT" not in calls


@pytest.mark.parametrize(
    "outcome", [202, 400, "bad_json", "bad_receipt", "network", "http_error"]
)
def test_ziggy_receipt_contract(monkeypatch: pytest.MonkeyPatch, outcome: Any) -> None:
    class Reply:
        status = 202 if isinstance(outcome, str) else outcome

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def read(self) -> bytes:
            if outcome == "bad_json":
                return b"bad"
            return json.dumps(
                {
                    "submissions": [
                        {
                            "url": "wrong"
                            if outcome == "bad_receipt"
                            else "https://example.com",
                            "receipt_id": "receipt",
                        }
                    ]
                }
            ).encode()

    def open_url(*args: Any, **kwargs: Any) -> Any:
        if outcome == "network":
            raise URLError("lost")
        if outcome == "http_error":
            raise HTTPError("http://ziggy", 400, "bad", cast(Any, None), None)
        return Reply()

    monkeypatch.setattr(archival, "urlopen", open_url)
    if outcome == 202:
        assert (
            submit_ziggy("http://ziggy", "saga", "https://example.com", 1)["receipt_id"]
            == "receipt"
        )
    else:
        with pytest.raises(RuntimeError):
            submit_ziggy("http://ziggy", "saga", "https://example.com", 1)
