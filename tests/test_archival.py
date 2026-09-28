import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from urllib.error import HTTPError, URLError

import pytest
from archivist import (
    AuthenticationError,
    InternetArchiveChecksumState,
    InternetArchiveFailedStatus,
    InternetArchiveFileDisposition,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemFile,
    InternetArchiveItemFileSource,
    InternetArchiveItemTask,
    InternetArchiveItemTaskState,
    InternetArchivePendingStatus,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryFileResult,
    InternetArchiveRecoveryPhase,
    InternetArchiveRecoveryResult,
    InternetArchiveSuccessStatus,
    InternetArchiveUploadError,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
    NetworkError,
    PollingTimeoutError,
    RateLimitError,
    ServiceError,
)
from niquests import Response
from sqlmodel import Session

from core import archival
from core import state as state_module
from core.archival import (
    ArchiveGateway,
    ArchiveProcessor,
    FrozenArchiveMetadata,
    Inspection,
    SubmissionRejectedError,
    SubmissionUncertainError,
    archive_identifier,
    freeze_metadata,
    inspect_item,
    submit_ziggy,
)
from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter
from core.config import Settings, YouTubeChannelConfig, YouTubeConfig
from core.service import SagaService
from core.state import (
    ArchiveProgress,
    ArchiveWork,
    Channel,
    DiscoveredVideo,
    ResolvedChannel,
    StateStore,
    Video,
    utcnow,
)
from core.youtube import DiskSpaceError, MediaFile, VideoMetadata, YouTubeClient


def settings(tmp_path: Path, *, ziggy: bool = False) -> Settings:
    return Settings(
        config_path=tmp_path / "config.toml",
        data_path=tmp_path,
        healthcheck_file=None,
        youtube_cookies_path=None,
        internet_archive_email="user@example.com",
        internet_archive_password="password",
        ziggy_host="http://ziggy:9449" if ziggy else None,
        ziggy_identifier="saga" if ziggy else None,
        log_level="INFO",
        archive_retry_delay=1,
        archive_poll_interval=1,
        minimum_free_bytes=0,
    )


def seed_upload(
    state: StateStore, root: Path
) -> tuple[str, Path, FrozenArchiveMetadata]:
    channel = ResolvedChannel(
        id="UCaaaaaaaaaaaaaaaaaaaaaa",
        title="Channel",
        handle="@Channel",
        url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    state.register_channel("@Channel", channel)
    config_channel = YouTubeChannelConfig(
        "@Channel", ("https://discord.com/api/webhooks/123/token",)
    )
    state.synchronize_subscriptions(
        [
            (
                YouTubeConfig(name="primary", channels=(config_channel,)),
                ((channel.id, config_channel),),
            )
        ]
    )
    state.establish_baseline(
        channel.id,
        [
            DiscoveredVideo(
                id="AbCdEfGhI12",
                title="Video",
                published_at=datetime(2020, 1, 2, tzinfo=UTC),
                live_status="not_live",
                replay_ready=True,
            )
        ],
    )
    frozen = freeze_metadata(
        VideoMetadata(
            id="AbCdEfGhI12",
            title="A title",
            description="Description",
            upload_date="20200102",
            channel_id=channel.id,
            channel_name="Channel",
            channel_handle="@Channel",
            channel_url=channel.url,
            webpage_url="https://www.youtube.com/watch?v=AbCdEfGhI12",
            estimated_bytes=4,
        )
    )
    directory = root / "downloads" / frozen.operation_id
    directory.mkdir(parents=True)
    media = directory / "media.mkv"
    media.write_bytes(b"test")
    state.update_archive(
        frozen.source_url.rsplit("=", 1)[1],
        status="upload",
        working_directory=str(directory),
        media_path=str(media),
        media_name=frozen.media_name,
        media_size=4,
        media_md5="098f6bcd4621d373cade4e832627b4f6",
        media_sha1="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        frozen_metadata=frozen.to_json(),
        ia_identifier=frozen.identifier,
        operation_id=frozen.operation_id,
    )
    return "AbCdEfGhI12", media, frozen


class FakeGateway:
    def __init__(self) -> None:
        self.upload_calls = 0
        self.inspection = Inspection("pending", "not visible")
        self.recovery: InternetArchiveRecoveryResult | None = None
        self.recovery_error: Exception | None = None
        self.recovery_calls = 0

    def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
        self.upload_calls += 1
        raise InternetArchiveUploadError(
            InternetArchiveUploadResult(metadata.identifier, ()),
            NetworkError("lost response"),
        )

    def inspect(self, *args: Any, **kwargs: Any) -> Inspection:
        return self.inspection

    def recover(
        self, metadata: FrozenArchiveMetadata, media_path: Path
    ) -> InternetArchiveRecoveryResult:
        self.recovery_calls += 1
        if self.recovery_error is not None:
            raise self.recovery_error
        assert self.recovery is not None
        return self.recovery

    def submit_page(self, url: str) -> str:
        return "job-id"

    def page_status(self, job_id: str) -> Any:
        raise AssertionError("not used")


def test_single_worker_finishes_staged_media_before_metadata_backlog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(state_module, "utcnow", lambda: now)
    monkeypatch.setattr(archival, "utcnow", lambda: now)
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="download")
    backlog = tuple(
        DiscoveredVideo(
            id=f"{index:011d}",
            title=f"Backlog {index}",
            published_at=datetime(2019, 1, index + 1, tzinfo=UTC),
            live_status="not_live",
            replay_ready=True,
        )
        for index in range(3)
    )
    state.record_discoveries("UCaaaaaaaaaaaaaaaaaaaaaa", backlog, backfill=True)
    with Session(state.engine) as session:
        staged = session.get(ArchiveProgress, video_id)
        assert staged is not None
        staged.updated_at = now
        session.add(staged)
        for item in backlog:
            progress = session.get(ArchiveProgress, item.id)
            assert progress is not None
            progress.updated_at = datetime(2020, 1, 1, tzinfo=UTC)
            session.add(progress)
            video = session.get(Video, item.id)
            assert video is not None
            video.notification_candidate = True
            session.add(video)
        session.commit()

    class Youtube:
        def download_video(self, *_: Any, **__: Any) -> Path:
            return media

    class CompletingGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.inspection = Inspection("verified")

        def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
            with Session(state.engine) as session:
                progress = session.get(ArchiveProgress, video_id)
                assert progress is not None
                assert progress.status == "uploading"
                assert progress.claimed_at is not None
            super().upload(metadata, media_path)

        def page_status(self, job_id: str) -> InternetArchiveSuccessStatus:
            return InternetArchiveSuccessStatus(
                job_id, "https://web.archive.org/example", utcnow()
            )

    monkeypatch.setattr(
        archival,
        "verify_media",
        lambda path: MediaFile(
            path,
            4,
            "098f6bcd4621d373cade4e832627b4f6",
            "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        ),
    )
    gateway = CompletingGateway()
    configured = settings(tmp_path)
    youtube = cast(Any, Youtube())
    processor = ArchiveProcessor(configured, state, youtube, gateway)
    service = SagaService(configured, (), state, youtube, archival=processor)
    stages = (
        "download",
        "upload",
        "reconcile_upload",
        "webpage",
        "wayback_wait",
        "cleanup",
    )
    claimed: list[tuple[str, str]] = []
    claim_archive = state.claim_archive

    def claim_next(*, prefer_recent: bool) -> ArchiveWork | None:
        nonlocal now
        if len(claimed) == len(stages):
            service.request_stop()
            return None
        # Let persisted retry and polling deadlines pass between worker iterations.
        now += timedelta(seconds=2)
        work = claim_archive(prefer_recent=prefer_recent)
        assert work is not None
        claimed.append((work.video.id, work.progress.status))
        return work

    monkeypatch.setattr(state, "claim_archive", claim_next)
    asyncio.run(service._archive_loop(0))

    assert claimed == [(video_id, stage) for stage in stages]
    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "complete"
        assert progress.media_verified_at is not None
        assert progress.cleaned_at is not None
        for item in backlog:
            queued = session.get(ArchiveProgress, item.id)
            assert queued is not None
            assert queued.status == "metadata"
            assert queued.attempts == 0
    assert gateway.upload_calls == 1
    assert not media.exists()
    next_work = claim_archive(prefer_recent=False)
    assert next_work is not None
    assert next_work.progress.status == "metadata"
    state.close()


def test_metadata_and_identifier_preserve_video_id_case() -> None:
    details = VideoMetadata(
        id="AbCdEfGhI12",
        title="Caf\u00e9: A / Test?",
        description="Original text",
        upload_date="20260203",
        channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
        channel_name="Display Name",
        channel_handle="@Handle",
        channel_url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
        webpage_url="https://www.youtube.com/watch?v=AbCdEfGhI12",
        estimated_bytes=None,
    )

    frozen = freeze_metadata(details)

    assert frozen.identifier == "cafe-a-test-AbCdEfGhI12"
    assert frozen.title == "Caf\u00e9: A / Test? | @Handle | YouTube"
    assert frozen.subjects == ("youtube", "video", "@Handle", "AbCdEfGhI12")
    assert "Original upload date: 2026-02-03" in frozen.description
    assert archive_identifier("x" * 200, details.id).endswith("-AbCdEfGhI12")
    assert len(archive_identifier("x" * 200, details.id)) == 100


def test_item_inspection_requires_provenance_and_all_checksums() -> None:
    metadata = FrozenArchiveMetadata(
        identifier="video-AbCdEfGhI12",
        operation_id="operation",
        source_url="https://www.youtube.com/watch?v=AbCdEfGhI12",
        media_name="video-AbCdEfGhI12.mkv",
        title="Title",
        description="Description",
        subjects=("youtube",),
        creator="Creator",
        upload_date="2020-01-02",
        channel_username="@Channel",
        estimated_bytes=None,
    )
    item = InternetArchiveItem(
        identifier=metadata.identifier,
        metadata={
            "source": metadata.source_url,
            "saga_operation_id": metadata.operation_id,
        },
        files=(
            InternetArchiveItemFile(
                name=metadata.media_name,
                size=4,
                md5="md5",
                sha1="sha1",
                source=InternetArchiveItemFileSource.ORIGINAL,
            ),
        ),
        availability=InternetArchiveItemAvailability.AVAILABLE,
        listing_complete=True,
    )

    assert inspect_item(item, metadata, 4, "md5", "sha1").state == "verified"
    assert inspect_item(item, metadata, 4, "wrong", "sha1").state == "conflict"
    unresolved = InternetArchiveItem(identifier=metadata.identifier)
    assert inspect_item(unresolved, metadata, 4, "md5", "sha1").state == "pending"
    failed_task = InternetArchiveItem(
        identifier=metadata.identifier,
        metadata=item.metadata,
        files=item.files,
        tasks=(
            InternetArchiveItemTask(
                command="archive.php",
                state=InternetArchiveItemTaskState.ERROR,
                error="failed",
            ),
        ),
        availability=InternetArchiveItemAvailability.AVAILABLE,
        listing_complete=True,
    )
    assert inspect_item(failed_task, metadata, 4, "md5", "sha1").state == "conflict"


@pytest.mark.parametrize("status_code", [400, 403, 429])
def test_rejected_creation_retries_with_archivist_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status_code: int
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    create_url = f"https://s3.us.archive.org/{frozen.identifier}"
    create_requests: list[dict[str, Any]] = []
    transfers: list[bytes] = []
    responses = {
        ("GET", "https://archive.org/services/csrf-token"): (
            b'{"success":true,"value":{"token":"token"}}'
        ),
        ("POST", "https://archive.org/services/account/login/"): b'{"success":true}',
        ("GET", "https://archive.org/upload"): (
            b'<input type="hidden" class="js-uploader-args" value=\''
            b'{"s3user":{"s3accesskey":"access","s3secretkey":"secret"}}\'>'
        ),
        ("POST", "https://archive.org/upload/app/upload_api.php"): json.dumps(
            {"success": True, "identifier": frozen.identifier}
        ).encode(),
        ("GET", "https://archive.org/upload/app/upload_api.php"): (
            b'{"success":true,"rows":[]}'
        ),
    }

    def request(self: Any, method: str, url: str, **kwargs: Any) -> Response:
        response = Response()
        response.status_code = 200
        response._content = b""
        if (method, url) == ("PUT", create_url):
            create_requests.append(kwargs)
            assert kwargs["data"] == b""
            assert kwargs["allow_redirects"] is False
            if len(create_requests) == 1:
                response.status_code = status_code
                response.headers["Retry-After"] = "1"
        elif (method, url) == ("PUT", f"{create_url}/{frozen.media_name}"):
            transfers.append(b"".join(kwargs["data"]))
        else:
            response._content = responses[method, url]
        return response

    monkeypatch.setattr(archival.Session, "request", request)
    gateway = ArchiveGateway(settings(tmp_path))
    monkeypatch.setattr(
        gateway, "inspect", lambda *args, **kwargs: Inspection("verified")
    )
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    first = state.claim_archive(prefer_recent=False)
    assert first is not None

    processor.process(first)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "upload"
        assert progress.failure_phase == "initial_upload"
        assert (progress.last_error or "").startswith("InternetArchiveUploadError:")
        assert progress.claimed_at is None
        assert progress.media_verified_at is None
        retry_at = progress.retry_at
        assert retry_at is not None
    assert len(create_requests) == 1
    assert transfers == []
    assert media.read_bytes() == b"test"
    assert (
        state.claim_archive(
            prefer_recent=False, now=retry_at - timedelta(microseconds=1)
        )
        is None
    )

    retry = state.claim_archive(prefer_recent=False, now=retry_at)
    assert retry is not None
    assert retry.progress.status == "upload"
    assert retry.progress.frozen_metadata == frozen.to_json()
    processor.process(retry)

    assert len(create_requests) == 2
    assert create_requests[0]["headers"] == create_requests[1]["headers"]
    assert transfers == [b"test"]
    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "webpage"
        assert progress.media_verified_at is not None
        assert progress.claimed_at is None
        assert progress.retry_at is None
        assert progress.failure_phase is None
        assert progress.last_error is None
    assert media.read_bytes() == b"test"
    state.close()


@pytest.mark.parametrize(
    ("cause", "failed_file", "transferred"),
    [
        (NetworkError("lost creation response"), False, (False,)),
        (NetworkError("lost response", status_code=429), False, (False,)),
        (
            InvalidServiceResponseError("unexpected body", status_code=200),
            False,
            (False,),
        ),
        (InvalidServiceResponseError("accepted", status_code=202), False, (False,)),
        (InvalidServiceResponseError("redirect", status_code=302), False, (False,)),
        (InvalidServiceResponseError("timeout", status_code=408), False, (False,)),
        (
            InvalidServiceResponseError("identifier conflict", status_code=409),
            False,
            (False,),
        ),
        (
            InvalidServiceResponseError("precondition failed", status_code=412),
            False,
            (False,),
        ),
        (InvalidServiceResponseError("server error", status_code=500), False, (False,)),
        (InvalidServiceResponseError("unavailable", status_code=503), False, (False,)),
        (RateLimitError("first transfer rejected", status_code=429), True, (False,)),
        (
            RateLimitError("later transfer rejected", status_code=429),
            True,
            (True, False),
        ),
        (RateLimitError("poll rejected", status_code=429), False, (True,)),
        (PollingTimeoutError("ingest timed out"), False, (True,)),
        (NetworkError("poll failed"), False, (True,)),
        (ServiceError("ingest failed"), False, (True,)),
        (RateLimitError("no file outcomes", status_code=429), False, ()),
    ],
)
def test_uncertain_upload_reconciles_without_second_create(
    tmp_path: Path,
    cause: ServiceError,
    failed_file: bool,
    transferred: tuple[bool, ...],
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)

    class FailingGateway(FakeGateway):
        def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
            self.upload_calls += 1
            files = tuple(
                InternetArchiveUploadFileResult(
                    metadata.media_name if index == 0 else "other.mkv",
                    4,
                    transferred=complete,
                )
                for index, complete in enumerate(transferred)
            )
            raise InternetArchiveUploadError(
                InternetArchiveUploadResult(metadata.identifier, files),
                cause,
                failed_file=files[-1].name if failed_file else None,
            )

    gateway = FailingGateway()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=gateway
    )

    first = state.claim_archive(prefer_recent=False)
    assert first is not None
    processor.process(first)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "reconcile_upload"
        assert progress.upload_started_at is not None
    assert media.exists()
    assert gateway.upload_calls == 1

    gateway.inspection = Inspection("missing")
    gateway.recovery = InternetArchiveRecoveryResult(
        identifier=frozen.identifier,
        files=(
            InternetArchiveRecoveryFileResult(
                name=frozen.media_name,
                size=4,
                md5="098f6bcd4621d373cade4e832627b4f6",
                sha1="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
                disposition=InternetArchiveFileDisposition.TRANSFERRED,
                checksum_state=InternetArchiveChecksumState.VERIFIED,
                transferred=True,
            ),
        ),
        processing_complete=True,
        verification_complete=True,
    )
    second = state.claim_archive(
        prefer_recent=False, now=utcnow() + timedelta(seconds=2)
    )
    assert second is not None
    processor.process(second)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "webpage"
        assert progress.media_verified_at is not None
        assert progress.recovery_result is not None
    assert gateway.upload_calls == 1
    assert media.exists()
    state.close()


@pytest.mark.parametrize("missing_field", ["size", "md5", "sha1"])
@pytest.mark.parametrize("initial_status", ["upload", "reconcile_upload"])
def test_incomplete_original_metadata_retries_without_reupload(
    tmp_path: Path, missing_field: str, initial_status: str
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    state.update_archive(video_id, status=initial_status)
    item = valid_archive_item(frozen)
    complete_file = replace(
        item.files[0],
        md5="098f6bcd4621d373cade4e832627b4f6",
        sha1="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
    )
    item = replace(item, files=(replace(complete_file, **{missing_field: None}),))

    class ItemGateway(FakeGateway):
        def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
            self.upload_calls += 1

        def inspect(
            self,
            metadata: FrozenArchiveMetadata,
            *,
            size: int,
            md5_hash: str,
            sha1_hash: str,
        ) -> Inspection:
            return inspect_item(item, metadata, size, md5_hash, sha1_hash)

    gateway = ItemGateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    work = state.claim_archive(prefer_recent=False)
    assert work is not None

    processor.process(work)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "reconcile_upload"
        assert progress.failure_phase == "reconciliation"
        assert "incomplete" in (progress.last_error or "")
        assert progress.media_verified_at is None
        assert progress.cleaned_at is None
        assert progress.media_path == str(media)
        assert progress.claimed_at is None
        retry_at = progress.retry_at
        assert retry_at is not None
    assert media.read_bytes() == b"test"
    assert (
        state.claim_archive(
            prefer_recent=False, now=retry_at - timedelta(microseconds=1)
        )
        is None
    )

    item = replace(item, files=(complete_file,))
    retry = state.claim_archive(prefer_recent=False, now=retry_at)
    assert retry is not None
    assert retry.progress.status == "reconcile_upload"

    processor.process(retry)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "webpage"
        assert progress.media_verified_at is not None
        assert progress.retry_at is None
        assert progress.claimed_at is None
        assert progress.failure_phase is None
        assert progress.last_error is None
        assert progress.cleaned_at is None
    assert gateway.upload_calls == (1 if initial_status == "upload" else 0)
    assert media.read_bytes() == b"test"
    state.close()


def test_preflight_network_failure_retries_create_safely(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)

    class PreflightGateway(FakeGateway):
        def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
            self.upload_calls += 1
            raise NetworkError("availability lookup failed")

    gateway = PreflightGateway()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=gateway
    )

    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    processor.process(work)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "upload"
        assert progress.upload_started_at is not None
        assert progress.failure_phase == "initial_upload_preflight"
    assert gateway.upload_calls == 1
    assert media.exists()
    state.close()


def test_incomplete_recovery_and_cleanup_gate_retain_media(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, frozen = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="reconcile_upload")
    gateway = FakeGateway()
    gateway.inspection = Inspection("missing")
    gateway.recovery = InternetArchiveRecoveryResult(
        identifier=frozen.identifier, files=(), verification_complete=False
    )
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=gateway
    )

    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    processor.process(work)
    assert media.exists()

    state.update_archive(video_id, status="cleanup", retry_after=None)
    cleanup = state.claim_archive(
        prefer_recent=False, now=utcnow() + timedelta(seconds=2)
    )
    assert cleanup is not None
    processor.process(cleanup)
    assert media.exists()
    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "cleanup"
        assert progress.last_error is not None
    state.close()


def test_recovery_errors_distinguish_temporary_and_operator_states(
    tmp_path: Path,
) -> None:
    cases = (
        (
            InternetArchiveRecoveryDeferredError(
                InternetArchiveRecoveryResult("deferred", ()),
                ServiceError("metadata is not visible"),
                phase=InternetArchiveRecoveryPhase.RECONCILIATION,
            ),
            "reconcile_upload",
        ),
        (
            InternetArchiveRecoveryError(
                InternetArchiveRecoveryResult("ownership", ()),
                ServiceError("uploader does not match", status_code=403),
                phase=InternetArchiveRecoveryPhase.OWNERSHIP,
            ),
            "operator_action",
        ),
        (
            InternetArchiveRecoveryError(
                InternetArchiveRecoveryResult("network", ()),
                NetworkError("connection lost"),
                phase=InternetArchiveRecoveryPhase.TRANSFER,
            ),
            "reconcile_upload",
        ),
    )
    for index, (error, expected_status) in enumerate(cases):
        case_root = tmp_path / str(index)
        case_root.mkdir()
        state = StateStore(case_root / "saga.sqlite")
        video_id, media, _ = seed_upload(state, case_root)
        state.update_archive(video_id, status="reconcile_upload")
        gateway = FakeGateway()
        gateway.inspection = Inspection("missing")
        gateway.recovery_error = error
        processor = ArchiveProcessor(
            settings(case_root), state, YouTubeClient(), gateway=gateway
        )

        work = state.claim_archive(prefer_recent=False)
        assert work is not None
        processor.process(work)

        with Session(state.engine) as session:
            progress = session.get(ArchiveProgress, video_id)
            assert progress is not None
            assert progress.status == expected_status
            assert progress.recovery_result is not None
        assert media.exists()
        state.close()


def test_acknowledged_recovery_timeout_reinspects_without_second_mutation(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, frozen = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="reconcile_upload")
    gateway = FakeGateway()
    gateway.inspection = Inspection("missing")
    transferred = InternetArchiveRecoveryResult(
        identifier=frozen.identifier,
        files=(
            InternetArchiveRecoveryFileResult(
                name=frozen.media_name,
                size=4,
                md5="098f6bcd4621d373cade4e832627b4f6",
                sha1="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
                disposition=InternetArchiveFileDisposition.TRANSFERRED,
                checksum_state=InternetArchiveChecksumState.PENDING,
                transferred=True,
            ),
        ),
    )
    gateway.recovery_error = InternetArchiveRecoveryDeferredError(
        transferred,
        ServiceError("deadline expired after transfer"),
        phase=InternetArchiveRecoveryPhase.RECONCILIATION,
    )
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    first = state.claim_archive(prefer_recent=False)
    assert first is not None
    processor.process(first)
    assert gateway.recovery_calls == 1

    gateway.inspection = Inspection("verified")
    second = state.claim_archive(
        prefer_recent=False, now=utcnow() + timedelta(seconds=2)
    )
    assert second is not None
    processor.process(second)

    assert gateway.recovery_calls == 1
    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "webpage"
    state.close()


def test_gateway_sessions_disable_retries_and_share_upload_limiter(
    tmp_path: Path,
) -> None:
    limiter = ByteLimiter(10)
    gateway = ArchiveGateway(settings(tmp_path), limiter)

    first = gateway._session()
    second = gateway._session()
    try:
        for session in (first, second):
            for prefix in ("https://", "http://"):
                adapter = session.get_adapter(prefix)
                assert isinstance(adapter, BandwidthHTTPAdapter)
                assert adapter.limiter is limiter
                assert getattr(adapter.max_retries, "total", None) == 0
        assert first.get_adapter("https://") is not second.get_adapter("https://")
    finally:
        first.close()
        second.close()


def test_gateway_injects_and_closes_fresh_sessions_for_media_mutations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = detached_work(tmp_path, "upload")
    metadata = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata or "")
    sessions: list[Any] = []

    class TrackingSession:
        def __init__(self, *, retries: int) -> None:
            assert retries == 0
            self.closed = False
            sessions.append(self)

        def __enter__(self) -> TrackingSession:
            return self

        def __exit__(self, *args: object) -> None:
            self.closed = True

    class Client:
        def __init__(self, *, session: TrackingSession, **values: Any) -> None:
            assert session is sessions[-1]

        def __enter__(self) -> Client:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def upload(self, *args: Any, **kwargs: Any) -> None:
            pass

        def add_files(self, *args: Any, **kwargs: Any) -> str:
            return "recovered"

    monkeypatch.setattr(archival, "Session", TrackingSession)
    monkeypatch.setattr(archival, "InternetArchiveClient", Client)
    gateway = ArchiveGateway(settings(tmp_path))
    media_path = Path(work.progress.media_path or "")

    gateway.upload(metadata, media_path)
    assert gateway.recover(metadata, media_path) == "recovered"

    assert len(sessions) == 2
    assert sessions[0] is not sessions[1]
    assert all(session.closed for session in sessions)


def test_changed_local_media_blocks_archive_mutation(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    media.write_bytes(b"changed")
    gateway = FakeGateway()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=gateway
    )

    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    processor.process(work)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "operator_action"
        assert progress.failure_phase == "local_verification"
    assert gateway.upload_calls == 0
    assert media.exists()
    state.close()


def test_restart_marks_inflight_webpage_submission_uncertain(tmp_path: Path) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    state.update_archive(video_id, status="wayback_submitting")

    state.reset_interrupted_claims()

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "operator_action"
        assert "uncertain" in (progress.last_error or "")
    assert media.exists()
    state.close()


def test_cleanup_is_restart_safe_after_directory_is_already_gone(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, media, _ = seed_upload(state, tmp_path)
    directory = media.parent
    state.update_archive(
        video_id, status="cleanup", media_verified_at=utcnow(), webpage_status="queued"
    )
    media.unlink()
    directory.rmdir()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=FakeGateway()
    )

    work = state.claim_archive(prefer_recent=False)
    assert work is not None
    processor.process(work)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "complete"
        assert progress.cleaned_at is not None
        assert progress.media_path is None
    state.close()


def test_failed_receipt_commit_does_not_resubmit_wayback(
    tmp_path: Path, monkeypatch: Any
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    video_id, _, _ = seed_upload(state, tmp_path)
    state.update_archive(video_id, status="webpage", media_verified_at=utcnow())
    gateway = FakeGateway()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), gateway=gateway
    )
    original_update = state.update_archive

    def fail_receipt_commit(*args: Any, **kwargs: Any) -> None:
        if kwargs.get("status") == "wayback_wait":
            raise RuntimeError("database write failed")
        original_update(*args, **kwargs)

    monkeypatch.setattr(state, "update_archive", fail_receipt_commit)
    work = state.claim_archive(prefer_recent=False)
    assert work is not None

    processor.process(work)

    with Session(state.engine) as session:
        progress = session.get(ArchiveProgress, video_id)
        assert progress is not None
        assert progress.status == "operator_action"
        assert progress.failure_phase == "wayback"
        assert "persistence failed" in (progress.last_error or "")
    state.close()


class ZiggyResponse:
    status = 202

    def __enter__(self) -> ZiggyResponse:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def read(self) -> bytes:
        return (
            b'{"submissions":[{"receipt_id":"receipt",'
            b'"url":"https://example.com/video"}]}'
        )


def test_ziggy_requires_202_and_matching_receipt(monkeypatch: Any) -> None:
    monkeypatch.setattr(archival, "urlopen", lambda *args, **kwargs: ZiggyResponse())

    receipt = submit_ziggy("http://ziggy:9449", "saga", "https://example.com/video", 1)

    assert receipt == {"receipt_id": "receipt", "url": "https://example.com/video"}


class RecordingState(StateStore):
    def __init__(self, *, fail_status: str | None = None) -> None:
        self.updates: list[dict[str, Any]] = []
        self.fail_status = fail_status

    def update_archive(self, video_id: str, **values: Any) -> None:
        if self.fail_status is not None and values.get("status") == self.fail_status:
            self.fail_status = None
            raise RuntimeError("database write failed")
        self.updates.append({"video_id": video_id, **values})


def detached_work(
    tmp_path: Path,
    status: str,
    *,
    frozen: FrozenArchiveMetadata | None = None,
    media: bool = True,
    **progress_values: Any,
) -> ArchiveWork:
    metadata = frozen or FrozenArchiveMetadata(
        identifier="video-AbCdEfGhI12",
        operation_id="operation",
        source_url="https://www.youtube.com/watch?v=AbCdEfGhI12",
        media_name="video-AbCdEfGhI12.mkv",
        title="Title",
        description="Description",
        subjects=("youtube",),
        creator="Creator",
        upload_date="2020-01-02",
        channel_username="@Channel",
        estimated_bytes=4,
    )
    directory = tmp_path / "downloads" / metadata.operation_id
    directory.mkdir(parents=True, exist_ok=True)
    media_path = directory / "media.mkv"
    if media:
        media_path.write_bytes(b"test")
    progress = ArchiveProgress(
        video_id="AbCdEfGhI12",
        status=status,
        frozen_metadata=metadata.to_json(),
        working_directory=str(directory),
        media_path=str(media_path),
        media_name=metadata.media_name,
        media_size=4,
        media_md5="098f6bcd4621d373cade4e832627b4f6",
        media_sha1="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        **progress_values,
    )
    video = Video(
        id=progress.video_id,
        channel_id="UCaaaaaaaaaaaaaaaaaaaaaa",
        webpage_url="https://youtu.be/AbCdEfGhI12",
    )
    channel = Channel(
        id=video.channel_id,
        title="Channel",
        canonical_url="https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
    )
    return ArchiveWork(progress, video, channel)


class ConfigurableGateway:
    def __init__(self) -> None:
        self.upload_error: Exception | None = None
        self.inspection = Inspection("verified")
        self.recovery: InternetArchiveRecoveryResult | None = None
        self.page_result: Any = None
        self.submit_error: Exception | None = None

    def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
        if self.upload_error is not None:
            raise self.upload_error

    def inspect(self, *args: Any, **kwargs: Any) -> Inspection:
        return self.inspection

    def recover(self, *args: Any, **kwargs: Any) -> InternetArchiveRecoveryResult:
        assert self.recovery is not None
        return self.recovery

    def submit_page(self, url: str) -> str:
        if self.submit_error is not None:
            raise self.submit_error
        return "job-id"

    def page_status(self, job_id: str) -> Any:
        return self.page_result


def test_gateway_delegates_archivist_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = detached_work(tmp_path, "upload")
    metadata = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata or "")
    item = InternetArchiveItem(
        identifier=metadata.identifier,
        metadata={
            "source": metadata.source_url,
            "saga_operation_id": metadata.operation_id,
        },
        files=(
            InternetArchiveItemFile(
                name=metadata.media_name,
                size=4,
                md5=work.progress.media_md5,
                sha1=work.progress.media_sha1,
                source=InternetArchiveItemFileSource.ORIGINAL,
            ),
        ),
        availability=InternetArchiveItemAvailability.AVAILABLE,
        listing_complete=True,
    )
    calls: list[str] = []

    class Client:
        def __init__(self, **values: Any) -> None:
            pass

        def __enter__(self) -> Client:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def upload(self, *args: Any, **kwargs: Any) -> None:
            calls.append("upload")

        def get_item(self, identifier: str) -> InternetArchiveItem:
            calls.append("get_item")
            return item

        def add_files(self, *args: Any, **kwargs: Any) -> str:
            calls.append("add_files")
            return "recovered"

        def submit(self, url: str) -> Any:
            calls.append("submit")
            return SimpleNamespace(job_id="job-id")

        def status(self, job_id: str) -> str:
            calls.append("status")
            return "status"

    monkeypatch.setattr(archival, "InternetArchiveClient", Client)
    gateway = ArchiveGateway(settings(tmp_path))
    media_path = Path(work.progress.media_path or "")

    gateway.upload(metadata, media_path)
    assert (
        gateway.inspect(
            metadata,
            size=4,
            md5_hash=work.progress.media_md5 or "",
            sha1_hash=work.progress.media_sha1 or "",
        ).state
        == "verified"
    )
    assert gateway.recover(metadata, media_path) == "recovered"
    assert gateway.submit_page(metadata.source_url) == "job-id"
    assert gateway.page_status("job-id") == "status"
    assert calls == ["upload", "get_item", "add_files", "submit", "status"]


def test_process_metadata_download_and_dispatch_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    details = VideoMetadata(
        "AbCdEfGhI12",
        "Title",
        "Description",
        "20200102",
        "UCaaaaaaaaaaaaaaaaaaaaaa",
        "Channel",
        "@Channel",
        "https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa",
        "https://www.youtube.com/watch?v=AbCdEfGhI12",
        4,
    )

    class Youtube:
        metadata_error: Exception | None = None
        download_error: Exception | None = None

        def video_metadata(self, video_id: str) -> VideoMetadata:
            if self.metadata_error is not None:
                raise self.metadata_error
            return details

        def download_video(self, video_id: str, directory: Path, **kwargs: Any) -> Path:
            if self.download_error is not None:
                raise self.download_error
            path = directory / "media.mkv"
            path.write_bytes(b"test")
            return path

    state = RecordingState()
    client = Youtube()
    processor = ArchiveProcessor(
        settings(tmp_path), state, cast(Any, client), ConfigurableGateway()
    )
    processor.process(detached_work(tmp_path, "metadata"))
    metadata_update = state.updates[-1]
    assert metadata_update["status"] == "download"

    frozen = FrozenArchiveMetadata.from_json(metadata_update["frozen_metadata"])
    download = detached_work(tmp_path, "download", frozen=frozen)
    monkeypatch.setattr(
        archival,
        "verify_media",
        lambda path: MediaFile(
            path,
            4,
            "098f6bcd4621d373cade4e832627b4f6",
            "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        ),
    )
    processor.process(download)
    assert state.updates[-1]["status"] == "upload"

    processor.process(detached_work(tmp_path, "unknown"))
    assert state.updates[-1]["status"] == "operator_action"
    client.metadata_error = archival.VideoUnavailableError("gone")
    processor.process(detached_work(tmp_path, "metadata"))
    assert state.updates[-1]["status"] == "unavailable"
    client.metadata_error = None
    client.download_error = DiskSpaceError("full")
    processor.process(detached_work(tmp_path, "download"))
    assert state.updates[-1] == {
        "video_id": "AbCdEfGhI12",
        "status": "download",
        "release": True,
        "retry_after": 1,
        "failure_phase": "disk",
        "last_error": "DiskSpaceError: full",
    }


@pytest.mark.parametrize("status", ["upload", "reconcile_upload"])
@pytest.mark.parametrize(
    ("inspection", "expected_status"),
    [
        (Inspection("verified"), "webpage"),
        (Inspection("conflict", "different"), "operator_action"),
        (Inspection("pending", "waiting"), "reconcile_upload"),
    ],
)
def test_upload_and_reconciliation_inspection_outcomes(
    tmp_path: Path, status: str, inspection: Inspection, expected_status: str
) -> None:
    state = RecordingState()
    gateway = ConfigurableGateway()
    gateway.inspection = inspection
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    work = detached_work(tmp_path, status)

    processor.process(work)

    assert state.updates[-1]["status"] == expected_status
    assert Path(work.progress.media_path or "").read_bytes() == b"test"


def test_upload_authentication_failure_requires_operator_action(tmp_path: Path) -> None:
    state = RecordingState()
    gateway = ConfigurableGateway()
    gateway.upload_error = AuthenticationError("bad credentials")
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    processor.process(detached_work(tmp_path, "upload"))

    assert state.updates[-1]["status"] == "operator_action"
    assert state.updates[-1]["failure_phase"] == "initial_upload_preflight"


def test_reconciliation_uses_nonmissing_inspection_result(tmp_path: Path) -> None:
    state = RecordingState()
    gateway = ConfigurableGateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    processor.process(detached_work(tmp_path, "reconcile_upload"))

    assert state.updates[-1]["status"] == "webpage"


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (NetworkError("uncertain"), "operator_action"),
        (ServiceError("rejected"), "webpage"),
    ],
)
def test_wayback_submission_failures(
    tmp_path: Path, error: Exception, expected_status: str
) -> None:
    work = detached_work(tmp_path, "webpage")
    state = RecordingState()
    gateway = ConfigurableGateway()
    gateway.submit_error = error
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    processor.process(work)

    assert state.updates[-1]["status"] == expected_status
    assert state.updates[-1]["failure_phase"] == "wayback"


def test_wayback_submission_persists_job(tmp_path: Path) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )

    processor.process(detached_work(tmp_path, "webpage"))

    assert state.updates[-1]["status"] == "wayback_wait"
    assert state.updates[-1]["wayback_job_id"] == "job-id"


def test_ziggy_submission_success_and_persistence_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = detached_work(tmp_path, "webpage")
    calls: list[tuple[str, str, str, float]] = []

    def submit(host: str, identifier: str, url: str, timeout: float) -> dict[str, str]:
        calls.append((host, identifier, url, timeout))
        return {"url": url, "receipt_id": "receipt"}

    monkeypatch.setattr(archival, "submit_ziggy", submit)
    state = RecordingState()
    processor = ArchiveProcessor(settings(tmp_path, ziggy=True), state, YouTubeClient())
    processor.process(work)
    assert state.updates[-1]["status"] == "cleanup"
    assert json.loads(state.updates[-1]["ziggy_receipt"])["receipt_id"] == "receipt"

    failed_state = RecordingState(fail_status="cleanup")
    fallback_settings = replace(settings(tmp_path, ziggy=True), ziggy_identifier=None)
    processor = ArchiveProcessor(fallback_settings, failed_state, YouTubeClient())
    processor.process(work)
    assert failed_state.updates[-1]["status"] == "operator_action"
    assert failed_state.updates[-1]["failure_phase"] == "ziggy"
    assert calls[-1][1] == "saga"


def test_ziggy_only_archival_does_not_require_internet_archive_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = replace(
        settings(tmp_path, ziggy=True),
        internet_archive_email=None,
        internet_archive_password=None,
    )
    calls: list[str] = []

    def submit(
        _host: str, _identifier: str, url: str, _timeout: float
    ) -> dict[str, str]:
        calls.append(url)
        return {"url": url, "receipt_id": "receipt"}

    monkeypatch.setattr(archival, "submit_ziggy", submit)
    state = RecordingState()
    processor = ArchiveProcessor(configured, state, YouTubeClient())

    processor.process(detached_work(tmp_path, "metadata"))

    assert calls == ["https://youtu.be/AbCdEfGhI12"]
    assert state.updates[-1]["status"] == "complete"
    assert state.updates[-1]["webpage_status"] == "queued"


def test_ziggy_only_routes_resumed_stages_without_internet_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = replace(
        settings(tmp_path, ziggy=True),
        internet_archive_email=None,
        internet_archive_password=None,
    )
    monkeypatch.setattr(
        archival,
        "submit_ziggy",
        lambda _host, _identifier, url, _timeout: {"url": url, "receipt_id": "id"},
    )
    state = RecordingState()
    processor = ArchiveProcessor(configured, state, YouTubeClient())

    processor.process(detached_work(tmp_path, "webpage"))
    assert state.updates[-1]["status"] == "cleanup"

    processor.process(detached_work(tmp_path, "download"))
    assert state.updates[-1]["status"] == "operator_action"
    assert state.updates[-1]["failure_phase"] == "download"

    processor.process(
        detached_work(
            tmp_path, "cleanup", media_verified_at=utcnow(), webpage_status="queued"
        )
    )
    assert state.updates[-1]["status"] == "complete"


def test_ziggy_only_process_handles_dispatch_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = replace(
        settings(tmp_path, ziggy=True),
        internet_archive_email=None,
        internet_archive_password=None,
    )
    monkeypatch.setattr(
        archival,
        "submit_ziggy",
        lambda _host, _identifier, url, _timeout: {"url": url, "receipt_id": "id"},
    )
    failed_state = RecordingState(fail_status="complete")

    ArchiveProcessor(configured, failed_state, YouTubeClient()).process(
        detached_work(tmp_path, "metadata")
    )

    assert failed_state.updates[-1]["status"] == "operator_action"
    assert failed_state.updates[-1]["failure_phase"] == "ziggy"

    unavailable = replace(configured, ziggy_host=None)
    state = RecordingState()
    processor = ArchiveProcessor(unavailable, state, YouTubeClient())
    processor.process(detached_work(tmp_path, "metadata"))

    assert state.updates[-1]["status"] == "metadata"
    assert "no configured provider" in state.updates[-1]["last_error"]
    with pytest.raises(RuntimeError, match="credentials are not configured"):
        processor._gateway()


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (archival.SubmissionRejectedError("rejected"), "webpage"),
        (archival.SubmissionUncertainError("uncertain"), "operator_action"),
        (RuntimeError("unknown"), "operator_action"),
    ],
)
def test_ziggy_submission_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_status: str,
) -> None:
    def fail(*args: Any, **kwargs: Any) -> None:
        raise error

    monkeypatch.setattr(archival, "submit_ziggy", fail)
    state = RecordingState()
    processor = ArchiveProcessor(settings(tmp_path, ziggy=True), state, YouTubeClient())

    processor.process(detached_work(tmp_path, "webpage"))

    assert state.updates[-1]["status"] == expected_status
    assert state.updates[-1]["failure_phase"] == "ziggy"


@pytest.mark.parametrize(
    ("result", "expected_status", "webpage_status"),
    [
        (InternetArchivePendingStatus("job-id"), None, None),
        (
            InternetArchiveSuccessStatus(
                "job-id", "https://example.com", datetime(2024, 1, 1, tzinfo=UTC)
            ),
            "cleanup",
            "captured",
        ),
        (InternetArchiveFailedStatus("job-id", "failed"), "operator_action", "failed"),
        (InternetArchiveFailedStatus("job-id"), "operator_action", "failed"),
    ],
)
def test_wayback_wait_statuses(
    tmp_path: Path, result: Any, expected_status: str | None, webpage_status: str | None
) -> None:
    state = RecordingState()
    gateway = ConfigurableGateway()
    gateway.page_result = result
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    processor.process(detached_work(tmp_path, "wayback_wait", wayback_job_id="job-id"))

    assert state.updates[-1].get("status") == expected_status
    assert state.updates[-1].get("webpage_status") == webpage_status
    if isinstance(result, InternetArchiveFailedStatus) and result.message is None:
        assert state.updates[-1]["last_error"] == "Wayback capture failed"


@pytest.mark.parametrize(
    ("job_id", "result", "message"),
    [
        (None, None, "no persisted job ID"),
        ("job-id", object(), "unexpected Wayback status"),
    ],
)
def test_wayback_wait_retries_invalid_state(
    tmp_path: Path, job_id: str | None, result: Any, message: str
) -> None:
    state = RecordingState()
    gateway = ConfigurableGateway()
    gateway.page_result = result
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)

    processor.process(detached_work(tmp_path, "wayback_wait", wayback_job_id=job_id))

    assert message in state.updates[-1]["last_error"]
    assert state.updates[-1]["status"] == "wayback_wait"


def valid_archive_item(metadata: FrozenArchiveMetadata) -> InternetArchiveItem:
    return InternetArchiveItem(
        identifier=metadata.identifier,
        metadata={
            "source": metadata.source_url,
            "saga_operation_id": metadata.operation_id,
        },
        files=(
            InternetArchiveItemFile(
                name=metadata.media_name,
                size=4,
                md5="md5",
                sha1="sha1",
                source=InternetArchiveItemFileSource.ORIGINAL,
            ),
        ),
        availability=InternetArchiveItemAvailability.AVAILABLE,
        listing_complete=True,
    )


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        (
            {"metadata": {"source": "wrong", "saga_operation_id": "operation"}},
            "conflict",
        ),
        (
            {
                "metadata": {
                    "source": "https://www.youtube.com/watch?v=AbCdEfGhI12",
                    "saga_operation_id": "wrong",
                }
            },
            "conflict",
        ),
        ({"has_redrow": True}, "conflict"),
        ({"is_dark": True}, "conflict"),
        ({"nodownload": True}, "conflict"),
        ({"server_unavailable": True}, "pending"),
        (
            {
                "tasks": (
                    InternetArchiveItemTask(
                        "task", InternetArchiveItemTaskState.PENDING
                    ),
                )
            },
            "pending",
        ),
        (
            {
                "tasks": (
                    InternetArchiveItemTask(
                        "task", InternetArchiveItemTaskState.UNKNOWN
                    ),
                )
            },
            "pending",
        ),
        ({"availability": InternetArchiveItemAvailability.PENDING}, "pending"),
        ({"listing_complete": False}, "pending"),
        ({"files": ()}, "missing"),
    ],
)
def test_item_inspection_remote_state_matrix(
    tmp_path: Path, changes: dict[str, Any], expected: str
) -> None:
    work = detached_work(tmp_path, "upload")
    metadata = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata or "")
    item = replace(valid_archive_item(metadata), **changes)

    assert inspect_item(item, metadata, 4, "md5", "sha1").state == expected


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"size": None}, "pending"),
        ({"md5": None}, "pending"),
        ({"sha1": None}, "pending"),
        ({"size": None, "md5": None}, "pending"),
        ({"size": None, "sha1": None}, "pending"),
        ({"md5": None, "sha1": None}, "pending"),
        ({"size": None, "md5": None, "sha1": None}, "pending"),
        ({"size": 5}, "conflict"),
        ({"size": 0}, "conflict"),
        ({"md5": "different"}, "conflict"),
        ({"sha1": "different"}, "conflict"),
        ({"size": 0, "md5": None, "sha1": None}, "conflict"),
        ({"size": None, "md5": "different", "sha1": None}, "conflict"),
        ({"size": None, "md5": None, "sha1": "different"}, "conflict"),
    ],
)
def test_item_inspection_original_file_metadata(
    tmp_path: Path, changes: dict[str, Any], expected: str
) -> None:
    work = detached_work(tmp_path, "upload")
    metadata = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata or "")
    item = valid_archive_item(metadata)
    item = replace(item, files=(replace(item.files[0], **changes),))

    assert item.listing_complete
    assert inspect_item(item, metadata, 4, "md5", "sha1").state == expected


def test_item_inspection_accepts_list_metadata(tmp_path: Path) -> None:
    work = detached_work(tmp_path, "upload")
    metadata = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata or "")
    item = replace(
        valid_archive_item(metadata),
        metadata={
            "source": [metadata.source_url],
            "saga_operation_id": (metadata.operation_id,),
        },
    )

    assert inspect_item(item, metadata, 4, "md5", "sha1").state == "verified"


class CustomZiggyResponse:
    def __init__(self, status: int, payload: bytes) -> None:
        self.status = status
        self.payload = payload

    def __enter__(self) -> CustomZiggyResponse:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def read(self) -> bytes:
        return self.payload


@pytest.mark.parametrize(
    ("outcome", "error"),
    [
        (
            HTTPError("http://ziggy", 400, "bad", cast(Any, None), None),
            SubmissionRejectedError,
        ),
        (URLError("lost"), SubmissionUncertainError),
        (CustomZiggyResponse(200, b"{}"), SubmissionRejectedError),
        (CustomZiggyResponse(202, b"not json"), RuntimeError),
        (CustomZiggyResponse(202, b'{"submissions":[]}'), RuntimeError),
        (CustomZiggyResponse(202, b'{"submissions":[{},{}]}'), RuntimeError),
        (CustomZiggyResponse(202, b'{"submissions":["receipt"]}'), RuntimeError),
        (
            CustomZiggyResponse(
                202, b'{"submissions":[{"url":"wrong","receipt_id":"id"}]}'
            ),
            RuntimeError,
        ),
        (
            CustomZiggyResponse(
                202, b'{"submissions":[{"url":"https://example.com","receipt_id":1}]}'
            ),
            RuntimeError,
        ),
        (
            CustomZiggyResponse(
                202, b'{"submissions":[{"url":"https://example.com","receipt_id":""}]}'
            ),
            RuntimeError,
        ),
    ],
)
def test_ziggy_transport_and_receipt_errors(
    monkeypatch: pytest.MonkeyPatch, outcome: Any, error: type[Exception]
) -> None:
    def open_url(*args: Any, **kwargs: Any) -> Any:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(archival, "urlopen", open_url)

    with pytest.raises(error):
        submit_ziggy("http://ziggy", "saga", "https://example.com", 1)


def test_cleanup_removes_verified_download(tmp_path: Path) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )
    work = detached_work(
        tmp_path, "cleanup", media_verified_at=utcnow(), webpage_status="captured"
    )
    directory = Path(work.progress.working_directory or "")

    processor.process(work)

    assert not directory.exists()


def test_cleanup_resumes_after_directory_removal(tmp_path: Path) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )
    work = detached_work(
        tmp_path, "cleanup", media_verified_at=utcnow(), webpage_status="queued"
    )
    work.progress.media_path = None

    processor.process(work)

    assert state.updates[-1]["status"] == "complete"


def test_cleanup_rejects_media_outside_work_directory(tmp_path: Path) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )
    work = detached_work(
        tmp_path, "cleanup", media_verified_at=utcnow(), webpage_status="captured"
    )
    directory = Path(work.progress.working_directory or "")
    work.progress.media_path = str(tmp_path / "downloads" / "other" / "media.mkv")

    processor.process(work)

    assert directory.exists()
    assert state.updates[-1]["status"] == "cleanup"


@pytest.mark.parametrize(
    ("error", "removed"),
    [(RuntimeError("corrupt"), True), (FileNotFoundError("ffprobe"), False)],
)
def test_failed_media_verification_only_removes_corrupt_downloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception, removed: bool
) -> None:
    state = RecordingState()
    work = detached_work(tmp_path, "download")
    media = Path(work.progress.media_path or "")

    class Youtube:
        def download_video(self, *_: Any, **__: Any) -> Path:
            return media

    processor = ArchiveProcessor(
        settings(tmp_path), state, cast(Any, Youtube()), ConfigurableGateway()
    )
    monkeypatch.setattr(
        "core.archival.verify_media", lambda _: (_ for _ in ()).throw(error)
    )

    processor.process(work)

    assert media.exists() is not removed
    assert state.updates[-1]["status"] == "download"


def test_failure_before_upload_intent_does_not_enter_reconciliation(
    tmp_path: Path,
) -> None:
    class State(RecordingState):
        calls = 0

        def update_archive(self, video_id: str, **values: Any) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("database failed")
            super().update_archive(video_id, **values)

    class Gateway(ConfigurableGateway):
        upload_calls = 0

        def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
            self.upload_calls += 1

    state = State()
    gateway = Gateway()
    processor = ArchiveProcessor(settings(tmp_path), state, YouTubeClient(), gateway)
    work = detached_work(tmp_path, "upload")

    processor.process(work)

    assert gateway.upload_calls == 0
    assert state.updates[-1]["status"] == "upload"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"media_verified_at": None, "webpage_status": "captured"},
            "media verification",
        ),
        (
            {"media_verified_at": datetime.now(UTC), "webpage_status": None},
            "webpage acceptance",
        ),
    ],
)
def test_cleanup_retries_when_gates_are_incomplete(
    tmp_path: Path, changes: dict[str, Any], message: str
) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )

    processor.process(detached_work(tmp_path, "cleanup", **changes))

    assert message in state.updates[-1]["last_error"]


def test_cleanup_refuses_directory_outside_download_root(tmp_path: Path) -> None:
    state = RecordingState()
    processor = ArchiveProcessor(
        settings(tmp_path), state, YouTubeClient(), ConfigurableGateway()
    )
    work = detached_work(
        tmp_path, "cleanup", media_verified_at=utcnow(), webpage_status="queued"
    )
    work.progress.working_directory = str(tmp_path / "outside")

    processor.process(work)

    assert "outside Saga downloads" in state.updates[-1]["last_error"]


def test_archive_invariant_helpers(tmp_path: Path) -> None:
    work = detached_work(tmp_path, "upload")
    work.progress.frozen_metadata = None
    with pytest.raises(RuntimeError, match="no frozen metadata"):
        archival._frozen(work)

    work = detached_work(tmp_path, "upload")
    work.progress.working_directory = None
    with pytest.raises(RuntimeError, match="no working directory"):
        archival._working_directory(work)

    for name in ("media_path", "media_size", "media_md5", "media_sha1"):
        work = detached_work(tmp_path / name, "upload")
        setattr(work.progress, name, None)
        with pytest.raises(RuntimeError, match="incomplete local media facts"):
            archival._media_facts(work)

    work = detached_work(tmp_path / "missing", "upload", media=False)
    with pytest.raises(RuntimeError, match="media file is missing"):
        archival._media_facts(work)


def test_frozen_metadata_validation_and_identifier_fallback() -> None:
    with pytest.raises(ValueError, match="not an object"):
        FrozenArchiveMetadata.from_json("[]")
    assert archive_identifier("🎬?!", "AbCdEfGhI12") == "video-AbCdEfGhI12"
