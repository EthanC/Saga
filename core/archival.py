"""Upload-first transfers with independent capture, recovery, and cleanup jobs."""

from __future__ import annotations

import json
import shutil
import time
import unicodedata
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date
from http.client import HTTPException
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from archivist import (
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveCaptureStatus,
    InternetArchiveFailedStatus,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchivePendingStatus,
    InternetArchiveSuccessStatus,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
    InternetArchiveUploadResult,
    InvalidOptionError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from loguru import logger
from niquests import Session

from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter, Limiter
from core.config import Settings
from core.logging import redact_diagnostics
from core.progress import DownloadCheckpoint
from core.shutdown import CooperativeStop, StopController
from core.staging import ensure_download_root
from core.state import ArchiveWork, StateStore, retry_delay, utcnow
from core.transport import RecoveryConflictError, TransportArchiveClient
from core.workers import run_managed
from core.youtube import (
    DiskSpaceError,
    VideoMetadata,
    VideoUnavailableError,
    YouTubeClient,
)

MAX_IDENTIFIER_LENGTH = 100


@dataclass(frozen=True, slots=True)
class FrozenArchiveMetadata:
    """Contain values that must remain stable across transfer retries."""

    identifier: str
    operation_id: str
    source_url: str
    media_name: str
    title: str
    description: str
    subjects: tuple[str, ...]
    creator: str
    upload_date: str
    channel_username: str
    estimated_bytes: int | None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, value: str) -> FrozenArchiveMetadata:
        document = json.loads(value)
        if not isinstance(document, dict):
            raise ValueError("frozen archive metadata is not an object")
        document["subjects"] = tuple(document["subjects"])
        return cls(**document)


@dataclass(frozen=True, slots=True)
class Inspection:
    """Describe remote evidence for the expected original file."""

    state: str
    detail: str | None = None
    receipt: str | None = None


class SubmissionRejectedError(RuntimeError):
    """A webpage request was rejected before acceptance."""


class SubmissionUncertainError(RuntimeError):
    """A webpage request might have been accepted."""


class LocalMediaChangedError(RuntimeError):
    """Staging no longer has the persisted download size."""


class AcceptedSubmissionPersistenceError(RuntimeError):
    """A remote receipt could not be saved."""


class _ProgressStream:
    def __init__(
        self,
        stream: Any,
        callback: Callable[[int], None],
        controller: StopController | None = None,
    ) -> None:
        self.stream = stream
        self.callback = callback
        self.last_update = time.monotonic()
        self.controller = controller

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)

    def read(self, amount: int = -1) -> bytes:
        if self.controller:
            self.controller.check()
        chunk = self.stream.read(amount)
        if time.monotonic() - self.last_update >= 5:
            self.callback(self.stream.tell())
            self.last_update = time.monotonic()
        return chunk


class _StopSession(Session):
    def __init__(self, controller: StopController) -> None:
        super().__init__(retries=0)
        self.controller = controller

    def request(self, *args: Any, **kwargs: Any) -> Any:
        self.controller.check()
        return super().request(*args, **kwargs)


class ArchiveGateway:
    """Keep transfers one-shot and status reads bounded by request timeouts."""

    def __init__(
        self,
        settings: Settings,
        upload_limiter: Limiter | None = None,
        *,
        stop_controller: StopController | None = None,
        managed: bool = False,
    ) -> None:
        assert settings.internet_archive_email is not None
        assert settings.internet_archive_password is not None
        self.account = InternetArchiveAccount(
            settings.internet_archive_email, settings.internet_archive_password
        )
        self.request_timeout = settings.archive_request_timeout
        self.upload_limiter = upload_limiter or ByteLimiter(0)
        self.settings = settings
        self.stop_controller = stop_controller or StopController()
        self.managed = managed

    def _session(self) -> Session:
        self.stop_controller.check()
        session = _StopSession(self.stop_controller)
        if self.upload_limiter.enabled:
            for prefix in ("https://", "http://"):
                previous = session.get_adapter(prefix)
                session.mount(
                    prefix, BandwidthHTTPAdapter(self.upload_limiter, max_retries=0)
                )
                previous.close()
        return session

    def _client(self, session: Session) -> TransportArchiveClient:
        return TransportArchiveClient(
            session=session, account=self.account, timeout=self.request_timeout
        )

    def upload(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
    ) -> InternetArchiveUploadResult:
        if self.managed:
            return self._managed("upload", (metadata, media_path), {}, progress)
        options = InternetArchiveUploadOptions(
            identifier=metadata.identifier,
            title=metadata.title,
            description=metadata.description,
            subjects=list(metadata.subjects),
            creator=metadata.creator,
            date=date.fromisoformat(metadata.upload_date),
            collection="opensource_movies",
            media_type="movies",
            metadata={
                "source": metadata.source_url,
                "saga_operation_id": metadata.operation_id,
            },
        )
        with (
            media_path.open("rb") as stream,
            self._session() as session,
            self._client(session) as client,
        ):
            file = InternetArchiveUploadFile(
                cast(Any, _ProgressStream(stream, progress, self.stop_controller)),
                name=metadata.media_name,
            )
            return client.upload([file], options, wait=False)

    def inspect(
        self,
        metadata: FrozenArchiveMetadata,
        *,
        size: int,
        md5_hash: str | None = None,
        sha1_hash: str | None = None,
        validation: bool = False,
    ) -> Inspection:
        if self.managed:
            return self._managed(
                "inspect",
                (metadata,),
                {
                    "size": size,
                    "md5_hash": md5_hash,
                    "sha1_hash": sha1_hash,
                    "validation": validation,
                },
            )
        with self._session() as session, self._client(session) as client:
            item = client.get_item(metadata.identifier)
        return inspect_item(
            item, metadata, size, md5_hash, sha1_hash, validation=validation
        )

    def recover(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
    ) -> InternetArchiveUploadResult:
        if self.managed:
            return self._managed("recover", (metadata, media_path), {}, progress)
        with (
            media_path.open("rb") as stream,
            self._session() as session,
            self._client(session) as client,
        ):
            return client.add_file_transport(
                metadata.identifier,
                InternetArchiveUploadFile(
                    cast(Any, _ProgressStream(stream, progress, self.stop_controller)),
                    name=metadata.media_name,
                ),
                expected_metadata={
                    "source": metadata.source_url,
                    "saga_operation_id": metadata.operation_id,
                },
                expected_size=media_path.stat().st_size,
            )

    def submit_page(self, url: str) -> str:
        if self.managed:
            return self._managed("submit_page", (url,), {})
        with self._session() as session, self._client(session) as client:
            return client.submit(url).job_id

    def page_status(self, job_id: str) -> InternetArchiveCaptureStatus:
        if self.managed:
            return self._managed("page_status", (job_id,), {})
        with self._session() as session, self._client(session) as client:
            return client.status(job_id)

    def _managed(
        self,
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        progress: Callable[[int], None] | None = None,
    ) -> Any:
        return run_managed(
            "gateway",
            method,
            {
                "settings": self.settings,
                "limiter_enabled": self.upload_limiter.enabled,
                "chunk_size": self.upload_limiter.chunk_size,
            },
            args,
            kwargs,
            self.stop_controller,
            {
                "acquire": self.upload_limiter.acquire,
                "refund": self.upload_limiter.refund,
                "progress": progress or (lambda value: None),
            },
        )


class ArchiveBackend(Protocol):
    def upload(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
    ) -> InternetArchiveUploadResult: ...
    def inspect(
        self,
        metadata: FrozenArchiveMetadata,
        *,
        size: int,
        md5_hash: str | None = None,
        sha1_hash: str | None = None,
        validation: bool = False,
    ) -> Inspection: ...
    def recover(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
    ) -> InternetArchiveUploadResult: ...
    def submit_page(self, url: str) -> str: ...
    def page_status(self, job_id: str) -> InternetArchiveCaptureStatus: ...


class ArchiveProcessor:
    """Perform one network or filesystem operation per independent claim."""

    def __init__(
        self,
        settings: Settings,
        state: StateStore,
        youtube: YouTubeClient,
        gateway: ArchiveBackend | None = None,
        upload_limiter: ByteLimiter | None = None,
        stop_controller: StopController | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.youtube = youtube
        self.stop_controller = stop_controller or StopController()
        self.managed_io = isinstance(youtube, YouTubeClient) and youtube.managed
        self.gateway = gateway
        if gateway is None and settings.media_archival_enabled:
            self.gateway = ArchiveGateway(
                settings,
                upload_limiter,
                stop_controller=self.stop_controller,
                managed=self.managed_io,
            )
        self.download_root = ensure_download_root(settings.download_root)

    def process(self, work: ArchiveWork) -> None:
        stage = work.job.kind if work.job else work.progress.status
        try:
            self.stop_controller.check()
            handlers = {
                "metadata": self._metadata,
                "download": self._download,
                "upload": self._upload,
                "recovery": self._reconcile,
                "validation": self._validate,
                "webpage": self._webpage,
                "cleanup": self._cleanup,
            }
            if stage not in handlers:
                raise RecoveryConflictError(f"unknown archive stage {stage!r}")
            handlers[stage](work)
        except CooperativeStop as error:
            if work.job:
                self.state.update_job(
                    work,
                    status="failed" if work.job.status == "submitting" else "waiting",
                    retry_after=0,
                    error=_error_text(error),
                )
            elif stage == "upload" and work.progress.status == "uploading":
                self.state.defer_transfer(
                    work,
                    status="reconcile_upload",
                    error=_error_text(error),
                    delay=0,
                    deadline=self.settings.background_deadline,
                )
            else:
                self.state.update_archive(
                    work.video.id,
                    release=True,
                    last_error=_error_text(error),
                    failure_phase="shutdown",
                )
        except VideoUnavailableError as error:
            self.state.update_archive(
                work.video.id,
                status="unavailable",
                release=True,
                failure_phase=stage,
                last_error=_error_text(error),
            )
        except (
            LocalMediaChangedError,
            RecoveryConflictError,
            AuthenticationError,
            InvalidOptionError,
        ) as error:
            if work.job:
                self.state.update_job(work, status="failed", error=_error_text(error))
            else:
                self.state.update_archive(
                    work.video.id,
                    status="operator_action",
                    release=True,
                    failure_phase=stage,
                    last_error=_error_text(error),
                )
        except Exception as error:
            delay = retry_delay(
                self.settings.archive_retry_delay,
                work.job.attempts if work.job else work.progress.retry_attempts + 1,
            )
            logger.warning(
                "Video {} phase {} attempt {} will retry in {:.0f}s: {}",
                work.video.id,
                stage,
                work.job.attempts if work.job else work.progress.attempts,
                delay,
                _error_text(error),
            )
            if work.job:
                self.state.update_job(
                    work, status="waiting", retry_after=delay, error=_error_text(error)
                )
            elif stage == "upload" and work.progress.status == "uploading":
                self.state.defer_transfer(
                    work,
                    status="reconcile_upload",
                    error=_error_text(error),
                    delay=delay,
                    deadline=self.settings.background_deadline,
                )
            else:
                self.state.update_archive(
                    work.video.id,
                    release=True,
                    retry_after=delay,
                    retry_attempts=work.progress.retry_attempts + 1,
                    failure_phase="disk"
                    if isinstance(error, DiskSpaceError)
                    else stage,
                    last_error=_error_text(error),
                )

    def _metadata(self, work: ArchiveWork) -> None:
        if self.gateway is None:
            self._ziggy_only(work)
            return
        frozen = freeze_metadata(self.youtube.video_metadata(work.video.id))
        self.state.update_archive(
            work.video.id,
            status="download",
            release=True,
            working_directory=str(self.download_root / frozen.operation_id),
            media_name=frozen.media_name,
            frozen_metadata=frozen.to_json(),
            ia_identifier=frozen.identifier,
            operation_id=frozen.operation_id,
            retry_attempts=0,
            last_error=None,
            failure_phase=None,
        )

    def _download(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        directory = _working_directory(work)
        if (
            directory.is_symlink()
            or directory.resolve().parent != self.download_root.resolve()
            or directory.name != frozen.operation_id
        ):
            raise RecoveryConflictError(
                "download staging is outside its owned operation"
            )
        checkpoint = DownloadCheckpoint(
            self.state, work, directory, self.settings.minimum_free_bytes
        )
        try:
            path = self.youtube.download_video(
                work.video.id,
                directory,
                minimum_free_bytes=self.settings.minimum_free_bytes,
                estimated_bytes=work.progress.download_estimated_bytes
                or frozen.estimated_bytes
                or self.settings.unknown_download_bytes,
                disk_guard=checkpoint.record,
                phase_hook=checkpoint.boundary,
                operation_id=frozen.operation_id,
            )
        except BaseException:
            checkpoint.flush()
            raise
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError("downloader did not produce nonempty media")
        facts = checkpoint.facts()
        facts["download_phase"] = "complete"
        facts["download_progress_at"] = utcnow()
        self.state.update_archive(
            work.video.id,
            status="upload",
            release=True,
            media_path=str(path),
            media_size=path.stat().st_size,
            media_md5=None,
            media_sha1=None,
            download_completed_at=utcnow(),
            ready_at=utcnow(),
            retry_attempts=0,
            failure_phase=None,
            last_error=None,
            **facts,
        )

    def _upload(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        path = Path(work.progress.media_path) if work.progress.media_path else None
        if path is None or not path.is_file():
            self.state.update_archive(
                work.video.id,
                status="download",
                release=True,
                media_path=None,
                download_completed_at=None,
                ready_at=None,
                last_error="staging media disappeared; downloading again",
                failure_phase=None,
            )
            return
        if work.progress.download_completed_at is None:
            self.state.update_archive(
                work.video.id,
                status="download",
                release=True,
                media_path=None,
                last_error="download completion is not recorded",
            )
            return
        if path.stat().st_size != work.progress.media_size or path.stat().st_size == 0:
            raise LocalMediaChangedError(
                "staging size changed after download completion"
            )
        directory = _working_directory(work)
        if (
            directory.is_symlink()
            or directory.resolve().parent != self.download_root.resolve()
            or directory.name != frozen.operation_id
            or path.is_symlink()
            or path.resolve().parent != directory.resolve()
        ):
            raise RecoveryConflictError(
                "upload media is outside its owned staging operation"
            )
        self.state.update_archive(
            work.video.id,
            status="uploading",
            upload_started_at=utcnow(),
            transfer_bytes=0,
            transfer_progress_at=utcnow(),
            failure_phase=None,
            last_error=None,
        )
        work.progress.status = "uploading"
        work.progress.transfer_bytes = 0

        def progress(bytes_sent: int) -> None:
            if bytes_sent > work.progress.transfer_bytes:
                self.state.update_archive(
                    work.video.id,
                    transfer_bytes=bytes_sent,
                    transfer_progress_at=utcnow(),
                )
                work.progress.transfer_bytes = bytes_sent

        try:
            operation = (
                self._gateway().recover
                if work.progress.upload_mode == "recover"
                else self._gateway().upload
            )
            result = operation(frozen, path, progress=progress)
        except InternetArchiveUploadError as error:
            if self._acknowledged(work, error.result):
                self._acknowledge(work, error.result)
                return
            self.stop_controller.check()
            code = error.cause.status_code
            known_failure = (
                not isinstance(error.cause, NetworkError)
                and code is not None
                and 400 <= code < 500
                and code not in {408, 409, 412}
            )
            if known_failure and error.failed_file:
                self.state.update_archive(work.video.id, upload_mode="recover")
            self.state.defer_transfer(
                work,
                status="upload" if known_failure else "reconcile_upload",
                error=_error_text(error),
                delay=retry_delay(
                    self.settings.archive_retry_delay, work.progress.retry_attempts + 1
                ),
                deadline=self.settings.background_deadline,
            )
            return
        except ServiceError as error:
            self.stop_controller.check()
            # Both upload APIs wrap service failures once a mutation starts.
            # Retry transient preflight failures without waiting for a remote item.
            if not (
                isinstance(error, (NetworkError, RateLimitError))
                or error.status_code is not None
                and (error.status_code in {408, 425, 429} or error.status_code >= 500)
            ):
                raise
            self.state.defer_transfer(
                work,
                status="upload",
                error=_error_text(error),
                delay=retry_delay(
                    self.settings.archive_retry_delay, work.progress.retry_attempts + 1
                ),
                deadline=self.settings.background_deadline,
            )
            return
        if not self._acknowledged(work, result):
            raise RuntimeError(
                "upload returned without acknowledgement of the expected original"
            )
        self._acknowledge(work, result)

    @staticmethod
    def _acknowledged(work: ArchiveWork, result: InternetArchiveUploadResult) -> bool:
        return (
            result.identifier == work.progress.ia_identifier
            and sum(
                file.name == work.progress.media_name
                and file.size == work.progress.media_size
                and file.transferred
                for file in result.files
            )
            == 1
        )

    def _acknowledge(
        self, work: ArchiveWork, result: InternetArchiveUploadResult
    ) -> None:
        file = next(
            file for file in result.files if file.name == work.progress.media_name
        )
        self.state.acknowledge_upload(
            work,
            receipt=json.dumps(asdict(result), sort_keys=True),
            etag=file.etag,
            validation=self.settings.validation_enabled,
            deadline=self.settings.background_deadline,
        )
        logger.info(
            "Upload acknowledged for video {} ({} bytes)", work.video.id, file.size
        )

    def _inspection(self, work: ArchiveWork, *, validation: bool = False) -> Inspection:
        if work.progress.media_size is None:
            raise RecoveryConflictError("no persisted size for remote reconciliation")
        return self._gateway().inspect(
            _frozen(work),
            size=work.progress.media_size,
            md5_hash=work.progress.media_md5,
            sha1_hash=work.progress.media_sha1,
            validation=validation,
        )

    def _reconcile(self, work: ArchiveWork) -> None:
        inspection = self._inspection(work)
        if inspection.state == "verified":
            self.state.acknowledge_upload(
                work,
                receipt=inspection.receipt or json.dumps({"remote_evidence": True}),
                etag=None,
                recovered=True,
                validation=self.settings.validation_enabled,
                deadline=self.settings.background_deadline,
            )
            self.state.update_job(work, status="passed")
        elif inspection.state == "missing":
            self.state.update_archive(
                work.video.id,
                status="upload",
                release=True,
                upload_mode="recover",
                ready_at=utcnow(),
                failure_phase=None,
                last_error=None,
            )
            self.state.update_job(work, status="passed")
        elif inspection.state == "conflict":
            self.state.update_job(work, status="failed", error=inspection.detail)
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                failure_phase="recovery",
                last_error=inspection.detail,
            )
        else:
            self._defer_job(work, inspection.detail)

    def _validate(self, work: ArchiveWork) -> None:
        if not self.settings.validation_enabled:
            self.state.update_job(
                work, status="expired", error="optional validation disabled"
            )
            return
        inspection = self._inspection(work, validation=True)
        if inspection.state == "verified":
            self.state.update_job(work, status="passed", media_verified_at=utcnow())
        elif inspection.state in {"conflict", "missing"}:
            self.state.update_job(work, status="failed", error=inspection.detail)
        else:
            self._defer_job(work, inspection.detail)

    def _defer_job(self, work: ArchiveWork, detail: str | None) -> None:
        assert work.job is not None
        self.state.update_job(
            work,
            status="waiting",
            error=detail,
            retry_after=retry_delay(
                self.settings.archive_retry_delay, work.job.attempts
            ),
        )

    def _webpage(self, work: ArchiveWork) -> None:
        assert work.job is not None
        if work.progress.wayback_job_id:
            result = self._gateway().page_status(work.progress.wayback_job_id)
            if isinstance(result, InternetArchivePendingStatus):
                self._defer_job(work, "Wayback job is pending")
            elif isinstance(result, InternetArchiveSuccessStatus):
                self.state.update_job(work, status="passed", webpage_status="captured")
            elif isinstance(result, InternetArchiveFailedStatus):
                self.state.update_job(
                    work, status="failed", webpage_status="failed", error=result.message
                )
            else:
                raise RuntimeError("unexpected Wayback status")
            return
        url = (
            _frozen(work).source_url
            if work.progress.frozen_metadata
            else work.video.webpage_url
        )
        self.state.update_job(work, status="submitting")
        work.job.status = "submitting"
        try:
            if self.settings.ziggy_host:
                arguments = (
                    self.settings.ziggy_host,
                    self.settings.ziggy_identifier or "saga",
                    url,
                    self.settings.archive_request_timeout,
                )
                receipt = (
                    run_managed(
                        "ziggy", "submit", {}, arguments, {}, self.stop_controller
                    )
                    if self.managed_io
                    else submit_ziggy(*arguments)
                )
                try:
                    self.state.update_job(
                        work,
                        status="passed",
                        webpage_status="queued",
                        ziggy_receipt=json.dumps(receipt, sort_keys=True),
                    )
                except Exception as error:
                    raise AcceptedSubmissionPersistenceError(str(error)) from error
            else:
                job_id = self._gateway().submit_page(url)
                try:
                    self.state.update_job(
                        work,
                        status="waiting",
                        webpage_status="submitted",
                        wayback_job_id=job_id,
                        retry_after=self.settings.archive_poll_interval,
                    )
                except Exception as error:
                    raise AcceptedSubmissionPersistenceError(str(error)) from error
        except (
            NetworkError,
            SubmissionUncertainError,
            AcceptedSubmissionPersistenceError,
        ) as error:
            self.state.update_job(
                work,
                status="failed",
                webpage_status="uncertain",
                error=_error_text(error),
            )

    def _ziggy_only(self, work: ArchiveWork) -> None:
        # There is no media transfer in this mode; submit through a background job.
        self.state.queue_webpage_only(
            work.video.id, deadline=self.settings.background_deadline
        )

    def _cleanup(self, work: ArchiveWork) -> None:
        if work.progress.media_uploaded_at is None:
            raise RecoveryConflictError(
                "cleanup requires persisted upload acknowledgement"
            )
        directory = _working_directory(work).resolve()
        root = self.download_root.resolve()
        if directory.parent != root or directory.name != work.progress.operation_id:
            raise RecoveryConflictError("refusing cleanup outside this Saga operation")
        if (
            work.progress.media_path
            and Path(work.progress.media_path).resolve().parent != directory
        ):
            raise RecoveryConflictError("media is outside its staging directory")
        if directory.exists():
            shutil.rmtree(directory)
        self.state.update_job(
            work,
            status="passed",
            cleaned_at=utcnow(),
            media_path=None,
            working_directory=None,
        )

    def _gateway(self) -> ArchiveBackend:
        if self.gateway is None:
            raise RuntimeError("Internet Archive credentials are not configured")
        return self.gateway


def freeze_metadata(details: VideoMetadata) -> FrozenArchiveMetadata:
    """Freeze provenance and the original filename before sending any bytes."""
    operation_id = str(uuid4())
    identifier = archive_identifier(details.title, details.id)
    username = details.channel_handle
    published = _iso_date(details.upload_date)
    if details.published_precise and details.published_at is not None:
        published = details.published_at.astimezone(UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    tags = f"Tags: {', '.join(details.tags)}\n" if details.tags else ""
    description = (
        f"{details.description.rstrip()}\n\nPublished: {published}\n"
        f"YouTube URL: {details.webpage_url}\nChannel username: {username}\n"
        f"Channel display name: {details.channel_name}\n"
        f"Channel URL: {details.channel_url}\n{tags}\n"
        "Powered by Saga (https://github.com/EthanC/Saga)"
    ).lstrip()
    return FrozenArchiveMetadata(
        identifier,
        operation_id,
        details.webpage_url,
        f"{identifier}.mkv",
        f"{details.title} | {username} | YouTube",
        description,
        tuple(dict.fromkeys(("youtube", "video", username, details.id, *details.tags))),
        details.channel_name,
        _iso_date(details.upload_date),
        username,
        details.estimated_bytes,
    )


def archive_identifier(title: str, video_id: str) -> str:
    ascii_title = (
        unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    )
    slug = "".join(
        character.lower() if character.isalnum() else "-" for character in ascii_title
    )
    slug = "-".join(part for part in slug.split("-") if part)
    suffix = f"-{video_id}"
    prefix = slug[: MAX_IDENTIFIER_LENGTH - len(suffix)].rstrip("-") or "video"
    return f"{prefix}{suffix}"


def inspect_item(
    item: InternetArchiveItem,
    metadata: FrozenArchiveMetadata,
    size: int,
    md5_hash: str | None = None,
    sha1_hash: str | None = None,
    *,
    validation: bool = False,
) -> Inspection:
    """Compare saved facts without reading staging or waiting for derivatives."""
    if item.identifier != metadata.identifier:
        return Inspection("conflict", "wrong item identifier")
    if not item.metadata:
        return Inspection("pending", "item metadata is unresolved")
    if any(
        not _metadata_matches(item.metadata, name, expected)
        for name, expected in (
            ("source", metadata.source_url),
            ("saga_operation_id", metadata.operation_id),
        )
    ):
        return Inspection("conflict", "Saga operation provenance does not match")
    matches = [file for file in item.original_files if file.name == metadata.media_name]
    if len(matches) > 1:
        return Inspection("conflict", "original filename is ambiguous")
    if not matches:
        if (
            item.listing_complete
            and not item.server_unavailable
            and item.availability is InternetArchiveItemAvailability.AVAILABLE
        ):
            return Inspection(
                "missing", "expected original is absent from a complete listing"
            )
        return Inspection("pending", "original listing is unresolved")
    remote = matches[0]
    if (
        remote.size is not None
        and remote.size != size
        or any(
            saved and actual and saved.lower() != actual.lower()
            for saved, actual in ((md5_hash, remote.md5), (sha1_hash, remote.sha1))
        )
    ):
        return Inspection("conflict", "saved original size or checksums do not match")
    if (
        remote.size is None
        or validation
        and any(
            saved and not actual
            for saved, actual in ((md5_hash, remote.md5), (sha1_hash, remote.sha1))
        )
    ):
        return Inspection("pending", "original facts are incomplete")
    return Inspection(
        "verified",
        receipt=json.dumps(
            {
                "identifier": item.identifier,
                "operation_id": metadata.operation_id,
                "original": asdict(remote),
            },
            sort_keys=True,
        ),
    )


def submit_ziggy(
    host: str, identifier: str, url: str, timeout: float
) -> dict[str, str]:
    body = json.dumps({"identifier": identifier, "urls": [url]}).encode()
    request = Request(
        f"{host.rstrip('/')}/v1/queue",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 202:
                raise SubmissionRejectedError(
                    f"Ziggy returned HTTP {response.status}, expected 202"
                )
            payload = response.read()
    except HTTPError as error:
        raise SubmissionRejectedError(f"Ziggy rejected the request: {error}") from error
    except (HTTPException, OSError) as error:
        raise SubmissionUncertainError(f"Ziggy response was lost: {error}") from error
    try:
        submissions = json.loads(payload)["submissions"]
        receipt = submissions[0]
        if (
            len(submissions) != 1
            or receipt.get("url") != url
            or not isinstance(receipt.get("receipt_id"), str)
            or not receipt["receipt_id"]
        ):
            raise ValueError("invalid receipt")
    except (KeyError, IndexError, TypeError, ValueError, AttributeError) as error:
        raise SubmissionUncertainError("Ziggy returned an invalid receipt") from error
    return {"receipt_id": receipt["receipt_id"], "url": receipt["url"]}


def _frozen(work: ArchiveWork) -> FrozenArchiveMetadata:
    if not work.progress.frozen_metadata:
        raise RuntimeError("archive stage has no frozen metadata")
    frozen = FrozenArchiveMetadata.from_json(work.progress.frozen_metadata)
    if (frozen.identifier, frozen.operation_id, frozen.media_name) != (
        work.progress.ia_identifier,
        work.progress.operation_id,
        work.progress.media_name,
    ):
        raise RecoveryConflictError(
            "frozen upload contract does not match persisted operation"
        )
    return frozen


def _working_directory(work: ArchiveWork) -> Path:
    if not work.progress.working_directory:
        raise RuntimeError("archive stage has no working directory")
    return Path(work.progress.working_directory)


def _metadata_matches(metadata: Any, name: str, expected: str) -> bool:
    value = metadata.get(name)
    return (
        list(value) == [expected]
        if isinstance(value, (tuple, list))
        else value == expected
    )


def _iso_date(upload_date: str) -> str:
    return date(
        int(upload_date[:4]), int(upload_date[4:6]), int(upload_date[6:8])
    ).isoformat()


def _error_text(error: Exception) -> str:
    return redact_diagnostics(f"{type(error).__name__}: {error}")[:2000]
