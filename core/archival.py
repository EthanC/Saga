"""Restart-safe video, Archive.org, webpage, and local cleanup workflow."""

from __future__ import annotations

import json
import shutil
import subprocess
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date
from hashlib import md5, sha1
from pathlib import Path
from threading import Lock
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from archivist import (
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveCaptureStatus,
    InternetArchiveClient,
    InternetArchiveFailedStatus,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveItemTaskState,
    InternetArchivePendingStatus,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveRecoveryError,
    InternetArchiveRecoveryResult,
    InternetArchiveSuccessStatus,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadOptions,
    InvalidOptionError,
    NetworkError,
    PollingTimeoutError,
    RateLimitError,
)
from loguru import logger
from niquests import Session

from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter
from core.config import Settings
from core.state import ArchiveWork, StateStore, utcnow
from core.youtube import (
    DiskSpaceError,
    VideoMetadata,
    VideoUnavailableError,
    YouTubeClient,
    verify_media,
)

MAX_IDENTIFIER_LENGTH = 100


@dataclass(frozen=True, slots=True)
class FrozenArchiveMetadata:
    """Contain every value that must remain stable across archive retries."""

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
        """Serialize the immutable upload contract for SQLite."""
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, value: str) -> FrozenArchiveMetadata:
        """Load and validate a persisted upload contract."""
        document: Any = json.loads(value)
        if not isinstance(document, dict):
            raise ValueError("frozen archive metadata is not an object")
        document["subjects"] = tuple(document["subjects"])
        return cls(**document)


@dataclass(frozen=True, slots=True)
class Inspection:
    """Describe a read-only comparison against one Archive.org item."""

    state: str
    detail: str | None = None


class SubmissionRejectedError(RuntimeError):
    """Indicate a response that confirms a webpage submission was rejected."""


class SubmissionUncertainError(RuntimeError):
    """Indicate that a webpage submission may have been accepted remotely."""


class LocalMediaChangedError(RuntimeError):
    """Indicate that a verified local source no longer matches persisted hashes."""


class AcceptedSubmissionPersistenceError(RuntimeError):
    """Indicate acceptance succeeded but its receipt could not be committed."""

    def __init__(self, phase: str, cause: Exception) -> None:
        super().__init__(
            f"{phase} accepted a submission but persistence failed: {cause}"
        )
        self.phase = phase


class ArchiveGateway:
    """Wrap Archivist 1.3.0 calls without adding mutation retries."""

    def __init__(
        self, settings: Settings, upload_limiter: ByteLimiter | None = None
    ) -> None:
        assert settings.internet_archive_email is not None
        assert settings.internet_archive_password is not None
        self.account = InternetArchiveAccount(
            settings.internet_archive_email, settings.internet_archive_password
        )
        self.request_timeout = settings.archive_request_timeout
        self.poll_timeout = settings.archive_poll_timeout
        self.poll_interval = settings.archive_poll_interval
        self.upload_limiter = upload_limiter or ByteLimiter(0)

    def _session(self) -> Session:
        session = Session(retries=0)
        if self.upload_limiter.enabled:
            for prefix in ("https://", "http://"):
                previous = session.get_adapter(prefix)
                session.mount(
                    prefix, BandwidthHTTPAdapter(self.upload_limiter, max_retries=0)
                )
                previous.close()
        return session

    def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None:
        """Attempt one create-only upload; the caller persists uncertainty."""
        logger.info("Uploading {} to Archive.org", metadata.identifier)
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
        upload_file = InternetArchiveUploadFile(media_path, name=metadata.media_name)
        with (
            self._session() as session,
            InternetArchiveClient(
                session=session, account=self.account, timeout=self.request_timeout
            ) as client,
        ):
            client.upload(
                [upload_file],
                options,
                wait=True,
                timeout=self.poll_timeout,
                poll_interval=self.poll_interval,
            )
        logger.info("Archive.org upload returned for {}", metadata.identifier)

    def inspect(
        self,
        metadata: FrozenArchiveMetadata,
        *,
        size: int,
        md5_hash: str,
        sha1_hash: str,
    ) -> Inspection:
        """Read and compare provenance plus all expected original-file facts."""
        logger.debug("Inspecting Archive.org item {}", metadata.identifier)
        with (
            self._session() as session,
            InternetArchiveClient(
                session=session, account=self.account, timeout=self.request_timeout
            ) as client,
        ):
            item = client.get_item(metadata.identifier)
        return inspect_item(item, metadata, size, md5_hash, sha1_hash)

    def recover(
        self, metadata: FrozenArchiveMetadata, media_path: Path
    ) -> InternetArchiveRecoveryResult:
        """Add only a missing file using exact persisted provenance guards."""
        logger.warning(
            "Recovering missing media in Archive.org item {}", metadata.identifier
        )
        upload_file = InternetArchiveUploadFile(media_path, name=metadata.media_name)
        with (
            self._session() as session,
            InternetArchiveClient(
                session=session, account=self.account, timeout=self.request_timeout
            ) as client,
        ):
            return client.add_files(
                metadata.identifier,
                [upload_file],
                expected_metadata={
                    "source": metadata.source_url,
                    "saga_operation_id": metadata.operation_id,
                },
                wait=True,
                timeout=self.poll_timeout,
                poll_interval=self.poll_interval,
            )

    def submit_page(self, url: str) -> str:
        """Submit a direct Wayback job and return its durable identifier."""
        logger.info("Submitting webpage to the Wayback Machine: {}", url)
        with (
            self._session() as session,
            InternetArchiveClient(
                session=session, account=self.account, timeout=self.request_timeout
            ) as client,
        ):
            return client.submit(url).job_id

    def page_status(self, job_id: str) -> InternetArchiveCaptureStatus:
        """Read a previously submitted Wayback job without resubmitting it."""
        logger.debug("Checking Wayback job {}", job_id)
        with (
            self._session() as session,
            InternetArchiveClient(
                session=session, account=self.account, timeout=self.request_timeout
            ) as client,
        ):
            return client.status(job_id)


class ArchiveBackend(Protocol):
    """Describe the external archive operations used by the state machine."""

    def upload(self, metadata: FrozenArchiveMetadata, media_path: Path) -> None: ...

    def inspect(
        self,
        metadata: FrozenArchiveMetadata,
        *,
        size: int,
        md5_hash: str,
        sha1_hash: str,
    ) -> Inspection: ...

    def recover(
        self, metadata: FrozenArchiveMetadata, media_path: Path
    ) -> InternetArchiveRecoveryResult: ...

    def submit_page(self, url: str) -> str: ...

    def page_status(self, job_id: str) -> InternetArchiveCaptureStatus: ...


class ArchiveProcessor:
    """Advance one claimed archive row by one externally visible operation."""

    def __init__(
        self,
        settings: Settings,
        state: StateStore,
        youtube: YouTubeClient,
        gateway: ArchiveBackend | None = None,
        upload_limiter: ByteLimiter | None = None,
    ) -> None:
        self.settings = settings
        self.state = state
        self.youtube = youtube
        self.gateway = gateway or ArchiveGateway(settings, upload_limiter)
        self.download_root = settings.data_path / "downloads"
        self._download_lock = Lock()

    def process(self, work: ArchiveWork) -> None:
        """Advance the stage captured in a detached archive claim."""
        logger.debug(
            "Processing archive stage {} for video {}",
            work.progress.status,
            work.video.id,
        )
        handlers = {
            "metadata": self._metadata,
            "download": self._download,
            "upload": self._upload,
            "uploading": self._reconcile,
            "reconcile_upload": self._reconcile,
            "webpage": self._webpage,
            "wayback_wait": self._wayback_wait,
            "cleanup": self._cleanup,
        }
        handler = handlers.get(work.progress.status)
        if handler is None:
            logger.error(
                "Video {} has unknown archive stage {!r}; operator action is required",
                work.video.id,
                work.progress.status,
            )
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                failure_phase="state",
                last_error=f"unknown archive stage {work.progress.status!r}",
            )
            return
        try:
            handler(work)
        except VideoUnavailableError as error:
            logger.warning("Video {} is unavailable: {}", work.video.id, error)
            self.state.update_archive(
                work.video.id,
                status="unavailable",
                release=True,
                failure_phase=work.progress.status,
                last_error=_error_text(error),
            )
        except DiskSpaceError as error:
            self._retry(work, error, "disk")
        except LocalMediaChangedError as error:
            logger.error(
                "Local media verification failed for video {}: {}", work.video.id, error
            )
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                failure_phase="local_verification",
                last_error=_error_text(error),
            )
        except AcceptedSubmissionPersistenceError as error:
            logger.critical(
                "Accepted {} submission for video {} could not be persisted: {}",
                error.phase,
                work.video.id,
                error,
            )
            self._uncertain_submission(work, error, error.phase)
        except Exception as error:
            self._retry(work, error, work.progress.status)

    def _metadata(self, work: ArchiveWork) -> None:
        details = self.youtube.video_metadata(work.video.id)
        frozen = freeze_metadata(details)
        directory = self.download_root / frozen.operation_id
        self.state.update_archive(
            work.video.id,
            status="download",
            release=True,
            working_directory=str(directory),
            media_name=frozen.media_name,
            frozen_metadata=frozen.to_json(),
            ia_identifier=frozen.identifier,
            operation_id=frozen.operation_id,
            failure_phase=None,
            last_error=None,
        )
        logger.info(
            "Prepared archive metadata for video {} as {}",
            work.video.id,
            frozen.identifier,
        )

    def _download(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        directory = _working_directory(work)
        with self._download_lock:
            path = self.youtube.download_video(
                work.video.id,
                directory,
                minimum_free_bytes=self.settings.minimum_free_bytes,
                estimated_bytes=frozen.estimated_bytes,
            )
        try:
            media = verify_media(path)
        except Exception as error:
            if (
                isinstance(
                    error,
                    (subprocess.CalledProcessError, json.JSONDecodeError, RuntimeError),
                )
                and path == directory / "media.mkv"
            ):
                path.unlink(missing_ok=True)
            raise
        self.state.update_archive(
            work.video.id,
            status="upload",
            release=True,
            media_path=str(media.path),
            media_size=media.size,
            media_md5=media.md5,
            media_sha1=media.sha1,
            failure_phase=None,
            last_error=None,
        )
        logger.info(
            "Staged verified media for video {} ({} bytes)", work.video.id, media.size
        )

    def _upload(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        path, size, md5_hash, sha1_hash = _media_facts(work)
        self.state.update_archive(
            work.video.id,
            status="uploading",
            upload_started_at=utcnow(),
            failure_phase=None,
            last_error=None,
        )
        work.progress.status = "uploading"
        try:
            self.gateway.upload(frozen, path)
        except InternetArchiveUploadError as error:
            # Archivist sets failed_file before transfer and records transferred
            # files before polling. Timeouts and ID conflicts do not prove absence.
            rejected = (
                error.failed_file is None
                and bool(error.result.files)
                and not any(file.transferred for file in error.result.files)
                and not error.result.processing_complete
                and not isinstance(error.cause, NetworkError)
                and error.cause.status_code is not None
                and 400 <= error.cause.status_code < 500
                and error.cause.status_code not in {408, 409, 412}
            )
            logger.warning(
                "Archive.org upload for video {} {}: {}",
                work.video.id,
                (
                    "was rejected; creation will retry"
                    if rejected
                    else "is uncertain and will be reconciled"
                ),
                error.cause,
            )
            self.state.update_archive(
                work.video.id,
                status="upload" if rejected else "reconcile_upload",
                release=True,
                retry_after=self.settings.archive_retry_delay,
                failure_phase="initial_upload",
                last_error=_error_text(error),
            )
            return
        except (AuthenticationError, InvalidOptionError) as error:
            logger.error(
                "Archive.org rejected upload preflight for video {}: {}",
                work.video.id,
                error,
            )
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                failure_phase="initial_upload_preflight",
                last_error=_error_text(error),
            )
            return
        except Exception as error:
            logger.warning(
                "Archive.org upload preflight failed for video {}; retry scheduled: {}",
                work.video.id,
                error,
            )
            self.state.update_archive(
                work.video.id,
                status="upload",
                release=True,
                retry_after=self.settings.archive_retry_delay,
                failure_phase="initial_upload_preflight",
                last_error=_error_text(error),
            )
            return
        inspection = self.gateway.inspect(
            frozen, size=size, md5_hash=md5_hash, sha1_hash=sha1_hash
        )
        self._finish_inspection(work, inspection)

    def _reconcile(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        path, size, md5_hash, sha1_hash = _media_facts(work)
        inspection = self.gateway.inspect(
            frozen, size=size, md5_hash=md5_hash, sha1_hash=sha1_hash
        )
        logger.debug(
            "Archive.org inspection for video {} returned {}",
            work.video.id,
            inspection.state,
        )
        if inspection.state == "missing":
            try:
                result = self.gateway.recover(frozen, path)
            except InternetArchiveRecoveryDeferredError as error:
                logger.warning(
                    "Archive.org recovery was deferred for video {}: {}",
                    work.video.id,
                    error,
                )
                self.state.update_archive(
                    work.video.id,
                    status="reconcile_upload",
                    release=True,
                    retry_after=self.settings.archive_retry_delay,
                    recovery_result=_recovery_json(error.result),
                    failure_phase=error.phase.value,
                    last_error=_error_text(error),
                )
                return
            except InternetArchiveRecoveryError as error:
                dispositions = {item.disposition.value for item in error.result.files}
                uncertain = bool(
                    dispositions & {"uncertain", "deferred"}
                ) or _temporary_recovery_error(error)
                log = logger.warning if uncertain else logger.error
                log(
                    "Archive.org recovery {} for video {}: {}",
                    "will retry" if uncertain else "requires operator action",
                    work.video.id,
                    error,
                )
                self.state.update_archive(
                    work.video.id,
                    status="reconcile_upload" if uncertain else "operator_action",
                    release=True,
                    retry_after=(
                        self.settings.archive_retry_delay if uncertain else None
                    ),
                    recovery_result=_recovery_json(error.result),
                    failure_phase=error.phase.value,
                    last_error=_error_text(error),
                )
                return
            if not result.verification_complete or not _recovery_matches(
                result, frozen, size=size, md5_hash=md5_hash, sha1_hash=sha1_hash
            ):
                logger.warning(
                    "Archive.org recovery verification is incomplete for video {}",
                    work.video.id,
                )
                self.state.update_archive(
                    work.video.id,
                    status="reconcile_upload",
                    release=True,
                    retry_after=self.settings.archive_retry_delay,
                    recovery_result=_recovery_json(result),
                    failure_phase="verification",
                    last_error="Archivist recovery verification is incomplete",
                )
                return
            self._mark_media_verified(work, _recovery_json(result))
            return
        self._finish_inspection(work, inspection)

    def _finish_inspection(self, work: ArchiveWork, inspection: Inspection) -> None:
        if inspection.state == "verified":
            self._mark_media_verified(work, None)
        elif inspection.state == "conflict":
            logger.error(
                "Archive.org item conflicts with video {}: {}",
                work.video.id,
                inspection.detail,
            )
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                failure_phase="verification",
                last_error=inspection.detail,
            )
        else:
            logger.warning(
                "Archive.org item for video {} is not yet verified: {}",
                work.video.id,
                inspection.detail,
            )
            self.state.update_archive(
                work.video.id,
                status="reconcile_upload",
                release=True,
                retry_after=self.settings.archive_retry_delay,
                failure_phase="reconciliation",
                last_error=inspection.detail,
            )

    def _mark_media_verified(
        self, work: ArchiveWork, recovery_result: str | None
    ) -> None:
        self.state.update_archive(
            work.video.id,
            status="webpage",
            release=True,
            media_verified_at=utcnow(),
            recovery_result=recovery_result,
            failure_phase=None,
            last_error=None,
        )
        logger.info("Verified archived media for video {}", work.video.id)

    def _webpage(self, work: ArchiveWork) -> None:
        frozen = _frozen(work)
        if self.settings.ziggy_host is not None:
            logger.info("Submitting webpage for video {} through Ziggy", work.video.id)
            self.state.update_archive(work.video.id, status="ziggy_submitting")
            try:
                receipt = submit_ziggy(
                    self.settings.ziggy_host,
                    self.settings.ziggy_identifier or "saga",
                    frozen.source_url,
                    self.settings.archive_request_timeout,
                )
            except SubmissionRejectedError as error:
                self._retry_submission(work, error, "ziggy")
                return
            except Exception as error:
                self._uncertain_submission(work, error, "ziggy")
                return
            try:
                self.state.update_archive(
                    work.video.id,
                    status="cleanup",
                    release=True,
                    webpage_status="queued",
                    ziggy_receipt=json.dumps(receipt, sort_keys=True),
                    failure_phase=None,
                    last_error=None,
                )
            except Exception as error:
                raise AcceptedSubmissionPersistenceError("ziggy", error) from error
            return
        logger.info("Submitting webpage for video {} directly", work.video.id)
        self.state.update_archive(work.video.id, status="wayback_submitting")
        try:
            job_id = self.gateway.submit_page(frozen.source_url)
        except NetworkError as error:
            self._uncertain_submission(work, error, "wayback")
            return
        except Exception as error:
            self._retry_submission(work, error, "wayback")
            return
        try:
            self.state.update_archive(
                work.video.id,
                status="wayback_wait",
                release=True,
                retry_after=self.settings.archive_poll_interval,
                wayback_job_id=job_id,
                webpage_status="submitted",
                failure_phase=None,
                last_error=None,
            )
            logger.info("Persisted Wayback job {} for video {}", job_id, work.video.id)
        except Exception as error:
            raise AcceptedSubmissionPersistenceError("wayback", error) from error

    def _retry_submission(
        self, work: ArchiveWork, error: Exception, phase: str
    ) -> None:
        logger.warning(
            "{} webpage submission failed for video {}; retry scheduled: {}",
            phase,
            work.video.id,
            error,
        )
        self.state.update_archive(
            work.video.id,
            status="webpage",
            release=True,
            retry_after=self.settings.archive_retry_delay,
            failure_phase=phase,
            last_error=_error_text(error),
        )

    def _uncertain_submission(
        self, work: ArchiveWork, error: Exception, phase: str
    ) -> None:
        logger.error(
            "{} webpage submission outcome is uncertain for video {}: {}",
            phase,
            work.video.id,
            error,
        )
        self.state.update_archive(
            work.video.id,
            status="operator_action",
            release=True,
            failure_phase=phase,
            last_error=f"submission outcome is uncertain; {_error_text(error)}",
        )

    def _wayback_wait(self, work: ArchiveWork) -> None:
        job_id = work.progress.wayback_job_id
        if not job_id:
            raise RuntimeError("direct Wayback stage has no persisted job ID")
        result = self.gateway.page_status(job_id)
        if isinstance(result, InternetArchivePendingStatus):
            logger.debug("Wayback job {} is still pending", job_id)
            self.state.update_archive(
                work.video.id,
                release=True,
                retry_after=self.settings.archive_poll_interval,
                last_error=None,
            )
        elif isinstance(result, InternetArchiveSuccessStatus):
            logger.info("Wayback capture completed for video {}", work.video.id)
            self.state.update_archive(
                work.video.id,
                status="cleanup",
                release=True,
                webpage_status="captured",
                failure_phase=None,
                last_error=None,
            )
        elif isinstance(result, InternetArchiveFailedStatus):
            logger.error(
                "Wayback capture failed for video {}: {}",
                work.video.id,
                result.message or "no detail returned",
            )
            self.state.update_archive(
                work.video.id,
                status="operator_action",
                release=True,
                webpage_status="failed",
                failure_phase="wayback",
                last_error=result.message or "Wayback capture failed",
            )
        else:
            raise RuntimeError(f"unexpected Wayback status {type(result).__name__}")

    def _cleanup(self, work: ArchiveWork) -> None:
        if work.progress.media_verified_at is None:
            raise RuntimeError("cleanup requested before media verification")
        if work.progress.webpage_status not in {"captured", "queued"}:
            raise RuntimeError("cleanup requested before webpage acceptance")
        directory = _working_directory(work).resolve()
        root = self.download_root.resolve()
        operation_id = work.progress.operation_id or _frozen(work).operation_id
        if directory.parent != root or directory.name != operation_id:
            raise RuntimeError("refusing to clean a directory outside Saga downloads")
        if work.progress.media_path is not None:
            media_path = Path(work.progress.media_path).resolve()
            if media_path.parent != directory:
                raise RuntimeError("refusing to clean media outside its work directory")
        if directory.exists():
            logger.debug("Removing archive staging directory {}", directory)
            shutil.rmtree(directory)
        self.state.update_archive(
            work.video.id,
            status="complete",
            release=True,
            cleaned_at=utcnow(),
            media_path=None,
            working_directory=None,
            failure_phase=None,
            last_error=None,
        )
        logger.info("Completed archive workflow for video {}", work.video.id)

    def _retry(self, work: ArchiveWork, error: Exception, phase: str) -> None:
        status = (
            "reconcile_upload"
            if work.progress.status == "uploading"
            else work.progress.status
        )
        logger.warning(
            "Archive phase {} failed for video {}; retry scheduled: {}",
            phase,
            work.video.id,
            error,
        )
        self.state.update_archive(
            work.video.id,
            status=status,
            release=True,
            retry_after=self.settings.archive_retry_delay,
            failure_phase=phase,
            last_error=_error_text(error),
        )


def freeze_metadata(details: VideoMetadata) -> FrozenArchiveMetadata:
    """Build the exact immutable Archive.org identifier and item metadata."""
    operation_id = str(uuid4())
    identifier = archive_identifier(details.title, details.id)
    username = details.channel_handle
    description = (
        f"{details.description.rstrip()}\n\n"
        f"Original upload date: {_iso_date(details.upload_date)}\n"
        f"YouTube URL: {details.webpage_url}\n"
        f"Channel username: {username}\n"
        f"Channel display name: {details.channel_name}\n"
        f"Channel URL: {details.channel_url}"
    ).lstrip()
    return FrozenArchiveMetadata(
        identifier=identifier,
        operation_id=operation_id,
        source_url=details.webpage_url,
        media_name=f"{identifier}.mkv",
        title=f"{details.title} | {username} | YouTube",
        description=description,
        subjects=("youtube", "video", username, details.id),
        creator=details.channel_name,
        upload_date=_iso_date(details.upload_date),
        channel_username=username,
        estimated_bytes=details.estimated_bytes,
    )


def archive_identifier(title: str, video_id: str) -> str:
    """Return an IA-safe ASCII title slug with the case-sensitive ID suffix."""
    ascii_title = (
        unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    )
    slug = "".join(
        character.lower() if character.isalnum() else "-" for character in ascii_title
    )
    slug = "-".join(part for part in slug.split("-") if part)
    suffix = f"-{video_id}"
    prefix = slug[: MAX_IDENTIFIER_LENGTH - len(suffix)].rstrip("-")
    if not prefix:
        prefix = "video"
    return f"{prefix}{suffix}"


def inspect_item(
    item: InternetArchiveItem,
    metadata: FrozenArchiveMetadata,
    size: int,
    md5_hash: str,
    sha1_hash: str,
) -> Inspection:
    """Compare a snapshot without interpreting an empty response as absence."""
    if not item.metadata:
        return Inspection("pending", "Archive.org item metadata is unresolved")
    if not _metadata_matches(
        item.metadata, "source", metadata.source_url
    ) or not _metadata_matches(
        item.metadata, "saga_operation_id", metadata.operation_id
    ):
        return Inspection("conflict", "Archive.org provenance metadata does not match")
    if item.has_redrow or item.is_dark or item.nodownload:
        return Inspection("conflict", "Archive.org item is blocked or unavailable")
    if item.server_unavailable:
        return Inspection("pending", "Archive.org item server is unavailable")
    if item.error_tasks:
        return Inspection("conflict", "Archive.org item has failed processing tasks")
    if item.pending_tasks or any(
        task.state is InternetArchiveItemTaskState.UNKNOWN for task in item.tasks
    ):
        return Inspection("pending", "Archive.org item processing is unresolved")
    if item.availability is not InternetArchiveItemAvailability.AVAILABLE:
        return Inspection(
            "pending", f"Archive.org item availability is {item.availability.value}"
        )
    if not item.listing_complete:
        return Inspection("pending", "Archive.org file listing is incomplete")
    matches = [file for file in item.original_files if file.name == metadata.media_name]
    if not matches:
        return Inspection("missing", "expected original file is missing")
    remote = matches[0]
    if (
        (remote.size is not None and remote.size != size)
        or (remote.md5 is not None and remote.md5 != md5_hash)
        or (remote.sha1 is not None and remote.sha1 != sha1_hash)
    ):
        return Inspection(
            "conflict", "Archive.org original file size or checksums do not match"
        )
    if remote.size is None or remote.md5 is None or remote.sha1 is None:
        return Inspection(
            "pending", "Archive.org original file size or checksums are incomplete"
        )
    return Inspection("verified")


def submit_ziggy(
    host: str, identifier: str, url: str, timeout: float
) -> dict[str, str]:
    """Queue one URL and require Ziggy's committed 202 receipt."""
    logger.debug("Submitting webpage to Ziggy at {}", host)
    endpoint = f"{host.rstrip('/')}/v1/queue"
    body = json.dumps({"identifier": identifier, "urls": [url]}).encode()
    request = Request(
        endpoint, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            payload = response.read()
    except HTTPError as error:
        raise SubmissionRejectedError(f"Ziggy rejected the request: {error}") from error
    except URLError as error:
        raise SubmissionUncertainError(
            f"Ziggy submission outcome is uncertain: {error}"
        ) from error
    if status != 202:
        raise SubmissionRejectedError(f"Ziggy returned HTTP {status}, expected 202")
    try:
        document: Any = json.loads(payload)
        submissions = document["submissions"]
        receipt = submissions[0]
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("Ziggy returned an invalid receipt") from error
    if (
        len(submissions) != 1
        or not isinstance(receipt, dict)
        or receipt.get("url") != url
        or not isinstance(receipt.get("receipt_id"), str)
        or not receipt["receipt_id"]
    ):
        raise RuntimeError("Ziggy returned an invalid receipt")
    logger.info("Ziggy accepted webpage submission for {}", url)
    return {"receipt_id": receipt["receipt_id"], "url": receipt["url"]}


def _frozen(work: ArchiveWork) -> FrozenArchiveMetadata:
    if not work.progress.frozen_metadata:
        raise RuntimeError("archive stage has no frozen metadata")
    return FrozenArchiveMetadata.from_json(work.progress.frozen_metadata)


def _working_directory(work: ArchiveWork) -> Path:
    if not work.progress.working_directory:
        raise RuntimeError("archive stage has no working directory")
    return Path(work.progress.working_directory)


def _media_facts(work: ArchiveWork) -> tuple[Path, int, str, str]:
    progress = work.progress
    media_path = progress.media_path
    media_size = progress.media_size
    media_md5 = progress.media_md5
    media_sha1 = progress.media_sha1
    if (
        media_path is None
        or media_size is None
        or media_md5 is None
        or media_sha1 is None
    ):
        raise RuntimeError("archive stage has incomplete local media facts")
    path = Path(media_path)
    if not path.is_file():
        raise RuntimeError("verified local media file is missing")
    current_size, current_md5, current_sha1 = _hash_file(path)
    if (current_size, current_md5, current_sha1) != (media_size, media_md5, media_sha1):
        raise LocalMediaChangedError("local media changed after verification")
    return path, media_size, media_md5, media_sha1


def _hash_file(path: Path) -> tuple[int, str, str]:
    md5_hash = md5(usedforsecurity=False)
    sha1_hash = sha1(usedforsecurity=False)
    size = 0
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            size += len(chunk)
            md5_hash.update(chunk)
            sha1_hash.update(chunk)
    return size, md5_hash.hexdigest(), sha1_hash.hexdigest()


def _recovery_matches(
    result: InternetArchiveRecoveryResult,
    metadata: FrozenArchiveMetadata,
    *,
    size: int,
    md5_hash: str,
    sha1_hash: str,
) -> bool:
    matches = [file for file in result.files if file.name == metadata.media_name]
    return (
        len(matches) == 1
        and matches[0].disposition.value in {"transferred", "already_matching"}
        and (
            matches[0].size,
            matches[0].md5,
            matches[0].sha1,
            matches[0].checksum_state.value,
        )
        == (size, md5_hash, sha1_hash, "verified")
    )


def _temporary_recovery_error(error: InternetArchiveRecoveryError) -> bool:
    cause = error.cause
    return (
        isinstance(cause, (NetworkError, PollingTimeoutError, RateLimitError))
        or cause.status_code in {408, 425, 429}
        or cause.status_code is not None
        and cause.status_code >= 500
    )


def _metadata_matches(metadata: Any, name: str, expected: str) -> bool:
    value = metadata.get(name)
    if isinstance(value, (tuple, list)):
        return list(value) == [expected]
    return value == expected


def _recovery_json(result: InternetArchiveRecoveryResult) -> str:
    return json.dumps(
        {
            "identifier": result.identifier,
            "processing_complete": result.processing_complete,
            "verification_complete": result.verification_complete,
            "files": [
                {
                    "name": file.name,
                    "size": file.size,
                    "md5": file.md5,
                    "sha1": file.sha1,
                    "disposition": file.disposition.value,
                    "checksum_state": file.checksum_state.value,
                    "transferred": file.transferred,
                }
                for file in result.files
            ],
        },
        sort_keys=True,
    )


def _iso_date(upload_date: str) -> str:
    return date(
        int(upload_date[:4]), int(upload_date[4:6]), int(upload_date[6:8])
    ).isoformat()


def _error_text(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:2000]
