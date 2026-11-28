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
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

from archivist import (
    AuthenticationError,
    InternetArchiveAccount,
    InternetArchiveCaptureStatus,
    InternetArchiveClient,
    InternetArchiveFailedStatus,
    InternetArchiveItem,
    InternetArchiveItemAvailability,
    InternetArchiveMultipartError,
    InternetArchiveMultipartResult,
    InternetArchiveMultipartState,
    InternetArchivePendingStatus,
    InternetArchiveRecoveryConflictError,
    InternetArchiveRecoveryDeferredError,
    InternetArchiveSuccessStatus,
    InternetArchiveTaskStatus,
    InternetArchiveTaskSubmission,
    InternetArchiveTaskSummary,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadOptions,
    InternetArchiveUploadResult,
    InvalidOptionError,
    NetworkError,
    RateLimitError,
    ServiceError,
)
from loguru import logger
from niquests import Session

from core.archive_derives import DeriveDispatcher
from core.archive_errors import (
    DEFERRED_PREFIX,
    RecoveryConflictError,
    failure_cause,
    failure_facts,
)
from core.archive_retry import ITEM_THROTTLED, retry_hint, retryable_error
from core.archive_tasks import TaskBacklog, TaskSource, TaskSummaryCache
from core.bandwidth import BandwidthHTTPAdapter, ByteLimiter, Limiter
from core.config import (
    MULTIPART_MAX_PART_SIZE,
    MULTIPART_MAX_PARTS,
    DerivePolicy,
    Settings,
)
from core.logging import redact_diagnostics
from core.progress import DownloadCheckpoint
from core.shutdown import CooperativeStop, StopController
from core.staging import ensure_download_root
from core.state import ArchiveWork, StateStore, retry_delay, utcnow
from core.webpage_errors import SubmissionNotSentError, known_connect_failure
from core.workers import run_managed
from core.youtube import (
    DiskSpaceError,
    VideoMetadata,
    VideoUnavailableError,
    YouTubeClient,
)
from core.youtube_errors import VideoAccessError

MAX_IDENTIFIER_LENGTH = 100
CREDENTIAL_PHASES = frozenset({"csrf", "login", "upload-key"})
TASK_PHASES = frozenset({"item-create", "file-upload"})


class _NoSubmissionRedirects(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


urlopen = build_opener(_NoSubmissionRedirects()).open


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
        return json.dumps(asdict(self), sort_keys=True)

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


class _MultipartProgressStream(_ProgressStream):
    """Report hashing reads as liveness and only part reads as transfer progress.

    Archivist 1.6.1 hashes the whole source with sequential reads before it
    sends any request, so the first ``size`` bytes read are never transferred.
    """

    def __init__(
        self,
        stream: Any,
        size: int,
        callback: Callable[[int], None],
        heartbeat: Callable[[], None],
        controller: StopController,
    ) -> None:
        super().__init__(stream, callback, controller)
        self.unhashed = size
        self.heartbeat = heartbeat

    def read(self, amount: int = -1) -> bytes:
        if not self.unhashed:
            return super().read(amount)
        cast(StopController, self.controller).check()
        chunk = self.stream.read(amount)
        self.unhashed = max(0, self.unhashed - len(chunk))
        if time.monotonic() - self.last_update >= 5:
            self.heartbeat()
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
        tasks: TaskSource | None = None,
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
        self.tasks = tasks or TaskSummaryCache(settings.archive_task_summary_ttl)

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

    def _client(self, session: Session) -> InternetArchiveClient:
        return InternetArchiveClient(
            session=session, account=self.account, timeout=self.request_timeout
        )

    def upload(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
        derive: DerivePolicy | None = None,
    ) -> InternetArchiveUploadResult:
        derive = derive or self.settings.archive_derive
        if self.managed:
            return self._observed(
                "item-create",
                lambda: self._managed(
                    "upload",
                    (metadata, media_path),
                    {"derive": derive},
                    progress,
                    backlog=self._backlog("item-create"),
                ),
                derive,
            )
        return self._observed(
            "item-create",
            lambda: self._upload(metadata, media_path, progress, derive),
            derive,
        )

    def _upload(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        progress: Callable[[int], None],
        derive: DerivePolicy,
    ) -> InternetArchiveUploadResult:
        options = self._options(metadata, derive)
        with (
            media_path.open("rb") as stream,
            self._session() as session,
            self._client(session) as client,
        ):
            queue = self._respect_upload_limit(
                client, metadata.identifier, "item-create"
            )
            file = InternetArchiveUploadFile(
                cast(Any, _ProgressStream(stream, progress, self.stop_controller)),
                name=metadata.media_name,
            )
            return self._with_queue(
                lambda: client.upload([file], options, wait=False), queue
            )

    def _options(
        self, metadata: FrozenArchiveMetadata, derive: DerivePolicy
    ) -> InternetArchiveUploadOptions:
        return InternetArchiveUploadOptions(
            identifier=metadata.identifier,
            title=_upload_text(metadata.title) or metadata.identifier,
            description=_upload_text(metadata.description, multiline=True)
            or metadata.source_url,
            subjects=list(
                dict.fromkeys(
                    value
                    for subject in metadata.subjects
                    if (value := _upload_text(subject))
                )
            )
            or ["youtube"],
            creator=_upload_text(metadata.creator) or metadata.channel_username,
            date=date.fromisoformat(metadata.upload_date),
            collection="opensource_movies",
            media_type="movies",
            metadata={
                "source": metadata.source_url,
                "saga_operation_id": metadata.operation_id,
            },
            queue_derive=derive == "immediate",
            interactive_priority=self.settings.archive_interactive_priority,
        )

    def upload_multipart(  # noqa: PLR0913 - Resume state and its callbacks.
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        create: bool,
        state: str | None,
        progress: Callable[[int], None],
        heartbeat: Callable[[], None],
        on_state: Callable[[str], None],
        derive: DerivePolicy | None = None,
    ) -> InternetArchiveUploadResult:
        """Send one original in resumable parts, saving each state before going on."""
        phase = "item-create" if create and state is None else "file-upload"
        derive = derive or self.settings.archive_derive
        if self.managed:
            return self._observed(
                phase,
                lambda: self._managed(
                    "upload_multipart",
                    (metadata, media_path),
                    {"create": create, "state": state, "derive": derive},
                    progress,
                    backlog=self._backlog(phase),
                    heartbeat=heartbeat,
                    on_state=on_state,
                ),
                derive,
            )
        return self._observed(
            phase,
            lambda: self._upload_multipart(
                metadata,
                media_path,
                create=create,
                state=state,
                progress=progress,
                heartbeat=heartbeat,
                on_state=on_state,
                derive=derive,
            ),
            derive,
        )

    def _upload_multipart(  # noqa: PLR0913 - Resume state and its callbacks.
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        create: bool,
        state: str | None,
        progress: Callable[[int], None],
        heartbeat: Callable[[], None],
        on_state: Callable[[str], None],
        derive: DerivePolicy,
    ) -> InternetArchiveUploadResult:
        resume = (
            InternetArchiveMultipartState.from_dict(json.loads(state))
            if state
            else None
        )
        size = media_path.stat().st_size
        # Larger originals use bigger parts to stay within Archivist's part limit.
        part_size = min(
            max(
                self.settings.archive_multipart_part_size,
                -(-size // MULTIPART_MAX_PARTS),
            ),
            MULTIPART_MAX_PART_SIZE,
        )

        def save(value: InternetArchiveMultipartState) -> None:
            on_state(json.dumps(value.to_dict(), sort_keys=True))

        with (
            media_path.open("rb") as stream,
            self._session() as session,
            self._client(session) as client,
        ):
            queue = self._respect_upload_limit(
                client,
                metadata.identifier,
                "item-create" if create and resume is None else "file-upload",
            )
            file = InternetArchiveUploadFile(
                cast(
                    Any,
                    _MultipartProgressStream(
                        stream, size, progress, heartbeat, self.stop_controller
                    ),
                ),
                name=metadata.media_name,
            )

            def send() -> InternetArchiveMultipartResult:
                if create:
                    return client.upload_multipart(
                        file,
                        self._options(metadata, derive),
                        part_size=part_size,
                        state=resume,
                        on_state=save,
                    )
                return client.add_file_multipart(
                    metadata.identifier,
                    file,
                    expected_metadata={
                        "source": metadata.source_url,
                        "saga_operation_id": metadata.operation_id,
                    },
                    part_size=part_size,
                    queue_derive=derive == "immediate",
                    interactive_priority=self.settings.archive_interactive_priority,
                    state=resume,
                    on_state=save,
                )

            result = self._with_queue(send, queue)
        # The completion reply has no per-file ETag; the saved resume state
        # already holds the whole-file MD5 and SHA-1.
        return InternetArchiveUploadResult(
            result.identifier,
            (
                InternetArchiveUploadFileResult(
                    result.name, result.size, transferred=True
                ),
            ),
        )

    def _respect_upload_limit(
        self, client: InternetArchiveClient, identifier: str, phase: str
    ) -> dict[str, Any] | None:
        """Defer a write while the account's Archive task queue is full."""
        queue = self._check_limit(client, identifier, phase)
        limit = self.settings.archive_task_queue_limit
        if limit <= 0 or phase not in TASK_PHASES:
            return queue
        backlog = self.tasks.read(
            client.task_summary,
            limit=limit,
            base_delay=self.settings.archive_retry_delay,
        )
        if backlog is None:
            return queue
        queue = {
            **(queue or {}),
            "task_queued": backlog.queued,
            "task_running": backlog.running,
            "task_error": backlog.error,
            "task_limit": limit,
        }
        if backlog.outstanding >= limit:
            error = RateLimitError(
                "Internet Archive account task backlog is at its limit",
                service="Internet Archive",
                service_code="SlowDown",
                service_message=DEFERRED_PREFIX
                + f"account task backlog {backlog.outstanding} >= {limit}",
                request_phase=phase,
                retry_after=backlog.retry_seconds,
            )
            error.__dict__["upload_queue"] = queue
            error.__dict__["deferred_backlog"] = True
            raise error
        return queue

    def _backlog(self, phase: str) -> TaskBacklog | None:
        """Read the shared backlog in the managing process for a managed write."""
        limit = self.settings.archive_task_queue_limit
        if limit <= 0 or phase not in TASK_PHASES:
            return None
        return self.tasks.read(
            self.task_summary, limit=limit, base_delay=self.settings.archive_retry_delay
        )

    def _observed[T](
        self, phase: str, operation: Callable[[], T], derive: DerivePolicy
    ) -> T:
        """Count queued tasks and log the backlog seen with each IA verdict."""
        try:
            result = operation()
        except Exception as error:
            cause = failure_cause(error)
            if isinstance(cause, ServiceError):
                if cause.retry_after is None and (hint := self.tasks.throttle_hint()):
                    cause.retry_after = hint
                latest = self.tasks.latest()
                if (
                    latest is not None
                    and isinstance(cause, RateLimitError)
                    and cause.service_code == "SlowDown"
                    and not (cause.service_message or "").startswith(DEFERRED_PREFIX)
                ):
                    logger.warning(
                        "Archive returned SlowDown at {} with {} outstanding "
                        "account tasks",
                        cause.request_phase,
                        latest.outstanding,
                    )
            raise
        self.tasks.note_write(2 if derive == "immediate" else 1)
        if phase == "item-create" and (latest := self.tasks.latest()) is not None:
            logger.info(
                "Archive created an item with {} outstanding account tasks",
                latest.outstanding,
            )
        return result

    def task_summary(self) -> InternetArchiveTaskSummary:
        """Read the account's catalog task counts through Archivist."""
        if self.managed:
            return self._managed("task_summary", (), {})
        with self._session() as session, self._client(session) as client:
            return client.task_summary()

    def queue_derive(self, identifier: str) -> InternetArchiveTaskSubmission:
        """Queue one derive unless the item already has one pending."""
        if self.managed:
            submission = self._managed("queue_derive", (identifier,), {})
        else:
            with self._session() as session, self._client(session) as client:
                submission = client.queue_derive(identifier)
        if not submission.already_pending:
            self.tasks.note_write(1)
        return submission

    def task_status(self, task_id: int) -> InternetArchiveTaskStatus:
        """Read one catalog task's live or finished state."""
        if self.managed:
            return self._managed("task_status", (task_id,), {})
        with self._session() as session, self._client(session) as client:
            return client.task_status(task_id)

    def get_item(self, identifier: str) -> InternetArchiveItem:
        """Read one item's files and outstanding catalog tasks."""
        if self.managed:
            return self._managed("get_item", (identifier,), {})
        with self._session() as session, self._client(session) as client:
            return client.get_item(identifier)

    @staticmethod
    def _check_limit(
        client: InternetArchiveClient, identifier: str, phase: str
    ) -> dict[str, Any] | None:
        """Defer a write while IA-S3 reports the account's task queue as full."""
        try:
            limit = client.upload_limit(identifier)
        except ServiceError as error:
            # Credential steps are shared with the write, so their failures are
            # final for this attempt. The limit read itself is only advisory.
            if error.request_phase in CREDENTIAL_PHASES:
                raise
            logger.warning(
                "Archive upload-limit check failed; sending without it: {}",
                _error_text(error),
            )
            return None
        queue: dict[str, Any] = dict(limit.counters)
        if limit.over_limit is not None:
            queue["over_limit"] = int(limit.over_limit)
        if limit.limit_reason:
            queue["limit_reason"] = limit.limit_reason
        if limit.over_limit:
            error = RateLimitError(
                "Internet Archive S3 reports the account task queue is over its limit",
                service="Internet Archive",
                service_code="SlowDown",
                service_message=DEFERRED_PREFIX + (limit.limit_reason or "over_limit"),
                request_phase=phase,
            )
            error.__dict__["upload_queue"] = queue
            raise error
        return queue

    @staticmethod
    def _with_queue[T](operation: Callable[[], T], queue: dict[str, Any] | None) -> T:
        """Attach the pre-write queue snapshot to a failed write for diagnosis."""
        try:
            return operation()
        except (InternetArchiveUploadError, ServiceError) as error:
            if queue is not None:
                failure_cause(error).__dict__["upload_queue"] = queue
            raise

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
        derive: DerivePolicy | None = None,
    ) -> InternetArchiveUploadResult:
        derive = derive or self.settings.archive_derive
        if self.managed:
            return self._observed(
                "file-upload",
                lambda: self._managed(
                    "recover",
                    (metadata, media_path),
                    {"derive": derive},
                    progress,
                    backlog=self._backlog("file-upload"),
                ),
                derive,
            )
        return self._observed(
            "file-upload",
            lambda: self._recover(metadata, media_path, progress, derive),
            derive,
        )

    def _recover(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        progress: Callable[[int], None],
        derive: DerivePolicy,
    ) -> InternetArchiveUploadResult:
        with (
            media_path.open("rb") as stream,
            self._session() as session,
            self._client(session) as client,
        ):
            queue = self._respect_upload_limit(
                client, metadata.identifier, "file-upload"
            )
            return self._with_queue(
                lambda: client.transfer_missing_file(
                    metadata.identifier,
                    InternetArchiveUploadFile(
                        cast(
                            Any, _ProgressStream(stream, progress, self.stop_controller)
                        ),
                        name=metadata.media_name,
                    ),
                    expected_metadata={
                        "source": metadata.source_url,
                        "saga_operation_id": metadata.operation_id,
                    },
                    expected_size=media_path.stat().st_size,
                    queue_derive=derive == "immediate",
                    interactive_priority=self.settings.archive_interactive_priority,
                ),
                queue,
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
        *,
        backlog: TaskBacklog | None = None,
        **callbacks: Callable[..., Any],
    ) -> Any:
        return run_managed(
            "gateway",
            method,
            {
                "settings": self.settings,
                "limiter_enabled": self.upload_limiter.enabled,
                "chunk_size": self.upload_limiter.chunk_size,
                "task_backlog": backlog,
            },
            args,
            kwargs,
            self.stop_controller,
            {
                "acquire": self.upload_limiter.acquire,
                "refund": self.upload_limiter.refund,
                "progress": progress or (lambda value: None),
                **callbacks,
            },
        )


class ArchiveBackend(Protocol):
    def upload(
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        progress: Callable[[int], None],
        derive: DerivePolicy | None = None,
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
        derive: DerivePolicy | None = None,
    ) -> InternetArchiveUploadResult: ...
    def upload_multipart(  # noqa: PLR0913
        self,
        metadata: FrozenArchiveMetadata,
        media_path: Path,
        *,
        create: bool,
        state: str | None,
        progress: Callable[[int], None],
        heartbeat: Callable[[], None],
        on_state: Callable[[str], None],
        derive: DerivePolicy | None = None,
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
                tasks=TaskSummaryCache(
                    settings.archive_task_summary_ttl,
                    listener=state.record_archive_tasks,
                ),
            )
        self.derives = (
            DeriveDispatcher(settings, state, self.gateway, self.stop_controller)
            if isinstance(self.gateway, ArchiveGateway)
            else None
        )
        self.download_root = ensure_download_root(settings.download_root)

    def process(self, work: ArchiveWork) -> None:
        try:
            self._process(work)
        finally:
            self.state.finish_archive_request(
                work, base_delay=self.settings.archive_retry_delay
            )

    def _failure_delay(self, work: ArchiveWork, error: Exception) -> float:
        return self._failure(work, error)["delay"]

    def _failure(self, work: ArchiveWork, error: Exception) -> dict[str, Any]:
        """Return the retry delay and diagnostics to save for one failed request."""
        facts = failure_facts(error)
        if work.archive_generation is not None and (
            retryable_error(error)
            or work.archive_probe_token is not None
            and facts["error_kind"] == "permission"
        ):
            verdict = self.state.record_archive_failure(
                work,
                base_delay=self.settings.archive_retry_delay,
                hint=retry_hint(error, utcnow()),
                error=_error_text(error),
                **facts,
            )
            if verdict.item_throttled:
                facts["error_kind"] = ITEM_THROTTLED
            return {"delay": verdict.delay, **facts}
        return {
            "delay": max(
                retry_hint(error, utcnow()),
                retry_delay(
                    self.settings.archive_retry_delay,
                    work.job.attempts if work.job else work.progress.retry_attempts + 1,
                ),
            ),
            **facts,
        }

    def _process(self, work: ArchiveWork) -> None:
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
        except VideoAccessError as error:
            self.state.update_archive(
                work.video.id,
                release=True,
                failure_phase="youtube_access",
                youtube_access_generation=error.cookie_generation,
                last_error=_error_text(error),
                retry_after=retry_delay(
                    self.settings.archive_retry_delay, work.progress.retry_attempts + 1
                ),
                retry_attempts=work.progress.retry_attempts + 1,
            )
        except VideoUnavailableError as error:
            self.state.update_archive(
                work.video.id,
                status="unavailable",
                release=True,
                failure_phase=stage,
                last_error=_error_text(error),
            )
        except AuthenticationError as error:
            if work.job:
                self._defer_job(work, _error_text(error), error=error)
            else:
                self.state.update_archive(
                    work.video.id,
                    release=True,
                    last_error=_error_text(error),
                    failure_phase="archive_access",
                    **failure_facts(error),
                    retry_after=self._failure_delay(work, error),
                    retry_attempts=work.progress.retry_attempts + 1,
                )
        except (
            LocalMediaChangedError,
            RecoveryConflictError,
            InternetArchiveRecoveryConflictError,
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
                    **failure_facts(error),
                )
        except Exception as error:
            delay = self._failure_delay(work, error)
            log = (
                logger.debug
                if work.archive_generation is not None and retryable_error(error)
                else logger.warning
            )
            log(
                "Video {} phase {} attempt {} will retry in {:.0f}s: {}",
                work.video.id,
                stage,
                work.job.attempts if work.job else work.progress.attempts,
                delay,
                _error_text(error),
            )
            if work.job:
                self.state.update_job(
                    work,
                    status="waiting",
                    retry_after=delay,
                    error=_error_text(error),
                    **failure_facts(error),
                )
            elif stage == "upload" and work.progress.status == "uploading":
                self.state.defer_transfer(
                    work,
                    status="reconcile_upload",
                    error=_error_text(error),
                    delay=delay,
                    deadline=self.settings.background_deadline,
                    **failure_facts(error),
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
                    **failure_facts(error),
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
            self.state,
            work,
            directory,
            self.settings.minimum_free_bytes,
            data_path=self.settings.data_path,
            max_data_bytes=self.settings.max_data_bytes,
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
                # A new download cannot resume parts sent from the lost bytes.
                multipart_state=None,
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
                multipart_state=None,
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
        # Every write to one item must agree on derive, so the first write's
        # option is stored and reused by resumes and recovery transfers.
        derive = work.progress.derive_policy or self.settings.archive_derive
        self.state.update_archive(
            work.video.id,
            status="uploading",
            derive_policy=derive,
            upload_started_at=utcnow(),
            transfer_bytes=0,
            transfer_progress_at=utcnow(),
            failure_phase=None,
            last_error=None,
            error_kind=None,
            error_status=None,
            error_phase=None,
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

        def heartbeat() -> None:
            # Hashing before the first part is work, not a stalled transfer.
            self.state.update_archive(work.video.id, transfer_progress_at=utcnow())

        def save_state(state: str) -> None:
            facts = json.loads(state)
            self.state.update_archive(
                work.video.id,
                multipart_state=state,
                media_md5=facts["md5"],
                media_sha1=facts["sha1"],
            )
            work.progress.multipart_state = state

        try:
            if (
                work.progress.multipart_state is not None
                or 0 < self.settings.archive_multipart_threshold <= path.stat().st_size
            ):
                result = self._gateway().upload_multipart(
                    frozen,
                    path,
                    create=work.progress.upload_mode != "recover",
                    state=work.progress.multipart_state,
                    progress=progress,
                    heartbeat=heartbeat,
                    on_state=save_state,
                    derive=cast(DerivePolicy, derive),
                )
            else:
                operation = (
                    self._gateway().recover
                    if work.progress.upload_mode == "recover"
                    else self._gateway().upload
                )
                result = operation(
                    frozen, path, progress=progress, derive=cast(DerivePolicy, derive)
                )
        except InternetArchiveUploadError as error:
            if self._acknowledged(work, error.result):
                self._acknowledge(work, error.result)
                return
            self.stop_controller.check()
            known_failure = _known_failure(error.cause)
            if known_failure and error.failed_file:
                self.state.update_archive(work.video.id, upload_mode="recover")
            self.state.defer_transfer(
                work,
                status="upload" if known_failure else "reconcile_upload",
                error=_error_text(error),
                deadline=self.settings.background_deadline,
                **self._failure(work, error),
            )
            return
        except InternetArchiveMultipartError as error:
            self.stop_controller.check()
            phase = error.cause.request_phase
            gone = error.uncertain or error.cause.service_code == "NoSuchUpload"
            if gone:
                # The upload ID cannot be resumed, and its completion is unknown.
                # Only the item's own listing decides whether to send it again.
                self.state.update_archive(work.video.id, multipart_state=None)
            # A live upload ID resumes from its saved parts. Before one exists,
            # only a rejected item creation is known to have changed nothing.
            retry = (
                not gone
                and error.state is not None
                or error.state is None
                and phase == "item-create"
                and _known_failure(error.cause)
            )
            self.state.defer_transfer(
                work,
                status="upload" if retry else "reconcile_upload",
                error=_error_text(error),
                deadline=self.settings.background_deadline,
                **self._failure(work, error),
            )
            return
        except InvalidOptionError as error:
            if str(error) != "Internet Archive item identifier is unavailable":
                raise
            # A create-only retry can lose a race with delayed item creation.
            # Inspect provenance again instead of abandoning its local original.
            self.state.defer_transfer(
                work,
                status="reconcile_upload",
                error=_error_text(error),
                delay=self._failure_delay(work, error),
                deadline=self.settings.background_deadline,
            )
            return
        except InternetArchiveRecoveryDeferredError as error:
            # Nothing was written; reconciliation reads the item again later.
            self.state.defer_transfer(
                work,
                status="reconcile_upload",
                error=_error_text(error),
                delay=self._failure_delay(work, error),
                deadline=self.settings.background_deadline,
                **failure_facts(error),
            )
            return
        except InternetArchiveRecoveryConflictError:
            # Recovery errors are service errors; keep them out of retry handling.
            raise
        except ServiceError as error:
            self.stop_controller.check()
            # Both upload APIs wrap service failures once a mutation starts.
            # Retry transient preflight failures without waiting for a remote item.
            if not (
                isinstance(error, (NetworkError, RateLimitError, AuthenticationError))
                or getattr(error, "request_phase", None) in {"csrf", "login"}
                or error.status_code is not None
                and (error.status_code in {408, 425, 429} or error.status_code >= 500)
            ):
                raise
            self.state.defer_transfer(
                work,
                status="upload",
                error=_error_text(error),
                deadline=self.settings.background_deadline,
                **self._failure(work, error),
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
        elif inspection.state in {"missing", "absent"}:
            self.state.update_archive(
                work.video.id,
                status="upload",
                release=True,
                upload_mode="create" if inspection.state == "absent" else "recover",
                ready_at=utcnow(),
                failure_phase=None,
                last_error=None,
                error_kind=None,
                error_status=None,
                error_phase=None,
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

    def _defer_job(
        self, work: ArchiveWork, detail: str | None, *, error: Exception | None = None
    ) -> None:
        assert work.job is not None
        self.state.update_job(
            work,
            status="waiting",
            error=detail,
            retry_after=self._failure_delay(work, error)
            if error
            else retry_delay(self.settings.archive_retry_delay, work.job.attempts),
            **(failure_facts(error) if error else {}),
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
        except SubmissionNotSentError as error:
            self.state.update_job(
                work,
                status="waiting",
                webpage_status="pending",
                error=_error_text(error),
                **failure_facts(error),
                retry_after=retry_delay(
                    self.settings.archive_retry_delay, work.job.attempts
                ),
            )
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
        if (
            not item.files
            and not item.tasks
            and not item.server_unavailable
            and not item.has_redrow
            and not item.is_dark
            and not item.nodownload
            and item.extended_error_code is None
            and item.unavailable_reason is None
            and item.availability is InternetArchiveItemAvailability.UNCERTAIN
        ):
            # An empty response is not permission to overwrite an item. Retrying
            # the client's create-only upload performs availability and S3 guards.
            return Inspection("absent", "empty item response; retry guarded creation")
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
        response = urlopen(request, timeout=timeout)
    except HTTPError as error:
        if 300 <= error.code < 400:
            raise SubmissionUncertainError(
                "Ziggy redirected the submission; acceptance is unknown"
            ) from error
        raise SubmissionRejectedError(f"Ziggy rejected the request: {error}") from error
    except (HTTPException, OSError) as error:
        if isinstance(error, OSError) and known_connect_failure(error):
            raise SubmissionNotSentError(
                "Ziggy connection failed before submission",
                service="Ziggy",
                request_phase="webpage-connect",
                cause_type=type(error).__name__,
            ) from error
        raise SubmissionUncertainError(f"Ziggy response was lost: {error}") from error
    try:
        with response:
            if response.status != 202:
                raise SubmissionRejectedError(
                    f"Ziggy returned HTTP {response.status}, expected 202"
                )
            payload = response.read()
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


def _known_failure(error: ServiceError) -> bool:
    """Return whether a rejection proves the request changed nothing remotely."""
    code = error.status_code
    return (
        # IA-S3 throttles item creation before it creates the item.
        isinstance(error, RateLimitError)
        and error.service_code == "SlowDown"
        and error.request_phase == "item-create"
    ) or (
        not isinstance(error, NetworkError)
        and code is not None
        and 400 <= code < 500
        and code not in {408, 409, 412}
    )


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
    detail = f"{type(error).__name__}: {error}"
    cause = failure_cause(error)
    if cause is not error:
        detail += f"; {type(cause).__name__}: {cause}"
    if isinstance(cause, ServiceError):
        context: list[str] = []
        if cause.status_code is not None:
            context.append(f"HTTP {cause.status_code}")
        phase = getattr(cause, "request_phase", None)
        if phase:
            context.append(f"phase={phase}")
        facts = failure_facts(error)
        context.append(f"reason={facts['error_code'] or 'unknown'}")
        if facts["error_message"]:
            context.append(f"message={facts['error_message']}")
        if facts["error_request_id"]:
            context.append(f"request_id={facts['error_request_id']}")
        detail += " (" + "; ".join(context) + ")"
    return redact_diagnostics(detail)[:2000]


def _upload_text(value: str, *, multiline: bool = False) -> str:
    """Remove rejected control/surrogate characters from YouTube display text."""
    return "".join(
        character
        for character in value
        if unicodedata.category(character) not in {"Cc", "Cs"}
        or multiline
        and character in "\r\n\t"
    ).strip()
