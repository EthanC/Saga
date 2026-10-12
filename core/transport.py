"""Transport-only recovery extension for the pinned Archivist 1.3.0 client."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from datetime import datetime
from typing import Any, cast
from urllib.parse import quote, urlsplit

from archivist import (
    InternetArchiveClient,
    InternetArchiveItemAvailability,
    InternetArchiveUploadError,
    InternetArchiveUploadFile,
    InternetArchiveUploadFileResult,
    InternetArchiveUploadResult,
    InvalidServiceResponseError,
    ServiceError,
)
from archivist.core._http import (
    ResponseLike,
    parse_retry_after,
    raise_for_common_status,
    response_mapping,
    response_text,
)
from archivist.services.internet_archive import _common, _items, _recovery

from core.archive_errors import failure_cause


class RecoveryDeferredError(RuntimeError):
    """Remote evidence is insufficient for a guarded file transfer."""


class RecoveryConflictError(RuntimeError):
    """The persisted Saga operation does not own this remote file slot."""


class TransportArchiveClient(InternetArchiveClient):
    """Add an acknowledged-file API without checksum or derivative readiness gates.

    Private transport helpers are version-pinned and covered by HTTP-level tests.
    This extension can move upstream without changing Saga's job contract.
    """

    def __enter__(self) -> TransportArchiveClient:
        super().__enter__()
        return self

    _request_phase: str | None = None
    _request_status: int | None = None
    _request_retry_after: float | datetime | None = None

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if isinstance(exc, ServiceError):
            self._annotate_failure(exc)
        super().__exit__(exc_type, exc, traceback)

    def _annotate_failure(self, error: ServiceError) -> None:
        cause = cast(ServiceError, failure_cause(error))
        cast(Any, cause).request_phase = (
            self._request_phase
            if getattr(cause, "request_phase", None) in {None, "request"}
            else cause.request_phase
        )
        if cause.status_code is None:
            cause.status_code = self._request_status
        if getattr(cause, "retry_after", None) is None:
            cast(Any, cause).retry_after = self._request_retry_after

    def _request(self, method: str, url: str, **kwargs: Any) -> Any:
        self._request_phase = request_phase(method, url)
        self._request_status = None
        self._request_retry_after = None
        try:
            response = super()._request(method, url, **kwargs)
            self._request_status = response.status_code
            self._request_retry_after = parse_retry_after(
                getattr(response, "headers", {})
            )[0]
            if url in {_common.CSRF_URL, _common.LOGIN_URL}:
                self._validate_login_response(response, url)
            return response
        except ServiceError as error:
            self._annotate_failure(error)
            raise

    def _recovery_request(self, method: str, url: str, key: Any, **kwargs: Any) -> Any:
        self._request_phase = request_phase(method, url)
        self._request_status = None
        self._request_retry_after = None
        try:
            response = super()._recovery_request(method, url, key, **kwargs)
            self._request_status = response.status_code
            self._request_retry_after = parse_retry_after(
                getattr(response, "headers", {})
            )[0]
            return response
        except ServiceError as error:
            self._annotate_failure(error)
            raise

    def login(self) -> None:
        try:
            super().login()
        except ServiceError as error:
            self._annotate_failure(error)
            raise

    def _validate_login_response(self, response: Any, url: str) -> None:
        # Archivist 1.3.0 labels every login HTTP 400 as bad credentials. Require
        # the login API's explicit rejection, rather than a proxy/server HTML page.
        if url != _common.LOGIN_URL or response.status_code != 400:
            raise_for_common_status(
                response,
                service=_common.SERVICE,
                authentication_statuses=(
                    frozenset() if url == _common.CSRF_URL else frozenset({401, 403})
                ),
            )
        try:
            data = response_mapping(response, service=_common.SERVICE)
        except InvalidServiceResponseError:
            raise InvalidServiceResponseError(
                "Internet Archive auth endpoint returned an unexpected response",
                service=_common.SERVICE,
                status_code=response.status_code,
            ) from None
        if url == _common.LOGIN_URL:
            valid = (
                data.get("success") is False
                if response.status_code == 400
                else isinstance(data.get("success"), bool)
            )
        else:
            value = data.get("value")
            token = value.get("token") if isinstance(value, Mapping) else None
            valid = (
                data.get("success") is True and isinstance(token, str) and bool(token)
            )
        if not valid:
            raise InvalidServiceResponseError(
                "Internet Archive auth endpoint returned an unexpected response",
                service=_common.SERVICE,
                status_code=response.status_code,
            )

    def add_file_transport(
        self,
        identifier: str,
        file: InternetArchiveUploadFile,
        *,
        expected_metadata: Mapping[str, str],
        expected_size: int,
    ) -> InternetArchiveUploadResult:
        """Attempt one missing original PUT after fresh ownership/provenance checks."""
        if not expected_metadata.get("source") or not expected_metadata.get(
            "saga_operation_id"
        ):
            raise RecoveryConflictError(
                "recovery requires a persisted source and operation ID"
            )
        self._ensure_item_transport()
        with ExitStack() as stack:
            prepared = _items.prepare_files([file], identifier, stack)[0]
            if prepared.size != expected_size or expected_size <= 0:
                raise RecoveryConflictError("staging size changed before recovery")
            key = self._recovery_key()
            identity_response = self._recovery_request(
                "GET",
                _common.USER_INFO_URL,
                key,
                request_timeout=self._timeout,
                params={"op": "whoami"},
            )
            _items.raise_for_item_status(cast(ResponseLike, identity_response))
            identity = _recovery.parse_identity(
                response_mapping(
                    cast(ResponseLike, identity_response), service=_common.SERVICE
                )
            )
            metadata_response = self._recovery_request(
                "GET",
                f"{_recovery.METADATA_URL}/{identifier}",
                key,
                request_timeout=self._timeout,
                params={"extended_err": "1"},
            )
            _items.raise_for_item_status(cast(ResponseLike, metadata_response))
            item = _recovery.parse_item(
                response_mapping(
                    cast(ResponseLike, metadata_response), service=_common.SERVICE
                ),
                identifier,
                status_code=cast(int, metadata_response.status_code),
            )
            if not item.metadata or item.uploader is None:
                raise RecoveryDeferredError("item ownership metadata is unresolved")
            if item.uploader != identity or any(
                item.metadata.get(name) not in (value, (value,))
                for name, value in expected_metadata.items()
            ):
                raise RecoveryConflictError(
                    "item ownership or Saga provenance does not match"
                )
            if item.has_redrow or item.is_dark or item.nodownload:
                raise RecoveryConflictError("item does not permit recovery")
            if any(remote.name == prepared.name for remote in item.files):
                raise RecoveryDeferredError(
                    "file slot is occupied; reconcile its saved facts"
                )
            if (
                not item.listing_complete
                or item.server_unavailable
                or item.availability is not InternetArchiveItemAvailability.AVAILABLE
            ):
                raise RecoveryDeferredError(
                    "missing-file evidence is not authoritative"
                )
            outcome = InternetArchiveUploadFileResult(prepared.name, prepared.size)
            try:
                response = self._recovery_request(
                    "PUT",
                    f"{_items.S3_URL}/{identifier}/{quote(prepared.name, safe='/')}",
                    key,
                    request_timeout=self._timeout,
                    mutation=True,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(prepared.size),
                        "If-None-Match": "*",
                    },
                    data=prepared,
                )
                _items.raise_for_item_status(cast(ResponseLike, response))
                if response.status_code != 200 or response_text(
                    cast(ResponseLike, response), service=_common.SERVICE
                ):
                    raise InvalidServiceResponseError(
                        "file transfer was not acknowledged",
                        status_code=response.status_code,
                    )
            except ServiceError as error:
                raise InternetArchiveUploadError(
                    InternetArchiveUploadResult(identifier, (outcome,)),
                    error,
                    failed_file=prepared.name,
                ) from error
            return InternetArchiveUploadResult(
                identifier,
                (
                    InternetArchiveUploadFileResult(
                        prepared.name,
                        prepared.size,
                        transferred=True,
                        etag=response.headers.get("ETag"),
                    ),
                ),
            )


def request_phase(method: str, url: str) -> str:
    if url == _common.CSRF_URL:
        return "csrf"
    if url == _common.LOGIN_URL:
        return "login"
    if url == _items.UPLOAD_URL:
        return "upload-key"
    if url == _items.UPLOAD_API_URL:
        return "identifier-check"
    if url.startswith(_recovery.METADATA_URL + "/"):
        return "item-metadata"
    if url == _common.USER_INFO_URL:
        return "identity"
    if method == "PUT" and url.startswith(_items.S3_URL + "/"):
        return (
            "item-create"
            if len(urlsplit(url).path.strip("/").split("/")) == 1
            else "file-upload"
        )
    return "request"
