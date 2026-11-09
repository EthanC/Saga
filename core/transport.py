"""Transport-only recovery extension for the pinned Archivist 1.5.0 client."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from typing import cast
from urllib.parse import quote

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
from archivist.core._http import ResponseLike, response_mapping, response_text
from archivist.services.internet_archive import _common, _diagnostics, _items, _recovery


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

    @_diagnostics.diagnostic_operation
    def add_file_transport(
        self,
        identifier: str,
        file: InternetArchiveUploadFile,
        *,
        expected_metadata: Mapping[str, str],
        expected_size: int,
        queue_derive: bool = True,
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
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(prepared.size),
                "If-None-Match": "*",
                # Match the original create and upload. IA warns that
                # mixed priorities can reorder an item's writes.
                "x-archive-interactive-priority": "1",
            }
            if not queue_derive:
                # Match an original upload that skipped derive processing.
                headers["x-archive-queue-derive"] = "0"
            try:
                response = self._recovery_request(
                    "PUT",
                    f"{_items.S3_URL}/{identifier}/{quote(prepared.name, safe='/')}",
                    key,
                    request_timeout=self._timeout,
                    mutation=True,
                    headers=headers,
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
