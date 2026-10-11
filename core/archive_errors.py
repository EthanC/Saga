"""Structured Archive failures shared by workers, durable queues, and alerts."""

from typing import Any

from archivist import (
    AuthenticationError,
    InternetArchiveUploadError,
    NetworkError,
    RateLimitError,
    ServiceError,
)


def failure_cause(error: Exception) -> Exception:
    return error.cause if isinstance(error, InternetArchiveUploadError) else error


def failure_facts(error: Exception) -> dict[str, Any]:
    cause = failure_cause(error)
    phase = getattr(cause, "request_phase", None)
    status = cause.status_code if isinstance(cause, ServiceError) else None
    kind = (
        "network"
        if isinstance(cause, NetworkError)
        else "rate_limit"
        if isinstance(cause, RateLimitError)
        else "permission"
        if isinstance(cause, AuthenticationError)
        and status == 403
        and phase in {"item-create", "file-upload", "item-metadata"}
        else "authentication"
        if isinstance(cause, AuthenticationError)
        else "service"
        if isinstance(cause, ServiceError)
        else None
    )
    return {"error_kind": kind, "error_status": status, "error_phase": phase}


def legacy_bad_login(detail: str | None) -> bool:
    return bool(
        detail
        and detail.startswith("AuthenticationError:")
        and "Bad Request" in detail
        and "Skip to main content" in detail
    )


def is_authentication_failure(kind: str | None, detail: str | None) -> bool:
    if kind is not None:
        return kind == "authentication"
    return "AuthenticationError:" in (detail or "") and not legacy_bad_login(detail)
