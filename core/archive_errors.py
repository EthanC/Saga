"""Structured Archive failures shared by workers, durable queues, and alerts."""

import json
import unicodedata
from typing import Any

from archivist import (
    AuthenticationError,
    InternetArchiveMultipartError,
    InternetArchiveRecoveryError,
    InternetArchiveUploadError,
    NetworkError,
    RateLimitError,
    ServiceError,
)

from core.logging import redact_diagnostics


class RecoveryConflictError(RuntimeError):
    """A persisted Saga operation contradicts its local staging or stage."""


DIAGNOSTIC_FIELDS = (
    "error_code",
    "error_message",
    "error_request_id",
    "error_headers",
    "error_queue",
)
# IA-S3 documents `detail` as internal and changeable, so only known counters stay.
QUEUE_COUNTERS = (
    "accesskey_ration",
    "accesskey_tasks_queued",
    "bucket_limit",
    "bucket_ration",
    "bucket_tasks_queued",
    "rationing_engaged",
    "rationing_level",
    "total_global_limit",
    "total_tasks_queued",
)
# Account catalog task counts that Saga reads before a write, with its limit.
TASK_COUNTERS = ("task_queued", "task_running", "task_error", "task_limit")
THROTTLING_CODES = {"SlowDown", "Throttling", "ThrottlingException", "TooManyRequests"}
AUTHENTICATION_CODES = {
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "InvalidToken",
    "ExpiredToken",
}
PERMISSION_CODES = {"AccessDenied", "AllAccessDisabled", "UnauthorizedAccess"}
TRANSIENT_CODES = {"InternalError", "ServiceUnavailable", "RequestTimeout"}
ITEM_PHASES = {
    "item-create",
    "file-upload",
    "item-metadata",
    "multipart-initiate",
    "multipart-part",
    "multipart-list",
    "multipart-complete",
}


def safe_text(value: str | None, limit: int = 256) -> str | None:
    if value is None:
        return None
    return "".join(
        char
        for char in redact_diagnostics(value)
        if unicodedata.category(char) not in {"Cc", "Cs"}
    )[:limit]


def diagnostic_fields(
    error_code: str | None = None,
    error_message: str | None = None,
    error_request_id: str | None = None,
    error_headers: str | None = None,
    error_queue: str | None = None,
) -> dict[str, Any]:
    headers = json.loads(error_headers) if error_headers else None
    bounded = {
        key.lower(): safe_text(value)
        for key, value in (headers or {}).items()
        if key.lower() in {"content-type", "retry-after", "x-amz-request-id"}
    }
    return {
        "error_code": safe_text(error_code),
        "error_message": safe_text(error_message, 1024),
        "error_request_id": safe_text(error_request_id),
        "error_headers": json.dumps(bounded, sort_keys=True)
        if headers is not None
        else None,
        "error_queue": queue_snapshot(json.loads(error_queue)) if error_queue else None,
    }


def queue_snapshot(data: Any) -> str | None:
    """Keep IA-S3 task-queue counters and its limit reason, bounded for storage."""
    if not isinstance(data, dict):
        return None
    snapshot: dict[str, Any] = {
        name: data[name]
        for name in ("over_limit", *QUEUE_COUNTERS, *TASK_COUNTERS)
        if isinstance(data.get(name), int)
    }
    if isinstance(data.get("limit_reason"), str) and data["limit_reason"]:
        snapshot["limit_reason"] = safe_text(data["limit_reason"])
    return json.dumps(snapshot, sort_keys=True) if snapshot else None


def apply_diagnostics(record: Any, **fields: Any) -> None:
    for name, value in diagnostic_fields(**fields).items():
        setattr(record, name, value)


def incident_category(kind: str | None, code: str | None) -> str:
    if kind in {"network", "rate_limit", "authentication", "permission", "deferred"}:
        return {
            "deferred": "deferred-backlog",
            "network": "transient",
            "rate_limit": "throttled",
            "authentication": "authentication",
            "permission": "item-specific",
        }[kind]
    return (
        "unclassified"
        if code and code not in TRANSIENT_CODES or kind != "service"
        else "transient"
    )


def diagnostic_snapshot(record: Any) -> dict[str, Any]:
    fields = diagnostic_fields(
        **{name: getattr(record, name) for name in DIAGNOSTIC_FIELDS}
    )
    for name in ("error_headers", "error_queue"):
        fields[name] = json.loads(fields[name]) if fields[name] is not None else None
    fields["incident_category"] = incident_category(
        record.error_kind, fields["error_code"]
    )
    return fields


def failure_cause(error: Exception) -> Exception:
    while isinstance(
        error,
        (
            InternetArchiveUploadError,
            InternetArchiveRecoveryError,
            InternetArchiveMultipartError,
        ),
    ):
        error = error.cause
    return error


def refusal_reason(error: Exception) -> str | None:
    """Return the stable reason Archivist gave for refusing a guarded transfer."""
    if isinstance(error, InternetArchiveRecoveryError) and error.reason is not None:
        return error.reason.value
    return None


def failure_facts(error: Exception) -> dict[str, Any]:
    if (reason := refusal_reason(error)) is not None:
        # A refusal is decided from fresh item evidence before any write, so it
        # is not a service failure; its reason enum is the stable error code.
        assert isinstance(error, InternetArchiveRecoveryError)
        return {
            "error_kind": None,
            "error_status": None,
            "error_phase": None,
            **diagnostic_fields(reason, str(error.cause) or None),
        }
    cause = failure_cause(error)
    phase = getattr(cause, "request_phase", None)
    status = cause.status_code if isinstance(cause, ServiceError) else None
    code = getattr(cause, "service_code", None)
    kind = (
        # Saga deferred the write itself; the Archive never saw a request.
        "deferred"
        if getattr(cause, "deferred_backlog", False)
        else "network"
        if isinstance(cause, NetworkError)
        else "rate_limit"
        if isinstance(cause, RateLimitError)
        or code in THROTTLING_CODES
        or status == 429
        else "permission"
        if code in PERMISSION_CODES
        and phase in ITEM_PHASES
        or isinstance(cause, AuthenticationError)
        and status == 403
        and phase in ITEM_PHASES
        and code not in AUTHENTICATION_CODES
        else "authentication"
        if isinstance(cause, AuthenticationError) or code in AUTHENTICATION_CODES
        else "service"
        if isinstance(cause, ServiceError)
        else None
    )
    message = getattr(cause, "service_message", None)
    if isinstance(cause, NetworkError):
        # Transport failures carry no response, so the exception type is the
        # only safe clue separating timeouts, resets, and TLS failures.
        code = code or getattr(cause, "cause_type", None)
        message = message or str(cause) or None
    headers = getattr(cause, "diagnostic_headers", None)
    raw = getattr(cause, "retry_after_raw", None)
    if raw is not None:
        headers = {**(headers or {}), "retry-after": raw}
    return {
        "error_kind": kind,
        "error_status": status,
        "error_phase": phase,
        **diagnostic_fields(
            code,
            message,
            getattr(cause, "request_id", None),
            json.dumps(headers) if headers is not None else None,
            json.dumps(queue)
            if (queue := getattr(cause, "upload_queue", None))
            else None,
        ),
    }


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
