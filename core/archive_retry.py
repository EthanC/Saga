"""Durable shared Archive cooldown and retryable service-failure predicates."""

import re
from datetime import UTC, datetime
from typing import Any

from archivist import parse_retry_after
from sqlmodel import Field, SQLModel

from core.archive_errors import failure_cause, failure_facts

MAX_PROBE_DELAY = 900.0
INCIDENT_REMINDER_DELAY = 3600.0
THROTTLE_PROBATION = 1800.0
MIN_WRITE_INTERVAL = 60.0
RELEASE_WRITE_INTERVAL = 15.0
INCIDENT_RECOVERY_DELAY = 1800.0


class ArchiveServiceState(SQLModel, table=True):
    id: int = Field(default=1, primary_key=True)
    generation: int = 0
    failures: int = 0
    retry_at: datetime | None = None
    first_failure_at: datetime | None = None
    last_success_at: datetime | None = None
    last_probe_at: datetime | None = None
    probe_token: str | None = None
    probe_video_id: str | None = None
    probe_kind: str | None = None
    probe_claimed_at: datetime | None = None
    error_kind: str | None = None
    error_status: int | None = None
    error_phase: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_request_id: str | None = None
    error_headers: str | None = None
    error_queue: str | None = None
    last_error: str | None = None
    incident_id: str | None = None
    incident_notified_category: str | None = None
    incident_escalated_at: datetime | None = None
    incident_last_notified_at: datetime | None = None
    recovery_event: str | None = None
    recovery_notified_at: datetime | None = None
    probation_until: datetime | None = None
    write_interval_seconds: float = 0.0
    incident_started_at: datetime | None = None
    incident_healthy_since: datetime | None = None
    incident_closed_at: datetime | None = None


def retryable_fields(
    kind: str | None, status: int | None, phase: str | None, detail: str
) -> bool:
    if kind in {"network", "authentication", "rate_limit"}:
        return True
    if kind == "service":
        return (
            phase in {"csrf", "login"}
            or status is not None
            and (status >= 500 or status in {408, 425, 429})
        )
    if kind is not None:
        return False
    return any(
        name in detail
        for name in ("NetworkError:", "RateLimitError:", "AuthenticationError:")
    ) or bool(re.search(r"\bHTTP\s+(?:5\d\d|408|425|429)\b", detail))


def retryable_error(error: Exception) -> bool:
    facts = failure_facts(error)
    return retryable_fields(
        facts["error_kind"], facts["error_status"], facts["error_phase"], ""
    )


def retryable_record(record: Any) -> bool:
    return bool(record.last_error) and retryable_fields(
        record.error_kind, record.error_status, record.error_phase, record.last_error
    )


def retry_hint(error: Exception, now: datetime) -> float:
    value = getattr(failure_cause(error), "retry_after", None)
    if value is None:
        value = parse_retry_after(
            {
                "retry-after": getattr(failure_cause(error), "retry_after_raw", None)
                or ""
            }
        )[0]
    if isinstance(value, datetime):
        return max(
            0.0,
            (
                value.replace(tzinfo=UTC)
                if value.tzinfo is None
                else value.astimezone(UTC)
            ).timestamp()
            - now.timestamp(),
        )
    return max(0.0, float(value or 0))
