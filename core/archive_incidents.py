"""Progress evidence for Archive notifications, separate from transport admission."""

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from core.archive_errors import incident_category
from core.archive_retry import ArchiveServiceState


def incident_open(gate: ArchiveServiceState) -> bool:
    return gate.incident_started_at is not None and gate.incident_closed_at is None


def incident_facts(
    gate: ArchiveServiceState, rows: Sequence[Any], now: datetime, stall_timeout: float
) -> dict[str, Any]:
    now = aware(now)
    start = aware(gate.incident_started_at or gate.first_failure_at or now)
    uploaded = [
        row
        for row in rows
        if row.media_uploaded_at is not None
        and start <= aware(row.media_uploaded_at) <= now
    ]
    active = [
        row
        for row in rows
        if row.status == "uploading"
        and row.claimed_at
        and row.transfer_bytes > 0
        and row.transfer_progress_at is not None
        and start <= aware(row.transfer_progress_at) <= now
    ]
    progress = max(
        [aware(row.media_uploaded_at) for row in uploaded]
        + [aware(row.transfer_progress_at) for row in active],
        default=None,
    )
    idle = max(0.0, (now - (progress or start)).total_seconds())
    category = incident_category(gate.error_kind, gate.error_code)
    actionable = gate.failures > 0 and category in {"authentication", "item-specific"}
    # Saga's own backoff can outlast the stall timeout after one failure. Time
    # alone escalates once a retry has failed or has itself stopped progressing.
    probe_idle = (
        max(
            0.0,
            (
                now - max(aware(gate.probe_claimed_at), progress or start)
            ).total_seconds(),
        )
        if gate.probe_claimed_at
        else 0.0
    )
    retried = gate.failures > 1 or probe_idle > stall_timeout
    error = gate.failures > 0 and (idle > stall_timeout and retried or actionable)
    return {
        "incident_age_seconds": max(0.0, (now - start).total_seconds()),
        "incident_category": category,
        "incident_progress_at": progress.isoformat() if progress else None,
        "incident_no_progress_seconds": idle,
        "acknowledged_uploads": len(uploaded),
        "acknowledged_bytes": sum(row.uploaded_bytes or 0 for row in uploaded),
        "active_upload_bytes": sum(row.transfer_bytes for row in active),
        "incident_notification_state": ("observing" if incident_open(gate) else "clear")
        if gate.failures == 0
        else "escalated"
        if error
        else "progressing"
        if progress
        else "waiting",
        "retry_delay_source": "server"
        if json.loads(gate.error_headers or "{}").get("retry-after")
        else "local",
        "severity": "error" if error else "warning",
    }


def aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
