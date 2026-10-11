"""Lane health, expected backpressure, and independent preservation-age alerts."""

from datetime import datetime, timedelta
from typing import Any

from core.state import ArchiveJob, ArchiveProgress, Video, _as_utc

STARTUP_GRACE = 120
RECENT_METADATA_WARNING = 300
RECENT_DOWNLOAD_WARNING = 900


def build_pipeline_health(
    rows: list[tuple[ArchiveProgress, Video]],
    jobs: list[ArchiveJob],
    *,
    now: datetime,
    started_at: datetime,
    stall_timeout: float,
    capacities: dict[str, int],
    eligible_since: dict[str, datetime],
    metadata_reasons: dict[str, str | None],
    download_reason: str | None,
    lane_progress: dict[str, datetime],
) -> dict[str, Any]:
    current = _as_utc(now)
    grace_end = _as_utc(started_at) + timedelta(seconds=STARTUP_GRACE)
    alerts: list[dict[str, Any]] = []
    lanes: dict[str, Any] = {}
    recent: dict[str, Any] = {}
    transfer_progress = max(
        (
            _as_utc(stamp)
            for row, _ in rows
            for amount, stamp in (
                (row.download_bytes + row.merge_bytes, row.download_progress_at),
                (row.transfer_bytes, row.transfer_progress_at),
                (1, row.media_uploaded_at),
            )
            if amount > 0 and stamp is not None
        ),
        default=grace_end,
    )
    for lane in ("metadata", "download", "upload"):
        candidates = [
            (row, video)
            for row, video in rows
            if row.status in ({"upload", "uploading"} if lane == "upload" else {lane})
            and row.media_uploaded_at is None
        ]
        active = [
            (row, video)
            for row, video in candidates
            if row.claimed_at and not row.maintenance_token or row.status == "uploading"
        ]
        due = [
            (row, video)
            for row, video in candidates
            if not row.claimed_at
            and row.status != "uploading"
            and not row.maintenance_token
            and (not row.retry_at or _as_utc(row.retry_at) <= current)
        ]

        def due_time(row: ArchiveProgress) -> datetime:
            base = _as_utc(row.ready_at or row.updated_at)
            return max(base, _as_utc(row.retry_at)) if row.retry_at else base

        oldest = min((due_time(row) for row, _ in due), default=None)
        reason = "worker occupied" if due and len(active) >= capacities[lane] else None
        if candidates and not due and not active:
            reason = (
                "retry backoff"
                if any(row.retry_at for row, _ in candidates)
                else "partial maintenance"
            )
        if lane == "download" and due and not reason:
            reason = (
                download_reason
                if download_reason not in {None, "no eligible download"}
                else None
            )
        if lane == "metadata" and due and not reason:
            blocked = [
                metadata_reasons.get(
                    "recent" if video.notification_candidate else "backfill"
                )
                for _, video in due
            ]
            if all(blocked):
                reason = "; ".join(sorted(set(str(value) for value in blocked)))
        waiting = bool(due) and len(active) < capacities[lane] and not reason
        if waiting:
            eligible_since.setdefault(lane, current)
        else:
            eligible_since.pop(lane, None)
        idle_stall = (
            waiting
            and (current - max(eligible_since[lane], grace_end)).total_seconds()
            > stall_timeout
        )
        storage_blocked = (
            lane == "download"
            and bool(due)
            and reason
            in {
                "disk budget full",
                "data budget full",
                "protected partials exceed retention target",
            }
        )
        blocked_key = f"{lane}:storage"
        if storage_blocked:
            eligible_since.setdefault(blocked_key, current)
        else:
            eligible_since.pop(blocked_key, None)
        storage_stall = (
            storage_blocked
            and (
                current - max(eligible_since[blocked_key], grace_end, transfer_progress)
            ).total_seconds()
            > stall_timeout
        )
        activity: list[dict[str, Any]] = []
        stalled: list[str] = []
        for row, _ in active:
            stamp = (
                (row.transfer_progress_at or row.upload_started_at)
                if lane == "upload"
                else row.download_progress_at
                if lane == "download"
                else row.claimed_at
            )
            stamp = _as_utc(stamp or row.claimed_at or row.updated_at)
            stall_since = (
                max(stamp, _as_utc(row.claimed_at)) if row.claimed_at else stamp
            )
            if (current - stall_since).total_seconds() > stall_timeout:
                stalled.append(row.video_id)
            activity.append(
                {
                    "video_id": row.video_id,
                    "operation_id": row.operation_id,
                    "phase": row.download_phase or "downloading"
                    if lane == "download"
                    else lane,
                    "bytes": row.transfer_bytes
                    if lane == "upload"
                    else row.download_bytes,
                    "merge_bytes": row.merge_bytes,
                    "size": row.media_size
                    if lane == "upload"
                    else row.download_estimated_bytes,
                    "progress_at": stamp.isoformat(),
                }
            )
        status = (
            "unhealthy"
            if stalled or idle_stall
            else "degraded"
            if reason and candidates
            else "healthy"
        )
        if stalled or idle_stall:
            alerts.append(
                {
                    "key": f"{lane}:stall",
                    "severity": "error",
                    "message": f"{lane} lane stalled",
                    "details": {
                        "active_without_progress": stalled,
                        "due_queue_count": len(due),
                        "waiting_reason": reason,
                    },
                }
            )
        if storage_stall:
            alerts.append(
                {
                    "key": "download:storage",
                    "severity": "error",
                    "message": "download storage remains blocked",
                    "details": {"due_queue_count": len(due), "waiting_reason": reason},
                }
            )
        progress = [item["progress_at"] for item in activity]
        if lane in lane_progress:
            progress.append(_as_utc(lane_progress[lane]).isoformat())
        lanes[lane] = {
            "status": status,
            "capacity": capacities[lane],
            "active_count": len(active),
            "due_queue_count": len(due),
            "recent_due_count": sum(video.notification_candidate for _, video in due),
            "backfill_due_count": sum(
                not video.notification_candidate for _, video in due
            ),
            "oldest_due_age_seconds": max(0, (current - oldest).total_seconds())
            if oldest
            else None,
            "last_progress_at": max(progress) if progress else None,
            "waiting_reason": reason,
            "retry_deadline": min(
                (
                    _as_utc(row.retry_at).isoformat()
                    for row, _ in candidates
                    if row.retry_at and _as_utc(row.retry_at) > current
                ),
                default=None,
            ),
            "active": activity,
        }
        recent_due = [
            (row, video) for row, video in due if video.notification_candidate
        ]
        if recent_due:
            row, video = min(
                recent_due, key=lambda pair: _as_utc(pair[1].discovered_at)
            )
            age = max(0, (current - due_time(row)).total_seconds())
            item_reason = (
                metadata_reasons.get("recent") if lane == "metadata" else reason
            )
            recent[lane] = {
                "video_id": video.id,
                "operation_id": row.operation_id,
                "stage": row.status,
                "discovered_age_seconds": max(
                    0, (current - _as_utc(video.discovered_at)).total_seconds()
                ),
                "due_age_seconds": age,
                "waiting_reason": item_reason or reason or "awaiting worker dispatch",
            }
            threshold = (
                RECENT_METADATA_WARNING
                if lane == "metadata"
                else RECENT_DOWNLOAD_WARNING
                if lane == "download"
                else None
            )
            if threshold and age >= threshold and current >= grace_end:
                alerts.append(
                    {
                        "key": f"{lane}:recent-age",
                        "severity": "warning",
                        "message": f"recent video waiting at {lane}",
                        "details": recent[lane],
                    }
                )
    for kind in ("recovery", "validation", "webpage", "cleanup"):
        work = [job for job in jobs if job.kind == kind]
        required = kind in {"recovery", "cleanup"}
        stalled_jobs = [
            job
            for job in work
            if job.claim_token
            and job.claimed_at
            and (current - _as_utc(job.claimed_at)).total_seconds() > stall_timeout
        ]
        failed = [
            job
            for job in work
            if job.status == "failed"
            or job.status == "expired"
            and kind != "recovery"
            or not required
            and job.status != "passed"
            and job.deadline_at
            and _as_utc(job.deadline_at) <= current
        ]
        due = [
            job
            for job in work
            if job.status
            in (
                {"pending", "waiting", "expired"}
                if kind == "recovery"
                else {"pending", "waiting"}
            )
            and not job.claim_token
            and (not job.retry_at or _as_utc(job.retry_at) <= current)
        ]
        oldest = min((_as_utc(job.updated_at) for job in due), default=None)
        dispatch_key = f"{kind}:dispatch"
        waiting = required and bool(due) and not any(job.claim_token for job in work)
        if waiting:
            eligible_since.setdefault(dispatch_key, current)
        else:
            eligible_since.pop(dispatch_key, None)
        dispatch_stall = (
            waiting
            and (current - max(eligible_since[dispatch_key], grace_end)).total_seconds()
            > stall_timeout
        )
        unhealthy = (
            required and current >= grace_end and (bool(stalled_jobs) or dispatch_stall)
        )
        reason = (
            "worker stalled"
            if stalled_jobs
            else "operator review required"
            if failed
            else "awaiting worker dispatch"
            if dispatch_stall or waiting
            else "worker occupied"
            if due
            else "recovery in progress"
            if kind == "recovery" and any(job.claim_token for job in work)
            else "retry backoff"
            if any(
                job.status in {"pending", "waiting", "expired"} and job.retry_at
                for job in work
            )
            else None
        )
        lanes[kind] = {
            "status": "unhealthy" if unhealthy else "degraded" if reason else "healthy",
            "active_count": sum(bool(job.claim_token) for job in work),
            "due_queue_count": len(due),
            "oldest_due_age_seconds": max(0, (current - oldest).total_seconds())
            if oldest
            else None,
            "last_progress_at": max(
                (_as_utc(job.updated_at).isoformat() for job in work), default=None
            ),
            "waiting_reason": reason,
            "active_without_progress": [job.video_id for job in stalled_jobs],
            "failed_or_expired": [
                {
                    "video_id": job.video_id,
                    "status": job.status,
                    "reason": job.last_error,
                }
                for job in failed
            ],
        }
        if failed or dispatch_stall or stalled_jobs:
            alerts.append(
                {
                    "key": f"{kind}:outcome",
                    "severity": "error"
                    if required and current >= grace_end
                    else "warning",
                    "message": f"{kind} jobs failed or stalled",
                    "details": {
                        "count": len(failed),
                        "due_queue_count": len(due),
                        "active_without_progress": [
                            job.video_id for job in stalled_jobs
                        ],
                    },
                }
            )
    access_failures = [
        row
        for row, _ in rows
        if row.failure_phase == "youtube_access"
        and row.status in {"metadata", "download"}
        and row.media_uploaded_at is None
    ]
    archive_failures = {
        row.video_id
        for row, _ in rows
        if row.media_uploaded_at is None
        and row.status in {"upload", "reconcile_upload"}
        and "AuthenticationError:" in (row.last_error or "")
    } | {
        job.video_id
        for job in jobs
        if job.kind == "recovery"
        and job.status != "passed"
        and "AuthenticationError:" in (job.last_error or "")
    }
    for key, message, count in (
        (
            "youtube:access",
            "YouTube access failures persist; refresh account cookies",
            len(access_failures),
        ),
        (
            "archive:access",
            "Internet Archive access failures persist; check account credentials",
            len(archive_failures),
        ),
    ):
        if count:
            eligible_since.setdefault(key, current)
        else:
            eligible_since.pop(key, None)
        if (
            count
            and (current - max(eligible_since[key], grace_end)).total_seconds()
            > stall_timeout
        ):
            alerts.append(
                {
                    "key": key,
                    "severity": "error",
                    "message": message,
                    "details": {"count": count},
                }
            )
    recent_work = [
        (row, video)
        for row, video in rows
        if video.notification_candidate
        and row.status not in {"complete", "unavailable", "operator_action"}
    ]
    oldest_recent = None
    if recent_work:
        row, video = min(recent_work, key=lambda pair: _as_utc(pair[1].discovered_at))
        lane = "upload" if row.status == "uploading" else row.status
        reason = (
            "retry deadline"
            if row.retry_at and _as_utc(row.retry_at) > current
            else "remote upload reconciliation"
            if row.status == "reconcile_upload"
            else "transfer in progress"
            if row.claimed_at
            else lanes.get(lane, {}).get("waiting_reason") or "awaiting worker dispatch"
        )
        oldest_recent = {
            "video_id": video.id,
            "operation_id": row.operation_id,
            "stage": row.status,
            "phase": row.download_phase,
            "age_seconds": max(
                0, (current - _as_utc(video.discovered_at)).total_seconds()
            ),
            "waiting_reason": reason,
            "retry_at": _as_utc(row.retry_at).isoformat() if row.retry_at else None,
        }
    return {
        "version": 2,
        "generated_at": current.isoformat(),
        "healthy": all(
            lanes[lane]["status"] != "unhealthy"
            for lane in ("metadata", "download", "upload", "recovery", "cleanup")
        ),
        "startup_grace": current < grace_end,
        "lanes": lanes,
        "recent": recent,
        "oldest_recent": oldest_recent,
        "alerts": alerts,
    }
