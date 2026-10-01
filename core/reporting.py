"""Generate durable scheduled reports and deliver them through Discord."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from clyde import AllowedMentions, Markdown, Timestamp, Webhook
from clyde.components import Container, Seperator, SeperatorSpacing, TextDisplay
from clyde.webhook import MessageFlags
from cronsim import CronSim

from core.state import Report


@dataclass(frozen=True, slots=True)
class ReportWindow:
    """Represent one closed UTC reporting interval."""

    start: datetime
    end: datetime


def latest_report_boundary(
    now: datetime, schedule: str, finalization_grace: float
) -> datetime:
    """Return the latest cron boundary whose grace period has elapsed."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("current time must be timezone-aware")
    if finalization_grace <= 0:
        raise ValueError("report finalization grace must be positive")
    cutoff = (now - timedelta(seconds=finalization_grace)).astimezone(UTC)
    # Five-field cron occurrences are minute-aligned. Advancing to the next
    # minute lets reverse iteration include a boundary exactly at the cutoff.
    cursor = cutoff.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return next(CronSim(schedule, cursor, reverse=True))


def next_report_window(
    last_end: datetime, now: datetime, schedule: str, finalization_grace: float
) -> ReportWindow | None:
    """Return the next closed cron window without skipping persisted windows."""
    start = _as_utc(last_end)
    end = next(CronSim(schedule, start))
    if end > latest_report_boundary(now, schedule, finalization_grace):
        return None
    return ReportWindow(start, end)


def report_has_activity(report: Report) -> bool:
    """Return whether a snapshot warrants a Discord message."""
    return any(
        (
            report.discovered_videos,
            report.discovered_shorts,
            report.discovered_livestreams,
            report.media_archived,
            report.media_bytes,
            report.unfinished_workflows,
            report.awaiting_replay,
            report.unavailable,
            report.operator_action,
            report.validation_pending,
            report.validation_failed,
            report.webpage_pending,
            report.webpage_failed,
        )
    )


def build_report_webhook(report: Report, webhook_url: str) -> Webhook:
    """Construct Saga's compact Components V2 report message."""
    period_discovered = (
        report.discovered_videos
        + report.discovered_shorts
        + report.discovered_livestreams
    )
    lifetime_discovered = (
        report.lifetime_discovered_videos
        + report.lifetime_discovered_shorts
        + report.lifetime_discovered_livestreams
    )
    statistics = Markdown.bulleted_list(
        [
            (
                "Discovered: "
                f"{Markdown.bold(f'{period_discovered:,}')}"
                f" | {Markdown.bold(f'{lifetime_discovered:,}')} Lifetime"
            ),
            {
                "value": (
                    f"Videos {report.discovered_videos:,}"
                    f" | {report.lifetime_discovered_videos:,} Lifetime"
                ),
                "indent": 1,
            },
            {
                "value": (
                    f"Shorts {report.discovered_shorts:,}"
                    f" | {report.lifetime_discovered_shorts:,} Lifetime"
                ),
                "indent": 1,
            },
            {
                "value": (
                    f"Livestreams {report.discovered_livestreams:,}"
                    f" | {report.lifetime_discovered_livestreams:,} Lifetime"
                ),
                "indent": 1,
            },
            (
                (
                    "Videos uploaded: "
                    if report.semantics_version >= 2
                    else "Media archived: "
                )
                + f"{Markdown.bold(f'{report.media_archived:,}')}"
                f" | {Markdown.bold(f'{report.lifetime_media_archived:,}')} Lifetime"
            ),
            (
                (
                    "Bytes uploaded: "
                    if report.semantics_version >= 2
                    else "Media preserved: "
                )
                + f"{Markdown.bold(format_bytes(report.media_bytes))}"
                " | "
                f"{Markdown.bold(format_bytes(report.lifetime_media_bytes))} Lifetime"
            ),
            f"Active channels: {Markdown.bold(f'{report.active_channels:,}')}",
            (
                "Unfinished workflows: "
                f"{Markdown.bold(f'{report.unfinished_workflows:,}')}"
            ),
            f"Awaiting replay: {Markdown.bold(f'{report.awaiting_replay:,}')}",
            f"Unavailable: {Markdown.bold(f'{report.unavailable:,}')}",
            f"Operator action: {Markdown.bold(f'{report.operator_action:,}')}",
            f"Ready uploads: {report.ready_uploads:,}"
            f" | Pending downloads: {report.pending_downloads:,}",
            f"Active transfers: {report.active_transfers:,}"
            f" | Failed: {report.failed_transfers:,}"
            f" | Uncertain: {report.uncertain_transfers:,}",
            f"Validation: {report.validation_pending:,} pending, "
            f"{report.validation_passed:,} passed, "
            f"{report.validation_failed:,} failed, "
            f"{report.validation_expired:,} expired",
            f"Webpages: {report.webpage_pending:,} pending, "
            f"{report.webpage_completed:,} completed, "
            f"{report.webpage_failed:,} failed/expired",
            f"Recovered/historical uploads: {report.recovered_uploads:,} "
            "(excluded from newly transferred bytes)",
        ]
    )
    footer = Markdown.subtext(
        f"Report for {Timestamp.short_date(_as_utc(report.window_start))} to "
        f"{Timestamp.short_date(_as_utc(report.window_end))} "
        f"(Generated {Timestamp.relative_time(_as_utc(report.generated_at))})"
    )
    container = Container(
        components=[
            TextDisplay(content=Markdown.header_2("YouTube Archival Report")),
            TextDisplay(content=statistics),
            Seperator(divider=True, spacing=SeperatorSpacing.SMALL),
            TextDisplay(content=footer),
        ],
        accent_color=0xFF0000,
    )
    webhook = Webhook(
        url=webhook_url, allowed_mentions=AllowedMentions(parse=[], replied_user=False)
    )
    webhook.set_wait(True)
    webhook.add_component(container)
    if not webhook.get_flag(MessageFlags.IS_COMPONENTS_V2):
        raise RuntimeError("Components V2 flag was not set")
    return webhook


async def send_report(report: Report, webhook_url: str, timeout: float) -> str:
    """Send one frozen report and return Discord's message ID."""
    webhook = build_report_webhook(report, webhook_url)
    async with asyncio.timeout(timeout):
        response = await webhook.execute_async()
    data: Any = response.json()
    message_id: Any = data.get("id") if isinstance(data, dict) else None
    if not isinstance(message_id, str) or not message_id:
        raise RuntimeError("Discord report delivery returned no message ID")
    return message_id


def format_bytes(value: int) -> str:
    """Format a non-negative byte count with binary units."""
    size = float(max(0, value))
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{int(size):,} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")  # pragma: no cover


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
