import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from clyde import AllowedMentions
from clyde.components import Container, Seperator, TextDisplay
from clyde.webhook import MessageFlags

from core import reporting
from core.state import Report

WEBHOOK = "https://discord.com/api/webhooks/123/token"


def report(**values: Any) -> Report:
    result = Report(
        id=1,
        window_start=datetime(2026, 1, 1, tzinfo=UTC),
        window_end=datetime(2026, 1, 2, tzinfo=UTC),
        generated_at=datetime(2026, 1, 2, 0, 5, tzinfo=UTC),
    )
    for name, value in values.items():
        setattr(result, name, value)
    return result


def test_cron_windows_observe_finalization_grace() -> None:
    schedule = "0 0 * * *"
    last_end = datetime(2026, 1, 1, tzinfo=UTC)

    assert (
        reporting.next_report_window(
            last_end, datetime(2026, 1, 2, 0, 4, 59, tzinfo=UTC), schedule, 300
        )
        is None
    )
    window = reporting.next_report_window(
        last_end, datetime(2026, 1, 2, 0, 5, tzinfo=UTC), schedule, 300
    )
    assert window == reporting.ReportWindow(
        datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)
    )


def test_cron_windows_can_have_variable_lengths() -> None:
    window = reporting.next_report_window(
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 1, 0, 1, tzinfo=UTC),
        "0 0 1 * *",
        60,
    )

    assert window == reporting.ReportWindow(
        datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
    )


def test_window_helpers_validate_inputs() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        reporting.latest_report_boundary(datetime(2026, 1, 1), "* * * * *", 5)
    with pytest.raises(ValueError, match="positive"):
        reporting.latest_report_boundary(datetime.now(UTC), "* * * * *", 0)
    assert reporting.latest_report_boundary(
        datetime(2026, 1, 1, 0, 1, 30, tzinfo=UTC), "* * * * *", 30
    ) == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)


def test_report_activity_ignores_active_channels_alone() -> None:
    assert reporting.report_has_activity(report(active_channels=4)) is False
    assert reporting.report_has_activity(report(awaiting_replay=1)) is True


def test_build_report_webhook_uses_exact_component_shape() -> None:
    webhook = reporting.build_report_webhook(
        report(
            discovered_videos=1,
            discovered_shorts=2,
            discovered_livestreams=3,
            lifetime_discovered_videos=10,
            lifetime_discovered_shorts=20,
            lifetime_discovered_livestreams=30,
            media_archived=2,
            lifetime_media_archived=8,
            media_bytes=1024,
            lifetime_media_bytes=1024**2,
            active_channels=4,
            unfinished_workflows=5,
            awaiting_replay=1,
            unavailable=2,
            operator_action=3,
        ),
        WEBHOOK,
    )

    assert webhook._query_params == {"wait": "True"}
    assert isinstance(webhook.allowed_mentions, AllowedMentions)
    assert webhook.allowed_mentions.parse == []
    assert webhook.allowed_mentions.replied_user is False
    assert webhook.get_flag(MessageFlags.IS_COMPONENTS_V2)
    assert isinstance(webhook.components, list)
    assert len(webhook.components) == 1
    container = webhook.components[0]
    assert isinstance(container, Container)
    assert [type(component) for component in container.components] == [
        TextDisplay,
        TextDisplay,
        Seperator,
        TextDisplay,
    ]
    heading = container.components[0]
    counts = container.components[1]
    separator = container.components[2]
    footer = container.components[3]
    assert isinstance(heading, TextDisplay)
    assert isinstance(counts, TextDisplay)
    assert isinstance(separator, Seperator)
    assert isinstance(footer, TextDisplay)
    assert heading.content == "## YouTube Archival Report"
    statistics = counts.content
    assert "Discovered: **6** | **60** Lifetime" in statistics
    assert "Videos 1 | 10 Lifetime" in statistics
    assert "Shorts 2 | 20 Lifetime" in statistics
    assert "Livestreams 3 | 30 Lifetime" in statistics
    assert "Media preserved: **1.0 KiB** | **1.0 MiB** Lifetime" in statistics
    assert separator.divider is True
    assert "Report for" in footer.content
    assert not isinstance(webhook.content, str)
    assert not isinstance(webhook.embeds, list)


def test_build_report_requires_components_v2_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeWebhook:
        def __init__(self, **_: Any) -> None:
            pass

        def set_wait(self, _: bool) -> None:
            pass

        def add_component(self, _: Any) -> None:
            pass

        def get_flag(self, _: Any) -> bool:
            return False

    monkeypatch.setattr(reporting, "Webhook", FakeWebhook)

    with pytest.raises(RuntimeError, match="Components V2"):
        reporting.build_report_webhook(report(), WEBHOOK)


@pytest.mark.parametrize("payload", [None, {}, {"id": ""}, {"id": 123}])
def test_send_report_rejects_missing_message_id(
    monkeypatch: pytest.MonkeyPatch, payload: Any
) -> None:
    class Response:
        def json(self) -> Any:
            return payload

    class FakeWebhook:
        async def execute_async(self) -> Response:
            return Response()

    monkeypatch.setattr(reporting, "build_report_webhook", lambda *_: FakeWebhook())

    with pytest.raises(RuntimeError, match="no message ID"):
        asyncio.run(reporting.send_report(report(), WEBHOOK, 1))


def test_send_report_returns_waited_message_id(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def json(self) -> dict[str, str]:
            return {"id": "message-id"}

    class FakeWebhook:
        async def execute_async(self) -> Response:
            await asyncio.sleep(0)
            return Response()

    monkeypatch.setattr(reporting, "build_report_webhook", lambda *_: FakeWebhook())

    assert asyncio.run(reporting.send_report(report(), WEBHOOK, 1)) == "message-id"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-1, "0 B"),
        (1023, "1,023 B"),
        (1024, "1.0 KiB"),
        (1024**3, "1.0 GiB"),
        (1024**6, "1024.0 PiB"),
    ],
)
def test_format_bytes(value: int, expected: str) -> None:
    assert reporting.format_bytes(value) == expected
