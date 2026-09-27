import asyncio
from typing import Any

from core import notifications


class Response:
    def json(self) -> dict[str, str]:
        return {"id": "message-id"}


class FakeWebhook:
    created: FakeWebhook | None = None

    def __init__(self, **values: Any) -> None:
        self.values = values
        self.wait: bool | None = None
        FakeWebhook.created = self

    def set_wait(self, wait: bool) -> FakeWebhook:
        self.wait = wait
        return self

    async def execute_async(self) -> Response:
        return Response()


def test_send_video_uses_plain_content(monkeypatch: Any) -> None:
    monkeypatch.setattr(notifications, "Webhook", FakeWebhook)

    result = asyncio.run(
        notifications.send_video(
            "https://discord.com/api/webhooks/123/token", "abcdefghijk", 1
        )
    )

    webhook = FakeWebhook.created
    assert webhook is not None
    assert result == "message-id"
    assert webhook.values["content"] == "https://youtu.be/abcdefghijk"
    assert webhook.values["allowed_mentions"].parse == []
    assert webhook.wait is True
    assert set(webhook.values) == {"url", "content", "allowed_mentions"}
