import asyncio
from typing import Any

import pytest

from core import notifications


@pytest.mark.parametrize("payload", [None, {}, {"id": ""}, {"id": 123}])
def test_send_video_rejects_missing_message_id(
    monkeypatch: pytest.MonkeyPatch, payload: Any
) -> None:
    class Response:
        def json(self) -> Any:
            return payload

    class Webhook:
        def __init__(self, **values: Any) -> None:
            pass

        def set_wait(self, wait: bool) -> Webhook:
            return self

        async def execute_async(self) -> Response:
            return Response()

    monkeypatch.setattr(notifications, "Webhook", Webhook)

    with pytest.raises(RuntimeError, match="no message ID"):
        asyncio.run(notifications.send_video("https://example.com", "abcdefghijk", 1))
