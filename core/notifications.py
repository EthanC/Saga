"""Deliver persisted video notifications through Clyde."""

import asyncio
from typing import Any

from clyde import AllowedMentions, Webhook
from loguru import logger


async def send_video(webhook_url: str, video_id: str, timeout: float) -> str:
    """Send exactly one YouTube URL and return its message ID."""
    logger.debug("Sending Discord notification for video {}", video_id)
    webhook = Webhook(
        url=webhook_url,
        content=f"https://youtu.be/{video_id}",
        allowed_mentions=AllowedMentions(parse=[]),
    )
    webhook.set_wait(True)

    async with asyncio.timeout(timeout):
        response = await webhook.execute_async()

    data: Any = response.json()
    message_id: Any = data.get("id") if isinstance(data, dict) else None
    if not isinstance(message_id, str) or not message_id:
        raise RuntimeError("Discord webhook delivery returned no message ID")
    logger.debug("Discord accepted notification for video {}", video_id)
    return message_id
