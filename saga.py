"""Saga application entrypoint."""

import asyncio
import signal
from pathlib import Path

from filelock import FileLock, Timeout
from loguru import logger

from core.bandwidth import ByteLimiter
from core.config import load_settings, load_youtube_configs
from core.logging import configure_logging
from core.service import SagaService
from core.state import StateStore
from core.youtube import YouTubeClient

PROJECT_ROOT = Path(__file__).resolve().parent


async def start() -> None:
    """Validate configuration and run Saga until a termination signal."""
    settings = load_settings(PROJECT_ROOT)
    configure_logging(settings)

    logger.debug("Loading YouTube configuration from {}", settings.config_path)
    configurations = load_youtube_configs(settings.config_path)
    settings.data_path.mkdir(parents=True, exist_ok=True)
    lock = FileLock(settings.data_path / "saga.lock")

    try:
        logger.debug("Acquiring process lock in {}", settings.data_path)
        lock.acquire(timeout=0)
    except Timeout:
        raise RuntimeError(
            f"Another Saga process is using {settings.data_path}"
        ) from None

    state = StateStore(settings.data_path / "saga.sqlite")
    download_limiter = ByteLimiter(settings.download_bytes_per_second)
    upload_limiter = ByteLimiter(settings.upload_bytes_per_second)
    service = SagaService(
        settings,
        configurations,
        state,
        YouTubeClient(settings.youtube_cookies_path, download_limiter),
        upload_limiter=upload_limiter,
    )
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        logger.info("Shutdown requested")
        service.request_stop()

    def stop(*_: object) -> None:
        loop.call_soon_threadsafe(request_shutdown)

    for name in ("SIGINT", "SIGTERM"):
        current = getattr(signal, name, None)
        if current is not None:
            signal.signal(current, stop)

    logger.info("Saga")
    logger.info("Loaded {} YouTube instance(s)", len(configurations))
    logger.info("Using persistent state at {}", state.path)
    try:
        await service.run()
    finally:
        logger.debug("Closing persistent state and releasing process lock")
        state.close()
        lock.release()
        logger.info("Saga stopped")


if __name__ == "__main__":
    try:
        asyncio.run(start())
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Saga stopped due to a fatal error")
        raise SystemExit(1) from None
