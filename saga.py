"""Saga application entrypoint."""

import asyncio
import signal
from pathlib import Path

from filelock import FileLock, Timeout
from loguru import logger

from core.bandwidth import ByteLimiter
from core.config import load_logging_settings, load_settings, load_youtube_configs
from core.logging import bind_stop_controller, configure_logging
from core.service import SagaService
from core.shutdown import CooperativeStop, StopController
from core.state import StateStore
from core.workers import shutdown_workers
from core.youtube import YouTubeClient

PROJECT_ROOT = Path(__file__).resolve().parent


async def start() -> None:
    """Validate configuration and run Saga until a termination signal."""
    configure_logging(load_logging_settings(PROJECT_ROOT))
    settings = load_settings(PROJECT_ROOT)

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

    state = StateStore(
        settings.data_path / "saga.sqlite", archival_enabled=settings.archival_enabled
    )
    controller = StopController(settings.shutdown_timeout)
    bind_stop_controller(controller)
    download_limiter = ByteLimiter(
        settings.download_bytes_per_second, stop_controller=controller
    )
    upload_limiter = ByteLimiter(
        settings.upload_bytes_per_second, stop_controller=controller
    )
    service = SagaService(
        settings,
        configurations,
        state,
        YouTubeClient(
            settings.youtube_cookies_path,
            download_limiter,
            pot_provider_url=settings.youtube_pot_provider_url,
            stop_controller=controller,
            managed=True,
        ),
        upload_limiter=upload_limiter,
        stop_controller=controller,
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
    except CooperativeStop:
        pass
    finally:
        logger.debug("Closing persistent state and releasing process lock")
        logger.info("Saved shutdown operations: {}", state.shutdown_summary())
        shutdown_workers()
        state.close()
        lock.release()
        logger.info("Saga stopped")
        logger.complete()


if __name__ == "__main__":
    try:
        asyncio.run(start())
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Saga stopped due to a fatal error")
        logger.complete()
        raise SystemExit(1) from None
