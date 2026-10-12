"""Exercise Saga's real signal handler with an uncertain local HTTP mutation."""

import asyncio
import signal
import sys
from pathlib import Path
from typing import Any

import pytest

import saga
from core.archival import ArchiveProcessor
from core.service import SagaService
from core.state import StateStore
from core.workers import run_managed
from tests.test_archival import FakeGateway, seed_upload, settings


async def main(root: Path, endpoint: str) -> None:
    configured = settings(root)
    state = StateStore(root / "saga.sqlite")
    seed_upload(state, root, "abcdefghijk")
    state.close()
    loop = asyncio.get_running_loop()
    patch = pytest.MonkeyPatch()

    async def interrupt_after_request() -> None:
        while not (root / "request-started").exists():
            await asyncio.sleep(0.05)
        signal.raise_signal(signal.SIGTERM)

    def service(*args: Any, **kwargs: Any) -> SagaService:
        controller = kwargs["stop_controller"]

        class Gateway(FakeGateway):
            def upload(self, *args: Any, **kwargs: Any) -> Any:
                loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(interrupt_after_request())
                )
                return run_managed(
                    "ziggy",
                    "submit",
                    {},
                    (endpoint, "fixture", "https://example.com/video", 60),
                    {},
                    controller,
                )

        instance = SagaService(
            *args,
            **kwargs,
            archival=ArchiveProcessor(
                configured, args[2], args[3], Gateway(), stop_controller=controller
            ),
        )

        async def initialize() -> tuple[Any, ...]:
            return ()

        patch.setattr(instance, "_initialize", initialize)
        return instance

    patch.setattr(saga, "load_settings", lambda _: configured)
    patch.setattr(saga, "load_youtube_configs", lambda _: ())
    patch.setattr(saga, "SagaService", service)
    await saga.start()


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1]), sys.argv[2]))
