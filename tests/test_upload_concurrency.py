import asyncio
from datetime import timedelta
from pathlib import Path
from threading import Event
from typing import Any

from core.archival import ArchiveProcessor, Inspection
from core.bandwidth import ByteLimiter, ThrottledBody
from core.service import SagaService
from core.state import StateStore, utcnow
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, run_job, saved, seed_upload, settings
from tests.test_bandwidth import FakeTime


def test_two_upload_lanes_share_bandwidth_and_recover_and_clean_independently(
    tmp_path: Path,
) -> None:
    state = StateStore(tmp_path / "saga.sqlite")
    first, first_media, first_frozen = seed_upload(state, tmp_path)
    second, second_media, _ = seed_upload(state, tmp_path, "SecondVid12")
    clock = FakeTime()
    limiter = ByteLimiter(1, clock=clock.clock, wait=clock.wait)
    started: list[str] = []
    finished: list[str] = []
    release = Event()
    payloads: list[bytes] = []

    class Gateway(FakeGateway):
        def upload(self, metadata: Any, media_path: Path, **kwargs: Any) -> Any:
            started.append(metadata.operation_id)
            assert release.wait(5)
            with media_path.open("rb") as stream:
                payloads.append(b"".join(ThrottledBody(stream, limiter)))
            finished.append(metadata.operation_id)
            if metadata.operation_id == first_frozen.operation_id:
                raise ConnectionResetError("remote acknowledgement lost")
            return super().upload(metadata, media_path, **kwargs)

    gateway = Gateway()
    configured = settings(tmp_path, upload_workers=2)
    processor = ArchiveProcessor(configured, state, YouTubeClient(), gateway)
    saga = SagaService(configured, (), state, YouTubeClient(), archival=processor)

    async def run() -> None:
        lanes = [
            asyncio.create_task(saga._archive_loop(index, "upload"))
            for index in range(2)
        ]
        try:
            for _ in range(300):
                if len(started) == 2:
                    break
                await asyncio.sleep(0.01)
            assert len(set(started)) == 2
            health = state.transfer_health(stall_timeout=900, upload_workers=2)
            assert health["lanes"]["upload"]["active_count"] == 2 and health["healthy"]
            release.set()
            for _ in range(300):
                if (
                    saved(state, second).media_uploaded_at
                    and saved(state, first).status == "reconcile_upload"
                ):
                    break
                await asyncio.sleep(0.01)
            assert saved(state, second).media_uploaded_at
            assert saved(state, first).status == "reconcile_upload"
        finally:
            release.set()
            saga.request_stop()
            await asyncio.gather(*lanes)

    asyncio.run(run())
    assert len(set(finished)) == 2 and payloads == [b"test", b"test"]
    assert sum(clock.waits) >= 7
    assert saved(state, first).operation_id == first_frozen.operation_id
    run_job(state, processor, "cleanup")
    assert first_media.exists() and not second_media.exists()
    gateway.inspection = Inspection("verified", receipt="remote original confirmed")
    run_job(state, processor, "recovery", now=utcnow() + timedelta(seconds=3))
    assert saved(state, first).upload_origin == "recovered"
    run_job(state, processor, "cleanup")
    assert not first_media.exists()
    state.close()
