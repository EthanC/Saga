import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from core import service, staging, youtube
from core.config import load_settings
from core.state import StateStore
from tests.test_archival import settings
from tests.test_partials import FORMATS, partial


@pytest.mark.parametrize(
    "case", ["guard", "plain", "disk", "identity", "merge_disk", "resume"]
)
def test_selected_formats_and_merge_recheck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    directory = tmp_path / "operation"
    if case == "resume":
        directory.mkdir()
        (directory / "media.f315.webm.part").write_bytes(b"part")
        (directory / "media.f315.webm.ytdl").write_text('{"downloader":{}}')
    free = 100
    events: list[tuple[int, int]] = []

    class Downloader:
        def __init__(self, options: Any) -> None:
            self.options = options

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def process_info(self, info: Any) -> None:
            pass

        def download(self, urls: Any) -> int:
            nonlocal free
            if case == "disk":
                free = 1
            self.process_info(
                {
                    "id": "other" if case == "identity" else "video",
                    "requested_formats": [dict(FORMATS[0], filesize=4)],
                }
            )
            for hook in self.options["postprocessor_hooks"]:
                hook({"status": "finished"})
                if case == "merge_disk":
                    free = 1
                hook({"status": "started"})
            (directory / "merging.mkv").write_bytes(b"media")
            return 0

    monkeypatch.setattr(youtube, "YoutubeDL", Downloader)
    monkeypatch.setattr(
        youtube.shutil, "disk_usage", lambda _: type("Space", (), {"free": free})()
    )
    client = youtube.YouTubeClient()

    def run() -> Path:
        return client.download_video(
            "video",
            directory,
            operation_id="operation",
            minimum_free_bytes=0,
            estimated_bytes=4,
            disk_guard=(
                lambda expected, downloaded: events.append((expected, downloaded))
            )
            if case == "guard"
            else None,
        )

    if case in {"disk", "merge_disk"}:
        with pytest.raises(youtube.DiskSpaceError):
            run()
    elif case == "identity":
        with pytest.raises(RuntimeError, match="match operation"):
            run()
    else:
        assert run().exists()
    if case == "guard":
        assert events == [(4, 0), (4, 0)]


@pytest.mark.parametrize(
    "problem",
    [
        "formats",
        "sidecar",
        "input",
        "unknown",
        "symlink",
        "temporary",
        "symlink_manifest",
        "missing",
        "identity",
    ],
)
def test_inventory_refuses_ambiguous_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    directory = tmp_path / "operation"
    directory.mkdir()
    staging.prepare_inputs(directory, "operation", "video", FORMATS)
    path = directory / "merging.f315.webm.part"
    path.write_bytes(b"input")
    if problem == "formats":
        (directory / "inputs.json").write_text(
            '{"operation_id":"operation","video_id":"video","formats":{}}'
        )
    elif problem == "sidecar":
        (directory / "merging.f315.webm.ytdl").write_text('{"downloader":1}')
        with pytest.raises(staging.StagingIdentityError):
            staging.prepare_inputs(directory, "operation", "video", FORMATS)
    elif problem == "input":
        (directory / "merging.f314.webm.part").write_bytes(b"other")
    elif problem == "unknown":
        (directory / "unknown.part").write_bytes(b"unknown")
    elif problem in {"symlink", "temporary", "symlink_manifest"}:
        original = Path.is_symlink
        name = {
            "symlink": path.name,
            "temporary": "inputs.tmp",
            "symlink_manifest": "inputs.json",
        }[problem]
        monkeypatch.setattr(
            Path, "is_symlink", lambda file: file.name == name or original(file)
        )
        with pytest.raises(staging.StagingIdentityError):
            staging.prepare_inputs(directory, "operation", "video", FORMATS)
    elif problem == "missing":
        assert (
            staging.inventory_inputs(directory / "gone", "gone", "video").reusable_bytes
            == 0
        )
        return
    elif problem == "identity":
        assert staging.inventory_inputs(directory, "other", "video").finding
        return
    inventory = staging.inventory_inputs(directory, "operation", "video")
    if problem != "temporary":
        assert inventory.reusable_bytes == 0


def test_staging_root_symlink_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Path, "is_symlink", lambda _: True)
    with pytest.raises(RuntimeError, match="symlink"):
        staging.ensure_download_root(tmp_path / "downloads")


def test_retention_preserves_protected_and_detects_claim_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    partial(store, tmp_path)
    configured = settings(tmp_path)
    assert not staging.maintain_partials(store, configured)["decisions"]
    store.update_archive("abcdefghijk", status="upload")
    assert not staging.maintain_partials(store, configured)["inactive_bytes"]
    store.update_archive(
        "abcdefghijk", status="download", working_directory=str(tmp_path / "outside")
    )
    assert staging.maintain_partials(store, configured)["findings"]
    store.close()


def test_existing_schema_backup_and_idempotent_migration(tmp_path: Path) -> None:
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    store.close()
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("ALTER TABLE archiveprogress DROP COLUMN download_phase")
        connection.execute("DELETE FROM schemamigration WHERE version=3")
    StateStore(path).close()
    StateStore(path).close()
    assert path.with_name(path.name + ".pre-staging-audit.sqlite3").exists()


def test_invalid_retention_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SAGA_PARTIAL_RETENTION_MODE", "delete-all")
    with pytest.raises(ValueError, match="observe or enforce"):
        load_settings(tmp_path)


def test_partial_worker_failure_is_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StateStore(tmp_path / "saga.sqlite")
    saga = service.SagaService(settings(tmp_path), (), store, youtube.YouTubeClient())
    monkeypatch.setattr(
        service,
        "maintain_partials",
        lambda *args: (_ for _ in ()).throw(OSError("fixture")),
    )

    async def wait(_: float) -> None:
        saga.request_stop()

    monkeypatch.setattr(saga, "_wait", wait)
    asyncio.run(saga._partial_maintenance_loop())
    store.close()
