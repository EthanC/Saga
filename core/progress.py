"""Worker-local download progress with conservative durable reservations."""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from core.staging import download_reservation
from core.state import ArchiveWork, StateStore, utcnow
from core.youtube import DiskSpaceError

CHECKPOINT_INTERVAL = 5.0


class DownloadCheckpoint:
    """Check disk on callbacks and commit routine progress once per interval."""

    def __init__(
        self,
        state: StateStore,
        work: ArchiveWork,
        directory: Path,
        minimum: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.state = state
        self.video_id = work.video.id
        self.directory = directory
        self.minimum = minimum
        self.clock = clock
        self.last_checkpoint = clock()
        self.allowance = work.progress.reserved_bytes
        self.expected = work.progress.download_estimated_bytes or 0
        self.downloaded = 0
        self.phase = "downloading"
        self.merge_bytes = 0
        self.progress_at: datetime | None = work.progress.download_progress_at
        self.saved: dict[str, Any] = {}

    def facts(self) -> dict[str, Any]:
        return {
            "download_bytes": self.downloaded,
            "download_phase": self.phase,
            "merge_bytes": self.merge_bytes,
            "download_progress_at": self.progress_at,
        }

    def record(self, expected: int, downloaded: int, *, force: bool = False) -> None:
        if downloaded > self.downloaded:
            self.progress_at = utcnow()
        estimate_grew = max(expected, downloaded) > self.expected
        self.expected = max(expected, downloaded, self.expected)
        self.downloaded = downloaded
        required = (
            max(self.expected - self.merge_bytes, 0)
            if self.phase in {"merging", "merged"}
            else download_reservation(self.expected, downloaded)
        )
        free = shutil.disk_usage(self.directory).free
        others = self.state.other_download_reservations(self.video_id)
        checkpoint = (
            force
            or estimate_grew
            or required > self.allowance
            or self.clock() - self.last_checkpoint >= CHECKPOINT_INTERVAL
        )
        if free < self.minimum + others + required:
            self.flush()
            raise DiskSpaceError("external disk depletion or download disk budget full")
        if checkpoint:
            self.state.guard_download_space(
                self.video_id,
                self.expected,
                downloaded,
                free,
                self.minimum,
                **self.facts(),
            )
            self.allowance = required
            self.last_checkpoint = self.clock()
            self.saved = self.facts()

    def boundary(self, phase: str, output_bytes: int = 0) -> None:
        if phase == "stream_finished":
            self.record(self.expected, self.downloaded, force=True)
            return
        changed = phase != self.phase
        if changed or output_bytes > self.merge_bytes:
            self.progress_at = utcnow()
        self.phase = phase
        self.merge_bytes = output_bytes
        self.record(self.expected, self.downloaded, force=changed)

    def flush(self) -> None:
        """Save pending progress on failure/stop without granting a larger write."""
        facts = self.facts()
        if facts != self.saved:
            self.state.update_archive(self.video_id, **facts)
            self.saved = facts
