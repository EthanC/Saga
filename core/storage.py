"""Measure Saga-owned data and bound download/merge reservations."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path


def data_usage(root: Path) -> int:
    """Count regular-file bytes recursively without following symlinks.

    Upload cleanup can remove entries during a scan. Other I/O failures must
    propagate rather than allowing an unreadable subtree to hide its usage.
    """
    total = 0
    with os.scandir(root) as entries:
        for entry in entries:
            with suppress(FileNotFoundError):
                if entry.is_dir(follow_symlinks=False):
                    total += data_usage(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


def remaining_data_bytes(root: Path, maximum: int) -> int | None:
    """Return the instance budget, or None when disabled; fail closed on I/O."""
    if maximum == 0:
        return None
    try:
        return max(maximum - data_usage(root), 0)
    except OSError:
        return 0
