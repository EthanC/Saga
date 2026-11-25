"""Operator maintenance commands that change saved state outside the service."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from pathlib import Path

from core.config import load_settings
from core.state import StateStore


def run_command(
    project_root: Path, argv: Sequence[str], output: Callable[[str], None] = print
) -> int:
    """Run one maintenance command and return its process exit status."""
    parser = argparse.ArgumentParser(prog="saga.py")
    commands = parser.add_subparsers(dest="command", required=True)
    retry = commands.add_parser(
        "retry-derives", help="queue one new derive for chosen failed derives"
    )
    retry.add_argument(
        "items", nargs="*", help="video IDs or Internet Archive identifiers"
    )
    retry.add_argument("--all", action="store_true", help="retry every failed derive")
    arguments = parser.parse_args(argv)
    if bool(arguments.items) == arguments.all:
        parser.error("name failed derives to retry, or pass --all")
    settings = load_settings(project_root)
    state = StateStore(
        settings.data_path / "saga.sqlite", archival_enabled=settings.archival_enabled
    )
    try:
        reset = state.reset_failed_derives(arguments.items)
    finally:
        state.close()
    for identifier in reset:
        output(f"Queued a new derive for {identifier}")
    if not reset:
        output("No failed derives matched")
        return 1
    return 0
