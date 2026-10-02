"""One thread-safe stop request and monotonic shutdown deadline."""

import time
from threading import Event
from typing import Any


class CooperativeStop(RuntimeError):
    """Stop blocking work while preserving its resumable or uncertain outcome."""


class StopController:
    def __init__(self, timeout: float = 90, *, event: Any = None) -> None:
        self.event = event if event is not None else Event()
        self.timeout = timeout
        self.deadline: float | None = None

    def request(self) -> None:
        if self.deadline is None:
            self.deadline = time.monotonic() + self.timeout
        self.event.set()

    def check(self) -> None:
        if self.event.is_set():
            raise CooperativeStop("shutdown requested")

    def wait(self, seconds: float) -> None:
        if self.event.wait(seconds):
            raise CooperativeStop("shutdown interrupted wait")

    def remaining(self) -> float:
        return (
            self.timeout
            if self.deadline is None
            else max(0, self.deadline - time.monotonic())
        )
