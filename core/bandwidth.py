"""Process-wide payload bandwidth limiting primitives."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from threading import Lock
from typing import Any, Protocol

from niquests.adapters import HTTPAdapter
from niquests.models import PreparedRequest, Response

from core.shutdown import StopController

MAX_CHUNK_BYTES = 64 * 1024


class Limiter(Protocol):
    @property
    def enabled(self) -> bool: ...

    chunk_size: int

    def acquire(self, byte_count: int) -> None: ...
    def refund(self, byte_count: int) -> None: ...


class ByteLimiter:
    """Share a bounded-burst byte budget safely across threads."""

    def __init__(
        self,
        bytes_per_second: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
        stop_controller: StopController | None = None,
    ) -> None:
        if bytes_per_second < 0:
            raise ValueError("bytes_per_second must be non-negative")
        self.bytes_per_second = bytes_per_second
        self.chunk_size = (
            min(bytes_per_second, MAX_CHUNK_BYTES)
            if bytes_per_second
            else MAX_CHUNK_BYTES
        )
        self.stop_controller = stop_controller
        self._clock = clock
        self._wait = wait
        self._lock = Lock()
        self._updated_at = clock()
        self._allowance = float(self.chunk_size)

    @property
    def enabled(self) -> bool:
        """Return whether this limiter performs accounting and waits."""
        return self.bytes_per_second > 0

    def acquire(self, byte_count: int) -> None:
        """Reserve allowance for a transfer, splitting requests when necessary."""
        if byte_count < 0:
            raise ValueError("byte_count must be non-negative")
        if self.stop_controller:
            self.stop_controller.check()
        if not self.enabled:
            return
        remaining = byte_count
        while remaining:
            amount = min(remaining, self.chunk_size)
            self._acquire_chunk(amount)
            remaining -= amount

    def refund(self, byte_count: int) -> None:
        """Return reserved bytes that a short read did not transfer."""
        if not self.enabled or byte_count <= 0:
            return
        with self._lock:
            self._refill(self._clock())
            self._allowance = min(float(self.chunk_size), self._allowance + byte_count)

    def _acquire_chunk(self, byte_count: int) -> None:
        while True:
            with self._lock:
                self._refill(self._clock())
                if self._allowance >= byte_count:
                    self._allowance -= byte_count
                    return
                delay = (byte_count - self._allowance) / self.bytes_per_second
            if self.stop_controller:
                self.stop_controller.wait(delay)
            else:
                self._wait(delay)

    def _refill(self, now: float) -> None:
        elapsed = max(now - self._updated_at, 0.0)
        self._updated_at = max(now, self._updated_at)
        self._allowance = min(
            float(self.chunk_size), self._allowance + elapsed * self.bytes_per_second
        )


class ThrottledBody:
    """Pace a file or iterable request body without changing its bytes."""

    def __init__(self, body: Any, limiter: Limiter) -> None:
        self.body = body
        self.limiter = limiter

    def __iter__(self) -> Iterator[bytes]:
        if isinstance(self.body, bytes):
            yield from self._split((self.body,))
            return
        read = getattr(self.body, "read", None)
        if callable(read):
            while True:
                requested = self.limiter.chunk_size
                self.limiter.acquire(requested)
                chunk = read(requested)
                if not chunk:
                    self.limiter.refund(requested)
                    return
                self.limiter.refund(requested - len(chunk))
                yield chunk
            return
        yield from self._split(self.body)

    def _split(self, chunks: Iterable[bytes]) -> Iterator[bytes]:
        for chunk in chunks:
            for offset in range(0, len(chunk), self.limiter.chunk_size):
                part = chunk[offset : offset + self.limiter.chunk_size]
                self.limiter.acquire(len(part))
                yield part


class BandwidthHTTPAdapter(HTTPAdapter):
    """Pace non-empty binary PUT bodies at the transport boundary."""

    def __init__(self, limiter: Limiter, **kwargs: Any) -> None:
        self.limiter = limiter
        super().__init__(**kwargs)

    def send(self, request: PreparedRequest, *args: Any, **kwargs: Any) -> Response:
        if _is_media_put(request):
            request.body = ThrottledBody(request.body, self.limiter)
        return super().send(request, *args, **kwargs)


def _is_media_put(request: PreparedRequest) -> bool:
    content_length = request.headers.get("Content-Length") if request.headers else None
    return (
        request.method == "PUT"
        and request.body is not None
        and request.headers is not None
        and request.headers.get("Content-Type") == "application/octet-stream"
        and isinstance(content_length, str)
        and content_length.isdecimal()
        and int(content_length) > 0
    )
