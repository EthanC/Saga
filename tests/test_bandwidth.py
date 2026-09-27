from __future__ import annotations

from io import BytesIO
from threading import Thread
from types import SimpleNamespace
from typing import Any

import pytest
from niquests import Request
from niquests.adapters import HTTPAdapter

from core.bandwidth import (
    BandwidthHTTPAdapter,
    ByteLimiter,
    ThrottledBody,
    _is_media_put,
)


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def clock(self) -> float:
        return self.now

    def wait(self, duration: float) -> None:
        self.waits.append(duration)
        self.now += duration


def test_unlimited_limiter_has_no_wait_or_accounting() -> None:
    fake = FakeTime()
    limiter = ByteLimiter(0, clock=fake.clock, wait=fake.wait)

    limiter.acquire(1_000_000)
    limiter.refund(10)

    assert limiter.enabled is False
    assert fake.waits == []


def test_limiter_refills_bounds_credit_and_splits_large_requests() -> None:
    fake = FakeTime()
    limiter = ByteLimiter(10, clock=fake.clock, wait=fake.wait)

    limiter.acquire(10)
    fake.now = 100
    limiter.acquire(25)

    assert fake.waits == [1.0, 0.5]
    assert fake.now == 101.5


def test_limiter_refunds_short_reservations_and_rejects_negative_values() -> None:
    fake = FakeTime()
    with pytest.raises(ValueError, match="non-negative"):
        ByteLimiter(-1)
    limiter = ByteLimiter(10, clock=fake.clock, wait=fake.wait)
    with pytest.raises(ValueError, match="non-negative"):
        limiter.acquire(-1)

    limiter.acquire(10)
    limiter.refund(4)
    limiter.acquire(4)

    assert fake.waits == []


def test_concurrent_threads_share_one_allowance() -> None:
    fake = FakeTime()
    limiter = ByteLimiter(10, clock=fake.clock, wait=fake.wait)
    limiter.acquire(10)
    threads = [Thread(target=limiter.acquire, args=(10,)) for _ in range(2)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sum(fake.waits) == pytest.approx(2.0)


def test_independent_limiters_keep_separate_budgets() -> None:
    fake = FakeTime()
    download = ByteLimiter(10, clock=fake.clock, wait=fake.wait)
    upload = ByteLimiter(10, clock=fake.clock, wait=fake.wait)

    download.acquire(10)
    upload.acquire(10)

    assert fake.waits == []


def test_throttled_body_streams_files_and_iterables_without_changing_bytes() -> None:
    fake = FakeTime()
    file_limiter = ByteLimiter(3, clock=fake.clock, wait=fake.wait)
    iterable_limiter = ByteLimiter(2, clock=fake.clock, wait=fake.wait)

    file_chunks = list(ThrottledBody(BytesIO(b"abcde"), file_limiter))
    iterable_chunks = list(ThrottledBody([b"abc", b"de"], iterable_limiter))
    byte_chunks = list(ThrottledBody(b"abcde", iterable_limiter))

    assert b"".join(file_chunks) == b"abcde"
    assert b"".join(iterable_chunks) == b"abcde"
    assert b"".join(byte_chunks) == b"abcde"
    assert max(map(len, file_chunks + iterable_chunks + byte_chunks)) <= 3


@pytest.mark.parametrize(
    ("method", "headers", "body", "expected"),
    [
        (
            "PUT",
            {"Content-Type": "application/octet-stream", "Content-Length": "3"},
            b"abc",
            True,
        ),
        (
            "POST",
            {"Content-Type": "application/octet-stream", "Content-Length": "3"},
            b"abc",
            False,
        ),
        (
            "PUT",
            {"Content-Type": "application/json", "Content-Length": "3"},
            b"abc",
            False,
        ),
        (
            "PUT",
            {"Content-Type": "application/octet-stream", "Content-Length": "0"},
            b"",
            False,
        ),
        (
            "PUT",
            {"Content-Type": "application/octet-stream", "Content-Length": "bad"},
            b"abc",
            False,
        ),
        ("PUT", {}, None, False),
    ],
)
def test_media_put_detection(
    method: str, headers: dict[str, str], body: bytes | None, expected: bool
) -> None:
    request = Request(
        method, "https://example.com/file", headers=headers, data=body
    ).prepare()
    if headers.get("Content-Length") == "bad":
        assert request.headers is not None
        request.headers["Content-Length"] = "bad"

    assert _is_media_put(request) is expected


def test_adapter_wraps_only_media_puts(monkeypatch: pytest.MonkeyPatch) -> None:
    bodies: list[Any] = []

    def send(
        self: HTTPAdapter, request: Any, *args: Any, **kwargs: Any
    ) -> SimpleNamespace:
        bodies.append(request.body)
        return SimpleNamespace()

    monkeypatch.setattr(HTTPAdapter, "send", send)
    adapter = BandwidthHTTPAdapter(ByteLimiter(10), max_retries=0)
    media = Request(
        "PUT",
        "https://example.com/file",
        headers={"Content-Type": "application/octet-stream", "Content-Length": "3"},
        data=b"abc",
    ).prepare()
    control = Request("GET", "https://example.com/status").prepare()

    adapter.send(media)
    adapter.send(control)

    assert isinstance(bodies[0], ThrottledBody)
    assert b"".join(bodies[0]) == b"abc"
    assert bodies[1] is None
