import json
import pickle
from dataclasses import replace
from datetime import timedelta
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socket import gaierror
from threading import Thread
from typing import Any
from urllib.error import URLError

import pytest
from sqlmodel import Session

from core import archival
from core.archival import ArchiveProcessor, SubmissionUncertainError, submit_ziggy
from core.state import ArchiveJob, SchemaMigration, StateStore, utcnow
from core.webpage_errors import SubmissionNotSentError, legacy_ziggy_not_sent
from core.youtube import YouTubeClient
from tests.test_archival import FakeGateway, run_job, saved, seed_upload, settings


@pytest.mark.parametrize(
    "error,expected",
    [
        (ConnectionRefusedError(111, "refused"), SubmissionNotSentError),
        (URLError(ConnectionRefusedError(111, "refused")), SubmissionNotSentError),
        (gaierror(-2, "name not known"), SubmissionNotSentError),
        (URLError(gaierror(-2, "name not known")), SubmissionNotSentError),
        (URLError(TimeoutError("timed out")), SubmissionUncertainError),
        (ConnectionResetError("reset"), SubmissionUncertainError),
        (HTTPException("unknown transport phase"), SubmissionUncertainError),
    ],
)
def test_only_known_pre_submission_failures_are_retryable(
    monkeypatch: pytest.MonkeyPatch, error: Exception, expected: type[Exception]
) -> None:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise error

    monkeypatch.setattr(archival, "urlopen", fail)
    with pytest.raises(expected) as raised:
        submit_ziggy("http://ziggy", "saga", "https://example.com", 1)
    restored = pickle.loads(pickle.dumps(raised.value))
    assert isinstance(restored, expected)


def test_refused_submission_retries_after_restart_and_saves_one_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts: list[str] = []

    class Reply:
        status = 202

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def read(self) -> bytes:
            return json.dumps(
                {
                    "submissions": [
                        {"receipt_id": "receipt", "url": "https://example.com/video"}
                    ]
                }
            ).encode()

    def open_request(*args: Any, **kwargs: Any) -> Any:
        attempts.append("attempt")
        if len(attempts) == 1:
            raise URLError(ConnectionRefusedError(111, "refused"))
        return Reply()

    monkeypatch.setattr(archival, "urlopen", open_request)
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    video_id, _, frozen = seed_upload(store, tmp_path)
    # Use the persisted source, as production receipt validation does.
    source = frozen.source_url
    monkeypatch.setattr(
        Reply,
        "read",
        lambda self: json.dumps(
            {"submissions": [{"receipt_id": "receipt", "url": source}]}
        ).encode(),
    )
    configured = replace(settings(tmp_path), ziggy_host="http://ziggy")
    operation = ArchiveProcessor(configured, store, YouTubeClient(), FakeGateway())
    work = store.claim_archive(lane="upload", prefer_recent=False)
    assert work is not None
    operation.process(work)
    run_job(store, operation, "webpage")
    with Session(store.engine) as session:
        job = session.get(ArchiveJob, (video_id, "webpage"))
        assert job is not None and job.status == "waiting" and job.retry_at
        assert job.error_kind == "network" and job.error_phase == "webpage-connect"
    assert saved(store, video_id).webpage_status == "pending"
    store.close()
    store = StateStore(path)
    store.reset_interrupted_claims()
    assert store.claim_background("webpage") is None
    operation = ArchiveProcessor(configured, store, YouTubeClient(), FakeGateway())
    run_job(store, operation, "webpage", now=utcnow() + timedelta(minutes=10))
    assert len(attempts) == 2 and saved(store, video_id).webpage_status == "queued"
    assert saved(store, video_id).ziggy_receipt
    assert store.claim_background("webpage", now=utcnow() + timedelta(hours=1)) is None
    store.close()


def test_submission_redirect_is_not_followed_or_retried() -> None:
    requests: list[str] = []

    class Ziggy(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(self.path)
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(307)
            self.send_header("Location", "/redirected")
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Ziggy) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with pytest.raises(SubmissionUncertainError, match="redirected"):
                submit_ziggy(
                    f"http://127.0.0.1:{server.server_port}",
                    "saga",
                    "https://example.com",
                    1,
                )
            assert requests == ["/v1/queue"]
        finally:
            server.shutdown()
            thread.join()


def test_legacy_refusal_migration_does_not_resubmit_uncertain_or_receipted_jobs(
    tmp_path: Path,
) -> None:
    old = (
        "SubmissionUncertainError: Ziggy response was lost: "
        "<urlopen error [Errno 111] Connection refused>"
    )
    path = tmp_path / "saga.sqlite"
    store = StateStore(path)
    cases = [
        (old, None, None, "waiting"),
        (None, None, None, "failed"),
        (old, "receipt", None, "failed"),
        (old, None, "wayback-job", "failed"),
        (
            "SubmissionUncertainError: Ziggy returned an invalid receipt",
            None,
            None,
            "failed",
        ),
    ]
    for index, (detail, receipt, wayback_id, _) in enumerate(cases):
        video_id, _, _ = seed_upload(store, tmp_path, f"Legacy{index:05}")
        store.update_archive(
            video_id,
            webpage_status="uncertain",
            ziggy_receipt=receipt,
            wayback_job_id=wayback_id,
        )
        with Session(store.engine) as session:
            session.add(
                ArchiveJob(
                    video_id=video_id,
                    kind="webpage",
                    status="failed",
                    attempts=40,
                    last_error=detail,
                    deadline_at=utcnow() - timedelta(days=1),
                )
            )
            session.commit()
    with Session(store.engine) as session:
        migration = session.get(SchemaMigration, 4)
        assert migration is not None
        session.delete(migration)
        session.commit()
    store.close()
    store = StateStore(path)
    with Session(store.engine) as session:
        for index, (_, _, _, expected) in enumerate(cases):
            job = session.get(ArchiveJob, (f"Legacy{index:05}", "webpage"))
            assert job is not None and job.status == expected
        eligible = session.get(ArchiveJob, ("Legacy00000", "webpage"))
        assert eligible is not None and eligible.attempts == 0 and eligible.deadline_at
        assert eligible.error_phase == "webpage-connect"
    assert saved(store, "Legacy00000").webpage_status == "pending"
    assert not legacy_ziggy_not_sent("Connection refused")
    store.reset_interrupted_claims()
    assert store.claim_background("webpage") is not None
    store.close()
