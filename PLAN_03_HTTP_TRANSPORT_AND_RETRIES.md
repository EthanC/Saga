# Plan 03: HTTP transport isolation and download retries

Priority: P1. Change types: build configuration and application code.

## Observations and hypothesis

The production image contains `urllib3==2.8.0` and `urllib3-future==2.25.900`. Their installed manifests overlap at 30 paths. Hash checks confirmed that the active `urllib3` files belong to the fork. This is the fork's default installation mode and changes the transport under yt-dlp's Requests handler.

Failed downloads included QUIC stream resets and `85:Network blackhole detected`. Separating the transports is a testable response to those failures, not a demonstrated cure. Saga also omits yt-dlp's HTTP `retries` option; the installed embedded downloader treats that omission as zero retries before Saga's outer queue handles the failure.

## Build configuration changes

Use the supported source-build cohabitation mode in `pyproject.toml`:

```toml
[tool.uv]
no-binary-package = ["urllib3-future"]
extra-build-variables = { "urllib3-future" = { URLLIB3_NO_OVERRIDE = "1" } }
```

1. Apply the settings to a fresh dependency environment. A runtime environment variable cannot convert an already-installed wheel.
2. Regenerate and inspect `uv.lock` with the project's pinned uv version. Keep unrelated dependency versions stable to make the comparison attributable to the transport change.
3. Confirm the Docker builder uses the source artifact and the build flag. Avoid reusing a dependency layer or wheel cache built under the old recipe for the first verification build.
4. Verify that Requests and yt-dlp select upstream `urllib3`, while Niquests selects `urllib3_future`. Add this assertion to the image smoke checks in CI.
5. Keep the source-build policy in the repository so local installs, CI, and both supported image architectures reproduce the same namespaces.

Reference: [urllib3.future cohabitation guide](https://urllib3future.readthedocs.io/en/latest/cohabitation.html). The setting is a build choice, not a production `.env` setting.

## Application code changes

Primary files: `core/youtube.py`, `core/bandwidth.py`, and their transport tests.

- Set explicit media-download HTTP and fragment retry counts. Start with three retries per failed request/fragment, a 30-second socket inactivity timeout, and bounded exponential sleeps. These are proposed values for testing.
- Apply media retry options at `download_video()` so metadata polling does not accidentally multiply its existing outer retry policy.
- Preserve resume support and the rule that unavailable fragments are errors. A retried request must not convert an incomplete file into a completed download.
- Verify that `_LimitedResponse` preserves the exception behavior required by yt-dlp's retry manager. Test actual mid-stream disconnects with bandwidth limiting enabled, rather than only testing that an option dictionary contains `retries`.
- Bound cancellation delay across retry sleeps and network timeouts in coordination with plan 08.
- Keep Internet Archive mutating requests single-attempt at the transport layer. Their uncertain outcomes require Saga's existing persisted-operation recovery path.
- Use Niquests' own transport namespace for any adapter types that need it. Confirm the existing bandwidth adapter behaves correctly with namespace isolation.

No new runtime knobs are required for the initial retry policy. If operational tuning later needs them, add validated settings and examples in a separate explicit change.

## Verification stages

### Package selection

In the newly built image, assert separate versions and module paths for `urllib3` and `urllib3_future`. Check that `requests.packages.urllib3` resolves to the former and `niquests.packages.urllib3` to the latter. Verify image startup and a healthcheck under the normal unprivileged application UID.

### Transfer behavior

Use a local HTTP fixture that supports range requests and intentionally interrupts a response. Verify bounded retries, correct continuation, unchanged output bytes, and failure after the retry budget is exhausted. Cover throttled and unthrottled downloads, a changed remote representation, and cancellation during a retry sleep.

Run the Internet Archive HTTP-level tests in `tests/test_upload_failures.py` and bandwidth tests to detect incompatible response, exception, or adapter types. No production archive PUT is needed for these tests.

### Production comparison

Record completed bytes, transfer duration, request-level failures, outer-queue retries, and resumable bytes for comparable large videos before and after the release. Keep worker counts, rate limits, and content selection fixed. Include at least several long downloads; normal QUIC connection-age messages alone are not failed transfers.

## Acceptance and rollback

Repository implementation: source-only cohabitation recipe, client namespace assertions in both architecture builds and image smoke checks, three HTTP/fragment retries, 30-second inactivity timeout, and capped exponential sleeps. A fresh environment built `urllib3-future` from source. Regenerating `uv.lock` with uv 0.12.19 produced no dependency changes. Local range/disconnect tests cover throttled and unthrottled continuation and exhaustion. Production throughput/reliability measurement remains a release check.

Accept the packaging change when namespace selection is reproducible and both HTTP clients pass their integration checks. Accept the reliability hypothesis only if the production comparison supports it. Record a neutral or negative result rather than attributing every upstream failure to HTTP/3.

Rollback is an image change to the prior digest. Keep the current database and partial files. Do not modify packages inside the running container or try to repair the shared namespace with a runtime uninstall.

Related plans: [partial resumption](PLAN_04_PARTIAL_DOWNLOAD_RETENTION.md), [logging](PLAN_06_LOGGING.md), and [shutdown](PLAN_08_GRACEFUL_SHUTDOWN.md).
