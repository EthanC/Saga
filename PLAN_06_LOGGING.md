# Plan 06: Bounded logs and usable downloader diagnostics

Priority: P2. Change types: deployment configuration and application code.

## Findings

The deployment's captured output contained approximately 629,000 carriage-return progress updates and 38.6 MB of output since container creation. `quiet=True` does not suppress yt-dlp's progress display in this embedded use; `noprogress` is unset. Both containers use Docker's `json-file` driver with no rotation options. Production also sets `LOG_LEVEL=DEBUG`, while yt-dlp warnings are suppressed with `no_warnings=True`.

The target is bounded storage with one readable record per event and enough upstream context to investigate failures.

## Deployment configuration changes

1. Set Saga's ordinary production level to `LOG_LEVEL=INFO`. Retain `LOG_DISCORD_LEVEL=WARNING` for actionable application warnings.
2. Add bounded logging to both Compose services. Proposed initial values:

   ```yaml
   logging:
     driver: json-file
     options:
       max-size: "10m"
       max-file: "3"
   ```

3. Verify the resulting container `HostConfig.LogConfig` after recreation. Editing Compose does not change the logging configuration of an existing container.
4. Coordinate service recreation with the pinned-image deployment in plan 07 and the shutdown behavior in plan 08. Retain the relevant audit-era error records before the old container is replaced.

Changing `LOG_LEVEL` alone will not eliminate progress bars because those messages bypass Saga's Loguru level.

## Application code changes

Primary files: `core/youtube.py`, `core/logging.py`, and their tests.

- Set `noprogress=True` for yt-dlp operations. Keep progress hooks enabled for reservations and health metrics.
- Provide a small yt-dlp logger adapter. Route useful warnings and errors through Saga's logging and redaction path; send extractor debug chatter only at DEBUG.
- Replace the blanket warning suppression with classified handling. Preserve warnings that explain format selection, PO-token failures, authentication, or extractor changes. Deduplicate repeated copies within one operation and across bounded time windows where appropriate.
- Redact complete Discord webhook URLs, authentication headers, cookies, signed query values, and generated tokens before any console or Discord sink receives downloader diagnostics. Avoid logging request/response bodies as routine failure context.
- Ensure a single download failure does not produce several Discord messages from the adapter, exception classifier, and archive processor. Emit one application warning containing the video ID, phase, attempt, retry decision, and safe cause.
- Use structured periodic transfer summaries or `transfers.json` for progress. Do not replace the progress bar with a log line for every callback.

The provider's own log output also included generated token values. Check whether the pinned provider supports a quiet/log-level option. Configure that option if available; otherwise record an upstream issue or maintain a reviewed provider change separately. Saga's logger cannot redact a different container's stdout.

## Verification

Run a downloader fixture with many progress callbacks and one injected error. Progress metrics must advance without carriage-return output. The error should produce one useful warning and a persisted retry reason.

Extend `tests/test_logging.py` and downloader tests with realistic redaction fixtures, including URLs containing credentials and multiline exceptions. Verify that no tested secret appears in either the captured console or Discord sink. Confirm important warnings remain visible at INFO/WARNING.

For configuration, render Compose and inspect the recreated containers. Exercise rotation in a disposable logging fixture rather than filling the production disk to reach the limit.

## Acceptance and rollback

Repository implementation: yt-dlp progress suppression, deduplicated diagnostics, sink-level credential/query/token redaction, and single processor retry warnings. README includes rotation for both services and INFO/WARNING settings. Reviewed provider 2.0.0 `server/src/main.ts` and `session_manager.ts`: token output exists and no quiet CLI option is exposed, so removing provider token stdout requires a separate reviewed provider release. Production log rotation/volume checks remain operator work.

- Both services have the intended rotation settings.
- Saga emits no terminal progress updates during a normal download.
- Error context identifies the failing video and retry decision without including credentials.
- Log volume per transferred GB declines relative to the audit baseline, and the next real format/network failure remains diagnosable.

Retain the logging limits if diagnostic verbosity needs to return temporarily to DEBUG. Roll back the adapter through the previous pinned image if it obscures failures; do not restore unbounded logging as part of that rollback.

Related plans: [transport and retries](PLAN_03_HTTP_TRANSPORT_AND_RETRIES.md), [deployment configuration](PLAN_07_DEPLOYMENT_CONFIGURATION.md), and [pipeline health](PLAN_09_PIPELINE_HEALTH.md).
