# Plan 07: Reproducible Compose configuration and pinned images

Priority: P2. Primary change type: deployment and build/release configuration.

## Current behavior

Production `compose.yaml` has no `environment` or `env_file` declaration. A standalone `docker compose config --format json` render returned empty environments for both services. The running containers nevertheless received project variables through Arcane's deployment path.

The provider also inherited Internet Archive credentials and Discord webhook variables. Both service image references use `latest`.

The deployment must describe its required environment explicitly and reproduce the same intended service configuration through Arcane and the Compose CLI.

## Production configuration changes

Affected directory: `/home/ethan/arcane/projects/saga`.

1. Inventory the effective running Saga settings and the source of each value. Compare secret values by presence or a local fingerprint rather than printing them. Record required defaults such as the config/data paths and cookie discovery behavior.
2. Move Saga-only variables into a service-scoped file, proposed name `.env.saga`, and reference it through Saga's `env_file`. Restrict the file to the deployment owner with permissions that the chosen deployment process can read.
3. Remove application secrets from Arcane's project-wide injection source. Confirm how the installed Arcane version handles `env_file`; adding explicit Compose values alone does not prove that project-wide variables stop reaching the provider.
4. Give `saga-pot` only the variables its documented options require. Verify the running provider contains no Internet Archive or Discord credentials after recreation.
5. Pin both service images to known digests. The audited images were:

   ```text
   ghcr.io/ethanc/saga@sha256:97eda21e8e472a055f581d4a0c26e4b2a4fb10dc7f0b44540f7c0051e1f19016
   brainicism/bgutil-ytdlp-pot-provider@sha256:ed86b6fdd5e430ddd7c8ce1adb55e1ab54db7c7dbc1bcbf3a82454a85b971164
   ```

   These identify the audited baseline, not the images that will contain the planned fixes. Record each replacement digest when promoting it.
6. Preserve the three effective bind mappings, including the nested staging mount. Keep the provider on the private Compose network and preserve its health-gated dependency while cookies remain configured.
7. Confirm how Arcane schedules image updates or container recreation for this project. Promote pinned revisions intentionally and record the previous digest with each release.

An illustrative environment declaration is:

```yaml
services:
  saga:
    env_file:
      - .env.saga
```

This is a fragment to merge into the existing service, not a replacement Compose file.

## Repository and code changes

No application Python change is needed for explicit environment injection or image pinning.

- Update README deployment instructions to show service-scoped configuration, the separate staging mount when used, and pinned-image promotion/rollback.
- Keep `.env.example` as a template with placeholders. Update `.gitignore` if needed so the chosen production environment filename cannot be committed accidentally.
- Add a reusable Compose example only if it becomes the documented source for deployment. Keep production credentials and host-specific private configuration out of that example.
- Keep CI's immutable `sha-<full-commit>` image tags. Record digests as release artifacts or deployment records; a moving `latest` tag is not a rollback reference.

## Verification procedure

1. Render the candidate Compose file with the installed CLI. Verify names, image references, mounts, healthchecks, restart policy, stop grace, logging limits, and environment key sets.
2. Compare Saga's resolved values with the running baseline, allowing only intended changes. Confirm that values containing spaces, such as the backup cron schedule, survive parsing.
3. Validate the same candidate through Arcane in an isolated deployment or its supported preview path. Check that service-scoped files are resolved from the expected project directory.
4. After production recreation, inspect effective environment key sets and image IDs. Confirm the provider has the intended restricted environment and plugin/server versions still match.
5. Verify channel polling, one ordinary transfer cycle, backup scheduling, and report delivery through the existing workflows. Avoid deliberately posting duplicate production notifications as a smoke test.

## Rollback

Retain a private copy of the prior deployment files and both image digests. Revert configuration and image references together if either deployment path resolves settings incorrectly. Use the current database when schema-compatible; database restoration follows plan 11 rather than being an automatic part of a Compose rollback.

## Acceptance

Arcane and standalone Compose resolve the same intended Saga settings. A deployment no longer depends on implicit project-wide secret injection. Each container runs the recorded digest, with the provider receiving only its own configuration.

## Repository implementation

README now documents service-scoped `.env.saga`, digest promotion and rollback, the nested staging mount, and Arcane environment-key verification. `.gitignore` already excludes `.env.saga`. Repository verification: reviewed the example against the existing Docker entrypoint and image defaults; the baseline suite passed (457 tests). Production rendering, environment isolation, image digests, and deployment time remain operator checks.

Dependencies: incorporate [log rotation](PLAN_06_LOGGING.md) and the final [shutdown grace](PLAN_08_GRACEFUL_SHUTDOWN.md) when those plans are released.
