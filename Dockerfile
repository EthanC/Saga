FROM denoland/deno:alpine-2.5.2 AS deno

FROM ghcr.io/astral-sh/uv:0.12.19 AS uv

FROM python:3.14-alpine AS builder

RUN apk add --no-cache build-base
COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.14-alpine

RUN apk add --no-cache ffmpeg libstdc++ su-exec \
    && addgroup -g 1000 saga \
    && adduser -D -H -u 1000 -G saga saga \
    && mkdir -p /app /config /data \
    && chown -R saga:saga /app /config /data

COPY --from=deno /bin/deno /opt/deno/bin/deno
COPY --from=deno /usr/local/lib/ld-linux* /usr/local/lib/
COPY --from=deno /usr/local/lib /opt/deno/lib
COPY --chmod=755 docker-deno.sh /usr/local/bin/deno
RUN case "$(apk --print-arch)" in \
        x86_64) loader=/lib64/ld-linux-x86-64.so.2 ;; \
        aarch64) loader=/lib/ld-linux-aarch64.so.1 ;; \
    esac \
    && mkdir -p "$(dirname "$loader")" \
    && ln -s "/usr/local/lib/$(basename "$loader")" "$loader"
COPY --from=builder --chown=saga:saga /app/.venv /app/.venv
COPY --chown=saga:saga saga.py README.md LICENSE /app/
COPY --chown=saga:saga core /app/core
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PUID=1000 \
    PGID=1000 \
    SAGA_CONFIG_PATH=/config/config.toml \
    SAGA_DATA_PATH=/data \
    SAGA_HEALTHCHECK_FILE=/data/healthy

WORKDIR /app
VOLUME ["/config", "/data"]
HEALTHCHECK --interval=1m --timeout=5s --start-period=2m --retries=3 \
    CMD ["python", "-m", "core.healthcheck"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python", "saga.py"]
