FROM denoland/deno:alpine-2.9.8 AS deno

FROM ghcr.io/astral-sh/uv:0.13.7 AS uv

FROM alpine:3.24 AS python

COPY --from=uv /uv /usr/local/bin/uv
RUN uv python install --install-dir /opt/python 3.15.2 \
    && ln -s "$(UV_PYTHON_INSTALL_DIR=/opt/python uv python find --managed-python 3.15.2)" /usr/local/bin/python

FROM python AS builder

RUN apk add --no-cache build-base clang compiler-rt
ENV CC=clang CXX=clang++

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache \
    && .venv/bin/python -c "import urllib3, urllib3_future, requests, niquests; from yt_dlp.networking import _requests; assert requests.packages.urllib3 is urllib3; assert _requests.urllib3 is urllib3; assert niquests.packages.urllib3 is urllib3_future; assert urllib3.__file__ != urllib3_future.__file__"

FROM alpine:3.24

COPY --from=python /opt/python /opt/python
COPY --from=python /usr/local/bin/python /usr/local/bin/python

RUN apk add --no-cache ffmpeg libstdc++ su-exec \
    && addgroup -g 1000 saga \
    && adduser -D -H -u 1000 -G saga saga \
    && mkdir -p /app /config /data /home/saga/.cache/deno \
    && chown -R saga:saga /app /config /data /home/saga

COPY --from=deno /bin/deno /opt/deno/bin/deno
COPY --from=deno /usr/local/lib/glibc /opt/deno/lib
COPY --chmod=755 docker-deno.sh /usr/local/bin/deno
RUN case "$(apk --print-arch)" in \
        x86_64) loader=/lib64/ld-linux-x86-64.so.2 ;; \
        aarch64) loader=/lib/ld-linux-aarch64.so.1 ;; \
    esac \
    && mkdir -p "$(dirname "$loader")" \
    && ln -s "/opt/deno/lib/$(basename "$loader")" "$loader"
COPY --from=builder --chown=saga:saga /app/.venv /app/.venv
COPY --chown=saga:saga saga.py README.md LICENSE /app/
COPY --chown=saga:saga core /app/core
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    HOME=/home/saga \
    XDG_CACHE_HOME=/home/saga/.cache \
    DENO_DIR=/home/saga/.cache/deno \
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
