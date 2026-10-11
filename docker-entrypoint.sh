#!/bin/sh
set -eu

case "$PUID:$PGID" in
    *[!0-9:]* | :* | *: | *:*:*)
        echo "PUID and PGID must be numeric" >&2
        exit 1
        ;;
esac

chown -R "$PUID:$PGID" /data /home/saga
exec su-exec "$PUID:$PGID" "$@"
