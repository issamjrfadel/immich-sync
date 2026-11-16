#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/app/data}"
mkdir -p "$DATA_DIR" "$DATA_DIR/.icloud_session" "$DATA_DIR/.temp"

# Restrict permissions on sensitive configuration, sessions, and staging folders
chmod 700 "$DATA_DIR" "$DATA_DIR/.icloud_session" "$DATA_DIR/.temp" 2>/dev/null || true
if [ -f "$DATA_DIR/config.json" ]; then
  chmod 600 "$DATA_DIR/config.json" 2>/dev/null || true
fi
if [ -f "$DATA_DIR/.env" ]; then
  chmod 600 "$DATA_DIR/.env" 2>/dev/null || true
fi

# Ensure default relative paths in copied config.json persist inside $DATA_DIR
if [ ! -e /app/.icloud_session ]; then
  ln -s "$DATA_DIR/.icloud_session" /app/.icloud_session
fi
if [ ! -e /app/.temp ]; then
  ln -s "$DATA_DIR/.temp" /app/.temp
fi

exec "$@"
