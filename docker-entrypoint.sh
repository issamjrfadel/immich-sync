#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/app/data}"
mkdir -p "$DATA_DIR" "$DATA_DIR/.icloud_session" "$DATA_DIR/.temp" "$DATA_DIR/.amazon_browser_profile"

# Restrict permissions on sensitive configuration, sessions, and staging folders
chmod 700 "$DATA_DIR" "$DATA_DIR/.icloud_session" "$DATA_DIR/.temp" "$DATA_DIR/.amazon_browser_profile" 2>/dev/null || true
if [ -f "$DATA_DIR/config.json" ]; then
  chmod 600 "$DATA_DIR/config.json" 2>/dev/null || true
fi
if [ -f "$DATA_DIR/.env" ]; then
  chmod 600 "$DATA_DIR/.env" 2>/dev/null || true
fi

# Ensure default relative paths in copied config.json / browser profile persist inside $DATA_DIR
if [ ! -e /app/.icloud_session ]; then
  ln -s "$DATA_DIR/.icloud_session" /app/.icloud_session
fi
if [ ! -e /app/.temp ]; then
  ln -s "$DATA_DIR/.temp" /app/.temp
fi
if [ ! -e /app/.amazon_browser_profile ]; then
  ln -s "$DATA_DIR/.amazon_browser_profile" /app/.amazon_browser_profile
fi

# Optional virtual display (Xvfb + x11vnc + noVNC) for interactive Playwright Amazon login
if [ "${ENABLE_NOVNC:-false}" = "true" ] && command -v Xvfb >/dev/null 2>&1; then
  DISPLAY="${DISPLAY:-:99}"
  export DISPLAY
  NOVNC_PORT="${NOVNC_PORT:-6090}"
  DISP_NUM="${DISPLAY#:}"
  rm -f "/tmp/.X${DISP_NUM}-lock" 2>/dev/null || true
  Xvfb "$DISPLAY" -screen 0 1280x800x24 -ac +extension GLX +render -noreset >/tmp/xvfb.log 2>&1 &

  i=0
  while [ ! -S "/tmp/.X11-unix/X${DISP_NUM}" ] && [ "$i" -lt 25 ]; do
    sleep 0.2
    i=$((i + 1))
  done

  if [ -z "${DASHBOARD_PASSWORD:-}" ] && [ -f "$DATA_DIR/.env" ]; then
    DASHBOARD_PASSWORD="$(python -c "from dotenv import dotenv_values; print((dotenv_values('$DATA_DIR/.env').get('DASHBOARD_PASSWORD') or '').strip())" 2>/dev/null || true)"
  fi

  if [ -n "${DASHBOARD_PASSWORD:-}" ]; then
    x11vnc -storepasswd "$DASHBOARD_PASSWORD" /tmp/.vncpass >/dev/null 2>&1
    chmod 600 /tmp/.vncpass 2>/dev/null || true
    x11vnc -display "$DISPLAY" -forever -shared -localhost -rfbport 5900 -rfbauth /tmp/.vncpass -bg -o /tmp/x11vnc.log >/dev/null 2>&1 || true
  else
    x11vnc -display "$DISPLAY" -forever -shared -localhost -rfbport 5900 -nopw -bg -o /tmp/x11vnc.log >/dev/null 2>&1 || true
  fi

  if command -v websockify >/dev/null 2>&1 && [ -d /usr/share/novnc ]; then
    websockify --web=/usr/share/novnc "$NOVNC_PORT" 127.0.0.1:5900 >/tmp/novnc.log 2>&1 &
  fi
fi

exec "$@"
