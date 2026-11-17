FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data \
    HOST=0.0.0.0 \
    PORT=5050 \
    AUTO_DAEMON=true \
    DISPLAY=:99 \
    ENABLE_NOVNC=true \
    NOVNC_PORT=6080

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    xvfb \
    x11vnc \
    novnc \
    websockify \
    && ln -sf /usr/share/novnc/vnc.html /usr/share/novnc/index.html \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*

COPY app.py sync.py config.example.json docker-entrypoint.sh ./
COPY src/ ./src/

RUN sed -i 's/\r$//' /app/docker-entrypoint.sh && chmod +x /app/docker-entrypoint.sh \
    && mkdir -p /app/data

VOLUME ["/app/data"]
EXPOSE 5050 6080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5050/api/status', timeout=3)" || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["python", "app.py", "--host", "0.0.0.0", "--port", "5050", "--no-browser", "--daemon"]
