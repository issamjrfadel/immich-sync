import time
import logging
import threading
from typing import Callable, Optional

logger = logging.getLogger("immich_sync.events")

IMMICH_TRIGGER_EVENTS = (
    "on_upload_success",
    "on_asset_trash",
    "on_asset_delete",
    "on_asset_restore",
    "on_asset_update",
)


class ImmichEventWatcher:
    """
    Listens to Immich's Socket.IO WebSocket stream (/api/socket.io) and triggers
    a debounced sync callback whenever assets are uploaded, trashed, deleted,
    restored, or updated.
    """

    def __init__(
        self,
        immich_url: str,
        api_key: str,
        verify_ssl: bool = True,
        debounce_seconds: int = 15,
        on_trigger: Optional[Callable[[str], None]] = None,
    ):
        self.immich_url = immich_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.debounce_seconds = max(0, int(debounce_seconds))
        self.on_trigger = on_trigger

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._timer_lock = threading.Lock()
        self._debounce_timer: Optional[threading.Timer] = None
        self._sio = None

        self.is_connected: bool = False
        self.last_event_name: Optional[str] = None
        self.last_event_time: Optional[str] = None
        self.last_error: Optional[str] = None

    def schedule_trigger(self, event_name: str) -> None:
        """Debounce rapid event bursts before invoking on_trigger."""
        self.last_event_name = event_name
        self.last_event_time = time.strftime("%Y-%m-%d %H:%M:%S")

        if self._stop_event.is_set() or not self.on_trigger:
            return

        if self.debounce_seconds == 0:
            self._fire_trigger(event_name)
            return

        with self._timer_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
            timer = threading.Timer(self.debounce_seconds, self._fire_trigger, args=(event_name,))
            timer.daemon = True
            self._debounce_timer = timer
            timer.start()

    def _fire_trigger(self, event_name: str) -> None:
        if self._stop_event.is_set() or not self.on_trigger:
            return
        try:
            self.on_trigger(event_name)
        except Exception as e:
            logger.warning(f"Immich event trigger callback error ({event_name}): {e}")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._timer_lock:
            if self._debounce_timer is not None:
                self._debounce_timer.cancel()
                self._debounce_timer = None
        if self._sio is not None:
            try:
                self._sio.disconnect()
            except Exception:
                pass
        self.is_connected = False

    def _build_client(self):
        import socketio

        sio = socketio.Client(
            ssl_verify=self.verify_ssl,
            reconnection=True,
            reconnection_delay=5,
            reconnection_delay_max=60,
            logger=False,
            engineio_logger=False,
        )

        @sio.event
        def connect():
            self.is_connected = True
            self.last_error = None
            logger.info(f"Connected to Immich real-time events at {self.immich_url}/api/socket.io")

        @sio.event
        def disconnect():
            self.is_connected = False
            if not self._stop_event.is_set():
                logger.debug("Disconnected from Immich real-time events; will reconnect.")

        @sio.event
        def connect_error(data):
            self.is_connected = False
            self.last_error = str(data)
            logger.debug(f"Immich Socket.IO connect error: {data}")

        for evt in IMMICH_TRIGGER_EVENTS:
            def _make_handler(name: str):
                def _handler(data=None):
                    logger.info(f"Immich real-time event received: {name} (debouncing {self.debounce_seconds}s)")
                    self.schedule_trigger(name)
                return _handler
            sio.on(evt, _make_handler(evt))

        return sio

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                sio = self._build_client()
                self._sio = sio
                sio.connect(
                    self.immich_url,
                    socketio_path="/api/socket.io",
                    headers={"x-api-key": self.api_key},
                    transports=["websocket", "polling"],
                    wait_timeout=15,
                )
                while not self._stop_event.is_set() and sio.connected:
                    sio.sleep(1)
            except Exception as e:
                self.is_connected = False
                self.last_error = str(e)
                logger.debug(f"Immich event watcher reconnect loop: {e}")
            finally:
                self.is_connected = False
                if self._sio is not None:
                    try:
                        self._sio.disconnect()
                    except Exception:
                        pass
                    self._sio = None

            for _ in range(15):
                if self._stop_event.is_set():
                    break
                time.sleep(1)
