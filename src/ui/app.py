import os
import re
import copy
import json
import time
import base64
import secrets
import logging
import asyncio
import threading
from pathlib import Path
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv, dotenv_values
from pydantic import BaseModel

from src.config import load_config, save_config, AppConfig
from src.database import SyncDatabase
from src.engine import SyncEngine
from src.events import ImmichEventWatcher
from src.schedule import parse_schedule_times, next_scheduled_run
from src.notifications import maybe_send_daily_summary, maybe_send_error_alert, send_test_notification
from src.clients.immich import ImmichClient
from src.clients.icloud import ICloudClient
from src.clients.amazon import AmazonPhotosClient, amazon_cookie_fingerprint

logger = logging.getLogger("immich_sync")

# In-memory log buffer & broadcaster
class WebSocketLogHandler(logging.Handler):
    def __init__(self, maxlen=300):
        super().__init__()
        self.buffer = deque(maxlen=maxlen)
        self.active_connections: List[WebSocket] = []
        self._lock = threading.Lock()
        self._loop = None

    def set_loop(self, loop):
        self._loop = loop

    def emit(self, record):
        log_entry = {
            "timestamp": time.strftime("%H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name
        }
        with self._lock:
            self.buffer.append(log_entry)

        if self._loop and self.active_connections:
            asyncio.run_coroutine_threadsafe(self.broadcast(log_entry), self._loop)

    async def broadcast(self, message: dict):
        dead_connections = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                dead_connections.append(connection)
        for dead in dead_connections:
            if dead in self.active_connections:
                self.active_connections.remove(dead)

    def register(self, ws: WebSocket):
        self.active_connections.append(ws)

    def unregister(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)

log_broadcaster = WebSocketLogHandler()
log_broadcaster.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logging.getLogger().addHandler(log_broadcaster)

# Global background worker state
sync_state = {
    "is_syncing": False,
    "is_paused": False,
    "last_sync_time": None,
    "last_stats": None,
    "daemon_running": False,
    "daemon_thread": None,
    "stop_daemon_event": threading.Event(),
    "trigger_sync_event": threading.Event(),
    "stop_sync_event": threading.Event(),
    "pause_sync_event": threading.Event(),
    "event_watcher": None,
    "pending_icloud_client": None,
    "amazon_auth_paused": False,
    "auth_paused_reason": None,
}

# Live Sync Progress Tracker
sync_progress = {
    "active": False,
    "percent": 0,
    "phase": "idle",
    "phase_name": "Ready",
    "current": 0,
    "total": 0,
    "item_name": "",
    "action": "Ready for sync",
    "last_updated": None
}

def handle_sync_progress(p: dict):
    sync_progress.update(p)
    sync_progress["active"] = True
    sync_progress["last_updated"] = time.strftime("%H:%M:%S")
    if log_broadcaster._loop and log_broadcaster.active_connections:
        asyncio.run_coroutine_threadsafe(
            log_broadcaster.broadcast({"type": "progress", "data": sync_progress}),
            log_broadcaster._loop
        )

# Fast in-memory service status cache (never blocks /api/status)
service_status_cache = {
    "immich": {"configured": False, "connected": False, "details": "Not tested yet"},
    "icloud": {"configured": False, "connected": False, "requires_2fa": False, "details": "Not tested yet"},
    "amazon": {"configured": False, "connected": False, "details": "Not tested yet"}
}

SECRET_MASK = "********"
SECRET_COOKIE_PREFIXES = ("at-", "sess-at-", "sst-", "x-acb")

def _is_secret_cookie_key(k: str) -> bool:
    kl = k.lower()
    return kl.startswith(SECRET_COOKIE_PREFIXES) or kl == "session-token"

def _clean_secret(val: Optional[str]) -> Optional[str]:
    if val is None or val == "" or val == SECRET_MASK:
        return None
    return val

def _mask_config_dict(data: dict) -> dict:
    masked = copy.deepcopy(data)
    if masked.get("immich", {}).get("api_key"):
        masked["immich"]["api_key"] = SECRET_MASK
    if masked.get("icloud", {}).get("password"):
        masked["icloud"]["password"] = SECRET_MASK
    cookies = masked.get("amazon", {}).get("cookies")
    if isinstance(cookies, dict):
        for k, v in list(cookies.items()):
            if v and _is_secret_cookie_key(k):
                cookies[k] = SECRET_MASK
    apprise_urls = masked.get("notifications", {}).get("apprise_urls")
    if isinstance(apprise_urls, list) and apprise_urls:
        masked["notifications"]["apprise_urls"] = [SECRET_MASK for u in apprise_urls if u]
    if masked.get("sync", {}).get("webhook_secret"):
        masked["sync"]["webhook_secret"] = SECRET_MASK
    return masked

def sync_test_service(service: str, payload: Optional[dict], cfg: Optional[AppConfig]) -> dict:
    import requests
    if service == "immich":
        url = (payload.get("url") if payload else None) or (cfg.immich.url if cfg else None)
        raw_key = payload.get("api_key") if payload else None
        api_key = _clean_secret(raw_key) or (cfg.immich.api_key if cfg else None)
        verify_ssl = (payload.get("verify_ssl") if payload and "verify_ssl" in payload else (cfg.immich.verify_ssl if cfg else True))

        if not url or not api_key:
            return {"success": False, "message": "Server URL and API Key are required to test Immich."}

        try:
            from src.config import ImmichConfig
            test_cfg = ImmichConfig(url=url, api_key=api_key, verify_ssl=verify_ssl)
            client = ImmichClient(test_cfg)
            user = client.test_connection()
            email = user.get("email", "User")
            msg = f"Connected as {email} ({url})"
            service_status_cache["immich"] = {"configured": True, "connected": True, "details": msg}
            return {"success": True, "message": msg}
        except requests.exceptions.ConnectionError:
            msg = f"Connection refused or host unreachable: {url}"
            service_status_cache["immich"] = {"configured": True, "connected": False, "details": msg}
            return {"success": False, "message": msg}
        except requests.exceptions.Timeout:
            msg = f"Connection timed out (host not responding): {url}"
            service_status_cache["immich"] = {"configured": True, "connected": False, "details": msg}
            return {"success": False, "message": msg}
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", "Error")
            if code in (401, 403):
                msg = f"Auth failed ({code}): Invalid Immich API Key"
            else:
                msg = f"HTTP {code}: {e}"
            service_status_cache["immich"] = {"configured": True, "connected": False, "details": msg}
            return {"success": False, "message": msg}
        except Exception as e:
            msg = str(e)
            service_status_cache["immich"] = {"configured": True, "connected": False, "details": msg}
            return {"success": False, "message": msg}

    elif service == "icloud":
        apple_id = (payload.get("apple_id") if payload else None) or (cfg.icloud.apple_id if cfg else None)
        raw_pw = payload.get("password") if payload else None
        password = _clean_secret(raw_pw) or (cfg.icloud.password if cfg else None)
        session_dir = cfg.icloud.session_dir if cfg else "./.icloud_session"

        if not apple_id:
            return {"success": False, "message": "Apple ID email is required to test iCloud."}

        try:
            from src.config import ICloudConfig
            test_cfg = ICloudConfig(apple_id=apple_id, password=password, session_dir=session_dir)
            client = ICloudClient(test_cfg)
            client.connect(interactive_2fa=False)
            msg = f"Session valid for {apple_id}"
            service_status_cache["icloud"] = {"configured": True, "connected": True, "requires_2fa": False, "details": msg}
            return {"success": True, "message": msg}
        except RuntimeError as e:
            if "2FA" in str(e):
                if getattr(client, "api", None) is not None:
                    sync_state["pending_icloud_client"] = client.api
                    sync_state["pending_icloud_apple_id"] = apple_id
                msg = "Two-Factor Authentication required"
                service_status_cache["icloud"] = {"configured": True, "connected": False, "requires_2fa": True, "details": msg}
                return {"success": False, "requires_2fa": True, "message": msg}
            service_status_cache["icloud"] = {"configured": True, "connected": False, "requires_2fa": False, "details": str(e)}
            return {"success": False, "message": str(e)}
        except Exception as e:
            service_status_cache["icloud"] = {"configured": True, "connected": False, "requires_2fa": False, "details": str(e)}
            return {"success": False, "message": str(e)}

    elif service == "amazon":
        existing_cookies = dict(cfg.amazon.cookies) if cfg else {}
        raw_cookies = (payload.get("cookies") if payload else None) or {}
        if raw_cookies:
            cookies = dict(existing_cookies)
            for k, v in raw_cookies.items():
                if v and v != SECRET_MASK:
                    cookies[k] = v
        else:
            cookies = existing_cookies
        tld = (payload.get("tld") if payload else None) or (cfg.amazon.tld if cfg else "ca")

        if not cookies or not cookies.get("session-id"):
            return {"success": False, "message": "Amazon 'session-id' cookie is required."}

        try:
            from src.config import AmazonConfig
            test_cfg = AmazonConfig(cookies=cookies, tld=tld)
            client = AmazonPhotosClient(test_cfg)
            info = client.test_connection()
            msg = f"Connected to Amazon Photos (amazon.{client.tld} | Root ID: {info.get('root_id', 'OK')[:12]}...)"
            service_status_cache["amazon"] = {"configured": True, "connected": True, "details": msg}
            return {"success": True, "message": msg, "tld": client.tld, "marketplace": info.get("marketplace")}
        except Exception as e:
            msg = str(e)
            service_status_cache["amazon"] = {"configured": True, "connected": False, "details": msg}
            return {"success": False, "message": msg}

    return {"success": False, "message": f"Unknown service '{service}'"}

class SyncRunRequest(BaseModel):
    dry_run: bool = False
    force_deletes: Optional[bool] = None
    limit: Optional[int] = None
    enable_immich: Optional[bool] = None
    enable_icloud: Optional[bool] = None
    enable_amazon: Optional[bool] = None

class ForceDeletesRequest(BaseModel):
    enabled: bool

class ToggleHostRequest(BaseModel):
    service: str
    enabled: bool

class Verify2FARequest(BaseModel):
    code: str

class AmazonCookiesPayload(BaseModel):
    tld: Optional[str] = "ca"
    cookies: Dict[str, str]

class AmazonLoginStartRequest(BaseModel):
    tld: Optional[str] = "ca"

def get_storage_warning(
    data_dir: Path,
    mountinfo_path: Path = Path("/proc/self/mountinfo"),
    dockerenv_path: Path = Path("/.dockerenv"),
) -> str:
    """Warn when running inside Docker without a persistent host bind mount at data_dir."""
    if not dockerenv_path.exists() or not mountinfo_path.is_file():
        return ""
    try:
        target = str(data_dir.resolve())
        lines = mountinfo_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return ""

    matched_root = None
    for line in lines:
        parts = line.split(" - ", 1)[0].split(" ")
        if len(parts) < 5:
            continue
        root = parts[3].replace("\\040", " ")
        mount_point = parts[4].replace("\\040", " ")
        if mount_point == target:
            matched_root = root
            break

    if matched_root is None:
        return (
            f"Storage warning: '{target}' is not mounted to a persistent host folder. "
            f"Map Host Path '/mnt/user/appdata/immich-sync' -> Container Path '{target}' so settings survive updates."
        )
    if re.search(r"(?:^|/)[0-9a-f]{64}/_data$", matched_root):
        return (
            f"Storage warning: '{target}' is using a temporary anonymous Docker volume instead of your Unraid appdata share. "
            f"Set Container Path to '{target}' and Host Path to '/mnt/user/appdata/immich-sync' so settings survive updates."
        )
    return ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_broadcaster.set_loop(asyncio.get_running_loop())
    storage_warn = getattr(app.state, "storage_warning", "")
    if storage_warn:
        logger.warning(storage_warn)
    auto_daemon = getattr(app.state, "auto_daemon", False)
    start_fn = getattr(app.state, "start_daemon_fn", None)
    if auto_daemon and callable(start_fn):
        try:
            start_fn()
        except Exception as e:
            logger.warning(f"Auto-daemon startup skipped: {e}")
    yield
    if sync_state["daemon_running"]:
        sync_state["stop_daemon_event"].set()

def create_app(
    config_path: str = "config.json",
    db_path: str = "sync_state.db",
    auto_daemon: bool = False,
    dashboard_password: Optional[str] = None,
    dashboard_user: Optional[str] = None,
) -> FastAPI:
    sibling_env = Path(config_path).parent / ".env"
    if sibling_env.is_file():
        for k, v in dotenv_values(sibling_env).items():
            if k and v and os.environ.get(k, "").strip() == "":
                os.environ.pop(k, None)
        load_dotenv(sibling_env, override=False)

    app = FastAPI(title="Immich Multi-Sync Dashboard", version="1.0.0", lifespan=lifespan)
    app.state.auto_daemon = auto_daemon
    data_dir = Path(os.environ.get("DATA_DIR", "").strip() or Path(config_path).parent)
    app.state.storage_warning = get_storage_warning(data_dir)

    def _is_basic_auth_authorized(auth_header: str) -> bool:
        expected_pass = (
            dashboard_password
            if dashboard_password is not None
            else os.environ.get("DASHBOARD_PASSWORD", "").strip()
        )
        if not expected_pass:
            return True
        expected_user = (
            dashboard_user
            if dashboard_user is not None
            else (os.environ.get("DASHBOARD_USER", "admin").strip() or "admin")
        )
        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:].strip()).decode("utf-8")
                if ":" in decoded:
                    user, pw = decoded.split(":", 1)
                    return secrets.compare_digest(user, expected_user) and secrets.compare_digest(pw, expected_pass)
            except Exception:
                return False
        return False

    @app.middleware("http")
    async def optional_basic_auth(request: Request, call_next):
        client_host = request.client.host if request.client else ""
        # Allow CORS preflight, localhost Docker healthchecks, and public app icon/favicon
        if request.method == "OPTIONS" or request.url.path in ("/icon.png", "/favicon.ico") or (
            request.url.path == "/api/status" and client_host in ("127.0.0.1", "::1", "localhost")
        ):
            return await call_next(request)

        # Allow Immich webhook calls authenticated by a configured webhook_secret
        if request.url.path == "/api/webhook/immich":
            cfg_wh = get_cfg()
            expected_secret = (cfg_wh.sync.webhook_secret or "").strip() if cfg_wh else ""
            provided_secret = (
                request.headers.get("X-Webhook-Secret", "").strip()
                or request.query_params.get("secret", "").strip()
            )
            if expected_secret and secrets.compare_digest(provided_secret, expected_secret):
                return await call_next(request)

        if not _is_basic_auth_authorized(request.headers.get("Authorization", "")):
            return Response(
                content="Unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Immich Multi-Sync Dashboard"'},
            )
        return await call_next(request)

    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^https://([a-z0-9-]+\.)?amazon\.[a-z.]+$|^http://(localhost|127\.0\.0\.1)(:\d+)?$",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_cfg() -> Optional[AppConfig]:
        try:
            return load_config(config_path)
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Error loading {config_path}: {e}")
            return None

    @app.get("/icon.png")
    @app.get("/favicon.ico")
    async def serve_app_icon():
        icon_file = Path(__file__).resolve().parent.parent.parent / "assets" / "icon.png"
        if not icon_file.is_file():
            raise HTTPException(status_code=404, detail="Icon not found")
        return FileResponse(icon_file, media_type="image/png")

    @app.get("/", response_class=HTMLResponse)
    async def serve_index():
        template_file = Path(__file__).parent / "templates" / "index.html"
        if not template_file.exists():
            return HTMLResponse("<h1>UI template not found</h1>", status_code=500)
        return HTMLResponse(template_file.read_text(encoding="utf-8"))

    @app.get("/api/status")
    async def get_system_status():
        cfg = get_cfg()
        db = SyncDatabase(db_path)
        active_assets = db.get_active_assets()
        albums = db.get_all_albums()

        # Update configured and enabled status flags without blocking network I/O
        service_status_cache["immich"]["configured"] = bool(cfg and cfg.immich.api_key)
        service_status_cache["immich"]["enabled"] = bool(cfg and cfg.sync.enable_immich and getattr(cfg.immich, "enabled", True))
        service_status_cache["icloud"]["configured"] = bool(cfg and cfg.icloud.apple_id)
        service_status_cache["icloud"]["enabled"] = bool(cfg and cfg.sync.enable_icloud and getattr(cfg.icloud, "enabled", True))
        service_status_cache["amazon"]["configured"] = bool(cfg and cfg.amazon.cookies.get("session-id"))
        service_status_cache["amazon"]["enabled"] = bool(cfg and cfg.sync.enable_amazon and getattr(cfg.amazon, "enabled", True))

        active_pause = _get_active_amazon_pause(cfg)
        sync_state["amazon_auth_paused"] = bool(active_pause)
        sync_state["auth_paused_reason"] = active_pause["reason"] if active_pause else None
        sync_mode = _effective_sync_mode(cfg)

        return {
            "is_syncing": sync_state["is_syncing"],
            "is_paused": sync_state["is_paused"],
            "amazon_auth_paused": bool(active_pause),
            "auth_paused_reason": active_pause["reason"] if active_pause else None,
            "sync_mode": sync_mode,
            "poll_interval_seconds": getattr(cfg.sync, "poll_interval_seconds", 300) if cfg else 300,
            "last_sync_time": sync_state["last_sync_time"],
            "last_stats": sync_state["last_stats"],
            "daemon_running": sync_state["daemon_running"],
            "db_stats": {
                "active_assets": len(active_assets),
                "total_albums": len(albums)
            },
            "services": service_status_cache,
            "progress": sync_progress,
            "one_way_immich_master": getattr(cfg.sync, "one_way_immich_master", True) if cfg else True,
            "force_deletes": getattr(cfg.sync, "force_deletes", False) if cfg else False,
            "schedule": {
                "times": list(getattr(cfg.sync, "scheduled_sync_times", []) or []) if cfg else [],
                "next_run": sync_state.get("next_scheduled_run") if sync_mode == "scheduled" else None,
            },
            "realtime_events": {
                "enabled": sync_mode == "realtime",
                "connected": bool(
                    sync_state.get("event_watcher")
                    and getattr(sync_state["event_watcher"], "is_connected", False)
                ),
                "last_event": getattr(sync_state.get("event_watcher"), "last_event_name", None),
                "last_event_time": getattr(sync_state.get("event_watcher"), "last_event_time", None),
            },
            "storage_warning": app.state.storage_warning,
            "config_path": str(Path(config_path).resolve()),
        }

    @app.post("/api/sync/force-deletes")
    async def toggle_force_deletes(req: ForceDeletesRequest):
        cfg = _get_or_init_cfg()
        cfg.sync.force_deletes = req.enabled
        save_config(cfg, config_path)
        logger.info(f"Force Deletions setting saved: {req.enabled}")
        return {"success": True, "force_deletes": req.enabled}

    @app.get("/api/sync/progress")
    async def get_sync_progress():
        return sync_progress

    @app.post("/api/host/toggle")
    async def toggle_host_endpoint(req: ToggleHostRequest):
        cfg = get_cfg()
        if not cfg:
            raise HTTPException(status_code=400, detail="Configuration not found")
        svc = req.service.lower().strip()
        if svc == "immich":
            cfg.immich.enabled = req.enabled
            cfg.sync.enable_immich = req.enabled
        elif svc == "icloud":
            cfg.icloud.enabled = req.enabled
            cfg.sync.enable_icloud = req.enabled
        elif svc == "amazon":
            cfg.amazon.enabled = req.enabled
            cfg.sync.enable_amazon = req.enabled
        else:
            raise HTTPException(status_code=400, detail=f"Unknown service '{req.service}'")

        save_config(cfg, config_path)
        service_status_cache[svc]["enabled"] = req.enabled
        status_word = "enabled" if req.enabled else "disabled"
        logger.info(f"Host '{svc}' has been {status_word} via Web UI.")
        return {"success": True, "service": svc, "enabled": req.enabled, "message": f"{svc.capitalize()} {status_word}."}

    def _effective_sync_mode(cfg: Optional[AppConfig]) -> str:
        if not cfg:
            return "realtime"
        mode = getattr(cfg.sync, "sync_mode", None)
        if mode in ("realtime", "polling", "scheduled"):
            return mode
        if getattr(cfg.sync, "scheduled_sync_times", []):
            return "scheduled"
        if not getattr(cfg.sync, "realtime_events_enabled", True):
            return "polling"
        return "realtime"

    def _get_or_init_cfg() -> AppConfig:
        cfg = get_cfg()
        if cfg is not None:
            return cfg
        ex_path = Path("config.example.json")
        if ex_path.exists():
            return AppConfig.model_validate(json.loads(ex_path.read_text(encoding="utf-8")))
        from src.config import ImmichConfig, ICloudConfig, AmazonConfig
        return AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key=""),
            icloud=ICloudConfig(apple_id=""),
            amazon=AmazonConfig(cookies={}, tld="ca"),
        )

    def _get_active_amazon_pause(cfg: Optional[AppConfig]) -> Optional[Dict[str, str]]:
        pause = SyncDatabase(db_path).get_amazon_auth_pause()
        if not pause:
            return None
        if cfg is not None:
            if not (cfg.amazon.enabled and cfg.sync.enable_amazon):
                return None
            current_fp = amazon_cookie_fingerprint(cfg.amazon.cookies, cfg.amazon.tld)
            if pause.get("cookie_fp") and current_fp != pause["cookie_fp"] and cfg.amazon.cookies.get("session-id"):
                return None
        return pause

    def _on_amazon_token_updated(cfg: AppConfig, source: str) -> bool:
        """Clear Amazon auth pause if cookies actually changed and immediately reprocess pending items."""
        db = SyncDatabase(db_path)
        pause = db.get_amazon_auth_pause()
        if not pause:
            sync_state["amazon_auth_paused"] = False
            sync_state["auth_paused_reason"] = None
            return False

        new_fp = amazon_cookie_fingerprint(cfg.amazon.cookies, cfg.amazon.tld)
        if not cfg.amazon.cookies.get("session-id") or (pause.get("cookie_fp") and new_fp == pause["cookie_fp"]):
            return False

        db.clear_amazon_auth_paused()
        sync_state["amazon_auth_paused"] = False
        sync_state["auth_paused_reason"] = None
        logger.info(
            f"Amazon Photos token updated via {source}. "
            "Clearing auth pause and immediately reprocessing pending items..."
        )
        if sync_state["daemon_running"]:
            sync_state["trigger_sync_event"].set()
        elif not sync_state["is_syncing"]:
            _start_manual_sync_worker(cfg.model_copy(deep=True))
        else:
            sync_state["trigger_sync_event"].set()
        return True

    def _persist_amazon_cookies(
        cookies: Dict[str, str],
        tld: str,
        details: str,
        connected: bool = False,
        source: str = "Amazon Login",
        trigger_reprocess: bool = True,
    ) -> bool:
        cfg = _get_or_init_cfg()
        cfg.amazon.tld = tld
        cfg.amazon.cookies = cookies
        cfg.amazon.enabled = True
        cfg.sync.enable_amazon = True
        save_config(cfg, config_path)
        service_status_cache["amazon"]["configured"] = bool(cookies.get("session-id"))
        service_status_cache["amazon"]["enabled"] = True
        if connected:
            service_status_cache["amazon"]["connected"] = True
        service_status_cache["amazon"]["details"] = details
        if trigger_reprocess:
            return _on_amazon_token_updated(cfg, source=source)
        SyncDatabase(db_path).clear_amazon_auth_paused()
        sync_state["amazon_auth_paused"] = False
        sync_state["auth_paused_reason"] = None
        return False

    @app.post("/api/amazon/cookies")
    async def receive_amazon_cookies(payload: AmazonCookiesPayload):
        tld = (payload.tld or "ca").lower().strip()
        reprocessing = _persist_amazon_cookies(
            payload.cookies,
            tld,
            details=f"Cookies updated via 1-Click Bookmarklet (amazon.{tld})",
            source="1-Click Bookmarklet",
        )
        logger.info(f"Fresh Amazon Photos cookies received and saved via 1-Click Bookmarklet for amazon.{tld}!")
        msg = f"Successfully synced fresh Amazon Photos cookies (amazon.{tld}) to Immich Multi-Sync!"
        if reprocessing:
            msg += " Reprocessing pending items now."
        return {"success": True, "reprocessing": reprocessing, "message": msg}

    @app.get("/api/amazon/cookie-import", response_class=HTMLResponse)
    async def cookie_import_fallback(data: str):
        try:
            parsed = json.loads(data)
            cookies = parsed.get("cookies", {})
            tld = (parsed.get("tld", "ca") or "ca").lower().strip()
            _persist_amazon_cookies(
                cookies,
                tld,
                details=f"Cookies updated via 1-Click Bookmarklet (amazon.{tld})",
                source="1-Click Bookmarklet",
            )
            logger.info("Fresh Amazon Photos cookies imported via bookmarklet redirect!")

            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Amazon Cookies Imported</title>
                <meta http-equiv="refresh" content="2;url=/" />
                <style>
                    body { background: #0f172a; color: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }
                    .box { background: #1e293b; padding: 2.5rem; border-radius: 1.25rem; border: 1px solid #334155; text-align: center; max-width: 420px; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); }
                    .icon { width: 52px; height: 52px; background: rgba(16, 185, 129, 0.2); color: #34d399; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 1.25rem auto; font-size: 26px; }
                    h2 { color: #f8fafc; margin: 0 0 0.5rem 0; font-size: 1.25rem; }
                    p { color: #94a3b8; font-size: 0.875rem; margin: 0 0 1rem 0; line-height: 1.5; }
                    .redirect { color: #64748b; font-size: 0.75rem; font-family: monospace; }
                </style>
            </head>
            <body>
                <div class="box">
                    <div class="icon">&#10003;</div>
                    <h2>Amazon Cookies Synced!</h2>
                    <p>Fresh session cookies for Amazon Photos have been saved to your configuration.</p>
                    <div class="redirect">Returning to Dashboard in 2 seconds...</div>
                </div>
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        except Exception as e:
            return HTMLResponse(f"<h2 style='color:red'>Import Failed: {e}</h2>", status_code=400)

    active_amazon_login: Dict[str, Any] = {"instance": None}

    def _novnc_status_fields() -> Dict[str, Any]:
        enabled = os.environ.get("ENABLE_NOVNC", "").lower() in ("1", "true", "yes")
        port_raw = os.environ.get("NOVNC_PORT", "6090").strip()
        return {
            "novnc_enabled": enabled,
            "novnc_port": int(port_raw) if (enabled and port_raw.isdigit()) else None,
        }

    @app.post("/api/amazon/login/start")
    async def start_amazon_login(payload: Optional[AmazonLoginStartRequest] = None):
        tld = (payload.tld if payload and payload.tld else "ca").lower().strip()
        existing = active_amazon_login.get("instance")
        if existing:
            st = existing.get_status()
            if st.get("state") in ("launching", "waiting_for_user", "verifying"):
                st.update(_novnc_status_fields())
                return {"status": "already_running", "login_status": st}

        from src.clients.amazon_login import AmazonBrowserLogin

        login_mgr = AmazonBrowserLogin(tld=tld)
        active_amazon_login["instance"] = login_mgr

        def on_success(cookies: Dict[str, str], resolved_tld: str):
            _persist_amazon_cookies(
                cookies,
                resolved_tld,
                details=f"Connected to Amazon Photos (amazon.{resolved_tld})",
                connected=True,
                source="Browser Login",
            )
            logger.info(f"Fresh Amazon session cookies captured & saved for amazon.{resolved_tld}!")
            if log_broadcaster._loop and log_broadcaster.active_connections:
                asyncio.run_coroutine_threadsafe(
                    log_broadcaster.broadcast({
                        "timestamp": time.strftime("%H:%M:%S"),
                        "level": "INFO",
                        "message": f"Amazon browser login successful! Fresh session cookies saved for amazon.{resolved_tld}.",
                        "name": "immich_sync.amazon"
                    }),
                    log_broadcaster._loop
                )

        started = login_mgr.start(on_success=on_success)
        st = login_mgr.get_status()
        st.update(_novnc_status_fields())
        return {"status": "started" if started else "failed", "login_status": st}

    @app.get("/api/amazon/login/status")
    async def get_amazon_login_status():
        existing = active_amazon_login.get("instance")
        if not existing:
            st = {"state": "idle", "message": "No login session active."}
        else:
            st = existing.get_status()
        st.update(_novnc_status_fields())
        return st

    @app.post("/api/amazon/login/cancel")
    async def cancel_amazon_login():
        existing = active_amazon_login.get("instance")
        if existing:
            existing.cancel()
            return {"status": "cancelled", "login_status": existing.get_status()}
        return {"status": "idle", "message": "No login session to cancel."}

    @app.post("/api/sync/pause")
    async def pause_sync():
        if not sync_state["is_syncing"]:
            return {"status": "not_syncing", "message": "No active sync cycle to pause."}
        sync_state["pause_sync_event"].set()
        sync_state["is_paused"] = True
        handle_sync_progress({
            "phase": "paused",
            "phase_name": "Sync Paused",
            "action": "Sync is paused. Click Resume to continue."
        })
        logger.info("Sync cycle paused by user via Web UI.")
        return {"status": "paused", "message": "Sync cycle paused."}

    @app.post("/api/sync/resume")
    async def resume_sync():
        if not sync_state["is_syncing"]:
            return {"status": "not_syncing", "message": "No active sync cycle to resume."}
        sync_state["pause_sync_event"].clear()
        sync_state["is_paused"] = False
        handle_sync_progress({
            "phase": "running",
            "phase_name": "Sync In Progress",
            "action": "Resuming sync cycle..."
        })
        logger.info("Sync cycle resumed by user via Web UI.")
        return {"status": "resumed", "message": "Sync cycle resumed."}

    @app.post("/api/sync/stop")
    async def stop_sync():
        if not sync_state["is_syncing"]:
            return {"status": "not_syncing", "message": "No active sync cycle to stop."}
        sync_state["stop_sync_event"].set()
        sync_state["pause_sync_event"].clear()
        sync_state["is_paused"] = False
        handle_sync_progress({
            "phase": "stopping",
            "phase_name": "Stopping Sync",
            "action": "Stop requested. Terminating active sync cycle..."
        })
        logger.info("Sync cycle stopped by user via Web UI.")
        return {"status": "stopping", "message": "Stopping active sync cycle..."}

    def _record_cycle_and_notify(cycle_cfg: AppConfig, stats: Dict[str, Any]) -> None:
        if getattr(cycle_cfg.sync, "dry_run", False):
            return
        try:
            db = SyncDatabase(db_path)
            if stats.get("amazon_session_expired"):
                details = stats.get("error_details") or []
                reason = details[0] if details else "Amazon Photos session token expired (HTTP 401)."
                fp = amazon_cookie_fingerprint(cycle_cfg.amazon.cookies, cycle_cfg.amazon.tld)
                db.set_amazon_auth_paused(reason, fp)
                sync_state["amazon_auth_paused"] = True
                sync_state["auth_paused_reason"] = reason
                service_status_cache["amazon"]["connected"] = False
                service_status_cache["amazon"]["details"] = reason
                sync_progress["phase"] = "auth_expired"
                sync_progress["phase_name"] = "Paused: Amazon Session Expired"
                sync_progress["action"] = reason
            elif cycle_cfg.amazon.enabled and cycle_cfg.sync.enable_amazon and int(stats.get("errors", 0) or 0) == 0:
                if db.get_amazon_auth_pause():
                    db.clear_amazon_auth_paused()
                sync_state["amazon_auth_paused"] = False
                sync_state["auth_paused_reason"] = None
            db.record_scan_stats(stats)
            maybe_send_error_alert(cycle_cfg, stats)
            maybe_send_daily_summary(cycle_cfg, db)
        except Exception as e:
            logger.warning(f"Failed recording daily scan stats / notifications: {e}")

    def _run_cycle_with_auto_refresh(
        cycle_cfg: AppConfig,
        limit: Optional[int] = None,
        force_deletes: Optional[bool] = None,
    ) -> tuple[AppConfig, Dict[str, Any]]:
        """Run a sync cycle; on Amazon token expiry, try one automatic browser-profile refresh before pausing."""
        effective_force = force_deletes if force_deletes is not None else cycle_cfg.sync.force_deletes
        active_cfg = cycle_cfg

        def _exec_cycle(cfg_to_run: AppConfig) -> Dict[str, Any]:
            engine = SyncEngine(
                cfg_to_run,
                db_path=db_path,
                progress_callback=handle_sync_progress,
                stop_event=sync_state["stop_sync_event"],
                pause_event=sync_state["pause_sync_event"],
            )
            return engine.run_sync_cycle(limit_intake=limit, force_deletes=effective_force)

        stats = _exec_cycle(active_cfg)

        if stats.get("amazon_session_expired") and not getattr(active_cfg.sync, "dry_run", False):
            if not (sync_state["stop_sync_event"].is_set() or sync_state["stop_daemon_event"].is_set()):
                logger.info("Amazon session expired — attempting automatic token refresh via saved browser profile...")
                handle_sync_progress({
                    "phase": "refreshing_auth",
                    "phase_name": "Refreshing Amazon Token",
                    "action": "Amazon token expired. Attempting automatic token refresh from browser profile...",
                })
                from src.clients.amazon_login import try_refresh_amazon_cookies_from_profile

                ok, refreshed_cookies, refresh_msg = try_refresh_amazon_cookies_from_profile(active_cfg.amazon.tld)
                if ok and refreshed_cookies:
                    _persist_amazon_cookies(
                        refreshed_cookies,
                        active_cfg.amazon.tld,
                        details=refresh_msg,
                        connected=True,
                        source="Auto Browser Refresh",
                        trigger_reprocess=False,
                    )
                    active_cfg = get_cfg() or active_cfg
                    logger.info("Amazon token automatically refreshed! Re-running sync cycle to process remaining items...")
                    stats = _exec_cycle(active_cfg)
                    if stats.get("amazon_session_expired"):
                        details = stats.setdefault("error_details", [])
                        note = "Auto-refreshed Amazon token still returned unauthorized (HTTP 401); manual intervention is required."
                        if note not in details:
                            details.insert(0, note)
                else:
                    logger.warning(f"Automatic Amazon token refresh did not succeed: {refresh_msg}")
                    details = stats.setdefault("error_details", [])
                    if refresh_msg and refresh_msg not in details:
                        details.insert(0, refresh_msg)

        sync_state["last_stats"] = stats
        sync_state["last_sync_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _record_cycle_and_notify(active_cfg, stats)
        return active_cfg, stats

    def _start_manual_sync_worker(
        sync_cfg: AppConfig,
        limit: Optional[int] = None,
        force_deletes: Optional[bool] = None,
    ) -> None:
        def run_worker():
            sync_state["is_syncing"] = True
            sync_state["is_paused"] = False
            sync_state["stop_sync_event"].clear()
            sync_state["pause_sync_event"].clear()
            logger.info("Triggering sync cycle...")
            handle_sync_progress({
                "percent": 0,
                "phase": "starting",
                "phase_name": "Starting Sync",
                "current": 0,
                "total": 0,
                "item_name": "",
                "action": "Initializing sync engine..."
            })
            try:
                _run_cycle_with_auto_refresh(sync_cfg, limit=limit, force_deletes=force_deletes)
            except Exception as e:
                logger.error(f"Manual sync failed: {e}", exc_info=True)
                handle_sync_progress({
                    "percent": sync_progress.get("percent", 0),
                    "phase": "error",
                    "phase_name": "Sync Error",
                    "current": 0,
                    "total": 0,
                    "item_name": "",
                    "action": f"Error: {str(e)}"
                })
            finally:
                sync_state["is_syncing"] = False
                sync_state["is_paused"] = False
                sync_progress["active"] = False
                if sync_state["stop_sync_event"].is_set():
                    sync_progress["phase"] = "stopped"
                    sync_progress["phase_name"] = "Sync Stopped"
                    sync_progress["action"] = "Sync cycle was stopped by user."
                elif sync_progress.get("phase") not in ("error", "auth_expired"):
                    sync_progress["percent"] = 100
                    sync_progress["phase"] = "completed"
                    sync_progress["phase_name"] = "Sync Complete"
                    sync_progress["action"] = "Sync cycle finished successfully."
                if log_broadcaster._loop and log_broadcaster.active_connections:
                    asyncio.run_coroutine_threadsafe(
                        log_broadcaster.broadcast({"type": "progress", "data": sync_progress}),
                        log_broadcaster._loop
                    )

        thread = threading.Thread(target=run_worker, daemon=True)
        thread.start()

    @app.post("/api/sync/run")
    async def trigger_sync(req: SyncRunRequest):
        if sync_state["is_syncing"]:
            raise HTTPException(status_code=400, detail="A sync cycle is already in progress.")

        cfg = get_cfg()
        if not cfg:
            raise HTTPException(status_code=400, detail="Configuration not found. Please setup config first.")

        if req.enable_amazon is not False:
            active_pause = _get_active_amazon_pause(cfg)
            if active_pause:
                raise HTTPException(
                    status_code=409,
                    detail=f"Sync paused — manual intervention required: {active_pause['reason']}",
                )

        sync_cfg = cfg.model_copy(deep=True)
        if req.dry_run:
            sync_cfg.sync.dry_run = True
        if req.enable_immich is not None:
            sync_cfg.sync.enable_immich = req.enable_immich
            sync_cfg.immich.enabled = req.enable_immich
        if req.enable_icloud is not None:
            sync_cfg.sync.enable_icloud = req.enable_icloud
            sync_cfg.icloud.enabled = req.enable_icloud
        if req.enable_amazon is not None:
            sync_cfg.sync.enable_amazon = req.enable_amazon
            sync_cfg.amazon.enabled = req.enable_amazon

        _start_manual_sync_worker(sync_cfg, limit=req.limit, force_deletes=req.force_deletes)
        return {"status": "started", "dry_run": req.dry_run}

    @app.post("/api/webhook/immich")
    async def immich_webhook_trigger(request: Request):
        cfg = get_cfg()
        if not cfg:
            raise HTTPException(status_code=400, detail="Configuration not found.")

        expected_secret = (cfg.sync.webhook_secret or "").strip()
        if expected_secret:
            provided_secret = (
                request.headers.get("X-Webhook-Secret", "").strip()
                or request.query_params.get("secret", "").strip()
            )
            if not secrets.compare_digest(provided_secret, expected_secret):
                raise HTTPException(status_code=401, detail="Invalid or missing webhook secret.")

        event_label = "webhook"
        try:
            body = await request.json()
            if isinstance(body, dict):
                event_label = str(body.get("event") or body.get("trigger") or body.get("type") or "webhook")
        except Exception:
            pass

        logger.info(f"Received Immich webhook trigger ({event_label}).")
        active_pause = _get_active_amazon_pause(cfg)
        if active_pause:
            logger.warning(f"Skipping webhook trigger ({event_label}): {active_pause['reason']}")
            return {"status": "paused_auth_expired", "event": event_label, "reason": active_pause["reason"]}

        sync_state["trigger_sync_event"].set()
        if sync_state["daemon_running"]:
            return {"status": "triggered", "mode": "daemon", "event": event_label}
        if sync_state["is_syncing"]:
            return {"status": "queued", "mode": "manual", "event": event_label}

        sync_state["trigger_sync_event"].clear()
        _start_manual_sync_worker(cfg.model_copy(deep=True))
        return {"status": "triggered", "mode": "manual", "event": event_label}

    def launch_daemon_thread() -> str:
        if sync_state["daemon_running"]:
            return "already_running"

        cfg = get_cfg()
        if not cfg:
            return "no_config"

        sync_state["stop_daemon_event"].clear()
        sync_state["daemon_running"] = True

        def _wait_for_next_cycle(active_cfg: AppConfig, mode: str, schedule: list, interval: int, auth_paused: bool = False) -> None:
            """Block until the next poll/scheduled slot, a webhook/token trigger, or a daemon stop."""
            if auth_paused:
                sync_state["next_scheduled_run"] = None
                deadline = float("inf")
            elif mode == "scheduled":
                if schedule:
                    next_run = next_scheduled_run(schedule)
                    sync_state["next_scheduled_run"] = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None
                    logger.info(f"Scheduled mode: next scan at {sync_state['next_scheduled_run']}.")
                    deadline = next_run.timestamp() if next_run else time.time() + 60
                else:
                    sync_state["next_scheduled_run"] = None
                    deadline = time.time() + 60
            else:
                sync_state["next_scheduled_run"] = None
                deadline = time.time() + max(1, int(interval))

            last_minute_check = time.time()
            while time.time() < deadline:
                if sync_state["stop_daemon_event"].is_set():
                    return
                if sync_state["trigger_sync_event"].is_set():
                    sync_state["trigger_sync_event"].clear()
                    logger.info("Waking sync daemon immediately from Immich event / webhook / token update.")
                    return
                if time.time() - last_minute_check >= 60:
                    last_minute_check = time.time()
                    latest_cfg = get_cfg() or active_cfg
                    try:
                        maybe_send_daily_summary(latest_cfg, SyncDatabase(db_path))
                    except Exception as e:
                        logger.debug(f"Periodic daily notification check failed: {e}")
                    if auth_paused:
                        if _get_active_amazon_pause(latest_cfg) is None:
                            logger.info("Amazon Photos auth pause cleared; waking daemon to reprocess pending items.")
                            return
                        continue
                    latest_mode = _effective_sync_mode(latest_cfg)
                    latest_schedule = parse_schedule_times(getattr(latest_cfg.sync, "scheduled_sync_times", [])) if latest_mode == "scheduled" else []
                    if latest_mode != mode or latest_schedule != schedule:
                        logger.info(f"Sync schedule/mode changed ({mode} -> {latest_mode}); recalculating wait.")
                        mode = latest_mode
                        schedule = latest_schedule
                        if mode == "scheduled":
                            if schedule:
                                next_run = next_scheduled_run(schedule)
                                sync_state["next_scheduled_run"] = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None
                                deadline = next_run.timestamp() if next_run else deadline
                            else:
                                sync_state["next_scheduled_run"] = None
                                deadline = time.time() + 60
                        else:
                            sync_state["next_scheduled_run"] = None
                            deadline = min(deadline, time.time() + max(1, int(latest_cfg.sync.poll_interval_seconds)))
                time.sleep(1)

        def daemon_worker():
            logger.info(f"Sync daemon started (mode: {_effective_sync_mode(cfg)})")
            watcher_sig = None
            logged_auth_pause = False

            def _ensure_watcher(active_cfg: AppConfig, is_auth_paused: bool = False) -> None:
                nonlocal watcher_sig
                want = bool(
                    _effective_sync_mode(active_cfg) == "realtime"
                    and not is_auth_paused
                    and active_cfg.immich.enabled
                    and active_cfg.immich.api_key
                    and active_cfg.immich.url
                )
                sig = (
                    want,
                    active_cfg.immich.url,
                    active_cfg.immich.api_key,
                    active_cfg.immich.verify_ssl,
                    getattr(active_cfg.sync, "event_debounce_seconds", 15),
                )
                if sig == watcher_sig:
                    return
                existing_w = sync_state.get("event_watcher")
                if existing_w is not None:
                    existing_w.stop()
                    sync_state["event_watcher"] = None
                watcher_sig = sig
                if want:
                    new_w = ImmichEventWatcher(
                        immich_url=active_cfg.immich.url,
                        api_key=active_cfg.immich.api_key,
                        verify_ssl=active_cfg.immich.verify_ssl,
                        debounce_seconds=getattr(active_cfg.sync, "event_debounce_seconds", 15),
                        on_trigger=lambda evt: sync_state["trigger_sync_event"].set(),
                    )
                    sync_state["event_watcher"] = new_w
                    new_w.start()

            try:
                first_iteration = True
                while not sync_state["stop_daemon_event"].is_set():
                    active_cfg = get_cfg() or cfg
                    interval = active_cfg.sync.poll_interval_seconds
                    mode = _effective_sync_mode(active_cfg)
                    schedule = parse_schedule_times(getattr(active_cfg.sync, "scheduled_sync_times", [])) if mode == "scheduled" else []

                    db = SyncDatabase(db_path)
                    raw_pause = db.get_amazon_auth_pause()
                    amazon_enabled = bool(active_cfg.amazon.enabled and active_cfg.sync.enable_amazon)
                    auth_paused = False
                    resumed_from_token_update = False

                    if raw_pause and amazon_enabled:
                        current_fp = amazon_cookie_fingerprint(active_cfg.amazon.cookies, active_cfg.amazon.tld)
                        if (
                            raw_pause.get("cookie_fp")
                            and current_fp != raw_pause["cookie_fp"]
                            and active_cfg.amazon.cookies.get("session-id")
                        ):
                            db.clear_amazon_auth_paused()
                            sync_state["amazon_auth_paused"] = False
                            sync_state["auth_paused_reason"] = None
                            logged_auth_pause = False
                            resumed_from_token_update = True
                            logger.info("Detected updated Amazon Photos cookies; immediately reprocessing pending items.")
                        else:
                            auth_paused = True
                            sync_state["amazon_auth_paused"] = True
                            sync_state["auth_paused_reason"] = raw_pause["reason"]
                            service_status_cache["amazon"]["connected"] = False
                            service_status_cache["amazon"]["details"] = raw_pause["reason"]
                            if not logged_auth_pause:
                                logger.warning(f"Sync daemon paused until Amazon token is updated: {raw_pause['reason']}")
                                logged_auth_pause = True
                    else:
                        logged_auth_pause = False

                    _ensure_watcher(active_cfg, is_auth_paused=auth_paused)

                    # In scheduled mode, wait for next scheduled slot unless resuming from a token update; skip if no times set.
                    if auth_paused:
                        run_now = False
                    elif mode == "scheduled":
                        run_now = bool(resumed_from_token_update or (not first_iteration and schedule))
                    else:
                        run_now = True
                    first_iteration = False

                    if run_now and not sync_state["is_syncing"]:
                        sync_state["is_syncing"] = True
                        sync_state["is_paused"] = False
                        sync_state["stop_sync_event"].clear()
                        sync_state["pause_sync_event"].clear()
                        sync_state["trigger_sync_event"].clear()
                        try:
                            active_cfg, stats = _run_cycle_with_auto_refresh(
                                active_cfg,
                                force_deletes=active_cfg.sync.force_deletes,
                            )
                            if stats.get("amazon_session_expired"):
                                auth_paused = True
                                logged_auth_pause = True
                                _ensure_watcher(active_cfg, is_auth_paused=True)
                        except Exception as e:
                            logger.error(f"Daemon cycle error: {e}")
                        finally:
                            was_stopped = sync_state["stop_sync_event"].is_set()
                            sync_state["is_syncing"] = False
                            sync_state["is_paused"] = False
                            sync_state["stop_sync_event"].clear()
                            sync_state["pause_sync_event"].clear()
                            sync_progress["active"] = False
                            if was_stopped:
                                sync_progress["phase"] = "stopped"
                                sync_progress["phase_name"] = "Sync Stopped"
                                sync_progress["action"] = "Sync cycle was stopped by user."
                                if log_broadcaster._loop and log_broadcaster.active_connections:
                                    asyncio.run_coroutine_threadsafe(
                                        log_broadcaster.broadcast({"type": "progress", "data": sync_progress}),
                                        log_broadcaster._loop,
                                    )

                    # Sleep in short increments to respond quickly to stop requests, webhooks, token updates, or schedule edits
                    _wait_for_next_cycle(active_cfg, mode, schedule, interval, auth_paused=auth_paused)
            finally:
                existing_w = sync_state.get("event_watcher")
                if existing_w is not None:
                    existing_w.stop()
                    sync_state["event_watcher"] = None
                logger.info("Sync daemon stopped.")
                sync_state["daemon_running"] = False

        thread = threading.Thread(target=daemon_worker, daemon=True)
        sync_state["daemon_thread"] = thread
        thread.start()
        return "started"

    app.state.start_daemon_fn = launch_daemon_thread

    @app.post("/api/sync/daemon/start")
    async def start_daemon():
        status = launch_daemon_thread()
        if status == "no_config":
            raise HTTPException(status_code=400, detail="Configuration not found.")
        return {"status": status}

    @app.post("/api/sync/daemon/stop")
    async def stop_daemon():
        if not sync_state["daemon_running"]:
            return {"status": "not_running"}

        sync_state["stop_daemon_event"].set()
        if sync_state["is_syncing"]:
            sync_state["stop_sync_event"].set()
            sync_state["pause_sync_event"].clear()
            sync_state["is_paused"] = False
        return {"status": "stopping"}

    @app.get("/api/assets")
    async def get_assets(limit: int = 50, offset: int = 0, search: Optional[str] = None):
        db = SyncDatabase(db_path)
        assets = db.get_active_assets()
        if search:
            s = search.lower()
            assets = [a for a in assets if s in (a.get("filename") or "").lower() or s in (a.get("sha1") or "").lower()]
        total = len(assets)
        paged = assets[offset:offset + limit]
        return {"total": total, "items": paged}

    @app.get("/api/albums")
    async def get_albums():
        db = SyncDatabase(db_path)
        albums = db.get_all_albums()
        return {"total": len(albums), "items": albums}

    @app.get("/api/config")
    async def get_config_endpoint():
        cfg = get_cfg()
        if not cfg:
            # Return template from example
            ex_path = Path("config.example.json")
            if ex_path.exists():
                return json.loads(ex_path.read_text(encoding="utf-8"))
            raise HTTPException(status_code=404, detail="Config not found")
        return _mask_config_dict(cfg.model_dump())

    @app.post("/api/config")
    async def save_config_endpoint(payload: dict):
        try:
            existing_cfg = get_cfg()
            merged_payload = copy.deepcopy(payload)
            if existing_cfg:
                imm = merged_payload.setdefault("immich", {})
                if imm.get("api_key") in (SECRET_MASK, "", None) and existing_cfg.immich.api_key:
                    imm["api_key"] = existing_cfg.immich.api_key

                ic = merged_payload.setdefault("icloud", {})
                if ic.get("password") in (SECRET_MASK, "", None) and existing_cfg.icloud.password:
                    ic["password"] = existing_cfg.icloud.password

                amz = merged_payload.setdefault("amazon", {})
                if "cookies" in amz and isinstance(amz["cookies"], dict):
                    merged_cookies = dict(existing_cfg.amazon.cookies)
                    for k, v in amz["cookies"].items():
                        if v == SECRET_MASK:
                            if k in existing_cfg.amazon.cookies:
                                merged_cookies[k] = existing_cfg.amazon.cookies[k]
                        elif v:
                            merged_cookies[k] = v
                        else:
                            merged_cookies.pop(k, None)
                    amz["cookies"] = merged_cookies

                notif = merged_payload.setdefault("notifications", {})
                if "apprise_urls" in notif and isinstance(notif["apprise_urls"], list):
                    raw_urls = [str(u).strip() for u in notif["apprise_urls"] if str(u).strip()]
                    existing_urls = list(existing_cfg.notifications.apprise_urls or [])
                    if raw_urls and all(u == SECRET_MASK for u in raw_urls) and existing_urls:
                        notif["apprise_urls"] = existing_urls
                    else:
                        resolved_urls: List[str] = []
                        for idx, u in enumerate(raw_urls):
                            if u == SECRET_MASK and idx < len(existing_urls):
                                resolved_urls.append(existing_urls[idx])
                            elif u != SECRET_MASK:
                                resolved_urls.append(u)
                        notif["apprise_urls"] = resolved_urls

                sync_sec = merged_payload.setdefault("sync", {})
                if sync_sec.get("webhook_secret") in (SECRET_MASK, "", None):
                    if sync_sec.get("webhook_secret") == SECRET_MASK and existing_cfg.sync.webhook_secret:
                        sync_sec["webhook_secret"] = existing_cfg.sync.webhook_secret
                    elif sync_sec.get("webhook_secret") == "":
                        sync_sec["webhook_secret"] = None

            validated = AppConfig.model_validate(merged_payload)
            save_config(validated, config_path)
            # Update cache flags
            service_status_cache["immich"]["configured"] = bool(validated.immich.api_key)
            service_status_cache["immich"]["enabled"] = bool(validated.sync.enable_immich and validated.immich.enabled)
            service_status_cache["icloud"]["configured"] = bool(validated.icloud.apple_id)
            service_status_cache["icloud"]["enabled"] = bool(validated.sync.enable_icloud and validated.icloud.enabled)
            service_status_cache["amazon"]["configured"] = bool(validated.amazon.cookies.get("session-id"))
            service_status_cache["amazon"]["enabled"] = bool(validated.sync.enable_amazon and validated.amazon.enabled)
            reprocessing = _on_amazon_token_updated(validated, source="Settings")
            msg = "Settings saved! Fresh Amazon token detected — reprocessing pending items now." if reprocessing else "Settings saved successfully!"
            return {"status": "saved", "reprocessing": reprocessing, "message": msg}
        except Exception as e:
            err_msg = str(e)
            if hasattr(e, "errors"):
                err_msg = "; ".join(f"{err.get('loc', [])}: {err.get('msg', '')}" for err in e.errors())
            logger.error(f"Failed to save configuration: {err_msg}")
            raise HTTPException(status_code=400, detail=err_msg)

    @app.post("/api/test-connection/{service}")
    async def test_service_connection(service: str, payload: Optional[dict] = None):
        cfg = get_cfg()
        result = await asyncio.to_thread(sync_test_service, service, payload, cfg)
        return result

    @app.post("/api/notifications/test")
    async def test_notifications_endpoint(payload: Optional[dict] = None):
        cfg = get_cfg()
        raw_urls = payload.get("apprise_urls") if isinstance(payload, dict) else None
        urls: List[str] = []
        if isinstance(raw_urls, list):
            cleaned = [str(u).strip() for u in raw_urls if str(u).strip()]
            existing_urls = list(cfg.notifications.apprise_urls or []) if cfg else []
            if cleaned and all(u == SECRET_MASK for u in cleaned) and existing_urls:
                urls = existing_urls
            else:
                for idx, u in enumerate(cleaned):
                    if u == SECRET_MASK and idx < len(existing_urls):
                        urls.append(existing_urls[idx])
                    elif u != SECRET_MASK:
                        urls.append(u)
        elif cfg:
            urls = list(cfg.notifications.apprise_urls)

        db = SyncDatabase(db_path)
        ok, msg = await asyncio.to_thread(send_test_notification, urls, db)
        return {"success": ok, "message": msg}

    @app.post("/api/icloud/2fa/start")
    async def start_icloud_2fa(payload: Optional[dict] = None):
        cfg = get_cfg()
        apple_id = (payload.get("apple_id") if payload else None) or (cfg.icloud.apple_id if cfg else None)
        raw_pw = payload.get("password") if payload else None
        password = _clean_secret(raw_pw) or (cfg.icloud.password if cfg else None)
        force = bool(payload.get("force")) if payload else False
        session_dir = cfg.icloud.session_dir if cfg else "./.icloud_session"

        if not apple_id:
            raise HTTPException(status_code=400, detail="Apple ID email is required. Please enter it in Settings first.")

        existing_api = sync_state.get("pending_icloud_client")
        existing_id = sync_state.get("pending_icloud_apple_id")
        if existing_api is not None and existing_id == apple_id and not force and getattr(existing_api, "requires_2fa", False):
            return {"status": "requires_2fa", "message": "Enter the 6-digit verification code sent to your Apple devices."}

        def run_2fa_start():
            from pyicloud import PyiCloudService
            api = PyiCloudService(
                apple_id=apple_id,
                password=password,
                cookie_directory=str(session_dir)
            )
            sync_state["pending_icloud_client"] = api
            sync_state["pending_icloud_apple_id"] = apple_id
            if api.requires_2fa:
                return {"status": "requires_2fa", "message": "Verification code has been sent to your Apple devices."}
            elif api.is_trusted_session:
                service_status_cache["icloud"] = {"configured": True, "connected": True, "requires_2fa": False, "details": f"Session valid for {apple_id}"}
                return {"status": "already_trusted", "message": "iCloud session is already trusted and active!"}
            else:
                service_status_cache["icloud"] = {"configured": True, "connected": True, "requires_2fa": False, "details": f"Session valid for {apple_id}"}
                return {"status": "connected", "message": "Connected without 2FA prompt."}

        try:
            return await asyncio.to_thread(run_2fa_start)
        except Exception as e:
            logger.error(f"iCloud 2FA start failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/icloud/2fa/verify")
    async def verify_icloud_2fa(req: Verify2FARequest):
        api = sync_state.get("pending_icloud_client")
        if not api:
            raise HTTPException(status_code=400, detail="No 2FA verification session active. Click Trigger 2FA Verification first.")

        code = req.code.strip()
        try:
            def run_verify():
                try:
                    ok = api.validate_2fa_code(code)
                except Exception as bridge_err:
                    err_lower = str(bridge_err).lower()
                    if "websocket" in err_lower or "bridge" in err_lower:
                        logger.warning(f"iCloud bridge verification failed ({bridge_err}); falling back to direct trusted-device code verification.")
                        api.use_existing_trusted_device_code()
                        ok = api.validate_2fa_code(code)
                    else:
                        raise
                if not ok:
                    return False
                if not api.is_trusted_session:
                    api.trust_session()
                return True

            result = await asyncio.to_thread(run_verify)
            if not result:
                logger.warning("iCloud 2FA code verification returned False.")
                return {
                    "success": False,
                    "message": "Invalid or expired verification code. If it keeps failing, close and click Trigger 2FA Verification to request a fresh code."
                }

            apple_id = sync_state.get("pending_icloud_apple_id") or "iCloud"
            sync_state["pending_icloud_client"] = None
            sync_state["pending_icloud_apple_id"] = None
            service_status_cache["icloud"] = {
                "configured": True,
                "connected": True,
                "requires_2fa": False,
                "details": f"Session valid for {apple_id}"
            }
            logger.info("iCloud 2FA code verified and session trusted via Web UI!")
            return {"success": True, "message": "iCloud session verified and trusted successfully!"}
        except Exception as e:
            logger.error(f"iCloud 2FA verification error: {e}")
            return {"success": False, "message": f"Verification error: {e}"}

    @app.websocket("/ws/logs")
    async def websocket_logs(websocket: WebSocket):
        await websocket.accept()
        log_broadcaster.register(websocket)
        try:
            # Send initial buffer backlog
            for entry in list(log_broadcaster.buffer):
                await websocket.send_json(entry)

            while True:
                # Keep alive
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        except WebSocketDisconnect:
            log_broadcaster.unregister(websocket)
        except Exception:
            log_broadcaster.unregister(websocket)

    def _novnc_root() -> Path:
        return Path(os.environ.get("NOVNC_WEB_DIR", "/usr/share/novnc"))

    @app.get("/novnc")
    async def novnc_index():
        target = _novnc_root() / "vnc.html"
        if not target.is_file():
            raise HTTPException(status_code=404, detail="noVNC web assets not found")
        return FileResponse(target)

    @app.get("/novnc/{asset_path:path}")
    async def novnc_assets(asset_path: str):
        novnc_dir = _novnc_root()
        if not novnc_dir.is_dir():
            raise HTTPException(status_code=404, detail="noVNC web assets not found")
        rel = asset_path.strip("/") or "vnc.html"
        root = novnc_dir.resolve()
        candidate = (root / rel).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            raise HTTPException(status_code=403, detail="Forbidden")
        if not candidate.is_file():
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(candidate)

    @app.websocket("/novnc/websockify")
    async def novnc_websockify_proxy(websocket: WebSocket):
        if not _is_basic_auth_authorized(websocket.headers.get("authorization", "")):
            await websocket.close(code=1008)
            return

        novnc_port = os.environ.get("NOVNC_PORT", "6090").strip() or "6090"
        requested_protos = [
            p.strip() for p in websocket.headers.get("sec-websocket-protocol", "").split(",") if p.strip()
        ]
        chosen_proto = "binary" if "binary" in requested_protos else (requested_protos[0] if requested_protos else None)
        await websocket.accept(subprotocol=chosen_proto)

        import websockets
        upstream_url = f"ws://127.0.0.1:{novnc_port}/websockify"
        try:
            connect_kwargs: Dict[str, Any] = {"max_size": None}
            if chosen_proto:
                connect_kwargs["subprotocols"] = [chosen_proto]
            async with websockets.connect(upstream_url, **connect_kwargs) as upstream:
                async def client_to_upstream():
                    try:
                        while True:
                            msg = await websocket.receive()
                            if msg.get("type") == "websocket.disconnect":
                                break
                            if msg.get("bytes") is not None:
                                await upstream.send(msg["bytes"])
                            elif msg.get("text") is not None:
                                await upstream.send(msg["text"])
                    except (WebSocketDisconnect, asyncio.CancelledError):
                        pass

                async def upstream_to_client():
                    try:
                        async for data in upstream:
                            if isinstance(data, (bytes, bytearray)):
                                await websocket.send_bytes(bytes(data))
                            else:
                                await websocket.send_text(str(data))
                    except (WebSocketDisconnect, asyncio.CancelledError):
                        pass

                tasks = [
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                ]
                _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.debug(f"noVNC WebSocket proxy ended: {e}")
        finally:
            try:
                await websocket.close()
            except Exception:
                pass

    return app
