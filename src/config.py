import os
import json
from pathlib import Path
from typing import Dict, Optional, Tuple, Any, List
from dotenv import dotenv_values
from pydantic import BaseModel, Field

class ImmichConfig(BaseModel):
    url: str = Field(..., description="Base URL of your Immich instance (e.g. http://192.168.1.100:2283)")
    api_key: str = Field(..., description="Immich API Key generated from user settings")
    verify_ssl: bool = Field(True, description="Whether to verify SSL certificates")
    enabled: bool = Field(True, description="Enable Immich synchronization")

class ICloudConfig(BaseModel):
    apple_id: str = Field(..., description="Apple ID email address")
    password: Optional[str] = Field(None, description="Apple ID password")
    session_dir: str = Field("./.icloud_session", description="Directory to cache 2FA/cookies")
    enabled: bool = Field(True, description="Enable iCloud synchronization")
    upload_videos: bool = Field(True, description="Whether to upload video files from Immich to iCloud Photos")

class AmazonConfig(BaseModel):
    cookies: Dict[str, str] = Field(default_factory=dict, description="Session cookies (session-id, ubid-acbca/ubid-main, at-acbca/at-main)")
    tld: str = Field("ca", description="Amazon marketplace TLD (ca for Canada https://www.amazon.ca/photos, com, co.uk, etc.)")
    enabled: bool = Field(True, description="Enable Amazon Photos synchronization")
    upload_videos: bool = Field(False, description="Whether to upload video files to Amazon Photos (False preserves 5GB video quota, only pictures synced)")

class SyncSettings(BaseModel):
    one_way_immich_master: bool = Field(True, description="One-way sync: Immich is master source. Never pulls iCloud/Amazon photos into Immich.")
    ingest_from_icloud: bool = Field(False, description="Whether to ingest photos from iCloud into Immich (False for one-way sync)")
    sync_deletions: bool = Field(True, description="Propagate deletions from Immich to iCloud and Amazon Photos")
    sync_albums: bool = Field(True, description="Propagate album creations and memberships across services")
    ignore_hidden_and_locked: bool = Field(True, description="Exclude hidden, locked, and archived assets in Immich from syncing")
    sync_videos_to_amazon: bool = Field(False, description="Whether to upload video files to Amazon Photos (False: photos/pictures only)")
    sync_videos_to_icloud: bool = Field(True, description="Whether to upload video files from Immich to iCloud Photos")
    max_delete_limit: int = Field(50, description="Safety guard: halt if more than X files would be deleted in one pass")
    force_deletes: bool = Field(False, description="Bypass max_delete_limit safety guard during deletion sync")
    download_temp_dir: str = Field("./.temp", description="Temporary staging directory for in-flight image downloads")
    poll_interval_seconds: int = Field(300, description="Polling interval in seconds for continuous daemon mode")
    dry_run: bool = Field(False, description="Preview actions without making destructive or write operations")
    enable_immich: bool = Field(True, description="Enable Immich as a sync source/destination")
    enable_icloud: bool = Field(True, description="Enable iCloud as a sync source/destination")
    enable_amazon: bool = Field(True, description="Enable Amazon Photos as a sync destination")

class NotificationConfig(BaseModel):
    enabled: bool = Field(False, description="Enable daily scan summary notifications via Apprise")
    apprise_urls: List[str] = Field(default_factory=list, description="Apprise URLs (e.g. tgram://<bot_token>/<chat_id>)")
    daily_summary_hour: int = Field(21, ge=0, le=23, description="Local hour (0-23) when the daily scan summary is sent")
    notify_on_error: bool = Field(False, description="Send an immediate notification when a scan cycle encounters errors")


class AppConfig(BaseModel):
    immich: ImmichConfig
    icloud: ICloudConfig
    amazon: AmazonConfig
    sync: SyncSettings = Field(default_factory=SyncSettings)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)

_ENV_BOOTSTRAP_KEYS = (
    "IMMICH_URL",
    "IMMICH_API_KEY",
    "ICLOUD_APPLE_ID",
    "AMAZON_SESSION_ID",
    "AMAZON_COOKIES_JSON",
)


def _parse_bool(val: str) -> bool:
    """Parse common boolean strings from environment/.env files."""
    normalized = val.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"Invalid boolean value: {val!r}")


def _merge_env_file(target: Dict[str, str], env_path: Path) -> None:
    if env_path.is_file():
        for k, v in dotenv_values(env_path).items():
            if k and v is not None and v.strip() != "":
                target[k] = v


def _collect_env(config_path: str | Path) -> Dict[str, str]:
    """Collect non-empty variables from sibling .env, ENV_FILE, and process env."""
    collected: Dict[str, str] = {}
    _merge_env_file(collected, Path(config_path).parent / ".env")

    env_file = os.environ.get("ENV_FILE", "").strip()
    if env_file:
        _merge_env_file(collected, Path(env_file))

    for k, v in os.environ.items():
        if v is not None and v.strip() != "":
            collected[k] = v

    return collected


def apply_env_overrides(data: Dict[str, Any], env: Dict[str, str]) -> Tuple[Dict[str, Any], bool]:
    """Apply environment/.env overrides to a raw config dictionary."""
    immich = data.setdefault("immich", {})
    icloud = data.setdefault("icloud", {})
    amazon = data.setdefault("amazon", {})
    cookies = amazon.setdefault("cookies", {})
    sync = data.setdefault("sync", {})
    notifications = data.setdefault("notifications", {})
    applied = False

    def get_val(key: str) -> Optional[str]:
        val = env.get(key)
        if val is None or val.strip() == "":
            return None
        return val.strip()

    # String, bool, and int single-target mappings: (env_key, section_dict, field, parser)
    field_mappings: list[tuple[str, Dict[str, Any], str, Any]] = [
        ("IMMICH_URL", immich, "url", str),
        ("IMMICH_API_KEY", immich, "api_key", str),
        ("IMMICH_VERIFY_SSL", immich, "verify_ssl", _parse_bool),
        ("ICLOUD_APPLE_ID", icloud, "apple_id", str),
        ("ICLOUD_PASSWORD", icloud, "password", str),
        ("ICLOUD_SESSION_DIR", icloud, "session_dir", str),
        ("AMAZON_TLD", amazon, "tld", lambda s: s.lower()),
        ("POLL_INTERVAL_SECONDS", sync, "poll_interval_seconds", int),
        ("MAX_DELETE_LIMIT", sync, "max_delete_limit", int),
        ("SYNC_DELETIONS", sync, "sync_deletions", _parse_bool),
        ("SYNC_ALBUMS", sync, "sync_albums", _parse_bool),
        ("IGNORE_HIDDEN_AND_LOCKED", sync, "ignore_hidden_and_locked", _parse_bool),
        ("FORCE_DELETES", sync, "force_deletes", _parse_bool),
        ("DRY_RUN", sync, "dry_run", _parse_bool),
        ("NOTIFICATIONS_ENABLED", notifications, "enabled", _parse_bool),
        ("DAILY_SUMMARY_HOUR", notifications, "daily_summary_hour", int),
        ("NOTIFY_ON_ERROR", notifications, "notify_on_error", _parse_bool),
    ]
    for env_key, section, field_name, parser in field_mappings:
        raw = get_val(env_key)
        if raw is not None:
            section[field_name] = parser(raw)
            applied = True

    # Mirrored toggle mappings (keep service flag and sync flag consistent)
    mirrored_bools = [
        ("IMMICH_ENABLED", immich, "enabled", sync, "enable_immich"),
        ("ICLOUD_ENABLED", icloud, "enabled", sync, "enable_icloud"),
        ("ICLOUD_UPLOAD_VIDEOS", icloud, "upload_videos", sync, "sync_videos_to_icloud"),
        ("AMAZON_ENABLED", amazon, "enabled", sync, "enable_amazon"),
        ("AMAZON_UPLOAD_VIDEOS", amazon, "upload_videos", sync, "sync_videos_to_amazon"),
    ]
    for env_key, svc_dict, svc_field, sync_dict, sync_field in mirrored_bools:
        raw = get_val(env_key)
        if raw is not None:
            parsed = _parse_bool(raw)
            svc_dict[svc_field] = parsed
            sync_dict[sync_field] = parsed
            applied = True

    # Amazon cookies (AMAZON_TLD was already applied above)
    tld = str(amazon.get("tld") or "ca").strip().lower()
    raw_cookies_json = get_val("AMAZON_COOKIES_JSON")
    if raw_cookies_json is not None:
        parsed_cookies = json.loads(raw_cookies_json)
        if not isinstance(parsed_cookies, dict):
            raise ValueError("AMAZON_COOKIES_JSON must be a JSON object")
        for ck, cv in parsed_cookies.items():
            if cv is not None and str(cv).strip() != "":
                cookies[str(ck)] = str(cv).strip()
        applied = True

    cookie_mappings = [
        ("AMAZON_SESSION_ID", "session-id"),
        ("AMAZON_UBID", "ubid-main" if tld == "com" else f"ubid-acb{tld}"),
        ("AMAZON_AT_TOKEN", "at-main" if tld == "com" else f"at-acb{tld}"),
    ]
    for env_key, cookie_name in cookie_mappings:
        raw = get_val(env_key)
        if raw is not None:
            cookies[cookie_name] = raw
            applied = True

    # Apprise / Telegram notification URLs
    raw_apprise = get_val("APPRISE_URLS")
    if raw_apprise is not None:
        parsed_urls = [u.strip() for u in raw_apprise.replace(",", "\n").splitlines() if u.strip()]
        if parsed_urls:
            notifications["apprise_urls"] = parsed_urls
            if get_val("NOTIFICATIONS_ENABLED") is None:
                notifications["enabled"] = True
            applied = True

    tg_token = get_val("TELEGRAM_BOT_TOKEN")
    tg_chat = get_val("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        tgram_url = f"tgram://{tg_token}/{tg_chat}"
        existing_urls = list(notifications.get("apprise_urls") or [])
        if tgram_url not in existing_urls:
            existing_urls.append(tgram_url)
        notifications["apprise_urls"] = existing_urls
        if get_val("NOTIFICATIONS_ENABLED") is None:
            notifications["enabled"] = True
        applied = True

    return data, applied


def load_config(config_path: str | Path = "config.json") -> AppConfig:
    path = Path(config_path)
    env = _collect_env(path)

    if not path.exists():
        has_bootstrap = any(env.get(k, "").strip() != "" for k in _ENV_BOOTSTRAP_KEYS)
        if not has_bootstrap:
            raise FileNotFoundError(
                f"Configuration file '{path.resolve()}' not found. "
                f"Please copy 'config.example.json' to 'config.json' (or '.env.example' to '.env') and fill in your credentials."
            )
        data: Dict[str, Any] = {
            "immich": {"url": "http://localhost:2283", "api_key": "", "verify_ssl": True, "enabled": True},
            "icloud": {"apple_id": "", "password": None, "session_dir": "./.icloud_session", "enabled": True, "upload_videos": True},
            "amazon": {"cookies": {}, "tld": "ca", "enabled": True, "upload_videos": False},
            "sync": {},
        }
        apply_env_overrides(data, env)
        cfg = AppConfig.model_validate(data)
        save_config(cfg, path)
        return cfg

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if env.get("ENV_OVERRIDE", "").strip().lower() in ("1", "true", "yes", "on"):
        apply_env_overrides(data, env)
    return AppConfig.model_validate(data)


def save_config(config: AppConfig, config_path: str | Path = "config.json"):
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(config.model_dump(), f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.chmod(tmp_path, 0o600)
    except OSError:
        pass
    os.replace(tmp_path, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
