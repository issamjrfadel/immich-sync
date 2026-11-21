import datetime
import logging
from typing import Any, Dict, List, Optional, Tuple

from src.config import AppConfig
from src.database import SyncDatabase

logger = logging.getLogger("immich_sync.notifications")


def send_apprise_notification(urls: List[str], title: str, body: str) -> Tuple[bool, str]:
    """Send a notification to one or more Apprise URLs (e.g. tgram://BOT_TOKEN/CHAT_ID)."""
    cleaned_urls = [str(u).strip() for u in (urls or []) if u and str(u).strip()]
    if not cleaned_urls:
        return False, "No Apprise notification URLs configured."

    try:
        import apprise
    except ImportError:
        return False, "Apprise package is not installed."

    ap = apprise.Apprise()
    added = 0
    for url in cleaned_urls:
        if ap.add(url):
            added += 1

    if added == 0:
        return False, "No valid Apprise URLs found. Check URL format (e.g. tgram://BOT_TOKEN/CHAT_ID)."

    try:
        ok = bool(ap.notify(title=title, body=body))
        if ok:
            return True, f"Notification sent to {added} target(s)."
        return False, "Apprise delivery failed. Check your bot token, chat ID, or network connectivity."
    except Exception as e:
        logger.error(f"Apprise notification error: {e}")
        return False, f"Notification error: {e}"


def _normalize_error_details(raw: Any, limit: int = 8) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            import json
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                raw = parsed
            else:
                raw = [raw]
        except (ValueError, TypeError):
            raw = [raw]
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for item in raw:
        text = " ".join(str(item or "").split())
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def format_daily_summary(
    daily_stats: Dict[str, Any],
    total_active_assets: int = 0,
    total_albums: int = 0,
    auth_pause: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """Build title and body for the daily scan statistics summary."""
    scan_date = daily_stats.get("scan_date") or datetime.date.today().isoformat()
    scans_count = int(daily_stats.get("scans_count", 0) or 0)
    mirrored = int(daily_stats.get("immich_direct_synced", 0) or 0)
    amazon_up = int(daily_stats.get("amazon_uploaded", 0) or 0)
    icloud_up = int(daily_stats.get("icloud_uploaded", 0) or 0)
    deletions = int(daily_stats.get("deletions_synced", 0) or 0)
    albums = int(daily_stats.get("albums_synced", 0) or 0)
    errors = int(daily_stats.get("errors", 0) or 0)
    error_details = _normalize_error_details(daily_stats.get("error_details"))

    if auth_pause:
        status_badge = f"Paused — Amazon Session Expired ({errors} errors)" if errors else "Paused — Amazon Session Expired"
    elif errors == 0:
        status_badge = "OK"
    else:
        status_badge = f"Warnings ({errors} errors)"

    title = f"Immich Multi-Sync Daily Report ({scan_date})"
    lines = [
        f"Status: {status_badge}",
        f"Scans Completed Today: {scans_count}",
        f"Immich Assets Mirrored: {mirrored}",
        f"Uploaded to Amazon Photos: {amazon_up}",
        f"Uploaded to iCloud Photos: {icloud_up}",
        f"Deletions Propagated: {deletions}",
        f"Albums Synced: {albums}",
        f"Errors Today: {errors}",
    ]

    if auth_pause:
        lines.append("")
        lines.append("Manual Intervention Required: Automatic token refresh failed. Syncing is paused until you update your Amazon Photos session cookies. Once updated, pending items will reprocess automatically.")

    if error_details:
        lines.append("")
        lines.append("Failure Details:")
        for reason in error_details:
            lines.append(f"- {reason}")

    lines.extend([
        "",
        f"Library Totals: {total_active_assets} tracked assets | {total_albums} albums",
    ])
    return title, "\n".join(lines)


def maybe_send_daily_summary(
    cfg: AppConfig,
    db: SyncDatabase,
    now: Optional[datetime.datetime] = None,
) -> bool:
    """Send today's daily statistics summary once if enabled and past daily_summary_hour."""
    notif = getattr(cfg, "notifications", None)
    if not notif or not notif.enabled or not notif.apprise_urls:
        return False

    current = now or datetime.datetime.now()
    if current.hour < int(notif.daily_summary_hour):
        return False

    today_key = current.date().isoformat()
    if db.was_daily_notified(today_key):
        return False

    daily_stats = db.get_daily_scan_stats(today_key)
    active_count = len(db.get_active_assets())
    albums_count = len(db.get_all_albums())
    auth_pause = db.get_amazon_auth_pause()
    title, body = format_daily_summary(
        daily_stats,
        total_active_assets=active_count,
        total_albums=albums_count,
        auth_pause=auth_pause,
    )

    ok, msg = send_apprise_notification(notif.apprise_urls, title=title, body=body)
    if ok:
        db.mark_daily_notified(today_key)
        db.log_sync("NOTIFY", f"Sent daily summary for {today_key}")
        logger.info(f"Daily scan statistics notification sent for {today_key}.")
        return True

    logger.warning(f"Daily scan statistics notification failed: {msg}")
    return False


def maybe_send_error_alert(cfg: AppConfig, stats: Dict[str, Any]) -> bool:
    """Send an immediate alert on Amazon session expiry (always when notifications enabled) or on errors when notify_on_error is enabled."""
    notif = getattr(cfg, "notifications", None)
    if not notif or not notif.enabled or not notif.apprise_urls:
        return False

    amazon_expired = bool(stats.get("amazon_session_expired"))
    errors = int(stats.get("errors", 0) or 0)
    if not amazon_expired and (not notif.notify_on_error or errors <= 0):
        return False

    error_details = _normalize_error_details(stats.get("error_details"))

    if amazon_expired:
        title = "Immich Multi-Sync Paused: Manual Intervention Required (Amazon Session Expired)"
        lines = [
            "Sync stopped because your Amazon Photos session token/cookies expired and automatic token renewal failed.",
            "Manual intervention is required: further syncing is paused until you update your Amazon Photos cookies via Settings, In-App Browser Login, or the 1-Click Bookmarklet.",
            "Once updated, pending photos, deletions, and albums will reprocess automatically.",
        ]
    else:
        title = f"Immich Multi-Sync Alert: {errors} Error(s)"
        lines = [f"A sync scan cycle finished with {errors} error(s)."]

    if error_details:
        lines.append("")
        lines.append("Failure Details:")
        for reason in error_details:
            lines.append(f"- {reason}")

    lines.extend([
        "",
        f"Mirrored: {stats.get('immich_direct_synced', 0)} | "
        f"Amazon: {stats.get('amazon_uploaded', 0)} | "
        f"iCloud: {stats.get('icloud_uploaded', 0)} | "
        f"Deletions: {stats.get('deletions_synced', 0)}",
    ])

    ok, _ = send_apprise_notification(notif.apprise_urls, title=title, body="\n".join(lines))
    return ok


def send_test_notification(urls: List[str], db: Optional[SyncDatabase] = None) -> Tuple[bool, str]:
    """Send a test notification with today's current scan statistics."""
    if db is not None:
        daily_stats = db.get_daily_scan_stats()
        active_count = len(db.get_active_assets())
        albums_count = len(db.get_all_albums())
        auth_pause = db.get_amazon_auth_pause()
        _, summary_body = format_daily_summary(
            daily_stats,
            total_active_assets=active_count,
            total_albums=albums_count,
            auth_pause=auth_pause,
        )
        body = f"Test notification successful!\n\n{summary_body}"
    else:
        body = "Test notification from Immich Multi-Sync Hub! Apprise Telegram delivery is working."
    return send_apprise_notification(
        urls,
        title="Immich Multi-Sync Test Notification",
        body=body,
    )
