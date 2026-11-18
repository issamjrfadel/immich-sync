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


def format_daily_summary(
    daily_stats: Dict[str, Any],
    total_active_assets: int = 0,
    total_albums: int = 0,
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

    status_badge = "OK" if errors == 0 else f"Warnings ({errors} errors)"
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
        "",
        f"Library Totals: {total_active_assets} tracked assets | {total_albums} albums",
    ]
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
    title, body = format_daily_summary(daily_stats, total_active_assets=active_count, total_albums=albums_count)

    ok, msg = send_apprise_notification(notif.apprise_urls, title=title, body=body)
    if ok:
        db.mark_daily_notified(today_key)
        db.log_sync("NOTIFY", f"Sent daily summary for {today_key}")
        logger.info(f"Daily scan statistics notification sent for {today_key}.")
        return True

    logger.warning(f"Daily scan statistics notification failed: {msg}")
    return False


def maybe_send_error_alert(cfg: AppConfig, stats: Dict[str, Any]) -> bool:
    """Send an immediate alert when a scan cycle has errors and notify_on_error is enabled."""
    notif = getattr(cfg, "notifications", None)
    if not notif or not notif.enabled or not notif.notify_on_error or not notif.apprise_urls:
        return False

    errors = int(stats.get("errors", 0) or 0)
    if errors <= 0:
        return False

    title = f"Immich Multi-Sync Alert: {errors} Error(s)"
    body = (
        f"A sync scan cycle finished with {errors} error(s).\n"
        f"Mirrored: {stats.get('immich_direct_synced', 0)} | "
        f"Amazon: {stats.get('amazon_uploaded', 0)} | "
        f"iCloud: {stats.get('icloud_uploaded', 0)} | "
        f"Deletions: {stats.get('deletions_synced', 0)}"
    )
    ok, _ = send_apprise_notification(notif.apprise_urls, title=title, body=body)
    return ok


def send_test_notification(urls: List[str], db: Optional[SyncDatabase] = None) -> Tuple[bool, str]:
    """Send a test notification with today's current scan statistics."""
    if db is not None:
        daily_stats = db.get_daily_scan_stats()
        active_count = len(db.get_active_assets())
        albums_count = len(db.get_all_albums())
        _, summary_body = format_daily_summary(daily_stats, total_active_assets=active_count, total_albums=albums_count)
        body = f"Test notification successful!\n\n{summary_body}"
    else:
        body = "Test notification from Immich Multi-Sync Hub! Apprise Telegram delivery is working."
    return send_apprise_notification(
        urls,
        title="Immich Multi-Sync Test Notification",
        body=body,
    )
