"""
Automated Amazon Browser Login & Cookie Capture using Playwright.
Launches a real Chrome window, lets the user log into Amazon,
and automatically extracts and verifies session cookies.
"""
import os
import sys
import logging
import time
import threading
from pathlib import Path
from typing import Dict, Any, Optional, Callable, Tuple

logger = logging.getLogger("immich_sync.amazon_login")

_AUTH_CHALLENGE_MARKERS = (
    "/signin",
    "/ap/signin",
    "openid.return_to",
    "/ap/cvf",
    "approval",
    "captcha",
    "/ap/mfa",
)


def _extract_amazon_cookies(all_cookies: list, tld: str) -> Dict[str, str]:
    """Filter Playwright cookie list, preferring exact amazon.<tld> domain over other Amazon TLDs."""
    clean_tld = (tld or "ca").lower().strip()
    exact_suffix = f"amazon.{clean_tld}"
    other_cookies: Dict[str, str] = {}
    exact_cookies: Dict[str, str] = {}
    for c in (all_cookies or []):
        domain = (c.get("domain") or "").lower()
        name = c.get("name")
        value = c.get("value")
        if not name or not value:
            continue
        if exact_suffix in domain:
            exact_cookies[name] = value
        elif "amazon" in domain:
            other_cookies[name] = value
    # Exact marketplace cookies win so stale .amazon.com cookies cannot overwrite .amazon.ca
    return {**other_cookies, **exact_cookies}


def _get_target_cookies(context, target_url: str, tld: str) -> Dict[str, str]:
    try:
        raw = context.cookies([target_url])
    except TypeError:
        raw = context.cookies()
    except Exception:
        raw = context.cookies()
    if not raw:
        try:
            raw = context.cookies()
        except Exception:
            raw = []
    return _extract_amazon_cookies(raw, tld)


def _has_required_amazon_tokens(cookie_dict: Dict[str, str], tld: str) -> bool:
    session_id = cookie_dict.get("session-id")
    at_token = (
        cookie_dict.get(f"at-acb{tld}")
        or cookie_dict.get("at-main")
        or cookie_dict.get("at")
    )
    return bool(session_id and at_token)


def _is_auth_challenge_url(url: str) -> bool:
    lower = (url or "").lower()
    return any(marker in lower for marker in _AUTH_CHALLENGE_MARKERS)


def _launch_amazon_context(playwright, user_data_dir: str, headless: bool):
    """Launch a persistent Chromium/Chrome context against user_data_dir."""
    is_linux = sys.platform.startswith("linux")
    launch_args = [
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
    ]
    if is_linux:
        launch_args.extend([
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--window-size=1280,800",
            "--window-position=0,0",
        ])
        return playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=headless,
            args=launch_args,
            viewport={"width": 1280, "height": 800},
        )

    if not headless:
        launch_args.append("--start-maximized")
    try:
        return playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            channel="chrome",
            headless=headless,
            args=launch_args,
            viewport={"width": 1280, "height": 800} if headless else None,
        )
    except Exception as e_chrome:
        logger.warning(f"Could not launch Chrome via channel='chrome' ({e_chrome}). Falling back to Chromium.")
        return playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=headless,
            args=launch_args,
            viewport={"width": 1280, "height": 800} if headless else None,
        )


def try_refresh_amazon_cookies_from_profile(
    tld: str = "ca",
    user_data_dir: Optional[str] = None,
    timeout_seconds: int = 35,
) -> Tuple[bool, Dict[str, str], str]:
    """
    Attempt a non-interactive refresh of Amazon Photos cookies using an existing
    saved Chromium profile (.amazon_browser_profile). Never attempts to solve
    passwords, 2FA, or CAPTCHAs; returns (False, {}, reason) if manual sign-in is required.
    """
    clean_tld = (tld or "ca").lower().strip()
    profile_dir = Path(user_data_dir or "./.amazon_browser_profile").resolve()
    if not profile_dir.exists() or not any(profile_dir.iterdir()):
        return False, {}, "No saved browser session profile found; manual intervention is required."

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        return False, {}, f"Playwright unavailable for automatic cookie refresh ({e})."

    # Inside Docker/Xvfb (DISPLAY set on Linux), headless=False runs inside virtual :99 matching interactive login.
    has_linux_display = sys.platform.startswith("linux") and bool(
        os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
    )
    use_headless = not has_linux_display

    target_url = f"https://www.amazon.{clean_tld}/photos"
    logger.info(
        f"Attempting silent Amazon cookie refresh via saved browser profile at {target_url} "
        f"(headless={use_headless})..."
    )

    try:
        from src.clients.amazon import AmazonPhotosClient, amazon_cookie_fingerprint
        from src.config import AmazonConfig

        with sync_playwright() as p:
            context = _launch_amazon_context(p, str(profile_dir), headless=use_headless)
            try:
                page = context.pages[0] if context.pages else context.new_page()
                try:
                    page.goto(target_url, wait_until="load", timeout=min(timeout_seconds, 25) * 1000)
                except Exception as nav_err:
                    logger.debug(f"Amazon cookie refresh navigation notice: {nav_err}")

                deadline = time.time() + max(10, timeout_seconds)
                auth_first_seen: Optional[float] = None
                auth_grace_seconds = 12.0
                tested_fps = set()

                while time.time() < deadline:
                    cur_url = ""
                    try:
                        cur_url = page.url
                    except Exception:
                        pass

                    on_auth_url = _is_auth_challenge_url(cur_url)
                    if on_auth_url:
                        if auth_first_seen is None:
                            auth_first_seen = time.time()
                        elif time.time() - auth_first_seen >= auth_grace_seconds:
                            return False, {}, "Amazon session requires interactive sign-in or verification; manual intervention is required."
                    else:
                        auth_first_seen = None
                        cookie_dict = _get_target_cookies(context, target_url, clean_tld)
                        if _has_required_amazon_tokens(cookie_dict, clean_tld):
                            fp = amazon_cookie_fingerprint(cookie_dict, clean_tld)
                            if fp not in tested_fps:
                                tested_fps.add(fp)
                                try:
                                    client = AmazonPhotosClient(AmazonConfig(cookies=cookie_dict, tld=clean_tld))
                                    client.test_connection(quiet=True)
                                    logger.info(f"Successfully refreshed Amazon Photos cookies automatically for amazon.{clean_tld}.")
                                    return True, cookie_dict, f"Automatically refreshed Amazon Photos session (amazon.{clean_tld})."
                                except Exception as verify_err:
                                    logger.debug(f"Auto-refresh cookie verification retry: {verify_err}")

                    time.sleep(1.5)

                return False, {}, "Timed out waiting for valid Amazon Photos cookies; manual intervention is required."
            finally:
                try:
                    context.close()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Silent Amazon cookie refresh failed: {e}")
        return False, {}, f"Automatic Amazon cookie refresh failed ({e}); manual intervention is required."

def ensure_default_desktop() -> bool:
    """
    On Windows, ensures the current thread is attached to the interactive
    user desktop ('WinSta0\\Default'). This allows GUI windows (like Chrome)
    spawned by background threads or services to appear on the user's primary monitor.
    """
    if os.name != "nt":
        return True
    try:
        import ctypes
        from ctypes import wintypes
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        DESKTOP_ALL_ACCESS = 0x01FF
        OpenDesktopA = user32.OpenDesktopA
        OpenDesktopA.restype = wintypes.HANDLE
        OpenDesktopA.argtypes = [wintypes.LPCSTR, wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        SetThreadDesktop = user32.SetThreadDesktop
        SetThreadDesktop.restype = wintypes.BOOL
        SetThreadDesktop.argtypes = [wintypes.HANDLE]

        hDesktop = OpenDesktopA(b"Default", 0, False, DESKTOP_ALL_ACCESS)
        if hDesktop:
            if SetThreadDesktop(hDesktop):
                logger.info("Successfully attached thread to interactive 'Default' desktop.")
                return True
            else:
                logger.warning(f"SetThreadDesktop failed with code: {ctypes.get_last_error()}")
        else:
            logger.warning(f"OpenDesktopA('Default') failed with code: {ctypes.get_last_error()}")
    except Exception as e:
        logger.warning(f"Could not attach thread to Default desktop: {e}")
    return False


class AmazonBrowserLogin:
    def __init__(self, tld: str = "ca", user_data_dir: Optional[str] = None):
        self.tld = (tld or "ca").lower().strip()
        self.user_data_dir = user_data_dir or str(Path("./.amazon_browser_profile").resolve())
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.status: Dict[str, Any] = {
            "state": "idle",  # "idle", "launching", "waiting_for_user", "verifying", "success", "error", "cancelled"
            "message": "Ready to launch Amazon sign-in window.",
            "tld": self.tld,
            "cookies": {},
            "user_info": {}
        }
        self.on_success: Optional[Callable[[Dict[str, str], str], None]] = None

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self.status)

    def cancel(self):
        with self._lock:
            if self.status["state"] in ("launching", "waiting_for_user", "verifying"):
                self.status["state"] = "cancelled"
                self.status["message"] = "Login cancelled by user."
            self.stop_event.set()

    def start(self, on_success: Optional[Callable[[Dict[str, str], str], None]] = None) -> bool:
        with self._lock:
            if self.status["state"] in ("launching", "waiting_for_user", "verifying"):
                return False  # Already in progress
            self.stop_event.clear()
            self.on_success = on_success
            self.status = {
                "state": "launching",
                "message": "Launching browser for Amazon sign-in...",
                "tld": self.tld,
                "cookies": {},
                "user_info": {}
            }

        self.thread = threading.Thread(target=self._run_browser_flow, daemon=True)
        self.thread.start()
        return True

    def _run_browser_flow(self):
        ensure_default_desktop()

        if sys.platform.startswith("linux") and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            msg = (
                "Interactive browser sign-in is unavailable inside a headless Docker/Unraid container "
                "(no desktop display). Use the 1-Click Cookie Bookmarklet or paste your Amazon cookies "
                "(session-id, ubid-acbca, at-acbca) directly under Settings -> Amazon Photos."
            )
            logger.warning(msg)
            with self._lock:
                self.status["state"] = "error"
                self.status["message"] = msg
            return

        from playwright.sync_api import sync_playwright

        target_url = f"https://www.amazon.{self.tld}/photos"
        logger.info(f"Starting Amazon browser login for amazon.{self.tld} at {target_url}...")

        try:
            with sync_playwright() as p:
                context = _launch_amazon_context(p, self.user_data_dir, headless=False)

                page = context.pages[0] if context.pages else context.new_page()

                novnc_enabled = os.environ.get("ENABLE_NOVNC", "").lower() in ("1", "true", "yes")
                with self._lock:
                    self.status["state"] = "waiting_for_user"
                    if novnc_enabled:
                        self.status["message"] = "Browser ready! Sign in to Amazon below — cookies will be captured automatically."
                    else:
                        self.status["message"] = "Please sign in to Amazon in the opened browser window."

                try:
                    page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                except Exception as nav_err:
                    logger.warning(f"Initial navigation notice: {nav_err}")

                start_time = time.time()
                timeout_seconds = 300  # 5 minutes

                while not self.stop_event.is_set():
                    if time.time() - start_time > timeout_seconds:
                        with self._lock:
                            self.status["state"] = "error"
                            self.status["message"] = "Sign-in timed out after 5 minutes."
                        break

                    # Check if all browser pages were closed by user
                    try:
                        if not context.pages or all(pg.is_closed() for pg in context.pages):
                            with self._lock:
                                if self.status["state"] != "success":
                                    self.status["state"] = "cancelled"
                                    self.status["message"] = "Browser window was closed."
                            break
                    except Exception:
                        break

                    # Check current page URL & cookies
                    try:
                        cookie_dict = _get_target_cookies(context, target_url, self.tld)

                        cur_url = ""
                        try:
                            cur_url = page.url
                        except Exception:
                            pass

                        # Not in active signin or MFA challenge page
                        if _has_required_amazon_tokens(cookie_dict, self.tld) and not _is_auth_challenge_url(cur_url):
                            with self._lock:
                                self.status["state"] = "verifying"
                                self.status["message"] = "Session cookies detected! Verifying with Amazon Photos..."

                            from src.clients.amazon import AmazonPhotosClient
                            from src.config import AmazonConfig

                            test_cfg = AmazonConfig(cookies=cookie_dict, tld=self.tld)
                            client = AmazonPhotosClient(test_cfg)
                            try:
                                info = client.test_connection(quiet=True)
                                with self._lock:
                                    self.status["state"] = "success"
                                    self.status["message"] = f"Connected to Amazon Photos (amazon.{self.tld})!"
                                    self.status["cookies"] = cookie_dict
                                    self.status["user_info"] = info

                                logger.info(f"Amazon browser login confirmed for amazon.{self.tld} (Root: {info.get('root_id')})")

                                if self.on_success:
                                    try:
                                        self.on_success(cookie_dict, self.tld)
                                    except Exception as cb_err:
                                        logger.error(f"Error in on_success callback: {cb_err}")

                                time.sleep(1.5)
                                break
                            except Exception as test_err:
                                logger.debug(f"Amazon test connection retry: {test_err}")

                    except Exception as poll_err:
                        logger.debug(f"Cookie polling error: {poll_err}")

                    time.sleep(1.5)

                try:
                    context.close()
                except Exception:
                    pass

        except Exception as main_err:
            logger.error(f"Amazon browser login failed: {main_err}", exc_info=True)
            err_str = str(main_err)
            err_lower = err_str.lower()
            if "connection closed while reading from the driver" in err_lower or "executable doesn't exist" in err_lower:
                msg = (
                    "Browser sign-in could not launch Chrome/Playwright in this environment "
                    "(expected on headless Docker/Unraid). Use the 1-Click Cookie Bookmarklet or paste your "
                    "Amazon cookies manually under Settings -> Amazon Photos."
                )
            else:
                msg = f"Error during sign-in: {err_str}"
            with self._lock:
                self.status["state"] = "error"
                self.status["message"] = msg


def run_cli_login(tld: str = "ca", config_path: str = "config.json"):
    """CLI runner for direct desktop login."""
    from rich.console import Console
    from rich.panel import Panel
    from src.config import load_config, save_config

    console = Console()
    console.print(Panel.fit(
        f"[bold amber]Amazon Photos Browser Login Helper[/bold amber]\n\n"
        f"Marketplace: [cyan]amazon.{tld}[/cyan]\n"
        f"A Chrome window will open. Please log in to your Amazon account.\n"
        f"Once signed in, cookies will be captured and saved automatically to [bold]{config_path}[/bold].",
        border_style="yellow"
    ))

    login_mgr = AmazonBrowserLogin(tld=tld)

    def on_success(cookies, resolved_tld):
        try:
            cfg = load_config(config_path)
            cfg.amazon.tld = resolved_tld
            cfg.amazon.cookies = cookies
            cfg.amazon.enabled = True
            cfg.sync.enable_amazon = True
            save_config(cfg, config_path)
            console.print("[bold green]✓ Configuration updated and saved to config.json![/bold green]")
        except Exception as e:
            console.print(f"[bold red]Failed to save config: {e}[/bold red]")

    login_mgr.start(on_success=on_success)

    try:
        last_msg = ""
        while True:
            st = login_mgr.get_status()
            msg = st.get("message", "")
            if msg != last_msg:
                console.print(f"[dim]{time.strftime('%H:%M:%S')}[/dim] {msg}")
                last_msg = msg

            if st.get("state") == "success":
                console.print(Panel.fit(
                    "[bold green]✓ Amazon Photos successfully connected and verified![/bold green]\n"
                    "You may now run `run_ui.bat` or `python sync.py`.",
                    border_style="green"
                ))
                break
            elif st.get("state") in ("error", "cancelled"):
                console.print(f"[bold red]Login ended: {st.get('message')}[/bold red]")
                break
            time.sleep(1.0)
    except KeyboardInterrupt:
        console.print("[yellow]Cancelling login...[/yellow]")
        login_mgr.cancel()


if __name__ == "__main__":
    tld_arg = "ca"
    if len(sys.argv) > 1:
        tld_arg = sys.argv[1].replace("--tld=", "")
    run_cli_login(tld=tld_arg)
