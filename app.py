import os
import sys
import socket
import argparse
import webbrowser
import threading
import urllib.request
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
import uvicorn
from rich.console import Console
from rich.panel import Panel

from src.ui.app import create_app
from src.clients.amazon_login import ensure_default_desktop


def _load_env_file(env_path: str | Path) -> None:
    p = Path(env_path)
    if p.is_file():
        for k, v in dotenv_values(p).items():
            if k and v and os.environ.get(k, "").strip() == "":
                os.environ.pop(k, None)
        load_dotenv(p, override=False)


_load_env_file(".env")
if os.environ.get("ENV_FILE", "").strip():
    _load_env_file(os.environ["ENV_FILE"].strip())

DATA_DIR = os.environ.get("DATA_DIR", "").strip()
if DATA_DIR:
    os.makedirs(DATA_DIR, exist_ok=True)
    _load_env_file(os.path.join(DATA_DIR, ".env"))
elif os.path.isfile("/app/data/.env"):
    _load_env_file("/app/data/.env")

ensure_default_desktop()

console = Console()

CONFIG_PATH = os.environ.get("CONFIG_PATH") or (os.path.join(DATA_DIR, "config.json") if DATA_DIR else "config.json")
DB_PATH = os.environ.get("DB_PATH") or (os.path.join(DATA_DIR, "sync_state.db") if DATA_DIR else "sync_state.db")
AUTO_DAEMON = os.environ.get("AUTO_DAEMON", "").lower() in ("1", "true", "yes")

app = create_app(config_path=CONFIG_PATH, db_path=DB_PATH, auto_daemon=AUTO_DAEMON)

def is_port_in_use(host: str, port: int) -> bool:
    """Check if a local TCP port is already open and bound."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except OSError:
            return True

def is_immich_sync_running(host: str, port: int) -> bool:
    """Check if an existing Immich Multi-Sync server is actively serving on host:port."""
    try:
        req = urllib.request.Request(
            f"http://{host}:{port}/api/status",
            headers={"User-Agent": "ImmichLauncher"}
        )
        with urllib.request.urlopen(req, timeout=1.5) as resp:
            if resp.status == 200:
                data = resp.read().decode("utf-8")
                return "is_syncing" in data and "db_stats" in data
    except Exception:
        pass
    return False

def find_available_port(host: str, start_port: int, max_attempts: int = 20) -> int:
    """Find the next available port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        if not is_port_in_use(host, port):
            return port
    return start_port

def main():
    parser = argparse.ArgumentParser(description="Immich Multi-Sync Dashboard Launcher")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"), help="Host address to bind to")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "5050")), help="Port to run web UI on")
    parser.add_argument("--no-browser", action="store_true", help="Do not automatically launch web browser")
    parser.add_argument("--daemon", action="store_true", help="Automatically start the continuous sync daemon on startup")
    args = parser.parse_args()

    if args.daemon:
        app.state.auto_daemon = True

    host = args.host
    port = args.port

    # If the default port is already taken, check if Immich Multi-Sync is already running
    if is_port_in_use(host, port):
        if is_immich_sync_running(host, port):
            url = f"http://{host}:{port}"
            console.print(Panel.fit(
                f"[bold green]Immich Multi-Sync is already running![/bold green]\n\n"
                f"Web UI: [link={url}][bold cyan]{url}[/bold cyan][/link]\n\n"
                f"[dim]Opening existing dashboard in your default browser...[/dim]",
                border_style="green"
            ))
            if not args.no_browser:
                webbrowser.open(url)
            sys.exit(0)

        # Port is in use by another application: find a free port
        new_port = find_available_port(host, port)
        if new_port != port:
            console.print(f"[yellow]Note: Port {port} is occupied by another process. Automatically switching to port {new_port}.[/yellow]")
            port = new_port

    url = f"http://{host}:{port}"

    console.print(Panel.fit(
        f"[bold cyan]Immich Multi-Sync Hub Dashboard[/bold cyan]\n\n"
        f"Web UI: [link={url}][bold green]{url}[/bold green][/link]\n"
        f"Config: [dim]{CONFIG_PATH}[/dim]\n"
        f"Database: [dim]{DB_PATH}[/dim]\n\n"
        f"[dim]Opening default browser in 1 second... (Ctrl+C to quit)[/dim]",
        border_style="cyan"
    ))

    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    except OSError as e:
        console.print(f"[bold red]Failed to start server on port {port}: {e}[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
