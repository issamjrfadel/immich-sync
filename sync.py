import sys
import time
import logging
from pathlib import Path
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.config import load_config, AppConfig
from src.engine import SyncEngine
from src.database import SyncDatabase
from src.notifications import maybe_send_daily_summary, maybe_send_error_alert
from src.clients.immich import ImmichClient
from src.clients.icloud import ICloudClient
from src.clients.amazon import AmazonPhotosClient

console = Console()

def setup_logging(verbose: bool):
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

@click.group()
def cli():
    """Immich <-> Amazon Photos <-> iCloud Photos Synchronization Hub"""
    pass

@cli.command()
@click.option("--config", "-c", default="config.json", help="Path to config.json file")
@click.option("--limit", "-l", default=None, type=int, help="Limit number of iCloud photos to ingest (great for testing)")
@click.option("--force-deletes", is_flag=True, help="Bypass max_delete_limit safety guard for deletions")
@click.option("--dry-run", is_flag=True, help="Simulate sync actions without uploading, deleting, or altering data")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose debug logging")
def run(config: str, limit: int, force_deletes: bool, dry_run: bool, verbose: bool):
    """Execute a single synchronization cycle across all 3 services."""
    setup_logging(verbose)
    try:
        cfg = load_config(config)
        if dry_run:
            cfg.sync.dry_run = True

        engine = SyncEngine(cfg)
        console.print(Panel.fit("[bold cyan]Starting Sync Cycle[/bold cyan]", border_style="cyan"))
        stats = engine.run_sync_cycle(limit_intake=limit, force_deletes=force_deletes)
        if not cfg.sync.dry_run:
            db_inst = SyncDatabase()
            db_inst.record_scan_stats(stats)
            maybe_send_error_alert(cfg, stats)
            maybe_send_daily_summary(cfg, db_inst)

        table = Table(title="Sync Results Summary", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="dim")
        table.add_column("Count", justify="right")

        table.add_row("iCloud Ingested", str(stats.get("icloud_ingested", 0)))
        table.add_row("Immich Uploaded", str(stats.get("immich_uploaded", 0)))
        table.add_row("Amazon Uploaded", str(stats.get("amazon_uploaded", 0)))
        table.add_row("iCloud Uploaded", str(stats.get("icloud_uploaded", 0)))
        table.add_row("Direct Immich Synced", str(stats.get("immich_direct_synced", 0)))
        table.add_row("Deletions Synced", str(stats.get("deletions_synced", 0)))
        table.add_row("Albums Synced", str(stats.get("albums_synced", 0)))
        table.add_row("Errors Encountered", str(stats.get("errors", 0)))

        console.print(table)
    except FileNotFoundError as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Sync Failed:[/bold red] {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)

@cli.command()
@click.option("--config", "-c", default="config.json", help="Path to config.json file")
@click.option("--interval", "-i", default=None, type=int, help="Override polling interval in seconds")
@click.option("--force-deletes", is_flag=True, help="Bypass deletion limit guard")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose debug logging")
def daemon(config: str, interval: int, force_deletes: bool, verbose: bool):
    """Run continuously in background daemon mode on a polling schedule."""
    setup_logging(verbose)
    cfg = load_config(config)
    poll_sec = interval or cfg.sync.poll_interval_seconds
    engine = SyncEngine(cfg)

    console.print(Panel.fit(
        f"[bold green]Immich Multi-Sync Daemon Started[/bold green]\n"
        f"Polling interval: every {poll_sec} seconds (Press Ctrl+C to stop)",
        border_style="green"
    ))

    try:
        while True:
            try:
                stats = engine.run_sync_cycle(force_deletes=force_deletes)
                if not cfg.sync.dry_run:
                    db_inst = SyncDatabase()
                    db_inst.record_scan_stats(stats)
                    maybe_send_error_alert(cfg, stats)
                    maybe_send_daily_summary(cfg, db_inst)
            except Exception as e:
                console.print(f"[bold red]Cycle Error:[/bold red] {e}")

            console.print(f"[dim]Sleeping for {poll_sec}s until next sync cycle...[/dim]")
            time.sleep(poll_sec)
    except KeyboardInterrupt:
        console.print("\n[yellow]Daemon stopped by user.[/yellow]")

@cli.command("login-icloud")
@click.option("--config", "-c", default="config.json", help="Path to config.json file")
def login_icloud(config: str):
    """Interactively authenticate with iCloud and complete Two-Factor Authentication (2FA)."""
    cfg = load_config(config)
    client = ICloudClient(cfg.icloud)
    console.print(Panel.fit(
        f"[bold blue]Authenticating Apple ID:[/bold blue] {cfg.icloud.apple_id}\n"
        f"Session tokens will be stored in: {cfg.icloud.session_dir}",
        border_style="blue"
    ))

    try:
        client.connect(interactive_2fa=True)
        console.print("[bold green]✔ Successfully authenticated and trusted iCloud session![/bold green]")
        console.print("[green]Headless and background daemon tasks can now access your iCloud library without prompts.[/green]")
    except Exception as e:
        console.print(f"[bold red]iCloud Login Failed:[/bold red] {e}")
        sys.exit(1)

@cli.command("test-connections")
@click.option("--config", "-c", default="config.json", help="Path to config.json file")
def test_connections(config: str):
    """Test API credentials and connectivity for Immich, iCloud, and Amazon Photos."""
    cfg = load_config(config)
    table = Table(title="Service Connection Status", show_header=True, header_style="bold cyan")
    table.add_column("Service", style="bold")
    table.add_column("Status")
    table.add_column("Details")

    # 1. Immich
    try:
        immich = ImmichClient(cfg.immich)
        user = immich.test_connection()
        email = user.get("email", "connected")
        table.add_row("Immich", "[green]CONNECTED[/green]", f"Logged in as {email} ({cfg.immich.url})")
    except Exception as e:
        table.add_row("Immich", "[red]FAILED[/red]", str(e))

    # 2. iCloud
    try:
        icloud = ICloudClient(cfg.icloud)
        api = icloud.connect(interactive_2fa=False)
        table.add_row("iCloud", "[green]CONNECTED[/green]", f"Session valid for {cfg.icloud.apple_id}")
    except RuntimeError as e:
        table.add_row("iCloud", "[yellow]NEEDS 2FA[/yellow]", "Run 'python sync.py login-icloud'")
    except Exception as e:
        table.add_row("iCloud", "[red]FAILED[/red]", str(e))

    # 3. Amazon Photos
    try:
        amazon = AmazonPhotosClient(cfg.amazon)
        info = amazon.test_connection()
        table.add_row("Amazon Photos", "[green]CONNECTED[/green]", f"Root folder ID: {info.get('root_id')}")
    except Exception as e:
        table.add_row("Amazon Photos", "[red]FAILED[/red]", str(e))

    console.print(table)

@cli.command()
@click.option("--db", default="sync_state.db", help="Path to SQLite database")
def status(db: str):
    """Display database statistics and tracked items."""
    db_inst = SyncDatabase(db)
    active = db_inst.get_active_assets()
    albums = db_inst.get_all_albums()

    console.print(Panel.fit(
        f"[bold]Total Active Tracked Assets:[/bold] {len(active)}\n"
        f"[bold]Total Tracked Albums:[/bold] {len(albums)}",
        title="Sync Database Status",
        border_style="magenta"
    ))

    if albums:
        album_table = Table(title="Tracked Albums", show_header=True)
        album_table.add_column("Album Name")
        album_table.add_column("Immich ID")
        album_table.add_column("Amazon ID")
        album_table.add_column("Last Synced")
        for a in albums:
            album_table.add_row(
                a.get("name") or "Unnamed",
                a.get("immich_id") or "-",
                a.get("amazon_id") or "-",
                str(a.get("last_synced_at") or "-")
            )
        console.print(album_table)

@cli.command()
@click.option("--host", default="127.0.0.1", help="Host interface to bind Web UI")
@click.option("--port", default=5050, type=int, help="Port to serve Web UI")
@click.option("--config", "-c", default="config.json", help="Path to config.json file")
@click.option("--db", default="sync_state.db", help="Path to SQLite database")
@click.option("--no-browser", is_flag=True, help="Do not automatically open browser on startup")
def ui(host: str, port: int, config: str, db: str, no_browser: bool):
    """Launch the interactive Web UI Dashboard."""
    import webbrowser
    import threading
    import uvicorn
    from src.ui.app import create_app

    app = create_app(config_path=config, db_path=db)
    url = f"http://{host}:{port}"
    console.print(Panel.fit(
        f"[bold green]Immich Multi-Sync Web UI Dashboard[/bold green]\n"
        f"Serving at: [link={url}]{url}[/link]\n"
        f"Configuration: {config}\n"
        f"Database: {db}\n"
        f"[dim]Press Ctrl+C to stop the server.[/dim]",
        border_style="green"
    ))

    if not no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    uvicorn.run(app, host=host, port=port, log_level="warning")

if __name__ == "__main__":
    cli()
