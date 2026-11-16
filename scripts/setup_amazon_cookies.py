import json
import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()

def setup_amazon_cookies():
    console.print(Panel.fit(
        "[bold cyan]Amazon Photos Cookie Setup Assistant (Canada / International)[/bold cyan]\n"
        "How to get cookies in 30 seconds:\n"
        "1. Open Chrome/Firefox/Edge and go to [link=https://www.amazon.ca/photos]https://www.amazon.ca/photos[/link]\n"
        "2. Press F12 to open Developer Tools, then click the 'Application' (or 'Storage') tab.\n"
        "3. Under 'Cookies', click 'https://www.amazon.ca' (or your regional domain).\n"
        "4. Find the values for: [bold]session-id[/bold], [bold]ubid-acbca[/bold] (or ubid-main), and [bold]at-acbca[/bold] (or at-main).",
        border_style="cyan"
    ))

    config_path = Path("config.json")
    if not config_path.exists():
        example_path = Path("config.example.json")
        if example_path.exists():
            with open(example_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"amazon": {"cookies": {}}}
    else:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

    tld = input("\nAmazon top-level domain (default: 'ca' for Canada): ").strip() or "ca"
    session_id = input("Enter 'session-id' cookie value: ").strip()
    ubid = input(f"Enter 'ubid-acb{tld}' (or ubid-main) cookie value: ").strip()
    at_token = input(f"Enter 'at-acb{tld}' (or at-main) cookie value: ").strip()

    if not session_id or not at_token:
        console.print("[bold red]Both session-id and authentication token (at-acbca / at-main) are required![/bold red]")
        sys.exit(1)

    if "amazon" not in data:
        data["amazon"] = {}

    ubid_key = "ubid-main" if tld == "com" else f"ubid-acb{tld}"
    at_key = "at-main" if tld == "com" else f"at-acb{tld}"

    data["amazon"]["cookies"] = {
        "session-id": session_id,
        ubid_key: ubid,
        at_key: at_token,
        "ubid-main": ubid,
        "at-main": at_token
    }
    data["amazon"]["tld"] = tld

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    console.print(f"[bold green]✔ Saved Amazon Photos cookies to {config_path.resolve()}[/bold green]")
    console.print("[dim]Run 'python sync.py test-connections' to test your credentials.[/dim]")

if __name__ == "__main__":
    setup_amazon_cookies()
