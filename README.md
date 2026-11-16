# Immich Multi-Sync: Immich ↔ Amazon Photos ↔ iCloud Photos

A robust Python synchronization engine that bridges **Immich**, **iCloud Photos**, and **Amazon Photos**.

## Highlights & Capabilities

- **Automatic Ingestion:** Automatically downloads new full-resolution original photos/videos from iCloud (e.g. taken on iPhone) and uploads them to both your self-hosted **Immich** server and **Amazon Photos**.
- **Bidirectional Ingestion from Immich:** If you upload photos directly to Immich from other cameras or your PC, they are automatically replicated to **both Amazon Photos and iCloud Photos**.
- **Deletion Synchronization:** When you delete a photo in Immich, it is safely propagated:
  - Moved to **Trash** in Amazon Photos.
  - Moved to **Recently Deleted** in iCloud Photos (allowing 30-day recovery on iOS).
- **Safety Deletion Guard:** Configurable safety limit (`max_delete_limit`, default: 50). If more than 50 files are flagged as deleted in a single pass (e.g., if Immich is offline or re-indexing), deletion sync halts immediately to prevent mass data loss.
- **Album Synchronization:** When you create an album and add photos to it in Immich, matching albums are automatically created in Amazon Photos and iCloud Photos with the same photos.
- **State Store (SQLite):** Tracks SHA-1 and MD5 hashes, cross-service asset IDs, and sync history in `sync_state.db`. Deduplication runs before downloading or uploading.

---

## Architecture & Data Flow

```
   ┌────────────────────────────────────────────────────────┐
   │                     Apple iCloud                       │
   │            (Photos from iPhone / iPad)                 │
   └───────────────────────────▲────────────────────────────┘
                               │ Pull originals & uploads
                               │ Propagate deletions & albums
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │               immich-sync Engine                       │
   │        (SQLite State DB: Hash & ID Mapping)            │
   └───────────────▲────────────────────────▲───────────────┘
                   │                        │
       Full 2-Way Sync / Deletes        Uploads / Trash / Albums
                   ▼                        ▼
   ┌───────────────────────────┐    ┌───────────────────────┐
   │          Immich           │    │     Amazon Photos     │
   │     (Self-Hosted Hub)     │    │ (Prime Cloud Backup)  │
   └───────────────────────────┘    └───────────────────────┘
```

---

## Web UI Dashboard

The app includes an interactive **Web UI Dashboard** with real-time log streaming, service health checks, metrics, and one-click sync controls.

### Launching the Web UI:
- **Option 1 (Double-Click on Windows):** Run [`run_ui.bat`](file:///c:/Users/Issam/Documents/antigravity/immich-sync/run_ui.bat)
- **Option 2 (Direct Python):**
  ```powershell
  .\.venv\Scripts\python.exe app.py
  ```
- **Option 3 (CLI):**
  ```powershell
  .\.venv\Scripts\python.exe sync.py ui
  ```
This serves on `http://127.0.0.1:5050` and automatically opens in your default browser.

### Web UI Features:
1. **Live Service Health Cards:** Real-time connection status for Immich, Amazon Photos, and iCloud Photos.
2. **One-Click Sync Controls:** Trigger full sync with optional Dry-Run and Force-Deletes checkboxes.
3. **Continuous Background Daemon:** Toggle continuous background sync on/off directly from the header switch.
4. **Live Activity Terminal:** Real-time log streaming via WebSockets.
5. **Interactive iCloud 2FA Modal:** Trigger and submit Apple 2FA verification codes directly in the browser.
6. **Tracked Assets Explorer:** Searchable, paginated table of all tracked assets with status badges and SHA-1/MD5 hashes.
7. **Synced Albums Viewer:** Visual cards displaying mapped albums and cross-service IDs.
8. **Configuration Manager:** Edit Immich, iCloud, Amazon, and sync policy settings right in the browser with one-click connection tests.

---

## Quick Start Setup

### 1. Requirements & Virtual Environment
Ensure Python 3.10+ is installed:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure Credentials (`config.json`)
Copy the template configuration file:
```powershell
Copy-Item config.example.json config.json
```

Edit `config.json`:
- **Immich**:
  - `url`: Your Immich server address (e.g. `http://192.168.1.50:2283` or `https://immich.yourdomain.com`).
  - `api_key`: In Immich web UI, go to **Account Settings** -> **API Keys** -> **New API Key**.
- **iCloud**:
  - `apple_id`: Your Apple ID email address.
  - `password`: Your Apple ID password.
- **Amazon Photos**:
  - Use the built-in helper script to set up cookies:
    ```powershell
    python scripts\setup_amazon_cookies.py
    ```
    Or paste `session-id`, `ubid-main`, and `at-main` into `config.json` manually (instructions below).

### 3. Authenticate iCloud Two-Factor (2FA)
Run the one-time interactive login command:
```powershell
python sync.py login-icloud
```
Enter the 6-digit verification code sent to your Apple devices. The trusted session token will be saved in `.icloud_session/`, allowing all future syncs and background daemons to run completely unattended.

### 4. Verify All Connections
```powershell
python sync.py test-connections
```
This tests your connection to Immich, iCloud, and Amazon Photos and displays a status table.

---

## How to Extract Amazon Photos Cookies

Because Amazon does not provide a public third-party REST API for Amazon Photos, the sync engine uses session authentication:
1. Open Chrome, Edge, or Firefox and log in to [https://www.amazon.com/photos](https://www.amazon.com/photos).
2. Press `F12` to open Developer Tools, then navigate to the **Application** (Chrome/Edge) or **Storage** (Firefox) tab.
3. Under the **Cookies** tree, click `https://www.amazon.com`.
4. Copy the values of these 3 cookies:
   - `session-id`
   - `ubid-main` (or regional variant e.g. `ubid-acbca` for Canada, `ubid-acbuk` for UK)
   - `at-main` (or regional variant e.g. `at-acbca`)
5. Run `python scripts\setup_amazon_cookies.py` and paste them when prompted.

---

## CLI Usage Reference

### Run a Single Sync Cycle
```powershell
# Standard run
python sync.py run

# Test run with dry-run (no files downloaded/uploaded/deleted)
python sync.py run --dry-run

# Ingest only 5 photos for initial testing
python sync.py run --limit 5

# Override deletion safety limit if deleting in bulk
python sync.py run --force-deletes
```

### Run as a Continuous Background Daemon
```powershell
# Polls every 300 seconds (5 minutes) by default
python sync.py daemon

# Or customize polling interval (e.g., every 60 seconds)
python sync.py daemon --interval 60
```

Or execute via the PowerShell script:
```powershell
.\scripts\run_daemon.ps1
```

### View Database Sync Status
```powershell
python sync.py status
```
Displays total active tracked photos, mapped albums across services, and last sync timestamps.

---

## Running the Automated Test Suite

```powershell
python -m unittest discover -s tests
```
All unit tests verify database persistence, SHA-1/MD5 hashing, deletion propagation, safety guards, and album synchronization.
