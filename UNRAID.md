# Running Immich Multi-Sync on Unraid (Daemon Mode)

This project is packaged as a Docker container that runs both the **Web Dashboard** (port `5050`) and the **continuous background sync daemon** (`AUTO_DAEMON=true` / `--daemon`).

---

## 1. Fixing an "Orphaned Image" in Unraid

When you use a local-only repository name like `immich-sync:latest` without a container registry prefix, Unraid's `dockerMan` tries to pull from Docker Hub (`docker.io/library/immich-sync:latest`) or fails partway through container creation, leaving the image untagged/unattached as an **Orphaned Image** in the Docker tab.

To clean up an orphaned image in Unraid:
1. Enable **Basic View -> Advanced View** (toggle at the top-right of Unraid's **Docker** tab).
2. Click the orphaned image icon (or **Remove** under Orphaned Images) and confirm removal (also run `docker image prune` in Unraid Terminal if desired).
3. Switch to the **hosted GitHub Container Registry (`ghcr.io`)** setup below so Unraid pulls and updates cleanly without ever orphaning the image.

---

## 2. Hosted Installer & 1-Click Updates via GitHub Container Registry (`ghcr.io`)

This repository includes `.github/workflows/docker-publish.yml`, which automatically builds and publishes `ghcr.io/<your-github-username>/immich-sync:latest` whenever you push updates.

### Initial Setup (Once on Windows)
1. Create a GitHub repository (e.g., `immich-sync`).
2. Push this project from your Windows workspace:
   ```powershell
   git init
   git branch -M main
   git add -A
   git commit -m "Initial immich-sync setup"
   git remote add origin https://github.com/YOUR_GITHUB_USERNAME/immich-sync.git
   git push -u origin main
   ```
3. In GitHub, wait ~2 minutes for the **Build & Publish Docker Image** Action to turn green.
4. Under your GitHub profile/repo **Packages -> immich-sync -> Package settings**, set package visibility to **Public** (or keep it **Private** and run `docker login ghcr.io` once in Unraid Terminal with a GitHub PAT having `read:packages`).

### Pushing Updates Anytime
* Double-click `publish_update.bat` or run `.\publish_update.ps1 -Message "Your update note"` from Windows (it commits, pushes to GitHub, and triggers GitHub Actions to rebuild `:latest`).
* Or push directly from Docker Desktop with `.\publish_update.ps1 -DirectDocker`.
* Then in **Unraid -> Docker**, click **Check for Updates** -> **Apply Update** on `immich-sync`!

---

## 3. Configuring Everything via `.env` (`/mnt/user/appdata/immich-sync/.env`)

Instead of manually editing `config.json` or typing all secrets into Unraid XML fields, copy `.env.example` to `/mnt/user/appdata/immich-sync/.env` and fill in your values:

* Any credentials/settings in `.env` automatically **bootstrap `config.json`** on first boot if `config.json` does not exist yet.
* Non-empty variables in `.env` also **override `config.json`** on startup/reload (`IMMICH_URL`, `IMMICH_API_KEY`, `ICLOUD_APPLE_ID`, `ICLOUD_PASSWORD`, `ICLOUD_UPLOAD_VIDEOS`, `AMAZON_TLD`, `AMAZON_SESSION_ID`, `AMAZON_UBID`, `AMAZON_AT_TOKEN`, `AMAZON_UPLOAD_VIDEOS`, `POLL_INTERVAL_SECONDS`, `MAX_DELETE_LIMIT`, `DASHBOARD_USER`, `DASHBOARD_PASSWORD`, etc.).
* Empty variables in `.env` are ignored so your saved cookies/sessions in `config.json` are never wiped.
* `docker-entrypoint.sh` automatically applies `chmod 600` to `/app/data/.env` and `/app/data/config.json`.

*(Optional migration)* If you already synced from Windows, also copy `sync_state.db`, `config.json`, and `.icloud_session/` into `/mnt/user/appdata/immich-sync/` so existing sync state is preserved without re-uploading anything.

> **Tip:** Make sure `IMMICH_URL` uses your Unraid/Immich LAN IP (e.g. `http://10.0.0.55:2283`), not `localhost`.

---

## 4. Install & Run on Unraid

### Option A: Native Unraid Docker UI (Template)
1. Copy `unraid/my-immich-sync.xml` to your Unraid flash drive and replace `YOUR_GITHUB_USERNAME` with your GitHub username (lowercase):
   ```bash
   cp /path/to/unraid/my-immich-sync.xml /boot/config/plugins/dockerMan/templates-user/my-immich-sync.xml
   ```
   *(Or in Unraid **Docker -> Add Container**, simply enter `ghcr.io/<your-github-username>/immich-sync:latest` in **Repository**.)*
2. Verify settings:
   * **Repository**: `ghcr.io/<your-github-username>/immich-sync:latest`
   * **Web UI Port**: `5050`
   * **AppData Directory**: `/mnt/user/appdata/immich-sync` -> `/app/data`
   * **Auto-Start Sync Daemon**: `true`
3. Click **Apply**. The container pulls from GHCR, reads `/mnt/user/appdata/immich-sync/.env`, and starts with `--restart unless-stopped`.

### Option B: Unraid Compose Manager (or CLI)
1. Put `.env` inside `/mnt/user/appdata/immich-sync/.env` (or alongside `docker-compose.yml`).
2. Update `docker-compose.yml` to point to `image: ghcr.io/<your-github-username>/immich-sync:latest` (remove `build: .`) and `/mnt/user/appdata/immich-sync:/app/data`.
3. Run:
   ```bash
   docker compose pull && docker compose up -d
   ```

*(Fallback local build)* If you prefer building locally without a registry, keep `build: .` in `docker-compose.yml` and use `docker compose up -d --build` (not standalone `my-immich-sync.xml`, since `dockerMan` expects a pullable registry image for updates).

---

## 5. Accessing the Web UI & Refreshing Cookies

* Open `http://<UNRAID-IP>:5050` in your browser.
* The daemon starts automatically (`AUTO_DAEMON=true`) and polls at `POLL_INTERVAL_SECONDS` (default `300s`).
* Because the container runs headless, use the **1-Click Bookmarklet** or **manual cookie paste** in the Web UI Settings (or update `AMAZON_*` in `.env` and restart) to refresh Amazon Photos cookies, and use the built-in **2FA Verification** modal for iCloud.

---

## 6. Security Features

* **Web UI Password Protection (`DASHBOARD_PASSWORD`)**: Set `DASHBOARD_PASSWORD` (and optionally `DASHBOARD_USER`, default `admin`) in `.env`, your Unraid container template, or `docker-compose.yml` to require HTTP Basic Auth before anyone on your LAN can open the dashboard or call API endpoints. (Localhost Docker healthchecks on `GET /api/status` remain allowed.)
* **Masked Secrets in API Responses**: `GET /api/config` masks `immich.api_key`, `icloud.password`, and Amazon auth tokens (`at-*`, `sess-at-*`, `sst-*`, `x-acb*`, `session-token`) with `********` while preserving stored secrets when saving other settings.
* **Restricted Origins & File Permissions**: CORS only permits Amazon Photos bookmarklet origins (`https://*.amazon.*`) and `localhost`, and `docker-entrypoint.sh` / `save_config()` automatically enforce `chmod 700` on `/app/data`, `.icloud_session`, and `.temp`, and `chmod 600` on `config.json` and `.env`. Keep Unraid's `appdata` share non-public and do not expose port `5050` directly to the internet.
