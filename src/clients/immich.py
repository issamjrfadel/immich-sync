import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from src.config import ImmichConfig
from src.utils import get_mime_type

class ImmichClient:
    def __init__(self, config: ImmichConfig):
        self.base_url = config.url.rstrip("/")
        self.api_key = config.api_key
        self.verify_ssl = config.verify_ssl
        self.headers = {
            "x-api-key": self.api_key,
            "Accept": "application/json"
        }

        # Persistent HTTP session with robust connection pooling and keep-alive
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=30,
            max_retries=Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
                raise_on_status=False
            )
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path if path.startswith('/') else '/' + path}"

    def test_connection(self) -> Dict[str, Any]:
        """Verify API key and fetch current user profile."""
        url = self._url("/api/users/me")
        resp = self.session.get(url, headers=self.headers, verify=self.verify_ssl, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_server_version(self) -> Dict[str, Any]:
        """Fetch Immich server version information."""
        url = self._url("/api/server/version")
        resp = self.session.get(url, headers=self.headers, verify=self.verify_ssl, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return {}

    def bulk_upload_check(self, items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Check if files already exist on Immich using their SHA-1 checksums.
        Items: [{'id': 'local_or_unique_id', 'checksum': 'sha1_hex_or_base64'}]
        Returns results list with 'action' ('accept' or 'reject') and 'assetId'.
        """
        url = self._url("/api/assets/bulk-upload-check")
        payload = {"assets": items}
        resp = self.session.post(url, json=payload, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def upload_asset(
        self,
        file_path: Path,
        device_asset_id: str,
        created_at: Optional[datetime] = None,
        is_favorite: bool = False
    ) -> str:
        """Upload a photo or video asset to Immich."""
        url = self._url("/api/assets")
        now_iso = datetime.now(timezone.utc).isoformat()
        created_iso = created_at.isoformat() if created_at else now_iso

        mime = get_mime_type(file_path)
        with open(file_path, "rb") as f:
            files = {
                "assetData": (file_path.name, f, mime)
            }
            data = {
                "deviceAssetId": device_asset_id,
                "deviceId": "immich-multi-sync-bridge",
                "fileCreatedAt": created_iso,
                "fileModifiedAt": now_iso,
                "isFavorite": "true" if is_favorite else "false"
            }
            # Note: Do not set Content-Type in headers; requests sets the multipart boundary automatically
            headers = {
                "x-api-key": self.api_key,
                "Accept": "application/json"
            }
            resp = self.session.post(url, data=data, files=files, headers=headers, verify=self.verify_ssl, timeout=120)
            resp.raise_for_status()
            res_json = resp.json()
            return res_json.get("id")

    def _search_metadata_paginated(self, base_payload: Dict[str, Any], page_size: int = 1000) -> List[Dict[str, Any]]:
        """Execute paginated POST /api/search/metadata queries."""
        url = self._url("/api/search/metadata")
        page = 1
        all_assets: List[Dict[str, Any]] = []

        while True:
            payload = {**base_payload, "page": page, "size": page_size}
            resp = self.session.post(url, json=payload, headers=self.headers, verify=self.verify_ssl, timeout=60)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            data = resp.json()

            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                assets_field = data.get("assets", {})
                if isinstance(assets_field, dict):
                    items = assets_field.get("items", [])
                elif isinstance(assets_field, list):
                    items = assets_field
                else:
                    items = data.get("items", [])

            if not items:
                break

            all_assets.extend(items)
            if len(items) < page_size:
                break
            page += 1

        return all_assets

    def get_all_assets(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        """Fetch all active assets from Immich across paginated search."""
        return self._search_metadata_paginated({"withExif": True}, page_size=page_size)

    def get_trashed_assets(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        """Fetch assets currently sitting in Immich Trash."""
        results = self._search_metadata_paginated(
            {"withDeleted": True, "trashedAfter": "1970-01-01T00:00:00.000Z"},
            page_size=page_size,
        )
        return [item for item in results if item.get("isTrashed") or item.get("deletedAt")]

    def download_asset(self, asset_id: str, dest_path: Path, max_retries: int = 2) -> Path:
        """
        Download an asset's original file from Immich with chunk-level read timeouts,
        atomic staging via .part files, and automatic transient-failure retry.
        """
        url = self._url(f"/api/assets/{asset_id}/original")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = dest_path.with_suffix(dest_path.suffix + ".part")

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                # timeout is (connect_timeout, read_timeout)
                resp = self.session.get(
                    url,
                    headers=self.headers,
                    verify=self.verify_ssl,
                    stream=True,
                    timeout=(10, 45)
                )
                resp.raise_for_status()

                with open(part_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)

                # Atomically rename to final destination
                if part_path.exists():
                    part_path.replace(dest_path)
                return dest_path

            except Exception as e:
                last_error = e
                if part_path.exists():
                    try:
                        part_path.unlink()
                    except OSError:
                        pass
                if attempt < max_retries:
                    time.sleep(1.0 * (attempt + 1))
                else:
                    raise last_error

        return dest_path

    def delete_assets(self, asset_ids: List[str], force: bool = False) -> Dict[str, Any]:
        """Delete or move assets to trash in Immich."""
        url = self._url("/api/assets")
        payload = {
            "ids": asset_ids,
            "force": force
        }
        resp = self.session.delete(url, json=payload, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_albums(self) -> List[Dict[str, Any]]:
        """Fetch all albums in Immich."""
        url = self._url("/api/albums")
        resp = self.session.get(url, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_album_details(self, album_id: str) -> Dict[str, Any]:
        """Fetch album details including full asset lists."""
        url = self._url(f"/api/albums/{album_id}")
        resp = self.session.get(url, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        album = resp.json()

        # In modern Immich versions, assets are not embedded in /api/albums/{id}.
        # Retrieve assets belonging to this album via /api/search/metadata using albumIds filter
        search_url = self._url("/api/search/metadata")
        page = 1
        page_size = 1000
        all_album_assets = []
        while True:
            payload = {"albumIds": [album_id], "page": page, "size": page_size}
            try:
                search_resp = self.session.post(
                    search_url,
                    json=payload,
                    headers=self.headers,
                    verify=self.verify_ssl,
                    timeout=30
                )
                if search_resp.status_code != 200:
                    break
                search_data = search_resp.json()
                if isinstance(search_data, dict):
                    items = search_data.get("assets", {}).get("items", [])
                elif isinstance(search_data, list):
                    items = search_data
                else:
                    items = []

                if not items:
                    break
                all_album_assets.extend(items)
                if len(items) < page_size:
                    break
                page += 1
            except Exception:
                break

        album["assets"] = all_album_assets
        return album

    def create_album(self, album_name: str, asset_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create a new album in Immich."""
        url = self._url("/api/albums")
        payload = {
            "albumName": album_name,
            "assetIds": asset_ids or []
        }
        resp = self.session.post(url, json=payload, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def add_assets_to_album(self, album_id: str, asset_ids: List[str]) -> Dict[str, Any]:
        """Add assets to an existing album."""
        url = self._url(f"/api/albums/{album_id}/assets")
        payload = {"ids": asset_ids}
        resp = self.session.put(url, json=payload, headers=self.headers, verify=self.verify_ssl, timeout=30)
        resp.raise_for_status()
        return resp.json()
