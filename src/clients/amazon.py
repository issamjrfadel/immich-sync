import time
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import httpx
from amazon_photos import AmazonPhotos

from src.config import AmazonConfig

logger = logging.getLogger("immich_sync.amazon")


def amazon_cookie_fingerprint(cookies: Optional[Dict[str, str]], tld: str = "ca") -> str:
    """Return a deterministic SHA-256 fingerprint of normalized Amazon session cookies and TLD."""
    normalized = AmazonPhotosClient._normalize_cookies(cookies or {}, tld=tld)
    clean = {str(k): str(v) for k, v in sorted(normalized.items()) if v}
    payload = json.dumps({"tld": (tld or "ca").lower().strip(), "cookies": clean}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def is_amazon_auth_error(exc: Exception) -> bool:
    """Return True if an exception indicates expired/unauthorized Amazon Photos cookies."""
    if isinstance(exc, PermissionError):
        return True
    resp = getattr(exc, "response", None)
    if getattr(resp, "status_code", None) in (401, 403):
        return True
    msg = str(exc).lower()
    return (
        "401" in msg
        or "unauthorized" in msg
        or "session cookies have expired" in msg
        or "session expired" in msg
    )

class RegionalAmazonPhotos(AmazonPhotos):
    """
    Subclass of AmazonPhotos that ensures exact TLD and regional endpoint resolution
    (e.g., Canada https://www.amazon.ca/drive/v1 and North American cdproxy),
    fixes the CancelledError bug in upstream get_folders, and skips full-account db crawling on init.
    """
    def __init__(self, cookies: dict, tld: str = "ca", **kwargs):
        self._target_tld = (tld or "ca").lower().strip()
        super().__init__(cookies=cookies, **kwargs)

    def determine_tld(self, cookies: dict) -> str:
        if getattr(self, "_target_tld", None):
            return self._target_tld
        detected = super().determine_tld(cookies)
        return detected or "ca"

    def get_folders(self) -> list:
        """
        Fixed get_folders implementation that handles asyncio CancelledError
        and avoids crashing during worker task cancellation.
        """
        import asyncio
        from httpx import AsyncClient

        aclient = AsyncClient(
            http2=False,
            limits=self.limits,
            headers=self.client.headers,
            cookies=self.client.cookies,
            verify=False,
        )

        async def helper(node):
            url = f"{self.drive_url}/nodes/{node['id']}/children"
            params = {'filters': 'kind:FOLDER'} | self.base_params
            r = await aclient.get(url, params=params)
            if r.status_code == 200:
                return r.json().get('data', [])
            return []

        async def process_node(queue):
            result = []
            while True:
                node = await queue.get()
                if node.get('id'):
                    subfolders = await helper(node)
                    for folder in subfolders:
                        await queue.put(folder)
                    result.extend(subfolders)
                queue.task_done()
                if queue.empty():
                    return result

        async def main(data, batch_size=min(self.n_threads, 4)):
            Q = asyncio.Queue()
            for node in data:
                await Q.put(node)
            tasks = [asyncio.create_task(process_node(Q)) for _ in range(batch_size)]
            await Q.join()
            for task in tasks:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await aclient.aclose()
            return [y for x in results if isinstance(x, list) for y in x]

        try:
            return asyncio.run(main([{'id': self.root['id']}]))
        except Exception:
            return []

    def load_db(self, **kwargs):
        """Skip full account metadata download on init; queries are performed on-demand."""
        import pandas as pd
        return pd.DataFrame()

    def build_tree(self):
        """Skip local tree construction on init."""
        return {}

    def backoff(self, fn, *args, m: int = 5, b: int = 1.5, max_retries: int = 2, **kwargs):
        """
        Fail-fast backoff implementation:
        - Never retries 400 (Bad Request), 401 (Unauthorized), 403 (Forbidden), 404 (Not Found).
        - Prevents spamming invalid fields against Amazon and protects session cookies from revocation.
        - Retries at most 2 times only for 429 rate limits or 5xx server errors.
        """
        import time, random
        for i in range(max_retries + 1):
            try:
                r = fn(*args, **kwargs)
                if r is None:
                    return None

                # Non-retriable responses
                if r.status_code in (200, 201, 204, 409):
                    return r
                if r.status_code in (400, 401, 403, 404):
                    logger.debug(f"Amazon Photos returned status {r.status_code}: not retrying.")
                    return r

                r.raise_for_status()
                return r
            except Exception as e:
                if i == max_retries:
                    logger.debug(f"Amazon Photos backoff max retries exceeded: {e}")
                    return None
                time.sleep(min(1 * (b ** i), m))
        return None

    def get_root(self) -> dict:
        r = self.backoff(self.client.get, f'{self.drive_url}/nodes', params={'filters': 'isRoot:true'} | self.base_params)
        if r is None or r.status_code != 200:
            status_code = getattr(r, 'status_code', 'unknown')
            if status_code == 401:
                raise PermissionError("Amazon Photos session cookies have expired. Please copy fresh cookies from https://www.amazon.ca/photos into Settings.")
            raise ConnectionError(f"Failed to connect to Amazon Photos (HTTP {status_code})")
        data = r.json().get('data', [])
        if not data:
            raise ConnectionError("Amazon Photos root folder not found.")
        return data[0]

class AmazonPhotosClient:
    def __init__(self, config: AmazonConfig):
        self.config = config
        self.tld = (config.tld or "ca").lower().strip()
        self.cookies = self._normalize_cookies(config.cookies, self.tld)
        self._ap: Optional[RegionalAmazonPhotos] = None
        self.session_expired: bool = False

    @staticmethod
    def _normalize_cookies(cookies: Dict[str, str], tld: str = "ca") -> Dict[str, str]:
        """
        Normalizes cookie dictionary across regional variations.
        For amazon.ca: ensures session-id, ubid-acbca, and at-acbca are present,
        while maintaining ubid-main / at-main aliases.
        """
        if not cookies:
            return {}
        c = dict(cookies)
        tld_clean = (tld or "ca").lower().strip()

        session_id = c.get("session-id")
        ubid = c.get(f"ubid-acb{tld_clean}") or c.get("ubid-main") or c.get("ubid")
        at_token = c.get(f"at-acb{tld_clean}") or c.get("at-main") or c.get("at_main") or c.get("at")

        if session_id:
            c["session-id"] = session_id

        if ubid:
            c["ubid-main"] = ubid
            c[f"ubid-acb{tld_clean}"] = ubid

        if at_token:
            c["at-main"] = at_token
            c["at_main"] = at_token
            c[f"at-acb{tld_clean}"] = at_token

        return c

    def _ensure_client(self, quiet: bool = False) -> RegionalAmazonPhotos:
        if self._ap is not None:
            return self._ap

        if not self.cookies or "session-id" not in self.cookies:
            raise ValueError(
                "Amazon Photos cookies not configured or missing 'session-id'. "
                f"Please configure 'session-id', 'ubid-acb{self.tld}', and 'at-acb{self.tld}' "
                f"from https://www.amazon.{self.tld}/photos in config.json."
            )

        try:
            self._ap = RegionalAmazonPhotos(cookies=self.cookies, tld=self.tld)
            logger.info(
                f"Initialized Amazon Photos client targeting {self._ap.drive_url} "
                f"(Marketplace: amazon.{self.tld}, Upload Proxy: {self._ap.cdproxy_url})"
            )
            return self._ap
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
                if quiet:
                    logger.debug(f"Amazon Photos auth check not yet valid for amazon.{self.tld}: {e}")
                else:
                    logger.warning(f"Amazon Photos session expired for amazon.{self.tld}: {e}")
            else:
                logger.error(f"Failed to initialize Amazon Photos client for amazon.{self.tld}: {e}")
            raise

    def test_connection(self, quiet: bool = False) -> Dict[str, Any]:
        """Test connection to Amazon Photos and return root folder metadata."""
        ap = self._ensure_client(quiet=quiet)
        root = ap.get_root()
        return {
            "connected": True,
            "tld": self.tld,
            "marketplace": f"https://www.amazon.{self.tld}/photos",
            "drive_url": ap.drive_url,
            "root_id": root.get("id"),
            "owner_id": root.get("ownerId")
        }

    def upload_photo(self, file_path: Path, md5_hash: Optional[str] = None) -> Optional[str]:
        """
        Uploads a single photo directly using the Amazon Cloud Drive / Photos upload endpoint.
        Returns the Amazon node ID on success or if already existing.
        """
        if self.session_expired:
            raise PermissionError("Amazon Photos session cookies have expired. Please copy fresh cookies from https://www.amazon.ca/photos into Settings.")

        ap = self._ensure_client()
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        root_id = ap.root.get("id")
        upload_url = ap.cdproxy_url

        try:
            with open(file_path, "rb") as f:
                # Direct stream upload to avoid creating unnecessary subdirectories
                resp = ap.client.post(
                    upload_url,
                    data=f,
                    params={
                        "name": file_path.name,
                        "kind": "FILE",
                        "parentNodeId": root_id
                    },
                    timeout=180
                )

                if resp.status_code in (200, 201):
                    node = resp.json()
                    return node.get("id")

                if resp.status_code == 409:
                    # File already exists on Amazon Photos (Conflict)
                    # Attempt to extract existing nodeId from conflict payload or search by MD5
                    data = resp.json()
                    existing_id = (
                        data.get("info", {}).get("nodeId")
                        or data.get("nodeId")
                    )
                    if existing_id:
                        return existing_id

                    # If not in conflict body, search by MD5
                    if md5_hash:
                        existing_node = self.find_by_md5(md5_hash)
                        if existing_node:
                            return existing_node

                    logger.info(f"File {file_path.name} already exists in Amazon Photos.")
                    return "CONFLICT_EXISTING"

                if resp.status_code == 401:
                    self.session_expired = True
                    raise PermissionError(
                        "Amazon Photos session cookies have expired. Please copy fresh cookies from https://www.amazon.ca/photos into Settings."
                    )

                resp.raise_for_status()

        except Exception as e:
            logger.error(f"Amazon Photos upload failed for {file_path.name}: {e}")
            raise

        return None

    def find_nodes_by_md5(self, md5_hash: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find photo nodes by MD5 checksum using the /nodes endpoint."""
        if not md5_hash or self.session_expired:
            return []
        try:
            ap = self._ensure_client()
            url = f"{ap.drive_url}/nodes"
            params = {
                "filters": f"contentProperties.md5:{md5_hash}",
                "limit": limit,
                "asset": "ALL",
                "tempLink": "false"
            }
            resp = ap.client.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data and isinstance(data, list):
                    return data
            elif resp.status_code == 400:
                logger.debug(f"Amazon /nodes query returned 400: {resp.text[:120]}")
            elif resp.status_code == 401:
                self.session_expired = True
                logger.warning("[Amazon Photos] Session cookies expired. Fast-failing Amazon checks for remaining photos.")
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
                logger.warning("[Amazon Photos] Session cookies expired during MD5 lookup.")
            else:
                logger.debug(f"MD5 search failed: {e}")
        return []

    def find_by_md5(self, md5_hash: str) -> Optional[str]:
        """
        Find photo node ID by its MD5 checksum using the /nodes endpoint.
        Does NOT use /search, preventing 400 Bad Request 'Invalid fieldName: contentProperties.md5'.
        """
        nodes = self.find_nodes_by_md5(md5_hash, limit=1)
        return nodes[0].get("id") if nodes else None

    def trash_nodes(self, node_ids: List[str]) -> bool:
        """Move photo/video nodes to Amazon Photos Trash."""
        if not node_ids:
            return True
        if self.session_expired:
            return False
        try:
            ap = self._ensure_client()
            ap.trash(node_ids)
            return True
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
                logger.warning("Amazon Photos session cookies expired while moving items to Trash.")
            else:
                logger.error(f"Failed to trash Amazon Photos nodes {node_ids}: {e}")
            return False

    def purge_nodes(self, node_ids: List[str]) -> bool:
        """Permanently delete photo/video nodes from Amazon Photos (bypassing trash/freeing quota)."""
        if not node_ids:
            return True
        if self.session_expired:
            return False
        try:
            ap = self._ensure_client()
            ap.delete(node_ids)
            return True
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
                logger.warning("Amazon Photos session cookies expired while purging nodes.")
            else:
                logger.error(f"Failed to permanently purge Amazon Photos nodes {node_ids}: {e}")
            return False

    def create_album(self, album_name: str, node_ids: Optional[List[str]] = None, cover_node_id: Optional[str] = None) -> Optional[str]:
        """Create a visual collection / album in Amazon Photos."""
        if self.session_expired:
            return None
        safe_name = album_name.replace("/", " - ").strip()
        try:
            ap = self._ensure_client()
            r = ap.client.post(
                f"{ap.drive_url}/nodes",
                json={
                    "kind": "VISUAL_COLLECTION",
                    "name": safe_name,
                    "resourceVersion": "V2",
                    "ContentType": "JSON",
                }
            )
            if r.status_code == 401:
                self.session_expired = True
                logger.warning(f"Amazon Photos session cookies expired while creating album '{safe_name}'.")
                return None
            if r.status_code not in (200, 201):
                logger.error(f"Failed to create Amazon Photos album '{safe_name}': HTTP {r.status_code} {r.text[:200]}")
                return None
            album_id = r.json().get("id")
            if album_id and node_ids:
                self.add_to_album(album_id, node_ids, cover_node_id=cover_node_id)
            return album_id
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
                logger.warning(f"Amazon Photos session cookies expired while creating album '{album_name}'.")
            else:
                logger.error(f"Failed to create Amazon Photos album '{album_name}': {e}")
            return None

    def get_album_children_ids(self, album_id: str) -> Optional[set[str]]:
        """Fetch node IDs currently inside an Amazon Photos album (returns None on query failure)."""
        if not album_id or self.session_expired:
            return None
        ap = self._ensure_client()
        child_ids: set[str] = set()
        offset = 0
        limit = 200
        try:
            while True:
                params = {"limit": limit, "offset": offset} | ap.base_params
                resp = ap.client.get(f"{ap.drive_url}/nodes/{album_id}/children", params=params, timeout=20)
                if resp.status_code == 401:
                    self.session_expired = True
                    return None
                if resp.status_code != 200:
                    return None
                body = resp.json()
                items = body.get("data", []) or []
                for item in items:
                    if isinstance(item, dict) and item.get("id"):
                        child_ids.add(item["id"])
                total_count = body.get("count")
                if not items or len(items) < limit or (total_count is not None and offset + len(items) >= total_count):
                    break
                offset += len(items)
            return child_ids
        except Exception as e:
            logger.debug(f"Could not list children of Amazon album {album_id}: {e}")
            return None

    def _patch_album_chunk_with_retry(self, ap: RegionalAmazonPhotos, album_id: str, chunk: List[str], retries: int = 3) -> bool:
        """Patch a chunk of nodes into an Amazon album, retrying transient 429/5xx (including 503)."""
        last_status = None
        last_body = ""
        for attempt in range(retries + 1):
            resp = ap.client.patch(
                f"{ap.drive_url}/nodes/{album_id}/children",
                json={
                    "op": "add",
                    "value": chunk,
                    "resourceVersion": "V2",
                    "ContentType": "JSON",
                },
                timeout=20,
            )
            if resp.status_code in (200, 202, 204):
                return True
            last_status = resp.status_code
            last_body = (resp.text or "")[:120]
            if resp.status_code == 401:
                self.session_expired = True
                logger.warning("Amazon Photos session cookies expired during album update.")
                return False
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(1.5 * (2 ** attempt))
                continue
            break

        # If a multi-item chunk keeps hitting 503, split it into smaller batches to isolate problematic nodes
        if last_status == 503 and len(chunk) > 1:
            mid = len(chunk) // 2
            left_ok = self._patch_album_chunk_with_retry(ap, album_id, chunk[:mid], retries=1)
            time.sleep(0.25)
            right_ok = self._patch_album_chunk_with_retry(ap, album_id, chunk[mid:], retries=1)
            return left_ok and right_ok

        logger.warning(f"  -> Batch add to Amazon album {album_id} failed (HTTP {last_status}): {last_body}")
        return False

    def add_to_album(self, album_id: str, node_ids: List[str], cover_node_id: Optional[str] = None) -> bool:
        """Add missing media nodes to an existing Amazon Photos album in resilient batches."""
        clean_ids = [
            nid for nid in (node_ids or [])
            if nid and nid != "CONFLICT_EXISTING" and not str(nid).startswith("dry_run")
        ]
        if self.session_expired:
            return False
        if not clean_ids or not album_id:
            return True
        try:
            ap = self._ensure_client()
            existing_ids = self.get_album_children_ids(album_id)
            if self.session_expired:
                return False
            if existing_ids is not None:
                pending_ids = [nid for nid in clean_ids if nid not in existing_ids]
            else:
                pending_ids = clean_ids

            if not pending_ids:
                return True

            # Deduplicate while preserving order
            pending_ids = list(dict.fromkeys(pending_ids))
            chunk_size = 15
            all_ok = True
            for i in range(0, len(pending_ids), chunk_size):
                if self.session_expired:
                    all_ok = False
                    break
                chunk = pending_ids[i:i + chunk_size]
                if not self._patch_album_chunk_with_retry(ap, album_id, chunk):
                    all_ok = False
                if i + chunk_size < len(pending_ids):
                    time.sleep(0.3)

            cover = cover_node_id or clean_ids[0]
            if cover and not self.session_expired:
                self.set_album_cover(album_id, cover)
            return all_ok and not self.session_expired
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
            logger.error(f"Failed to add nodes to Amazon Photos album {album_id}: {e}")
            return False

    def set_album_cover(self, album_id: str, cover_node_id: str) -> bool:
        """Set the cover photo for an Amazon Photos album."""
        if not album_id or not cover_node_id or self.session_expired:
            return False
        try:
            ap = self._ensure_client()
            resp = ap.client.patch(
                f"{ap.drive_url}/nodes/{album_id}",
                json={
                    "resourceVersion": "V2",
                    "ContentType": "JSON",
                    "coverObject": {"id": cover_node_id}
                }
            )
            if resp.status_code == 401:
                self.session_expired = True
                logger.warning(f"Amazon Photos session cookies expired while setting cover for album {album_id}.")
                return False
            return resp.status_code == 200
        except Exception as e:
            if is_amazon_auth_error(e):
                self.session_expired = True
            logger.debug(f"Could not set cover for album {album_id}: {e}")
            return False

    def get_albums(self) -> List[Dict[str, Any]]:
        """Fetch all visual collections/albums in Amazon Photos."""
        ap = self._ensure_client()
        try:
            folders = ap.get_folders()
            albums = [f for f in folders if f.get("kind") == "VISUAL_COLLECTION"]
            return albums
        except Exception as e:
            logger.error(f"Failed to fetch Amazon Photos albums: {e}")
            return []
