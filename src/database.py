import sqlite3
import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

class SyncDatabase:
    def __init__(self, db_path: str | Path = "sync_state.db"):
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sha1 TEXT UNIQUE,
                    md5 TEXT,
                    filename TEXT,
                    file_size INTEGER,
                    immich_id TEXT UNIQUE,
                    icloud_id TEXT UNIQUE,
                    amazon_id TEXT UNIQUE,
                    status TEXT DEFAULT 'ACTIVE',
                    created_at TIMESTAMP,
                    last_synced_at TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS albums (
                    immich_id TEXT PRIMARY KEY,
                    name TEXT,
                    amazon_id TEXT,
                    icloud_id TEXT,
                    synced_state TEXT,
                    last_synced_at TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    action TEXT,
                    details TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_scan_stats (
                    scan_date TEXT PRIMARY KEY,
                    scans_count INTEGER DEFAULT 0,
                    immich_direct_synced INTEGER DEFAULT 0,
                    amazon_uploaded INTEGER DEFAULT 0,
                    icloud_uploaded INTEGER DEFAULT 0,
                    icloud_ingested INTEGER DEFAULT 0,
                    immich_uploaded INTEGER DEFAULT 0,
                    deletions_synced INTEGER DEFAULT 0,
                    albums_synced INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    last_scan_at TIMESTAMP,
                    notified_at TIMESTAMP
                )
            """)

            cursor.execute("PRAGMA table_info(albums)")
            album_cols = {row[1] for row in cursor.fetchall()}
            if "synced_state" not in album_cols:
                cursor.execute("ALTER TABLE albums ADD COLUMN synced_state TEXT")

            # Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_sha1 ON assets(sha1)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_immich ON assets(immich_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_icloud ON assets(icloud_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_amazon ON assets(amazon_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_status ON assets(status)")
            conn.commit()

    def get_asset_by_sha1(self, sha1: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM assets WHERE sha1 = ?", (sha1,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_asset_by_immich_id(self, immich_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM assets WHERE immich_id = ?", (immich_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_asset_by_icloud_id(self, icloud_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM assets WHERE icloud_id = ?", (icloud_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_active_assets(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM assets WHERE status = 'ACTIVE'")
            return [dict(r) for r in cursor.fetchall()]

    def upsert_asset(
        self,
        sha1: str,
        filename: str,
        md5: Optional[str] = None,
        file_size: Optional[int] = None,
        immich_id: Optional[str] = None,
        icloud_id: Optional[str] = None,
        amazon_id: Optional[str] = None,
        status: str = "ACTIVE",
        created_at: Optional[str] = None
    ) -> int:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO assets (sha1, md5, filename, file_size, immich_id, icloud_id, amazon_id, status, created_at, last_synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sha1) DO UPDATE SET
                    md5 = COALESCE(excluded.md5, assets.md5),
                    filename = COALESCE(excluded.filename, assets.filename),
                    file_size = COALESCE(excluded.file_size, assets.file_size),
                    immich_id = COALESCE(excluded.immich_id, assets.immich_id),
                    icloud_id = COALESCE(excluded.icloud_id, assets.icloud_id),
                    amazon_id = COALESCE(excluded.amazon_id, assets.amazon_id),
                    status = excluded.status,
                    last_synced_at = excluded.last_synced_at
            """, (sha1, md5, filename, file_size, immich_id, icloud_id, amazon_id, status, created_at or now, now))
            conn.commit()
            return cursor.lastrowid

    def update_asset_ids(
        self,
        sha1: str,
        immich_id: Optional[str] = None,
        icloud_id: Optional[str] = None,
        amazon_id: Optional[str] = None
    ):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE assets SET
                    immich_id = COALESCE(?, immich_id),
                    icloud_id = COALESCE(?, icloud_id),
                    amazon_id = COALESCE(?, amazon_id),
                    last_synced_at = ?
                WHERE sha1 = ?
            """, (immich_id, icloud_id, amazon_id, now, sha1))
            conn.commit()

    def mark_asset_deleted(self, immich_id: str, reason: str = "DELETED_IN_IMMICH"):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE assets SET status = ?, last_synced_at = ? WHERE immich_id = ?
            """, (reason, now, immich_id))
            conn.commit()

    def get_album(self, immich_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM albums WHERE immich_id = ?", (immich_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_albums(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM albums")
            return [dict(r) for r in cursor.fetchall()]

    def upsert_album(
        self,
        immich_id: str,
        name: str,
        amazon_id: Optional[str] = None,
        icloud_id: Optional[str] = None,
        synced_state: Optional[str] = None
    ):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO albums (immich_id, name, amazon_id, icloud_id, synced_state, last_synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(immich_id) DO UPDATE SET
                    name = excluded.name,
                    amazon_id = COALESCE(excluded.amazon_id, albums.amazon_id),
                    icloud_id = COALESCE(excluded.icloud_id, albums.icloud_id),
                    synced_state = COALESCE(excluded.synced_state, albums.synced_state),
                    last_synced_at = excluded.last_synced_at
            """, (immich_id, name, amazon_id, icloud_id, synced_state, now))
            conn.commit()

    def log_sync(self, action: str, details: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO sync_history (action, details) VALUES (?, ?)", (action, details))
            conn.commit()

    @staticmethod
    def _today_key(scan_date: Optional[str] = None) -> str:
        return scan_date or datetime.date.today().isoformat()

    def record_scan_stats(self, stats: Dict[str, Any], scan_date: Optional[str] = None):
        date_key = self._today_key(scan_date)
        now = datetime.datetime.now().isoformat(timespec="seconds")
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily_scan_stats (
                    scan_date, scans_count, immich_direct_synced, amazon_uploaded,
                    icloud_uploaded, icloud_ingested, immich_uploaded,
                    deletions_synced, albums_synced, errors, last_scan_at
                )
                VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scan_date) DO UPDATE SET
                    scans_count = daily_scan_stats.scans_count + 1,
                    immich_direct_synced = daily_scan_stats.immich_direct_synced + excluded.immich_direct_synced,
                    amazon_uploaded = daily_scan_stats.amazon_uploaded + excluded.amazon_uploaded,
                    icloud_uploaded = daily_scan_stats.icloud_uploaded + excluded.icloud_uploaded,
                    icloud_ingested = daily_scan_stats.icloud_ingested + excluded.icloud_ingested,
                    immich_uploaded = daily_scan_stats.immich_uploaded + excluded.immich_uploaded,
                    deletions_synced = daily_scan_stats.deletions_synced + excluded.deletions_synced,
                    albums_synced = daily_scan_stats.albums_synced + excluded.albums_synced,
                    errors = daily_scan_stats.errors + excluded.errors,
                    last_scan_at = excluded.last_scan_at
            """, (
                date_key,
                int(stats.get("immich_direct_synced", 0) or 0),
                int(stats.get("amazon_uploaded", 0) or 0),
                int(stats.get("icloud_uploaded", 0) or 0),
                int(stats.get("icloud_ingested", 0) or 0),
                int(stats.get("immich_uploaded", 0) or 0),
                int(stats.get("deletions_synced", 0) or 0),
                int(stats.get("albums_synced", 0) or 0),
                int(stats.get("errors", 0) or 0),
                now,
            ))
            conn.commit()

    def get_daily_scan_stats(self, scan_date: Optional[str] = None) -> Dict[str, Any]:
        date_key = self._today_key(scan_date)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM daily_scan_stats WHERE scan_date = ?", (date_key,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return {
            "scan_date": date_key,
            "scans_count": 0,
            "immich_direct_synced": 0,
            "amazon_uploaded": 0,
            "icloud_uploaded": 0,
            "icloud_ingested": 0,
            "immich_uploaded": 0,
            "deletions_synced": 0,
            "albums_synced": 0,
            "errors": 0,
            "last_scan_at": None,
            "notified_at": None,
        }

    def was_daily_notified(self, scan_date: Optional[str] = None) -> bool:
        row = self.get_daily_scan_stats(scan_date)
        return bool(row.get("notified_at"))

    def mark_daily_notified(self, scan_date: Optional[str] = None):
        date_key = self._today_key(scan_date)
        now = datetime.datetime.now().isoformat(timespec="seconds")
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily_scan_stats (scan_date, notified_at)
                VALUES (?, ?)
                ON CONFLICT(scan_date) DO UPDATE SET notified_at = excluded.notified_at
            """, (date_key, now))
            conn.commit()
