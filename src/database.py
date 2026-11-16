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
        icloud_id: Optional[str] = None
    ):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO albums (immich_id, name, amazon_id, icloud_id, last_synced_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(immich_id) DO UPDATE SET
                    name = excluded.name,
                    amazon_id = COALESCE(excluded.amazon_id, albums.amazon_id),
                    icloud_id = COALESCE(excluded.icloud_id, albums.icloud_id),
                    last_synced_at = excluded.last_synced_at
            """, (immich_id, name, amazon_id, icloud_id, now))
            conn.commit()

    def log_sync(self, action: str, details: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO sync_history (action, details) VALUES (?, ?)", (action, details))
            conn.commit()
