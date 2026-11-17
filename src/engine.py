import time
import logging
import shutil
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Set, Callable
from datetime import datetime

from src.config import AppConfig
from src.database import SyncDatabase
from src.clients.immich import ImmichClient
from src.clients.icloud import ICloudClient
from src.clients.amazon import AmazonPhotosClient
from src.utils import (
    compute_hashes,
    format_bytes,
    is_video_file,
    is_video_asset,
    is_hidden_or_locked_asset
)

logger = logging.getLogger("immich_sync.engine")

class SyncEngine:
    def __init__(
        self,
        config: AppConfig,
        db_path: str = "sync_state.db",
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        stop_event: Optional[threading.Event] = None,
        pause_event: Optional[threading.Event] = None
    ):
        self.config = config
        self.db = SyncDatabase(db_path)
        self.immich = ImmichClient(config.immich)
        self.icloud = ICloudClient(config.icloud)
        self.amazon = AmazonPhotosClient(config.amazon)
        self.progress_callback = progress_callback
        self.stop_event = stop_event
        self.pause_event = pause_event

        self.temp_dir = Path(config.sync.download_temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        # Clean any stale temporary staging files from previous interrupted runs
        for stale_file in self.temp_dir.glob("immich_*"):
            try:
                stale_file.unlink()
            except OSError:
                pass

        self.dry_run = config.sync.dry_run

    def check_pause_and_stop(
        self,
        item_name: str = "",
        current_pct: int = 0,
        current_idx: int = 0,
        total_items: int = 0
    ) -> bool:
        """
        Checks whether pause or stop has been signaled.
        If paused, blocks until resumed or stopped.
        Returns True if stop was requested (caller should abort), False otherwise.
        """
        if self.stop_event and self.stop_event.is_set():
            return True

        if self.pause_event and self.pause_event.is_set():
            logger.info(f"Sync paused at item {current_idx}/{total_items}: {item_name}")
            self.report_progress(
                percent=current_pct,
                phase="paused",
                phase_name="Sync Paused",
                current=current_idx,
                total=total_items,
                item_name=item_name,
                action="Sync is paused. Click Resume to continue."
            )
            while self.pause_event.is_set() and not (self.stop_event and self.stop_event.is_set()):
                time.sleep(0.5)

            if self.stop_event and self.stop_event.is_set():
                return True

            logger.info("Sync resumed.")
            self.report_progress(
                percent=current_pct,
                phase="running",
                phase_name="Sync In Progress",
                current=current_idx,
                total=total_items,
                item_name=item_name,
                action=f"Resuming sync on {item_name or 'next asset'}..."
            )

        return False

    @property
    def is_immich_enabled(self) -> bool:
        return bool(self.config.sync.enable_immich and getattr(self.config.immich, "enabled", True))

    @property
    def is_icloud_enabled(self) -> bool:
        return bool(self.config.sync.enable_icloud and getattr(self.config.icloud, "enabled", True))

    @property
    def is_amazon_enabled(self) -> bool:
        return bool(self.config.sync.enable_amazon and getattr(self.config.amazon, "enabled", True))

    def report_progress(
        self,
        percent: int,
        phase: str,
        phase_name: str,
        current: int = 0,
        total: int = 0,
        item_name: str = "",
        action: str = ""
    ):
        """Dispatch progress updates to attached progress callback."""
        if self.progress_callback:
            try:
                self.progress_callback({
                    "percent": max(0, min(100, int(percent))),
                    "phase": phase,
                    "phase_name": phase_name,
                    "current": current,
                    "total": total,
                    "item_name": item_name,
                    "action": action
                })
            except Exception as e:
                logger.debug(f"Progress callback error: {e}")

    def run_sync_cycle(self, limit_intake: Optional[int] = None, force_deletes: bool = False) -> Dict[str, Any]:
        """
        Executes a synchronization cycle:
        In one-way master mode (default):
        1. Immich intake: Mirror photos in Immich to Amazon Photos & iCloud Photos
        2. Deletion Sync: Deletions in Immich -> Trash in Amazon Photos and Recently Deleted in iCloud
        3. Album Sync: Sync Immich visual albums and contents to Amazon Photos and iCloud Photos
        4. Skip iCloud/Amazon intake into Immich to maintain one-way source of truth.
        """
        stats = {
            "icloud_ingested": 0,
            "immich_uploaded": 0,
            "amazon_uploaded": 0,
            "icloud_uploaded": 0,
            "immich_direct_synced": 0,
            "deletions_synced": 0,
            "albums_synced": 0,
            "errors": 0
        }

        logger.info("=== Starting Sync Cycle ===")
        if self.dry_run:
            logger.info("[DRY RUN MODE ENABLED - No changes will be written]")
        logger.info(
            f"Enabled hosts: Immich={self.is_immich_enabled}, "
            f"iCloud={self.is_icloud_enabled}, Amazon={self.is_amazon_enabled}"
        )

        self.report_progress(2, "starting", "Initializing Sync", 0, 0, "", "Checking sync configurations and targets...")

        # -------------------------------------------------------------
        # STEP 1: Ingest from iCloud -> Immich & Amazon Photos (Two-way only)
        # -------------------------------------------------------------
        is_one_way = getattr(self.config.sync, "one_way_immich_master", True) or not getattr(self.config.sync, "ingest_from_icloud", False)
        if not is_one_way and self.is_icloud_enabled:
            try:
                logger.info("Step 1: Checking iCloud Photos for new items...")
                self.report_progress(5, "icloud_intake", "Ingesting from iCloud", 0, 0, "", "Connecting to iCloud Photos...")
                self.icloud.connect()
                stats["icloud_ingested"] = self._sync_from_icloud(limit=limit_intake, stats=stats)
            except Exception as e:
                logger.error(f"Error during iCloud intake: {e}", exc_info=True)
                stats["errors"] += 1
        elif is_one_way:
            logger.info("Step 1: One-way sync active (Immich is master). Skipping iCloud-to-Immich ingestion.")
        else:
            logger.info("Step 1: iCloud sync is disabled. Skipping iCloud intake.")

        # -------------------------------------------------------------
        # STEP 2: Sync Direct Immich Additions -> Amazon Photos & iCloud
        # -------------------------------------------------------------
        if self.stop_event and self.stop_event.is_set():
            logger.info("Sync cycle cancelled by stop event.")
            self.report_progress(0, "stopped", "Sync Stopped", 0, 0, "", "Sync cycle stopped by user.")
            return stats

        if self.is_immich_enabled:
            try:
                logger.info("Step 2: Checking for photos in Immich to mirror to Amazon & iCloud...")
                self.report_progress(10, "immich_sync", "Scanning Immich Library", 0, 0, "", "Fetching photo metadata from Immich...")
                stats["immich_direct_synced"] = self._sync_from_immich(stats=stats)
            except Exception as e:
                logger.error(f"Error syncing Immich assets: {e}", exc_info=True)
                stats["errors"] += 1
        else:
            logger.info("Step 2: Immich sync is disabled. Skipping Immich intake.")

        # -------------------------------------------------------------
        # STEP 3: Sync Deletions (Immich -> Amazon & iCloud)
        # -------------------------------------------------------------
        if self.stop_event and self.stop_event.is_set():
            logger.info("Sync cycle cancelled by stop event.")
            self.report_progress(0, "stopped", "Sync Stopped", 0, 0, "", "Sync cycle stopped by user.")
            return stats

        if self.config.sync.sync_deletions and self.is_immich_enabled:
            try:
                logger.info("Step 3: Checking for deletions in Immich to propagate...")
                self.report_progress(70, "deletions", "Checking Deletions", 0, 0, "", "Comparing Immich catalog with tracked database...")
                stats["deletions_synced"] = self._sync_deletions(force=force_deletes)
            except Exception as e:
                logger.error(f"Error during deletion sync: {e}", exc_info=True)
                stats["errors"] += 1
        elif not self.is_immich_enabled:
            logger.info("Step 3: Immich is disabled. Skipping deletion sync.")
        else:
            logger.info("Step 3: Deletion sync disabled in configuration.")

        # -------------------------------------------------------------
        # STEP 4: Sync Albums (Immich -> Amazon & iCloud)
        # -------------------------------------------------------------
        if self.stop_event and self.stop_event.is_set():
            logger.info("Sync cycle cancelled by stop event.")
            self.report_progress(0, "stopped", "Sync Stopped", 0, 0, "", "Sync cycle stopped by user.")
            return stats

        if self.config.sync.sync_albums and self.is_immich_enabled:
            try:
                logger.info("Step 4: Synchronizing Albums from Immich...")
                self.report_progress(90, "albums", "Syncing Albums", 0, 0, "", "Fetching albums from Immich...")
                stats["albums_synced"] = self._sync_albums()
            except Exception as e:
                logger.error(f"Error during album sync: {e}", exc_info=True)
                stats["errors"] += 1
        elif not self.is_immich_enabled:
            logger.info("Step 4: Immich is disabled. Skipping album sync.")
        else:
            logger.info("Step 4: Album sync disabled in configuration.")

        if self.stop_event and self.stop_event.is_set():
            logger.info("Sync cycle stopped by user.")
            self.report_progress(0, "stopped", "Sync Stopped", 0, 0, "", "Sync cycle stopped by user.")
            return stats

        summary_msg = f"Completed: {stats['immich_direct_synced']} mirrored, {stats['deletions_synced']} deletions, {stats['albums_synced']} albums."
        self.report_progress(100, "completed", "Sync Complete", 0, 0, "", summary_msg)
        logger.info(f"=== Sync Cycle Finished. Summary: {stats} ===")
        return stats

    def _sync_from_icloud(self, limit: Optional[int], stats: Dict[str, Any]) -> int:
        count = 0
        for photo in self.icloud.iter_all_photos(limit=limit):
            photo_id = getattr(photo, "id", None) or getattr(photo, "asset_id", None)
            filename = getattr(photo, "filename", "photo.jpg")

            if self.check_pause_and_stop(filename, 5, count + 1, 0):
                logger.info("Stop signal received during iCloud sync. Exiting loop.")
                break

            # Check if already tracked in our database
            existing_record = self.db.get_asset_by_icloud_id(photo_id)
            needs_immich = self.is_immich_enabled and not (existing_record and existing_record.get("immich_id"))
            needs_amazon = self.is_amazon_enabled and not (existing_record and existing_record.get("amazon_id"))
            if needs_amazon and getattr(self.amazon, "session_expired", False):
                needs_amazon = False

            if not needs_immich and not needs_amazon:
                continue

            staging_path = self.temp_dir / f"icloud_{photo_id}_{filename}"
            try:
                logger.info(f"[iCloud -> Sync] Processing: {filename} ({photo_id})")
                self.icloud.download_photo(photo, staging_path)

                sha1, md5 = compute_hashes(staging_path)
                file_size = staging_path.stat().st_size

                # Check if asset exists in Immich (if Immich enabled)
                immich_id = existing_record.get("immich_id") if existing_record else None
                if needs_immich:
                    check_res = self.immich.bulk_upload_check([{"id": photo_id, "checksum": sha1}])
                    if check_res and check_res[0].get("action") == "reject" and check_res[0].get("reason") == "duplicate":
                        immich_id = check_res[0].get("assetId")
                        logger.info(f"  -> Already exists in Immich (ID: {immich_id})")
                    else:
                        if not self.dry_run:
                            immich_id = self.immich.upload_asset(
                                file_path=staging_path,
                                device_asset_id=photo_id,
                                created_at=getattr(photo, "created", None)
                            )
                            logger.info(f"  -> Uploaded to Immich (ID: {immich_id})")
                            stats["immich_uploaded"] += 1
                        else:
                            logger.info(f"  -> [DRY RUN] Would upload to Immich: {filename}")
                            immich_id = f"dry_run_{photo_id}"

                # Check if asset exists in Amazon Photos (if Amazon enabled)
                sync_videos_to_amz = (
                    getattr(self.config.sync, "sync_videos_to_amazon", False)
                    and getattr(self.config.amazon, "upload_videos", False)
                )
                if not sync_videos_to_amz and is_video_file(staging_path):
                    needs_amazon = False

                amazon_id = existing_record.get("amazon_id") if existing_record else None
                if needs_amazon:
                    existing_amazon = self.amazon.find_by_md5(md5)
                    if existing_amazon:
                        amazon_id = existing_amazon
                        logger.info(f"  -> Already exists in Amazon Photos (ID: {amazon_id})")
                    else:
                        if not self.dry_run:
                            amazon_id = self.amazon.upload_photo(staging_path, md5_hash=md5)
                            logger.info(f"  -> Uploaded to Amazon Photos (ID: {amazon_id})")
                            stats["amazon_uploaded"] += 1
                        else:
                            logger.info(f"  -> [DRY RUN] Would upload to Amazon Photos: {filename}")
                            amazon_id = f"dry_run_amz_{md5}"

                # Update database state
                if not self.dry_run:
                    self.db.upsert_asset(
                        sha1=sha1,
                        md5=md5,
                        filename=filename,
                        file_size=file_size,
                        immich_id=immich_id,
                        icloud_id=photo_id,
                        amazon_id=amazon_id,
                        status="ACTIVE"
                    )

                count += 1

            except Exception as e:
                logger.error(f"Failed processing iCloud photo {filename}: {e}")
                stats["errors"] += 1
            finally:
                if staging_path.exists():
                    try:
                        staging_path.unlink()
                    except OSError:
                        pass
                part_path = staging_path.with_suffix(staging_path.suffix + ".part")
                if part_path.exists():
                    try:
                        part_path.unlink()
                    except OSError:
                        pass

        return count

    def _sync_from_immich(self, stats: Dict[str, Any]) -> int:
        count = 0
        immich_assets = self.immich.get_all_assets()

        sync_videos_to_amz = (
            getattr(self.config.sync, "sync_videos_to_amazon", False)
            and getattr(self.config.amazon, "upload_videos", False)
        )
        sync_videos_to_icloud = (
            getattr(self.config.sync, "sync_videos_to_icloud", True)
            and getattr(self.config.icloud, "upload_videos", True)
        )
        ignore_hidden_and_locked = getattr(self.config.sync, "ignore_hidden_and_locked", True)

        # Pre-filter assets that actually need mirroring to enabled destinations
        items_to_sync = []
        for asset in immich_assets:
            immich_id = asset.get("id")
            if not immich_id:
                continue

            # Skip hidden, locked, archived, or private assets
            if ignore_hidden_and_locked and is_hidden_or_locked_asset(asset):
                filename = asset.get("originalFileName", immich_id)
                logger.debug(f"Skipping hidden/locked/archived asset from sync: {filename} ({immich_id})")
                continue

            existing = self.db.get_asset_by_immich_id(immich_id)
            is_video = is_video_asset(asset)

            # Amazon only gets pictures/photos unless video sync is explicitly enabled
            allow_amazon_for_asset = self.is_amazon_enabled and (not is_video or sync_videos_to_amz)
            allow_icloud_for_asset = self.is_icloud_enabled and (not is_video or sync_videos_to_icloud)
            needs_amazon = allow_amazon_for_asset and not (existing and existing.get("amazon_id"))
            needs_icloud = allow_icloud_for_asset and not (existing and existing.get("icloud_id"))

            if needs_amazon or needs_icloud:
                items_to_sync.append((asset, existing, needs_amazon, needs_icloud, is_video))

        total_items = len(items_to_sync)
        if total_items == 0:
            logger.info("All Immich assets are already synchronized to enabled targets.")
            self.report_progress(70, "immich_sync", "Immich Assets Up to Date", 0, 0, "", "All Immich assets already mirrored to targets.")
            return 0

        logger.info(f"Found {total_items} Immich assets that need mirroring to targets...")

        for idx, (asset, existing, needs_amazon, needs_icloud, is_video) in enumerate(items_to_sync):
            if needs_amazon and getattr(self.amazon, "session_expired", False):
                needs_amazon = False

            if not needs_amazon and not needs_icloud:
                continue

            immich_id = asset.get("id")
            filename = asset.get("originalFileName", "asset.jpg")

            # Progress tracking (allocates 10% to 70% range)
            pct = 10 + int((idx / total_items) * 60)
            if self.check_pause_and_stop(filename, pct, idx + 1, total_items):
                logger.info(f"Stop signal received during Immich sync. Exiting loop at {idx + 1}/{total_items}.")
                break

            target_labels = []
            if needs_amazon:
                target_labels.append("Amazon Photos")
            if needs_icloud:
                target_labels.append("iCloud Photos")
            target_str = " & ".join(target_labels)
            action_desc = f"Syncing {idx + 1}/{total_items}: {filename} -> {target_str}"
            self.report_progress(pct, "immich_sync", "Mirroring Photos from Immich", idx + 1, total_items, filename, action_desc)

            staging_path = self.temp_dir / f"immich_{immich_id}_{filename}"
            try:
                logger.info(f"[Immich -> Cross Sync] ({idx + 1}/{total_items}) Downloading {filename} from Immich...")
                self.immich.download_asset(immich_id, staging_path)
                sha1, md5 = compute_hashes(staging_path)
                file_size = staging_path.stat().st_size

                # Secondary safety check: respect per-destination video upload settings
                if is_video or is_video_file(staging_path):
                    if not sync_videos_to_amz and needs_amazon:
                        logger.info(f"  -> Skipping Amazon upload for {filename} (Video excluded from Amazon Photos)")
                        needs_amazon = False
                    if not sync_videos_to_icloud and needs_icloud:
                        logger.info(f"  -> Skipping iCloud upload for {filename} (Video excluded from iCloud Photos)")
                        needs_icloud = False

                # 1. Sync to Amazon Photos if enabled and missing
                amazon_id = existing.get("amazon_id") if existing else None
                if needs_amazon:
                    existing_amazon = self.amazon.find_by_md5(md5)
                    if existing_amazon:
                        amazon_id = existing_amazon
                        logger.info(f"  -> Already exists in Amazon Photos (ID: {amazon_id})")
                    else:
                        if not self.dry_run:
                            amazon_id = self.amazon.upload_photo(staging_path, md5_hash=md5)
                            logger.info(f"  -> Uploaded to Amazon Photos (ID: {amazon_id})")
                            stats["amazon_uploaded"] += 1
                        else:
                            logger.info(f"  -> [DRY RUN] Would upload to Amazon Photos: {filename}")
                            amazon_id = f"dry_run_amz_{md5}"

                # 2. Sync to iCloud Photos if enabled and missing
                icloud_id = existing.get("icloud_id") if existing else None
                if needs_icloud:
                    if not self.dry_run:
                        icloud_id = self.icloud.upload_photo(staging_path)
                        if icloud_id:
                            logger.info(f"  -> Uploaded to iCloud Photos (ID: {icloud_id})")
                            stats["icloud_uploaded"] = stats.get("icloud_uploaded", 0) + 1
                    else:
                        logger.info(f"  -> [DRY RUN] Would upload to iCloud Photos: {filename}")
                        icloud_id = f"dry_run_icloud_{sha1}"

                if not self.dry_run:
                    self.db.upsert_asset(
                        sha1=sha1,
                        md5=md5,
                        filename=filename,
                        file_size=file_size,
                        immich_id=immich_id,
                        icloud_id=icloud_id,
                        amazon_id=amazon_id,
                        status="ACTIVE"
                    )
                count += 1
            except Exception as e:
                logger.error(f"Failed syncing Immich asset {filename} to Amazon/iCloud: {e}")
                stats["errors"] += 1
            finally:
                if staging_path.exists():
                    try:
                        staging_path.unlink()
                    except OSError:
                        pass
                part_path = staging_path.with_suffix(staging_path.suffix + ".part")
                if part_path.exists():
                    try:
                        part_path.unlink()
                    except OSError:
                        pass

        if self.stop_event and self.stop_event.is_set():
            return count

        self.report_progress(70, "immich_sync", "Photo Mirroring Complete", total_items, total_items, "", f"Mirrored {count} assets from Immich.")
        return count

    def _sync_deletions(self, force: bool = False) -> int:
        active_assets_immich = self.immich.get_all_assets()
        ignore_hidden_and_locked = getattr(self.config.sync, "ignore_hidden_and_locked", True)

        live_immich_ids: Set[str] = set()
        for a in active_assets_immich:
            if "id" in a:
                if ignore_hidden_and_locked and is_hidden_or_locked_asset(a):
                    continue
                live_immich_ids.add(a["id"])

        tracked_active = self.db.get_active_assets()
        # Disallowed video purge on Amazon Photos if video sync is disabled
        sync_videos_to_amz = (
            getattr(self.config.sync, "sync_videos_to_amazon", False)
            and getattr(self.config.amazon, "upload_videos", False)
        )
        if self.is_amazon_enabled and not sync_videos_to_amz:
            for a in tracked_active:
                fn = a.get("filename") or ""
                if self.check_pause_and_stop(fn, 70, 0, len(tracked_active)):
                    return 0
                amz_id = a.get("amazon_id")
                if amz_id and amz_id != "CONFLICT_EXISTING" and not amz_id.startswith("dry_run") and is_video_file(fn):
                    logger.info(f"Purging video '{fn}' from Amazon Photos (video sync disabled)...")
                    if not self.dry_run:
                        self.amazon.purge_nodes([amz_id])
                        with self.db._get_connection() as conn:
                            conn.execute("UPDATE assets SET amazon_id = NULL WHERE id = ?", (a["id"],))
                            conn.commit()

        deletion_candidates = [
            a for a in tracked_active
            if a.get("immich_id") and a["immich_id"] not in live_immich_ids
        ]

        if not deletion_candidates:
            logger.info("No deleted assets detected in Immich.")
            self.report_progress(90, "deletions", "Deletions Checked", 0, 0, "", "No deleted assets detected in Immich.")
            return 0

        num_deletions = len(deletion_candidates)
        max_limit = self.config.sync.max_delete_limit

        logger.info(f"Detected {num_deletions} assets deleted in Immich.")

        if num_deletions > max_limit and not force:
            msg = (
                f"[SAFETY GUARD TRIGGERED] Number of deletions ({num_deletions}) exceeds "
                f"max_delete_limit ({max_limit}). Deletion sync skipped to protect against accidental mass deletion. "
                f"Pass --force-deletes or increase max_delete_limit in config.json to proceed."
            )
            logger.warning(msg)
            self.report_progress(90, "deletions", "Deletion Guard Triggered", 0, num_deletions, "", msg)
            return 0

        # Batch lookup iCloud photos if iCloud enabled
        icloud_photos_map = {}
        if self.is_icloud_enabled:
            target_icloud_ids = {
                a["icloud_id"] for a in deletion_candidates
                if a.get("icloud_id") and not a["icloud_id"].startswith("dry_run")
            }
            if target_icloud_ids:
                try:
                    icloud_photos_map = self.icloud.find_photos_by_ids(target_icloud_ids)
                except Exception as e:
                    logger.warning(f"Failed batch lookup in iCloud: {e}")

        synced_count = 0
        for idx, asset in enumerate(deletion_candidates):
            immich_id = asset["immich_id"]
            amazon_id = asset.get("amazon_id")
            icloud_id = asset.get("icloud_id")
            filename = asset.get("filename", "asset")

            pct = 70 + int((idx / num_deletions) * 20)
            if self.check_pause_and_stop(filename, pct, idx + 1, num_deletions):
                logger.info(f"Stop signal received during deletion sync. Exiting loop at {idx + 1}/{num_deletions}.")
                break

            self.report_progress(pct, "deletions", "Propagating Deletions", idx + 1, num_deletions, filename, f"Deleting {idx + 1}/{num_deletions}: {filename} -> Trash")
            logger.info(f"[Sync Deletion] ({idx + 1}/{num_deletions}) Propagating deletion of '{filename}' (Immich ID: {immich_id})...")

            # 1. Trash in Amazon Photos (if Amazon enabled)
            amazon_ok = True
            if self.is_amazon_enabled and amazon_id and amazon_id != "CONFLICT_EXISTING" and not amazon_id.startswith("dry_run"):
                if not self.dry_run:
                    res = self.amazon.trash_nodes([amazon_id])
                    if res:
                        logger.info(f"  -> Moved '{filename}' to Trash in Amazon Photos.")
                    else:
                        logger.error(f"  -> Failed to trash '{filename}' in Amazon Photos.")
                        amazon_ok = False
                else:
                    logger.info(f"  -> [DRY RUN] Would trash '{filename}' in Amazon Photos.")

            # 2. Delete in iCloud Photos (if iCloud enabled)
            icloud_ok = True
            if self.is_icloud_enabled and icloud_id and not icloud_id.startswith("dry_run"):
                if not self.dry_run:
                    photo = icloud_photos_map.get(icloud_id) or self.icloud.find_photo_by_id(icloud_id)
                    if photo:
                        res = self.icloud.delete_photo(photo)
                        if res:
                            logger.info(f"  -> Moved '{filename}' to Recently Deleted in iCloud Photos.")
                        else:
                            icloud_ok = False
                    else:
                        logger.warning(f"  -> Photo {icloud_id} not found in iCloud library.")
                else:
                    logger.info(f"  -> [DRY RUN] Would delete '{filename}' from iCloud Photos.")

            if not self.dry_run:
                if amazon_ok and icloud_ok:
                    self.db.mark_asset_deleted(immich_id, reason="DELETED_IN_IMMICH")
                    self.db.log_sync("DELETE", f"Synced deletion of {filename}")
                    synced_count += 1
                else:
                    logger.warning(f"Deletion of '{filename}' deferred: target service failed (Amazon OK: {amazon_ok}, iCloud OK: {icloud_ok}).")

        if self.stop_event and self.stop_event.is_set():
            return synced_count

        self.report_progress(90, "deletions", "Deletions Synced", num_deletions, num_deletions, "", f"Propagated {synced_count} deletions from Immich.")
        return synced_count

    def _sync_albums(self) -> int:
        immich_albums = self.immich.get_albums()
        synced_albums = 0
        total_albums = len(immich_albums)

        if total_albums == 0:
            self.report_progress(99, "albums", "Albums Checked", 0, 0, "", "No albums found in Immich.")
            return 0

        ignore_hidden_and_locked = getattr(self.config.sync, "ignore_hidden_and_locked", True)

        for idx, album in enumerate(immich_albums):
            immich_album_id = album.get("id")
            album_name = album.get("albumName", "Untitled Album")

            if ignore_hidden_and_locked:
                name_lower = album_name.lower()
                if any(k in name_lower for k in ("locked", "hidden", "private", "secret")):
                    logger.info(f"Skipping private/locked album '{album_name}' from sync.")
                    continue

            pct = 90 + int((idx / total_albums) * 9)
            if self.check_pause_and_stop(album_name, pct, idx + 1, total_albums):
                logger.info(f"Stop signal received during album sync. Exiting loop at {idx + 1}/{total_albums}.")
                break

            self.report_progress(pct, "albums", "Syncing Albums", idx + 1, total_albums, album_name, f"Syncing album {idx + 1}/{total_albums}: '{album_name}'")

            details = self.immich.get_album_details(immich_album_id)
            album_assets = details.get("assets", [])
            asset_ids = [a["id"] for a in album_assets if "id" in a]

            logger.info(f"[Sync Album] Checking Immich album '{album_name}' ({len(asset_ids)} photos)...")

            # Resolve Amazon node IDs for these photos
            amazon_node_ids = []
            for aid in asset_ids:
                record = self.db.get_asset_by_immich_id(aid)
                if record and record.get("amazon_id") and not record["amazon_id"].startswith("dry_run"):
                    amazon_node_ids.append(record["amazon_id"])

            thumb_asset_id = details.get("albumThumbnailAssetId")
            cover_node_id = None
            if thumb_asset_id:
                thumb_rec = self.db.get_asset_by_immich_id(thumb_asset_id)
                if thumb_rec and thumb_rec.get("amazon_id") and not thumb_rec["amazon_id"].startswith("dry_run"):
                    cover_node_id = thumb_rec["amazon_id"]

            db_album = self.db.get_album(immich_album_id)

            # Sync to Amazon Photos (if Amazon enabled)
            amazon_album_id = db_album.get("amazon_id") if db_album else None
            if self.is_amazon_enabled:
                if not amazon_album_id:
                    if not amazon_node_ids:
                        logger.info(f"  -> Skipping creation of empty/video-only album '{album_name}' in Amazon Photos (0 synced photos).")
                    elif not self.dry_run:
                        if cover_node_id:
                            amazon_album_id = self.amazon.create_album(album_name, amazon_node_ids, cover_node_id=cover_node_id)
                        else:
                            amazon_album_id = self.amazon.create_album(album_name, amazon_node_ids)
                        logger.info(f"  -> Created album '{album_name}' in Amazon Photos (ID: {amazon_album_id})")
                    else:
                        logger.info(f"  -> [DRY RUN] Would create album '{album_name}' in Amazon Photos with {len(amazon_node_ids)} items.")
                        amazon_album_id = f"dry_run_album_{album_name}"
                else:
                    if not self.dry_run and amazon_node_ids:
                        self.amazon.add_to_album(amazon_album_id, amazon_node_ids, cover_node_id=cover_node_id)
                        logger.info(f"  -> Updated album '{album_name}' in Amazon Photos with {len(amazon_node_ids)} items.")

            # Sync to iCloud Photos (if iCloud enabled)
            icloud_album_id = db_album.get("icloud_id") if db_album else None
            if self.is_icloud_enabled:
                try:
                    if not self.dry_run:
                        icloud_album = self.icloud.create_album(album_name)
                        if icloud_album:
                            logger.info(f"  -> Created/Verified album '{album_name}' in iCloud Photos.")
                            for aid in asset_ids:
                                if self.stop_event and self.stop_event.is_set():
                                    break
                                record = self.db.get_asset_by_immich_id(aid)
                                if record and record.get("icloud_id"):
                                    photo = self.icloud.find_photo_by_id(record["icloud_id"])
                                    if photo:
                                        self.icloud.add_to_album(album_name, photo)
                    else:
                        logger.info(f"  -> [DRY RUN] Would sync album '{album_name}' to iCloud Photos.")
                except Exception as e:
                    logger.warning(f"  -> Note: iCloud album sync for '{album_name}' had limitations: {e}")

            if not self.dry_run:
                self.db.upsert_album(
                    immich_id=immich_album_id,
                    name=album_name,
                    amazon_id=amazon_album_id,
                    icloud_id=icloud_album_id
                )

            synced_albums += 1

        return synced_albums
