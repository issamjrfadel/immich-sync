import unittest
from unittest.mock import MagicMock, patch
import tempfile
import json
from pathlib import Path

from src.config import load_config, AppConfig, ImmichConfig, ICloudConfig, AmazonConfig, SyncSettings
from src.database import SyncDatabase
from src.engine import SyncEngine
from src.utils import compute_hashes, get_mime_type, format_bytes

class TestSyncComponents(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_state.db"
        self.db = SyncDatabase(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_database_asset_lifecycle(self):
        # 1. Upsert new asset
        self.db.upsert_asset(
            sha1="abc123sha1",
            md5="abc123md5",
            filename="vacation.jpg",
            file_size=102400,
            immich_id="immich-uuid-1",
            icloud_id="icloud-asset-1",
            amazon_id="amz-node-1"
        )

        asset = self.db.get_asset_by_sha1("abc123sha1")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["filename"], "vacation.jpg")
        self.assertEqual(asset["immich_id"], "immich-uuid-1")
        self.assertEqual(asset["status"], "ACTIVE")

        # 2. Check active assets
        active = self.db.get_active_assets()
        self.assertEqual(len(active), 1)

        # 3. Mark asset deleted from Immich
        self.db.mark_asset_deleted("immich-uuid-1", reason="DELETED_IN_IMMICH")
        active_after = self.db.get_active_assets()
        self.assertEqual(len(active_after), 0)

        updated_asset = self.db.get_asset_by_immich_id("immich-uuid-1")
        self.assertEqual(updated_asset["status"], "DELETED_IN_IMMICH")

    def test_database_albums(self):
        # 1. Track album
        self.db.upsert_album(
            immich_id="immich-alb-1",
            name="Summer Trip",
            amazon_id="amz-alb-1"
        )

        album = self.db.get_album("immich-alb-1")
        self.assertIsNotNone(album)
        self.assertEqual(album["name"], "Summer Trip")
        self.assertEqual(album["amazon_id"], "amz-alb-1")

        # 2. Update album with icloud ID
        self.db.upsert_album(
            immich_id="immich-alb-1",
            name="Summer Trip Updated",
            icloud_id="icloud-alb-1"
        )
        album_updated = self.db.get_album("immich-alb-1")
        self.assertEqual(album_updated["name"], "Summer Trip Updated")
        self.assertEqual(album_updated["amazon_id"], "amz-alb-1")
        self.assertEqual(album_updated["icloud_id"], "icloud-alb-1")

    def test_utils_hashing(self):
        sample_file = Path(self.temp_dir.name) / "sample.txt"
        sample_file.write_bytes(b"hello photo sync world")
        sha1, md5 = compute_hashes(sample_file)
        self.assertTrue(len(sha1) == 40)
        self.assertTrue(len(md5) == 32)

    def test_mime_detection(self):
        self.assertEqual(get_mime_type("photo.heic"), "image/heic")
        self.assertEqual(get_mime_type("photo.jpg"), "image/jpeg")
        self.assertEqual(get_mime_type("video.mov"), "video/quicktime")

    def test_config_parsing(self):
        sample_cfg = {
            "immich": {
                "url": "http://localhost:2283",
                "api_key": "test_key",
                "verify_ssl": True
            },
            "icloud": {
                "apple_id": "test@icloud.com",
                "session_dir": "./.icloud_session"
            },
            "amazon": {
                "cookies": {"session-id": "test-session"},
                "tld": "com"
            },
            "sync": {
                "sync_deletions": True,
                "sync_albums": True,
                "max_delete_limit": 25
            }
        }
        cfg_file = Path(self.temp_dir.name) / "config.json"
        cfg_file.write_text(json.dumps(sample_cfg), encoding="utf-8")

        loaded = load_config(cfg_file)
        self.assertEqual(loaded.immich.url, "http://localhost:2283")
        self.assertEqual(loaded.sync.max_delete_limit, 25)
        self.assertTrue(loaded.sync.sync_deletions)

    def test_engine_deletion_sync(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com"),
            amazon=AmazonConfig(cookies={"session-id": "test"}),
            sync=SyncSettings(max_delete_limit=10, sync_deletions=True, dry_run=False)
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Insert active asset in DB
        engine.db.upsert_asset(
            sha1="hash111",
            filename="delete_me.jpg",
            immich_id="immich-123",
            icloud_id="icloud-123",
            amazon_id="amz-123",
            status="ACTIVE"
        )

        # Mock Immich returning empty list of active assets (asset was deleted in Immich)
        engine.immich.get_all_assets = MagicMock(return_value=[])

        # Mock Amazon trash and iCloud delete
        engine.amazon.trash_nodes = MagicMock(return_value=True)
        fake_photo = MagicMock()
        engine.icloud.find_photo_by_id = MagicMock(return_value=fake_photo)
        engine.icloud.delete_photo = MagicMock(return_value=True)

        deleted_count = engine._sync_deletions(force=False)
        self.assertEqual(deleted_count, 1)

        # Verify Amazon trash called
        engine.amazon.trash_nodes.assert_called_once_with(["amz-123"])

        # Verify iCloud delete called
        engine.icloud.delete_photo.assert_called_once_with(fake_photo)

        # Verify DB status updated
        asset = engine.db.get_asset_by_immich_id("immich-123")
        self.assertEqual(asset["status"], "DELETED_IN_IMMICH")

    def test_engine_deletion_safety_guard(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com"),
            amazon=AmazonConfig(cookies={"session-id": "test"}),
            sync=SyncSettings(max_delete_limit=2, sync_deletions=True, dry_run=False)
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Insert 3 active assets
        for i in range(3):
            engine.db.upsert_asset(
                sha1=f"hash_{i}",
                filename=f"file_{i}.jpg",
                immich_id=f"immich_{i}",
                status="ACTIVE"
            )

        # Immich returns empty (3 deletions exceed max_delete_limit of 2)
        engine.immich.get_all_assets = MagicMock(return_value=[])
        engine.amazon.trash_nodes = MagicMock()

        deleted_count = engine._sync_deletions(force=False)
        # Should be blocked by safety guard
        self.assertEqual(deleted_count, 0)
        engine.amazon.trash_nodes.assert_not_called()

        # With force=True, it should proceed
        engine.amazon.trash_nodes = MagicMock(return_value=True)
        deleted_count_forced = engine._sync_deletions(force=True)
        self.assertEqual(deleted_count_forced, 3)

    def test_engine_album_sync(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com"),
            amazon=AmazonConfig(cookies={"session-id": "test"}),
            sync=SyncSettings(sync_albums=True, dry_run=False)
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Track an asset in DB
        engine.db.upsert_asset(
            sha1="photo_hash_1",
            filename="nature.jpg",
            immich_id="immich-photo-1",
            amazon_id="amz-node-99"
        )

        # Mock Immich album
        engine.immich.get_albums = MagicMock(return_value=[
            {"id": "immich-alb-55", "albumName": "Nature 2026"}
        ])
        engine.immich.get_album_details = MagicMock(return_value={
            "id": "immich-alb-55",
            "assets": [{"id": "immich-photo-1"}]
        })

        engine.amazon.create_album = MagicMock(return_value="amz-alb-77")
        engine.icloud.create_album = MagicMock(return_value=MagicMock())

        synced_albums = engine._sync_albums()
        self.assertEqual(synced_albums, 1)

        # Verify Amazon album created with node
        engine.amazon.create_album.assert_called_once_with("Nature 2026", ["amz-node-99"])

        # Verify DB tracked album mapping
        alb = engine.db.get_album("immich-alb-55")
        self.assertIsNotNone(alb)
        self.assertEqual(alb["name"], "Nature 2026")
        self.assertEqual(alb["amazon_id"], "amz-alb-77")

    def test_engine_immich_direct_sync_to_amazon_and_icloud(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com"),
            amazon=AmazonConfig(cookies={"session-id": "test"}),
            sync=SyncSettings(download_temp_dir=str(self.temp_dir.name), dry_run=False)
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Asset uploaded directly to Immich
        engine.immich.get_all_assets = MagicMock(return_value=[
            {"id": "immich-direct-101", "originalFileName": "camera_card.jpg"}
        ])

        # Mock download from Immich
        def mock_download(asset_id, dest_path):
            Path(dest_path).write_bytes(b"camera raw bytes 12345")
            return dest_path

        engine.immich.download_asset = MagicMock(side_effect=mock_download)
        engine.amazon.find_by_md5 = MagicMock(return_value=None)
        engine.amazon.upload_photo = MagicMock(return_value="amz-direct-node-555")
        engine.icloud.upload_photo = MagicMock(return_value="icloud-direct-asset-777")

        stats = {"immich_direct_synced": 0, "amazon_uploaded": 0, "icloud_uploaded": 0, "errors": 0}
        count = engine._sync_from_immich(stats=stats)

        self.assertEqual(count, 1)
        self.assertEqual(stats["amazon_uploaded"], 1)
        self.assertEqual(stats["icloud_uploaded"], 1)

        # Verify calls to both Amazon and iCloud upload
        engine.amazon.upload_photo.assert_called_once()
        engine.icloud.upload_photo.assert_called_once()

        # Verify DB tracked the asset with all IDs mapped
        asset = engine.db.get_asset_by_immich_id("immich-direct-101")
        self.assertIsNotNone(asset)
        self.assertEqual(asset["amazon_id"], "amz-direct-node-555")
        self.assertEqual(asset["icloud_id"], "icloud-direct-asset-777")
        self.assertEqual(asset["filename"], "camera_card.jpg")
        self.assertEqual(asset["status"], "ACTIVE")

    def test_amazon_client_canadian_marketplace(self):
        from src.clients.amazon import AmazonPhotosClient, RegionalAmazonPhotos
        cfg = AmazonConfig(
            cookies={
                "session-id": "141-test-session",
                "ubid-acbca": "131-test-ubid",
                "at-acbca": "Atza|test-token"
            },
            tld="ca"
        )
        client = AmazonPhotosClient(cfg)
        self.assertEqual(client.tld, "ca")
        # Ensure cookie normalization populates both regional and fallback keys
        self.assertIn("ubid-acbca", client.cookies)
        self.assertIn("at-acbca", client.cookies)
        self.assertIn("ubid-main", client.cookies)
        self.assertIn("at-main", client.cookies)

        # Verify RegionalAmazonPhotos subclass targets Canadian drive URL
        with patch.object(RegionalAmazonPhotos, "__init__", return_value=None):
            ap = RegionalAmazonPhotos.__new__(RegionalAmazonPhotos)
            ap._target_tld = "ca"
            self.assertEqual(ap.determine_tld(client.cookies), "ca")
            ap.tld = "ca"
            ap.drive_url = f"https://www.amazon.{ap.tld}/drive/v1"
            ap.cdproxy_url = ap.determine_cdproxy(None)
            self.assertEqual(ap.drive_url, "https://www.amazon.ca/drive/v1")
            self.assertEqual(ap.cdproxy_url, "https://content-na.drive.amazonaws.com/cdproxy/nodes")

    def test_engine_with_disabled_amazon_host(self):
        # Configure engine with Amazon disabled
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy", enabled=True),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=True),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=False),
            sync=SyncSettings(
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False,
                enable_amazon=False
            )
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))
        self.assertFalse(engine.is_amazon_enabled)
        self.assertTrue(engine.is_immich_enabled)
        self.assertTrue(engine.is_icloud_enabled)

        # Asset uploaded directly to Immich
        engine.immich.get_all_assets = MagicMock(return_value=[
            {"id": "immich-direct-disabled-amz", "originalFileName": "test.jpg"}
        ])

        def mock_download(asset_id, dest_path):
            Path(dest_path).write_bytes(b"image bytes 123")
            return dest_path

        engine.immich.download_asset = MagicMock(side_effect=mock_download)
        engine.amazon.upload_photo = MagicMock()
        engine.icloud.upload_photo = MagicMock(return_value="icloud-asset-only")

        stats = {"immich_direct_synced": 0, "amazon_uploaded": 0, "icloud_uploaded": 0, "errors": 0}
        count = engine._sync_from_immich(stats=stats)

        self.assertEqual(count, 1)
        self.assertEqual(stats["icloud_uploaded"], 1)
        self.assertEqual(stats["amazon_uploaded"], 0)
        # Amazon upload should NOT have been called because Amazon was disabled
        engine.amazon.upload_photo.assert_not_called()
        engine.icloud.upload_photo.assert_called_once()

    def test_amazon_find_by_md5_queries_nodes_endpoint(self):
        from src.clients.amazon import AmazonPhotosClient
        cfg = AmazonConfig(cookies={"session-id": "test-session"}, tld="ca")
        client = AmazonPhotosClient(cfg)

        mock_ap = MagicMock()
        mock_ap.drive_url = "https://www.amazon.ca/drive/v1"
        client._ap = mock_ap

        # Mock 200 response with data
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": [{"id": "node-md5-found-123"}]}
        mock_ap.client.get.return_value = mock_resp

        node_id = client.find_by_md5("6c0dab90197309050e02748a820db05c")
        self.assertEqual(node_id, "node-md5-found-123")

        # Verify it targeted /nodes with filters, not /search
        called_url, called_kwargs = mock_ap.client.get.call_args
        self.assertEqual(called_url[0], "https://www.amazon.ca/drive/v1/nodes")
        self.assertIn("filters", called_kwargs["params"])
        self.assertEqual(called_kwargs["params"]["filters"], "contentProperties.md5:6c0dab90197309050e02748a820db05c")

        # Verify it handles 400 Bad Request safely without raising exception
        mock_400_resp = MagicMock()
        mock_400_resp.status_code = 400
        mock_400_resp.text = '{"message":"Invalid fieldName"}'
        mock_ap.client.get.return_value = mock_400_resp

        result = client.find_by_md5("bad_hash")
        self.assertIsNone(result)

    def test_one_way_sync_skips_icloud_to_immich_ingestion(self):
        # Configure one-way sync with Immich as master
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=True),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=True),
            sync=SyncSettings(
                one_way_immich_master=True,
                ingest_from_icloud=False,
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False
            )
        )
        progress_events = []
        engine = SyncEngine(cfg, db_path=str(self.db_path), progress_callback=lambda p: progress_events.append(p))

        # Mock clients
        engine.icloud.connect = MagicMock()
        engine._sync_from_icloud = MagicMock()
        engine.immich.get_all_assets = MagicMock(return_value=[])
        engine.immich.get_albums = MagicMock(return_value=[])

        stats = engine.run_sync_cycle()

        # Step 1 (_sync_from_icloud) MUST NOT be called in one-way mode
        engine._sync_from_icloud.assert_not_called()
        self.assertEqual(stats["icloud_ingested"], 0)
        self.assertEqual(stats["immich_uploaded"], 0)

        # Progress events should reach 100%
        self.assertTrue(len(progress_events) > 0)
        self.assertEqual(progress_events[-1]["percent"], 100)
        self.assertEqual(progress_events[-1]["phase"], "completed")

    def test_one_way_deletion_propagation_to_amazon_and_icloud(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=True),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=True),
            sync=SyncSettings(
                one_way_immich_master=True,
                sync_deletions=True,
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False
            )
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Seed asset in database that is active
        self.db.upsert_asset(
            sha1="del_hash_1",
            filename="deleted_family_photo.jpg",
            immich_id="immich-photo-to-del",
            amazon_id="amz-node-del-1",
            icloud_id="icloud-asset-del-1"
        )

        # Immich now returns empty assets (photo was deleted in Immich)
        engine.immich.get_all_assets = MagicMock(return_value=[])

        # Mock Amazon trash and iCloud delete
        engine.amazon.trash_nodes = MagicMock(return_value=True)
        mock_icloud_photo = MagicMock()
        mock_icloud_photo.delete = MagicMock(return_value=True)
        engine.icloud.find_photos_by_ids = MagicMock(return_value={"icloud-asset-del-1": mock_icloud_photo})
        engine.icloud.delete_photo = MagicMock(return_value=True)

        deletions_count = engine._sync_deletions()
        self.assertEqual(deletions_count, 1)

        # Verified trashed in Amazon Photos
        engine.amazon.trash_nodes.assert_called_once_with(["amz-node-del-1"])

        # Verified deleted in iCloud Photos
        engine.icloud.delete_photo.assert_called_once_with(mock_icloud_photo)

        # Verified marked DELETED_IN_IMMICH in database
        asset = self.db.get_asset_by_immich_id("immich-photo-to-del")
        self.assertEqual(asset["status"], "DELETED_IN_IMMICH")

    def test_video_detection_utils(self):
        from src.utils import is_video_file, is_video_asset

        self.assertTrue(is_video_file("trip.mp4"))
        self.assertTrue(is_video_file("movie.MOV"))
        self.assertTrue(is_video_file("clip.mkv"))
        self.assertFalse(is_video_file("photo.jpg"))
        self.assertFalse(is_video_file("image.heic"))
        self.assertFalse(is_video_file("raw.dng"))

        video_asset_1 = {"type": "VIDEO", "originalFileName": "clip.mp4"}
        video_asset_2 = {"type": "IMAGE", "originalMimeType": "video/mp4"}
        video_asset_3 = {"originalFileName": "birthday.MOV"}
        photo_asset = {"type": "IMAGE", "originalFileName": "family.jpg", "originalMimeType": "image/jpeg"}

        self.assertTrue(is_video_asset(video_asset_1))
        self.assertTrue(is_video_asset(video_asset_2))
        self.assertTrue(is_video_asset(video_asset_3))
        self.assertFalse(is_video_asset(photo_asset))

    def test_hidden_and_locked_detection_utils(self):
        from src.utils import is_hidden_or_locked_asset

        self.assertTrue(is_hidden_or_locked_asset({"visibility": "hidden"}))
        self.assertTrue(is_hidden_or_locked_asset({"visibility": "locked"}))
        self.assertTrue(is_hidden_or_locked_asset({"visibility": "archive"}))
        self.assertTrue(is_hidden_or_locked_asset({"isArchived": True}))
        self.assertTrue(is_hidden_or_locked_asset({"isTrashed": True}))
        self.assertTrue(is_hidden_or_locked_asset({"isLocked": True}))
        self.assertTrue(is_hidden_or_locked_asset({"originalPath": "/data/upload/locked/secret.jpg"}))
        self.assertTrue(is_hidden_or_locked_asset({"originalFileName": ".hidden_pic.jpg"}))

        normal_asset = {"visibility": "timeline", "isArchived": False, "isTrashed": False, "originalFileName": "normal.jpg"}
        self.assertFalse(is_hidden_or_locked_asset(normal_asset))

    def test_videos_excluded_from_amazon_sync(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=False),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=True, upload_videos=False),
            sync=SyncSettings(
                one_way_immich_master=True,
                sync_videos_to_amazon=False,
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False
            )
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        # Assets: 1 photo and 1 video
        photo_asset = {"id": "immich-photo-1", "type": "IMAGE", "originalFileName": "photo1.jpg"}
        video_asset = {"id": "immich-video-2", "type": "VIDEO", "originalFileName": "video2.mp4"}
        engine.immich.get_all_assets = MagicMock(return_value=[photo_asset, video_asset])

        def fake_download(asset_id, dest_path):
            Path(dest_path).write_bytes(b"dummy image data" if "photo" in asset_id else b"dummy video data")

        engine.immich.download_asset = MagicMock(side_effect=fake_download)
        engine.amazon.find_by_md5 = MagicMock(return_value=None)
        engine.amazon.upload_photo = MagicMock(return_value="amz-node-uploaded")

        stats = {"amazon_uploaded": 0}
        engine._sync_from_immich(stats=stats)

        # Only the photo was uploaded to Amazon Photos; video was skipped!
        self.assertEqual(engine.amazon.upload_photo.call_count, 1)
        self.assertEqual(stats["amazon_uploaded"], 1)

    def test_hidden_and_locked_assets_excluded_from_sync(self):
        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=False),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=True),
            sync=SyncSettings(
                one_way_immich_master=True,
                ignore_hidden_and_locked=True,
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False
            )
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        hidden_asset = {"id": "immich-hidden-1", "type": "IMAGE", "visibility": "hidden", "originalFileName": "hidden.jpg"}
        archived_asset = {"id": "immich-archived-2", "type": "IMAGE", "isArchived": True, "originalFileName": "archived.jpg"}
        normal_asset = {"id": "immich-normal-3", "type": "IMAGE", "visibility": "timeline", "originalFileName": "normal.jpg"}

        engine.immich.get_all_assets = MagicMock(return_value=[hidden_asset, archived_asset, normal_asset])

        def fake_download(asset_id, dest_path):
            Path(dest_path).write_bytes(b"normal image content")

        engine.immich.download_asset = MagicMock(side_effect=fake_download)
        engine.amazon.find_by_md5 = MagicMock(return_value=None)
        engine.amazon.upload_photo = MagicMock(return_value="amz-normal-uploaded")

        stats = {"amazon_uploaded": 0}
        engine._sync_from_immich(stats=stats)

        # Only normal asset was downloaded and uploaded; hidden/archived were completely skipped
        self.assertEqual(engine.immich.download_asset.call_count, 1)
        self.assertEqual(engine.amazon.upload_photo.call_count, 1)

    def test_env_file_bootstrap_and_overrides(self):
        cfg_file = Path(self.temp_dir.name) / "config.json"
        env_file = Path(self.temp_dir.name) / ".env"

        # 1. Bootstrap when config.json does not exist yet
        env_file.write_text(
            "\n".join([
                "IMMICH_URL=http://192.168.1.50:2283",
                "IMMICH_API_KEY=env-api-key",
                "ICLOUD_APPLE_ID=env@icloud.com",
                "ICLOUD_UPLOAD_VIDEOS=false",
                "AMAZON_TLD=ca",
                "AMAZON_SESSION_ID=env-sess-1",
                "AMAZON_UBID=env-ubid-2",
                "AMAZON_AT_TOKEN=Atza|env-token-3",
                "POLL_INTERVAL_SECONDS=180",
            ]),
            encoding="utf-8",
        )
        self.assertFalse(cfg_file.exists())

        bootstrapped = load_config(cfg_file)
        self.assertTrue(cfg_file.exists())
        self.assertEqual(bootstrapped.immich.url, "http://192.168.1.50:2283")
        self.assertEqual(bootstrapped.immich.api_key, "env-api-key")
        self.assertEqual(bootstrapped.icloud.apple_id, "env@icloud.com")
        self.assertFalse(bootstrapped.icloud.upload_videos)
        self.assertFalse(bootstrapped.sync.sync_videos_to_icloud)
        self.assertEqual(bootstrapped.amazon.tld, "ca")
        self.assertEqual(bootstrapped.amazon.cookies["session-id"], "env-sess-1")
        self.assertEqual(bootstrapped.amazon.cookies["ubid-acbca"], "env-ubid-2")
        self.assertEqual(bootstrapped.amazon.cookies["at-acbca"], "Atza|env-token-3")
        self.assertEqual(bootstrapped.sync.poll_interval_seconds, 180)

        # 2. Once config.json exists, Web UI / config.json edits are NOT overwritten by .env unless ENV_OVERRIDE=true
        env_file.write_text(
            "\n".join([
                "IMMICH_URL=http://192.168.1.99:2283",
                "IMMICH_API_KEY=",
                "POLL_INTERVAL_SECONDS=600",
                "ICLOUD_UPLOAD_VIDEOS=true",
            ]),
            encoding="utf-8",
        )
        preserved = load_config(cfg_file)
        self.assertEqual(preserved.immich.url, "http://192.168.1.50:2283")
        self.assertEqual(preserved.immich.api_key, "env-api-key")
        self.assertEqual(preserved.sync.poll_interval_seconds, 180)
        self.assertFalse(preserved.icloud.upload_videos)

        # 3. With ENV_OVERRIDE=true explicitly set, .env overrides existing config.json
        env_file.write_text(
            "\n".join([
                "ENV_OVERRIDE=true",
                "IMMICH_URL=http://192.168.1.99:2283",
                "IMMICH_API_KEY=",
                "POLL_INTERVAL_SECONDS=600",
                "ICLOUD_UPLOAD_VIDEOS=true",
            ]),
            encoding="utf-8",
        )
        overridden = load_config(cfg_file)
        self.assertEqual(overridden.immich.url, "http://192.168.1.99:2283")
        self.assertEqual(overridden.immich.api_key, "env-api-key")
        self.assertEqual(overridden.sync.poll_interval_seconds, 600)
        self.assertTrue(overridden.icloud.upload_videos)
        self.assertTrue(overridden.sync.sync_videos_to_icloud)

    def test_untracked_immich_trash_trashed_on_amazon_and_counted_as_deleted(self):
        import base64
        import hashlib

        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com", enabled=False),
            amazon=AmazonConfig(cookies={"session-id": "test"}, enabled=True),
            sync=SyncSettings(
                max_delete_limit=10,
                sync_deletions=True,
                enable_icloud=False,
                download_temp_dir=str(self.temp_dir.name),
                dry_run=False,
            ),
        )
        engine = SyncEngine(cfg, db_path=str(self.db_path))

        content_active = b"trashed-in-immich-still-available-on-amazon"
        content_already_amz_trash = b"trashed-in-immich-already-in-amazon-trash"
        sha1_a = hashlib.sha1(content_active).digest()
        sha1_b = hashlib.sha1(content_already_amz_trash).digest()

        trashed_items = [
            {
                "id": "immich-trash-1",
                "originalFileName": "old_photo.jpg",
                "isTrashed": True,
                "checksum": base64.b64encode(sha1_a).decode("ascii"),
            },
            {
                "id": "immich-trash-2",
                "originalFileName": "already_amz_trashed.jpg",
                "isTrashed": True,
                "checksum": base64.b64encode(sha1_b).decode("ascii"),
            },
        ]

        engine.immich.get_all_assets = MagicMock(return_value=[])
        engine.immich.get_trashed_assets = MagicMock(return_value=trashed_items)

        def fake_download(asset_id, dest_path):
            Path(dest_path).write_bytes(content_active if asset_id == "immich-trash-1" else content_already_amz_trash)
            return Path(dest_path)

        engine.immich.download_asset = MagicMock(side_effect=fake_download)

        md5_a = hashlib.md5(content_active).hexdigest()
        md5_b = hashlib.md5(content_already_amz_trash).hexdigest()

        def fake_find_nodes(md5_hash, limit=5):
            if md5_hash == md5_a:
                return [{"id": "amz-node-available", "status": "AVAILABLE"}]
            if md5_hash == md5_b:
                return [{"id": "amz-node-trashed", "status": "TRASH"}]
            return []

        engine.amazon.find_nodes_by_md5 = MagicMock(side_effect=fake_find_nodes)
        engine.amazon.trash_nodes = MagicMock(return_value=True)

        # First cycle: trashes active Amazon node, records both as DELETED_IN_IMMICH, counts both (2)
        deleted = engine._sync_deletions()
        self.assertEqual(deleted, 2)
        engine.amazon.trash_nodes.assert_called_once_with(["amz-node-available"])

        rec1 = engine.db.get_asset_by_immich_id("immich-trash-1")
        rec2 = engine.db.get_asset_by_immich_id("immich-trash-2")
        self.assertEqual(rec1["status"], "DELETED_IN_IMMICH")
        self.assertEqual(rec1["amazon_id"], "amz-node-available")
        self.assertEqual(rec2["status"], "DELETED_IN_IMMICH")
        self.assertEqual(rec2["amazon_id"], "amz-node-trashed")

        # Second cycle: already recorded in DB -> 0 downloads, 0 Amazon calls, 0 deletions
        engine.immich.download_asset.reset_mock()
        engine.amazon.find_nodes_by_md5.reset_mock()
        engine.amazon.trash_nodes.reset_mock()
        deleted_again = engine._sync_deletions()
        self.assertEqual(deleted_again, 0)
        engine.immich.download_asset.assert_not_called()
        engine.amazon.find_nodes_by_md5.assert_not_called()
        engine.amazon.trash_nodes.assert_not_called()

    def test_daily_scan_stats_and_apprise_telegram_notification(self):
        import datetime
        from src.config import NotificationConfig
        from src.notifications import maybe_send_daily_summary

        # Accumulate multiple scans for the same day
        self.db.record_scan_stats(
            {"immich_direct_synced": 3, "amazon_uploaded": 2, "icloud_uploaded": 1, "deletions_synced": 1, "albums_synced": 2, "errors": 0},
            scan_date="2026-11-17",
        )
        self.db.record_scan_stats(
            {"immich_direct_synced": 4, "amazon_uploaded": 4, "icloud_uploaded": 3, "deletions_synced": 0, "albums_synced": 1, "errors": 1},
            scan_date="2026-11-17",
        )

        daily = self.db.get_daily_scan_stats("2026-11-17")
        self.assertEqual(daily["scans_count"], 2)
        self.assertEqual(daily["immich_direct_synced"], 7)
        self.assertEqual(daily["amazon_uploaded"], 6)
        self.assertEqual(daily["icloud_uploaded"], 4)
        self.assertEqual(daily["deletions_synced"], 1)
        self.assertEqual(daily["albums_synced"], 3)
        self.assertEqual(daily["errors"], 1)

        cfg = AppConfig(
            immich=ImmichConfig(url="http://localhost:2283", api_key="dummy"),
            icloud=ICloudConfig(apple_id="user@icloud.com"),
            amazon=AmazonConfig(cookies={"session-id": "test"}),
            notifications=NotificationConfig(
                enabled=True,
                apprise_urls=["tgram://123456:ABC-DEF/987654321"],
                daily_summary_hour=21,
            ),
        )

        with patch("apprise.Apprise") as mock_apprise_cls:
            mock_ap = MagicMock()
            mock_ap.add.return_value = True
            mock_ap.__len__.return_value = 1
            mock_ap.notify.return_value = True
            mock_apprise_cls.return_value = mock_ap

            # Before 21:00 -> should not send yet
            early = datetime.datetime(2026, 11, 17, 20, 45)
            self.assertFalse(maybe_send_daily_summary(cfg, self.db, now=early))
            mock_ap.notify.assert_not_called()

            # At 21:05 -> sends once with accumulated daily stats
            due = datetime.datetime(2026, 11, 17, 21, 5)
            self.assertTrue(maybe_send_daily_summary(cfg, self.db, now=due))
            mock_ap.add.assert_called_once_with("tgram://123456:ABC-DEF/987654321")
            mock_ap.notify.assert_called_once()
            kwargs = mock_ap.notify.call_args.kwargs
            self.assertIn("2026-11-17", kwargs["title"])
            self.assertIn("Scans Completed Today: 2", kwargs["body"])
            self.assertIn("Uploaded to Amazon Photos: 6", kwargs["body"])
            self.assertIn("Uploaded to iCloud Photos: 4", kwargs["body"])

            # Later same day -> already notified, does not re-send
            later = datetime.datetime(2026, 11, 17, 22, 10)
            self.assertFalse(maybe_send_daily_summary(cfg, self.db, now=later))
            self.assertEqual(mock_ap.notify.call_count, 1)

    def test_telegram_env_variables_configure_apprise(self):
        cfg_file = Path(self.temp_dir.name) / "config.json"
        env_file = Path(self.temp_dir.name) / ".env"
        env_file.write_text(
            "\n".join([
                "IMMICH_URL=http://192.168.1.50:2283",
                "IMMICH_API_KEY=env-api-key",
                "TELEGRAM_BOT_TOKEN=111222:TOKEN",
                "TELEGRAM_CHAT_ID=555666777",
                "DAILY_SUMMARY_HOUR=20",
            ]),
            encoding="utf-8",
        )
        loaded = load_config(cfg_file)
        self.assertTrue(loaded.notifications.enabled)
        self.assertEqual(loaded.notifications.apprise_urls, ["tgram://111222:TOKEN/555666777"])
        self.assertEqual(loaded.notifications.daily_summary_hour, 20)

    def test_amazon_album_add_skips_existing_and_retries_503(self):
        from src.clients.amazon import AmazonPhotosClient

        client = AmazonPhotosClient(AmazonConfig(cookies={"session-id": "test-sess"}, tld="ca"))
        fake_ap = MagicMock()
        fake_ap.drive_url = "https://www.amazon.ca/drive/v1"
        fake_ap.base_params = {"asset": "ALL"}

        # Existing album already has node-1; node-2 is new, CONFLICT_EXISTING should be filtered
        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {"count": 1, "data": [{"id": "node-1"}]}
        fake_ap.client.get.return_value = get_resp

        # First PATCH returns 503 HTML, second PATCH succeeds with 202, cover PATCH returns 200
        resp_503 = MagicMock(status_code=503, text="<!doctype html><html>503</html>")
        resp_202 = MagicMock(status_code=202, text="")
        resp_cover = MagicMock(status_code=200, text="")
        fake_ap.client.patch.side_effect = [resp_503, resp_202, resp_cover]
        client._ap = fake_ap

        with patch("src.clients.amazon.time.sleep"):
            ok = client.add_to_album("alb-1", ["node-1", "node-2", "CONFLICT_EXISTING"], cover_node_id="node-1")

        self.assertTrue(ok)
        children_patches = [
            c for c in fake_ap.client.patch.call_args_list
            if "/children" in c.args[0]
        ]
        self.assertEqual(len(children_patches), 2)
        self.assertEqual(children_patches[0].kwargs["json"]["value"], ["node-2"])

        # When all nodes are already in the album, 0 PATCH calls happen
        fake_ap.client.patch.reset_mock()
        get_resp.json.return_value = {"count": 2, "data": [{"id": "node-1"}, {"id": "node-2"}]}
        ok2 = client.add_to_album("alb-1", ["node-1", "node-2"])
        self.assertTrue(ok2)
        fake_ap.client.patch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
