import unittest
import tempfile
import json
from pathlib import Path
from fastapi.testclient import TestClient

from src.ui.app import create_app
from src.database import SyncDatabase

class TestUIDashboard(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cfg_path = Path(self.temp_dir.name) / "config.json"
        self.db_path = Path(self.temp_dir.name) / "test_state.db"

        sample_cfg = {
            "immich": {"url": "http://localhost:2283", "api_key": "test_immich_key", "verify_ssl": False},
            "icloud": {"apple_id": "test@icloud.com", "session_dir": str(self.temp_dir.name)},
            "amazon": {"cookies": {"session-id": "123-session"}, "tld": "com"},
            "sync": {"max_delete_limit": 30, "poll_interval_seconds": 120, "download_temp_dir": str(self.temp_dir.name), "dry_run": False}
        }
        self.cfg_path.write_text(json.dumps(sample_cfg), encoding="utf-8")

        # Prepopulate database
        db = SyncDatabase(self.db_path)
        db.upsert_asset(
            sha1="sample_sha1_val",
            md5="sample_md5_val",
            filename="ui_test_image.jpg",
            file_size=2048,
            immich_id="immich_id_101",
            amazon_id="amz_id_202",
            icloud_id="icloud_id_303"
        )
        db.upsert_album(
            immich_id="immich_alb_101",
            name="UI Test Album",
            amazon_id="amz_alb_202"
        )

        app = create_app(config_path=str(self.cfg_path), db_path=str(self.db_path))
        self.client = TestClient(app)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_serve_html_dashboard(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Immich Multi-Sync", resp.text)
        self.assertIn("Live Activity Stream", resp.text)

    def test_status_endpoint(self):
        resp = self.client.get("/api/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("db_stats", data)
        self.assertEqual(data["db_stats"]["active_assets"], 1)
        self.assertEqual(data["db_stats"]["total_albums"], 1)
        self.assertIn("services", data)

    def test_assets_endpoint(self):
        resp = self.client.get("/api/assets")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["items"][0]["filename"], "ui_test_image.jpg")

    def test_albums_endpoint(self):
        resp = self.client.get("/api/albums")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["items"][0]["name"], "UI Test Album")

    def test_config_get_and_post(self):
        # 1. Get config
        resp = self.client.get("/api/config")
        self.assertEqual(resp.status_code, 200)
        cfg = resp.json()
        self.assertEqual(cfg["sync"]["max_delete_limit"], 30)

        # 2. Update config
        cfg["sync"]["max_delete_limit"] = 99
        save_resp = self.client.post("/api/config", json=cfg)
        self.assertEqual(save_resp.status_code, 200)

        # 3. Verify updated config
        resp_after = self.client.get("/api/config")
        self.assertEqual(resp_after.json()["sync"]["max_delete_limit"], 99)

    def test_test_connection_endpoint(self):
        resp = self.client.post("/api/test-connection/immich", json={"url": "http://127.0.0.1:59999", "api_key": "dummy"})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("success", data)
        self.assertFalse(data["success"])
        self.assertIn("message", data)

    def test_toggle_host_endpoint(self):
        # Disable amazon
        resp = self.client.post("/api/host/toggle", json={"service": "amazon", "enabled": False})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["success"])
        self.assertFalse(data["enabled"])

        # Check /api/status reflects disabled host
        status_resp = self.client.get("/api/status")
        status_data = status_resp.json()
        self.assertFalse(status_data["services"]["amazon"]["enabled"])

        # Re-enable amazon
        resp2 = self.client.post("/api/host/toggle", json={"service": "amazon", "enabled": True})
        self.assertEqual(resp2.status_code, 200)
        self.assertTrue(resp2.json()["enabled"])

        status_resp2 = self.client.get("/api/status")
        self.assertTrue(status_resp2.json()["services"]["amazon"]["enabled"])

    def test_progress_endpoint(self):
        resp = self.client.get("/api/sync/progress")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("percent", data)
        self.assertIn("phase", data)
        self.assertIn("action", data)

    def test_one_way_sync_config_flags(self):
        resp = self.client.get("/api/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("one_way_immich_master", data)
        self.assertTrue(data["one_way_immich_master"])

    def test_amazon_cookies_bookmarklet_endpoint(self):
        payload = {
            "tld": "ca",
            "cookies": {
                "session-id": "fresh-session-123",
                "ubid-acbca": "fresh-ubid-456",
                "at-acbca": "Atza|fresh-token-789"
            }
        }
        resp = self.client.post("/api/amazon/cookies", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["success"])

        # Check config reflects the fresh cookies
        cfg_resp = self.client.get("/api/config")
        saved_cfg = cfg_resp.json()
        self.assertEqual(saved_cfg["amazon"]["cookies"]["session-id"], "fresh-session-123")
        self.assertEqual(saved_cfg["amazon"]["tld"], "ca")
        self.assertTrue(saved_cfg["amazon"]["enabled"])

    def test_amazon_cookie_import_fallback(self):
        payload = {
            "tld": "ca",
            "cookies": {
                "session-id": "redirect-session-999",
                "ubid-acbca": "redirect-ubid-888",
                "at-acbca": "Atza|redirect-token-777"
            }
        }
        import urllib.parse
        encoded_data = urllib.parse.quote(json.dumps(payload))
        resp = self.client.get(f"/api/amazon/cookie-import?data={encoded_data}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Amazon Cookies Synced!", resp.text)

        cfg_resp = self.client.get("/api/config")
        saved_cfg = cfg_resp.json()
        self.assertEqual(saved_cfg["amazon"]["cookies"]["session-id"], "redirect-session-999")

    def test_amazon_login_status_initial(self):
        resp = self.client.get("/api/amazon/login/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["state"], "idle")

    def test_amazon_login_cancel(self):
        resp = self.client.post("/api/amazon/login/cancel")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "idle")

    def test_amazon_browser_login_class(self):
        from src.clients.amazon_login import AmazonBrowserLogin

        login = AmazonBrowserLogin(tld="ca", user_data_dir=str(self.temp_dir.name))
        status = login.get_status()
        self.assertEqual(status["state"], "idle")
        self.assertEqual(status["tld"], "ca")

        login.cancel()
        self.assertTrue(login.stop_event.is_set())

    def test_sync_pause_resume_endpoints(self):
        from src.ui.app import sync_state

        # When not syncing
        sync_state["is_syncing"] = False
        resp = self.client.post("/api/sync/pause")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "not_syncing")

        resp = self.client.post("/api/sync/resume")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "not_syncing")

        resp = self.client.post("/api/sync/stop")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "not_syncing")

        # When syncing is active
        sync_state["is_syncing"] = True
        try:
            # 1. Pause
            resp = self.client.post("/api/sync/pause")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "paused")
            self.assertTrue(sync_state["is_paused"])
            self.assertTrue(sync_state["pause_sync_event"].is_set())

            # Verify /api/status reports is_paused=True
            status_resp = self.client.get("/api/status")
            self.assertTrue(status_resp.json()["is_paused"])
            self.assertTrue(status_resp.json()["is_syncing"])

            # 2. Resume
            resp = self.client.post("/api/sync/resume")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "resumed")
            self.assertFalse(sync_state["is_paused"])
            self.assertFalse(sync_state["pause_sync_event"].is_set())

            status_resp = self.client.get("/api/status")
            self.assertFalse(status_resp.json()["is_paused"])

            # 3. Stop
            resp = self.client.post("/api/sync/stop")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "stopping")
            self.assertTrue(sync_state["stop_sync_event"].is_set())
            self.assertFalse(sync_state["pause_sync_event"].is_set())
            self.assertFalse(sync_state["is_paused"])
        finally:
            sync_state["is_syncing"] = False
            sync_state["is_paused"] = False
            sync_state["stop_sync_event"].clear()
            sync_state["pause_sync_event"].clear()

    def test_config_masks_secrets_and_preserves_on_save(self):
        cfg = json.loads(self.cfg_path.read_text(encoding="utf-8"))
        cfg["immich"]["api_key"] = "real-immich-key"
        cfg["icloud"]["password"] = "real-icloud-pass"
        cfg["amazon"]["cookies"]["at-acbca"] = "Atza|real-secret"
        self.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

        resp = self.client.get("/api/config")
        self.assertEqual(resp.status_code, 200)
        masked = resp.json()
        self.assertEqual(masked["immich"]["api_key"], "********")
        self.assertEqual(masked["icloud"]["password"], "********")
        self.assertEqual(masked["amazon"]["cookies"]["at-acbca"], "********")
        self.assertEqual(masked["amazon"]["cookies"]["session-id"], "123-session")

        # Modify a non-secret field and save the masked payload back
        masked["sync"]["max_delete_limit"] = 77
        save_resp = self.client.post("/api/config", json=masked)
        self.assertEqual(save_resp.status_code, 200)

        saved_disk = json.loads(self.cfg_path.read_text(encoding="utf-8"))
        self.assertEqual(saved_disk["immich"]["api_key"], "real-immich-key")
        self.assertEqual(saved_disk["icloud"]["password"], "real-icloud-pass")
        self.assertEqual(saved_disk["amazon"]["cookies"]["at-acbca"], "Atza|real-secret")
        self.assertEqual(saved_disk["sync"]["max_delete_limit"], 77)

    def test_dashboard_basic_auth(self):
        auth_app = create_app(
            config_path=str(self.cfg_path),
            db_path=str(self.db_path),
            dashboard_password="secret123",
            dashboard_user="admin",
        )
        auth_client = TestClient(auth_app)

        self.assertEqual(auth_client.get("/").status_code, 401)
        self.assertEqual(auth_client.get("/", auth=("admin", "wrong")).status_code, 401)
        self.assertEqual(auth_client.get("/", auth=("admin", "secret123")).status_code, 200)


if __name__ == "__main__":
    unittest.main()
