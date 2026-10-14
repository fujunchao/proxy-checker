"""订阅与版本化保存的真实本地 HTTP 回归，沿用临时固定身份服务。"""
import json
from pathlib import Path
import unittest
import urllib.request
import urllib.error
import test_workspace as workspace


class MaintenanceHTTP(workspace.WorkspaceTest):
    def get_export(self, ext):
        with urllib.request.urlopen(self.base + f"/api/repo/{workspace.FIXED}.{ext}") as response:
            return response.read().decode(), int(response.headers["X-Repo-Revision"])

    def test_subscription_revision_and_current_manual_replace(self):
        raw, revision = self.get_export("json")
        txt, text_revision = self.get_export("txt")
        self.assertEqual(revision, text_revision)
        self.assertEqual(json.loads(raw)[0]["proxy"], txt)
        saved = self.request("/api/repo/save", {"token": "other_browser", "mode": "replace", "repo": [],
                                               "base_count": 1, "base_revision": revision})
        self.assertTrue(saved["ok"])
        self.assertEqual(json.loads(self.get_export("json")[0]), [])
        self.assertEqual(self.get_export("txt")[0], "")
        stale = self.request("/api/repo/save", {"mode": "replace", "repo": [{"proxy": "http://192.0.2.1:80"}],
                                               "base_revision": revision})
        self.assertTrue(stale["stale_repo"])

    def test_corrupt_state_does_not_get_overwritten_or_kill_other_workspaces(self):
        path = Path(self.server.auto_json_path(workspace.FIXED))
        good = path.read_bytes()
        try:
            path.write_text("{damaged", encoding="utf-8")
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request("/api/auto/save", {"config": {"enabled": True}})
            self.assertEqual(error.exception.code, 503)
            self.assertEqual(path.read_text(), "{damaged")
            self.server.scheduler_tick()
            self.assertTrue(self.server.load_auto_record("user_other")["config"]["enabled"])
        finally:
            path.write_bytes(good)

    def test_readonly_rollback_export(self):
        from export_legacy_repo import export_snapshot
        store = self.server.repo_store(workspace.FIXED)
        before = Path(store.path).read_bytes()
        output = self.root / "rollback_export"
        manifest = export_snapshot(store.path, output)
        self.assertEqual(manifest["repo_revision"], store.summary()["repo_revision"])
        self.assertEqual(json.loads((output / (workspace.FIXED + ".json")).read_text()),
                         json.loads(self.get_export("json")[0]))
        self.assertEqual(Path(store.path).read_bytes(), before)
        with self.assertRaises(ValueError):
            export_snapshot(store.path, output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
