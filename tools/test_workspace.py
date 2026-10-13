"""固定身份的 HTTP 回归测试：python tools/test_workspace.py。

只在临时目录读写合成数据，不访问真实部署。
完整冒烟需 RUN_FULL_SMOKE=1，仅在 GitHub Actions 中执行真实的关闭端口检测。
"""
import importlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
FIXED = "user_existing_workspace"


class WorkspaceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="proxy-checker-workspace-")
        cls.root = Path(cls.tmp.name).resolve()
        assert cls.root.is_relative_to(Path(tempfile.gettempdir()).resolve())
        for name in ("server.py", "proxy_check.py", "fetch_proxies.py", "config.json",
                     "app.js", "index.html", "login.html"):
            shutil.copy2(ROOT / name, cls.root / name)
        cls.env = patch.dict(os.environ, {"WORKSPACE_TOKEN": FIXED, "AUTH_PASSWORD": "workspace-test",
                                        "LOG_FILE": str(cls.root / "server.log")})
        cls.env.start()
        sys.path.insert(0, str(cls.root))
        cls.server = importlib.import_module("server")
        cls.httpd = cls.server.ThreadingHTTPServer(("127.0.0.1", 0), cls.server.Handler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.httpd.server_port}"
        cls.auth = cls.server.make_auth_token()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=3)
        logging.shutdown()
        sys.path.remove(str(cls.root))
        cls.env.stop()
        cls.tmp.cleanup()

    def setUp(self):
        self.server.write_repo_data(FIXED, [{"proxy": "http://127.0.0.1:9", "grade": "A"}])
        self.server.write_repo_data("user_other", [{"proxy": "http://127.0.0.1:10"}])
        self.server.save_auto_record(FIXED, {"config": {"enabled": True}, "state": {}})
        self.server.save_auto_record("user_other", {"config": {"enabled": True}, "state": {}})
        self.server.write_run_logs(FIXED, [{"id": "existing-log", "type": "auto", "started_at": 100}])

    def request(self, path, payload=None, authenticated=True):
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = f"Bearer {self.auth}"
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(self.base + path, data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read().decode()
            return json.loads(raw) if "application/json" in response.headers.get("Content-Type", "") else raw

    def test_capabilities_bind_only_after_login(self):
        public = self.request("/api/capabilities", {}, authenticated=False)
        self.assertFalse(public.get("workspace_token"))
        private = self.request("/api/capabilities", {})
        self.assertEqual(private.get("workspace_token"), FIXED)

    def test_fresh_browsers_share_existing_auto_and_logs(self):
        for token in ("user_browser_a", "user_browser_b", ""):
            with self.subTest(token=token):
                status = self.request("/api/auto/status", {"token": token})
                self.assertTrue(status["config"]["enabled"])
                logs = self.request("/api/logs/list", {"token": token})
                self.assertEqual(logs["count"], 1)
                self.assertEqual(logs["logs"][0]["id"], "existing-log")

    def test_writes_use_fixed_identity(self):
        result = self.request("/api/repo/save", {
            "token": "user_browser_a", "repo": [{"proxy": "http://127.0.0.1:11"}], "mode": "merge"})
        self.assertEqual(result["url"], f"/api/repo/{FIXED}.json")
        self.assertEqual(len(self.server.read_repo_data(FIXED)), 2)
        self.assertFalse(Path(self.server.repo_json_path("user_browser_a")).exists())
        self.request("/api/checked/save", {"token": "user_browser_a", "proxies": ["http://127.0.0.1:9"]})
        result = self.request("/api/checked/filter", {"token": "user_browser_b", "proxies": ["http://127.0.0.1:9"]})
        self.assertEqual(result["skipped"], 1)
        self.request("/api/auto/save", {"token": "user_browser_b", "config": {"enabled": False}})
        self.assertFalse(self.server.load_auto_record(FIXED)["config"]["enabled"])

    def test_scheduler_ignores_other_identities_without_deleting_them(self):
        self.assertEqual(self.server.list_auto_tokens(), [FIXED])
        self.assertTrue(Path(self.server.auto_json_path("user_other")).exists())
        self.assertTrue(self.server.load_auto_record("user_other")["config"]["enabled"])

    def test_public_subscription_paths_keep_original_meaning(self):
        self.assertEqual(len(self.request(f"/api/repo/{FIXED}.json", authenticated=False)), 1)
        other = self.request("/api/repo/user_other.json", authenticated=False)
        self.assertEqual(other[0]["proxy"], "http://127.0.0.1:10")
        self.assertEqual(self.request("/api/repo/user_unknown.json", authenticated=False), [])
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.request("/api/repo/user_unknown.txt", authenticated=False)
        self.assertEqual(error.exception.code, 404)

    def test_unauthenticated_operations_still_rejected(self):
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.request("/api/auto/status", {"token": "user_browser_a"}, authenticated=False)
        self.assertEqual(error.exception.code, 401)

    def test_unconfigured_deployment_keeps_isolation(self):
        with patch.object(self.server, "WORKSPACE_TOKEN", "", create=True):
            self.assertFalse(self.request("/api/capabilities", {}).get("workspace_token"))
            status = self.request("/api/auto/status", {"token": "user_browser_b"})
            self.assertFalse(status["config"]["enabled"])
            self.assertEqual(self.request("/api/logs/list", {"token": "user_browser_b"})["count"], 0)
            self.assertIn("user_other", self.server.list_auto_tokens())

    def test_restart_reuses_existing_data_and_configuration(self):
        code = (
            "import server; "
            f"assert server.WORKSPACE_TOKEN == {FIXED!r}; "
            "assert server.load_auto_record(server.WORKSPACE_TOKEN)['config']['enabled']; "
            "assert len(server.read_run_logs(server.WORKSPACE_TOKEN)) == 1; "
            "assert len(server.read_repo_data(server.WORKSPACE_TOKEN)) == 1"
        )
        result = subprocess.run([sys.executable, "-c", code], cwd=self.root,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_fixed_identity_does_not_silently_select_default(self):
        env = dict(os.environ, WORKSPACE_TOKEN="../invalid")
        result = subprocess.run([sys.executable, "-c", "import server"], cwd=self.root,
                                env=env, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"WORKSPACE_TOKEN", result.stderr)

    @unittest.skipUnless(os.environ.get("RUN_FULL_SMOKE") == "1", "完整冒烟由 GitHub Actions 执行")
    def test_existing_smoke_checks_in_both_modes(self):
        # 检测目标只使用 smoke.py 自带的本机关闭端口，不访问外部代理。
        for token in (FIXED, ""):
            with self.subTest(fixed=bool(token)), patch.object(self.server, "WORKSPACE_TOKEN", token):
                result = subprocess.run(
                    [sys.executable, str(ROOT / "tools" / "smoke.py"), "--base-url", self.base,
                     "--password", "workspace-test", "--expected-workspace-token", token],
                    capture_output=True, text=True, timeout=180)
                print(result.stdout, flush=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("smoke ok", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
