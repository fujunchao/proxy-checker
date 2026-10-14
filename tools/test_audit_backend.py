"""审核 S3/S4/T1/T2/T3：真实 HTTP／协调器路径上的确定性交错。"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import unittest
import urllib.error
from urllib.parse import urlencode
from unittest.mock import patch
import test_workspace as workspace


def item(n):
    return {"proxy": f"http://192.0.2.{n}:80", "target_profile": "generic", "grade": "A", "usable": True}


class AuditBackend(workspace.WorkspaceTest):
    def setUp(self):
        super().setUp()
        self.server.auto_runtime.clear()
        self.server.sessions.clear()

    def tearDown(self):
        self.server.auto_runtime.clear()
        self.server.sessions.clear()

    def test_S3_auto_last_slot_is_atomic(self):
        s = self.server
        s.auto_runtime.update({f"seed{i}": {} for i in range(15)})
        first_token = "slot_first"
        second_token = next(f"slot_second{i}" for i in range(100) if
                            s.auto_workspace_lock(f"slot_second{i}") is not s.auto_workspace_lock(first_token))
        entered, release = threading.Event(), threading.Event()
        write = s.atomic_write_json
        def blocked_write(path, value):
            if Path(path).parent == Path(s.RUN_LOG_DIR) and not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError("测试未释放持久化屏障")
            return write(path, value)
        with patch.object(s, "atomic_write_json", side_effect=blocked_write), \
                patch.object(s, "execute_auto_run", lambda *args: None), ThreadPoolExecutor(2) as pool:
            one = pool.submit(s.start_auto_run, first_token, "manual")
            self.assertTrue(entered.wait(3))
            try:
                two = pool.submit(s.start_auto_run, second_token, "manual").result(3)
            finally:
                release.set()
            first = one.result(3)
        self.assertEqual(sum(bool(r[0]) for r in (first, two)), 1, "最后一个维护名额只能分配一次")
        self.assertEqual(len(s.auto_runtime), 16)

    def test_S3_manual_last_slot_is_atomic(self):
        s = self.server
        s.sessions.update({f"seed{i}": {"finished": False} for i in range(31)})
        entered, release = threading.Event(), threading.Event()
        write = s.atomic_write_json
        def blocked_write(path, value):
            if Path(path).parent == Path(s.RUN_LOG_DIR) and not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError("测试未释放持久化屏障")
            return write(path, value)
        def request():
            try:
                return self.request("/api/start", {"proxies": [item(1)["proxy"]]})
            except urllib.error.HTTPError as exc:
                return {"http_status": exc.code}
        with patch.object(s, "atomic_write_json", side_effect=blocked_write), \
                patch.object(s, "run_check", lambda *args: None), ThreadPoolExecutor(2) as pool:
            one = pool.submit(request)
            self.assertTrue(entered.wait(3))
            try:
                two = pool.submit(request).result(3)
            finally:
                release.set()
            first = one.result(3)
        self.assertEqual(sum("session_id" in r for r in (first, two)), 1, "最后一个手动名额只能分配一次")
        self.assertEqual(len(s.sessions), 32)

    def test_S3_failed_start_releases_slot(self):
        s = self.server
        with patch.object(s, "save_auto_record", side_effect=OSError("合成写盘失败")):
            with self.assertRaises(OSError):
                s.start_auto_run("failed_slot", "manual")
        self.assertNotIn("failed_slot", s.auto_runtime)
        with patch.object(s, "execute_auto_run", lambda *args: None):
            self.assertTrue(s.start_auto_run("failed_slot", "manual")[0])

    def test_S4_case_insensitive_boolean_subscription_filters(self):
        s = self.server
        s.write_repo_data(workspace.FIXED, [
            {**item(1), "pass_streak": 3},
            {**item(2), "proxy": "socks4://192.0.2.2:80", "usable": False, "ip_shared": True}])
        for ext in ("json", "txt"):
            for field in ("usable", "stable", "exclude_socks4"):
                for value in ("1", "true", "TRUE", "Yes", "on", "ON", " True "):
                    with self.subTest(ext=ext, field=field, value=value):
                        body = self.request(f"/api/repo/{workspace.FIXED}.{ext}?" + urlencode({field: value}), authenticated=False)
                        rows = body if ext == "json" else body.splitlines()
                        self.assertEqual(len(rows), 1)
        for value in ("", "0", "false", "FALSE", "No", "off"):
            self.assertFalse(s.truthy_param(value))
        self.assertNotEqual(s.proxy_key("http://User:Pass@example.com:80"), s.proxy_key("http://User:pass@example.com:80"))

    def runtime(self, token, run):
        runtime = {"run_id": run, "stop": threading.Event(), "results": [], "counts": [0, 0, 0], "done": 0}
        self.server.auto_runtime[token] = runtime
        return runtime

    def config(self, **values):
        return {**self.server.default_auto_config(), "maintenance_mode": "continuous", "publish_count": 1, **values}

    def test_T1_clear_between_selection_and_begin_cannot_resurrect(self):
        s, token = self.server, "selection_clear"
        store = s.repo_store(token)
        store.save([item(1)], "replace", 0)
        maintenance = store.maintenance
        cleared = []
        def after_save(update=None):
            result = maintenance(update)
            if update and update.get("inflight_inputs") and not cleared:
                cleared.append(True)
                store.save([], "replace", store.summary()["repo_revision"])
            return result
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                for proxy in proxies:
                    await on_result({**item(1), "proxy": proxy, "original": proxy, "terminal_complete": True})
        with patch.object(store, "maintenance", side_effect=after_save):
            s.execute_maintenance_run(token, self.config(), self.runtime(token, "clear_window"), engine=Engine())
        self.assertTrue(cleared)
        self.assertEqual(store.snapshot()["items"], [], "旧候选不能在 begin 时领到人工清空后的新代次")

    def test_T2_clock_advances_past_budget_without_starving_later_stock(self):
        s, token = self.server, "slow_stock"
        store = s.repo_store(token)
        store.save([item(1), item(2), item(3)], "replace", 0)
        now, seen = [2000.0], []
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                seen.extend(proxies)
                now[0] += 121
                await asyncio.Event().wait()
        for i in range(2):
            # 此处单独验证存量轮转；发现时间保底由 test_discovery_fairness 独立覆盖。
            s.execute_maintenance_run(token, self.config(refresh_batch=1, refresh_budget_seconds=600), self.runtime(token, f"slow_{i}"),
                                      engine=Engine(), clock=lambda: now[0], timer_clock=lambda: now[0])
        self.assertEqual(seen, [item(1)["proxy"], item(2)["proxy"]], "跨过 120 秒预算后不能继续选同一最老前缀")
        self.assertTrue(all("last_checked_at" not in row for row in store.snapshot()["items"]))

    def test_T3_slow_tail_cannot_pin_source_cursor(self):
        s, token = self.server, "slow_source"
        now, fetched = [2000.0], []
        def source(index, stop):
            fetched.append(index)
            return [item(1 if index == 0 else 2)["proxy"]], "synthetic"
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                if item(1)["proxy"] in proxies:
                    now[0] += 2
                    await asyncio.Event().wait()
                for proxy in proxies:
                    await on_result({**item(2), "proxy": proxy, "original": proxy, "terminal_complete": True,
                                     "started_at": int(now[0] * 1000),
                                     "completed_at": int(now[0] * 1000)})
        for i in range(5):
            s.execute_maintenance_run(token, self.config(batch_seconds=1, discover_batch=1),
                                      self.runtime(token, f"source_{i}"), engine=Engine(), source=source,
                                      clock=lambda: now[0], timer_clock=lambda: now[0])
        self.assertIn(1, fetched, "未完成尾项不能永久阻止访问下一来源")
        rows = s.repo_store(token).snapshot()["items"]
        self.assertEqual([row["proxy"] for row in rows], [item(2)["proxy"]])


if __name__ == "__main__":
    unittest.main(verbosity=2)
