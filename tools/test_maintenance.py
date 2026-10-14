"""持续维护的真实提交／协调接口回归；仅在临时合成目录运行。"""
import os
import shutil
import sys
import tempfile
import unittest
import asyncio
import copy
import threading
import json
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMP = tempfile.TemporaryDirectory(prefix="prx-maintenance-")
COPY = os.path.join(TEMP.name, "app")
shutil.copytree(ROOT, COPY, ignore=shutil.ignore_patterns(
    ".git", "node_modules", "work", "__pycache__", ".env", "*.log",
    "repo_data", "checked_data", "auto_data", "run_logs", "config.local.json"))
sys.path.insert(0, COPY)
import server


def item(n, **fields):
    return {"proxy": f"http://192.0.2.{n}:8080", "grade": "A",
            "target_profile": "generic", "usable": True, **fields}


class RepositoryRegression(unittest.TestCase):
    def store(self, name=None):
        return server.repo_store(name or self.id().rsplit(".", 1)[-1])

    def context(self, store, inputs=None, run="run", profile="generic"):
        return store.begin(run, profile, 1, inputs or [item(1)["proxy"]])

    def observation(self, context, n=1, oid="one", **fields):
        result = item(n, original=item(n)["proxy"], terminal_complete=True, **fields)
        return {"observation_id": oid, "complete": True, "started_at": 1000,
                "completed_at": 2000, "result": result}

    def test_same_count_manual_replacement_survives_late_snapshot(self):
        token = "same_count"
        old = server.write_repo_data(token, [item(1), item(2)])
        kw = {"base_revision": server.repo_store(token).summary()["repo_revision"]} if hasattr(server, "repo_store") else {}
        saved, _ = server.save_repo_payload(token, [item(3), item(4)], "replace", 2, **kw)
        self.assertIsNotNone(saved)
        server.merge_repo_results(token, old, [item(1)], [item(1)["proxy"]], "stable_only")
        self.assertEqual([r["proxy"] for r in server.read_repo_data(token)],
                         [item(3)["proxy"], item(4)["proxy"]],
                         "迟到结果不得恢复被人工替换的旧仓库（数量相同也必须识别）")

    def test_current_revision_not_count_controls_manual_save(self):
        store = self.store()
        store.save([item(1)], "replace", 0)
        rev = store.summary()["repo_revision"]
        store.save([item(2)], "replace", rev)
        saved, error = store.save([item(3)], "replace", rev, 1)
        self.assertIsNone(saved)
        self.assertTrue(error["stale_repo"])
        self.assertEqual(store.snapshot()["items"][0]["proxy"], item(2)["proxy"])

    def test_manual_clear_and_modify_fence_inflight(self):
        for replacement in ([], [item(1, country="JP")], [item(2)]):
            store = self.store(str(len(replacement)) + str(replacement))
            # 使用独立合成身份，不能让 sanitize 把非法名称合并。
            store = self.store("edit_" + str(abs(hash(str(replacement)))))
            store.save([item(1)], "replace", 0)
            context = self.context(store)
            store.save(replacement, "replace", store.summary()["repo_revision"])
            result = store.publish(context, [self.observation(context)], "stable_only")
            self.assertEqual(result["conflict_skipped"], 1)
            self.assertEqual([r["proxy"] for r in store.snapshot()["items"]], [r["proxy"] for r in replacement])

    def test_duplicate_and_reopen_are_idempotent(self):
        store = self.store()
        context = self.context(store)
        observation = self.observation(context)
        store.publish(context, [observation], "stable_only")
        server._repository.cache_clear()
        reopened = self.store()
        result = reopened.publish(context, [observation], "stable_only")
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(reopened.snapshot()["items"][0]["pass_streak"], 1)
        self.assertEqual(reopened.snapshot()["items"][0]["last_ok"], 2000)
        with patch("repository.BOOT_ID", "new_process"):
            self.assertEqual(reopened.publish(context, [observation], "stable_only")["conflict_skipped"], 1)

    def test_newer_observation_wins_and_profiles_do_not_cross(self):
        store = self.store()
        older = self.context(store, run="older")
        newer = self.context(store, run="newer")
        store.publish(newer, [self.observation(newer)], "stable_only")
        self.assertEqual(store.publish(older, [self.observation(older)], "stable_only")["conflict_skipped"], 1)
        zai = self.context(store, run="zai", profile="zai")
        result = store.publish(zai, [self.observation(zai, target_profile="zai", grade="F", usable=False)], "stable_only")
        self.assertEqual(result["repo_removed"], 0)
        self.assertEqual(store.snapshot()["items"][0]["target_profile"], "generic")

    def test_complete_failure_policy_and_incomplete_are_distinct(self):
        for policy in ("stable_only", "archive_all"):
            store = self.store("policy_" + policy)
            store.save([item(1)], "replace", 0)
            context = self.context(store)
            incomplete = self.observation(context, grade="F", usable=False)
            incomplete["complete"] = False
            store.publish(context, [incomplete], policy)
            self.assertEqual(len(store.snapshot()["items"]), 1)
            complete = {**incomplete, "complete": True}
            store.publish(context, [complete], policy)
            self.assertEqual(len(store.snapshot()["items"]), int(policy == "archive_all"))

    def test_commit_failures_never_expose_half_snapshot(self):
        for stage in ("json", "txt", "metadata", "replace", "committed"):
            store = self.store("fault_" + stage)
            store.save([item(1)], "replace", 0)
            before = store.snapshot()
            def fail(where):
                if where == stage:
                    raise OSError("synthetic disk failure")
            store.fault = fail
            with self.assertRaises(OSError):
                store.save([item(2)], "replace", before["revision"])
            store.fault = lambda _: None
            server._repository.cache_clear()
            after = self.store("fault_" + stage).snapshot()
            expected = item(2 if stage == "committed" else 1)["proxy"]
            self.assertEqual(after["items"][0]["proxy"], expected)
            self.assertEqual(json.loads(after["json"])[0]["proxy"], expected)
            self.assertEqual(after["txt"], expected)
            self.assertEqual(len([f for f in os.listdir(store.directory) if f.startswith(store.token + ".snapshot")]),
                             1 if stage == "committed" or stage in ("json", "txt", "metadata") else 2)

    def test_corrupt_legacy_and_snapshot_do_not_become_empty(self):
        store = self.store()
        legacy = os.path.join(store.directory, store.token + ".json")
        with open(legacy, "w") as f:
            f.write("{broken")
        with self.assertRaises(server.RepositoryError):
            store.save([], "replace", 0)
        self.assertFalse(os.path.exists(store.path))
        store2 = self.store("broken_snapshot")
        store2.save([item(1)], "replace", 0)
        with open(store2.path, "w") as f:
            f.write("{}")
        with self.assertRaises(server.RepositoryError):
            store2.snapshot()

    def test_old_unmodified_exports_and_empty_are_preserved(self):
        store = self.store()
        raw = '[ {"proxy": "http://192.0.2.8:80"} ]'
        with open(os.path.join(store.directory, store.token + ".json"), "w") as f:
            f.write(raw)
        with open(os.path.join(store.directory, store.token + ".txt"), "w") as f:
            f.write("http://192.0.2.8:80\n")
        context = self.context(store)
        self.assertEqual(store.snapshot()["json"], raw)
        self.assertEqual(store.snapshot()["txt"], "http://192.0.2.8:80\n")
        store.finish(context["run_id"])
        self.assertEqual(store.snapshot()["json"], raw)


class CoordinatorRegression(unittest.TestCase):
    def config(self, **values):
        return {**server.default_auto_config(), "maintenance_mode": "continuous", **values}

    def test_fast_result_published_before_blocked_slow_result(self):
        from maintenance import IncrementalBatch, FairBudget
        store = server.repo_store("fast_slow")
        context = store.begin("fast_slow_run", "generic", 1, [item(1)["proxy"], item(2)["proxy"]])
        now = [0.0]
        observed = threading.Event()
        stop = threading.Event()
        slow_released = [False]
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                await on_result(item(1, original=item(1)["proxy"], terminal_complete=True,
                                     started_at=1000, completed_at=2000))
                await asyncio.to_thread(observed.wait, 5)
                if not observed.is_set():
                    raise AssertionError("慢项阻塞时快项没有发布")
                slow_released[0] = True
        def status(**fields):
            if fields.get("repo_added"):
                self.assertFalse(slow_released[0])
                self.assertEqual(json.loads(store.snapshot()["json"])[0]["last_ok"], 2000)
                observed.set()
        async def tick(_):
            now[0] += 1
            await asyncio.sleep(0)
        batch = IncrementalBatch(store, Engine(), FairBudget(2), "fast_slow", self.config(batch_seconds=100000),
                                 context, stop, server.make_observation, on_status=status,
                                 clock=lambda: now[0], wait=tick)
        summary, _ = asyncio.run(batch.run([item(1)["proxy"], item(2)["proxy"]]))
        self.assertTrue(observed.is_set())
        self.assertEqual(summary["repo_added"], 1)
        self.assertEqual(summary["incomplete_count"], 1)

    def test_selection_refresh_skip_fairness_and_large_backlog(self):
        from maintenance import choose_batch
        snapshot = {"items": [item(i, last_checked_at=1000) for i in range(1, 251)] + [item(252, target_profile="zai")]}
        state = {"candidates": [f"192.0.2.1:{i}" for i in range(50000)], "cursor": 0}
        types = []
        cfg = self.config(refresh_batch=50)
        for _ in range(5):
            kind, selected, info = choose_batch(snapshot, state, cfg, 2000)
            types.append(kind)
            self.assertLessEqual(len(selected), 50)
            self.assertNotIn(item(252)["proxy"], selected)
        self.assertEqual(types, ["refresh"] * 4 + ["discover"])
        self.assertEqual(info["due_count"], 250)
        self.assertEqual(info["discovery_pending"], 50000)

    def test_real_coordinator_refreshes_checked_stock_without_source(self):
        token = "refresh_checked"
        cfg = self.config(publish_count=1)
        server.write_repo_data(token, [item(1)])
        server.write_checked_list(token, [item(1)["proxy"]])
        runtime = {"run_id": "refresh_checked_run", "stop": threading.Event(), "results": [],
                   "counts": [0, 0, 0], "done": 0, "started_at": 1}
        server.auto_runtime[token] = runtime
        seen = []
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                seen.extend(proxies)
                for p in proxies:
                    await on_result(item(1, original=p, terminal_complete=True, started_at=1000, completed_at=2000))
        def unavailable(*_):
            raise AssertionError("存量刷新不应抓来源")
        result = server.execute_maintenance_run(token, cfg, runtime, engine=Engine(), source=unavailable)
        self.assertEqual(seen, [item(1)["proxy"]])
        self.assertEqual(result["repo_updated"], 1)
        self.assertEqual(server.read_repo_data(token)[0]["last_checked_at"], 2000)
        server.auto_runtime.pop(token)

    def test_discovery_skip_and_restart_cursor(self):
        token = "discover_skip"
        cfg = self.config(discover_batch=2, publish_count=1)
        server.write_checked_list(token, [item(1)["proxy"]])
        store = server.repo_store(token)
        seen = []
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                seen.extend(proxies)
                for p in proxies:
                    await on_result({**item(2), "proxy": p, "original": p, "terminal_complete": True,
                                     "started_at": 1000, "completed_at": 2000})
        def source(*_):
            return [item(i)["proxy"] for i in range(1, 5)], "synthetic"
        for n in range(2):
            runtime = {"run_id": "discover_" + str(n), "stop": threading.Event(), "results": [], "counts": [0, 0, 0]}
            server.auto_runtime[token] = runtime
            # 排期时间在完成后一秒，已完成库存尚未到期。
            server.execute_maintenance_run(token, cfg, runtime, engine=Engine(), source=source, clock=lambda: 3)
            server._repository.cache_clear()
        self.assertEqual(seen, [item(i)["proxy"] for i in (2, 3, 4)])
        self.assertEqual(server.repo_store(token).maintenance()["candidates"], [])
        server.auto_runtime.pop(token)

    def test_old_configuration_is_not_enabled_or_forced(self):
        cfg = server.normalize_auto_config({"enabled": False, "detect_mode": "skip"})
        self.assertEqual(cfg["maintenance_mode"], "legacy")
        self.assertEqual(cfg["detect_mode"], "skip")
        self.assertFalse(cfg["enabled"])


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        TEMP.cleanup()
