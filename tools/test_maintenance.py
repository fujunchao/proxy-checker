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

    def test_post_commit_failure_retry_does_not_repeat_streak(self):
        store = self.store()
        context = self.context(store)
        obs = self.observation(context)
        def crash(stage):
            if stage == "committed":
                raise OSError("synthetic crash after rename")
        store.fault = crash
        with self.assertRaises(OSError):
            store.publish(context, [obs], "stable_only")
        store.fault = lambda _: None
        self.assertEqual(store.publish(context, [obs], "stable_only")["duplicates"], 1)
        self.assertEqual(store.snapshot()["items"][0]["pass_streak"], 1)

    def test_legacy_txt_only_and_mismatched_pair(self):
        store = self.store()
        with open(os.path.join(store.directory, store.token + ".txt"), "w") as f:
            f.write(item(1)["proxy"])
        self.assertEqual(store.snapshot()["json"], "[]", "旧版缺 JSON 时公共 JSON 返回空数组")
        self.assertEqual(len(store.snapshot()["items"]), 1, "内部仍能读入旧 TXT 库存")
        self.context(store)
        self.assertEqual(store.snapshot()["json"], "[]")
        store2 = self.store("legacy_mismatch")
        with open(os.path.join(store2.directory, store2.token + ".json"), "w") as f:
            json.dump([item(1)], f)
        with open(os.path.join(store2.directory, store2.token + ".txt"), "w") as f:
            f.write(item(2)["proxy"])
        with self.assertRaises(server.RepositoryError):
            store2.begin("bad", "generic", 0, [])

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

    def test_cancel_configuration_generation_and_protocol_identity(self):
        store = self.store()
        context = self.context(store)
        wrong_generation = {**context, "config_generation": 2}
        self.assertEqual(store.publish(wrong_generation, [self.observation(context)], "archive_all")["conflict_skipped"], 1)
        store.fence()
        self.assertEqual(store.publish(context, [self.observation(context)], "archive_all")["conflict_skipped"], 1)
        context = self.context(store, run="new", inputs=["socks5://192.0.2.1:8080"])
        store.publish(context, [self.observation(context)], "archive_all")
        self.assertEqual(store.snapshot()["items"], [])

    def test_manual_save_preserves_measurement_and_streak(self):
        store = self.store()
        context = self.context(store)
        store.publish(context, [self.observation(context)], "stable_only")
        store.save([item(1, last_ok=90000, pass_streak=99, country="SG")], "replace", store.summary()["repo_revision"])
        row = store.snapshot()["items"][0]
        self.assertEqual((row["last_ok"], row["pass_streak"]), (2000, 1))

    def test_receipt_capacity_never_discards_required_protection(self):
        store = self.store()
        context = self.context(store, [item(1)["proxy"], item(2)["proxy"]])
        with patch("repository.MAX_OBSERVATIONS", 1):
            store.publish(context, [self.observation(context)], "stable_only")
            with self.assertRaises(server.RepositoryError):
                store.publish(context, [self.observation(context, n=2, oid="second")], "stable_only")
        self.assertEqual(store.snapshot()["items"][0]["pass_streak"], 1)

    def test_parallel_manual_adds_are_serialized_without_lost_rows(self):
        import concurrent.futures
        store = self.store()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda n: store.save([item(n)], "merge"), range(1, 9)))
        self.assertEqual(len(store.snapshot()["items"]), 8)


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
            server.execute_maintenance_run(token, cfg, runtime, engine=Engine(), source=source, clock=lambda: 3,
                                           source_ids=["synthetic"])
            server._repository.cache_clear()
        self.assertEqual(seen, [item(i)["proxy"] for i in (2, 3, 4)])
        self.assertEqual(server.repo_store(token).maintenance()["source_queues"]["synthetic"], [])
        server.auto_runtime.pop(token)

    def test_old_configuration_is_not_enabled_or_forced(self):
        cfg = server.normalize_auto_config({"enabled": False, "detect_mode": "skip"})
        self.assertEqual(cfg["maintenance_mode"], "legacy")
        self.assertEqual(cfg["detect_mode"], "skip")
        self.assertFalse(cfg["enabled"])

    def test_restart_recovers_inputs_without_replaying_success(self):
        token = "restart_inputs"
        store = server.repo_store(token)
        store.save([item(1, last_checked_at=2000)], "replace", 0)
        store.maintenance({"profile": "generic", "candidates": [item(1)["proxy"], item(2)["proxy"], item(3)["proxy"]],
                           "cursor": 2, "inflight_type": "discover", "inflight_inputs": [item(1)["proxy"], item(2)["proxy"]]})
        seen = []
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                seen.extend(proxies)
                for p in proxies:
                    await on_result({**item(2), "proxy": p, "original": p, "terminal_complete": True,
                                     "started_at": 3000, "completed_at": 4000})
        def source(*_):
            raise AssertionError("恢复时不应重新抓源")
        runtime = {"run_id": "recovered", "stop": threading.Event(), "results": [], "counts": [0, 0, 0]}
        server.auto_runtime[token] = runtime
        server.execute_maintenance_run(token, self.config(), runtime, engine=Engine(), source=source, clock=lambda: 3)
        self.assertEqual(seen, [item(3)["proxy"], item(2)["proxy"]])
        self.assertNotIn("pass_streak", store.snapshot()["items"][0])
        self.assertNotIn("inflight_inputs", store.maintenance())
        server.auto_runtime.pop(token)

    def test_source_byte_and_candidate_limits(self):
        import io
        import fetch_proxies
        for content in (b"x" * (4 * 1024 * 1024 + 1),
                        "\n".join(f"192.0.2.1:{i}" for i in range(10000, 60001)).encode()):
            with patch.object(fetch_proxies, "PROXY_SOURCES", [{"id": "synthetic", "parser": "plain", "url": "http://example.invalid"}]), \
                    patch("urllib.request.urlopen", return_value=io.BytesIO(content)):
                with self.assertRaises(RuntimeError):
                    fetch_proxies.fetch_maintenance_source(0, threading.Event())

    def test_budget_interrupt_publishes_completed_not_fake_failures(self):
        from maintenance import IncrementalBatch, FairBudget
        store = server.repo_store("budget_interrupt")
        store.save([item(1), item(2)], "replace", 0)
        context = store.begin("budget", "generic", 1, [item(1)["proxy"], item(2)["proxy"]])
        now = [0]
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                await on_result(item(1, original=proxies[0], terminal_complete=True, started_at=1000, completed_at=2000))
                await asyncio.Event().wait()
        async def tick(_):
            now[0] += 1
            await asyncio.sleep(0)
        batch = IncrementalBatch(store, Engine(), FairBudget(2), "budget_interrupt", self.config(batch_seconds=3),
                                 context, threading.Event(), server.make_observation, clock=lambda: now[0], wait=tick)
        result, _ = asyncio.run(batch.run([item(1)["proxy"], item(2)["proxy"]]))
        self.assertEqual(result["incomplete_count"], 1)
        self.assertEqual(result["repo_removed"], 0)
        self.assertEqual(len(store.snapshot()["items"]), 2)
        self.assertEqual(store.snapshot()["items"][0]["last_ok"], 2000)

    def test_manual_cancel_keeps_committed_and_discards_buffer(self):
        from maintenance import IncrementalBatch, FairBudget
        store = server.repo_store("cancel_buffer")
        context = store.begin("cancel", "generic", 1, [item(1)["proxy"], item(2)["proxy"]])
        stop = threading.Event()
        first = asyncio.Event()
        cfg = self.config(publish_count=1)
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                await on_result(item(1, original=proxies[0], terminal_complete=True, started_at=1000, completed_at=2000))
                await first.wait()
                cfg["publish_count"] = 20
                await on_result(item(2, original=proxies[1], terminal_complete=True, started_at=1000, completed_at=2000))
                stop.set()
                store.fence()
        def status(**fields):
            if fields.get("repo_added"):
                first.set()
        batch = IncrementalBatch(store, Engine(), FairBudget(2), "cancel_buffer", cfg, context, stop,
                                 server.make_observation, on_status=status)
        asyncio.run(batch.run([item(1)["proxy"], item(2)["proxy"]]))
        self.assertEqual([i["proxy"] for i in store.snapshot()["items"]], [item(1)["proxy"]])

    def test_engine_half_round_is_not_a_result_and_hard_timeout_is(self):
        from proxy_check import ProxyCheckEngine, CheckConfig, RoundResult
        stopped = threading.Event()
        class HalfRound(ProxyCheckEngine):
            async def check_once_async(self, *args, **kwargs):
                stopped.set()
                return RoundResult(valid=True, service_reachable=True, ip="192.0.2.1")
        class HardTimeout(ProxyCheckEngine):
            async def check_proxy_full_async(self, *args, **kwargs):
                raise asyncio.TimeoutError()
        results = []
        asyncio.run(HalfRound(CheckConfig(timeout=1, detect_timeout=1, check_rounds=2)).check_many_async(
            [item(1)["proxy"]], stopped, 2, 1, results.append, "generic"))
        self.assertEqual(results, [])
        asyncio.run(HardTimeout(CheckConfig(timeout=1, detect_timeout=1, check_rounds=2)).check_many_async(
            [item(1)["proxy"]], threading.Event(), 2, 1, results.append, "generic"))
        self.assertEqual(results[0]["grade"], "F")
        self.assertTrue(results[0]["terminal_complete"])

    def test_fair_budget_shared_across_manual_and_workspaces(self):
        from maintenance import FairBudget
        budget = FairBudget(2)
        order, active, peak = [], [0], [0]
        async def worker(workspace, name):
            for _ in range(2):
                async with budget.lease(workspace, 2, threading.Event()) as slots:
                    active[0] += slots
                    peak[0] = max(peak[0], active[0])
                    order.append(name)
                    await asyncio.sleep(0)
                    active[0] -= slots
        async def run():
            await asyncio.gather(worker("one", "refresh"), worker("one", "manual"), worker("two", "discover"))
        asyncio.run(run())
        self.assertEqual(peak[0], 2)
        self.assertEqual(order[:3], ["refresh", "manual", "discover"])
        self.assertEqual(budget.available, 2)

    def test_source_failure_does_not_clear_or_starve_stock(self):
        token = "source_failure"
        store = server.repo_store(token)
        store.save([item(1)], "replace", 0)
        store.maintenance({"profile": "generic", "refresh_streak": 4})
        def source(*_):
            raise RuntimeError("synthetic secret must not appear in status")
        runtime = {"run_id": "source_fail", "stop": threading.Event(), "results": [], "counts": [0, 0, 0]}
        server.auto_runtime[token] = runtime
        server.execute_maintenance_run(token, self.config(), runtime, source=source)
        self.assertEqual(len(store.snapshot()["items"]), 1)
        self.assertNotIn("secret", runtime["source_error"])
        from maintenance import choose_batch
        kind, proxies, _ = choose_batch(store.snapshot(), store.maintenance(), self.config(), 2000)
        self.assertEqual(kind, "refresh")
        self.assertEqual(proxies, [item(1)["proxy"]])
        server.auto_runtime.pop(token)


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        import logging
        logging.shutdown()
        TEMP.cleanup()
