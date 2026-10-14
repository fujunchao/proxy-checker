"""性能接口的真实 HTTP 回归；数据和服务均位于临时目录。"""
import concurrent.futures
import json
import time
import unittest
import urllib.error
from unittest.mock import patch
import test_workspace as workspace


class PerformanceAPITest(workspace.WorkspaceTest):
    # 基类的完整冒烟由 CI 后续步骤单独执行，避免重复运行。
    @unittest.skip('完整冒烟由 test_workspace.py 执行')
    def test_existing_smoke_checks_in_both_modes(self):
        pass

    def setUp(self):
        super().setUp()
        self.server.auto_runtime.clear()
        self.server.auto_stopped_results.clear()
        self.server.write_checked_list(workspace.FIXED, [])

    def runtime(self, count=2505):
        rows = [{"original": f"192.0.2.1:{i}", "valid": i % 2 == 0,
                 "checks_detail": {"text": "x" * 100}} for i in range(count)]
        runtime = {"run_id": "test_run", "results": rows, "done": count,
                   "total": count, "stage": "detecting", "started_at": time.time()}
        self.server.auto_runtime[workspace.FIXED] = runtime
        return runtime

    def test_summary_and_pages(self):
        self.runtime()
        summary = self.request('/api/auto/status', {"summary_only": True})
        self.assertEqual(summary['new'], [])
        self.assertEqual(summary['results_session_id'], 'test_run')
        self.assertEqual(summary['state']['done'], 2505)
        self.assertLess(len(json.dumps(summary)), 5000)
        collected = []
        cursor = 0
        while True:
            page = self.request('/api/auto/results', {"session_id": "test_run", "since": cursor, "limit": 1000})
            self.assertLessEqual(len(page['new']), 1000)
            collected += page['new']
            cursor = page['results_index']
            if not page['has_more']:
                break
        self.assertEqual(cursor, 2505)
        self.assertEqual(len({r['original'] for r in collected}), 2505)
        legacy = self.request('/api/auto/status', {"since": 2500})
        self.assertEqual(len(legacy['new']), 5, '旧版客户端仍能按原接口续传')

    def test_page_bounds_and_session_change(self):
        self.runtime()
        page = self.request('/api/auto/results', {"session_id": "test_run", "since": -1, "limit": 999999})
        self.assertEqual(len(page['new']), 1000)
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.request('/api/auto/results', {"session_id": "old_run", "since": 10})
        self.assertEqual(error.exception.code, 409)

    def test_summary_stays_small_for_fifty_thousand_results(self):
        self.runtime(50000)
        summary = self.request('/api/auto/status', {"summary_only": True})
        self.assertEqual(summary['new'], [])
        self.assertEqual(summary['results_total'], 50000)
        self.assertLess(len(json.dumps(summary)), 5000)
        page = self.request('/api/auto/results', {"session_id": 'test_run', 'limit': 1000})
        self.assertEqual(len(page['new']), 1000)
        print(f"50000 条：摘要 {len(json.dumps(summary))} 字节，首批 {len(page['new'])} 条")
        seen = {row['original'] for row in page['new']}
        while page['has_more']:
            page = self.request('/api/auto/results', {"session_id": 'test_run', 'since': page['results_index']})
            seen.update(row['original'] for row in page['new'])
        self.assertEqual(len(seen), 50000)
        self.assertEqual(page['results_index'], 50000)

    def test_live_summary_counts_follow_new_results(self):
        self.runtime(1)
        self.request('/api/auto/status', {"summary_only": True})
        self.server.publish_auto_result(workspace.FIXED, {'original': 'a:80', 'unstable': True})
        summary = self.request('/api/auto/status', {"summary_only": True})
        self.assertEqual(summary['state']['valid_count'], 1)
        self.assertEqual(summary['state']['unstable_count'], 1)

    def test_terminal_results_keep_tail_pages(self):
        for status in ['completed', 'stopped', 'failed']:
            with self.subTest(status=status):
                runtime = self.runtime()
                self.server.finalize_auto_run(workspace.FIXED, runtime, status)
                summary = self.request('/api/auto/status', {"summary_only": True})
                self.assertEqual(summary['results_session_id'], 'test_run')
                page = self.request('/api/auto/results', {"session_id": "test_run", "since": 2500})
                self.assertEqual(len(page['new']), 5)
                self.server.auto_stopped_results[workspace.FIXED]['expires'] = 0
                with self.assertRaises(urllib.error.HTTPError) as error:
                    self.request('/api/auto/results', {"session_id": "test_run"})
                self.assertEqual(error.exception.code, 410)

    def test_append_is_idempotent_and_preserves_other_records(self):
        self.server.write_checked_list(workspace.FIXED, ['http://192.0.2.1:80'])
        with patch.object(self.server, 'atomic_write_text', wraps=self.server.atomic_write_text) as write:
            self.request('/api/checked/append', {"proxies": ['http://192.0.2.2:80']})
            self.request('/api/checked/append', {"proxies": ['HTTP://192.0.2.2:80']})
            self.assertEqual(write.call_count, 1, '重复追加不应重写历史')
        self.assertEqual(len(self.server.read_checked_list(workspace.FIXED)), 2)

    def test_concurrent_append_and_explicit_clear(self):
        def append(i):
            return self.request('/api/checked/append', {"proxies": [f'http://192.0.2.{i}:80']})
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(append, range(1, 9)))
        self.assertEqual(len(self.server.read_checked_list(workspace.FIXED)), 8)
        self.request('/api/checked/save', {"proxies": []})
        self.assertEqual(self.server.read_checked_list(workspace.FIXED), [])

    def test_new_endpoints_require_auth(self):
        for route in ['/api/auto/results', '/api/checked/append']:
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request(route, {}, authenticated=False)
            self.assertEqual(error.exception.code, 401)


if __name__ == '__main__':
    unittest.main(verbosity=2)
