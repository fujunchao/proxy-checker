"""发现公平性回归：真实协调器、临时仓库、合成来源，不访问外网。"""
import threading
import unittest
import logging
import asyncio
from unittest.mock import patch

from test_maintenance import server, item


def tearDownModule():
    logging.shutdown()


class DiscoveryFairness(unittest.TestCase):
    def setUp(self):
        self.token = self.id().rsplit('.', 1)[-1]
        self.now = 2000.0
        self.visited = []
        self.checked = []
        self.types = []
        self.config = {**server.default_auto_config(), 'maintenance_mode': 'continuous',
                       'publish_count': 1, 'detect_mode': 'force'}
        self.store = server.repo_store(self.token)

    def tearDown(self):
        server.auto_runtime.pop(self.token, None)

    def source(self, index, stop):
        self.visited.append(index)
        return [f'http://198.51.100.{index + 1}:{port}' for port in range(8000, 8006)], 'synthetic'

    def run_batch(self, elapsed=0, engine=None):
        case = self
        class Engine:
            async def check_many_async(self, proxies, stop_event, rounds, max_concurrent, on_result, target_profile):
                case.now += elapsed
                for proxy in proxies:
                    case.checked.append(proxy)
                    await on_result({**item(1), 'proxy': proxy, 'original': proxy,
                                     'terminal_complete': True, 'started_at': int(case.now * 1000),
                                     'completed_at': int(case.now * 1000)})
        runtime = {'run_id': f'run_{len(self.types)}', 'stop': threading.Event(),
                   'results': [], 'counts': [0, 0, 0], 'done': 0}
        server.auto_runtime[self.token] = runtime
        result = server.execute_maintenance_run(self.token, self.config, runtime, engine=engine or Engine(),
                    source=self.source, clock=lambda: self.now, timer_clock=lambda: self.now,
                    source_ids=getattr(self, 'source_ids', None))
        self.types.append(runtime['work_type'])
        return result

    def test_large_source_yields_before_its_queue_is_drained(self):
        self.config['discover_batch'] = 2
        self.run_batch()
        server._repository.cache_clear()  # 重开仓库不能丢失来源轮转位置。
        self.store = server.repo_store(self.token)
        self.run_batch()
        self.assertEqual(self.visited, [0, 1], '第一源还有候选时也必须抓取第二源')
        self.assertIn('http://198.51.100.2:8000', self.checked)

    def test_sparse_expirations_do_not_take_consecutive_refresh_batches(self):
        self.store.save([item(1, last_checked_at=1000),
                         item(2, last_checked_at=int((self.now - 890) * 1000))], 'replace', 0)
        self.run_batch(elapsed=20)
        self.run_batch()
        self.assertEqual(self.types, ['refresh', 'discover'], '零散到期项不能连续挤占发现机会')

    def test_refresh_time_budget_guarantees_discovery_with_bulk_backlog(self):
        self.config['publish_count'] = 100
        self.store.save([{'proxy': f'http://192.0.2.1:{p}', 'target_profile': 'generic',
                          'last_checked_at': 1000} for p in range(10000, 10400)], 'replace', 0)
        self.run_batch(elapsed=80)
        self.run_batch(elapsed=80)
        self.run_batch()
        self.assertEqual(self.types, ['refresh', 'refresh', 'discover'],
                         '耗尽复测时间预算后，即便未满四批也应让出发现机会')

    def test_full_rotation_preserves_tails_and_cools_down_empty_queues(self):
        self.source_ids = ['one', 'two']
        self.config['discover_batch'] = 2
        for _ in range(6):
            self.run_batch()
            server._repository.cache_clear()
        self.assertEqual(self.visited, [0, 1])
        self.assertEqual(len(set(self.checked)), 12)
        self.assertEqual(server.repo_store(self.token).maintenance()['source_queues'], {'one': [], 'two': []})
        self.run_batch()
        self.assertEqual(self.visited, [0, 1], '来源刚排空时不能立即重新抓取')
        self.now += 301
        self.run_batch()
        self.assertEqual(self.visited, [0, 1, 0])

    def test_legacy_large_queue_migrates_without_delaying_next_source(self):
        self.source_ids = ['one', 'two']
        self.config['discover_batch'] = 2
        old = [f'http://203.0.113.1:{n}' for n in range(8000, 8006)]
        self.store.maintenance({'profile': 'generic', 'source_index': 1, 'candidates': old, 'cursor': 2})
        self.run_batch()
        self.assertEqual(self.visited, [1])
        self.assertEqual(self.store.maintenance()['source_queues']['one'], old[2:])
        self.run_batch()
        self.assertEqual(self.checked[-2:], old[2:4])
        self.assertEqual(self.store.maintenance()['source_queues']['one'], old[4:])

    def test_restart_recovers_inflight_but_never_replays_published_success(self):
        self.source_ids = ['one', 'two']
        self.store.save([item(1, last_checked_at=int(self.now * 1000))], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'source_queues': {'one': [item(3)['proxy']]},
            'source_next': 'two', 'inflight_type': 'discover', 'inflight_queue': 'source',
            'inflight_inputs': [item(1)['proxy'], item(2)['proxy']], 'discovery_retry_turn': True})
        self.run_batch()
        self.assertEqual(self.checked, [item(2)['proxy'], item(3)['proxy']],
                         '恢复输入优先，剩余批量可使用其他来源缓存，不重放已提交成功')
        self.assertEqual(self.visited, [])
        self.assertEqual(self.store.maintenance()['source_queues']['one'], [])
        self.assertNotIn('pass_streak', self.store.snapshot()['items'][0])

    def test_slow_inputs_are_retried_without_pinning_the_next_source(self):
        self.source_ids = ['one', 'two']
        self.config['discover_batch'] = 2
        case = self
        class Slow:
            async def check_many_async(self, *args):
                case.now += 121
                await asyncio.Event().wait()
        self.run_batch(engine=Slow())
        self.assertEqual(len(self.store.maintenance()['retry_candidates']), 2)
        self.run_batch()  # 独立重试。
        self.run_batch()  # 下一个来源，不回到第一源剩余四条。
        self.assertEqual(self.visited, [0, 1])
        self.assertIn('http://198.51.100.2:8000', self.checked)

    def test_source_failure_yields_and_is_counted_without_leaking_error_text(self):
        self.source_ids = ['one', 'two']
        source = self.source
        def failing(index, stop):
            if index == 0:
                raise RuntimeError('secret URL and credentials')
            return source(index, stop)
        self.source = failing
        self.run_batch()
        self.assertNotIn('secret', server.auto_runtime[self.token]['source_error'])
        self.run_batch()
        totals = self.store.summary()['maintenance_totals']
        self.assertEqual((totals['source_errors'], totals['source_fetches']), (1, 1))
        self.assertEqual(totals['repo_added'], 6)

    def test_capacity_preserves_admitted_tail_and_retries_unadmitted_source(self):
        self.source_ids = ['one', 'two']
        self.config['discover_batch'] = 2
        with patch('maintenance.MAX_SOURCE_CANDIDATES', 8):
            self.run_batch()  # 第一源接纳六条，余四条。
            self.run_batch()  # 第二源不能完整接纳；继续检测原队列，不空耗本批。
            self.assertEqual(len(self.store.maintenance()['source_queues']['one']), 2)
            self.assertEqual(len(self.checked), 4, '已接纳的唯一候选只能被检测消费，不能被容量处理丢弃')
            self.assertNotIn('two', self.store.maintenance()['source_queues'])
            self.assertIn('容量不足', server.auto_runtime[self.token]['source_error'])
            self.run_batch()  # 原队列继续排空，为下一来源腾出空间。
            self.now += 61
            self.run_batch()
            self.assertEqual(self.visited, [0, 1, 1])
            self.assertIn('http://198.51.100.2:8000', self.checked)

    def test_cumulative_publication_is_atomic_and_idempotent_after_reopen(self):
        context = self.store.begin('atomic', 'generic', 0, [item(1)['proxy']], work_type='discover')
        observation = server.make_observation(context, {**item(1), 'original': item(1)['proxy'],
            'terminal_complete': True, 'started_at': 1000, 'completed_at': 2000}, 0)
        def fail_after_commit(stage):
            if stage == 'committed':
                raise OSError('synthetic post-commit failure')
        with patch.object(self.store, 'fault', fail_after_commit):
            with self.assertRaises(OSError):
                self.store.publish(context, [observation], 'stable_only')
        server._repository.cache_clear()
        self.store = server.repo_store(self.token)
        retry = self.store.publish(context, [observation], 'stable_only')
        self.assertEqual(retry['duplicates'], 1)
        self.assertEqual(retry['maintenance_totals']['repo_added'], 1)
        self.assertEqual(retry['maintenance_totals']['discover_checked'], 1)
        self.store.maintenance({'profile': 'generic'})  # 旧协调器状态不能回滚累计计数。
        self.assertEqual(self.store.summary()['maintenance_totals']['repo_added'], 1)

    def test_source_counter_retries_do_not_double_count(self):
        state = {'profile': 'generic', 'source_queues': {'one': [item(1)['proxy']]}}
        event = {'id': 'fetch_once', 'ok': True, 'count': 1}
        self.store.maintenance(state, source_event=event)
        server._repository.cache_clear()
        self.store = server.repo_store(self.token)
        self.store.maintenance(state, source_event=event)
        totals = self.store.summary()['maintenance_totals']
        self.assertEqual((totals['source_fetches'], totals['source_candidates']), (1, 1))

    def test_manual_clear_during_fetch_still_fences_discovery_publication(self):
        source = self.source
        def clearing(index, stop):
            self.store.save([], 'replace', self.store.summary()['repo_revision'])
            return source(index, stop)
        self.source = clearing
        self.run_batch()
        self.assertEqual(self.store.snapshot()['items'], [])
        self.assertEqual(self.store.summary()['maintenance_totals'].get('repo_added', 0), 0)

    def test_retired_source_queue_is_drained_even_without_active_sources(self):
        self.source_ids = []
        self.store.maintenance({'profile': 'generic', 'source_index': 1,
                                'candidates': [item(1)['proxy']], 'cursor': 0})
        self.run_batch()
        self.assertEqual(self.checked, [item(1)['proxy']])
        self.assertEqual(self.visited, [])

    def test_old_config_preserves_opt_in_and_detection_policy(self):
        cfg = server.normalize_auto_config({'enabled': False, 'detect_mode': 'skip', 'refresh_share': 7})
        self.assertFalse(cfg['enabled'])
        self.assertEqual(cfg['maintenance_mode'], 'legacy')
        self.assertEqual(cfg['detect_mode'], 'skip')
        self.assertEqual(cfg['refresh_share'], 7)
        self.assertEqual(cfg['refresh_budget_seconds'], 120)


if __name__ == '__main__':
    unittest.main(verbosity=2)
