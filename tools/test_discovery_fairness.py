"""发现公平性回归：真实协调器、临时仓库、合成来源，不访问外网。"""
import threading
import unittest
import logging

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

    def run_batch(self, elapsed=0):
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
        result = server.execute_maintenance_run(self.token, self.config, runtime, engine=Engine(),
                    source=self.source, clock=lambda: self.now, timer_clock=lambda: self.now)
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
        self.store.save([{'proxy': f'http://192.0.2.1:{p}', 'target_profile': 'generic',
                          'last_checked_at': 1000} for p in range(10000, 10400)], 'replace', 0)
        self.run_batch(elapsed=80)
        self.run_batch(elapsed=80)
        self.run_batch()
        self.assertEqual(self.types, ['refresh', 'refresh', 'discover'],
                         '耗尽复测时间预算后，即便未满四批也应让出发现机会')


if __name__ == '__main__':
    unittest.main(verbosity=2)
