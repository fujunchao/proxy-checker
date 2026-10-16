"""并发利用率回归：通过真实协调器、临时仓库和可替换网络检测器验证。"""
import logging
import threading
import unittest

from test_maintenance import server, item


def tearDownModule():
    logging.shutdown()


class MaintenancePipeline(unittest.TestCase):
    def setUp(self):
        self.token = self.id().rsplit('.', 1)[-1]
        self.now = 2000.0
        self.config = {**server.default_auto_config(), 'maintenance_mode': 'continuous',
                       'publish_count': 20, 'detect_mode': 'force'}
        self.store = server.repo_store(self.token)
        self.source_ids = []
        self.sources = {}
        self.fetched = []
        self.batches = []
        self.runtimes = []

    def tearDown(self):
        server.auto_runtime.pop(self.token, None)

    def source(self, index, stop):
        self.fetched.append(self.source_ids[index])
        return self.sources.get(self.source_ids[index], []), self.source_ids[index]

    def run_batch(self, engine=None, elapsed=0):
        case = self

        class Engine:
            async def check_many_async(self, proxies, stop, rounds, concurrent, on_result, profile):
                case.batches.append(list(proxies))
                case.now += elapsed
                for proxy in proxies:
                    await on_result({**item(1), 'proxy': proxy, 'original': proxy,
                                     'terminal_complete': True, 'started_at': int(case.now * 1000),
                                     'completed_at': int(case.now * 1000)})

        runtime = {'run_id': f'pipeline_{len(self.runtimes)}', 'stop': threading.Event(),
                   'results': [], 'counts': [0, 0, 0], 'done': 0}
        server.auto_runtime[self.token] = runtime
        self.runtimes.append(runtime)
        return server.execute_maintenance_run(
            self.token, self.config, runtime, engine=engine or Engine(), source=self.source,
            clock=lambda: self.now, timer_clock=lambda: self.now, source_ids=self.source_ids)

    def test_completed_stock_is_due_again_before_newer_stock(self):
        self.config['refresh_batch'] = 2
        self.store.save([item(1, last_checked_at=1000), item(2, last_checked_at=1000)], 'replace', 0)
        self.run_batch()
        self.run_batch()  # 无到期库存时让出发现机会，结束连续复测段。
        self.now = 3001.0
        self.store.save([item(3, last_checked_at=2001000)], 'merge', self.store.summary()['repo_revision'])
        self.run_batch()
        self.assertEqual(self.batches[-1], [item(1)['proxy'], item(2)['proxy']],
                         '已完成节点再次到期后，不能被上一轮尝试集合挡在更新的节点之后')


if __name__ == '__main__':
    unittest.main(verbosity=2)
