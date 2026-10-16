"""并发利用率回归：通过真实协调器、临时仓库和可替换网络检测器验证。"""
import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

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
        self.batch_sources = []
        self.runtimes = []

    def tearDown(self):
        server.auto_runtime.pop(self.token, None)

    def source(self, index, stop):
        self.fetched.append(self.source_ids[index])
        return self.sources.get(self.source_ids[index], []), self.source_ids[index]

    def run_batch(self, engine=None, elapsed=0, usable=True):
        case = self

        class Engine:
            async def check_many_async(self, proxies, stop, rounds, concurrent, on_result, profile):
                case.batches.append(list(proxies))
                case.batch_sources.append(case.store.maintenance().get('candidate_sources', {}))
                case.now += elapsed
                for proxy in proxies:
                    await on_result({**item(1), 'proxy': proxy, 'original': proxy,
                                     'grade': 'A' if usable else 'F', 'usable': usable, 'valid': usable,
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

    def test_upgrade_releases_completed_marks_but_keeps_unfinished_rotation(self):
        self.config['refresh_batch'] = 1
        self.store.save([item(1, last_checked_at=1000), item(2, last_checked_at=2000), item(3)], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'refresh_seen': [item(1)['proxy'], item(3)['proxy']],
                                'retry_after': {item(3)['proxy']: self.now - 1}})
        self.run_batch()
        self.assertEqual(self.batches[-1], [item(1)['proxy']],
                         '升级应解除旧的完整检测标记，但不能让已尝试的未完成项立即再次抢占')

    def test_duplicate_pending_candidates_are_checked_once_even_when_they_fail(self):
        self.source_ids = ['one', 'two']
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'one': [item(1)['proxy']], 'two': [item(1)['proxy'], item(2)['proxy']]}})
        self.run_batch(usable=False)
        self.run_batch(usable=False)
        self.assertEqual([p for batch in self.batches for p in batch], [item(1)['proxy'], item(2)['proxy']],
                         '未入库的失败候选也不能因同时排在不同来源中而重复检测')

    def test_filtered_small_source_is_filled_from_other_cached_sources(self):
        self.config['discover_batch'] = 4
        self.source_ids = ['small', 'large']
        self.store.save([item(1, last_checked_at=int(self.now * 1000))], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'small': [item(1)['proxy']], 'large': [item(n)['proxy'] for n in (2, 3, 4, 5)]}})
        self.run_batch()
        self.assertEqual(self.batches, [[item(n)['proxy'] for n in (2, 3, 4, 5)]],
                         '主来源过滤后不足一批时，应使用已有缓存补足有效输入')
        self.assertEqual(self.fetched, [], '已有缓存补量不能触发额外网络抓取')

    def test_status_separates_dedup_filtering_cached_sources_and_new_fetches(self):
        self.source_ids = ['one', 'two']
        self.store.save([item(1, last_checked_at=int(self.now * 1000))], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'one': [item(1)['proxy'], item(2)['proxy']],
            'two': [item(2)['proxy'], item(3)['proxy']]}})
        self.run_batch()
        state = server.get_auto_status(self.token, summary_only=True)['state']
        self.assertEqual(state.get('source_current_ids'), ['one', 'two'])
        self.assertEqual(state.get('candidate_deduplicated'), 1)
        self.assertEqual(state.get('candidate_filtered'), 1)
        self.assertEqual((state['total'], state['source_count']), (2, 0),
                         '缓存补量不是新抓取；有效检测条数与重复合并、历史过滤分别计数')
        summary = server.build_auto_summary(self.runtimes[-1], 'completed')
        saved = server.compact_run_log({'id': 'pipeline_metrics', **summary})
        self.assertEqual(saved.get('source_current_ids'), ['one', 'two'])
        self.assertEqual(saved.get('selection_seconds'), 0)

    def test_skip_filtering_does_not_leave_the_effective_batch_empty(self):
        self.config.update(discover_batch=2, detect_mode='skip')
        self.source_ids = ['one']
        server.write_checked_list(self.token, [item(1)['proxy'], item(2)['proxy']])
        self.store.save([item(3, last_checked_at=int(self.now * 1000))], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'one': [item(n)['proxy'] for n in (1, 2, 3, 4, 5, 6)]}})
        self.run_batch()
        self.assertEqual(self.batches, [[item(4)['proxy'], item(5)['proxy']]])
        self.assertEqual(self.runtimes[-1]['candidate_filtered'], 3)
        self.assertEqual(self.store.maintenance()['source_queues']['one'], [item(6)['proxy']])

    def test_cached_fill_does_not_skip_an_empty_sources_primary_turn(self):
        self.config['discover_batch'] = 3
        self.source_ids = ['small', 'fresh', 'large']
        self.sources['fresh'] = [item(n)['proxy'] for n in (7, 8, 9)]
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'small': [item(1)['proxy']], 'large': [item(n)['proxy'] for n in (2, 3, 4, 5, 6)]}})
        self.run_batch()
        self.assertEqual(self.batches[0], [item(n)['proxy'] for n in (1, 2, 3)])
        self.run_batch()
        self.assertEqual(self.fetched, ['fresh'], '补量游标不能把尚未读取的主来源跳过去')
        self.assertEqual(self.batches[1], [item(n)['proxy'] for n in (7, 8, 9)])

    def test_retry_identity_and_all_source_owners_survive_reopen(self):
        self.config['discover_batch'] = 2
        self.source_ids = ['one', 'two']
        self.store.maintenance({'profile': 'generic', 'discovery_retry_turn': True,
            'retry_candidates': [item(1)['proxy']], 'candidate_sources': {item(1)['proxy']: ['retired']},
            'source_queues': {'one': [item(1)['proxy'], item(2)['proxy']],
                              'two': [item(1)['proxy'], item(3)['proxy']]}})
        self.run_batch()
        self.assertEqual(self.batch_sources[0][item(1)['proxy']], ['retired', 'one', 'two'])
        server._repository.cache_clear()
        self.store = server.repo_store(self.token)
        self.run_batch()
        self.assertEqual([p for batch in self.batches for p in batch], [item(n)['proxy'] for n in (1, 2, 3)])
        self.assertEqual(self.store.maintenance()['candidate_sources'], {}, '来源归属不能变成无限增长的端点历史')
        self.assertTrue(all(row['pass_streak'] == 1 for row in self.store.snapshot()['items']))

    def test_dedup_preserves_protocol_and_case_sensitive_credentials(self):
        self.source_ids = ['one', 'two']
        values = ['http://User:Pass@EXAMPLE.invalid:8080', 'http://user:Pass@example.invalid:8080',
                  'socks5://User:Pass@example.invalid:8080', 'socks5h://User:Pass@example.invalid:8080',
                  'example.invalid:8080']
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'one': values[:2], 'two': ['HTTP://User:Pass@example.invalid:8080'] + values[2:]}})
        self.run_batch()
        self.assertEqual(self.batches, [values], '只能合并相同端点，不能按 IP 或小写凭据合并不同身份')

    def test_force_can_refetch_a_completed_failure_after_source_cooldown(self):
        self.source_ids = ['one']
        self.sources['one'] = [item(1)['proxy']]
        self.run_batch(usable=False)
        self.now += 301
        self.run_batch(usable=False)
        self.assertEqual(self.batches, [[item(1)['proxy']], [item(1)['proxy']]])
        self.assertEqual(self.store.summary()['maintenance_totals']['discover_checked'], 2)

    def test_source_capacity_counts_unique_pending_inputs_and_keeps_owners(self):
        self.source_ids = ['one', 'two']
        self.sources['two'] = [item(1)['proxy'], item(3)['proxy']]
        self.store.maintenance({'profile': 'generic', 'source_next': 'two',
                                'source_queues': {'one': [item(1)['proxy'], item(2)['proxy']]}})
        with patch('maintenance.MAX_SOURCE_CANDIDATES', 3):
            self.run_batch()
        self.assertEqual(self.batches, [[item(3)['proxy'], item(1)['proxy'], item(2)['proxy']]])
        self.assertEqual(self.batch_sources[0][item(1)['proxy']], ['one', 'two'])
        self.assertEqual(self.runtimes[-1]['source_count'], 2)
        self.assertIsNone(self.runtimes[-1]['source_error'])

    def test_over_capacity_source_is_not_partially_admitted_while_cache_runs(self):
        self.source_ids = ['one', 'two']
        self.sources['two'] = [item(3)['proxy'], item(4)['proxy']]
        self.store.maintenance({'profile': 'generic', 'source_next': 'two',
                                'source_queues': {'one': [item(1)['proxy'], item(2)['proxy']]}})
        with patch('maintenance.MAX_SOURCE_CANDIDATES', 3):
            self.run_batch()
        self.assertEqual(self.batches, [[item(1)['proxy'], item(2)['proxy']]])
        self.assertNotIn('two', self.store.maintenance()['source_queues'])
        self.assertEqual(self.store.summary()['maintenance_totals']['source_errors'], 1)

    def test_empty_source_does_not_trigger_a_chain_of_network_fetches(self):
        self.source_ids = ['one', 'two', 'three']
        self.sources['three'] = [item(1)['proxy']]
        self.run_batch()
        self.assertEqual(self.fetched, ['one'], '一批最多抓取一个主来源，不能串行等待所有空来源')
        self.assertEqual(self.batches, [])

    def test_cancelling_a_fetch_does_not_lose_previously_admitted_inputs(self):
        self.source_ids = ['one', 'two']
        self.store.maintenance({'profile': 'generic', 'source_next': 'two',
                                'source_queues': {'one': [item(1)['proxy'], item(2)['proxy']]}})

        def cancelled(index, stop):
            stop.set()
            return [item(3)['proxy']], 'two'

        self.source = cancelled
        self.run_batch()
        self.assertEqual(self.store.maintenance()['source_queues']['one'], [item(1)['proxy'], item(2)['proxy']])
        self.assertEqual(self.batches, [])
        self.assertEqual(self.store.snapshot()['items'], [])

    def test_partial_publication_recovers_mixed_sources_without_repeating_success(self):
        self.config.update(discover_batch=2, publish_count=1)
        self.source_ids = ['one', 'two']
        self.store.maintenance({'profile': 'generic', 'source_queues': {
            'one': [item(1)['proxy']], 'two': [item(2)['proxy'], item(3)['proxy']]}})
        case = self

        class Interrupted:
            async def check_many_async(self, proxies, stop, rounds, concurrent, on_result, profile):
                case.batches.append(list(proxies))
                await on_result({**item(1), 'original': item(1)['proxy'], 'terminal_complete': True,
                                 'started_at': 2000000, 'completed_at': 2000000})

                async def published():
                    while case.store.summary()['repo_count'] != 1:
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(published(), timeout=5)
                raise RuntimeError('合成中断：首条已提交，后续输入尚未完成')

        with self.assertRaisesRegex(RuntimeError, '合成中断'):
            self.run_batch(engine=Interrupted())
        self.assertEqual(self.store.maintenance()['candidate_sources'][item(2)['proxy']], ['two'])
        server._repository.cache_clear()
        self.store = server.repo_store(self.token)
        self.run_batch()
        self.assertEqual(self.batches, [[item(1)['proxy'], item(2)['proxy']], [item(2)['proxy'], item(3)['proxy']]])
        self.assertEqual(self.store.summary()['maintenance_totals']['repo_added'], 3)
        self.assertTrue(all(row['pass_streak'] == 1 for row in self.store.snapshot()['items']))
        self.assertEqual(self.store.maintenance()['candidate_sources'], {})

    def test_published_refresh_is_not_blocked_by_an_interrupted_attempt_marker(self):
        self.config['refresh_batch'] = 1
        self.store.save([item(1, last_checked_at=1000), item(2, last_checked_at=2001000)], 'replace', 0)
        self.store.maintenance({'profile': 'generic', 'refresh_seen': [item(1)['proxy']],
            'refresh_attempted_at': {item(1)['proxy']: 1900000}, 'retry_after': {item(1)['proxy']: 1960}})
        context = self.store.begin('interrupted_refresh', 'generic', 0, [item(1)['proxy']], work_type='refresh')
        observation = server.make_observation(context, {**item(1), 'original': item(1)['proxy'],
            'terminal_complete': True, 'started_at': 1900000, 'completed_at': 2000000}, 0)
        self.store.publish(context, [observation], 'stable_only')
        server._repository.cache_clear()
        self.store = server.repo_store(self.token)
        self.now = 3001.0
        self.run_batch()
        self.assertEqual(self.batches[-1], [item(1)['proxy']], '已提交完成时间优先于清理前残留的尝试标记')

    def test_unknown_age_is_not_a_completed_observation_at_clock_zero(self):
        self.now = 0.0
        self.config.update(refresh_batch=1, refresh_budget_seconds=600)
        self.store.save([item(1), item(2), item(3)], 'replace', 0)
        case = self

        class Slow:
            async def check_many_async(self, proxies, *args):
                case.batches.append(list(proxies))
                case.now += 121
                await asyncio.Event().wait()

        for _ in range(3):
            self.run_batch(engine=Slow())
        self.assertEqual(self.batches, [[item(n)['proxy']] for n in (1, 2, 3)],
                         '未知时间不能被当成已完成，跨过退避后仍应先轮转未尝试的慢节点')


if __name__ == '__main__':
    unittest.main(verbosity=2)
