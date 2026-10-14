// 回放真实状态接口回调，验证批量恢复的工作量与去重、分类行为。
// 用法：node tools/test_result_performance.js；大批量基准：加 --bench。
const assert = require('node:assert/strict');
const {performance} = require('node:perf_hooks');
const {browser, bind} = require('./test_workspace_frontend');

function rows(count) {
  return Array.from({length: count}, (_, i) => ({
    original: `192.0.${Math.floor(i / 256)}.${i % 256}:8080`,
    proxy: `http://192.0.${Math.floor(i / 256)}.${i % 256}:8080`,
    valid: i % 100 < 4, unstable: i % 100 >= 4 && i % 100 < 7,
    latency: 750, target_profile: 'zai'
  }));
}

function automatic() {
  const b = browser();
  bind(b, 'performance_fixture');
  return b;
}

function replyAuto(b, incoming, index = incoming.length) {
  const request = b.requests.filter(r => r.url.endsWith('/api/auto/status')).at(-1);
  b.reply(request, {
    config: {enabled: true},
    state: {running: true, stage: 'detecting', session_id: 'fixture_run', done: index, total: 50163},
    new: incoming, results_index: index
  });
}

// 原实现每新增一条都遍历已有结果，4000 条需要约 800 万次键计算。
// 用确定性的工作量预算而非机器相关的秒数，防止二次复杂度回归。
function replay(count) {
  const b = automatic();
  let evaluations = 0;
  const originalKey = b.context.resultKey;
  b.context.resultKey = r => {evaluations++; return originalKey(r);};
  const incoming = rows(count);
  const started = performance.now();
  replyAuto(b, incoming);
  const elapsed = performance.now() - started;
  assert.equal(b.context.V.length + b.context.U.length + b.context.F.length, count);
  assert.equal(b.context.document.getElementById('sTotal').textContent, count);
  assert.equal(b.context.document.getElementById('autoStatusBadge').textContent, `自动检测中 ${count}/50163`);
  console.log(`${count} 条：${elapsed.toFixed(1)} ms，键计算 ${evaluations} 次`);
  assert.ok(evaluations <= count * 20, `恢复 ${count} 条进行了 ${evaluations} 次键计算，应为线性工作量`);
  return b;
}

for (const count of [1000, 2000, 4000]) replay(count);

// 跨批次、跨协议前缀及大小写的相同代理仍按最后一条覆盖，并移到目标分类末尾。
const updates = automatic();
replyAuto(updates, [
  {original: ' A.example:80 ', proxy: 'http://a.example:80', valid: true},
  {original: 'b.example:80', valid: true},
  {original: 'c.example:80', unstable: true}
]);
updates.context.loadAutoStatus();
replyAuto(updates, [
  {original: 'SOCKS5://A.EXAMPLE:80', valid: false, error: '超时'},
  {original: 'c.example:80', valid: true},
  {original: 'HTTP://a.example:80', unstable: true, latency: 42},
  {original: 'd.example:80', valid: false}
], 7);
assert.deepEqual(Array.from(updates.context.V, r => r.original), ['b.example:80', 'c.example:80']);
assert.deepEqual(Array.from(updates.context.U, r => r.original), ['HTTP://a.example:80']);
assert.equal(updates.context.U[0].latency, 42);
assert.deepEqual(Array.from(updates.context.F, r => r.original), ['d.example:80']);
assert.equal(updates.context.autoResultsIndex, 7);
updates.context.loadAutoStatus();
assert.equal(updates.requests.at(-1).body.since, 7, '续传游标按原始结果数而非去重后的数量递增');
replyAuto(updates, [], 7);
assert.equal(updates.context.document.getElementById('sTotal').textContent, 4);

updates.context.clearAll();
updates.context.loadAutoStatus();
replyAuto(updates, [{original: 'b.example:80', valid: true}], 8);
assert.equal(updates.context.document.getElementById('sTotal').textContent, 1, '清空后不能残留旧索引');

// 已恢复的本地历史与新一批重叠时，也只应扫描旧列表一次。
const cached = replay(4000);
let cachedKeys = 0;
const cachedKey = cached.context.resultKey;
cached.context.resultKey = r => {cachedKeys++; return cachedKey(r);};
cached.context.loadAutoStatus();
replyAuto(cached, rows(2000), 6000);
assert.equal(cached.context.document.getElementById('sTotal').textContent, 4000);
assert.ok(cachedKeys <= (4000 + 2000) * 20, '合并缓存与重复结果仍应为线性工作量');

// 手动检测使用相同的合并路径；通过实际轮询回调验证，而非只测辅助函数。
const manual = automatic();
manual.context.busy = true;
manual.context.sid = 'manual_fixture';
manual.context.totalCount = 4000;
manual.context.poll();
let manualKeys = 0;
const key = manual.context.resultKey;
manual.context.resultKey = r => {manualKeys++; return key(r);};
manual.reply(manual.requests.find(r => r.url.endsWith('/api/status')),
  {new: rows(4000), total_done: 4000, finished: false});
assert.equal(manual.context.resultsIndex, 4000);
assert.equal(manual.context.document.getElementById('sTotal').textContent, 4000);
assert.ok(manualKeys <= 4000 * 20, '手动检测也不能逐条全表扫描');

if (process.argv.includes('--bench')) {
  for (const count of [23575, 50163]) replay(count);
}
console.log('result performance ok');
