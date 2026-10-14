// 用真实 app.js 的请求边界回放分页、任务切换与增量历史上传。
const assert = require('node:assert/strict');
const {browser, bind} = require('./test_workspace_frontend');
function create() {const b = browser(); bind(b, 'pipeline'); return b;}
function summary(b, session, total, running = true) {
  const req = b.requests.filter(r => r.url.endsWith('/api/auto/status')).at(-1);
  b.reply(req, {config: {enabled: true}, state: {session_id: running ? session : null,
    running, stage: running ? 'detecting' : 'completed', status: running ? 'running' : 'completed', done: total, total},
    new: [], results_session_id: session, results_total: total, results_index: total});
}
const b = create();
assert.equal(b.requests.find(r => r.url.endsWith('/api/auto/status')).body.summary_only, true);
summary(b, 'first', 3);
assert.equal(b.context.autoResultsIndex, 0, '摘要不能把结果游标提前到末尾');
assert.match(b.context.document.getElementById('autoStatusBadge').textContent, /自动检测中/);
let req = b.requests.at(-1);
assert.ok(req.url.endsWith('/api/auto/results'));
b.reply(req, {session_id: 'first', new: [{proxy: 'http://a:80', valid: true}], results_index: 1, results_total: 3, has_more: true});
assert.equal(b.context.autoResultsIndex, 1);
b.runTimer(b.context.autoResultsTimer);
const stale = b.requests.at(-1);
assert.equal(stale.body.since, 1);
b.context.loadAutoStatus();
summary(b, 'second', 1);
const current = b.requests.at(-1);
b.reply(stale, {session_id: 'first', new: [{proxy: 'http://stale:80'}], results_index: 3, results_total: 3});
assert.equal(b.context.autoResultsIndex, 0);
b.reply(current, {session_id: 'second', new: [{proxy: 'http://b:80', valid: true}], results_index: 1, results_total: 1, has_more: false});
assert.equal(b.context.autoResultsIndex, 1);
assert.ok(!b.context.F.some(r => r.proxy.includes('stale')));

const ended = create();
summary(ended, 'ended', 1, false);
assert.ok(ended.requests.at(-1).url.endsWith('/api/auto/results'), '任务已结束也继续恢复尾页');

const history = create();
history.context.checkedIncremental = true;
for (let i = 0; i < 50000; i++) history.context.checkedProxies.add(`http://old:${i}`);
history.context.markChecked('http://new:80');
history.context.syncCheckedToServer();
history.runTimer(history.context.checkedSyncTimer);
let upload = history.requests.at(-1);
assert.ok(upload.url.endsWith('/api/checked/append'));
assert.deepEqual(upload.body.proxies, ['http://new:80']);
upload.onerror();
history.runTimer(history.context.checkedSyncTimer);
upload = history.requests.at(-1);
assert.deepEqual(upload.body.proxies, ['http://new:80']);
history.context.confirm = () => true;
history.context.clearCheckedHistory();
assert.equal(history.requests.at(-1), upload, '清空等待正在进行的追加完成');
history.reply(upload, {ok: true});
history.runTimer(history.context.checkedSyncTimer);
const clearing = history.requests.at(-1);
assert.ok(clearing.url.endsWith('/api/checked/save'));
assert.deepEqual(clearing.body.proxies, []);
history.context.markChecked('http://after-clear:80');
history.reply(clearing, {ok: true});
history.runTimer(history.context.checkedSyncTimer);
assert.deepEqual(history.requests.at(-1).body.proxies, ['http://after-clear:80']);
console.log('frontend pipeline ok');
