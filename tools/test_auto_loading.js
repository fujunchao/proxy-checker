// 通过实际 XHR 回调验证状态恢复、错误提示及重试，不连接真实部署。
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {browser, bind} = require('./test_workspace_frontend');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
assert.ok(/id="autoStatusBadge"[^>]*>正在加载任务状态/.test(html), '尚未收到状态时不能显示自动未开启');

for (const failure of ['network', 'timeout', 'http', 'html', 'null']) {
  const b = browser();
  bind(b, 'loading_fixture');
  const first = b.requests.find(r => r.url.endsWith('/api/auto/status'));
  assert.equal(first.timeout, 60000, '状态请求有超时上限，同时给首次全量传输留出时间');
  if (failure === 'network') first.onerror();
  if (failure === 'timeout') first.ontimeout();
  if (failure === 'http') b.reply(first, {}, 502);
  if (failure === 'html') b.reply(first, '<h1>Bad Gateway</h1>', 502);
  if (failure === 'null') b.reply(first, null, 502);
  const badge = b.context.document.getElementById('autoStatusBadge');
  assert.match(badge.textContent, /状态加载失败.*重试/);
  assert.equal(b.timers.get(b.context.autoStatusTimer).delay, 5000);
  b.runTimer(b.context.autoStatusTimer);
  const retry = b.requests.at(-1);
  assert.ok(retry.url.endsWith('/api/auto/status'));
  b.reply(retry, {config: {enabled: true},
    state: {running: true, stage: 'detecting', session_id: 'loaded', done: 1, total: 10},
    new: [{proxy: 'http://192.0.2.1:80', valid: true}], results_index: 1});
  assert.equal(badge.textContent, '自动检测中 1/10');
  assert.equal(b.context.document.getElementById('sTotal').textContent, 1);
  assert.equal(b.timers.get(b.context.autoStatusTimer).delay, 2500);
  // 断线不能清空已恢复的结果，也不能把续传游标重置为零。
  b.runTimer(b.context.autoStatusTimer);
  const next = b.requests.at(-1);
  assert.equal(next.body.since, 1);
  next.onerror();
  assert.equal(b.context.document.getElementById('sTotal').textContent, 1);
  assert.equal(b.context.autoResultsIndex, 1);
  b.runTimer(b.context.autoStatusTimer);
  assert.equal(b.requests.at(-1).body.since, 1);
}

const disabled = browser();
bind(disabled, 'disabled_fixture');
disabled.reply(disabled.requests.find(r => r.url.endsWith('/api/auto/status')),
  {config: {enabled: false}, state: {running: false}, new: [], results_index: 0});
assert.equal(disabled.context.document.getElementById('autoStatusBadge').textContent, '自动未开启');
console.log('auto loading ok');
