// 测试真实页面函数，不连接服务器；订阅字段、版本与会话时序必须保持正确。
const assert = require('node:assert/strict');
const {browser, bind} = require('./test_workspace_frontend');
const {JSDOM} = require('jsdom');
const fs = require('node:fs');
const path = require('node:path');
const b = browser();
bind(b, 'maintenance_ui');
const w = b.context;
const read = b.requests.find(r => r.url.endsWith('/api/repo/maintenance_ui.json'));
read.getResponseHeader = name => name === 'X-Repo-Revision' ? '7' : null;
b.reply(read, [{proxy: 'http://192.0.2.1:80', usable: false, aux_reachable: {zcode: false},
  last_ok: 1234, last_checked_at: 2000, pass_streak: 2, target_profile: 'zai'}]);
assert.equal(w.loadRepo()[0].usable, false);
assert.equal(w.loadRepo()[0].last_ok, 1234);
w.syncRepoReplace([], 1);
assert.equal(b.requests.at(-1).body.base_revision, 7);
b.reply(b.requests.at(-1), {ok: true, count: 0, repo_revision: 8});
assert.equal(w.readRepoSyncMeta().revision, 8);

const pending = b.requests.find(r => r.url.endsWith('/api/auto/status'));
// 渲染断言使用真实 DOM 转义语义，不使用仅用于时序测试的简化节点。
const dom = new JSDOM(fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8'));
w.document = dom.window.document;
const progress = w.document.createElement('div');
progress.id = 'autoProgress';
w.document.body.appendChild(progress);
w.acceptAutoStatus({config: {enabled: true, maintenance_mode: 'continuous', target_profile: 'zai', detect_mode: 'skip'},
  state: {running: true, work_type: 'refresh', done: 1, total: 2, due_count: 9,
    unknown_age_count: 3, oldest_refresh_age_seconds: 1800, repo_count: 8, repo_revision: 8,
    source_count: 0, source_supported_count: 22, source_total_count: 33,
    source_last_id: 'databay_http', source_last_fetched_at: 2000000,
    maintenance_totals: {since: 1000000, source_fetches: 2, source_candidates: 7200,
      discover_checked: 300, refresh_checked: 100, repo_added: 85, repo_removed: 29}},
  results_session_id: 'new-run', results_total: 0});
assert.match(w.document.getElementById('autoStatusBadge').textContent, /存量复测中/);
assert.match(w.document.getElementById('autoProgress').innerHTML, /存量到期 9/);
assert.match(w.document.getElementById('autoProgress').innerHTML, /版本 8/);
assert.match(w.document.getElementById('autoProgress').textContent, /累计新增 85/);
assert.match(w.document.getElementById('autoProgress').textContent, /累计移除 29/);
assert.match(w.document.getElementById('autoProgress').textContent, /来源覆盖 22\/33/);
assert.match(w.document.getElementById('autoProgress').textContent, /最近抓取.*databay_http/);
assert.match(w.document.getElementById('autoProgress').textContent, /本批新拉取 0/);

// 多来源缓存补量必须保留本批口径；来源名称作为文本显示，不能注入页面。
w.renderAutoProgress({config: {maintenance_mode: 'continuous', target_profile: 'zai'},
  state: {total: 200, source_count: 0, source_current_id: 'one',
    source_current_ids: ['one', 'two<img src=x onerror=alert(1)>'],
    candidate_deduplicated: 12, candidate_filtered: 8, selection_seconds: 0.25}});
assert.match(progress.textContent, /本批来源 one、two<img src=x onerror=alert\(1\)>/);
assert.match(progress.textContent, /有效输入 200/);
assert.match(progress.textContent, /过滤库存／历史 8/);
assert.match(progress.textContent, /合并重复候选 12/);
assert.match(progress.textContent, /选批（含抓取） 0\.25 秒/);
assert.equal(progress.querySelector('img'), null);
w.renderAutoProgress({config: {maintenance_mode: 'continuous'}, state: {source_current_id: 'legacy_source'}});
assert.match(progress.textContent, /本批来源 legacy_source/);
assert.doesNotMatch(progress.textContent, /合并重复候选/, '旧状态不应把缺失指标伪装成零');
b.reply(pending, {config: {enabled: false}, state: {}, new: []});
assert.equal(w.autoStatusCache.config.enabled, true, '旧摘要不能覆盖更新的配置响应');

w.logoutAuth();
const oldState = w.autoStatusCache;
b.reply(pending, {config: {enabled: false}, state: {}, new: []});
assert.equal(w.autoStatusCache, oldState, '登出后的旧请求不能更改状态');
assert.equal(w.autoSessionId, '');
assert.equal(w.autoPageRequest, null);
dom.window.close();
console.log('maintenance frontend ok');
