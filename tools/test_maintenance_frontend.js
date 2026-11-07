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

// 自动任务弹窗不能超框：869px 高的窗口里，长状态曾把整排按钮顶到屏幕外，点不到“保存设置”。
// 卡片限高在视口内，头部与底部按钮常驻，只有正文滚动；状态明细默认折叠成摘要。
const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const css = /<style>([\s\S]*?)<\/style>/.exec(indexHtml)[1];
assert.match(css, /\.modal-overlay\{[^}]*overflow-y:auto/, '遮罩层自身要能滚动，兜住比内容更矮的窗口');
assert.match(css, /\.modal-box\{[^}]*margin:auto/, '卡片用 auto 外边距居中，内容超高时才从顶部开始排');
assert.match(css, /\.modal-scroll\{[^}]*max-height:calc/, '长内容弹窗的卡片要限制在视口高度内');
assert.match(css, /\.modal-scroll-body\{[^}]*overflow-y:auto/, '长内容弹窗的正文要独立滚动');
assert.match(css, /\.auto-progress \.auto-detail\{display:none/, '状态明细默认折叠');

// 前面手搭的 #autoProgress 站在文档里会抢到 getElementById，先摘掉，让渲染落到弹窗自己的状态区。
if (progress.parentNode) progress.parentNode.removeChild(progress);
w.showAutoModal({config: {enabled: true, maintenance_mode: 'continuous', target_profile: 'zai'},
  state: {running: true, done: 1, total: 2, due_count: 7, repo_count: 8, repo_revision: 9,
    candidate_filtered: 3, source_current_ids: ['one']}});
const autoOverlay = w.document.querySelector('.modal-overlay[data-modal="auto"]');
assert.ok(autoOverlay, '自动任务弹窗应当渲染出来');
const autoBox = autoOverlay.querySelector('.modal-box');
assert.ok(autoBox.classList.contains('modal-scroll'), '自动任务卡片要使用限高滚动布局');
const autoBody = autoBox.querySelector('.modal-scroll-body');
assert.ok(autoBody, '自动任务正文要有独立滚动容器');
assert.ok(autoBody.contains(autoBox.querySelector('#autoProgress')), '状态区必须在滚动正文内');
assert.ok(autoBody.contains(autoBox.querySelector('.auto-form')), '表单必须在滚动正文内');
assert.equal(autoBox.querySelector('.auto-action-row').parentElement, autoBox,
  '操作按钮不能被放进滚动正文，否则仍会被长状态顶出视口');

// 状态区默认只显示摘要：状态/阶段/进度常显，存量、来源、累计等明细折叠。
const autoProgressBox = autoBox.querySelector('#autoProgress');
assert.ok(autoProgressBox.querySelectorAll('.auto-detail').length > 0, '明细行要带 auto-detail 标记');
assert.equal(autoProgressBox.classList.contains('expanded'), false, '每次打开都从折叠摘要开始');
const summaryText = Array.from(autoProgressBox.children)
  .filter(el => !el.classList.contains('auto-detail') && !el.classList.contains('auto-detail-toggle'))
  .map(el => el.textContent).join(' ');
assert.match(summaryText, /状态: /, '摘要要保留状态');
assert.match(summaryText, /阶段: /, '摘要要保留阶段');
assert.match(summaryText, /进度: 1\/2/, '摘要要保留本批进度');
assert.doesNotMatch(summaryText, /存量到期/, '折叠时明细不能混进摘要');
assert.match(autoProgressBox.querySelector('.auto-detail-toggle button').textContent, /展开全部明细/);
w.toggleAutoProgressDetail();
assert.equal(autoProgressBox.classList.contains('expanded'), true, '点击后要展开全部明细');
assert.match(autoProgressBox.querySelector('.auto-detail-toggle button').textContent, /收起明细/);
w.toggleAutoProgressDetail();
assert.equal(autoProgressBox.classList.contains('expanded'), false, '再点一次要收回摘要');

// 设置与日志弹窗共用同一套限高布局：正文滚动、按钮常驻底部。
w.showSettingsModal(w.appSettings);
const settingsOverlay = w.document.querySelector('.modal-overlay[data-modal="settings"]');
const settingsBox = settingsOverlay.querySelector('.modal-box');
assert.ok(settingsBox.classList.contains('modal-scroll'), '设置卡片也要限高');
assert.ok(settingsBox.querySelector('.modal-scroll-body').contains(settingsBox.querySelector('.auto-form')),
  '设置表单要在滚动正文内');
assert.equal(settingsBox.querySelector('.modal-footer').parentElement, settingsBox, '设置按钮要常驻卡片底部');

w.showRunLogsModal([]);
const logsOverlay = w.document.querySelector('.modal-overlay[data-modal="logs"]');
const logsBox = logsOverlay.querySelector('.modal-box');
assert.ok(logsBox.classList.contains('modal-scroll'), '日志卡片也要限高');
assert.ok(logsBox.querySelector('.modal-scroll-body').contains(logsBox.querySelector('#runLogList')),
  '日志列表要在滚动正文内');
assert.equal(logsBox.querySelector('.modal-footer').parentElement, logsBox, '日志按钮要常驻卡片底部');

w.logoutAuth();
const oldState = w.autoStatusCache;
b.reply(pending, {config: {enabled: false}, state: {}, new: []});
assert.equal(w.autoStatusCache, oldState, '登出后的旧请求不能更改状态');
assert.equal(w.autoSessionId, '');
assert.equal(w.autoPageRequest, null);
dom.window.close();
console.log('maintenance frontend ok');
