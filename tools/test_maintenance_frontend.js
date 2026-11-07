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
  last_ok: 1234, last_checked_at: 2000, pass_streak: 2, target_profile: 'zai',
  up_kbps: 320, up_error: 'timeout'}]);
assert.equal(w.loadRepo()[0].usable, false);
assert.equal(w.loadRepo()[0].last_ok, 1234);
// 上行读数由检测链路写入：仓库加载要原样留住它们，否则下一次本地保存就会抹掉。
assert.equal(w.loadRepo()[0].up_kbps, 320, '加载仓库不能丢上行读数');
assert.equal(w.loadRepo()[0].up_error, 'timeout', '加载仓库不能丢测速失败原因');
const compactedReading = w.compactRepoItem({proxy: 'http://192.0.2.9:80', grade: 'A',
  up_kbps: 320, up_error: 'timeout'});
assert.equal(compactedReading.up_kbps, 320, '仓库压缩白名单必须保留 up_kbps');
assert.equal(compactedReading.up_error, 'timeout', '仓库压缩白名单必须保留 up_error');
assert.equal(w.compactRepoItem({proxy: 'http://192.0.2.9:80', up_kbps: 0}).up_kbps, 0,
  '0KB/s 是实测读数，不能当成没有读数丢掉');
const manualReading = w.resultToRepoItem({proxy: 'http://192.0.2.10:80', grade: 'A', up_kbps: 96});
assert.equal(manualReading.up_kbps, 96, '手动检测结果入库要带读数');
assert.ok(!('up_error' in manualReading), '没测到/没失败原因就不要写出这个键');
assert.equal(w.resultToRepoItem({proxy: 'http://192.0.2.10:80', up_error: 'timeout'}).up_error, 'timeout',
  '有失败原因时要透传');
// 合并语义：手动检测没测到读数时，不能把仓库里已有的读数抹掉（Object.assign 的易踩点）。
w.saveRepo([{proxy: 'http://192.0.2.11:80', grade: 'A', up_kbps: 320, up_error: 'timeout'}], {sync: false});
w.addRepoItems([{proxy: 'http://192.0.2.11:80', grade: 'B'}]);
assert.equal(w.loadRepo()[0].up_kbps, 320, '无读数的保存不能抹掉已有读数');
assert.equal(w.loadRepo()[0].grade, 'B', '非测量字段仍按新结果改写');
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
// 保存成功后要回读云端：服务端会保护 up_kbps/up_error 等只读测量字段，
// 只拿提交的那份回写本地，读数就永远回不到浏览器（P2）。
// 包一层 loadRepoFromServer 来数「回读是否走既有的加载函数」，应用内按全局名调用，包得住。
function spyRepoLoads(ctx) {
  const state = {count: 0};
  const original = ctx.loadRepoFromServer;
  ctx.loadRepoFromServer = function (cb, opt) { state.count++; return original(cb, opt); };
  return state;
}
function newReadsSince(bucket, index, suffix) {
  return bucket.requests.slice(index).filter(r => r.url.endsWith(suffix));
}

const refresh = browser();
bind(refresh, 'save_refresh');
const rw = refresh.context;
const refreshLoads = spyRepoLoads(rw);
const firstLoad = refresh.requests.find(r => r.url.endsWith('/api/repo/save_refresh.json'));
firstLoad.getResponseHeader = name => name === 'X-Repo-Revision' ? '7' : null;
refresh.reply(firstLoad, [{proxy: 'http://192.0.2.20:80', grade: 'A'}]);
assert.equal(rw.loadRepo()[0].up_kbps, undefined, '客户端手里这份本来就没有读数');
rw.syncRepoToServer();
const saveReq = refresh.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
const beforeSaveReply = refresh.requests.length;
refresh.reply(saveReq, {ok: true, count: 1, current_count: 1, repo_revision: 8});
const reRead = newReadsSince(refresh, beforeSaveReply, '/api/repo/save_refresh.json')[0];
assert.ok(reRead, '保存成功后必须回读云端仓库');
assert.equal(refreshLoads.count, 1, '回读复用既有的云端加载函数，没有另起一套请求栈');
reRead.getResponseHeader = () => '8';
refresh.reply(reRead, [{proxy: 'http://192.0.2.20:80', grade: 'A', up_kbps: 320, up_error: 'timeout'}]);
assert.equal(rw.loadRepo()[0].up_kbps, 320, '服务端保护下来的读数要回到浏览器');
assert.equal(rw.loadRepo()[0].up_error, 'timeout', '测速失败原因同样要回读回来');
assert.equal(rw.readRepoSyncMeta().revision, 8, '回写要带上服务端版本号');
assert.equal(refresh.requests.filter(r => r.url.endsWith('/api/repo/save')).length, 1,
  '回读不得再触发一次保存（否则会打成循环）');

// 保存期间用户又改了仓库（代次变化）→ 本地不回写，也不再发起回读。
const edited = browser();
bind(edited, 'save_edited');
const editedW = edited.context;
const editedLoads = spyRepoLoads(editedW);
const editedLoad = edited.requests.find(r => r.url.endsWith('/api/repo/save_edited.json'));
editedLoad.getResponseHeader = () => '1';
edited.reply(editedLoad, [{proxy: 'http://192.0.2.50:80', grade: 'A'}]);
editedW.syncRepoToServer();
const editedSave = edited.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
editedW.addRepoItems([{proxy: 'http://192.0.2.51:80', grade: 'B'}]);
edited.reply(editedSave, {ok: true, count: 1, current_count: 1, repo_revision: 2});
assert.equal(editedLoads.count, 0, '保存期间用户改了仓库就不要再回读');
assert.equal(editedW.loadRepo().length, 2, '保存响应不能覆盖用户在等待期间的新编辑');

// 回读在途时用户又改了仓库 → 放弃这次回写（用过期快照盖新编辑是最坏的情况）。
const race = browser();
bind(race, 'save_race');
const raceW = race.context;
const raceLoads = spyRepoLoads(raceW);
const raceLoad = race.requests.find(r => r.url.endsWith('/api/repo/save_race.json'));
raceLoad.getResponseHeader = () => '1';
race.reply(raceLoad, [{proxy: 'http://192.0.2.40:80', grade: 'A'}]);
raceW.syncRepoToServer();
const raceSave = race.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
const beforeRaceReply = race.requests.length;
race.reply(raceSave, {ok: true, count: 1, current_count: 1, repo_revision: 2});
const raceRead = newReadsSince(race, beforeRaceReply, '/api/repo/save_race.json')[0];
assert.ok(raceRead, '保存成功后应当发起回读');
assert.equal(raceLoads.count, 1, '回读只发起一次');
raceW.addRepoItems([{proxy: 'http://192.0.2.41:80', grade: 'B'}]);
race.reply(raceRead, [{proxy: 'http://192.0.2.40:80', grade: 'A', up_kbps: 999}]);
assert.equal(raceW.loadRepo().length, 2, '回读不能覆盖用户在等待期间的新编辑');
assert.equal(raceW.loadRepo().filter(item => item.up_kbps === 999).length, 0,
  '代次已变时这次回读必须被放弃');

// 回读只认 JSON：.json 失败退到 .txt 时，占位条目（grade='?'、无读数）不能覆盖本地缓存，
// 否则保存一次就把 grade/latency/up_kbps 一起抹成占位值，且用户毫无提示。
const jsonless = browser();
bind(jsonless, 'save_jsonless');
const jw = jsonless.context;
const jsonlessLoads = spyRepoLoads(jw);
const jsonlessLoad = jsonless.requests.find(r => r.url.endsWith('/api/repo/save_jsonless.json'));
jsonlessLoad.getResponseHeader = () => '5';
jsonless.reply(jsonlessLoad, [{proxy: 'http://192.0.2.60:80', grade: 'A', latency: 100, up_kbps: 320}]);
assert.equal(jw.loadRepo()[0].up_kbps, 320);
let toasts = 0;
const originalToast = jw.toast;
jw.toast = function (message) { toasts++; return originalToast(message); };
jw.syncRepoToServer();
const jsonlessSave = jsonless.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
const beforeJsonlessReply = jsonless.requests.length;
jsonless.reply(jsonlessSave, {ok: true, count: 1, current_count: 1, repo_revision: 6});
const jsonRead = newReadsSince(jsonless, beforeJsonlessReply, '/api/repo/save_jsonless.json')[0];
assert.ok(jsonRead, '保存成功后要回读');
assert.equal(jsonlessLoads.count, 1, '回读走既有加载函数');
jsonless.reply(jsonRead, 'server error', 500);
const txtRead = jsonless.requests.filter(r => r.url.endsWith('/api/repo/save_jsonless.txt')).at(-1);
assert.ok(txtRead, '.txt 兜底请求确实发出（否则这条用例测不到兜底路径）');
jsonless.reply(txtRead, 'http://192.0.2.60:80');
assert.equal(jw.loadRepo()[0].grade, 'A', 'txt 兜底的占位条目不能覆盖本地 grade');
assert.equal(jw.loadRepo()[0].latency, 100, 'latency 同样不能被占位值抹掉');
assert.equal(jw.loadRepo()[0].up_kbps, 320, '上行读数更不能被抹掉');
assert.equal(toasts, 0, '回读失败必须是静默的，不打扰用户');

// 保存后的回读不能让另一笔在途保存的成功响应失效。
// 顺序：保存1成功 -> 回读1在途 -> 保存2发出 -> 回读1/保存2响应交错。
for (const readFirst of [true, false]) {
  const overlap = browser();
  const token = 'save_overlap_' + Number(readFirst);
  bind(overlap, token);
  const ow = overlap.context;
  const suffix = '/api/repo/' + token + '.json';
  const rows = [{proxy: 'http://192.0.2.70:80', grade: 'A', up_kbps: 320}];
  const initial = overlap.requests.find(r => r.url.endsWith(suffix));
  initial.getResponseHeader = () => '1';
  overlap.reply(initial, rows);
  ow.syncRepoToServer();
  const firstSave = overlap.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
  const beforeFirstReply = overlap.requests.length;
  overlap.reply(firstSave, {ok: true, count: 1, current_count: 1, repo_revision: 2});
  const oldRead = overlap.requests.slice(beforeFirstReply).find(r => r.url.endsWith(suffix));
  assert.ok(oldRead);
  ow.saveRepoToCloud();
  const secondSave = overlap.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
  assert.equal(secondSave.body.base_revision, 2);
  const replyOldRead = () => {
    oldRead.getResponseHeader = () => '2';
    overlap.reply(oldRead, rows);
  };
  if (readFirst) replyOldRead();
  const beforeSecondReply = overlap.requests.length;
  overlap.reply(secondSave, {ok: true, count: 1, current_count: 1, repo_revision: 3});
  if (!readFirst) replyOldRead();
  assert.equal(ow.repoCacheRevision, 3, '后台回读不能阻止成功保存推进本地版本');
  const latestRead = overlap.requests.slice(beforeSecondReply).find(r => r.url.endsWith(suffix));
  assert.ok(latestRead, '后一笔保存仍应回读权威快照');
  latestRead.getResponseHeader = () => '3';
  overlap.reply(latestRead, rows);
  ow.addRepoItems([{proxy: 'http://192.0.2.71:80', grade: 'B'}]);
  ow.syncRepoToServer();
  const thirdSave = overlap.requests.filter(r => r.url.endsWith('/api/repo/save')).at(-1);
  assert.equal(thirdSave.body.base_revision, 3, '下一次编辑必须携带已确认的新版本');
  assert.equal(thirdSave.body.repo.length, 2, '交错后新增代理仍完整提交');
  overlap.reply(thirdSave, {ok: true, count: 2, current_count: 1, repo_revision: 4});
  assert.equal(ow.loadRepo().length, 2, '成功响应不能丢失新增代理');
}

dom.window.close();
console.log('maintenance frontend ok');
