// jsdom 提供真实 DOM 操作语义；fake-indexeddb 提供异步事务语义。
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const {IDBFactory} = require('fake-indexeddb');
const root = path.join(__dirname, '..');
function page(factory = new IDBFactory()) {
  const dom = new JSDOM(fs.readFileSync(path.join(root, 'index.html'), 'utf8'), {url: 'http://localhost', runScripts: 'outside-only'});
  const w = dom.window, timers = new Map(); let timer = 0;
  w.indexedDB = factory;
  w.setTimeout = (fn, delay) => {const id = ++timer; timers.set(id, {fn, delay}); return id;};
  w.clearTimeout = id => timers.delete(id);
  w.XMLHttpRequest = function () {this.open = () => {}; this.setRequestHeader = () => {}; this.send = () => {};};
  w.eval(fs.readFileSync(path.join(root, 'app.js'), 'utf8'));
  return {w, timers, close: () => dom.window.close(), run(id) {const t = timers.get(id); assert.ok(t); timers.delete(id); t.fn();}};
}
const wait = () => new Promise(resolve => setTimeout(resolve, 20));
const load = w => new Promise(resolve => w.loadSavedResults(resolve));

(async () => {
  const p = page(), w = p.w;
  const a = {proxy: 'http://a:80', valid: true, checks_detail: {service: {status: 200}}};
  w.upsertResults([a, {proxy: 'http://b:80', valid: true}]);
  w.renderResultLists();
  const list = w.document.getElementById('validList'), first = list.firstElementChild;
  first.querySelector('.detail-panel').classList.add('show');
  w.renderResultLists();
  assert.equal(list.firstElementChild, first, '相同结果必须复用原节点');
  assert.ok(first.querySelector('.detail-panel').classList.contains('show'));
  w.upsertResults([{proxy: 'http://b:80', valid: false}]);
  w.renderResultLists();
  assert.equal(list.firstElementChild, first, '其他行分类变化不能重建未变化行');
  assert.equal(w.document.getElementById('failList').querySelectorAll('.proxy-item').length, 1);
  w.upsertResults([Object.assign({}, a, {latency: 20})]);
  w.renderResultLists();
  assert.notEqual(list.firstElementChild, first);
  assert.ok(list.firstElementChild.querySelector('.detail-panel').classList.contains('show'), '更新一行时保留详情展开状态');

  w.repoCache = [{proxy: 'http://repo-a:80', grade: 'A'}, {proxy: 'http://repo-b:80', grade: 'B'}];
  w.renderRepo();
  const repo = w.document.getElementById('repoList'), repoFirst = repo.firstElementChild;
  w.renderRepo();
  assert.equal(repo.firstElementChild, repoFirst, '仓库行也应复用');
  w.repoCache = w.repoCache.slice(1); w.renderRepo();
  assert.match(repo.firstElementChild.innerHTML, /removeFromRepo\(0\)/, '删除后的按钮索引必须更新');

  w.saveResults(); const scheduled = w.resultsSaveTimer;
  w.saveResults(); w.saveResults();
  assert.equal(w.resultsSaveTimer, scheduled, '连续更新合并为一次写入且不能无限推迟');
  p.run(scheduled); await wait();
  assert.equal(w.localStorage.getItem(w.RESULTS_KEY), null, '正常使用 IndexedDB，不再全量写 localStorage');
  w.V = []; w.U = []; w.F = [];
  assert.equal(await load(w), true);
  assert.equal(w.V.length, 1);
  assert.equal(w.F.length, 1);
  const db = w.resultsCacheDB;
  w.resultsCacheDB = {transaction() {throw Error('模拟缓存写入失败');}};
  w.V = [{proxy: 'http://fallback-new:80', valid: true}]; w.F = []; w.saveResultsNow();
  w.resultsCacheDB = db;
  assert.equal(await load(w), true);
  assert.equal(w.V[0].proxy, 'http://fallback-new:80', '写入失败后的新降级缓存优先于旧 IDB 快照');
  await wait();
  w.resultsCacheDB = {transaction() {throw Error('模拟缓存删除失败');}};
  w.clearAll(); w.resultsCacheDB = db;
  await load(w);
  assert.equal(w.V.length, 0, '删除失败时不能从 IDB 恢复已清空的数据');

  w.localStorage.setItem(w.RESULTS_KEY, JSON.stringify({valid: [{proxy: 'http://legacy:80', valid: true}], unstable: [], invalid: []}));
  w.RESULTS_KEY = 'migration';
  w.localStorage.setItem(w.RESULTS_KEY, JSON.stringify({valid: [{proxy: 'http://legacy:80', valid: true}]}));
  assert.equal(await load(w), true);
  await wait();
  assert.equal(w.localStorage.getItem('migration'), null, '旧快照迁移成功后才删除');
  assert.equal(w.V[0].proxy, 'http://legacy:80');

  // 尚未完成的旧缓存读取不能覆盖新网络结果，也不能使清空的结果复活。
  const reading = load(w);
  w.clearAll();
  w.upsertResults([{proxy: 'http://fresh:80', valid: true}]); w.saveResults();
  await reading;
  assert.equal(w.V[0].proxy, 'http://fresh:80');
  await wait();
  w.V=[]; w.U=[]; w.F=[];
  await load(w);
  assert.equal(w.V.length + w.U.length + w.F.length, 0);

  const fallback = page(null);
  fallback.w.V = Array.from({length: 5000}, (_, i) => ({proxy: `http://fallback:${i}`, valid: true}));
  fallback.w.saveResultsNow();
  assert.ok((fallback.w.localStorage.getItem(fallback.w.RESULTS_KEY)||'').length < 100, '禁用 IndexedDB 时不能退回巨量同步写入');
  fallback.close(); p.close();
  console.log('cache and DOM ok');
})().catch(error => {console.error(error); process.exitCode = 1;});
