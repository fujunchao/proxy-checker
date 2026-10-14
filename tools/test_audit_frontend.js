// 审核 S1/S2/T4：跨页面共享持久化、独立内存及可控缓存事务。
const assert = require('node:assert/strict');
const {browser, bind} = require('./test_workspace_frontend');
let failed = 0;
async function test(name, fn) {
  try {await fn(); console.log(`PASS ${name}`);}
  catch (e) {failed++; console.error(`FAIL ${name}: ${e.stack}`);}
}
function checkedRead(b, text) {
  const req = b.requests.find(r => /\/api\/checked\/.+\.txt$/.test(r.url));
  b.reply(req, text);
}
function repoRead(b, rows, revision) {
  const req = b.requests.find(r => /\/api\/repo\/.+\.json$/.test(r.url));
  req.getResponseHeader = () => String(revision);
  b.reply(req, rows);
}
(async () => {
  for (const offline of [false, true]) await test(`S1 重载恢复未确认增量 offline=${offline}`, () => {
    const shared = new Map();
    const a = browser(shared); bind(a, 'audit_history'); a.context.checkedIncremental = true;
    checkedRead(a, 'http://192.0.2.1:80');
    a.context.markChecked('http://192.0.2.2:80');
    a.context.saveCheckedLocal(); a.runTimer(a.context.checkedLocalTimer);
    a.context.syncCheckedToServer();
    if (offline) {a.runTimer(a.context.checkedSyncTimer); a.requests.at(-1).onerror();}
    const b = browser(shared); bind(b, 'audit_history'); b.context.checkedIncremental = true;
    checkedRead(b, 'http://192.0.2.1:80');
    b.context.syncCheckedToServer();
    assert.ok(b.context.checkedSyncTimer, '刷新后必须恢复待确认增量，不能只恢复 checked 集合');
    b.runTimer(b.context.checkedSyncTimer);
    assert.deepEqual(b.requests.at(-1).body.proxies, ['http://192.0.2.2:80']);
    b.reply(b.requests.at(-1), {ok: true});
    const c = browser(shared); bind(c, 'audit_history'); c.context.checkedIncremental = true;
    checkedRead(c, 'http://192.0.2.1:80\nhttp://192.0.2.2:80');
    c.context.syncCheckedToServer();
    assert.equal(c.context.checkedPending.size, 0, '确认后不再重复排队');
  });

  await test('S1 清空意图重载后仍先清空服务器', () => {
    const shared = new Map(); const a = browser(shared);
    bind(a, 'audit_clear'); a.context.checkedIncremental = true;
    checkedRead(a, 'http://192.0.2.1:80');
    a.context.confirm = () => true; a.context.clearCheckedHistory();
    if (a.context.checkedLocalTimer) a.runTimer(a.context.checkedLocalTimer);
    const b = browser(shared); bind(b, 'audit_clear'); b.context.checkedIncremental = true;
    checkedRead(b, 'http://192.0.2.1:80'); b.context.syncCheckedToServer();
    assert.ok(b.context.checkedSyncTimer, '待确认清空不能在重载后消失');
    b.runTimer(b.context.checkedSyncTimer);
    assert.ok(b.requests.at(-1).url.endsWith('/api/checked/save'));
    assert.deepEqual(b.requests.at(-1).body.proxies, []);
  });

  await test('S2 旧写完成不能擦掉较新的清空标记', async () => {
    const shared = new Map(); const a = browser(shared); let oldTx, persisted;
    const db = {transaction(name, mode) {
      if (mode === 'readonly') return {objectStore() {return {get() {
        const request = {}; setImmediate(() => {request.result = persisted; request.onsuccess();}); return request;
      }}}};
      const tx = {objectStore() {return {
        put(value) {persisted = value; oldTx = tx;},
        delete() {throw Error('合成的删除失败');}
      };}}; return tx;
    }};
    a.context.window.indexedDB = {};
    a.context.resultsCacheDB = db;
    a.context.V = [{proxy:'http://192.0.2.1:80', valid:true}];
    a.context.saveResultsNow();
    a.context.V = []; a.context.clearResultsCache();
    oldTx.oncomplete();
    const b = browser(shared); b.context.window.indexedDB = {}; b.context.resultsCacheDB = db;
    await new Promise(resolve => b.context.loadSavedResults(resolve));
    assert.equal(b.context.V.length, 0, '清空标记不能被旧事务删除后恢复旧 IDB 结果');
  });

  await test('T4 两个标签页不能给旧仓库配新版本', () => {
    const shared = new Map(); const a = browser(shared); bind(a, 'audit_repo');
    repoRead(a, [{proxy:'http://192.0.2.1:80'}], 1);
    const b = browser(shared); bind(b, 'audit_repo');
    repoRead(b, [{proxy:'http://192.0.2.1:80'}], 1);
    b.context.addRepoItems([{proxy:'http://192.0.2.2:80'}]);
    b.runTimer(b.context.saveRepo._timer);
    const upload = b.requests.at(-1);
    assert.equal(upload.body.base_revision, 1);
    b.reply(upload, {ok:true, count:2, repo_revision:2});
    assert.equal(a.context.loadRepo().length, 1, 'A 仍持有独立旧缓存');
    a.context.removeFromRepo(0);
    const deleting = a.requests.at(-1);
    assert.deepEqual(deleting.body.repo, []);
    assert.equal(deleting.body.base_revision, 1, '必须发送 A 读取内容时的 v1，而不是共享元信息 v2');
    a.reply(deleting, {ok:false, stale_repo:true, repo_revision:2});
    assert.ok(a.requests.at(-1).url.endsWith('.json'), '冲突只能刷新，不能自动重放旧 replace');
  });
  process.exitCode = failed ? 1 : 0;
})();
