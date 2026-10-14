// 用真实 app.js 和可控的 DOM/XHR 替身验证初始化时序，不依赖浏览器或外网。
// 用法：node tools/test_workspace_frontend.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');

function browser(saved = {}) {
  const storage = new Map(Object.entries(saved));
  const requests = [];
  const elements = new Map();
  const timers = new Map();
  let nextTimer = 0;
  function element(id = '') {
    if (elements.has(id)) return elements.get(id);
    const node = {value: id === 'roundsSelect' ? '2' : id === 'concurrentInput' ? '30' : '',
      textContent: '', innerHTML: '', style: {setProperty() {}}, dataset: {},
      classList: {add() {}, remove() {}, toggle() {}, contains() {return false;}},
      addEventListener() {}, appendChild() {}, removeChild() {}, insertAdjacentHTML() {},
      querySelector() {return element('child');}, querySelectorAll() {return [];},
      closest() {return element('parent');}, getBoundingClientRect() {return {bottom: 100};}};
    elements.set(id, node);
    node.parentElement = node;
    return node;
  }
  const context = {console, Set, Map, Date, Math, Intl,
    localStorage: {getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value)),
      removeItem: key => storage.delete(key)},
    document: {getElementById: element, createElement: () => element('created'),
      querySelector: element, querySelectorAll: () => [], addEventListener() {}, body: element('body')},
    window: {location: {hostname: 'localhost', origin: 'http://localhost', reload() {throw Error('unexpected reload');}}},
    setTimeout(fn, delay) {const id = ++nextTimer; timers.set(id, {fn, delay}); return id;},
    clearTimeout(id) {timers.delete(id);}, requestAnimationFrame(fn) {fn();},
    XMLHttpRequest: function () {
      this.open = (method, url) => {this.method = method; this.url = url;};
      this.setRequestHeader = () => {};
      this.send = body => {this.body = body ? JSON.parse(body) : null; requests.push(this);};
    }};
  vm.createContext(context);
  vm.runInContext(source, context);
  function reply(request, payload, status = 200) {
    request.status = status;
    request.responseText = typeof payload === 'string' ? payload : JSON.stringify(payload);
    request.onload();
  }
  function runTimer(id) {
    const timer = timers.get(id);
    assert.ok(timer, `定时器 ${id} 不存在`);
    timers.delete(id);
    timer.fn();
  }
  return {context, storage, requests, reply, timers, runTimer};
}

function bind(b, token) {
  const cap = b.requests.find(r => r.url.endsWith('/api/capabilities'));
  assert.ok(cap);
  b.reply(cap, {auth_required: true, authenticated: true, auto_mode: true, workspace_token: token});
}

module.exports = {browser, bind};

if (require.main === module) {
for (const previous of ['', 'user_another_browser']) {
  const b = browser(previous ? {'proxy_checker_token': previous,
    'proxy_checker_repo': '[{"proxy":"http://wrong-cache:80"}]',
    'proxy_checker_checked': '["http://wrong-cache:80"]'} : {});
  assert.equal(b.requests.filter(r => /\/api\/(repo|checked|auto|logs|start)[/]/.test(r.url)).length, 0,
    '身份返回之前不能读取随机 token 的仓库或自动任务');
  bind(b, 'user_existing_workspace');
  assert.equal(b.context.getUserToken(), 'user_existing_workspace');
  assert.equal(b.context.loadRepo().length, 0, '其他浏览器身份的旧缓存不能混入固定身份');
  assert.equal(b.context.getCheckedCount(), 0);
  assert.ok(b.requests.some(r => r.url.endsWith('/api/repo/user_existing_workspace.json')));
  assert.ok(b.requests.some(r => r.url.endsWith('/api/checked/user_existing_workspace.txt')));
  const auto = b.requests.find(r => r.url.endsWith('/api/auto/status'));
  assert.equal(auto.body.token, 'user_existing_workspace');
  assert.equal(b.requests.filter(r => r.method === 'POST' && /\/save$/.test(r.url)).length, 0,
    '初始化不能把旧缓存写回固定身份');
  b.reply(auto, {config: {enabled: true}, state: {running: true, stage: 'detecting', done: 1, total: 2}, new: []});
  assert.equal(b.context.document.getElementById('autoStatusBadge').textContent, '自动检测中 1/2');
  b.reply(b.requests.find(r => r.url.endsWith('/api/repo/user_existing_workspace.json')),
    [{proxy: 'http://127.0.0.1:9', grade: 'A'}]);
  assert.equal(b.context.loadRepo().length, 1);
}

const legacy = browser({'proxy_checker_token': 'user_legacy'});
bind(legacy, '');
assert.equal(legacy.context.getUserToken(), 'user_legacy', '未配置固定身份时维持旧行为');

const existing = browser({'proxy_checker_token': 'user_existing_workspace',
  'proxy_checker_repo': '[{"proxy":"http://127.0.0.1:9"}]'});
bind(existing, 'user_existing_workspace');
assert.equal(existing.context.loadRepo().length, 1, '原身份的本地缓存可以安全沿用');
assert.ok(existing.requests.some(r => r.url.endsWith('/api/repo/user_existing_workspace.json')),
  '固定身份每次打开都刷新服务端仓库，不能仅依赖本地缓存');
existing.reply(existing.requests.find(r => r.url.endsWith('/api/repo/user_existing_workspace.json')), []);
existing.reply(existing.requests.find(r => r.url.endsWith('/api/repo/user_existing_workspace.txt')), '', 404);
assert.equal(existing.context.loadRepo().length, 0, '服务端空仓库也应清除固定身份的旧缓存');

const offline = browser({'proxy_checker_token': 'user_existing_workspace',
  'proxy_checker_repo': '[{"proxy":"http://127.0.0.1:9"}]'});
bind(offline, 'user_existing_workspace');
offline.requests.find(r => r.url.endsWith('/api/repo/user_existing_workspace.json')).onerror();
offline.requests.find(r => r.url.endsWith('/api/repo/user_existing_workspace.txt')).onerror();
assert.equal(offline.context.loadRepo().length, 1, '网络失败不能当作空仓库覆盖本地缓存');

const migrated = browser({'proxy_checker_token': 'user_existing_workspace',
  'proxy_checker_results': '{"valid":[{"proxy":"http://old-cache:80"}]}',
  'proxy_checker_cache_migrated:user_existing_workspace': '1'});
bind(migrated, 'user_existing_workspace');
assert.equal(migrated.context.V.length, 0, '清除已迁移结果后不能再次从旧缓存恢复');

const locked = browser();
locked.reply(locked.requests.find(r => r.url.endsWith('/api/capabilities')),
  {auth_required: true, authenticated: false, auto_mode: true});
assert.equal(locked.requests.filter(r => /\/api\/(repo|checked|auto|logs)[/]/.test(r.url)).length, 0);
console.log('workspace frontend ok');
}
