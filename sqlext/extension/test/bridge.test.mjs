// bridge.test.mjs —— 用 Node 模拟浏览器的三个世界，验证桥接协议真的能跑通。
//
// 浏览器里这部分最难调试（主世界/隔离世界/service worker 三个上下文），
// 所以这里用 vm 模块搭个最小模拟环境：
//
//   模拟 window（同时被两个世界共享）  ← postMessage 的唯一公共地带
//   ├── 主世界上下文：跑 page-bridge.js，暴露 window.sqlext
//   └── 隔离世界上下文：跑 host.js，把 postMessage 转成 chrome.runtime.sendMessage
//                       再把 service worker 的回复转回 postMessage
//
// 跑法：node --test sqlext/extension/test/
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import vm from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const SRC = join(here, '..', 'src');

const read = (p) => readFileSync(join(SRC, p), 'utf8');

/**
 * 造一个模拟 window：支持 addEventListener / postMessage，
 * 派发时带上 source=window（两个世界共用同一个 window 对象）。
 */
function makeWindow() {
  const listeners = [];
  const w = {
    location: { origin: 'https://example.com', href: 'https://example.com/page' },
    addEventListener(type, fn) {
      if (type === 'message') {
        listeners.push(fn);
      }
    },
    removeEventListener(type, fn) {
      const i = listeners.indexOf(fn);
      if (i >= 0) {
        listeners.splice(i, 1);
      }
    },
    postMessage(data) {
      // 真实浏览器里 postMessage 是异步派发的，这里也用微任务模拟
      queueMicrotask(() => {
        for (const fn of listeners.slice()) {
          fn({ source: w, data });
        }
      });
    }
  };
  return w;
}

/** 造一个假的 document，够 host.js 注入脚本用。 */
function makeDocument() {
  const head = {
    appendChild(el) {
      el.__appended = true;
    }
  };
  return {
    head,
    documentElement: head,
    createElement() {
      return {
        src: '',
        async: false,
        dataset: {},
        remove() {},
        __appended: false
      };
    }
  };
}

/**
 * 装好三个世界。
 * @param {(msg: object) => Promise<object>} swHandler 模拟 service worker
 */
function setup(swHandler) {
  const win = makeWindow();
  const doc = makeDocument();

  const sent = [];

  // 假的 chrome.runtime
  const chromeStub = {
    runtime: {
      getURL: (p) => 'chrome-extension://fake/' + p,
      sendMessage: (msg) => {
        sent.push(msg);
        return swHandler(msg);
      }
    }
  };

  // 主世界上下文
  const mainCtx = vm.createContext({ window: win, document: doc, console, Promise, setTimeout, clearTimeout });
  // 隔离世界上下文
  const isoCtx = vm.createContext({ window: win, document: doc, console, chrome: chromeStub, Promise, setTimeout, clearTimeout });

  // 先跑隔离世界（它会注入主世界脚本），再跑主世界
  vm.runInContext(read('content/host.js'), isoCtx, { filename: 'host.js' });
  vm.runInContext(read('content/page-bridge.js'), mainCtx, { filename: 'page-bridge.js' });

  return { win, sent, mainCtx };
}

test('页面能拿到 window.sqlext 接口', () => {
  const { win } = setup(async () => ({ ok: true, result: {} }));
  assert.ok(win.sqlext, 'window.sqlext 应该存在');
  assert.equal(typeof win.sqlext.parse, 'function');
  assert.equal(typeof win.sqlext.account, 'function');
});

test('重复注入不会覆盖已有接口', () => {
  const { win, mainCtx } = setup(async () => ({ ok: true, result: {} }));
  const first = win.sqlext;
  // 再跑一次主世界脚本
  vm.runInContext(read('content/page-bridge.js'), mainCtx, { filename: 'page-bridge.js' });
  assert.equal(win.sqlext, first, '第二次注入应该被挡住');
});

test('parse 请求能穿过隔离世界到达 service worker 并拿到回复', async () => {
  const received = [];
  const { win, sent } = setup(async (msg) => {
    received.push(msg);
    return {
      ok: true,
      result: {
        statement: { type: 'Select', details: { from: 'users' } },
        balance: 19,
        cost: 1
      }
    };
  });

  const r = await win.sqlext.parse('SELECT id FROM users');

  // 页面拿到的是 service worker 返回的 result
  assert.equal(r.statement.details.from, 'users');
  assert.equal(r.balance, 19);

  // service worker 收到的东西
  assert.equal(received.length, 1);
  assert.equal(received[0].kind, 'sqlext');
  assert.equal(received[0].type, 'parse');
  assert.equal(received[0].payload.sql, 'SELECT id FROM users');
  // 页面来源也带过去了，服务端要靠它做站点授权
  assert.equal(received[0].pageOrigin, 'https://example.com');
});

test('service worker 报错时页面拿到的是 Error', async () => {
  const { win } = setup(async () => ({
    ok: false,
    error: '当前网站未获授权',
    code: 'site_not_authorized'
  }));

  await assert.rejects(
    () => win.sqlext.parse('SELECT 1'),
    (err) => {
      assert.equal(err.message, '当前网站未获授权');
      assert.equal(err.code, 'site_not_authorized');
      return true;
    }
  );
});

test('service worker 抛异常时页面也能收到错误而不是挂死', async () => {
  const { win } = setup(async () => {
    throw new Error('service worker 崩了');
  });
  await assert.rejects(() => win.sqlext.parse('SELECT 1'), /service worker 崩了/);
});

test('并发多次调用各自拿到正确的回复', async () => {
  let n = 0;
  const { win } = setup(async () => {
    n += 1;
    const id = n;
    // 让第一个请求慢一点，验证请求 id 没有串
    await new Promise((r) => setTimeout(r, id === 1 ? 30 : 0));
    return { ok: true, result: { id } };
  });

  const [a, b, c] = await Promise.all([
    win.sqlext.parse('a'),
    win.sqlext.parse('b'),
    win.sqlext.parse('c')
  ]);
  assert.deepEqual([a.id, b.id, c.id], [1, 2, 3]);
});
