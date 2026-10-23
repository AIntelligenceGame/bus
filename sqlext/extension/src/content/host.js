// host.js —— 内容脚本，跑在**隔离世界**（ISOLATED world）。
//
// 它干两件事：
//  1. 把 page-bridge.js 注入主世界，让页面能拿到 window.sqlext
//  2. 在 window.postMessage 和 chrome.runtime.sendMessage 之间做转发
//
// 为什么不让内容脚本直接用 chrome.*：隔离世界才有 chrome.* 权限，
// 主世界没有。两者只能靠 window.postMessage 通信（两个世界共享同一个 window）。

(function () {
  'use strict';

  // ---- 1) 注入主世界脚本 ------------------------------------------------
  try {
    var s = document.createElement('script');
    s.src = chrome.runtime.getURL('src/content/page-bridge.js');
    s.async = false;
    s.dataset.sqlext = '1';
    (document.head || document.documentElement).appendChild(s);
    s.remove();
  } catch (e) {
    // CSP 严格的页面会拦住注入——此时页面拿不到接口，属于预期情况
    console.warn('[sqlext] 注入主世界脚本失败：', e);
  }

  // ---- 2) 转发消息 ------------------------------------------------------
  window.addEventListener('message', function (e) {
    // 只接受来自本窗口的消息
    if (e.source !== window) {
      return;
    }
    var d = e.data;
    if (!d || d.__sqlext !== 'req') {
      return;
    }

    chrome.runtime
      .sendMessage({
        kind: 'sqlext',
        type: d.type,
        payload: d.payload || {},
        pageOrigin: window.location.origin,
        pageHref: window.location.href
      })
      .then(function (res) {
        var out = { __sqlext: 'reply', id: d.id };
        if (res && res.ok) {
          out.result = res.result;
        } else {
          out.error = (res && res.error) || '扩展无响应';
          out.code = (res && res.code) || '';
        }
        window.postMessage(out, '*');
      })
      .catch(function (err) {
        window.postMessage(
          { __sqlext: 'reply', id: d.id, error: String((err && err.message) || err) },
          '*'
        );
      });
  });
})();
