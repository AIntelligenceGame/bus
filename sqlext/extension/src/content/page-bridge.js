// page-bridge.js —— 注入到**页面主世界**（MAIN world）的脚本。
//
// 它是网页开发者唯一能看到的接口：
//
//     const r = await window.sqlext.parse('SELECT id FROM t');
//     console.log(r.statement.details.from);   // "t"
//
// 为什么在主世界：内容脚本（content script）跑在隔离世界，
// 页面里的 JS 拿不到它的变量。只能由主世界脚本把接口挂到 window 上，
// 再用 window.postMessage 和隔离世界的内容脚本通信。
//
// 安全边界（重要）：
//   - 这个接口**只做转发**，真正的鉴权、扣费、解析都在服务器端
//   - 登录令牌存在扩展的 chrome.storage.local 里，页面永远拿不到
//   - 页面能拿到的只有"解析结果"和"错误信息"

(function () {
  'use strict';

  var NS = '__sqlext_bridge__';
  var API_NAME = 'sqlext';

  // 防止同一页面重复注入
  if (window[NS]) {
    return;
  }
  window[NS] = true;

  var seq = 0;
  var pending = new Map();

  // 接收隔离世界的回复
  window.addEventListener('message', function (e) {
    if (e.source !== window) {
      return;
    }
    var d = e.data;
    if (!d || d.__sqlext !== 'reply') {
      return;
    }
    var p = pending.get(d.id);
    if (!p) {
      return;
    }
    pending.delete(d.id);
    clearTimeout(p.timer);
    if (d.error) {
      var err = new Error(d.error);
      err.code = d.code || '';
      p.reject(err);
    } else {
      p.resolve(d.result);
    }
  });

  function call(type, payload) {
    return new Promise(function (resolve, reject) {
      seq += 1;
      var id = NS + ':' + seq;
      var timer = setTimeout(function () {
        if (pending.delete(id)) {
          reject(new Error('请求超时'));
        }
      }, 30000);
      pending.set(id, { resolve: resolve, reject: reject, timer: timer });
      window.postMessage({ __sqlext: 'req', id: id, type: type, payload: payload }, '*');
    });
  }

  /**
   * window.sqlext —— 页面可用的接口。
   */
  window[API_NAME] = {
    version: '0.1.0',

    /**
     * 解析一条 SQL。
     * @param {string} sql SQL 文本
     * @param {{origin?: string}} [options]
     * @returns {Promise<{statement: object, balance: number, cost: number, duration_ms: number}>}
     */
    parse: function (sql, options) {
      options = options || {};
      return call('parse', { sql: String(sql == null ? '' : sql), origin: options.origin || '' });
    },

    /**
     * 取当前登录账号与余额。
     * @returns {Promise<{user: object, balance: number}>}
     */
    account: function () {
      return call('account', {});
    },

    /**
     * 请求把当前网站加入允许名单。
     * 会弹出扩展的授权页，需要用户点确认。
     * @returns {Promise<{ok: boolean, origin: string}>}
     */
    requestSite: function () {
      return call('requestSite', {});
    },

    /**
     * 列出已授权的网站。
     * @returns {Promise<{sites: string[]}>}
     */
    listSites: function () {
      return call('listSites', {});
    }
  };

  // 通知隔离世界：主世界接口就绪
  window.postMessage({ __sqlext: 'ready' }, '*');
})();
