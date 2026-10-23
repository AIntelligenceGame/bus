// ui.js —— 扩展内部页面的共用小工具。

/** 发送消息给 Service Worker。 */
export function send(type, payload = {}) {
  return chrome.runtime.sendMessage({ kind: 'sqlext-ui', type, payload });
}

/** 弹一条消息。kind: ok / err / warn */
export function msg(el, text, kind = '') {
  if (!el) {
    return;
  }
  el.textContent = text || '';
  el.className = 'msg' + (kind ? ' ' + kind : '');
}

/** 把按钮置为加载中。 */
export function busy(btn, on) {
  if (!btn) {
    return;
  }
  btn.disabled = !!on;
  btn.textContent = on ? '处理中…' : btn.dataset.label || btn.textContent;
}

/** 格式化时间。 */
export function fmtTime(ts) {
  if (!ts) {
    return '-';
  }
  const d = new Date(ts);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

/** 打开扩展内部页面。 */
export function openPage(page, params) {
  const url = chrome.runtime.getURL('src/pages/' + page);
  chrome.tabs.create({ url: params ? url + '?' + params : url });
}
