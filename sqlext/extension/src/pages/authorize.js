// authorize.js —— 用户确认授权某个网站。
//
// 这个页面由 Service Worker 打开（chrome.windows.create），
// 页面里的脚本请求授权当前站点——必须由用户点"允许"才生效。
import { send, msg, busy } from './ui.js';

const $ = (s) => document.querySelector(s);
const origin = new URLSearchParams(location.search).get('origin') || '';
$('#origin').textContent = origin;

$('#btn-allow').onclick = async (e) => {
  busy(e.target, true);
  const res = await send('addSite', { origin });
  busy(e.target, false);
  if (res && res.ok) {
    msg($('#msg'), '已允许，正在关闭…', 'ok');
    setTimeout(() => window.close(), 500);
  } else {
    msg($('#msg'), (res && res.error) || '授权失败', 'err');
  }
};

$('#btn-deny').onclick = () => window.close();
