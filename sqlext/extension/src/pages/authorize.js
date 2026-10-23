// authorize.js —— 用户确认授权某个网站。
//
// 这个页面由 Service Worker 打开（chrome.windows.create），
// 页面里的脚本请求授权当前站点——必须由用户点"允许"才生效。
//
// 关键：**主机权限必须在这里（扩展页面）申请，不能在 Service Worker 里申请**。
// chrome.permissions.request() 依赖用户手势，Service Worker 没有手势上下文，
// 调用会静默失败——表现是"授权成功了，但那个网站上没有 window.sqlext"。
import { send, msg, busy } from './ui.js';

const $ = (s) => document.querySelector(s);
const origin = new URLSearchParams(location.search).get('origin') || '';
$('#origin').textContent = origin;

/** 把 origin 转成 Chrome 的主机匹配模式。 */
function toMatchPattern(o) {
  try {
    const u = new URL(o);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
      return null;
    }
    return u.protocol + '//' + u.hostname + (u.port ? ':' + u.port : '') + '/*';
  } catch (_) {
    return null;
  }
}

$('#btn-allow').onclick = async (e) => {
  busy(e.target, true);

  // 第一步：申请主机权限。
  // chrome.permissions.request() 必须直接由用户手势触发，
  // 所以它要排在第一个 await 之前——busy() 是同步的，没问题。
  const pattern = toMatchPattern(origin);
  if (!pattern) {
    msg($('#msg'), '网站地址不合法', 'err');
    busy(e.target, false);
    return;
  }

  let granted = false;
  try {
    granted = await chrome.permissions.request({ origins: [pattern] });
  } catch (err) {
    msg($('#msg'), '申请权限失败：' + (err && err.message ? err.message : err), 'err');
    busy(e.target, false);
    return;
  }
  if (!granted) {
    msg($('#msg'), '未授予该网站的主机权限，授权已取消', 'warn');
    busy(e.target, false);
    return;
  }

  // 第二步：登记到服务端（跨设备同步用），并注册内容脚本。
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
