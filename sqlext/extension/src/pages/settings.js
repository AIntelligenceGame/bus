// settings.js
import { send, msg, busy } from './ui.js';

const $ = (s) => document.querySelector(s);

async function load() {
  const base = await send('getApiBase');
  if (base && base.ok) {
    $('#apiBase').value = base.result.apiBase;
  }

  const me = await send('me');
  if (me && me.ok) {
    $('#email').textContent = me.result.user.email;
    $('#balance').textContent = me.result.balance + ' 积分';
  } else {
    $('#email').textContent = '未登录';
  }

  const sites = await send('listSites');
  const ul = $('#sites');
  ul.innerHTML = '';
  const list = sites && sites.ok ? sites.result.sites : [];
  if (!list.length) {
    ul.innerHTML = '<li class="hint">还没有授权任何网站</li>';
    return;
  }
  for (const origin of list) {
    const li = document.createElement('li');
    const span = document.createElement('span');
    span.textContent = origin;
    const btn = document.createElement('button');
    btn.className = 'ghost';
    btn.textContent = '移除';
    btn.onclick = async () => {
      const res = await send('removeSite', { origin });
      if (res && res.ok) {
        load();
      }
    };
    li.appendChild(span);
    li.appendChild(btn);
    ul.appendChild(li);
  }
}

/** 把服务端地址转成 Chrome 的主机匹配模式。 */
function toMatchPattern(base) {
  try {
    const u = new URL(base);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
      return null;
    }
    return u.protocol + '//' + u.hostname + (u.port ? ':' + u.port : '') + '/*';
  } catch (_) {
    return null;
  }
}

$('#btn-save').onclick = async (e) => {
  const v = $('#apiBase').value.trim();
  if (!v) {
    msg($('#msg'), '请填写服务端地址', 'err');
    return;
  }
  const pattern = toMatchPattern(v);
  if (!pattern) {
    msg($('#msg'), '服务端地址不合法（要以 http:// 或 https:// 开头）', 'err');
    return;
  }

  busy(e.target, true);
  msg($('#msg'), '');

  // 主机权限必须由扩展页面在用户手势里申请。
  // Service Worker 没有手势上下文，在那边调 chrome.permissions.request() 会静默失败。
  let granted = true;
  try {
    granted = await chrome.permissions.request({ origins: [pattern] });
  } catch (err) {
    msg($('#msg'), '申请权限失败：' + (err && err.message ? err.message : err), 'err');
    busy(e.target, false);
    return;
  }
  if (!granted) {
    msg($('#msg'), '未授予访问该服务端的权限', 'warn');
    busy(e.target, false);
    return;
  }

  const res = await send('setApiBase', { apiBase: v });
  busy(e.target, false);
  if (res && res.ok) {
    msg($('#msg'), '已保存', 'ok');
  } else {
    msg($('#msg'), (res && res.error) || '保存失败', 'err');
  }
};

$('#btn-logout').onclick = async (e) => {
  busy(e.target, true);
  await send('logout');
  load();
};

load();
