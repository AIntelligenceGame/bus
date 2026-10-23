// popup.js —— 点击插件图标弹出的面板。

import { send, msg, busy, openPage } from './ui.js';

const $ = (s) => document.querySelector(s);

async function init() {
  const res = await send('me');
  if (!res || !res.ok) {
    // 没登录（或服务端连不上）
    $('#logged-out').style.display = '';
    $('#msg').className = 'msg warn';
    $('#msg').textContent = res && res.error ? res.error : '';
    return;
  }
  $('#logged-in').style.display = '';
  $('#user-email').textContent = res.result.user.email;

  const bal = await send('balance');
  if (bal && bal.ok) {
    $('#balance').textContent = bal.result.balance;
    $('#cost-hint').textContent = `每次解析消耗 ${bal.result.cost_per_parse} 积分`;
  }

  const sites = await send('listSites');
  const list = sites && sites.ok ? sites.result.sites : [];
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const origin = tab ? originOf(tab.url) : '';
  $('#site-origin').textContent = origin || '（无法识别的页面）';
  const allowed = origin && list.some((s) => sameHost(s, origin));
  $('#site-state').textContent = allowed
    ? '✓ 已授权，页面可以调用 window.sqlext.parse(...)'
    : '✗ 未授权，页面调用会返回 site_not_authorized';
  $('#btn-authorize').textContent = allowed ? '已授权（点击重新确认）' : '授权当前网站';
}

function originOf(url) {
  try {
    const u = new URL(url);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
      return '';
    }
    return u.protocol + '//' + u.host;
  } catch (_) {
    return '';
  }
}

function sameHost(a, b) {
  try {
    return new URL(a).host === new URL(b).host;
  } catch (_) {
    return a === b;
  }
}

$('#btn-login').onclick = () => openPage('login.html');
$('#btn-register').onclick = () => openPage('register.html');
$('#btn-recharge').onclick = () => openPage('recharge.html');
$('#btn-settings').onclick = () => openPage('settings.html');

$('#btn-authorize').onclick = async (e) => {
  const btn = e.target;
  busy(btn, true);
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const origin = originOf(tab ? tab.url : '');
  if (!origin) {
    msg($('#msg'), '当前页面不是 http/https，无法授权', 'err');
    busy(btn, false);
    return;
  }
  const res = await send('addSite', { origin });
  busy(btn, false);
  if (res && res.ok) {
    msg($('#msg'), '已授权：' + origin, 'ok');
    init();
  } else {
    msg($('#msg'), (res && res.error) || '授权失败', 'err');
  }
};

$('#btn-logout').onclick = async (e) => {
  busy(e.target, true);
  await send('logout');
  window.location.reload();
};

init();
