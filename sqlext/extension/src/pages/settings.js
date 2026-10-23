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

$('#btn-save').onclick = async (e) => {
  const v = $('#apiBase').value.trim();
  if (!v) {
    msg($('#msg'), '请填写服务端地址', 'err');
    return;
  }
  busy(e.target, true);
  msg($('#msg'), '');
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
