// login.js
import { send, msg, busy } from './ui.js';

const $ = (s) => document.querySelector(s);

$('#btn').onclick = async (e) => {
  const email = $('#email').value.trim();
  const password = $('#password').value;
  if (!email || !password) {
    msg($('#msg'), '请填写邮箱和口令', 'err');
    return;
  }
  busy(e.target, true);
  msg($('#msg'), '');
  const res = await send('login', { email, password });
  busy(e.target, false);
  if (res && res.ok) {
    msg($('#msg'), '登录成功，正在跳转…', 'ok');
    setTimeout(() => window.close(), 600);
  } else {
    msg($('#msg'), (res && res.error) || '登录失败', 'err');
  }
};

document.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    $('#btn').click();
  }
});
