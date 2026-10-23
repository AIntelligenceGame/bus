// register.js
import { send, msg, busy } from './ui.js';

const $ = (s) => document.querySelector(s);

$('#btn').onclick = async (e) => {
  const email = $('#email').value.trim();
  const password = $('#password').value;
  const password2 = $('#password2').value;
  if (!email || !password) {
    msg($('#msg'), '请填写邮箱和口令', 'err');
    return;
  }
  if (password.length < 8) {
    msg($('#msg'), '口令至少 8 位', 'err');
    return;
  }
  if (password !== password2) {
    msg($('#msg'), '两次输入的口令不一致', 'err');
    return;
  }
  busy(e.target, true);
  msg($('#msg'), '');
  const res = await send('register', { email, password });
  busy(e.target, false);
  if (res && res.ok) {
    msg($('#msg'), '注册成功，正在跳转…', 'ok');
    setTimeout(() => window.close(), 600);
  } else {
    msg($('#msg'), (res && res.error) || '注册失败', 'err');
  }
};
