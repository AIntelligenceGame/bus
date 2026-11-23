/** 身份验证弹窗:口令 + (可选)验证码,换取敏感操作票据。 */

import { api, setVerifyToken, verifyTokenValid, getVerifyToken } from './api';
import type { SecurityResponse, VerifyResponse } from './types';
import { el } from './dom';
import { openModal } from './ui';

let pendingResolve: ((token: string) => void) | null = null;
let pendingReject: ((err: Error) => void) | null = null;
let inflight: Promise<string> | null = null;

/**
 * 取得敏感操作票据。
 *
 * force 为 true 时忽略已有票据,强制重新验证。
 */
export function requireVerify(force = false): Promise<string> {
  if (!force && verifyTokenValid()) {
    return Promise.resolve(getVerifyToken());
  }
  if (inflight) return inflight;
  inflight = new Promise<string>((resolve, reject) => {
    pendingResolve = resolve;
    pendingReject = reject;
  }).finally(() => {
    inflight = null;
  });
  openVerifyModal();
  return inflight;
}

function openVerifyModal(): void {
  const passwordInput = el('input', {
    type: 'password', id: 'vf-password', placeholder: '登录口令', autocomplete: 'current-password',
  });
  const codeInput = el('input', {
    type: 'text', id: 'vf-code', placeholder: '6 位验证码', autocomplete: 'one-time-code',
  });
  const sendBtn = el('button', { class: 'btn btn-secondary btn-sm', text: '获取验证码' }) as HTMLButtonElement;

  // 验证码区块默认隐藏:没开二次验证的账户不该看到一个填了也没用的框
  // (而且点「获取验证码」只会换来一句「尚未开启二次验证」)。
  const codeField = el('div', { class: 'field' },
    el('label', { class: 'label', text: '验证码', for: 'vf-code' }),
    el('div', { class: 'row' }, codeInput, sendBtn));
  codeField.style.display = 'none';

  const body = el('div', {},
    el('div', { class: 'field' },
      el('label', { class: 'label', text: '登录口令', for: 'vf-password' }),
      passwordInput),
    codeField);

  // 拉账户状态:开了二次验证才把验证码框显示出来。
  void api<SecurityResponse>('/api/account/security')
    .then((sec) => {
      if (!sec.mfa_enabled) return;
      // 绑定在、但验证码发不出去:那个码用户永远收不到,再要它就是死锁
      // (连通道配置都改不了)。后端此时也只校验口令,所以这里把验证码框收起来。
      if (sec.mfa_deliverable === false) {
        codeField.style.display = 'none';
        const hint = el('div', { class: 'hint-text',
          text: '当前二次验证的验证码发不出去(没有可用下发通道),本次验证只需口令;' +
            '建议到「账户安全」页关闭它或改用验证器 App。' });
        codeField.parentElement?.insertBefore(hint, codeField);
        return;
      }
      codeField.style.display = '';
      if (sec.mfa_type === 'totp') {
        // 验证器 App:动态码由 App 本地算出,没有「下发」这回事,
        // 也就没有「获取验证码」按钮 —— 点了只会换来一句「不需要下发验证码」。
        codeField.querySelector('label')!.textContent = '验证器 App 动态码';
        codeInput.placeholder = 'App 里的 6 位动态码';
        sendBtn.style.display = 'none';
        return;
      }
      codeInput.placeholder = '6 位验证码(' + (sec.mfa_type === 'sms' ? '短信' : '邮箱') + ')';
    })
    .catch(() => undefined);

  const handle = openModal({
    title: '身份验证',
    desc: '该操作涉及资金或密钥,需要验证身份。',
    body,
    okText: '验证并继续',
    cancelText: '取消',
    onOk: async (h) => {
      const password = passwordInput.value;
      const code = codeInput.value.trim();
      if (!password) {
        h.error('请输入登录口令');
        return;
      }
      handle.okButton.classList.add('is-busy');
      try {
        const res = await api<VerifyResponse>('/api/account/verify', {
          method: 'POST',
          body: { password, code },
        });
        setVerifyToken(res.verify_token, res.expires_in);
        h.close(true);
        if (pendingResolve) pendingResolve(res.verify_token);
        pendingResolve = null;
        pendingReject = null;
      } catch (err) {
        h.error(err instanceof Error ? err.message : String(err));
      } finally {
        handle.okButton.classList.remove('is-busy');
      }
    },
  });

  // 取消 / ESC / 点遮罩 → 拒绝等待中的调用方。
  const originalClose = handle.close;
  handle.close = (ok: boolean) => {
    if (!ok) {
      if (pendingReject) pendingReject(new Error('已取消验证'));
      pendingResolve = null;
      pendingReject = null;
    }
    originalClose(ok);
  };

  sendBtn.addEventListener('click', () => {
    void (async () => {
      sendBtn.classList.add('is-busy');
      try {
        // 目标地址由后端强制取「已绑定的地址」,这里不传 target。
        const res = await api<{ target?: string; message?: string }>('/api/account/mfa/code', {
          method: 'POST',
          body: { purpose: 'sensitive' },
        });
        handle.error('');
        codeInput.placeholder = '验证码已发送到 ' + (res.target || '已绑定地址');
        codeInput.focus();
      } catch (err) {
        handle.error(err instanceof Error ? err.message : String(err));
      } finally {
        sendBtn.classList.remove('is-busy');
      }
    })();
  });

  window.setTimeout(() => passwordInput.focus(), 30);
}
