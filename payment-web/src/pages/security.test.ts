import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
const blobMock = vi.hoisted(() => vi.fn(async () => 'blob:fake-qr'));
const releaseMock = vi.hoisted(() => vi.fn());

vi.mock('../lib/api', () => ({
  api: apiMock,
  apiBlobURL: blobMock,
  releaseBlobURL: releaseMock,
}));
vi.mock('../lib/verify', () => ({ requireVerify: vi.fn(async () => 'vt') }));

import { renderSecurity } from './security';

/** 账户安全状态的可变副本。 */
function securityPayload(over: Record<string, unknown> = {}) {
  return {
    user_id: 'u1',
    name: '商户甲',
    password_set: true,
    mfa_enabled: false,
    mfa_type: '',
    mfa_target: '',
    verify_ttl_sec: 300,
    email_channel_configured: true,
    sms_channel_configured: false,
    mfa_deliverable: true,
    mfa_backup: false,
    ...over,
  };
}

function channelsPayload() {
  return {
    channels: {
      email: {
        configured: true,
        host: 'smtp.qq.com',
        port: 587,
        username: 'merchant@qq.com',
        from_addr: '',
        tls_mode: 'starttls',
        password_set: true,
      },
      sms: { configured: false, url: '', code_field: 'code', target_field: 'target' },
    },
  };
}

/** 找到文案匹配的按钮。 */
function button(text: string): HTMLButtonElement {
  const btn = [...document.querySelectorAll('button')].find((b) => b.textContent === text);
  if (!btn) throw new Error('找不到按钮: ' + text);
  return btn as HTMLButtonElement;
}

/** 找到某个路径的调用记录(可选只看某个方法)。 */
function calls(path: string, method?: string): { path: string; opts: any }[] {
  return apiMock.mock.calls
    .filter((c) => String(c[0]).startsWith(path))
    .filter((c) => !method || (c[1] as any)?.method === method)
    .map((c) => ({ path: String(c[0]), opts: c[1] as any }));
}

/** 节点(或它的祖先)是否被 display:none 藏起来了。 */
function isHidden(node: Element | null): boolean {
  let cur: Element | null = node;
  while (cur) {
    if ((cur as HTMLElement).style?.display === 'none') return true;
    cur = cur.parentElement;
  }
  return false;
}

function setInput(id: string, value: string): void {
  const node = document.getElementById(id) as HTMLInputElement | null;
  if (!node) throw new Error('找不到输入框: ' + id);
  node.value = value;
}

beforeEach(() => {
  document.body.innerHTML = '';
  apiMock.mockReset();
  blobMock.mockReset();
  blobMock.mockImplementation(async () => 'blob:fake-qr');
  releaseMock.mockReset();
  apiMock.mockImplementation(async (path: string) => {
    if (path === '/api/account/security') return securityPayload();
    if (path === '/api/account/mfa/channels') return channelsPayload();
    if (path.startsWith('/api/audit-logs')) return { logs: [], page: 1, total: 0, pages: 0 };
    if (path === '/api/account/mfa/totp/setup') {
      return { secret: 'JBSWY3DPEHPK3PXP', qrcode_url: '/api/account/mfa/totp/qrcode' };
    }
    return { ok: true };
  });
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('二次验证页面的状态渲染', () => {
  it('没开二次验证时默认选「关闭」,并且不给「关闭」按钮', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('off');
    });
    // 没开的时候「关闭二次验证」那一套要藏起来(留着只是摆设)。
    const titles = [...document.querySelectorAll('.section-title')].find((n) => n.textContent === '关闭二次验证');
    expect(titles).toBeDefined();
    expect(isHidden(titles!)).toBe(true);
    expect(isHidden(button('关闭二次验证'))).toBe(true);
  });

  it('已经开了邮箱方式时,下拉框选中它,「关闭」区出现', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/account/security') {
        return securityPayload({ mfa_enabled: true, mfa_type: 'email', mfa_target: 'a@qq.com' });
      }
      if (path === '/api/account/mfa/channels') return channelsPayload();
      if (path.startsWith('/api/audit-logs')) return { logs: [], page: 1, total: 0, pages: 0 };
      return { ok: true };
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('email');
    });
    const texts = [...document.querySelectorAll('.section-title')].map((n) => n.textContent);
    expect(texts).toContain('关闭二次验证');
    expect(isHidden(button('关闭二次验证'))).toBe(false);
  });
});

describe('TOTP(验证器 App)绑定流程', () => {
  it('选到「验证器 App」时自动生成密钥(不带当前绑定确认)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('off');
    });
    const sel = document.getElementById('mfa-type') as HTMLSelectElement;
    sel.value = 'totp';
    sel.dispatchEvent(new Event('change'));

    await vi.waitFor(() => expect(calls('/api/account/mfa/totp/setup').length).toBe(1));
    const body = calls('/api/account/mfa/totp/setup')[0].opts.body;
    // 没开过二次验证 → 不需要「当前绑定的人」确认。
    expect(body.current_code).toBeUndefined();
  });

  it('换绑(已开启)时先弹窗确认当前绑定,确认后才发新的密钥', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/account/security') {
        return securityPayload({ mfa_enabled: true, mfa_type: 'totp', mfa_target: '' });
      }
      if (path === '/api/account/mfa/channels') return channelsPayload();
      if (path.startsWith('/api/audit-logs')) return { logs: [], page: 1, total: 0, pages: 0 };
      if (path === '/api/account/mfa/totp/setup') {
        return { secret: 'JBSWY3DPEHPK3PXP', qrcode_url: '/api/account/mfa/totp/qrcode' };
      }
      return { ok: true };
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('totp');
    });
    button('重新生成密钥').click();

    // 弹窗出现,确认之前不能发密钥请求。
    await vi.waitFor(() => expect(document.getElementById('mfa-current-code')).not.toBeNull());
    expect(calls('/api/account/mfa/totp/setup').length).toBe(0);

    setInput('mfa-current-code', '123456');
    const ok = document.querySelector('.modal-mask .btn-primary') as HTMLButtonElement;
    ok.click();

    await vi.waitFor(() => expect(calls('/api/account/mfa/totp/setup').length).toBe(1));
    expect(calls('/api/account/mfa/totp/setup')[0].opts.body.current_code).toBe('123456');
  });

  it('在弹窗里取消 → 不会发出新的密钥请求', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/account/security') {
        return securityPayload({ mfa_enabled: true, mfa_type: 'totp', mfa_target: '' });
      }
      if (path === '/api/account/mfa/channels') return channelsPayload();
      if (path.startsWith('/api/audit-logs')) return { logs: [], page: 1, total: 0, pages: 0 };
      return { ok: true };
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('totp');
    });
    button('重新生成密钥').click();
    await vi.waitFor(() => expect(document.getElementById('mfa-current-code')).not.toBeNull());

    const cancel = [...document.querySelectorAll('.modal-mask button')]
      .find((b) => b.textContent === '取消') as HTMLButtonElement;
    cancel.click();

    await new Promise((r) => setTimeout(r, 10));
    expect(calls('/api/account/mfa/totp/setup').length).toBe(0);
  });

  it('绑定成功后清掉页面上残留的密钥与二维码(密钥已生效,不能再「保存」一次)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('off');
    });
    const sel = document.getElementById('mfa-type') as HTMLSelectElement;
    sel.value = 'totp';
    sel.dispatchEvent(new Event('change'));
    await vi.waitFor(() => expect(calls('/api/account/mfa/totp/setup').length).toBe(1));

    setInput('mfa-totp-code', '123456');
    setInput('mfa-bind-password', 'secret-pass');
    button('开启二次验证').click();

    await vi.waitFor(() => expect(calls('/api/account/mfa/enable').length).toBe(1));
    const body = calls('/api/account/mfa/enable')[0].opts.body;
    expect(body.type).toBe('totp');
    expect(body.code).toBe('123456');

    // 密钥生效后,页面不该再留着旧的密钥/二维码:
    // 服务端已清掉「待确认密钥」,留着只会让人以为还能再扫一次。
    expect((document.getElementById('mfa-totp-secret') as HTMLElement).textContent).toBe('');
    const qr = document.querySelector('img[alt="TOTP 二维码"]') as HTMLImageElement;
    expect(qr.style.display).toBe('none');
  });
});

describe('邮箱 / 短信方式的绑定与关闭', () => {
  it('开启邮箱方式:带上接收地址、口令与绑定验证码', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('off');
    });
    const sel = document.getElementById('mfa-type') as HTMLSelectElement;
    sel.value = 'email';
    sel.dispatchEvent(new Event('change'));
    setInput('mfa-target', 'a@qq.com');
    setInput('mfa-bind-password', 'secret-pass');
    setInput('mfa-bind-code', '654321');

    button('开启二次验证').click();
    await vi.waitFor(() => expect(calls('/api/account/mfa/enable').length).toBe(1));
    const body = calls('/api/account/mfa/enable')[0].opts.body;
    expect(body.type).toBe('email');
    expect(body.target).toBe('a@qq.com');
    expect(body.password).toBe('secret-pass');
    expect(body.code).toBe('654321');
  });

  it('关闭二次验证要口令 + 验证码', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/account/security') {
        return securityPayload({ mfa_enabled: true, mfa_type: 'email', mfa_target: 'a@qq.com' });
      }
      if (path === '/api/account/mfa/channels') return channelsPayload();
      if (path.startsWith('/api/audit-logs')) return { logs: [], page: 1, total: 0, pages: 0 };
      return { ok: true };
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('mfa-type') as HTMLSelectElement).value).toBe('email');
    });
    setInput('mfa-disable-password', 'secret-pass');
    setInput('mfa-disable-code', '123123');

    button('关闭二次验证').click();
    await vi.waitFor(() => expect(calls('/api/account/mfa/disable').length).toBe(1));
    const body = calls('/api/account/mfa/disable')[0].opts.body;
    expect(body.password).toBe('secret-pass');
    expect(body.code).toBe('123123');
  });
});

describe('验证码下发通道', () => {
  it('已保存的授权码永不回显:输入框清空,占位符说明「留空表示不修改」', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('ch-mail-host') as HTMLInputElement).value).toBe('smtp.qq.com');
    });
    const pass = document.getElementById('ch-mail-pass') as HTMLInputElement;
    expect(pass.value).toBe('');
    expect(pass.placeholder).toContain('已保存');
  });

  it('保存邮件通道:请求里带上主机、端口与授权码', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => {
      expect((document.getElementById('ch-mail-host') as HTMLInputElement).value).toBe('smtp.qq.com');
    });
    setInput('ch-mail-user', 'merchant@qq.com');
    setInput('ch-mail-pass', 'new-auth-code');

    button('保存邮件通道').click();
    await vi.waitFor(() => expect(calls('/api/account/mfa/channels', 'POST').length).toBe(1));
    const body = calls('/api/account/mfa/channels', 'POST')[0].opts.body;
    expect(body.channel).toBe('email');
    const config = body.config as Record<string, unknown>;
    expect(config.host).toBe('smtp.qq.com');
    expect(config.port).toBe(587);
    expect(config.username).toBe('merchant@qq.com');
    expect(config.password).toBe('new-auth-code');
    expect(config.tls_mode).toBe('starttls');
  });
});

describe('审计日志', () => {
  it('按动作过滤时把动作带进查询串', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderSecurity(root);

    await vi.waitFor(() => expect(calls('/api/audit-logs').length).toBeGreaterThan(0));
    const sel = document.getElementById('log-action') as HTMLSelectElement;
    sel.value = 'account.login';
    sel.dispatchEvent(new Event('change'));

    await vi.waitFor(() => {
      expect(calls('/api/audit-logs').some((c) => c.path.includes('action=account.login'))).toBe(true);
    });
  });
});
