import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
vi.mock('../lib/api', () => ({ api: apiMock }));
vi.mock('../lib/apps', () => ({
  loadApps: vi.fn(async () => ({ apps: [], unclassified: { order_count: 0, success_count: 0, amount: 0 } })),
}));
vi.mock('../lib/store', () => ({
  CHANNEL_LABELS: { wechat: '微信支付', alipay: '支付宝', applepay: 'Apple Pay', googlepay: 'Google Pay' },
  refreshMe: vi.fn(async () => mePayload()),
}));
vi.mock('../lib/verify', () => ({ requireVerify: vi.fn(async () => 'vt') }));

import { renderChannels } from './channels';
import { loadApps } from '../lib/apps';

function mePayload() {
  return {
    user: { user_id: 'u1', user_name: '商户甲' },
    channels: [
      {
        channel: 'applepay',
        enabled: true,
        is_default: false,
        wallet: {
          merchant_id: 'apple.merchant',
          display_name: 'Apple 展示名',
          domain_name: 'apple.example.com',
          currency: 'CNY',
          stripe_secret_key: 'sk_apple',
          stripe_publishable_key: 'pk_apple',
          stripe_webhook_secret: 'whsec_apple',
        },
      },
      {
        channel: 'googlepay',
        enabled: true,
        is_default: false,
        wallet: {
          merchant_id: 'google.merchant',
          display_name: 'Google 展示名',
          domain_name: 'google.example.com',
          currency: 'USD',
          stripe_secret_key: 'sk_google',
          stripe_publishable_key: 'pk_google',
          stripe_webhook_secret: 'whsec_google',
        },
      },
    ],
    notify_urls: { wechat: 'https://pay.example.com/notify/wechat/u1', alipay: '' },
    default_channel: 'wechat',
  };
}

function saveBody(): Record<string, unknown> | undefined {
  const call = apiMock.mock.calls.find((c) => String(c[0]).startsWith('/api/channels/')
    && (c[1] as { method?: string } | undefined)?.method === 'POST');
  return call?.[1]?.body as Record<string, unknown> | undefined;
}

/** 找出「保存渠道」那次 POST 的 URL(带查询串)。 */
function saveURL(): string | undefined {
  const call = apiMock.mock.calls.find((c) => String(c[0]).startsWith('/api/channels/')
    && (c[1] as { method?: string } | undefined)?.method === 'POST');
  return call ? String(call[0]) : undefined;
}

beforeEach(() => {
  document.body.innerHTML = '';
  apiMock.mockReset();
  apiMock.mockImplementation(async (path: string) => {
    if (path === '/api/me') return mePayload();
    if (path.startsWith('/api/channels/')) return { ok: true };
    return {};
  });
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('钱包渠道配置', () => {
  it('Apple Pay 与 Google Pay 的表单各自回填各自渠道的配置', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);

    await vi.waitFor(() => {
      expect((document.getElementById('w-applepay-merchant') as HTMLInputElement).value)
        .toBe('apple.merchant');
    });
    expect((document.getElementById('w-googlepay-merchant') as HTMLInputElement).value)
      .toBe('google.merchant');
    expect((document.getElementById('w-googlepay-currency') as HTMLInputElement).value)
      .toBe('USD');
  });

  it('保存 Apple Pay 不会把 Google Pay 的商户号写过去', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);
    await vi.waitFor(() => {
      expect((document.getElementById('w-applepay-merchant') as HTMLInputElement).value)
        .toBe('apple.merchant');
    });

    const appleBtn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '保存 Apple Pay 配置') as HTMLButtonElement;
    appleBtn.click();
    await vi.waitFor(() => expect(saveBody()).toBeDefined());

    const body = saveBody()!;
    const wallet = body.wallet as Record<string, string>;
    expect(wallet.merchant_id).toBe('apple.merchant');
    // 关键:不能出现 Google Pay 的值
    expect(JSON.stringify(body)).not.toContain('google.merchant');
    expect(JSON.stringify(body)).not.toContain('pk_google');
  });

  it('保存 Google Pay 不会把 Apple Pay 的商户号写过去', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);
    await vi.waitFor(() => {
      expect((document.getElementById('w-googlepay-merchant') as HTMLInputElement).value)
        .toBe('google.merchant');
    });

    const googleBtn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '保存 Google Pay 配置') as HTMLButtonElement;
    googleBtn.click();
    await vi.waitFor(() => expect(saveBody()).toBeDefined());

    const body = saveBody()!;
    const wallet = body.wallet as Record<string, string>;
    expect(wallet.merchant_id).toBe('google.merchant');
    expect(JSON.stringify(body)).not.toContain('apple.merchant');
  });

  it('两个钱包的字段 id 不相同(没有共用一份表单)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);
    const apple = document.getElementById('w-applepay-merchant');
    const google = document.getElementById('w-googlepay-merchant');
    expect(apple).not.toBeNull();
    expect(google).not.toBeNull();
    expect(apple!.id).not.toBe(google!.id);
  });
});

/**
 * 应用范围(用户原话:「我看只有微信和支付宝,别的支付,好像没有设置应用指定?」)。
 *
 * 钱包渠道必须和微信/支付宝一样:在「配置范围」里选了应用之后,
 * 保存请求要带上 app_id —— 不带就是静默存成全局配置,用户根本发现不了。
 */
describe('应用范围下的渠道保存', () => {
  beforeEach(() => {
    vi.mocked(loadApps).mockResolvedValue({
      apps: [{ app_id: 'APP_IOS', name: 'iOS 客户端' }],
      unclassified: { order_count: 0, success_count: 0, amount: 0 },
    } as never);
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/me') return mePayload();
      if (path.startsWith('/api/apps/') && path.endsWith('/channels')) {
        return { app: { app_id: 'APP_IOS' }, channels: [], notify_urls: {} };
      }
      if (path.startsWith('/api/channels/')) return { ok: true };
      return {};
    });
  });

  it('选中应用后保存 Apple Pay,请求要带上 app_id', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);

    const sel = document.getElementById('ch-scope') as HTMLSelectElement;
    await vi.waitFor(() => {
      expect([...sel.options].some((o) => o.value === 'APP_IOS')).toBe(true);
    });
    sel.value = 'APP_IOS';
    sel.dispatchEvent(new Event('change'));

    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '保存 Apple Pay 配置') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(saveURL()).toBeDefined());
    expect(saveURL()).toContain('/api/channels/applepay');
    expect(saveURL()).toContain('app_id=APP_IOS');
  });

  it('选中应用后保存 Google Pay,请求要带上 app_id', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);

    const sel = document.getElementById('ch-scope') as HTMLSelectElement;
    await vi.waitFor(() => {
      expect([...sel.options].some((o) => o.value === 'APP_IOS')).toBe(true);
    });
    sel.value = 'APP_IOS';
    sel.dispatchEvent(new Event('change'));

    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '保存 Google Pay 配置') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(saveURL()).toBeDefined());
    expect(saveURL()).toContain('/api/channels/googlepay');
    expect(saveURL()).toContain('app_id=APP_IOS');
  });

  it('不选应用(全局)时,请求里不能有 app_id', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderChannels(root);

    await vi.waitFor(() => {
      expect((document.getElementById('w-applepay-merchant') as HTMLInputElement).value)
        .toBe('apple.merchant');
    });
    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '保存 Apple Pay 配置') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(saveURL()).toBeDefined());
    expect(saveURL()).not.toContain('app_id');
  });
});
