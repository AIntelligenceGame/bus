import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
vi.mock('../lib/api', () => ({ api: apiMock }));
vi.mock('../lib/apps', () => ({
  loadApps: vi.fn(async () => ({
    apps: [{ app_id: 'APP_IOS', name: 'iOS 客户端' }],
    unclassified: { order_count: 0, success_count: 0, amount: 0 },
  })),
}));
vi.mock('../lib/verify', () => ({ requireVerify: vi.fn(async () => 'vt') }));

import { renderApps } from './apps';

/** 找「订单必须绑定应用」开关发出去的请求体。 */
function policyBody(): Record<string, unknown> | undefined {
  const call = apiMock.mock.calls.find((c) => String(c[0]) === '/api/apps/require-binding');
  return call?.[1]?.body as Record<string, unknown> | undefined;
}

beforeEach(() => {
  document.body.innerHTML = '';
  apiMock.mockReset();
  apiMock.mockImplementation(async (path: string) => {
    if (path === '/api/me') {
      return { user: { user_id: 'u1', user_name: '商户甲', require_app_binding: false } };
    }
    if (path === '/api/apps') {
      return {
        apps: [{ app_id: 'APP_IOS', name: 'iOS 客户端', order_count: 0, amount: 0 }],
        unclassified: { order_count: 0, success_count: 0, amount: 0 },
      };
    }
    return {};
  });
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('应用管理页的「订单必须绑定应用」开关', () => {
  it('显示当前状态(未开启)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderApps(root);

    await vi.waitFor(() => {
      expect(root.textContent).toContain('未开启');
    });
    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '开启强制绑定') as HTMLButtonElement;
    expect(btn).toBeTruthy();
  });

  it('开启时先把请求发出去(require=true)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderApps(root);

    await vi.waitFor(() => {
      expect(root.textContent).toContain('未开启');
    });
    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '开启强制绑定') as HTMLButtonElement;
    btn.click();

    // 确认弹窗要先点「确定」。
    await vi.waitFor(() => {
      const ok = [...document.querySelectorAll('button')]
        .find((b) => b.textContent === '确认执行') as HTMLButtonElement | undefined;
      expect(ok).toBeTruthy();
      ok!.click();
    });

    await vi.waitFor(() => expect(policyBody()).toBeDefined());
    expect(policyBody()!.require).toBe(true);
  });

  it('已开启时按钮变成「关闭强制绑定」', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/me') {
        return { user: { user_id: 'u1', user_name: '商户甲', require_app_binding: true } };
      }
      if (path === '/api/apps') {
        return {
          apps: [{ app_id: 'APP_IOS', name: 'iOS 客户端', order_count: 0, amount: 0 }],
          unclassified: { order_count: 0, success_count: 0, amount: 0 },
        };
      }
      return {};
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderApps(root);

    await vi.waitFor(() => {
      expect(root.textContent).toContain('已开启');
    });
    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '关闭强制绑定') as HTMLButtonElement;
    expect(btn).toBeTruthy();
  });
});
