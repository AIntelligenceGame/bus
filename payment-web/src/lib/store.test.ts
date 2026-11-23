import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
vi.mock('./api', () => ({
  api: apiMock,
  clearToken: vi.fn(() => localStorage.removeItem('pay_token')),
  clearVerifyToken: vi.fn(() => {
    localStorage.removeItem('pay_verify_token');
    localStorage.removeItem('pay_verify_exp');
  }),
  getToken: vi.fn(() => localStorage.getItem('pay_token') || ''),
  setToken: vi.fn((t: string) => localStorage.setItem('pay_token', t)),
}));

// apps.ts 用的是真的实现(不 mock),才能验证「退出登录后缓存真的被清掉」。
import { cachedApps, loadApps } from './apps';
import { login, logout, refreshMe, state } from './store';

function appsPayload(names: string[]) {
  return {
    apps: names.map((n, i) => ({ app_id: `app${i}`, name: n })),
    unclassified: { order_count: 0, success_count: 0, amount: 0 },
  };
}

function mePayload(userID: string, name: string) {
  return {
    user: { user_id: userID, user_name: name },
    channels: [],
    notify_urls: {},
    default_channel: '',
  };
}

beforeEach(() => {
  localStorage.clear();
  apiMock.mockReset();
  state.logged = false;
  state.userId = '';
  state.userName = '';
  state.channels = [];
});

afterEach(() => {
  localStorage.clear();
});

describe('login', () => {
  it('登录后保存令牌并刷新用户信息', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/login') return { token: 'tok-1' };
      if (path === '/api/me') return mePayload('u1', '商户甲');
      return {};
    });
    await login('商户甲', 'pw', '');
    expect(localStorage.getItem('pay_token')).toBe('tok-1');
    expect(state.logged).toBe(true);
    expect(state.userId).toBe('u1');
    expect(state.userName).toBe('商户甲');
  });

  it('换账号登录要清掉上一个账号的应用列表缓存', async () => {
    // 先用账号 A 的数据把缓存填上。
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/apps') return appsPayload(['A 的应用']);
      if (path === '/api/me') return mePayload('u1', '商户A');
      return {};
    });
    await loadApps();
    expect(cachedApps().map((a) => a.name)).toEqual(['A 的应用']);

    // 账号 B 登录。
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/login') return { token: 'tok-2' };
      if (path === '/api/me') return mePayload('u2', '商户B');
      if (path === '/api/apps') return appsPayload(['B 的应用']);
      return {};
    });
    await login('商户B', 'pw', '');
    expect(cachedApps()).toEqual([]);
  });
});

describe('logout', () => {
  it('清掉令牌与登录状态', async () => {
    localStorage.setItem('pay_token', 'tok-1');
    apiMock.mockResolvedValue({ ok: true });
    await logout();
    expect(localStorage.getItem('pay_token')).toBeNull();
    expect(state.logged).toBe(false);
    expect(state.userId).toBe('');
    expect(state.userName).toBe('');
  });

  it('后端失败也要清掉本地状态', async () => {
    localStorage.setItem('pay_token', 'tok-1');
    apiMock.mockRejectedValue(new Error('网络断了'));
    await logout();
    expect(localStorage.getItem('pay_token')).toBeNull();
    expect(state.logged).toBe(false);
  });

  it('清掉敏感操作票据', async () => {
    // 票据属于上一个账号;留着的话下一个登录的用户会带着上一张票据去请求。
    localStorage.setItem('pay_token', 'tok-1');
    localStorage.setItem('pay_verify_token', 'vt-1');
    localStorage.setItem('pay_verify_exp', String(Date.now() + 60_000));
    apiMock.mockResolvedValue({ ok: true });
    await logout();
    expect(localStorage.getItem('pay_verify_token')).toBeNull();
    expect(localStorage.getItem('pay_verify_exp')).toBeNull();
  });

  it('清掉应用列表缓存,避免下一个账号看到上一个账号的应用名', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/apps') return appsPayload(['A 的应用']);
      return {};
    });
    await loadApps();
    expect(cachedApps().length).toBe(1);

    apiMock.mockResolvedValue({ ok: true });
    await logout();
    expect(cachedApps()).toEqual([]);
  });
});

describe('refreshMe', () => {
  it('令牌换了人(直接换账号)时清掉旧的应用缓存', async () => {
    // 没走 logout 就换号(比如令牌过期后重新登录了另一个账号)。
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/apps') return appsPayload(['A 的应用']);
      if (path === '/api/me') return mePayload('u1', '商户A');
      return {};
    });
    await refreshMe();
    await loadApps();
    expect(cachedApps().length).toBe(1);

    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/me') return mePayload('u2', '商户B');
      return {};
    });
    await refreshMe();
    expect(cachedApps()).toEqual([]);
  });

  it('同一个账号刷新不清缓存', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/apps') return appsPayload(['我的应用']);
      if (path === '/api/me') return mePayload('u1', '商户A');
      return {};
    });
    await refreshMe();
    await loadApps();
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/me') return mePayload('u1', '商户A');
      return {};
    });
    await refreshMe();
    expect(cachedApps().length).toBe(1);
  });
});
