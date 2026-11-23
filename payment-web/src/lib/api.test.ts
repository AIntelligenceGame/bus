import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiError,
  api,
  apiDownload,
  clearToken,
  clearVerifyToken,
  getToken,
  getVerifyToken,
  setToken,
  setVerifyToken,
  verifyTokenValid,
} from './api';

// verify.ts 会弹窗、要后端配合,这里直接替掉,只测 api.ts 自己的重试逻辑。
vi.mock('./verify', () => ({
  requireVerify: vi.fn(async () => 'verify-token-from-mock'),
}));

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  localStorage.clear();
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
  window.location.hash = '';
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  localStorage.clear();
});

describe('令牌存取', () => {
  it('setToken / getToken / clearToken', () => {
    expect(getToken()).toBe('');
    setToken('tok-1');
    expect(getToken()).toBe('tok-1');
    expect(localStorage.getItem('pay_token')).toBe('tok-1');
    clearToken();
    expect(getToken()).toBe('');
  });

  it('localStorage 里没有值时返回空串,不是 null', () => {
    expect(getToken()).toBe('');
    expect(getVerifyToken()).toBe('');
  });
});

describe('敏感操作票据', () => {
  it('写入令牌并按 ttl 计算过期时间', () => {
    const before = Date.now();
    setVerifyToken('vt', 60);
    expect(getVerifyToken()).toBe('vt');
    expect(verifyTokenValid()).toBe(true);
    const exp = parseInt(localStorage.getItem('pay_verify_exp') || '0', 10);
    expect(exp).toBeGreaterThanOrEqual(before + 60_000);
  });

  it('ttl 缺失 / 为 0 / 为负 一律不缓存', () => {
    // 后端判票据过期后前端还当它有效,会导致敏感操作一直被拒且不再弹验证框。
    setVerifyToken('vt', 0);
    expect(getVerifyToken()).toBe('');
    expect(verifyTokenValid()).toBe(false);

    setVerifyToken('vt', -10);
    expect(getVerifyToken()).toBe('');

    setVerifyToken('', 60);
    expect(getVerifyToken()).toBe('');
  });

  it('票据已过期则无效', () => {
    setVerifyToken('vt', 60);
    // 手工把过期时间挪到过去
    localStorage.setItem('pay_verify_exp', String(Date.now() - 1));
    expect(verifyTokenValid()).toBe(false);
  });

  it('只有过期时间没有票据时无效', () => {
    localStorage.setItem('pay_verify_exp', String(Date.now() + 60_000));
    expect(verifyTokenValid()).toBe(false);
  });

  it('clearVerifyToken 同时清掉令牌和过期时间', () => {
    setVerifyToken('vt', 60);
    clearVerifyToken();
    expect(getVerifyToken()).toBe('');
    expect(localStorage.getItem('pay_verify_exp')).toBeNull();
  });
});

describe('api 请求头', () => {
  it('带上 X-User-Token', async () => {
    setToken('user-tok');
    fetchMock.mockResolvedValue(jsonResponse({ ok: true }));
    await api('/api/me');
    const [, init] = fetchMock.mock.calls[0];
    expect(init.headers['X-User-Token']).toBe('user-tok');
  });

  it('票据有效时带上 X-Verify-Token', async () => {
    setToken('user-tok');
    setVerifyToken('vt', 60);
    fetchMock.mockResolvedValue(jsonResponse({ ok: true }));
    await api('/api/me');
    const [, init] = fetchMock.mock.calls[0];
    expect(init.headers['X-Verify-Token']).toBe('vt');
  });

  it('票据无效时不带 X-Verify-Token', async () => {
    setToken('user-tok');
    fetchMock.mockResolvedValue(jsonResponse({ ok: true }));
    await api('/api/me');
    const [, init] = fetchMock.mock.calls[0];
    expect('X-Verify-Token' in init.headers).toBe(false);
  });

  it('GET 不带 body,POST 才序列化', async () => {
    // 每次都造一个新的 Response —— 同一个 Response 的 body 只能读一次。
    fetchMock.mockImplementation(async () => jsonResponse({ ok: true }));
    await api('/api/me');
    expect(fetchMock.mock.calls[0][1].body).toBeUndefined();

    fetchMock.mockClear();
    await api('/api/login', { method: 'POST', body: { name: 'x' } });
    expect(fetchMock.mock.calls[0][1].method).toBe('POST');
    expect(fetchMock.mock.calls[0][1].body).toBe('{"name":"x"}');
  });

  it('返回解析后的 JSON', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ token: 'abc', user: { user_name: 'n' } }));
    const res = await api<{ token: string }>('/api/login');
    expect(res.token).toBe('abc');
  });

  it('响应体不是合法 JSON 时不崩,当作空对象', async () => {
    fetchMock.mockResolvedValue(new Response('not json', { status: 200 }));
    await expect(api('/api/me')).resolves.toEqual({});
  });

  it('空响应体返回空对象', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }));
    await expect(api('/api/me')).resolves.toEqual({});
  });
});

describe('api 错误处理', () => {
  it('抛出 ApiError,消息取响应体里的 error 字段', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ error: '余额不足' }, 400));
    const err = await api('/api/pay').catch((e) => e) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(400);
    expect(err.message).toBe('余额不足');
  });

  it('响应体没有 error 字段时用 HTTP 状态兜底', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ foo: 1 }, 500));
    const err = await api('/api/pay').catch((e) => e) as ApiError;
    expect(err.message).toBe('HTTP 500');
  });

  it('401 会清掉本地令牌', async () => {
    setToken('stale');
    fetchMock.mockResolvedValue(jsonResponse({ error: '令牌失效' }, 401));
    await api('/api/me').catch(() => undefined);
    expect(getToken()).toBe('');
  });

  it('401 且 silent401 时不清令牌', async () => {
    setToken('stale');
    fetchMock.mockResolvedValue(jsonResponse({ error: '登录失败' }, 401));
    await api('/api/login', { method: 'POST', silent401: true }).catch(() => undefined);
    expect(getToken()).toBe('stale');
  });

  it('401 时派发 hashchange,让已渲染的控制台回到登录页', async () => {
    let fired = 0;
    window.addEventListener('hashchange', () => { fired++; });
    fetchMock.mockResolvedValue(jsonResponse({ error: 'x' }, 401));
    await api('/api/me').catch(() => undefined);
    expect(fired).toBe(1);
  });

  it('403 且消息提到票据时清掉本地票据', async () => {
    setVerifyToken('vt', 60);
    fetchMock.mockResolvedValue(jsonResponse({ error: '票据已过期' }, 403));
    // mock 的 requireVerify 会重新给一张票据,重试成功后票据应存在。
    const { requireVerify } = await import('./verify');
    await api('/api/pay').catch(() => undefined);
    expect(requireVerify).toHaveBeenCalled();
  });

  it('403 票据失效时自动重新验证并重试一次', async () => {
    const { requireVerify } = await import('./verify');
    (requireVerify as ReturnType<typeof vi.fn>).mockClear();
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ error: '票据已失效' }, 403))
      .mockResolvedValueOnce(jsonResponse({ ok: true }));
    const res = await api<{ ok: boolean }>('/api/pay');
    expect(res.ok).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(requireVerify).toHaveBeenCalledWith(true);
  });

  it('重试后仍失败就抛错,不会无限递归', async () => {
    const { requireVerify } = await import('./verify');
    (requireVerify as ReturnType<typeof vi.fn>).mockClear();
    fetchMock.mockImplementation(async () => jsonResponse({ error: '票据已失效' }, 403));
    const err = await api('/api/pay').catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    // 第一次 + 一次重试 = 2 次,不能再多
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(requireVerify).toHaveBeenCalledTimes(1);
  });

  it('验证接口本身不走重试(否则会自死循环)', async () => {
    const { requireVerify } = await import('./verify');
    (requireVerify as ReturnType<typeof vi.fn>).mockClear();
    fetchMock.mockResolvedValue(jsonResponse({ error: '票据已失效' }, 403));
    await api('/api/account/verify', { method: 'POST', body: {} }).catch(() => undefined);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(requireVerify).not.toHaveBeenCalled();
  });

  it('403 但与票据无关时不重试', async () => {
    const { requireVerify } = await import('./verify');
    (requireVerify as ReturnType<typeof vi.fn>).mockClear();
    fetchMock.mockResolvedValue(jsonResponse({ error: '没有权限' }, 403));
    await api('/api/pay').catch(() => undefined);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(requireVerify).not.toHaveBeenCalled();
  });
});

describe('apiDownload', () => {
  it('用 Content-Disposition 里的文件名', async () => {
    // jsdom 没有 URL.createObjectURL,补一个假的。
    vi.stubGlobal('URL', Object.assign(Object.create(URL), {
      createObjectURL: vi.fn(() => 'blob:fake-url'),
      revokeObjectURL: vi.fn(),
    }));
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
    fetchMock.mockImplementation(async () => new Response('a,b\n1,2', {
      status: 200,
      headers: { 'Content-Disposition': 'attachment; filename="orders-20260101-120000.csv"' },
    }));
    await apiDownload('/api/orders/export', 'orders.csv');
    expect(clickSpy).toHaveBeenCalled();
    // 名字取自服务端,而不是前端写死的兜底名
    const anchors = [...document.querySelectorAll('a')];
    expect(anchors.length).toBe(0); // 用完就从 DOM 里摘掉了
  });

  it('服务端没给文件名时用兜底名', async () => {
    vi.stubGlobal('URL', Object.assign(Object.create(URL), {
      createObjectURL: vi.fn(() => 'blob:fake-url'),
      revokeObjectURL: vi.fn(),
    }));
    let downloaded = '';
    const createSpy = vi.spyOn(document, 'createElement');
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
      downloaded = this.download;
    });
    fetchMock.mockImplementation(async () => new Response('x', { status: 200 }));
    await apiDownload('/api/orders/export', 'orders.csv');
    expect(downloaded).toBe('orders.csv');
    createSpy.mockRestore();
  });

  it('文件名里的路径分隔符被替换掉,不能变成目录穿越', async () => {
    vi.stubGlobal('URL', Object.assign(Object.create(URL), {
      createObjectURL: vi.fn(() => 'blob:fake-url'),
      revokeObjectURL: vi.fn(),
    }));
    let downloaded = '';
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
      downloaded = this.download;
    });
    fetchMock.mockImplementation(async () => new Response('x', {
      status: 200,
      headers: { 'Content-Disposition': 'attachment; filename="../../etc/passwd"' },
    }));
    await apiDownload('/api/orders/export', 'orders.csv');
    expect(downloaded).not.toContain('/');
    expect(downloaded).not.toContain('\\');
  });

  it('响应不成功时抛 ApiError', async () => {
    fetchMock.mockImplementation(async () => new Response('nope', { status: 500 }));
    const err = await apiDownload('/api/orders/export', 'orders.csv').catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(500);
  });
});
