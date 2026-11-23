import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
const loadAppsMock = vi.hoisted(() => vi.fn());
vi.mock('../lib/api', () => ({ api: apiMock }));
vi.mock('../lib/apps', () => ({ loadApps: loadAppsMock }));
vi.mock('../lib/verify', () => ({ requireVerify: vi.fn(async () => 'vt') }));

import { renderKeys } from './keys';

const APPS = [
  { app_id: 'APP_IOS', name: 'iOS 客户端' },
  { app_id: 'APP_H5', name: 'H5' },
];

/** 找 POST /api/keys 的请求体。 */
function createBody(): Record<string, unknown> | undefined {
  const call = apiMock.mock.calls.find((c) => String(c[0]) === '/api/keys'
    && (c[1] as { method?: string } | undefined)?.method === 'POST');
  return call?.[1]?.body as Record<string, unknown> | undefined;
}

beforeEach(() => {
  document.body.innerHTML = '';
  apiMock.mockReset();
  loadAppsMock.mockReset();
  loadAppsMock.mockImplementation(async () => ({
    apps: APPS,
    unclassified: { order_count: 0, success_count: 0, amount: 0 },
  }));
  apiMock.mockImplementation(async (path: string) => {
    if (path === '/api/me') {
      return { user: { user_id: 'u1', user_name: '商户甲', require_app_binding: false } };
    }
    if (path === '/api/keys') {
      return {
        keys: [
          {
            key_id: 'pk_1',
            name: '绑了的',
            app_id: 'APP_IOS',
            scopes: ['pay'],
            enabled: true,
          },
          {
            key_id: 'pk_2',
            name: '没绑的',
            scopes: ['pay'],
            enabled: true,
          },
        ],
      };
    }
    if (path === '/api/openapi/sample') return { client: '', readme: '', curl: '' };
    return {};
  });
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('开放接口密钥页的应用绑定', () => {
  it('下拉框里有「不绑定」和各个应用', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    const select = document.getElementById('key-app') as HTMLSelectElement;
    await vi.waitFor(() => expect(select.options.length).toBe(3));
    expect(select.options[0].value).toBe('');
    expect(select.options[0].text).toContain('不绑定');
    expect([...select.options].map((o) => o.value)).toEqual(['', 'APP_IOS', 'APP_H5']);
  });

  it('选了应用之后创建密钥,请求体里带上 app_id', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    const select = document.getElementById('key-app') as HTMLSelectElement;
    await vi.waitFor(() => expect(select.options.length).toBe(3));
    select.value = 'APP_IOS';

    (document.getElementById('key-name') as HTMLInputElement).value = 'iOS 专用';
    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '创建密钥') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(createBody()).toBeDefined());
    const body = createBody()!;
    expect(body.app_id).toBe('APP_IOS');
    expect(body.name).toBe('iOS 专用');
  });

  it('没选应用时请求体里的 app_id 是空串(不绑定的老行为)', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    await vi.waitFor(() => {
      expect((document.getElementById('key-app') as HTMLSelectElement).options.length).toBe(3);
    });

    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '创建密钥') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(createBody()).toBeDefined());
    expect(createBody()!.app_id).toBe('');
  });

  it('列表里显示每把密钥绑定的应用', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    await vi.waitFor(() => {
      expect(root.textContent).toContain('iOS 客户端');
    });
    // 绑了 APP_IOS 的那把显示应用名;没绑的显示「未绑定」。
    expect(root.textContent).toContain('未绑定');
  });
});

describe('强制绑定应用时的开放接口页', () => {
  beforeEach(() => {
    apiMock.mockImplementation(async (path: string) => {
      if (path === '/api/me') {
        return { user: { user_id: 'u1', user_name: '商户甲', require_app_binding: true } };
      }
      if (path === '/api/keys') return { keys: [] };
      if (path === '/api/openapi/sample') return { client: '', readme: '', curl: '' };
      return {};
    });
  });

  it('「不绑定」那一项被禁用', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    const select = document.getElementById('key-app') as HTMLSelectElement;
    await vi.waitFor(() => expect(select.options.length).toBe(3));
    expect(select.options[0].disabled).toBe(true);
    expect(select.options[0].text).toContain('必须绑定应用');
  });

  it('没选应用就点创建:不发请求,并提示必须绑定应用', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    const select = document.getElementById('key-app') as HTMLSelectElement;
    await vi.waitFor(() => expect(select.options.length).toBe(3));
    // 「不绑定」被禁用时,下拉框的值停在第一项(空)。
    expect(select.value).toBe('');

    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '创建密钥') as HTMLButtonElement;
    btn.click();

    // 请求不能发出去(否则后端会拒,前端应该自己先拦一道)。
    await new Promise((r) => setTimeout(r, 20));
    expect(createBody()).toBeUndefined();
  });

  it('选了应用就能正常创建', async () => {
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderKeys(root);

    const select = document.getElementById('key-app') as HTMLSelectElement;
    await vi.waitFor(() => expect(select.options.length).toBe(3));
    select.value = 'APP_IOS';

    const btn = [...document.querySelectorAll('button')]
      .find((b) => b.textContent === '创建密钥') as HTMLButtonElement;
    btn.click();

    await vi.waitFor(() => expect(createBody()).toBeDefined());
    expect(createBody()!.app_id).toBe('APP_IOS');
  });
});
