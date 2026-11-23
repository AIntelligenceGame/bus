import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const apiMock = vi.hoisted(() => vi.fn());
vi.mock('../lib/api', () => ({
  api: apiMock,
  apiDownload: vi.fn(),
}));
vi.mock('../lib/apps', () => ({
  loadApps: vi.fn(async () => ({ apps: [], unclassified: { order_count: 0, success_count: 0, amount: 0 } })),
  appLabel: (names: Record<string, string>, appID?: string) =>
    (!appID ? '未分类' : names[appID] || appID),
  appNameMap: (apps: { app_id: string; name: string }[]) =>
    Object.fromEntries(apps.map((a) => [a.app_id, a.name])),
}));
vi.mock('../lib/verify', () => ({ requireVerify: vi.fn(async () => 'vt') }));

import { renderOrders } from './orders';

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

function order(outTradeNo: string, amount = 100): Record<string, unknown> {
  return {
    out_trade_no: outTradeNo,
    channel: 'wechat',
    subject: '测试订单',
    amount,
    status: 'SUCCESS',
    trade_type: 'jsapi',
    app_id: '',
    created_at: '2026-01-01T00:00:00Z',
  };
}

function pageOf(orders: Record<string, unknown>[]): Record<string, unknown> {
  return { orders, total: orders.length, page: 1, pages: 1, size: 20 };
}

/** 订单表格在页面里的容器:第二个 .card 里带 table 的那块。 */
function ordersTable(): HTMLTableElement | null {
  const tables = [...document.querySelectorAll('table')];
  return (tables[0] as HTMLTableElement) ?? null;
}

beforeEach(() => {
  document.body.innerHTML = '';
  apiMock.mockReset();
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('订单页渲染', () => {
  it('渲染订单表格', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path.startsWith('/api/orders')) return pageOf([order('ORDER-1')]);
      if (path.startsWith('/api/stats')) return { today_count: 0, success_rate: 1 };
      if (path.startsWith('/api/refunds')) return { refunds: [] };
      return {};
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderOrders(root);
    await vi.waitFor(() => expect(ordersTable()).not.toBeNull());
    expect(ordersTable()!.textContent).toContain('ORDER-1');
  });

  it('没有订单时显示空状态', async () => {
    apiMock.mockImplementation(async (path: string) => {
      if (path.startsWith('/api/orders')) return pageOf([]);
      if (path.startsWith('/api/stats')) return {};
      if (path.startsWith('/api/refunds')) return { refunds: [] };
      return {};
    });
    const root = document.createElement('div');
    document.body.appendChild(root);
    renderOrders(root);
    await vi.waitFor(() => expect(root.textContent).toContain('没有符合条件的订单'));
  });
});

describe('订单页的异步竞态', () => {
  it('连改筛选条件时,后到的旧响应不能覆盖新结果', async () => {
    // 第一次请求(全部订单)先发出但最后才回来,
    // 第二次请求(已关闭)先回来。
    // 如果没有序号守卫,表格会被旧响应覆盖成「全部订单」,
    // 而下拉框停在「已关闭」—— 界面自相矛盾。
    const first = deferred<Record<string, unknown>>();
    const second = deferred<Record<string, unknown>>();

    apiMock.mockImplementation(async (path: string) => {
      if (path.startsWith('/api/orders')) {
        return path.includes('status=CLOSED') ? second.promise : first.promise;
      }
      if (path.startsWith('/api/stats')) return {};
      if (path.startsWith('/api/refunds')) return { refunds: [] };
      return {};
    });

    const root = document.createElement('div');
    document.body.appendChild(root);
    renderOrders(root);

    // 等第一次请求发出。
    await vi.waitFor(() => expect(apiMock.mock.calls.some(
      (c) => String(c[0]).startsWith('/api/orders'))).toBe(true));

    // 用户改成「已关闭」。
    const statusSel = document.getElementById('order-status') as HTMLSelectElement;
    statusSel.value = 'CLOSED';
    statusSel.dispatchEvent(new Event('change'));

    // 第二个请求发出后,让它先回来。
    await vi.waitFor(() => expect(apiMock.mock.calls.filter(
      (c) => String(c[0]).includes('status=CLOSED')).length).toBe(1));
    second.resolve(pageOf([order('CLOSED-ORDER')]));
    await vi.waitFor(() => expect(root.textContent).toContain('CLOSED-ORDER'));

    // 第一个请求现在才回来 —— 结果必须被丢弃。
    first.resolve(pageOf([order('OLD-ORDER'), order('OLD-ORDER-2')]));
    await new Promise((r) => setTimeout(r, 30));

    expect(root.textContent).not.toContain('OLD-ORDER');
    expect(root.textContent).toContain('CLOSED-ORDER');
  });
});
