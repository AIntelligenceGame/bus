import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { appLabel, appNameMap, cachedApps, invalidateApps, loadApps } from './apps';

const apiMock = vi.hoisted(() => vi.fn());
vi.mock('./api', () => ({ api: apiMock }));

interface AppsPayload {
  apps: { app_id: string; name: string }[];
  unclassified: { order_count: number; success_count: number; amount: number };
}

function payload(names: string[]): AppsPayload {
  return {
    apps: names.map((n, i) => ({ app_id: `app${i}`, name: n })),
    unclassified: { order_count: 1, success_count: 1, amount: 100 },
  };
}

/** 可手动 resolve 的 fetch,用来模拟「请求还在飞」的状态。 */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

beforeEach(() => {
  apiMock.mockReset();
  invalidateApps();
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
});

afterEach(() => {
  vi.useRealTimers();
});

describe('loadApps 缓存', () => {
  it('首次访问拉一次', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    const res = await loadApps();
    expect(apiMock).toHaveBeenCalledTimes(1);
    expect(apiMock).toHaveBeenCalledWith('/api/apps');
    expect(res.apps.map((a) => a.name)).toEqual(['A']);
  });

  it('缓存期内不再拉', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    await loadApps();
    vi.advanceTimersByTime(60_000);
    await loadApps();
    expect(apiMock).toHaveBeenCalledTimes(1);
  });

  it('超过 5 分钟后重新拉', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    await loadApps();
    vi.advanceTimersByTime(5 * 60 * 1000 + 1);
    await loadApps();
    expect(apiMock).toHaveBeenCalledTimes(2);
  });

  it('force=true 跳过缓存重新拉', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    await loadApps();
    await loadApps(true);
    expect(apiMock).toHaveBeenCalledTimes(2);
  });

  it('invalidateApps 后重新拉', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    await loadApps();
    invalidateApps();
    await loadApps();
    expect(apiMock).toHaveBeenCalledTimes(2);
  });

  it('并发访问只发一个请求', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    const [a, b, c] = await Promise.all([loadApps(), loadApps(), loadApps()]);
    expect(apiMock).toHaveBeenCalledTimes(1);
    expect(a).toBe(b);
    expect(b).toBe(c);
  });

  it('强制刷新不能复用还在飞的旧请求', async () => {
    // 旧请求返回的是「建应用之前」的列表;建完应用后强制刷新,
    // 必须拿到新列表,而不是把旧请求的结果当成刷新结果。
    const old = deferred<AppsPayload>();
    apiMock.mockReturnValueOnce(old.promise).mockResolvedValue(payload(['新应用']));

    const inflightOld = loadApps();
    const forced = loadApps(true); // 强制刷新 → 必须另发一次

    expect(apiMock).toHaveBeenCalledTimes(2);

    old.resolve(payload(['旧应用']));
    await inflightOld;
    const fresh = await forced;
    expect(fresh.apps.map((a) => a.name)).toEqual(['新应用']);
  });

  it('旧请求后到时不能覆盖新缓存', async () => {
    // R1(旧)先发出,R2(force,新)后发出;但 R2 先回、R1 后回。
    // 如果 .then 里无条件写 cache,旧数据会把新数据盖掉,
    // 并且把缓存时间刷新成「刚刚」—— 接下来 5 分钟谁都看不到新应用。
    const old = deferred<AppsPayload>();
    const fresh = deferred<AppsPayload>();
    apiMock.mockReturnValueOnce(old.promise).mockReturnValueOnce(fresh.promise);

    const pOld = loadApps();
    const pFresh = loadApps(true);
    expect(apiMock).toHaveBeenCalledTimes(2);

    fresh.resolve(payload(['新应用']));
    await pFresh;
    expect(cachedApps().map((a) => a.name)).toEqual(['新应用']);

    old.resolve(payload(['旧应用']));
    await pOld;
    // 旧请求后到 —— 缓存必须还是新的
    expect(cachedApps().map((a) => a.name)).toEqual(['新应用']);
  });

  it('请求失败后不留下脏缓存,下次能重新拉', async () => {
    apiMock.mockRejectedValueOnce(new Error('网络断了'));
    await expect(loadApps()).rejects.toThrow('网络断了');
    expect(cachedApps()).toEqual([]);

    apiMock.mockResolvedValue(payload(['A']));
    const res = await loadApps();
    expect(res.apps.map((a) => a.name)).toEqual(['A']);
  });

  it('后端没返回 apps 时给空数组,不能是 undefined', async () => {
    apiMock.mockResolvedValue({});
    const res = await loadApps();
    expect(res.apps).toEqual([]);
    expect(res.unclassified).toEqual({ order_count: 0, success_count: 0, amount: 0 });
  });

  it('后端没返回 unclassified 时给零值', async () => {
    apiMock.mockResolvedValue({ apps: [] });
    const res = await loadApps();
    expect(res.unclassified).toEqual({ order_count: 0, success_count: 0, amount: 0 });
  });
});

describe('cachedApps', () => {
  it('没拉过时是空数组', () => {
    expect(cachedApps()).toEqual([]);
  });

  it('拉过之后返回缓存内容', async () => {
    apiMock.mockResolvedValue(payload(['A', 'B']));
    await loadApps();
    expect(cachedApps().map((a) => a.name)).toEqual(['A', 'B']);
  });

  it('invalidateApps 后清空', async () => {
    apiMock.mockResolvedValue(payload(['A']));
    await loadApps();
    invalidateApps();
    expect(cachedApps()).toEqual([]);
  });
});

describe('appNameMap / appLabel', () => {
  it('app_id → 名称', () => {
    const map = appNameMap([{ app_id: 'app1', name: '商城' }]);
    expect(map.app1).toBe('商城');
  });

  it('app_id 为空显示「未分类」', () => {
    // 财务看到空格子会以为导出漏了数据。
    expect(appLabel({}, '')).toBe('未分类');
    expect(appLabel({}, undefined)).toBe('未分类');
  });

  it('找不到的应用退回显示 app_id 本身', () => {
    expect(appLabel({}, 'app404')).toBe('app404');
  });

  it('有名字时显示名字', () => {
    expect(appLabel({ app1: '商城' }, 'app1')).toBe('商城');
  });
});
