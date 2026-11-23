/**
 * 应用 / 产品维度的共享数据。
 *
 * 一个商户可以有多个产品(APP、小程序、H5……)。订单、退款、统计都能按产品分开,
 * 所以「当前有哪些应用」是多个页面都要用的东西(订单页的筛选器、概览页的
 * 分产品统计、渠道页的应用范围切换),放在这里缓存一份,避免每个页面各拉一次。
 *
 * 缓存策略:应用列表变动很慢(建应用、删应用才变),所以
 *   - 首次访问时拉一次;
 *   - 5 分钟内复用缓存;
 *   - 保存 / 删除应用后由调用方显式失效。
 *
 * 注意:应用名只是**展示**用的;真正定位一个应用一律用 app_id。
 */

import { api } from './api';
import type { App, AppsResponse } from './types';

/** 缓存有效期(毫秒)。 */
const CACHE_TTL = 5 * 60 * 1000;

interface Cache {
  apps: App[];
  unclassified: AppsResponse['unclassified'];
  at: number;
}

let cache: Cache | null = null;

/** 正在进行的拉取(避免并发页面同时触发多个请求)。 */
let inflight: Promise<Cache> | null = null;

/**
 * 请求序号。
 *
 * 每次发请求都拿一个自增号;响应回来时只有「自己是最新那次」才允许写缓存。
 *
 * 少了这道闸,强制刷新(force=true)会出这种问题:
 * 旧请求 R1 先发出、强制刷新的新请求 R2 后发出,但 R2 先回、R1 后回 ——
 * R1 的 .then 里无条件写缓存,就把刚拿到的新列表覆盖成旧列表,
 * 还把缓存时间刷新成「刚刚」,接下来 5 分钟谁都看不到新建的应用。
 */
let latestSeq = 0;

/** 拉取应用列表(带缓存)。 */
export async function loadApps(force = false): Promise<Cache> {
  if (!force && cache && Date.now() - cache.at < CACHE_TTL) {
    return cache;
  }
  // force=true 时不能复用**已经在飞**的请求:
  // 那个请求可能是缓存还没过期时发起的,拿到的还是旧列表 ——
  // 「刚建完应用,列表里却没有」就是这么来的。
  // 强制刷新宁可多发一次请求,也不能返回旧数据。
  if (inflight && !force) return inflight;
  const seq = ++latestSeq;
  const req = api<AppsResponse>('/api/apps')
    .then((res) => {
      // 只有最新那次请求的结果才写缓存(见 latestSeq 的说明)。
      if (seq === latestSeq) {
        cache = {
          apps: res.apps || [],
          unclassified: res.unclassified || { order_count: 0, success_count: 0, amount: 0 },
          at: Date.now(),
        };
      }
      return cache ?? {
        apps: res.apps || [],
        unclassified: res.unclassified || { order_count: 0, success_count: 0, amount: 0 },
        at: Date.now(),
      };
    })
    .finally(() => {
      if (inflight === req) inflight = null;
    });
  inflight = req;
  return req;
}

/** 失效缓存(应用有增删改之后调用)。 */
export function invalidateApps(): void {
  cache = null;
  // 让还在飞的旧请求回来时也写不进缓存。
  latestSeq++;
}

/** 当前缓存里的应用(没拉过就是空数组)。 */
export function cachedApps(): App[] {
  return cache ? cache.apps : [];
}

/**
 * 应用 ID → 显示名。
 *
 * 未分类(app_id 为空)显示「未分类」而不是空串 —— 财务看到空格子
 * 会以为是导出漏了数据。
 */
export function appNameMap(apps: App[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (const app of apps) map[app.app_id] = app.name;
  return map;
}

/** 取应用的显示名。 */
export function appLabel(names: Record<string, string>, appID: string | undefined): string {
  if (!appID) return '未分类';
  return names[appID] || appID;
}
