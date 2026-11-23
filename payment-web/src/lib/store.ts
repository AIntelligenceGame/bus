/** 登录态与当前用户信息的共享状态。 */

import { api, clearToken, clearVerifyToken, getToken, setToken } from './api';
import { invalidateApps } from './apps';
import type { LoginResponse, MeResponse } from './types';

export interface AppState {
  logged: boolean;
  userName: string;
  userId: string;
  channels: MeResponse['channels'];
  notifyUrls: Record<string, string>;
  defaultChannel: string;
  /** 是否强制订单绑定应用(每一笔支付都必须归属某个应用)。 */
  requireAppBinding: boolean;
}

export const state: AppState = {
  logged: false,
  userName: '',
  userId: '',
  channels: [],
  notifyUrls: {},
  defaultChannel: '',
  requireAppBinding: false,
};

/** 是否已登录(本地有令牌)。 */
export function hasToken(): boolean {
  return !!getToken();
}

/** 保存登录令牌并刷新用户信息。 */
export async function login(
  name: string,
  password: string,
  code: string,
  registrationCode = '',
): Promise<LoginResponse> {
  const res = await api<LoginResponse>('/api/login', {
    method: 'POST',
    body: { name, password, code, registration_code: registrationCode },
    // 登录失败返回 401 是正常反馈,不要触发「清令牌 + 跳登录页」。
    silent401: true,
  });
  setToken(res.token);
  // 换账号了:上一个账号的应用列表缓存必须扔掉,
  // 否则新用户会看到别人的应用名 / app_id。
  invalidateApps();
  await refreshMe();
  return res;
}

/** 退出登录。 */
export async function logout(): Promise<void> {
  try {
    await api<{ ok: boolean }>('/api/logout', { method: 'POST' });
  } catch {
    /* 忽略:即使后端失败也要清掉本地状态 */
  }
  clearToken();
  // 敏感操作票据也属于上一个账号:留着的话,下一个登录的用户会被
  // 带着上一张票据去请求(后端判定「票据不属于当前用户」)。
  clearVerifyToken();
  // 应用列表缓存同理:不清的话同一台机器换个商户登录,
  // 订单页的筛选下拉里会出现上一个商户的应用名。
  invalidateApps();
  state.logged = false;
  state.userName = '';
  state.userId = '';
  state.channels = [];
  state.requireAppBinding = false;
}

/** 拉取当前用户与渠道配置(密钥已脱敏)。 */
export async function refreshMe(): Promise<MeResponse> {
  const me = await api<MeResponse>('/api/me');
  // 令牌被换过(比如 401 之后重新登录)时,缓存里可能还是上一个账号的数据。
  if (state.userId && state.userId !== me.user.user_id) {
    invalidateApps();
  }
  state.logged = true;
  state.userName = me.user.user_name;
  state.userId = me.user.user_id;
  state.channels = me.channels || [];
  state.notifyUrls = me.notify_urls || {};
  state.defaultChannel = me.default_channel || '';
  state.requireAppBinding = !!me.user.require_app_binding;
  return me;
}

/** 渠道显示名。 */
export const CHANNEL_LABELS: Record<string, string> = {
  wechat: '微信支付',
  alipay: '支付宝',
  applepay: 'Apple Pay',
  googlepay: 'Google Pay',
};

/**
 * 金额换算工具(实现在 lib/money.ts)。
 *
 * 这里再导出一次,是为了让「已经从 store 拿东西」的页面不用多引一个模块。
 */
export { money, yuanToCents } from './money';

/** 订单状态 → 展示用的中文名与徽章颜色。 */
export const ORDER_STATUS: Record<
  string,
  { label: string; kind: 'success' | 'warning' | 'danger' | 'info' | 'muted' }
> = {
  SUCCESS: { label: '支付成功', kind: 'success' },
  PENDING: { label: '待支付', kind: 'warning' },
  CLOSED: { label: '已关闭', kind: 'muted' },
  FAILED: { label: '支付失败', kind: 'danger' },
  REFUNDED: { label: '已退款', kind: 'info' },
  PARTIALLY_REFUNDED: { label: '部分退款', kind: 'info' },
};

/** 交易类型 → 中文名。 */
export const TRADE_TYPES: Record<string, string> = {
  app: 'APP 拉起',
  jsapi: 'JSAPI / 公众号',
  h5: 'H5 网页',
  wap: '手机网站',
  web: '电脑网站',
  native: '扫码支付',
  wallet: '钱包支付',
};
