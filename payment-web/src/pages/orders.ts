/** 订单页:统计概览、订单筛选/分页、导出 CSV、退款与退款流水。 */

import { api, apiDownload } from '../lib/api';
import { el, table } from '../lib/dom';
import { appLabel, appNameMap, loadApps } from '../lib/apps';
import { CHANNEL_LABELS, money, ORDER_STATUS, TRADE_TYPES } from '../lib/store';
import {
  amountDialog, badge, card, confirmDialog, emptyState, stat, toast, withBusy,
} from '../lib/ui';
import { requireVerify } from '../lib/verify';
import { SeqGuard } from '../lib/seq';
import type { OrderRecord, PageResponse, RefundRecord, Stats } from '../lib/types';

const STATUSES: { value: string; label: string }[] = [
  { value: '', label: '全部状态' },
  { value: 'SUCCESS', label: '支付成功' },
  { value: 'PENDING', label: '待支付' },
  { value: 'CLOSED', label: '已关闭' },
  { value: 'FAILED', label: '支付失败' },
  { value: 'REFUNDED', label: '已退款' },
  { value: 'PARTIALLY_REFUNDED', label: '部分退款' },
];

const CHANNELS: { value: string; label: string }[] = [
  { value: '', label: '全部渠道' },
  { value: 'wechat', label: '微信支付' },
  { value: 'alipay', label: '支付宝' },
  { value: 'applepay', label: 'Apple Pay' },
  { value: 'googlepay', label: 'Google Pay' },
];

export function renderOrders(root: HTMLElement): void {
  const statsBox = el('div', { class: 'stat-grid' });
  root.appendChild(card('交易概览', '平台侧的收款统计', null, statsBox));

  // ---- 筛选条 ----
  const keyword = el('input', { type: 'text', id: 'order-keyword', placeholder: '订单号 / 标题 / 渠道流水号' });
  const statusSel = el('select', { id: 'order-status' }) as HTMLSelectElement;
  for (const s of STATUSES) statusSel.appendChild(el('option', { value: s.value, text: s.label }));
  const channelSel = el('select', { id: 'order-channel' }) as HTMLSelectElement;
  for (const c of CHANNELS) channelSel.appendChild(el('option', { value: c.value, text: c.label }));
  // 应用(产品)筛选:一个商户多个产品时,要能分开看各自的订单。
  const appSel = el('select', { id: 'order-app' }) as HTMLSelectElement;
  appSel.appendChild(el('option', { value: '', text: '全部应用' }));
  // 时间区间:后端早就支持 from/to,但页面一直没暴露出来 ——
  // 订单多了以后只能一页页翻,既慢又费流量。
  const fromInput = el('input', { type: 'date', id: 'order-from' }) as HTMLInputElement;
  const toInput = el('input', { type: 'date', id: 'order-to' }) as HTMLInputElement;

  const refreshBtn = el('button', { class: 'btn btn-primary', text: '查询' }) as HTMLButtonElement;
  const exportBtn = el('button', { class: 'btn btn-secondary', text: '导出 CSV' }) as HTMLButtonElement;
  const body = el('div');
  const pager = el('div', { class: 'pager' });

  root.appendChild(card('我的订单', '按状态、渠道、关键字或时间区间筛选',
    el('div', { class: 'row' }, exportBtn, refreshBtn),
    el('div', { class: 'grid grid-4', style: 'margin-bottom:16px' },
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '状态', for: 'order-status' }), statusSel),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '渠道', for: 'order-channel' }), channelSel),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '应用', for: 'order-app' }), appSel),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '关键字', for: 'order-keyword' }), keyword)),
    el('div', { class: 'grid grid-3', style: 'margin-bottom:16px' },
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '开始日期', for: 'order-from' }), fromInput),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '结束日期', for: 'order-to' }), toInput),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '提示', for: 'order-to' }),
        el('div', { class: 'hint-text', text: '结束日期含当天。订单多时用时间区间缩小范围,页面会快很多。' }))),
    body,
    pager));

  // ---- 退款流水 ----
  const refundBody = el('div');
  root.appendChild(card('退款流水', '平台记录的每一笔退款;累计退款金额不能超过订单金额', null, refundBody));

  let page = 1;
  const size = 20;
  /** 应用 ID → 名称(订单表里的「应用」列要用)。 */
  let appNames: Record<string, string> = {};

  /**
   * 重新拉取列表 / 统计 / 退款流水(状态变更后调用)。
   *
   * 三个请求**并发**发,不是串行:
   * 串行的话每次操作都要等 3 个来回(弱网下尤其明显),
   * 而它们互不依赖,并发只需 1 个来回。
   */
  // 请求序号守卫:连改筛选条件时,后到的旧响应不能覆盖新结果。
  // 否则「下拉框停在已关闭、表格里却是全部订单」,界面自相矛盾。
  //
  // 三条数据流各用一个守卫:订单列表、统计、退款流水是**互相独立**的
  // (reloadAll 里三个请求并发发出),共用一个守卫的话后发的请求会把
  // 先发的直接判成「过时」,数据永远渲染不出来。
  const orderGuard = new SeqGuard();
  const statsGuard = new SeqGuard();
  const refundGuard = new SeqGuard();

  const reloadAll = async (): Promise<void> => {
    await Promise.all([refresh(), loadStats(), loadRefunds()]);
  };

  function currentQuery(): string {
    const params = new URLSearchParams();
    const kw = keyword.value.trim();
    const st = statusSel.value;
    const ch = channelSel.value;
    if (kw) params.set('keyword', kw);
    if (st) params.set('status', st);
    if (ch) params.set('channel', ch);
    if (appSel.value) params.set('app_id', appSel.value);
    if (fromInput.value) params.set('from', fromInput.value);
    if (toInput.value) params.set('to', toInput.value);
    return params.toString();
  }

  const refresh = async (): Promise<void> => {
    const seq = orderGuard.next();
    body.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const res = await api<{ orders: OrderRecord[] } & PageResponse>(
        '/api/orders?' + currentQuery() + '&page=' + page + '&size=' + size,
      );
      if (!orderGuard.isCurrent(seq)) return;   // 已经有更新的请求了,这次结果过时
      const orders = res.orders || [];
      body.replaceChildren();
      if (!orders.length) {
        body.appendChild(emptyState(
          '没有符合条件的订单',
          page > 1 ? '换个页码或放宽筛选条件试试。' : '调整筛选条件,或去「支付体验」发起一笔支付。',
        ));
      } else {
        const rows = orders.map((o) => {
          const refundBtn = el('button', { class: 'btn btn-secondary btn-sm', text: '退款' });
          refundBtn.addEventListener('click', () => void doRefund(o, refundBtn, reloadAll));
          const queryBtn = el('button', { class: 'btn btn-ghost btn-sm', text: '刷新状态' });
          queryBtn.addEventListener('click', () => void doQuery(o, queryBtn, reloadAll));
          const st = ORDER_STATUS[o.status] || { label: o.status, kind: 'muted' as const };
          // 只有待支付的订单能关单;已成功/已关闭的订单给关单按钮纯属摆设。
          const closeBtn = o.status === 'PENDING'
            ? el('button', { class: 'btn btn-ghost btn-sm', text: '关闭订单' })
            : null;
          if (closeBtn) closeBtn.addEventListener('click', () => void doClose(o, closeBtn, reloadAll));
          return [
            el('span', { class: 'cell-mono', text: o.out_trade_no }),
            el('span', { text: appLabel(appNames, o.app_id) }),
            CHANNEL_LABELS[o.channel] || o.channel,
            o.subject || '—',
            el('span', { class: 'num', text: money(o.amount) }),
            badge(st.label, st.kind),
            TRADE_TYPES[o.trade_type] || o.trade_type || '—',
            o.created_at ? new Date(o.created_at).toLocaleString() : '—',
            el('td', { class: 'cell-actions' }, queryBtn, closeBtn, refundBtn),
          ];
        });
        body.appendChild(table(
          ['订单号', '应用', '渠道', '标题', '金额(元)', '状态', '类型', '创建时间', '操作'], rows));
      }
      renderPager(pager, res, (p) => {
        page = p;
        void refresh();
      });
    } catch (e) {
      if (!orderGuard.isCurrent(seq)) return;
      body.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  };

  const loadStats = async (): Promise<void> => {
    const seq = statsGuard.next();
    try {
      // 统计跟着「应用」筛选走:筛选了某个产品,上面这排数字就只算那个产品。
      const q = appSel.value ? '?app_id=' + encodeURIComponent(appSel.value) : '';
      const s = await api<Stats>('/api/stats' + q);
      if (!statsGuard.isCurrent(seq)) return;
      statsBox.replaceChildren(
        stat('今日订单', String(s.today_count ?? 0), '笔'),
        stat('今日成交', money(s.today_amount ?? 0), '元', true),
        stat('累计成交', money(s.total_amount ?? 0), '元'),
        stat('成功率', ((s.success_rate ?? 0) * 100).toFixed(1), '%'),
        stat('已退款', money(s.refund_amount ?? 0), '元'),
        stat('净收入', money(s.net_amount ?? 0), '元'),
      );
    } catch (e) {
      if (!statsGuard.isCurrent(seq)) return;
      statsBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  };

  const loadRefunds = async (): Promise<void> => {
    const seq = refundGuard.next();
    refundBody.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const res = await api<{ refunds: RefundRecord[] }>('/api/refunds?limit=100');
      if (!refundGuard.isCurrent(seq)) return;
      const refunds = res.refunds || [];
      refundBody.replaceChildren();
      if (!refunds.length) {
        refundBody.appendChild(emptyState('还没有退款记录', '订单退款后会在这里留下流水。'));
        return;
      }
      const rows = refunds.map((r) => {
        // 退款失败也显示「处理中」是不对的:钱没退出去,商户会以为还在流程里。
        const st = REFUND_STATUS[r.status] || { label: r.status || '未知', kind: 'muted' as const };
        return [
          el('span', { class: 'cell-mono', text: r.out_trade_no }),
          el('span', { class: 'cell-mono', text: r.out_refund_no }),
          CHANNEL_LABELS[r.channel] || r.channel,
          el('span', { class: 'num', text: money(r.amount) }),
          badge(st.label, st.kind),
          r.created_at ? new Date(r.created_at).toLocaleString() : '—',
        ];
      });
      refundBody.appendChild(table(
        ['订单号', '退款单号', '渠道', '退款金额(元)', '状态', '时间'], rows));
    } catch (e) {
      if (!refundGuard.isCurrent(seq)) return;
      refundBody.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  };

  refreshBtn.addEventListener('click', () => {
    page = 1;
    void withBusy(refreshBtn, refresh);
  });
  statusSel.addEventListener('change', () => {
    page = 1;
    void refresh();
  });
  channelSel.addEventListener('change', () => {
    page = 1;
    void refresh();
  });
  appSel.addEventListener('change', () => {
    page = 1;
    void Promise.all([refresh(), loadStats()]);
  });
  fromInput.addEventListener('change', () => {
    page = 1;
    void refresh();
  });
  toInput.addEventListener('change', () => {
    page = 1;
    void refresh();
  });
  keyword.addEventListener('keydown', (ev) => {
    if ((ev as KeyboardEvent).key === 'Enter') {
      page = 1;
      void refresh();
    }
  });
  exportBtn.addEventListener('click', () => {
    void withBusy(exportBtn, async () => {
      try {
        const q = currentQuery();
        await apiDownload('/api/orders/export' + (q ? '?' + q : ''), 'orders.csv');
        toast('导出已开始(CSV 可用 Excel 打开)', 'ok');
      } catch (e) {
        toast(errText(e), 'err');
      }
    });
  });

  void refresh();
  void loadStats();
  void loadRefunds();

  // 应用列表(筛选下拉 + 表格里的应用名)。
  void loadApps()
    .then((res) => {
      appNames = appNameMap(res.apps);
      appSel.replaceChildren(el('option', { value: '', text: '全部应用' }));
      for (const app of res.apps) {
        appSel.appendChild(el('option', { value: app.app_id, text: app.name }));
      }
      // 列表里的应用名要跟着刷新。
      void refresh();
    })
    .catch(() => undefined);
}

/** 退款状态 → 中文名与徽章颜色。 */
const REFUND_STATUS: Record<
  string,
  { label: string; kind: 'success' | 'warning' | 'danger' | 'info' | 'muted' }
> = {
  SUCCESS: { label: '退款成功', kind: 'success' },
  PROCESSING: { label: '退款处理中', kind: 'warning' },
  PENDING: { label: '退款处理中', kind: 'warning' },
  FAILED: { label: '退款失败', kind: 'danger' },
  CLOSED: { label: '退款已关闭', kind: 'muted' },
};

/** 渲染分页条。 */
function renderPager(
  box: HTMLElement,
  res: { page: number; size: number; total: number; pages: number },
  goto: (page: number) => void,
): void {
  box.replaceChildren();
  const pages = res.pages || 0;
  if (pages <= 1) {
    box.appendChild(el('span', { class: 'muted', text: '共 ' + (res.total || 0) + ' 条' }));
    return;
  }
  const prev = el('button', { class: 'btn btn-secondary btn-sm', text: '上一页' }) as HTMLButtonElement;
  const next = el('button', { class: 'btn btn-secondary btn-sm', text: '下一页' }) as HTMLButtonElement;
  prev.disabled = res.page <= 1;
  next.disabled = res.page >= pages;
  prev.addEventListener('click', () => goto(res.page - 1));
  next.addEventListener('click', () => goto(res.page + 1));
  box.appendChild(prev);
  box.appendChild(next);
  box.appendChild(el('span', { class: 'muted', text: `第 ${res.page}/${pages} 页 · 共 ${res.total} 条` }));
}

/** 退款:先填金额(带上限校验),再二次验证,再执行。 */
async function doRefund(order: OrderRecord, btn: HTMLElement, reload: () => Promise<void>): Promise<void> {
  // 可退余额用后端给的 refundable_amount(订单金额 - 已退金额)。
  // 用订单全额当上限的话,部分退款过的订单会让用户填出一个必然被拒的金额。
  const refundable = order.refundable_amount ?? order.amount;
  const cents = await amountDialog({
    title: '订单退款',
    desc: '订单 ' + order.out_trade_no + ' · 金额 ' + money(order.amount) + ' 元'
      + (order.refunded_amount ? ' · 已退 ' + money(order.refunded_amount) + ' 元' : ''),
    maxCents: refundable,
  });
  if (cents === null) return;
  await withBusy(btn as HTMLButtonElement, async () => {
    try {
      await requireVerify();
      const res = await api<{ refund_amount?: number; order_status?: string }>('/api/refund', {
        method: 'POST',
        body: { out_trade_no: order.out_trade_no, refund_amount: cents },
      });
      toast('退款成功 ' + money(res.refund_amount ?? cents) + ' 元,订单状态 '
        + (res.order_status || ''), 'ok');
      // 状态变了要重画列表:不然表格里还是「支付成功」,用户以为没退。
      await reload();
    } catch (e) {
      toast(errText(e), 'err');
    }
  });
}

/** 关闭订单:待支付的订单可以让它失效,避免用户一直挂着不付。 */
async function doClose(order: OrderRecord, btn: HTMLElement, reload: () => Promise<void>): Promise<void> {
  const ok = await confirmDialog(
    '关闭这笔订单?',
    '订单 ' + order.out_trade_no + ' 将被关闭,用户不能再支付这笔单。已支付成功的订单不能关闭。',
  );
  if (!ok) return;
  await withBusy(btn as HTMLButtonElement, async () => {
    try {
      await requireVerify();
      await api('/api/close', {
        method: 'POST',
        body: { out_trade_no: order.out_trade_no },
      });
      toast('订单已关闭', 'ok');
      await reload();
    } catch (e) {
      toast(errText(e), 'err');
    }
  });
}

/** 主动刷新订单状态。 */
async function doQuery(order: OrderRecord, btn: HTMLElement, reload: () => Promise<void>): Promise<void> {
  await withBusy(btn as HTMLButtonElement, async () => {
    try {
      const res = await api<{ status?: string }>('/api/query', {
        method: 'POST',
        body: { out_trade_no: order.out_trade_no },
      });
      const st = ORDER_STATUS[res.status || ''];
      toast('最新状态:' + (st ? st.label : (res.status || '未知')), 'ok');
      await reload();
    } catch (e) {
      toast(errText(e), 'err');
    }
  });
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
