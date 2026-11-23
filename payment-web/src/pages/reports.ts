/**
 * 报表页:按应用 / 渠道 / 天分组看交易数据。
 *
 * 一个商户有多个产品之后,「这个月 iOS 端赚了多少」「微信渠道成功率多少」
 * 「哪天流水最高」这些问题是绕不开的。导出 CSV 再用 Excel 透视也能做到,
 * 但订单多了以后导出一次就是几 MB、还得等 —— 页面上直接分组看更快。
 *
 * 口径与概览页完全一致(后端同一套统计代码):
 *   - 成功金额只算支付成功的订单;
 *   - 已退金额算占位与处理中的退款(钱可能已经出去了),不算失败的。
 */

import { api } from '../lib/api';
import { el, table } from '../lib/dom';
import { loadApps } from '../lib/apps';
import { CHANNEL_LABELS, money } from '../lib/store';
import { card, emptyState, stat } from '../lib/ui';
import { SeqGuard } from '../lib/seq';
import type { StatsGroupedResponse } from '../lib/types';

type GroupBy = 'app' | 'channel' | 'day';

const GROUPS: { value: GroupBy; label: string }[] = [
  { value: 'app', label: '按应用' },
  { value: 'channel', label: '按渠道' },
  { value: 'day', label: '按天' },
];

export function renderReports(root: HTMLElement): void {
  const groupSel = el('select', { id: 'rp-group' }) as HTMLSelectElement;
  for (const g of GROUPS) groupSel.appendChild(el('option', { value: g.value, text: g.label }));
  groupSel.value = 'app';

  const appSel = el('select', { id: 'rp-app' }) as HTMLSelectElement;
  appSel.appendChild(el('option', { value: '', text: '全部应用' }));

  const statsBox = el('div', { class: 'stat-grid' });
  const bodyBox = el('div');

  root.appendChild(card('筛选', '按维度分组,看清每个产品 / 渠道 / 每天的流水',
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '分组维度', for: 'rp-group' }), groupSel),
      el('div', { class: 'field', style: 'margin:0' },
        el('label', { class: 'label', text: '应用范围', for: 'rp-app' }), appSel)),
    statsBox));

  root.appendChild(card('分组明细', '合计口径与概览页一致', null, bodyBox));

  // 请求序号守卫:连改分组维度/应用范围时,后到的旧响应不能覆盖新结果,
  // 否则「下拉框选的是按渠道、表格里却是按天」,两边对不上。
  const guard = new SeqGuard();

  const load = async (): Promise<void> => {
    const seq = guard.next();
    bodyBox.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    const groupBy = groupSel.value as GroupBy;
    const appID = appSel.value;
    const params = new URLSearchParams();
    params.set('group_by', groupBy);
    if (appID) params.set('app_id', appID);
    try {
      const res = await api<StatsGroupedResponse>('/api/stats?' + params.toString());
      if (!guard.isCurrent(seq)) return;
      const s = res.stats;
      statsBox.replaceChildren(
        stat('订单总数', String(s.order_count ?? 0), '笔'),
        stat('支付成功', String(s.success_count ?? 0), '笔'),
        stat('成功金额', money(s.total_amount ?? 0), '元', true),
        stat('已退款', money(s.refund_amount ?? 0), '元'),
        stat('净收入', money(s.net_amount ?? 0), '元'),
        stat('成功率', ((s.success_rate ?? 0) * 100).toFixed(1), '%'),
      );

      const groups = res.groups || [];
      bodyBox.replaceChildren();
      // 被截断时要明说:给出的是「最近一部分」,不是全部。
      if (res.truncated) {
        bodyBox.appendChild(el('div', { class: 'notice notice-info', style: 'margin-bottom:12px' },
          el('div', { text: '数据量超过单次统计上限,这里只统计了最近的一部分。' }),
          el('div', { class: 'hint-text',
            text: '想看全量的话,按应用或渠道分组(不受这个上限影响),' +
              '或者用订单导出分时间段导。' })));
      }
      if (!groups.length) {
        bodyBox.appendChild(emptyState('没有数据', '这个范围下还没有交易记录。'));
        return;
      }
      // 分组名直接用后端给的 label —— 后端已经知道应用名和渠道名,
      // 前端再拿本地缓存(appNameMap)算一遍只会更差:
      // 应用列表是异步加载的,首屏那一遍缓存还是空的,
      // 结果「按应用」分组会显示成裸的 app_id。
      const rows = groups.map((g) => [
        el('span', { text: g.label || groupFallbackLabel(groupBy, g.key) }),
        el('span', { class: 'num', text: String(g.order_count) }),
        el('span', { class: 'num', text: String(g.success_count) }),
        el('span', { class: 'num', text: String(g.refund_count) }),
        el('span', { class: 'num', text: money(g.total_amount) }),
        el('span', { class: 'num', text: money(g.refund_amount) }),
        el('span', { class: 'num', text: money(g.net_amount) }),
      ]);
      bodyBox.appendChild(table(
        ['分组', '订单数', '成功数', '退款数', '成功金额(元)', '已退款(元)', '净收入(元)'], rows));
    } catch (e) {
      bodyBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  };

  groupSel.addEventListener('change', () => void load());
  appSel.addEventListener('change', () => void load());

  // 应用列表(给「应用范围」下拉用)。
  void loadApps()
    .then((res) => {
      appSel.replaceChildren(el('option', { value: '', text: '全部应用' }));
      for (const app of res.apps) {
        appSel.appendChild(el('option', { value: app.app_id, text: app.name }));
      }
    })
    .catch(() => undefined);

  void load();
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * 后端没给 label 时的兜底展示名。
 *
 * 只是兜底:正常情况下后端一定会带 label 过来(应用名、渠道名它都知道)。
 */
function groupFallbackLabel(groupBy: GroupBy, key: string): string {
  if (groupBy === 'channel') return CHANNEL_LABELS[key] || key;
  if (groupBy === 'app') return key || '未分类';
  return key;
}
