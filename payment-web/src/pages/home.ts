/** 概览页:账户与渠道的当前状态 + 快捷入口。 */

import { el } from '../lib/dom';
import { api } from '../lib/api';
import { loadApps } from '../lib/apps';
import { CHANNEL_LABELS, money, refreshMe, state } from '../lib/store';
import { badge, card, emptyState, stat } from '../lib/ui';
import type { Stats } from '../lib/types';

export function renderHome(root: HTMLElement): void {
  const greeting = el('div', { class: 'card' },
    el('h2', { text: '欢迎回来' }),
    el('p', { class: 'muted', text: '正在读取账户信息…' }));
  root.appendChild(greeting);

  // ---- 统计概览 ----
  const statsBox = el('div', { class: 'stat-grid' });
  root.appendChild(card('交易概览', '今日与累计的收款情况', null, statsBox));

  // ---- 渠道状态 ----
  const channelBox = el('div');
  root.appendChild(card('支付渠道', '渠道密钥加密入库,保存后立即生效', null, channelBox));

  // ---- 应用(产品) ----
  const appBox = el('div');
  root.appendChild(card('我的应用', '一个产品建一个应用;订单按应用归类,统计和报表分开看', null, appBox));

  // ---- 快捷入口 ----
  const quick = el('div', { class: 'grid grid-3' });
  for (const [href, title, desc] of [
    ['#/playground', '发起支付', '生成二维码或客户端调起参数'],
    ['#/orders', '查询订单', '按状态、渠道、关键字筛选,支持导出'],
    ['#/channels', '配置渠道', '填写密钥并设为默认渠道'],
    ['#/apps', '应用管理', '多产品各配各的渠道密钥'],
    ['#/reports', '报表', '按应用 / 渠道 / 天分组统计'],
  ] as [string, string, string][]) {
    quick.appendChild(el('a', { class: 'channel-card', href },
      el('div', { class: 'channel-card-name', text: title }),
      el('div', { class: 'channel-card-meta', text: desc })));
  }
  root.appendChild(card('快捷入口', null, null, quick));

  void load();

  async function load(): Promise<void> {
    try {
      const me = await refreshMe();
      greeting.replaceChildren(
        el('h2', { text: '欢迎回来,' + (me.user.user_name || state.userName) }),
        el('div', { class: 'row', style: 'margin-top:8px' },
          badge('商户 ID:' + me.user.user_id, 'info'),
          me.user.mfa_enabled
            ? badge('二次验证已开启', 'success')
            : badge('二次验证未开启', 'warning')),
      );
      renderChannels(channelBox, me.channels);
    } catch {
      greeting.replaceChildren(
        el('h2', { text: '欢迎回来' }),
        el('p', { class: 'error-text', text: '读取账户信息失败,请刷新页面重试。' }),
      );
    }

    try {
      const s = await api<Stats>('/api/stats');
      statsBox.replaceChildren(
        stat('今日订单', String(s.today_count ?? 0), '笔'),
        stat('今日成交', money(s.today_amount ?? 0), '元', true),
        stat('累计成交', money(s.total_amount ?? 0), '元'),
        stat('成功率', ((s.success_rate ?? 0) * 100).toFixed(1), '%'),
        stat('已退款', money(s.refund_amount ?? 0), '元'),
        stat('净收入', money(s.net_amount ?? 0), '元'),
      );
    } catch {
      statsBox.replaceChildren(
        el('p', { class: 'muted', text: '统计数据读取失败。' }),
      );
    }

    // 应用列表(概览页顺带显示一下:有几个产品、各自多少流水)。
    try {
      const res = await loadApps();
      appBox.replaceChildren();
      if (!res.apps.length) {
        // 强制绑定应用的账户:没应用就发不了支付,提示要说清楚。
        const desc = state.requireAppBinding
          ? '该账户已开启「订单必须绑定应用」:必须先建一个应用才能发起支付' +
            '(订单要归属到某个产品,不然事后分不清是哪个应用的流水)。'
          : '只有一个产品时可以先不建(订单会记在「未分类」里)。多个产品建议各建一个。';
        appBox.appendChild(emptyState(
          '还没有创建应用',
          desc,
          el('a', { class: 'btn btn-primary', href: '#/apps', text: '去建应用' }),
        ));
      } else {
        const grid = el('div', { class: 'channel-grid' });
        for (const app of res.apps) {
          grid.appendChild(el('a', { class: 'channel-card', href: '#/apps' },
            el('div', { class: 'channel-card-head' },
              el('div', { class: 'channel-card-name', text: app.name }),
              badge(String(app.order_count ?? 0) + ' 笔', 'info')),
            el('div', { class: 'channel-card-meta',
              text: '成功金额 ' + money(app.amount ?? 0) + ' 元' })));
        }
        appBox.appendChild(grid);
        if (res.unclassified.order_count > 0) {
          appBox.appendChild(el('div', { class: 'hint-text mt-4',
            text: '未分类订单 ' + res.unclassified.order_count + ' 笔,成功金额 '
              + money(res.unclassified.amount) + ' 元(建应用之前下的那些)' }));
        }
      }
    } catch {
      appBox.replaceChildren(el('p', { class: 'muted', text: '应用列表读取失败。' }));
    }
  }
}

/** 渠道状态卡片。 */
function renderChannels(box: HTMLElement, channels: { channel: string; enabled: boolean; is_default: boolean }[]): void {
  box.replaceChildren();
  if (!channels.length) {
    box.appendChild(emptyState(
      '还没有配置任何支付渠道',
      '配置一个渠道后才能发起支付。',
      el('a', { class: 'btn btn-primary', href: '#/channels', text: '去配置渠道' }),
    ));
    return;
  }
  const grid = el('div', { class: 'channel-grid' });
  for (const cfg of channels) {
    grid.appendChild(el('div', { class: 'channel-card' + (cfg.enabled ? ' is-on' : '') },
      el('div', { class: 'channel-card-head' },
        el('div', { class: 'channel-card-name', text: CHANNEL_LABELS[cfg.channel] || cfg.channel }),
        cfg.enabled ? badge('已启用', 'success') : badge('未启用', 'muted'),
        cfg.is_default ? badge('默认', 'info') : null),
      el('div', { class: 'channel-card-meta',
        text: cfg.enabled ? '下单时可用于收款' : '启用后才会出现在支付渠道列表里' })));
  }
  box.appendChild(grid);
}
