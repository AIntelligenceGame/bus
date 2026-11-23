/**
 * 应用 / 产品管理页。
 *
 * 一个商户常常有多个产品(APP、小程序、H5……)。每个产品可以配自己的一套
 * 渠道密钥(两个微信 App 各配各的 appid/mchid),出来的订单、退款、统计
 * 也能分开看。
 *
 * 这一页做三件事:
 *   1. 建 / 改 / 删应用;
 *   2. 看每个应用下的订单数与成功金额(以及「未分类」那批老订单);
 *   3. 看某个应用的**专属**渠道配置(哪些是单独配的、哪些继承全局)。
 *
 * 删除应用是危险操作:应用下还有订单时后端会拒绝(订单是钱的事实,
 * 删了应用那批订单就成了算不出报表的孤儿)。
 */

import { api } from '../lib/api';
import { el, table } from '../lib/dom';
import { loadApps } from '../lib/apps';
import { CHANNEL_LABELS, money } from '../lib/store';
import {
  badge, card, confirmDialog, emptyState, openModal, toast,
} from '../lib/ui';
import { requireVerify } from '../lib/verify';
import type { App, AppChannelsResponse, MeResponse } from '../lib/types';

export function renderApps(root: HTMLElement): void {
  // ---- 应用列表 ----
  const listBox = el('div');
  root.appendChild(card('我的应用', '一个产品建一个应用;订单按应用归类,统计和报表也能分开看',
    el('button', { class: 'btn btn-primary', id: 'app-new', text: '新建应用' }),
    listBox));

  // ---- 应用详情(点某个应用后展开) ----
  const detailBox = el('div');
  root.appendChild(card('应用详情', '查看某个应用的专属渠道配置;专属配置覆盖全局同名渠道', null, detailBox));

  // ---- 强制绑定应用(账户级策略) ----
  //
  // 不开的时候,没带 app_id 的下单会被放行,订单落进「未分类」——
  // 钱进来了,但事后没人说得清是哪个产品的。开了之后这类下单当场被拒。
  const policyBox = el('div');
  root.appendChild(card('订单必须绑定应用',
    '开启后,这个账户的每一笔支付都必须归属某个应用', null, policyBox));

  async function renderPolicy(): Promise<void> {
    policyBox.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const me = await api<MeResponse>('/api/me');
      const on = !!me.user.require_app_binding;
      const toggle = el('button', {
        class: on ? 'btn btn-secondary' : 'btn btn-primary',
        text: on ? '关闭强制绑定' : '开启强制绑定',
      }) as HTMLButtonElement;
      toggle.addEventListener('click', () => void doTogglePolicy(!on));
      policyBox.replaceChildren(
        el('div', { class: 'row' },
          badge(on ? '已开启' : '未开启', on ? 'success' : 'muted'),
          toggle),
        el('div', { class: 'hint-text', style: 'margin-top:8px',
          text: on
            ? '现在没带 app_id 的下单会被拒绝;开放接口里没绑应用的密钥也不能用来下单。' +
              '关闭之后这类下单会重新放行,订单会落进「未分类」。'
            : '现在没带 app_id 的下单会被放行,订单落进「未分类」——事后分不清是哪个产品的钱。' +
              '开启之后这类下单当场被拒。' }));
    } catch (e) {
      policyBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  async function doTogglePolicy(require: boolean): Promise<void> {
    if (require) {
      const ok = await confirmDialog(
        '开启「订单必须绑定应用」?',
        '开启之后,这个账户所有没带 app_id 的下单都会被拒绝,' +
        '开放接口里没绑定应用的密钥也不能用来下单。' +
        '请先确认你的业务后端已经在传 app_id(或者密钥已经绑了应用)。',
        true,
      );
      if (!ok) return;
    }
    try {
      await requireVerify();
      await api('/api/apps/require-binding', { method: 'POST', body: { require } });
      toast(require ? '已开启强制绑定应用' : '已关闭强制绑定应用', 'ok');
      await renderPolicy();
    } catch (e) {
      toast(errText(e), 'err');
    }
  }

  const reload = async (): Promise<void> => {
    // renderList 内部已经强制刷新了共享缓存,这里不用再刷一次。
    await renderList();
  };

  // ---------------------------------------------------------------- 列表

  async function renderList(): Promise<void> {
    listBox.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      // 走共享缓存那条路(强制刷新),不要自己再拉一次:
      // 这一页看到的应用列表要和订单页、报表页看到的是同一份,
      // 自己拉一份会把共享缓存留在旧数据上,别的页面就按旧的应用名显示了。
      const res = await loadApps(true);
      const apps = res.apps || [];
      const unclassified = res.unclassified || { order_count: 0, success_count: 0, amount: 0 };

      listBox.replaceChildren();
      if (!apps.length) {
        listBox.appendChild(emptyState(
          '还没有创建应用',
          '如果你只有一个产品,可以先不建应用(订单会记在「未分类」里)。' +
          '有多个产品时建一个,每个产品配自己的一套渠道密钥,订单和报表就能分开。',
        ));
      } else {
        const rows = apps.map((app) => [
          el('span', { class: 'cell-mono', text: app.app_id }),
          el('span', { text: app.name }),
          app.remark ? el('span', { class: 'dim', text: app.remark }) : el('span', { class: 'dim', text: '—' }),
          el('span', { class: 'num', text: String(app.order_count ?? 0) }),
          el('span', { class: 'num', text: money(app.amount ?? 0) }),
          el('td', { class: 'cell-actions' },
            viewBtn(app),
            editBtn(app),
            deleteBtn(app)),
        ]);
        listBox.appendChild(table(
          ['应用 ID', '名称', '备注', '订单数', '成功金额(元)', '操作'], rows));
        // 「未分类」那一档也要露出来:老订单都在里面,
        // 商户建了应用之后那笔钱不能凭空消失。
        if (unclassified.order_count > 0) {
          listBox.appendChild(el('div', { class: 'notice notice-info', style: 'margin-top:12px' },
            el('div', { text: '未分类订单:' + unclassified.order_count + ' 笔,成功金额 '
              + money(unclassified.amount) + ' 元' }),
            el('div', { class: 'hint-text',
              text: '这些是还没归到任何应用的订单(通常是建应用之前下的)。' +
                '可以先用它们熟悉流程,之后再决定要不要归类。' })));
        }
      }
    } catch (e) {
      listBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  function viewBtn(app: App): HTMLElement {
    const btn = el('button', { class: 'btn btn-ghost btn-sm', text: '查看配置' });
    btn.addEventListener('click', () => void showDetail(app));
    return btn;
  }

  function editBtn(app: App): HTMLElement {
    const btn = el('button', { class: 'btn btn-secondary btn-sm', text: '编辑' });
    btn.addEventListener('click', () => openAppDialog(app, reload));
    return btn;
  }

  function deleteBtn(app: App): HTMLElement {
    const btn = el('button', { class: 'btn btn-danger btn-sm', text: '删除' });
    btn.addEventListener('click', () => void doDelete(app));
    return btn;
  }

  async function doDelete(app: App): Promise<void> {
    const ok = await confirmDialog(
      '删除应用「' + app.name + '」?',
      '该应用下如果还有订单,平台会拒绝删除(那些订单要用这个应用去对账和退款)。' +
      '绑在这个应用上的开放接口密钥也一样:得先吊销密钥才能删应用。' +
      '删掉之后,这个应用就不再出现在订单筛选与报表里。',
      true,
    );
    if (!ok) return;
    try {
      await requireVerify();
      await api('/api/apps/' + encodeURIComponent(app.app_id), { method: 'DELETE' });
      toast('应用已删除', 'ok');
      await reload();
    } catch (e) {
      toast(errText(e), 'err');
    }
  }

  // ---------------------------------------------------------------- 详情

  async function showDetail(app: App): Promise<void> {
    detailBox.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const res = await api<AppChannelsResponse>('/api/apps/' + encodeURIComponent(app.app_id) + '/channels');
      const channels = res.channels || [];
      detailBox.replaceChildren();
      detailBox.appendChild(el('div', { class: 'row', style: 'margin-bottom:12px' },
        el('h3', { text: app.name }),
        badge('应用 ID:' + app.app_id, 'info'),
        app.remark ? el('span', { class: 'dim', text: app.remark }) : null));

      if (!channels.length) {
        detailBox.appendChild(emptyState(
          '这个应用还没有渠道配置',
          '它现在用的是全局那套渠道配置。要给它单独配一套' +
          '(微信 AppID / 商户号、支付宝 AppID、Apple Pay / Google Pay 的收单账户),' +
          '到「渠道配置」页选择这个应用后再保存。',
        ));
      } else {
        const rows = channels.map((cfg) => [
          CHANNEL_LABELS[cfg.channel] || cfg.channel,
          cfg.app_specific
            ? badge('专属配置', 'info')
            : badge('继承全局', 'muted'),
          cfg.enabled ? badge('已启用', 'success') : badge('未启用', 'muted'),
          cfg.is_default ? badge('默认', 'info') : el('span', { class: 'dim', text: '—' }),
          describeChannel(cfg),
        ]);
        detailBox.appendChild(table(
          ['渠道', '来源', '状态', '默认', '配置摘要'], rows));
      }

      // 回调地址:应用专属的回调地址带了应用标识,验签用的是这个应用自己的密钥。
      const notify = res.notify_urls || {};
      const rows2 = Object.entries(notify).map(([ch, url]) => [
        CHANNEL_LABELS[ch] || ch,
        el('span', { class: 'cell-mono', text: url }),
      ]);
      if (rows2.length) {
        detailBox.appendChild(el('div', { class: 'section-title', style: 'margin-top:16px',
          text: '回调地址(微信 / 支付宝填到对方后台;钱包渠道填到收单机构的 Webhook 地址)' }));
        detailBox.appendChild(table(['渠道', '回调地址'], rows2));
        detailBox.appendChild(el('div', { class: 'hint-text',
          text: '这里的地址带了应用标识 —— 平台用「这个应用自己的密钥」验签,' +
            '所以不要手动去掉那一段,否则回调会被判为签名不合法。' }));
      }
    } catch (e) {
      detailBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  // ---------------------------------------------------------------- 新建

  const newBtn = document.getElementById('app-new') as HTMLButtonElement | null;
  if (newBtn) newBtn.addEventListener('click', () => openAppDialog(null, reload));

  void renderList();
  void renderPolicy();
}

/** 渠道配置摘要(不显示密钥明文,只说「有没有」)。 */
function describeChannel(cfg: {
  channel: string;
  wechat?: { app_id?: string; mch_id?: string };
  alipay?: { app_id?: string };
  wallet?: { merchant_id?: string };
}): string {
  if (cfg.channel === 'wechat') {
    const w = cfg.wechat;
    if (!w) return '—';
    return [w.app_id ? 'AppID ' + w.app_id : '', w.mch_id ? '商户号 ' + w.mch_id : '']
      .filter(Boolean).join(' · ') || '未填写';
  }
  if (cfg.channel === 'alipay') {
    const a = cfg.alipay;
    if (!a) return '—';
    return a.app_id ? 'AppID ' + a.app_id : '未填写';
  }
  if (cfg.channel === 'applepay' || cfg.channel === 'googlepay') {
    const w = cfg.wallet;
    if (!w) return '—';
    return w.merchant_id ? '商户号 ' + w.merchant_id : '未填写';
  }
  return '—';
}

/**
 * 新建 / 编辑应用对话框。
 *
 * 应用名必须唯一(后端唯一索引保证),重复会返回 409,这里直接把后端的
 * 提示显示出来。
 */
function openAppDialog(app: App | null, onSaved: () => Promise<void>): void {
  const nameInput = el('input', {
    type: 'text', id: 'app-name', placeholder: '例如:iOS 客户端',
    value: app ? app.name : '',
  }) as HTMLInputElement;
  const remarkInput = el('input', {
    type: 'text', id: 'app-remark', placeholder: '选填',
    value: app ? app.remark || '' : '',
  }) as HTMLInputElement;

  const body = el('div', {},
    el('div', { class: 'field' },
      el('label', { class: 'label', text: '应用名称', for: 'app-name' }),
      nameInput,
      el('div', { class: 'hint-text', text: '同一个商户下不能重名 —— 名字是报表里区分产品的标识' })),
    el('div', { class: 'field' },
      el('label', { class: 'label', text: '备注', for: 'app-remark' }),
      remarkInput));

  const handle = openModal({
    title: app ? '编辑应用' : '新建应用',
    desc: app ? '应用 ID:' + app.app_id : '建好之后可以给它单独配一套渠道密钥',
    body,
    okText: app ? '保存' : '创建',
    onOk: async (h) => {
      const name = nameInput.value.trim();
      if (!name) {
        h.error('请填写应用名称');
        return;
      }
      try {
        await requireVerify();
        await api(app ? '/api/apps/' + encodeURIComponent(app.app_id) : '/api/apps', {
          method: 'POST',
          body: { name, remark: remarkInput.value.trim() },
        });
        toast(app ? '应用已更新' : '应用已创建', 'ok');
        h.close(true);
        await onSaved();
      } catch (e) {
        h.error(errText(e));
      }
    },
  });
  window.setTimeout(() => nameInput.focus(), 30);
  void handle;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
