/** 支付体验页:发起支付、生成二维码、查看客户端参数。 */

import { api, apiBlobURL, releaseBlobURL } from '../lib/api';
import { el } from '../lib/dom';
import { loadApps } from '../lib/apps';
import { CHANNEL_LABELS, money, refreshMe, state, yuanToCents } from '../lib/store';
import { card, emptyState, toast, withBusy } from '../lib/ui';
import { requireVerify } from '../lib/verify';
import type { PayResponse, PaySession } from '../lib/types';

/** 各渠道支持的交易类型(和后端 provider 的 SupportedTradeTypes 对齐)。 */
const TRADE_TYPES_BY_CHANNEL: Record<string, { value: string; label: string }[]> = {
  wechat: [
    { value: 'native', label: '扫码支付' },
    { value: 'app', label: 'APP 拉起' },
    { value: 'jsapi', label: 'JSAPI / 公众号' },
    { value: 'h5', label: 'H5 网页' },
  ],
  alipay: [
    { value: 'native', label: '扫码支付(当面付)' },
    { value: 'app', label: 'APP 拉起' },
    { value: 'wap', label: '手机网站' },
    { value: 'web', label: '电脑网站' },
  ],
  applepay: [{ value: 'wallet', label: '钱包支付' }],
  googlepay: [{ value: 'wallet', label: '钱包支付' }],
};

const ALL_TRADE_TYPES: { value: string; label: string }[] = [
  { value: 'native', label: '扫码支付' },
  { value: 'app', label: 'APP 拉起' },
  { value: 'jsapi', label: 'JSAPI / 公众号' },
  { value: 'h5', label: 'H5 网页' },
  { value: 'wap', label: '手机网站' },
  { value: 'web', label: '电脑网站' },
  { value: 'wallet', label: '钱包支付' },
];

export function renderPlayground(root: HTMLElement): void {
  const out = el('div');

  const channelSelect = el('select', { id: 'pay-channel' }) as HTMLSelectElement;
  const tradeSelect = el('select', { id: 'pay-trade' }) as HTMLSelectElement;

  /**
   * 按当前渠道重建交易类型下拉。
   *
   * 各渠道支持的交易类型不一样(支付宝没有 jsapi/h5,钱包只有 wallet),
   * 混在一起的话用户选一个必然被后端拒,看起来就像「功能坏了」。
   */
  const syncTradeTypes = (): void => {
    const channel = channelSelect.value;
    const list = channel ? (TRADE_TYPES_BY_CHANNEL[channel] || []) : ALL_TRADE_TYPES;
    tradeSelect.replaceChildren(el('option', { value: '', text: '自动(按渠道推断)' }));
    for (const t of list) {
      tradeSelect.appendChild(el('option', { value: t.value, text: t.label }));
    }
  };
  syncTradeTypes();
  channelSelect.addEventListener('change', syncTradeTypes);

  const amountInput = el('input', { type: 'text', id: 'pay-amount', placeholder: '0.01', value: '0.01' });
  // 应用(产品):选了哪个应用,这笔单就用那个应用自己的渠道配置与回调地址。
  const appSelect = el('select', { id: 'pay-app' }) as HTMLSelectElement;
  appSelect.appendChild(el('option', { value: '', text: '未分类(用全局配置)' }));
  const noInput = el('input', { type: 'text', id: 'pay-no', placeholder: '留空自动生成' });
  const subjectInput = el('input', { type: 'text', id: 'pay-subject', placeholder: '留空自动生成' });
  const payerInput = el('input', { type: 'text', id: 'pay-payer', placeholder: 'openid / 用户 ID(JSAPI 必填)' });
  const attachInput = el('input', { type: 'text', id: 'pay-attach', placeholder: '原样回传' });

  const submit = el('button', { class: 'btn btn-primary', text: '发起支付' }) as HTMLButtonElement;

  const field = (label: string, input: HTMLElement, hint?: string): HTMLElement =>
    el('div', { class: 'field' },
      el('label', { class: 'label', text: label, for: input.id }),
      input,
      hint ? el('div', { class: 'hint-text', text: hint }) : null);

  root.appendChild(card('发起支付', '金额单位为元,提交时换算成「分」;不选渠道时走默认渠道',
    null,
    el('div', { class: 'grid grid-3' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '支付渠道', for: 'pay-channel' }),
        channelSelect),
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '交易类型', for: 'pay-trade' }),
        tradeSelect),
      field('金额(元)', amountInput),
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '应用(产品)', for: 'pay-app' }),
        appSelect)),
    el('div', { class: 'grid grid-3' },
      field('商户订单号', noInput),
      field('商品标题', subjectInput),
      field('付款人标识', payerInput)),
    field('附加数据', attachInput),
    el('div', { class: 'row' }, submit)));

  root.appendChild(card('支付结果', '二维码、跳转地址与客户端调起参数', null, out));

  submit.addEventListener('click', () =>
    void withBusy(submit, async () => {
      // 金额按**字符串**精确解析成「分」:不用 Number(x)*100,
      // 那样 8.115 会被浮点悄悄改成 8.11(用户输入的和实际扣钱的对不上),
      // 而且超过两位小数的输入会被静默截断 —— 后端是会直接报错的。
      const cents = yuanToCents(amountInput.value);
      if (cents === null) {
        toast('请输入正确的金额(最多两位小数,必须大于 0)', 'err');
        return;
      }
      // 强制绑定应用的账户:订单必须归属某个应用,后端会拒掉没带 app_id 的下单。
      // 这里先拦一道并说清楚原因,省得用户只看到一个「400」。
      if (state.requireAppBinding && !appSelect.value) {
        toast('该账户已开启「订单必须绑定应用」:请先选择一个应用再发起支付', 'err');
        return;
      }
      out.replaceChildren(el('p', { class: 'muted', text: '正在向渠道下单…' }));
      try {
        await requireVerify();
        const res = await api<PayResponse>('/api/pay', {
          method: 'POST',
          body: {
            channel: channelSelect.value,
            trade_type: tradeSelect.value,
            app_id: appSelect.value,
            out_trade_no: noInput.value.trim(),
            subject: subjectInput.value.trim(),
            amount: cents,
            payer_id: payerInput.value.trim(),
            attach: attachInput.value.trim(),
          },
        });
        toast('订单 ' + res.order_no + ' 已创建,金额 ' + res.yuan + ' 元', 'ok');
        renderResult(out, res);
      } catch (e) {
        out.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
        toast(errText(e), 'err');
      }
    }));

  void refreshMe()
    .then(() => {
      channelSelect.replaceChildren(el('option', { value: '', text: '默认渠道' }));
      for (const ch of state.channels) {
        if (!ch.enabled) continue;
        channelSelect.appendChild(el('option', {
          value: ch.channel,
          text: (CHANNEL_LABELS[ch.channel] || ch.channel) + (ch.is_default ? '(默认)' : ''),
        }));
      }
      syncTradeTypes();
    })
    .catch(() => undefined);

  // 应用下拉。
  void loadApps()
    .then((res) => {
      for (const app of res.apps) {
        appSelect.appendChild(el('option', { value: app.app_id, text: app.name }));
      }
    })
    .catch(() => undefined);
}

/**
 * 当前展示的二维码 object URL。
 *
 * object URL 会把 Blob 一直钉在内存里,不释放就一直在涨 ——
 * 反复发起支付几十次,页面会明显变卡(手机端更早)。
 * 所以每次换二维码之前先把旧的释放掉。
 */
let currentQRCodeURL: string | null = null;

/**
 * 二维码代次号。
 *
 * 二维码是异步取的:请求发出时页面可能还是这一版,响应回来时用户
 * 已经切走页面 / 重新发起了一笔支付。这时候 currentQRCodeURL 还没被赋值,
 * 释放函数看不到这个 URL —— 它就成了没人管的孤儿,一直占着内存。
 *
 * 用代次号兜住:响应回来时先看自己是不是最新那次,不是就当场释放。
 */
let qrGeneration = 0;

/** 释放当前二维码的 object URL(页面重绘、离开页面时调用)。 */
export function releasePlaygroundResources(): void {
  qrGeneration++;
  releaseBlobURL(currentQRCodeURL);
  currentQRCodeURL = null;
}

function renderResult(box: HTMLElement, res: PayResponse): void {
  const session: Partial<PaySession> = res.session || {};
  box.replaceChildren();
  // 旧二维码不再显示,先释放。
  releasePlaygroundResources();

  // 摘要
  box.appendChild(el('div', { class: 'row', style: 'margin-bottom:16px' },
    el('span', { class: 'dim', text: '订单号' }),
    el('span', { class: 'mono', text: res.order_no }),
    el('span', { class: 'dim', text: '金额' }),
    el('span', { class: 'mono num', text: money(res.amount) + ' 元' })));

  if (res.qrcode_url) {
    // 二维码图片需要令牌,这里用请求头取(令牌不拼进 URL)。
    const img = el('img', { alt: '支付二维码' });
    const box2 = el('div', { class: 'qr-box' }, img, el('div', { class: 'dim', text: '扫码支付' }));
    box.appendChild(box2);
    // 记下这一版的代次号:响应回来时页面可能已经重绘过了。
    const gen = qrGeneration;
    void apiBlobURL(res.qrcode_url)
      .then((url) => {
        if (gen !== qrGeneration) {
          // 页面已经重绘 / 用户已经离开 —— 这个 URL 没人管,当场释放。
          releaseBlobURL(url);
          return;
        }
        img.src = url;
        currentQRCodeURL = url;
      })
      .catch(() => {
        box2.appendChild(el('div', { class: 'error-text', text: '二维码加载失败,请重新发起支付' }));
      });
  }

  if (session.redirect_url) {
    box.appendChild(el('div', { class: 'notice notice-info' },
      el('div', { style: 'margin-bottom:4px', text: 'H5 / 网页支付跳转地址' }),
      el('a', { href: session.redirect_url, target: '_blank', rel: 'noopener', text: session.redirect_url })));
  }
  if (session.token) {
    box.appendChild(el('div', { class: 'notice notice-info', text: '钱包支付令牌:' + session.token }));
  }
  if (session.app_params) {
    box.appendChild(el('div', { class: 'section-title', text: '原始调起参数' }));
    box.appendChild(el('pre', { text: JSON.stringify(session.app_params, null, 2) }));
  }
  if (session.client_params) {
    box.appendChild(el('div', { class: 'section-title', text: '客户端调起参数(client_params)' }));
    box.appendChild(el('p', { class: 'muted',
      text: '字段名已按各端 SDK 对齐,Swift / Kotlin 客户端可直接取用:'
            + '微信 wechat.prepay_id、支付宝 alipay.order_string、'
            + 'Apple Pay apple_pay.merchant_id、Google Pay google_pay.client_secret。' }));
    box.appendChild(el('pre', { text: JSON.stringify(session.client_params, null, 2) }));
  }
  if (res.usage && res.usage.length) {
    box.appendChild(el('div', { class: 'section-title', text: '接入说明' }));
    const usage = el('ul', { class: 'bullets' });
    for (const line of res.usage) usage.appendChild(el('li', { text: line }));
    box.appendChild(usage);
  }
  if (!res.qrcode_url && !session.redirect_url && !session.token && !session.app_params && !session.client_params) {
    box.appendChild(emptyState('这笔订单没有可展示的会话信息', '只有扫码 / 网页 / 钱包支付会返回二维码或调起参数。'));
  }
  box.appendChild(el('div', { class: 'hint-text mt-4', text: '回调地址:' + res.notify_url }));
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
