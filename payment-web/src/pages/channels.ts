/** 渠道配置页:填写密钥、启用渠道、设为默认。 */

import { api } from '../lib/api';
import { el } from '../lib/dom';
import { loadApps } from '../lib/apps';
import { CHANNEL_LABELS, refreshMe } from '../lib/store';
import {
  badge, card, checkbox, copyText, emptyState, field, markSecret,
  switchRow, toast, val, withBusy,
} from '../lib/ui';
import { requireVerify } from '../lib/verify';
import { SeqGuard } from '../lib/seq';
import type { AppChannelsResponse, ChannelConfig, MeResponse } from '../lib/types';

const CHANNELS = ['wechat', 'alipay', 'applepay', 'googlepay'] as const;

export function renderChannels(root: HTMLElement): void {
  // 请求序号守卫:作用域连切两次时,后到的旧响应不能覆盖新的表单内容。
  // 覆盖的后果很严重:表格里显示的是 A 应用的配置,用户却以为自己在编辑 B,
  // 保存时就把 A 的密钥写到了 B 的应用上。
  //
  // 声明必须在第一次 load() 调用**之前** —— 否则 load 里读 guard 会撞上
  // const 的暂时性死区(页面直接白屏)。
  const guard = new SeqGuard();

  /**
   * 当前的作用范围(空 = 全局)。
   *
   * 决定了三件事:
   *   - 表单里回填哪套配置;
   *   - 保存时写到全局还是写到那个应用名下;
   *   - 回调地址用哪套密钥验签(所以地址里带不带应用标识也不同)。
   *
   * 声明必须在第一次 load() 调用**之前**:load 一进来就读 scope,
   * 撞上 let 的暂时性死区会让整页白屏。
   */
  let scope = '';

  // ---- 作用范围(全局 / 某个应用) ----
  //
  // 一个商户有多个产品时,每个产品可以用自己的一套渠道密钥
  // (两个微信 App 各配各的 appid/mchid)。这里选「全局」= 所有应用共用的
  // 那套配置;选某个应用 = 只改这个应用自己的那套(它没配过的渠道仍然
  // 回退到全局)。
  const scopeSel = el('select', { id: 'ch-scope' }) as HTMLSelectElement;
  scopeSel.appendChild(el('option', { value: '', text: '全局(所有应用共用)' }));
  const scopeHint = el('div', { class: 'hint-text' });
  root.appendChild(card('作用范围', '决定下面这套配置是全局的,还是只属于某个产品',
    null,
    el('div', { class: 'field' },
      el('label', { class: 'label', text: '配置范围', for: 'ch-scope' }), scopeSel),
    scopeHint));

  // ---- 渠道总览 ----
  const overview = el('div');
  root.appendChild(card('我的支付渠道', '启用后即可用于收款;设为默认后,下单不指定渠道就走它', null, overview));

  // ---- 回调地址(对外访问地址) ----
  const baseCard = buildNotifyBaseCard();
  root.appendChild(baseCard.wrap);

  // ---- 微信 ----
  const wechatCard = buildWechatCard();
  root.appendChild(wechatCard.wrap);

  // ---- 支付宝 ----
  const alipayCard = buildAlipayCard();
  root.appendChild(alipayCard.wrap);

  // ---- 钱包 ----
  const walletCard = buildWalletCard();
  root.appendChild(walletCard.wrap);

  void load();

  /** 当前范围的查询串(空 = 全局)。 */
  const scopeQuery = (): string =>
    scope ? '?app_id=' + encodeURIComponent(scope) : '';

  async function load(): Promise<void> {
    const seq = guard.next();
    // 作用域要在发请求**之前**锁定。
    // 之前是 await refreshMe() 之后才读 scope,期间用户切了作用域,
    // 于是这个请求虽然是为旧作用域发出的,却拿去请求了新作用域 ——
    // 两个请求都指向新作用域,谁先回来谁说了算。
    const currentScope = scope;
    try {
      // 「回调地址(对外访问地址)」是**账户级**的设置(不管在看哪个范围的
      // 渠道配置,都得显示当前生效值),所以每次都先拉一次 /api/me。
      const me = await refreshMe();
      if (!guard.isCurrent(seq)) return;
      fillNotifyBase(me);
      if (currentScope) {
        // 应用范围:渠道配置从应用接口取(合并后的结果)。
        const res = await api<AppChannelsResponse>(
          '/api/apps/' + encodeURIComponent(currentScope) + '/channels');
        if (!guard.isCurrent(seq)) return;
        renderOverview(overview, res.channels || []);
        fillForms(res.channels || []);
        applyNotifyUrls(res.notify_urls || {});
        scopeHint.textContent = '当前编辑的是某个应用「专属」的配置:'
          + '该应用没单独配过的渠道,仍然回退到全局那套。';
      } else {
        renderOverview(overview, me.channels);
        fillForms(me.channels);
        applyNotifyUrls(me.notify_urls);
        scopeHint.textContent = '当前编辑的是全局配置:所有没有专属配置的应用都用这一套。';
      }
    } catch (e) {
      if (!guard.isCurrent(seq)) return;
      toast(errText(e), 'err');
    }
  }

  scopeSel.addEventListener('change', () => {
    scope = scopeSel.value;
    void load();
  });

  // 应用下拉(拉一次就够,应用列表变动很少)。
  void loadApps()
    .then((res) => {
      for (const app of res.apps) {
        scopeSel.appendChild(el('option', { value: app.app_id, text: app.name }));
      }
    })
    .catch(() => undefined);

  /** 把后端算好的回调地址刷到页面上的各个渠道卡片。 */
  function applyNotifyUrls(urls: Record<string, string>): void {
    setText('w-notify', urls.wechat || '');
    setText('a-notify', urls.alipay || '');
    markLocalNotify(urls.wechat || '');
    markLocalNotify(urls.alipay || '');
  }

  // ---------------------------------------------------------------- 渠道卡片

  /**
   * 「回调地址(对外访问地址)」卡片。
   *
   * 平台用这个地址生成给微信 / 支付宝的回调地址(填到对方后台)。
   * 以前这个地址只能靠启动参数 -base 给,改一次要改命令行、重启一次进程 ——
   * 太死板。现在可以在页面上填自己的域名,存库后立即生效,不用重启。
   *
   * 留空 = 用平台默认地址(启动参数 -base)。
   */
  function buildNotifyBaseCard(): { wrap: HTMLElement } {
    const input = el('input', {
      type: 'text', id: 'nb-base', placeholder: 'https://pay.example.com', autocomplete: 'off',
    }) as HTMLInputElement;
    const saveBtn = el('button', { class: 'btn btn-primary', text: '保存回调地址' }) as HTMLButtonElement;
    const resetBtn = el('button', { class: 'btn btn-secondary', text: '恢复平台默认' }) as HTMLButtonElement;
    const status = el('div', { class: 'hint-text', id: 'nb-status', text: '(加载中…)' });
    const defLine = el('div', { class: 'hint-text', id: 'nb-default' });
    const warn = el('div', { class: 'error-text', id: 'nb-warn' });

    /** 保存(传空字符串 = 恢复平台默认)。 */
    const save = async (value: string): Promise<void> => {
      try {
        await requireVerify();
        const res = await api<{
          ok: boolean;
          notify_base_url?: string;
          notify_base_default?: string;
          notify_urls?: Record<string, string>;
        }>('/api/account/notify-base', {
          method: 'POST',
          body: { notify_base_url: value },
        });
        if (res.ok === false) throw new Error('保存失败');
        toast(value ? '回调地址已更新,记得把新地址同步到微信 / 支付宝后台' : '已恢复平台默认回调地址', 'ok');
        await load();
      } catch (e) {
        toast(errText(e), 'err');
      }
    };

    saveBtn.addEventListener('click', () =>
      void withBusy(saveBtn, () => save(input.value.trim())));
    resetBtn.addEventListener('click', () =>
      void withBusy(resetBtn, async () => {
        input.value = '';
        await save('');
      }));

    // 输入时就提示「这个域名和当前平台地址不一样」——
    // 回调地址填错域名是这类配置最容易犯、后果最隐蔽的错(订单永远收不到回调)。
    input.addEventListener('input', () => checkNotifyBaseWarn(input.value.trim()));

    const wrap = card(
      '回调地址(对外访问地址)',
      '平台用它生成给微信 / 支付宝的回调地址。默认取平台启动参数 -base;在这里填自己的域名后,换域名不用重启平台',
      null,
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '对外访问地址', for: 'nb-base' }),
        el('div', { class: 'row' },
          el('div', { style: 'flex:1;min-width:260px' }, input),
          saveBtn,
          resetBtn),
        el('div', { class: 'hint-text', text: '只填到域名即可(https://pay.example.com);留空表示用平台默认地址' })),
      status,
      defLine,
      warn,
      el('div', { class: 'hint-text',
        text: '这个域名必须能访问到本平台(把自己的域名反代到平台即可)。' +
          '改完地址后,请把新的回调地址同步到微信 / 支付宝后台 —— 那边不会自动跟着变。' }));

    return { wrap };
  }

  function buildWechatCard(): { wrap: HTMLElement } {
    const saveBtn = el('button', { class: 'btn btn-primary', text: '保存微信配置' }) as HTMLButtonElement;
    const testBtn = el('button', { class: 'btn btn-secondary', text: '测试连通性' }) as HTMLButtonElement;

    const wrap = card('微信支付(APIv3)', '商户号 + APIv3 密钥 + 商户证书私钥',
      el('div', { class: 'row' }, testBtn, saveBtn),
      channelToggles('wechat'),
      el('div', { class: 'grid grid-3' },
        field({ id: 'w-appid', label: 'AppID', placeholder: 'wx1234567890' }),
        field({ id: 'w-mchid', label: '商户号 MchID', placeholder: '1900000109' }),
        field({ id: 'w-serial', label: '证书序列号 SerialNo' })),
      el('div', { class: 'grid grid-2' },
        field({ id: 'w-apiv3key', label: 'APIv3 密钥', secret: true, hint: '32 字节,用于回调解密' }),
        el('div', { class: 'field' },
          el('label', { class: 'label', text: '平台证书缓存目录', for: 'w-certdir' }),
          el('input', { type: 'text', id: 'w-certdir', value: '由平台分配', disabled: true }),
          el('div', { class: 'hint-text', text: '由平台按商户目录分配,不可修改' }))),
      field({ id: 'w-privkey', label: '商户 API 证书私钥', textarea: true, hint: 'apiclient_key.pem 的完整内容(含 BEGIN/END 行)' }),
      switchRow('w-autofetch', '自动下载平台证书', '首次支付时自动拉取并缓存微信平台证书', true),
      notifyLine('w-notify', 'w-copy-notify'));

    saveBtn.addEventListener('click', () =>
      void withBusy(saveBtn, async () => {
        try {
          await requireVerify();
          await saveChannel('wechat', buildWechat(), scope);
          toast('微信配置已保存(密钥已加密入库)', 'ok');
          await load();
        } catch (e) {
          toast(errText(e), 'err');
        }
      }));

    testBtn.addEventListener('click', () =>
      void withBusy(testBtn, async () => {
        try {
          await requireVerify();
          const res = await api<{ ok: boolean; error?: string; message?: string; endpoint?: string }>(
            '/api/channels/wechat/test' + scopeQuery(), { method: 'POST' });
          if (!res.ok) throw new Error(testErrorText(res));
          toast(testText(res), 'ok');
        } catch (e) {
          toast(errText(e), 'err');
        }
      }));

    return { wrap };
  }

  function buildAlipayCard(): { wrap: HTMLElement } {
    const saveBtn = el('button', { class: 'btn btn-primary', text: '保存支付宝配置' }) as HTMLButtonElement;
    // 支付宝这边会真的联调一次网关(查一个不存在的单号),
    // 所以按钮叫「测试连通性」——它确实能验出「网关不通 / 签名不对」。
    const testBtn = el('button', { class: 'btn btn-secondary', text: '测试连通性' }) as HTMLButtonElement;

    const wrap = card('支付宝', '开放平台应用私钥 + 支付宝公钥',
      el('div', { class: 'row' }, testBtn, saveBtn),
      channelToggles('alipay'),
      el('div', { class: 'grid grid-2' },
        field({ id: 'a-appid', label: 'AppID', placeholder: '2021000000000000' }),
        field({ id: 'a-gateway', label: '网关地址', placeholder: 'https://openapi.alipay.com/gateway.do' })),
      el('div', { class: 'grid grid-2' },
        field({ id: 'a-privkey', label: '应用私钥(RSA2)', textarea: true }),
        field({ id: 'a-pubkey', label: '支付宝公钥', textarea: true })),
      field({ id: 'a-certpath', label: '支付宝公钥证书路径', placeholder: '/path/to/alipayCertPublicKey.crt', optional: '证书模式必填' }),
      el('div', { class: 'grid grid-2' },
        field({ id: 'a-appcertpath', label: '应用公钥证书路径', placeholder: '/path/to/appCertPublicKey.crt', optional: '证书模式必填' }),
        field({ id: 'a-rootcertpath', label: '支付宝根证书路径', placeholder: '/path/to/alipayRootCert.crt', optional: '证书模式必填' })),
      notifyLine('a-notify', 'a-copy-notify'));

    saveBtn.addEventListener('click', () =>
      void withBusy(saveBtn, async () => {
        try {
          await requireVerify();
          await saveChannel('alipay', buildAlipay(), scope);
          toast('支付宝配置已保存(密钥已加密入库)', 'ok');
          await load();
        } catch (e) {
          toast(errText(e), 'err');
        }
      }));

    testBtn.addEventListener('click', () =>
      void withBusy(testBtn, async () => {
        try {
          await requireVerify();
          const res = await api<{ ok: boolean; error?: string; message?: string; endpoint?: string }>(
            '/api/channels/alipay/test' + scopeQuery(), { method: 'POST' });
          if (!res.ok) throw new Error(testErrorText(res));
          toast(testText(res), 'ok');
        } catch (e) {
          toast(errText(e), 'err');
        }
      }));

    return { wrap };
  }

  function buildWalletCard(): { wrap: HTMLElement } {
    const appleBtn = el('button', { class: 'btn btn-primary', text: '保存 Apple Pay 配置' }) as HTMLButtonElement;
    const googleBtn = el('button', { class: 'btn btn-primary', text: '保存 Google Pay 配置' }) as HTMLButtonElement;
    const appleTestBtn = el('button', { class: 'btn btn-secondary', text: '校验配置' }) as HTMLButtonElement;
    const googleTestBtn = el('button', { class: 'btn btn-secondary', text: '校验配置' }) as HTMLButtonElement;

    const wrap = card('Apple Pay / Google Pay', '钱包不直接与商户结算,需要收单机构(默认内置 Stripe);两个钱包的收单配置各自独立,互不影响;同样支持按应用单独配一套',
      el('div', { class: 'row' }, googleTestBtn, googleBtn, appleTestBtn, appleBtn),
      el('div', { class: 'grid grid-2' },
        el('div', {},
          el('div', { class: 'section-title', text: 'Apple Pay' }),
          channelToggles('applepay')),
        el('div', {},
          el('div', { class: 'section-title', text: 'Google Pay' }),
          channelToggles('googlepay'))),
      el('div', { class: 'grid grid-2' },
        el('div', {},
          el('div', { class: 'section-title', text: 'Apple Pay 收单配置' }),
          el('div', { class: 'grid grid-2' },
            field({ id: walletField('applepay', 'merchant'), label: '钱包商户号', placeholder: 'merchant.com.example' }),
            field({ id: walletField('applepay', 'display'), label: '展示名称' })),
          el('div', { class: 'grid grid-2' },
            field({ id: walletField('applepay', 'domain'), label: '商户域名', placeholder: 'pay.example.com' }),
            field({ id: walletField('applepay', 'currency'), label: '币种', value: 'CNY' })),
          el('div', { class: 'grid grid-3' },
            field({ id: walletField('applepay', 'stripe-secret'), label: '收单私钥', secret: true, hint: 'stripe secret_key' }),
            field({ id: walletField('applepay', 'stripe-publishable'), label: '收单可发布密钥' }),
            field({ id: walletField('applepay', 'stripe-webhook'), label: 'Webhook 签名密钥', secret: true }))),
        el('div', {},
          el('div', { class: 'section-title', text: 'Google Pay 收单配置' }),
          el('div', { class: 'grid grid-2' },
            field({ id: walletField('googlepay', 'merchant'), label: '钱包商户号', placeholder: 'merchant.com.example' }),
            field({ id: walletField('googlepay', 'display'), label: '展示名称' })),
          el('div', { class: 'grid grid-2' },
            field({ id: walletField('googlepay', 'domain'), label: '商户域名', placeholder: 'pay.example.com' }),
            field({ id: walletField('googlepay', 'currency'), label: '币种', value: 'CNY' })),
          el('div', { class: 'grid grid-3' },
            field({ id: walletField('googlepay', 'stripe-secret'), label: '收单私钥', secret: true, hint: 'stripe secret_key' }),
            field({ id: walletField('googlepay', 'stripe-publishable'), label: '收单可发布密钥' }),
            field({ id: walletField('googlepay', 'stripe-webhook'), label: 'Webhook 签名密钥', secret: true })))));

    const save = async (channel: string): Promise<void> => {
      try {
        await requireVerify();
        await saveChannel(channel, buildWallet(channel), scope);
        toast((channel === 'applepay' ? 'Apple Pay' : 'Google Pay') + '配置已保存', 'ok');
        await load();
      } catch (e) {
        toast(errText(e), 'err');
      }
    };
    appleBtn.addEventListener('click', () => void withBusy(appleBtn, () => save('applepay')));
    googleBtn.addEventListener('click', () => void withBusy(googleBtn, () => save('googlepay')));

    const test = async (channel: string): Promise<void> => {
      try {
        await requireVerify();
        const res = await api<{ ok: boolean; error?: string; message?: string; endpoint?: string }>(
          `/api/channels/${channel}/test` + scopeQuery(), { method: 'POST' });
        if (!res.ok) throw new Error(testErrorText(res));
        toast(testText(res), 'ok');
      } catch (e) {
        toast(errText(e), 'err');
      }
    };
    appleTestBtn.addEventListener('click', () => void withBusy(appleTestBtn, () => test('applepay')));
    googleTestBtn.addEventListener('click', () => void withBusy(googleTestBtn, () => test('googlepay')));

    return { wrap };
  }

  // ---------------------------------------------------------------- 组包

  function buildWechat() {
    return {
      enabled: isChecked('ch-enabled-wechat'),
      is_default: isChecked('ch-default-wechat'),
      wechat: {
        app_id: val('w-appid'),
        mch_id: val('w-mchid'),
        serial_no: val('w-serial'),
        private_key_pem: val('w-privkey'),
        api_v3_key: val('w-apiv3key'),
        cert_dir: val('w-certdir'),
        auto_fetch_certs: isChecked('w-autofetch'),
      },
    };
  }

  function buildAlipay() {
    return {
      enabled: isChecked('ch-enabled-alipay'),
      is_default: isChecked('ch-default-alipay'),
      alipay: {
        app_id: val('a-appid'),
        private_key_pem: val('a-privkey'),
        alipay_public_key_pem: val('a-pubkey'),
        alipay_public_cert_path: val('a-certpath'),
        app_cert_path: val('a-appcertpath'),
        root_cert_path: val('a-rootcertpath'),
        gateway: val('a-gateway'),
      },
    };
  }

  function buildWallet(channel: string) {
    return {
      enabled: isChecked(`ch-enabled-${channel}`),
      is_default: isChecked(`ch-default-${channel}`),
      wallet: {
        merchant_id: val(walletField(channel, 'merchant')),
        display_name: val(walletField(channel, 'display')),
        domain_name: val(walletField(channel, 'domain')),
        currency: val(walletField(channel, 'currency')),
        stripe_secret_key: val(walletField(channel, 'stripe-secret')),
        stripe_publishable_key: val(walletField(channel, 'stripe-publishable')),
        stripe_webhook_secret: val(walletField(channel, 'stripe-webhook')),
      },
    };
  }
}

// ---------------------------------------------------------------- 辅助

/**
 * 渠道卡片里的「启用 / 设为默认」开关。
 *
 * 放在渠道卡片里(而不是集中放在总览卡片里),是为了让用户看到的开关
 * 和点下去的那个保存按钮属于同一个渠道 —— 否则「勾了启用却要点别的
 * 渠道的保存才生效」,看起来就像开关没用。
 */
function channelToggles(channel: string): HTMLElement {
  return el('div', { class: 'row', style: 'gap:24px;margin-bottom:14px' },
    checkbox(`ch-enabled-${channel}`, '启用该渠道', false),
    checkbox(`ch-default-${channel}`, '设为默认渠道', false));
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * 测试结果文案。
 *
 * 带上实际请求的地址:「测试连通性」最没用的形态就是只回一句「校验通过」——
 * 用户不知道平台替自己去哪儿问了谁,出问题也没法照着地址排查。
 */
function testText(res: { message?: string; endpoint?: string }): string {
  const msg = res.message || '渠道校验通过';
  return res.endpoint ? msg + ' → ' + res.endpoint : msg;
}

/** 测试失败文案(同样带上实际请求的地址)。 */
function testErrorText(res: { error?: string; endpoint?: string }): string {
  const msg = res.error || '校验失败';
  return res.endpoint ? msg + ' → ' + res.endpoint : msg;
}

function isChecked(id: string): boolean {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.checked : false;
}

function setText(id: string, text: string): void {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
}

/** 回调地址一行 + 复制按钮。 */
function notifyLine(id: string, copyId: string): HTMLElement {
  const code = el('code', { id, text: '(加载中…)' });
  const copyBtn = el('button', { class: 'btn btn-secondary btn-sm', id: copyId, text: '复制' });
  copyBtn.addEventListener('click', () => {
    const text = code.textContent || '';
    if (!text || text.includes('加载中')) {
      toast('回调地址还没加载出来', 'err');
      return;
    }
    void copyText(text);
  });
  return el('div', { class: 'notice notice-info', 'data-notify-box': id },
    el('div', { style: 'margin-bottom:6px', text: '回调地址(填到微信 / 支付宝后台)' }),
    el('div', { class: 'row' }, code, el('span', { class: 'spacer' }), copyBtn));
}

/**
 * 回调地址指向本机时,给一句醒目提示。
 *
 * 回调地址是平台按「商户配的对外地址 / 平台默认地址」算出来的:
 * 本地启动没配对外地址时,它是 http://127.0.0.1:8089 —— 本机调试完全正常,
 * 但这个地址填到微信 / 支付宝后台,对方根本访问不到(订单会永远停在 pending)。
 *
 * 生产部署在上面的「回调地址(对外访问地址)」里填对外域名即可,不用重启平台。
 */
function markLocalNotify(url: string): void {
  if (!url) return;
  const local = /^https?:\/\/(127\.0\.0\.1|localhost|\[::1\])(:|\/|$)/i.test(url);
  document.querySelectorAll<HTMLElement>('[data-notify-box]').forEach((box) => {
    const code = box.querySelector('code');
    if (!code || code.textContent !== url) return; // 只处理对应渠道那个框
    let warn = box.querySelector('.notify-local-warn') as HTMLElement | null;
    if (!local) {
      if (warn) warn.remove();
      return;
    }
    if (!warn) {
      warn = el('div', { class: 'error-text notify-local-warn' });
      box.appendChild(warn);
    }
    warn.textContent = '当前回调地址指向本机,只在本机调试有效。' +
      '生产部署请在上面的「回调地址(对外访问地址)」里填对外域名(不用重启平台),这里的地址会自动变成对外域名。';
  });
}

/** 回填「回调地址」卡片:当前生效值 + 平台默认值。 */
function fillNotifyBase(me: MeResponse): void {
  const own = me.notify_base_url || '';
  const def = me.notify_base_default || '';
  setValue('nb-base', own);
  const cur = own || def;
  setText('nb-status', own
    ? '当前生效:' + cur + '(你自己配置的)'
    : '当前生效:' + (cur || '(未设置)') + '(平台默认)');
  setText('nb-default', '平台默认(-base):' + (def || '(未设置)'));
  checkNotifyBaseWarn(own);
}

/**
 * 提示「填的域名和当前平台地址不一致」。
 *
 * 回调地址填错域名是这类配置最容易犯、后果又最隐蔽的错:
 * 页面看着一切正常,但微信 / 支付宝把回调发过去没人接,
 * 订单就永远停在「待支付」—— 从控制台上完全看不出问题在哪。
 */
function checkNotifyBaseWarn(value: string): void {
  const warn = document.getElementById('nb-warn');
  if (!warn) return;
  if (!value) {
    warn.textContent = '';
    return;
  }
  let host = '';
  try {
    host = new URL(value).host;
  } catch {
    warn.textContent = '地址格式不对,应该像 https://pay.example.com';
    return;
  }
  if (!host) {
    warn.textContent = '地址里要带域名';
    return;
  }
  if (host === location.host) {
    warn.textContent = '';
    return;
  }
  warn.textContent = '这个域名(' + host + ')和当前平台地址(' + location.host + ')不一样:' +
    '请确认它已经反代到本平台,否则微信 / 支付宝的回调发不到平台,订单会一直停在「待支付」。';
}

/** 渠道总览。 */
function renderOverview(
  box: HTMLElement,
  channels: { channel: string; enabled: boolean; is_default: boolean }[],
): void {
  box.replaceChildren();
  if (!channels.length) {
    box.appendChild(emptyState('还没有配置任何渠道', '在下面填写密钥并保存,平台会自动加密入库。'));
    return;
  }
  const grid = el('div', { class: 'channel-grid' });
  for (const cfg of channels) {
    grid.appendChild(el('div', { class: 'channel-card' + (cfg.enabled ? ' is-on' : '') },
      el('div', { class: 'channel-card-head' },
        el('div', { class: 'channel-card-name', text: CHANNEL_LABELS[cfg.channel] || cfg.channel }),
        cfg.enabled ? badge('已启用', 'success') : badge('未启用', 'muted'),
        cfg.is_default ? badge('默认', 'info') : null),
      el('div', { class: 'channel-card-meta', text: cfg.enabled ? '可用于收款' : '启用后生效' })));
  }
  box.appendChild(grid);
  box.appendChild(el('div', { class: 'hint-text mt-4',
    text: '各渠道的「启用 / 设为默认」在对应渠道卡片里设置,点该渠道的保存按钮生效。' }));
}

/** 保存渠道配置。scope 非空时保存为该应用的专属配置。 */
async function saveChannel(channel: string, payload: unknown, scope = ''): Promise<void> {
  const q = scope ? '?app_id=' + encodeURIComponent(scope) : '';
  const res = await api<{ ok: boolean; error?: string }>(`/api/channels/${channel}${q}`, {
    method: 'POST',
    body: payload,
  });
  if (res.ok === false) throw new Error(res.error || '保存失败');
}

/**
 * 钱包渠道字段的 id。
 *
 * Apple Pay 与 Google Pay 各有一套 —— 后端也是**各存各的**
 * (每个渠道一行,各自带一份 wallet 配置),表单必须对上这个模型,
 * 否则「保存 Apple Pay」会把 Google Pay 的商户号一起改掉。
 */
function walletField(channel: string, name: string): string {
  return `w-${channel}-${name}`;
}

/** 回填表单。 */
function fillForms(channels: ChannelConfig[]): void {
  const wechat = channels.find((c) => c.channel === 'wechat');
  if (wechat?.wechat) {
    const w = wechat.wechat;
    setValue('w-appid', w.app_id);
    setValue('w-mchid', w.mch_id);
    setValue('w-serial', w.serial_no);
    setValue('w-certdir', w.cert_dir || './certs');
    setChecked('w-autofetch', !!w.auto_fetch_certs);
    markSecret('w-apiv3key', !!w.api_v3_key, 'APIv3 密钥');
    markSecret('w-privkey', !!w.private_key_pem, '商户 API 证书私钥');
  }
  const alipay = channels.find((c) => c.channel === 'alipay');
  if (alipay?.alipay) {
    const a = alipay.alipay;
    setValue('a-appid', a.app_id);
    setValue('a-pubkey', a.alipay_public_key_pem);
    setValue('a-certpath', a.alipay_public_cert_path);
    setValue('a-appcertpath', a.app_cert_path);
    setValue('a-rootcertpath', a.root_cert_path);
    setValue('a-gateway', a.gateway);
    markSecret('a-privkey', !!a.private_key_pem, '应用私钥');
  }
  // 两个钱包**各自**回填各自渠道的配置。
  //
  // 之前是共用一份表单、只回填「找到的第一个」钱包渠道:
  // 商户先给 Apple Pay 配了商户号 A,再给 Google Pay 配了商户号 B,
  // 两边就不一样了;而表单只显示其中一个,点另一个的「保存」就把
  // 显示出来的那个值写进了另一个渠道 —— **商户号被悄悄改掉,钱就收不到了**。
  for (const channel of ['applepay', 'googlepay'] as const) {
    const cfg = channels.find((c) => c.channel === channel);
    if (!cfg?.wallet) continue;
    const w = cfg.wallet;
    setValue(walletField(channel, 'merchant'), w.merchant_id);
    setValue(walletField(channel, 'display'), w.display_name);
    setValue(walletField(channel, 'domain'), w.domain_name);
    setValue(walletField(channel, 'currency'), w.currency || 'CNY');
    setValue(walletField(channel, 'stripe-publishable'), w.stripe_publishable_key);
    markSecret(walletField(channel, 'stripe-secret'), !!w.stripe_secret_key, '收单私钥');
    markSecret(walletField(channel, 'stripe-webhook'), !!w.stripe_webhook_secret, 'Webhook 签名密钥');
  }
  for (const channel of CHANNELS) {
    const cfg = channels.find((c) => c.channel === channel);
    setChecked(`ch-enabled-${channel}`, !!cfg?.enabled);
    setChecked(`ch-default-${channel}`, !!cfg?.is_default);
  }
}

function setValue(id: string, value: string | undefined): void {
  const node = document.getElementById(id) as HTMLInputElement | null;
  if (node) node.value = value || '';
}

function setChecked(id: string, value: boolean): void {
  const node = document.getElementById(id) as HTMLInputElement | null;
  if (node) node.checked = value;
}
