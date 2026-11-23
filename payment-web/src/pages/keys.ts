/** 开放接口页:管理 API 密钥(创建 / 查看 / 吊销),并给出签名接入说明。 */

import { api } from '../lib/api';
import { loadApps } from '../lib/apps';
import { el, table } from '../lib/dom';
import {
  badge, card, confirmDialog, copyText, emptyState, field, openModal, setVal, toast, val, withBusy,
} from '../lib/ui';
import { requireVerify } from '../lib/verify';
import type { APIKeyRecord, MeResponse } from '../lib/types';

const SCOPES: { value: string; label: string; hint: string }[] = [
  { value: 'pay', label: 'pay', hint: '下单 / 查单 / 关单 / 订单与统计查询' },
  { value: 'refund', label: 'refund', hint: '退款(资金操作,单独授权)' },
  { value: 'wallet', label: 'wallet', hint: '钱包渠道扣款(Apple Pay / Google Pay)' },
];

export function renderKeys(root: HTMLElement): void {
  // ---- 创建密钥 ----
  //
  // 注意:输入框由下面的 field() 生成,取值必须走 val('key-name') / val('key-ttl')。
  // 这里**不要**再额外 new 一个同 id 的 input —— 那个元素不会进 DOM,
  // 读它永远是空值(备注名传空、有效期永远 0),是那种「界面上能填、后端收不到」的坑。
  const scopeInputs: HTMLInputElement[] = [];
  const scopeBox = el('div', { class: 'grid grid-3' });
  for (const s of SCOPES) {
    const input = el('input', { type: 'checkbox', id: 'key-scope-' + s.value }) as HTMLInputElement;
    input.checked = s.value === 'pay';
    scopeInputs.push(input);
    scopeBox.appendChild(el('label', {
      class: 'channel-card', style: 'cursor:pointer;display:block',
    },
      el('div', { class: 'channel-card-head' },
        el('span', { class: 'checkbox' }, input),
        el('span', { class: 'channel-card-name', text: s.label })),
      el('div', { class: 'channel-card-meta', text: s.hint })));
  }

  const createBtn = el('button', { class: 'btn btn-primary', text: '创建密钥' }) as HTMLButtonElement;

  // 绑定应用的下拉框。
  //
  // 绑定之后,业务后端用这把密钥下单**不用再传 app_id**:平台按密钥绑定的应用
  // 自动落到那个产品,也就自动用那个产品自己的渠道密钥。不绑定的话,
  // 客户端忘了传 app_id 会静默走全局渠道配置,订单全进了「未分类」。
  const appSelect = el('select', { id: 'key-app' }) as HTMLSelectElement;
  appSelect.appendChild(el('option', { value: '', text: '不绑定(全局渠道配置)' }));
  const appSelectField = el('div', { class: 'field' },
    el('label', { class: 'label', text: '绑定应用', for: 'key-app' }),
    appSelect,
    el('div', {
      class: 'hint-text',
      text: '绑定后下单自动落到该应用,客户端不必再传 app_id;请求里传了别的应用会被拒绝。',
    }));

  root.appendChild(card('创建开放接口密钥',
    '给商户自己的业务后端调用支付接口用,不需要浏览器会话',
    null,
    el('div', { class: 'grid grid-2' },
      field({ id: 'key-name', label: '备注名', hint: '只用于自己识别这把钥匙' }),
      field({ id: 'key-ttl', label: '有效期(天)', type: 'number', hint: '0 = 长期有效' }),
      appSelectField),
    el('div', { class: 'field' },
      el('label', { class: 'label', text: '权限范围' }),
      scopeBox),
    el('div', { class: 'row' }, createBtn)));

  // ---- 密钥列表 ----
  const refreshBtn = el('button', { class: 'btn btn-secondary', text: '刷新' }) as HTMLButtonElement;
  const body = el('div');
  root.appendChild(card('我的密钥', '密钥明文只在创建时显示一次;吊销后立即失效', refreshBtn, body));

  // ---- 接入说明 ----
  root.appendChild(el('details', { class: 'card' },
    el('summary', { style: 'cursor:pointer;font-weight:600;font-size:15px', text: '接入说明(签名算法)' }),
    el('p', { class: 'card-desc mt-2', text: '按下面的算法对请求签名,平台用同一把密钥验签。' }),
    el('pre', { text: SIGN_GUIDE })));

  // ---- 真实代码示例 ----
  //
  // 这里显示的是**仓库里那份真代码**(后端用 go:embed 嵌进来),
  // 不是手写的片段:代码改了页面自动跟着变,不会出现「文档说的和代码做的
  // 不一样」这种最耗时间的问题。
  const codeBox = el('div', {}, el('p', { class: 'muted', text: '加载中…' }));
  root.appendChild(card('接入代码示例',
    '可以直接复制走的真代码:Go 客户端、调用示例、curl(不用装 SDK 时的备选)',
    null,
    codeBox));

  async function loadSample(): Promise<void> {
    try {
      const res = await api<{ client: string; readme: string; curl: string }>('/api/openapi/sample');
      codeBox.replaceChildren(buildCodeTabs([
        { label: '调用示例(Go)', code: res.readme },
        { label: '客户端源码', code: res.client },
        { label: 'curl', code: res.curl },
      ]));
    } catch (e) {
      codeBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }
  void loadSample();

  createBtn.addEventListener('click', () =>
    void withBusy(createBtn, async () => {
      try {
        // 创建密钥 = 授权别人的程序动钱,必须二次验证。
        await requireVerify();
        const scopes = scopeInputs.filter((i) => i.checked).map((i) => i.id.replace('key-scope-', ''));
        if (!scopes.length) {
          toast('至少选择一个权限范围', 'err');
          return;
        }
        // 强制绑定应用的账户:不绑应用的密钥下单一定被拒,当场说清楚。
        if (requireApp && !val('key-app')) {
          toast('该账户已开启「订单必须绑定应用」:创建的密钥必须绑定一个应用', 'err');
          return;
        }
        const res = await api<{ key: APIKeyRecord; secret: string; notice: string }>('/api/keys', {
          method: 'POST',
          body: {
            name: val('key-name'),
            scopes,
            ttl_days: Number(val('key-ttl') || 0),
            app_id: val('key-app'),
          },
        });
        showSecret(res.secret, res.key.key_id, res.notice);
        setVal('key-name', '');
        setVal('key-ttl', '0');
        setVal('key-app', '');
        await loadKeys();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  refreshBtn.addEventListener('click', () => void loadKeys());

  // 应用列表:填下拉框 + 给列表页显示应用名。
  let appNames: Record<string, string> = {};
  // 该账户是不是强制绑定应用(强制时只能建绑定了应用的密钥)。
  let requireApp = false;
  async function loadAppOptions(): Promise<void> {
    try {
      const res = await loadApps();
      const names: Record<string, string> = {};
      for (const a of res.apps) names[a.app_id] = a.name;
      appNames = names;
      // 强制绑定应用的账户:只能建绑定了应用的密钥,
      // 「不绑定」这一项直接不给选(后端也会拒)。
      const me = await api<MeResponse>('/api/me');
      const forced = !!me.user.require_app_binding;
      requireApp = forced;
      // 重建选项时保留当前选择,免得刷新一次就把已选的应用丢了。
      const current = appSelect.value;
      const none = el('option', {
        value: '',
        text: forced ? '必须绑定应用(该账户已开启强制绑定)' : '不绑定(全局渠道配置)',
      }) as HTMLOptionElement;
      none.disabled = forced;
      appSelect.replaceChildren(none);
      for (const a of res.apps) {
        appSelect.appendChild(el('option', {
          value: a.app_id,
          text: a.name + '(' + a.app_id + ')',
        }));
      }
      appSelect.value = current;
      // 提示可能重复出现(页面重渲染),先清掉上一条。
      for (const old of appSelectField.querySelectorAll('.error-text')) old.remove();
      if (forced && !res.apps.length) {
        appSelectField.appendChild(el('div', {
          class: 'error-text',
          text: '该账户已开启「订单必须绑定应用」,但一个应用都没有。请先到「应用管理」页建应用。',
        }));
      }
      await loadKeys();
    } catch (e) {
      // 应用列表拿不到不影响密钥管理(下拉框保持「不绑定」)。
      appSelect.replaceChildren(el('option', { value: '', text: '不绑定(全局渠道配置)' }));
      await loadKeys();
    }
  }
  void loadAppOptions();

  async function loadKeys(): Promise<void> {
    body.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const res = await api<{ keys: APIKeyRecord[] }>('/api/keys');
      const keys = res.keys || [];
      body.replaceChildren();
      if (!keys.length) {
        body.appendChild(emptyState('还没有创建密钥', '创建一把之后,你的业务后端就能直接下单了。'));
        return;
      }
      const rows = keys.map((k) => {
        const revoke = el('button', { class: 'btn btn-danger btn-sm', text: '吊销' });
        revoke.addEventListener('click', () => void revokeKey(k, revoke));
        return [
          el('span', { class: 'cell-mono', text: k.key_id }),
          k.name || '—',
          k.app_id ? badge(appNames[k.app_id] || k.app_id, 'info') : el('span', { class: 'dim', text: '未绑定' }),
          el('span', { class: 'row' }, ...(k.scopes || []).map((s) => badge(s, 'info'))),
          k.enabled ? badge('启用中', 'success') : badge('已吊销', 'muted'),
          fmtTime(k.expires_at) || '长期有效',
          fmtTime(k.last_used_at) || '从未使用',
          el('td', { class: 'cell-actions' }, revoke),
        ];
      });
      body.appendChild(table(
        ['密钥 ID', '备注名', '绑定应用', '权限', '状态', '过期时间', '最近使用', '操作'], rows));
    } catch (e) {
      body.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  async function revokeKey(k: APIKeyRecord, btn: HTMLElement): Promise<void> {
    const ok = await confirmDialog(
      '吊销这把密钥?',
      '吊销后使用该密钥的所有请求会立即失败,且无法恢复。密钥 ' + k.key_id + ' 将永久失效。',
      true,
    );
    if (!ok) return;
    await withBusy(btn as HTMLButtonElement, async () => {
      try {
        await requireVerify();
        await api('/api/keys/' + encodeURIComponent(k.key_id) + '/revoke', { method: 'POST' });
        toast('密钥已吊销', 'ok');
        await loadKeys();
      } catch (e) {
        toast(errText(e), 'err');
      }
    });
  }

  // 密钥列表由 loadAppOptions 统一触发(它要先拿到应用名才能显示「绑定应用」)。
}

/** 密钥明文只显示一次:用弹窗弹出来,关掉就没了。 */
function showSecret(secret: string, keyID: string, notice: string): void {
  const copyBtn = el('button', { class: 'btn btn-secondary btn-sm', text: '复制密钥' });
  copyBtn.addEventListener('click', () => void copyText(secret));
  openModal({
    title: '密钥已创建',
    desc: notice || '密钥明文只显示这一次,关闭后无法再次查看。',
    body: el('div', {},
      el('div', { class: 'notice notice-danger mb-0', text: '请立即保存到你的密钥管理系统,不要留在聊天记录或代码仓库里。' }),
      el('div', { class: 'section-title', text: '密钥 ID' }),
      el('div', { class: 'mono', text: keyID }),
      el('div', { class: 'section-title', text: '密钥明文' }),
      el('pre', { text: secret }),
      el('div', { class: 'row' }, copyBtn)),
    okText: '我已保存',
  });
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * 代码块 + 标签切换。
 *
 * 标签切换只换 <pre> 里的文本,不动 DOM 结构 —— 复制按钮绑在 pre 上,
 * 换内容不用重新绑事件。
 */
function buildCodeTabs(tabs: { label: string; code: string }[]): HTMLElement {
  const pre = el('pre', { text: tabs[0]?.code || '' });
  const bar = el('div', { class: 'row', style: 'margin-bottom:8px' });
  const buttons: HTMLButtonElement[] = [];
  for (const tab of tabs) {
    const btn = el('button', { class: 'btn btn-secondary btn-sm', text: tab.label }) as HTMLButtonElement;
    buttons.push(btn);
    btn.addEventListener('click', () => {
      pre.textContent = tab.code;
      buttons.forEach((b, i) => {
        b.className = 'btn btn-sm ' + (i === buttons.indexOf(btn) ? 'btn-primary' : 'btn-secondary');
      });
    });
    bar.appendChild(btn);
  }
  if (buttons[0]) {
    buttons[0].className = 'btn btn-sm btn-primary';
  }
  const copy = el('button', { class: 'btn btn-secondary btn-sm', text: '复制代码' });
  copy.addEventListener('click', () => void copyText(pre.textContent || ''));
  bar.appendChild(el('span', { style: 'flex:1' }));
  bar.appendChild(copy);
  return el('div', {}, bar, pre);
}

/**
 * 格式化时间。
 *
 * 后端的零值时间会序列化成 "0001-01-01T00:00:00Z",直接 toLocaleString
 * 就会在表格里显示「0001/1/1」——看起来像 bug。零值统一当空处理。
 */
function fmtTime(value?: string): string {
  if (!value || value.startsWith('0001-')) return '';
  const d = new Date(value);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleString();
}

/** 签名算法说明(给接入方照着实现)。 */
const SIGN_GUIDE = `请求头:
  X-Pay-Key        密钥 ID(pk_xxx)
  X-Pay-Timestamp  Unix 秒(平台允许 ±300 秒偏差)
  X-Pay-Nonce      随机串(时间窗口内不可重复)
  X-Pay-Signature  HMAC-SHA256 签名(hex,小写)

待签名字符串(用 \\n 连接):
  METHOD \\n TARGET \\n TIMESTAMP \\n NONCE \\n sha256(body) 的 hex
  METHOD   大写的请求方法
  TARGET   路径 + "?" + 原始查询串(没有查询串时只有路径)
  sha256(body) 为空时取空字节串的 sha256

签名:
  hex(HMAC-SHA256(密钥明文, 待签名字符串))

示例(Go):
  target := r.URL.Path
  if r.URL.RawQuery != "" { target += "?" + r.URL.RawQuery }
  sum := sha256.Sum256(body)
  canonical := strings.Join([]string{
    strings.ToUpper(r.Method), target, ts, nonce, hex.EncodeToString(sum[:]),
  }, "\\n")
  mac := hmac.New(sha256.New, []byte(secret))
  mac.Write([]byte(canonical))
  signature := hex.EncodeToString(mac.Sum(nil))

接口:
  POST /api/open/pay              下单            (pay)
  POST /api/open/query            查单(同步渠道)  (pay)
  POST /api/open/order            查订单详情      (pay)
  POST /api/open/close            关闭订单        (pay)
  POST /api/open/refund           退款            (refund)
  POST /api/open/wallet/charge    钱包扣款        (wallet)
  GET  /api/open/orders           订单列表        (pay)
  GET  /api/open/stats            交易统计        (pay)

注意:
  - 密钥明文只在创建时返回一次,之后平台只保存密文,无法再次查看。
  - 退款权限(refund)需要单独授权,不要和下单密钥混用。
  - 时间戳超窗、随机串重复(重放)、签名不符,一律拒绝。
  - 密钥绑定了应用时,下单**不用传 app_id**:平台自动落到该应用,
    并用该应用自己的渠道配置;请求里传了别的 app_id 会直接 400。
  - 账户开了「订单必须绑定应用」时,没带 app_id 的下单会被拒绝,
    没绑定应用的密钥也建不出来。`;
