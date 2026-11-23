/** 表单、提示、弹层等通用 UI 组件。 */

import { el, clear, append } from './dom';
import { money, yuanToCents } from './money';

// ---------------------------------------------------------------- 字段

export interface FieldOptions {
  id: string;
  label: string;
  value?: string;
  placeholder?: string;
  type?: string;
  secret?: boolean;
  textarea?: boolean;
  /** 必填:标签后加红星,并在失焦时提示 */
  required?: boolean;
  /** 字段下方的辅助说明 */
  hint?: string;
  /** 字段下方的错误说明(用 setFieldError 更新) */
  error?: string;
  /** 标签后缀(例如「选填」) */
  optional?: string;
  /** 输入框下方的额外节点 */
  extra?: Node;
}

/** 生成一个带标签的输入框(带辅助说明 / 错误位)。 */
export function field(opts: FieldOptions): HTMLElement {
  const input = opts.textarea
    ? el('textarea', { id: opts.id, placeholder: opts.placeholder || '' })
    : el('input', {
        type: opts.type || (opts.secret ? 'password' : 'text'),
        id: opts.id,
        placeholder: opts.placeholder || '',
        autocomplete: opts.type === 'password' ? 'current-password' : 'off',
      });
  if (opts.value) input.value = opts.value;

  const label = el('label', { class: 'label', text: opts.label, for: opts.id });
  if (opts.required) label.appendChild(el('span', { class: 'req', text: '*' }));
  if (opts.optional) label.appendChild(el('span', { class: 'optional', text: opts.optional }));

  const wrap = el('div', { class: 'field' }, label, input);
  if (opts.hint) wrap.appendChild(el('div', { class: 'hint-text', text: opts.hint }));
  if (opts.error) wrap.appendChild(el('div', { class: 'error-text', text: opts.error }));
  if (opts.extra) wrap.appendChild(opts.extra);
  return wrap;
}

/** 设置某个字段的错误提示(没有就创建一个)。 */
export function setFieldError(id: string, message: string): void {
  const input = document.getElementById(id);
  const wrap = input?.closest('.field') as HTMLElement | null;
  if (!wrap) return;
  let node = wrap.querySelector('.error-text') as HTMLElement | null;
  if (!message) {
    node?.remove();
    return;
  }
  if (!node) {
    node = el('div', { class: 'error-text' });
    wrap.appendChild(node);
  }
  node.textContent = message;
}

/** 生成一个带标签的复选框。 */
export function checkbox(id: string, label: string, checked = false): HTMLElement {
  const input = el('input', { type: 'checkbox', id }) as HTMLInputElement;
  input.checked = checked;
  return el('label', { class: 'checkbox' }, input, el('span', { text: label }));
}

/** 生成一个「开关行」:标题 + 说明 + 右侧复选框。 */
export function switchRow(
  id: string,
  title: string,
  desc: string,
  checked = false,
): HTMLElement {
  const input = el('input', { type: 'checkbox', id }) as HTMLInputElement;
  input.checked = checked;
  return el('div', { class: 'switch-row' },
    el('div', { class: 'switch-text' },
      el('div', { class: 'switch-title', text: title }),
      el('div', { class: 'switch-desc', text: desc })),
    el('label', { class: 'checkbox' }, input));
}

/** 生成一个下拉框。 */
export function select(
  id: string,
  options: { value: string; label: string }[],
  value?: string,
  label?: string,
): HTMLElement {
  const node = el('select', { id }) as HTMLSelectElement;
  for (const opt of options) {
    node.appendChild(el('option', { value: opt.value, text: opt.label }));
  }
  if (value) node.value = value;
  if (!label) return el('div', { class: 'field' }, node);
  return el('div', { class: 'field' }, el('label', { class: 'label', text: label, for: id }), node);
}

/** 读取输入框的值。 */
export function val(id: string): string {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.value.trim() : '';
}

/** 读取复选框状态。 */
export function checked(id: string): boolean {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.checked : false;
}

/** 读取下拉框的值。 */
export function sel(id: string): string {
  const node = document.getElementById(id) as HTMLSelectElement | null;
  return node ? node.value : '';
}

/** 写回输入框的值(找不到就忽略)。 */
export function setVal(id: string, value: string): void {
  const node = document.getElementById(id) as HTMLInputElement | null;
  if (node) node.value = value;
}

/** 标记敏感输入框:已有值时清空并提示「已保存」。 */
export function markSecret(id: string, hasValue: boolean, label: string): void {
  const node = document.getElementById(id) as HTMLInputElement | null;
  if (!node) return;
  node.value = '';
  node.placeholder = hasValue ? `已保存(留空表示不修改):${label}` : '';
}

// ---------------------------------------------------------------- 区块

/** 页面区块卡片:标题 + 说明 + 右侧操作区。 */
export function card(
  title: string,
  desc?: string | null,
  actions?: Node | null,
  ...children: (Node | string)[]
): HTMLElement {
  const head = el('div', { class: 'card-head' },
    el('div', {},
      el('h2', { text: title }),
      desc ? el('div', { class: 'card-desc', text: desc }) : null));
  if (actions) head.appendChild(el('div', { class: 'card-head-actions' }, actions));
  return el('div', { class: 'card' }, head, ...children);
}

/** 空状态。 */
export function emptyState(title: string, desc?: string, action?: Node): HTMLElement {
  return el('div', { class: 'empty' },
    el('div', { class: 'empty-icon', text: '◍' }),
    el('div', { class: 'empty-title', text: title }),
    desc ? el('div', { class: 'empty-desc', text: desc }) : null,
    action || null);
}

/** 一段提示块。 */
export function notice(text: string, kind: 'info' | 'warn' | 'danger' | 'success' = 'info'): HTMLElement {
  return el('div', { class: `notice notice-${kind}`, text });
}

/** 徽章。 */
export function badge(text: string, kind: 'success' | 'danger' | 'warning' | 'info' | 'muted' = 'muted'): HTMLElement {
  return el('span', { class: `badge badge-${kind}`, text });
}

/** 统计卡:标签 + 数值 + 单位(可选)。 */
export function stat(label: string, value: string, unit?: string, accent = false): HTMLElement {
  return el('div', { class: 'stat' + (accent ? ' stat-accent' : '') },
    el('div', { class: 'stat-label', text: label }),
    el('div', { class: 'stat-value' },
      el('span', { text: value }),
      unit ? el('span', { class: 'dim', style: 'font-size:13px;margin-left:3px', text: unit }) : null));
}

// ---------------------------------------------------------------- Toast

let toastHost: HTMLElement | null = null;

function ensureToastHost(): HTMLElement {
  if (toastHost && document.body.contains(toastHost)) return toastHost;
  toastHost = el('div', { class: 'toast-host' });
  document.body.appendChild(toastHost);
  return toastHost;
}

/** 全局轻提示。kind: ok / err / info */
export function toast(message: string, kind: 'ok' | 'err' | 'info' = 'info', ms = 3200): void {
  const host = ensureToastHost();
  const icon = kind === 'ok' ? '✓' : kind === 'err' ? '!' : 'i';
  const node = el('div', { class: `toast toast-${kind}` },
    el('span', { class: 'toast-icon', text: icon }),
    el('span', { text: message }));
  host.appendChild(node);
  window.setTimeout(() => {
    node.style.opacity = '0';
    node.style.transition = 'opacity .2s';
    window.setTimeout(() => node.remove(), 220);
  }, ms);
}

// ---------------------------------------------------------------- 弹层

interface ModalOptions {
  title: string;
  desc?: string;
  body?: Node;
  okText?: string;
  cancelText?: string;
  danger?: boolean;
  /** 点「确定」时调用;抛错则留在弹窗里并显示错误 */
  onOk?: (handle: ModalHandle) => void | Promise<void>;
}

export interface ModalHandle {
  mask: HTMLElement;
  okButton: HTMLButtonElement;
  /** 关闭弹窗 */
  close: (ok: boolean) => void;
  /** 在弹窗内显示错误 */
  error: (msg: string) => void;
}

/** 通用模态框。 */
export function openModal(opts: ModalOptions): ModalHandle {
  const mask = el('div', { class: 'modal-mask' });
  const errBox = el('div', { class: 'modal-error' });
  const okButton = el('button', {
    class: opts.danger ? 'btn btn-danger' : 'btn btn-primary',
    text: opts.okText || '确定',
  }) as HTMLButtonElement;
  const cancelButton = el('button', { class: 'btn btn-secondary', text: opts.cancelText || '取消' });

  const dialog = el('div', { class: 'modal', role: 'dialog', 'aria-modal': 'true' },
    el('h2', { text: opts.title }),
    opts.desc ? el('p', { class: 'modal-desc', text: opts.desc }) : null,
    opts.body || null,
    errBox,
    el('div', { class: 'modal-actions' }, cancelButton, okButton));

  mask.appendChild(dialog);
  document.body.appendChild(mask);
  mask.classList.add('open');

  const handle: ModalHandle = {
    mask,
    okButton,
    close: () => {
      mask.classList.remove('open');
      mask.remove();
      document.removeEventListener('keydown', onKey);
    },
    error: (msg: string) => {
      errBox.textContent = msg;
    },
  };

  const onKey = (ev: KeyboardEvent): void => {
    if (ev.key === 'Escape') handle.close(false);
  };
  document.addEventListener('keydown', onKey);
  cancelButton.addEventListener('click', () => handle.close(false));
  mask.addEventListener('click', (ev) => {
    if (ev.target === mask) handle.close(false);
  });
  okButton.addEventListener('click', () => {
    if (!opts.onOk) {
      handle.close(true);
      return;
    }
    // 必须用 async IIFE 包一层,不能直接 Promise.resolve(opts.onOk(handle)):
    // onOk 同步抛错时,异常会直接从 click 事件里抛出去,
    // 后面的 .catch 根本接不到,弹窗里也不会显示错误 —— 而 ModalOptions
    // 承诺的是「抛错则留在弹窗里并显示错误」。
    void (async () => {
      await opts.onOk!(handle);
    })().catch((err: unknown) => {
      handle.error(err instanceof Error ? err.message : String(err));
    });
  });
  return handle;
}

/**
 * 确认对话框:返回 true 表示用户点了确定。
 *
 * 危险操作(吊销密钥、关闭二次验证、退款)都必须先过这一关。
 */
export function confirmDialog(title: string, desc: string, danger = false): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    let settled = false;
    const settle = (ok: boolean): void => {
      if (settled) return;
      settled = true;
      resolve(ok);
    };
    const handle = openModal({
      title,
      desc,
      okText: danger ? '确认执行' : '确定',
      cancelText: '取消',
      danger,
      onOk: (h) => {
        settle(true);
        h.close(true);
      },
    });
    // 取消按钮 / ESC / 点遮罩都走 close(false)
    const originalClose = handle.close;
    handle.close = (ok: boolean) => {
      if (!ok) settle(false);
      originalClose(ok);
    };
  });
}

/**
 * 金额输入对话框。
 *
 * 返回金额(分);用户取消返回 null。
 * **留空返回 0**:0 的含义是「全额退掉剩下的」(由后端按实际已退金额算),
 * 不是「退 0 元」—— 这样部分退款过的订单也能走「全额退款」这条主路径。
 *
 * 单位换算是这类页面最容易出错的地方,所以统一在这里做:界面输入「元」,内部用「分」。
 */
export function amountDialog(opts: {
  title: string;
  desc?: string;
  /** 最大可退金额(分);0 表示不限 */
  maxCents?: number;
  defaultYuan?: string;
}): Promise<number | null> {
  return new Promise<number | null>((resolve) => {
    let settled = false;
    const settle = (value: number | null): void => {
      if (settled) return;
      settled = true;
      resolve(value);
    };

    const input = el('input', {
      type: 'text',
      id: 'amount-input',
      placeholder: '留空表示全额退款',
      value: opts.defaultYuan || '',
    }) as HTMLInputElement;
    const hint = el('div', { class: 'hint-text' });
    if (opts.maxCents) {
      hint.textContent = '最多可退 ' + money(opts.maxCents) + ' 元';
    }
    const body = el('div', { class: 'field' },
      el('label', { class: 'label', text: '退款金额(元)', for: 'amount-input' }),
      input,
      hint);

    const handle = openModal({
      title: opts.title,
      desc: opts.desc,
      body,
      okText: '确认退款',
      danger: true,
      onOk: (h) => {
        const raw = input.value.trim();
        if (raw === '') {
          // 留空 = 全额。返回 0,让**后端**按「订单金额 - 已退金额」算实际要退多少 ——
          // 前端只知道发起弹窗那一刻的余额,后端才是准的。
          // 之前这里返回 maxCents(订单全额),部分退款过的订单会被后端拒掉
          // (已退 + 订单全额 > 订单金额),「全额退款」那条主路径是坏的。
          settle(0);
          h.close(true);
          return;
        }
        // 精确解析成「分」(不用 num*100 的浮点运算),
        // 顺便挡住超过两位小数的输入 —— 后端是会直接报错的,
        // 前端静默截断只会让两边对不上账。
        const cents = yuanToCents(raw);
        if (cents === null) {
          // 0 和格式错误要分开说:前者是用户没填金额,后者是填错了。
          const num = Number(raw);
          if (isFinite(num) && num <= 0) {
            h.error('请输入大于 0 的金额');
          } else {
            h.error('金额格式不对(最多两位小数)');
          }
          return;
        }
        if (opts.maxCents && cents > opts.maxCents) {
          h.error('超过订单可退金额 ' + money(opts.maxCents) + ' 元');
          return;
        }
        settle(cents);
        h.close(true);
      },
    });
    const originalClose = handle.close;
    handle.close = (ok: boolean) => {
      if (!ok) settle(null);
      originalClose(ok);
    };
    window.setTimeout(() => input.focus(), 30);
  });
}

// ---------------------------------------------------------------- 按钮态

/**
 * 让按钮进入「处理中」状态并禁止重复点击,执行完毕后自动恢复。
 *
 * 支付、退款这类操作如果允许连点,会重复下单 —— 所以所有提交按钮都必须走这里。
 */
export async function withBusy<T>(
  button: HTMLButtonElement,
  fn: () => Promise<T>,
): Promise<T | undefined> {
  if (button.classList.contains('is-busy')) return undefined;
  const original = button.textContent || '';
  // 记下按钮**原本**的禁用状态:结束时必须还原成它,
  // 而不是一律设成 enabled —— 有些按钮是按当前状态禁用的
  // (比如「验证方式」还没选时禁用的「开启二次验证」),
  // 一律放开等于把「选好方式才能开」这条约束悄悄拆了。
  const wasDisabled = button.disabled;
  button.classList.add('is-busy');
  button.disabled = true;
  try {
    return await fn();
  } finally {
    button.classList.remove('is-busy');
    button.disabled = wasDisabled;
    button.textContent = original;
  }
}

/** 旧的行内错误提示条(逐步被 toast / 内联错误取代,保留给个别场景)。 */
export function errorBox(id: string): HTMLElement {
  return el('div', { class: 'error-text', id });
}

/** 在提示条上显示错误。 */
export function showError(box: HTMLElement | null, err: unknown): void {
  if (!box) return;
  box.textContent = err instanceof Error ? err.message : String(err);
  box.className = 'error-text';
}

/** 在提示条上显示成功信息。 */
export function showOK(box: HTMLElement | null, text: string): void {
  if (!box) return;
  box.textContent = text;
  box.className = 'hint-text';
}

/** 复制到剪贴板,成功给 toast 提示。 */
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    toast('已复制到剪贴板', 'ok');
    return true;
  } catch {
    toast('复制失败,请手动选中复制', 'err');
    return false;
  }
}

/** 把节点渲染成「键: 值」的行,用于详情展示。 */
export function detailList(rows: [string, string][]): HTMLElement {
  const wrap = el('div', { class: 'stack' });
  for (const [k, v] of rows) {
    wrap.appendChild(el('div', { class: 'row' },
      el('span', { class: 'dim', style: 'min-width:96px', text: k }),
      el('span', { class: 'mono', text: v })));
  }
  return wrap;
}

/** 清理子节点(与 dom.clear 同名,方便页面直接用)。 */
export { clear, append };
