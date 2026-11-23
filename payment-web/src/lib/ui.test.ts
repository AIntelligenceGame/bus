import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  amountDialog,
  badge,
  card,
  checkbox,
  confirmDialog,
  copyText,
  detailList,
  emptyState,
  field,
  markSecret,
  notice,
  openModal,
  select,
  setFieldError,
  setVal,
  showError,
  showOK,
  stat,
  switchRow,
  toast,
  val,
  withBusy,
} from './ui';
import { checked, sel } from './ui';

function q<T extends Element = HTMLElement>(sel_: string, root: ParentNode = document): T {
  const node = root.querySelector<T>(sel_);
  if (!node) throw new Error('找不到: ' + sel_);
  return node;
}

function qAll(sel_: string): HTMLElement[] {
  return [...document.querySelectorAll<HTMLElement>(sel_)];
}

beforeEach(() => {
  document.body.innerHTML = '';
});

afterEach(() => {
  document.body.innerHTML = '';
  vi.restoreAllMocks();
});

describe('field', () => {
  it('生成带标签的输入框,标签 for 指向输入框 id', () => {
    const wrap = field({ id: 'f1', label: '应用名' });
    document.body.appendChild(wrap);
    const input = q<HTMLInputElement>('#f1');
    expect(input.tagName).toBe('INPUT');
    expect(q('label').getAttribute('for')).toBe('f1');
    expect(q('label').textContent).toBe('应用名');
  });

  it('secret 默认是密码框', () => {
    const wrap = field({ id: 'f2', label: '密钥', secret: true });
    document.body.appendChild(wrap);
    expect(q<HTMLInputElement>('#f2').type).toBe('password');
  });

  it('textarea 时生成 textarea 而不是 input', () => {
    const wrap = field({ id: 'f3', label: '备注', textarea: true });
    document.body.appendChild(wrap);
    expect(q('textarea').id).toBe('f3');
    expect(document.querySelector('input#f3')).toBeNull();
  });

  it('必填加红星,选填加后缀', () => {
    document.body.appendChild(field({ id: 'f4', label: '名字', required: true, optional: '选填' }));
    expect(q('.req').textContent).toBe('*');
    expect(q('.optional').textContent).toBe('选填');
  });

  it('hint 与 error 都能显示', () => {
    document.body.appendChild(field({ id: 'f5', label: 'x', hint: '提示', error: '错误' }));
    expect(q('.hint-text').textContent).toBe('提示');
    expect(q('.error-text').textContent).toBe('错误');
  });

  it('初值写进输入框', () => {
    document.body.appendChild(field({ id: 'f6', label: 'x', value: 'abc' }));
    expect(q<HTMLInputElement>('#f6').value).toBe('abc');
  });
});

describe('setFieldError', () => {
  it('给已有字段加错误提示', () => {
    document.body.appendChild(field({ id: 'g1', label: 'x' }));
    setFieldError('g1', '必填');
    expect(q('.error-text').textContent).toBe('必填');
  });

  it('重复设置只更新文字,不会堆多个错误节点', () => {
    document.body.appendChild(field({ id: 'g2', label: 'x' }));
    setFieldError('g2', '第一次');
    setFieldError('g2', '第二次');
    expect(qAll('.error-text').length).toBe(1);
    expect(q('.error-text').textContent).toBe('第二次');
  });

  it('空消息删掉错误节点', () => {
    document.body.appendChild(field({ id: 'g3', label: 'x', error: '旧错误' }));
    setFieldError('g3', '');
    expect(document.querySelector('.error-text')).toBeNull();
  });

  it('字段不存在时不抛错', () => {
    expect(() => setFieldError('nonexistent', 'x')).not.toThrow();
  });
});

describe('checkbox / switchRow / select', () => {
  it('checkbox 生成勾选框并带文案', () => {
    const node = checkbox('c1', '启用', true);
    document.body.appendChild(node);
    expect(q<HTMLInputElement>('#c1').checked).toBe(true);
    expect(node.textContent).toContain('启用');
  });

  it('switchRow 带标题与说明', () => {
    document.body.appendChild(switchRow('s1', '标题', '说明', true));
    expect(q('.switch-title').textContent).toBe('标题');
    expect(q('.switch-desc').textContent).toBe('说明');
    expect(q<HTMLInputElement>('#s1').checked).toBe(true);
  });

  it('select 有选项并可选中指定值', () => {
    document.body.appendChild(select('sel1', [
      { value: 'a', label: 'A' },
      { value: 'b', label: 'B' },
    ], 'b', '选择'));
    expect(sel('sel1')).toBe('b');
    expect(qAll('option').length).toBe(2);
  });

  it('select 指定的值不存在时,当前值为空串', () => {
    document.body.appendChild(select('sel2', [{ value: 'a', label: 'A' }], 'zzz'));
    expect(sel('sel2')).toBe('');
  });

  it('select 没给 label 时不生成 label 元素', () => {
    document.body.appendChild(select('sel3', [{ value: 'a', label: 'A' }]));
    expect(document.querySelector('label')).toBeNull();
  });
});

describe('val / checked / setVal', () => {
  it('读取并去掉首尾空格', () => {
    document.body.appendChild(field({ id: 'v1', label: 'x' }));
    q<HTMLInputElement>('#v1').value = '  hello  ';
    expect(val('v1')).toBe('hello');
  });

  it('元素不存在时返回空串 / false,不是抛错', () => {
    expect(val('nope')).toBe('');
    expect(checked('nope')).toBe(false);
  });

  it('setVal 写回值,元素不存在时静默忽略', () => {
    document.body.appendChild(field({ id: 'v2', label: 'x' }));
    setVal('v2', 'written');
    expect(q<HTMLInputElement>('#v2').value).toBe('written');
    expect(() => setVal('nope', 'x')).not.toThrow();
  });
});

describe('markSecret', () => {
  it('已有值时清空输入并提示「已保存」', () => {
    document.body.appendChild(field({ id: 'm1', label: 'x' }));
    q<HTMLInputElement>('#m1').value = 'super-secret';
    markSecret('m1', true, 'API 密钥');
    const input = q<HTMLInputElement>('#m1');
    expect(input.value).toBe('');
    expect(input.placeholder).toContain('已保存');
    expect(input.placeholder).toContain('API 密钥');
  });
});

describe('区块组件', () => {
  it('card 有标题、说明和操作区', () => {
    const node = card('标题', '说明', document.createElement('button'));
    expect(q('h2', node).textContent).toBe('标题');
    expect(node.querySelector('.card-desc')?.textContent).toBe('说明');
    expect(node.querySelector('.card-head-actions')).not.toBeNull();
  });

  it('card 不给说明时不生成 card-desc', () => {
    const node = card('只有标题');
    expect(node.querySelector('.card-desc')).toBeNull();
  });

  it('emptyState / notice / badge / stat 的文案与类名', () => {
    expect(emptyState('暂无数据').querySelector('.empty-title')?.textContent).toBe('暂无数据');
    expect(notice('注意', 'warn').className).toContain('notice-warn');
    expect(badge('成功', 'success').className).toContain('badge-success');
    const s = stat('总额', '123.00', '元');
    expect(s.querySelector('.stat-value')?.textContent).toContain('123.00');
    expect(s.querySelector('.stat-value')?.textContent).toContain('元');
  });

  it('detailList 渲染成「键: 值」', () => {
    const node = detailList([['商户号', 'M001'], ['状态', '正常']]);
    expect(node.textContent).toContain('商户号');
    expect(node.textContent).toContain('M001');
  });
});

describe('toast', () => {
  it('插入提示并按类型给图标', () => {
    toast('成功', 'ok');
    toast('失败', 'err');
    const nodes = qAll('.toast');
    expect(nodes.length).toBe(2);
    expect(nodes[0].querySelector('.toast-icon')?.textContent).toBe('✓');
    expect(nodes[1].querySelector('.toast-icon')?.textContent).toBe('!');
  });

  it('超时后自动移除', () => {
    vi.useFakeTimers();
    toast('稍后消失', 'info', 100);
    expect(qAll('.toast').length).toBe(1);
    vi.advanceTimersByTime(100);
    vi.advanceTimersByTime(300);
    expect(qAll('.toast').length).toBe(0);
    vi.useRealTimers();
  });
});

describe('openModal', () => {
  it('显示标题与描述', () => {
    openModal({ title: '标题', desc: '描述' });
    expect(q('.modal h2').textContent).toBe('标题');
    expect(q('.modal-desc').textContent).toBe('描述');
  });

  it('点「确定」调用 onOk', async () => {
    let called = 0;
    const handle = openModal({ title: 't', onOk: () => { called++; } });
    handle.okButton.click();
    await Promise.resolve();
    expect(called).toBe(1);
    // onOk 自己不调 close 时弹窗留着 —— 由 onOk 决定何时关。
    expect(document.querySelector('.modal-mask')).not.toBeNull();
  });

  it('onOk 里调 close(true) 才关闭弹窗', async () => {
    const handle = openModal({
      title: 't',
      onOk: (h) => { h.close(true); },
    });
    handle.okButton.click();
    await Promise.resolve();
    await Promise.resolve();
    expect(document.querySelector('.modal-mask')).toBeNull();
  });

  it('没有 onOk 时点确定直接关闭', async () => {
    const handle = openModal({ title: 't' });
    handle.okButton.click();
    await Promise.resolve();
    expect(document.querySelector('.modal-mask')).toBeNull();
  });

  it('onOk 抛错时错误显示在弹窗里,弹窗不关', async () => {
    const handle = openModal({
      title: 't',
      onOk: () => { throw new Error('接口炸了'); },
    });
    handle.okButton.click();
    await Promise.resolve();
    await Promise.resolve();
    expect(q('.modal-error').textContent).toBe('接口炸了');
    expect(document.querySelector('.modal-mask')).not.toBeNull();
  });

  it('ESC 关闭弹窗', () => {
    openModal({ title: 't' });
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
    expect(document.querySelector('.modal-mask')).toBeNull();
  });

  it('点遮罩关闭弹窗', () => {
    const handle = openModal({ title: 't' });
    handle.mask.click();
    expect(document.querySelector('.modal-mask')).toBeNull();
  });

  it('点弹窗内部不关闭', () => {
    openModal({ title: 't' });
    q('.modal').click();
    expect(document.querySelector('.modal-mask')).not.toBeNull();
  });
});

describe('confirmDialog', () => {
  it('点确定返回 true', async () => {
    const p = confirmDialog('危险操作', '真的?', true);
    q<HTMLButtonElement>('.modal-actions .btn-danger').click();
    await expect(p).resolves.toBe(true);
  });

  it('点取消返回 false', async () => {
    const p = confirmDialog('危险操作', '真的?');
    q<HTMLButtonElement>('.modal-actions .btn-secondary').click();
    await expect(p).resolves.toBe(false);
  });

  it('ESC 返回 false', async () => {
    const p = confirmDialog('t', 'd');
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
    await expect(p).resolves.toBe(false);
  });

  it('危险操作的主按钮文案是「确认执行」', () => {
    void confirmDialog('t', 'd', true);
    expect(q('.modal-actions .btn-danger').textContent).toBe('确认执行');
  });

  it('普通操作的主按钮文案是「确定」', () => {
    void confirmDialog('t', 'd');
    expect(q('.modal-actions .btn-primary').textContent).toBe('确定');
  });
});

describe('amountDialog', () => {
  function fill(value: string): void {
    q<HTMLInputElement>('#amount-input').value = value;
  }

  function ok(): void {
    q<HTMLButtonElement>('.modal-actions .btn-danger').click();
  }

  it('留空表示全额退款,返回 0 交给后端算实际可退余额', async () => {
    // 返回 0 而不是 maxCents:部分退款过的订单,「全额」指的是剩下的那部分,
    // 只有后端知道已退了多少 —— 前端填订单全额会被后端拒掉。
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('');
    ok();
    await expect(p).resolves.toBe(0);
  });

  it('输入合法金额返回分', async () => {
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('12.34');
    ok();
    await expect(p).resolves.toBe(1234);
  });

  it('取消返回 null', async () => {
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('12.34');
    q<HTMLButtonElement>('.modal-actions .btn-secondary').click();
    await expect(p).resolves.toBeNull();
  });

  it('金额格式错误时留在弹窗里并提示,不返回', async () => {
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('12.345');
    ok();
    await Promise.resolve();
    expect(q('.modal-error').textContent).toContain('格式');
    // 弹窗还开着,promise 没有结算
    expect(document.querySelector('.modal-mask')).not.toBeNull();
    q<HTMLButtonElement>('.modal-actions .btn-secondary').click();
    await expect(p).resolves.toBeNull();
  });

  it('金额为 0 时提示「大于 0」', async () => {
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('0');
    ok();
    await Promise.resolve();
    expect(q('.modal-error').textContent).toContain('大于 0');
    q<HTMLButtonElement>('.modal-actions .btn-secondary').click();
    await expect(p).resolves.toBeNull();
  });

  it('超过可退金额时拒绝', async () => {
    const p = amountDialog({ title: '退款', maxCents: 19900 });
    fill('300');
    ok();
    await Promise.resolve();
    expect(q('.modal-error').textContent).toContain('超过');
    q<HTMLButtonElement>('.modal-actions .btn-secondary').click();
    await expect(p).resolves.toBeNull();
  });

  it('提示里显示最多可退金额(元)', () => {
    void amountDialog({ title: '退款', maxCents: 19900 });
    expect(q('.hint-text').textContent).toBe('最多可退 199.00 元');
  });
});

describe('withBusy', () => {
  it('执行期间按钮禁用,结束后恢复', async () => {
    const btn = document.createElement('button');
    btn.textContent = '提交';
    let released!: () => void;
    const gate = new Promise<void>((r) => { released = r; });
    const p = withBusy(btn, async () => { await gate; return 'ok'; });
    expect(btn.disabled).toBe(true);
    expect(btn.classList.contains('is-busy')).toBe(true);
    released();
    await expect(p).resolves.toBe('ok');
    expect(btn.disabled).toBe(false);
    expect(btn.classList.contains('is-busy')).toBe(false);
    expect(btn.textContent).toBe('提交');
  });

  it('已经在 busy 时直接返回,不再执行第二次(防连点重复下单)', async () => {
    const btn = document.createElement('button');
    let count = 0;
    let released!: () => void;
    const gate = new Promise<void>((r) => { released = r; });
    const p1 = withBusy(btn, async () => { count++; await gate; return count; });
    const p2 = await withBusy(btn, async () => { count++; return count; });
    expect(p2).toBeUndefined();
    released();
    await p1;
    expect(count).toBe(1);
  });

  it('函数抛错时按钮也恢复', async () => {
    const btn = document.createElement('button');
    await expect(withBusy(btn, async () => { throw new Error('炸'); })).rejects.toThrow('炸');
    expect(btn.disabled).toBe(false);
    expect(btn.classList.contains('is-busy')).toBe(false);
  });

  it('本来就禁用的按钮,结束后仍然禁用', async () => {
    // 有些按钮是按当前状态禁用的(比如「没选验证方式」时的「开启二次验证」)。
    // 一律在结束时设成 enabled,等于把这条约束悄悄拆了。
    const btn = document.createElement('button');
    btn.disabled = true;
    await withBusy(btn, async () => 'ok');
    expect(btn.disabled).toBe(true);
  });
});

describe('showError / showOK', () => {
  it('showError 显示错误文案', () => {
    const box = document.createElement('div');
    showError(box, new Error('出错了'));
    expect(box.textContent).toBe('出错了');
    expect(box.className).toBe('error-text');
  });

  it('showOK 显示成功文案', () => {
    const box = document.createElement('div');
    showOK(box, '保存成功');
    expect(box.textContent).toBe('保存成功');
    expect(box.className).toBe('hint-text');
  });

  it('box 为 null 时不崩', () => {
    expect(() => showError(null, new Error('x'))).not.toThrow();
    expect(() => showOK(null, 'x')).not.toThrow();
  });
});

describe('copyText', () => {
  it('复制成功返回 true', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    await expect(copyText('abc')).resolves.toBe(true);
    expect(writeText).toHaveBeenCalledWith('abc');
  });

  it('复制失败返回 false,不抛错', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'));
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    await expect(copyText('abc')).resolves.toBe(false);
  });
});
