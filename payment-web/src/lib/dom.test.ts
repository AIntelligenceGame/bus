import { describe, expect, it } from 'vitest';
import {
  $, append, card, checked, clear, el, inputValue, option, selectValue, table,
} from './dom';

describe('el', () => {
  it('建元素并设 class / text', () => {
    const node = el('div', { class: 'card', text: '标题' });
    expect(node.tagName).toBe('DIV');
    expect(node.className).toBe('card');
    expect(node.textContent).toBe('标题');
  });

  it('子节点可以是字符串、数字、元素', () => {
    const child = el('span', { text: 'x' });
    const node = el('div', {}, '文字', 42, child);
    expect(node.childNodes.length).toBe(3);
    expect(node.textContent).toBe('文字42x');
  });

  it('null / undefined / false 子节点被跳过', () => {
    const node = el('div', {}, null, undefined, false, '保留');
    expect(node.childNodes.length).toBe(1);
    expect(node.textContent).toBe('保留');
  });

  it('undefined / null / false 属性被忽略', () => {
    const node = el('input', { value: undefined, name: null, disabled: false });
    expect(node.getAttribute('name')).toBeNull();
    expect(node.hasAttribute('disabled')).toBe(false);
  });

  it('data- / aria- 用 setAttribute', () => {
    const node = el('div', { 'data-x': '1', 'aria-label': '标签' });
    expect(node.getAttribute('data-x')).toBe('1');
    expect(node.getAttribute('aria-label')).toBe('标签');
  });

  it('函数值当事件监听器', () => {
    let hit = 0;
    const node = el('button', { onclick: () => { hit++; } });
    node.dispatchEvent(new Event('click'));
    expect(hit).toBe(1);
  });

  it('on 前缀的事件名要映射到真实 DOM 事件名', () => {
    // el() 内部把 onclick 归一化成 click,否则监听器永远不触发。
    let submit = 0;
    let input = 0;
    const node = el('form', {
      onsubmit: () => { submit++; },
      oninput: () => { input++; },
    });
    node.dispatchEvent(new Event('submit'));
    node.dispatchEvent(new Event('input'));
    expect(submit).toBe(1);
    expect(input).toBe(1);
  });

  it('非 on 前缀的函数值当自定义事件名注册', () => {
    let hit = 0;
    const node = el('div', { myevent: () => { hit++; } });
    node.dispatchEvent(new Event('myevent'));
    expect(hit).toBe(1);
  });

  it('text 属性走 textContent,不会当成 HTML 解析(XSS)', () => {
    const node = el('div', { text: '<img src=x onerror=alert(1)>' });
    expect(node.querySelector('img')).toBeNull();
    expect(node.textContent).toBe('<img src=x onerror=alert(1)>');
  });
});

describe('append / clear', () => {
  it('append 追加子节点并跳过空值', () => {
    const parent = el('div');
    append(parent, ['a', null, el('b'), undefined, false, 7]);
    expect(parent.childNodes.length).toBe(3);
    expect(parent.textContent).toBe('a7');
  });

  it('clear 清空所有子节点', () => {
    const parent = el('div', {}, 'a', 'b');
    clear(parent);
    expect(parent.childNodes.length).toBe(0);
  });
});

describe('$', () => {
  it('找到就返回', () => {
    const node = el('div', { id: 'target' });
    document.body.appendChild(node);
    expect($('#target')).toBe(node);
    node.remove();
  });

  it('找不到必须抛错,不能返回 null 让调用方到处判空', () => {
    expect(() => $('#nope')).toThrow(/找不到元素/);
  });
});

describe('输入类辅助', () => {
  it('inputValue 读值并去空格,元素不存在返回空串', () => {
    const input = el('input', { id: 'i1' }) as HTMLInputElement;
    input.value = '  hello  ';
    document.body.appendChild(input);
    expect(inputValue('i1')).toBe('hello');
    expect(inputValue('missing')).toBe('');
    input.remove();
  });

  it('checked 读勾选状态,元素不存在返回 false', () => {
    const box = el('input', { id: 'c1' }) as HTMLInputElement;
    box.checked = true;
    document.body.appendChild(box);
    expect(checked('c1')).toBe(true);
    expect(checked('missing')).toBe(false);
    box.remove();
  });

  it('selectValue 读下拉框当前值', () => {
    const sel = el('select', { id: 's1' }) as HTMLSelectElement;
    sel.appendChild(option('a', 'A'));
    sel.appendChild(option('b', 'B'));
    document.body.appendChild(sel);
    expect(selectValue('s1')).toBe('a');
    sel.value = 'b';
    expect(selectValue('s1')).toBe('b');
    expect(selectValue('missing')).toBe('');
    sel.remove();
  });
});

describe('card / table', () => {
  it('card 带标题', () => {
    const node = card('我的标题');
    expect(node.querySelector('h2')?.textContent).toBe('我的标题');
  });

  it('table 表头与行数正确', () => {
    const node = table(['名称', '金额'], [['a', '1'], ['b', '2']]);
    expect(node.querySelectorAll('th').length).toBe(2);
    expect(node.querySelectorAll('tbody tr').length).toBe(2);
    expect(node.querySelector('tbody td')?.textContent).toBe('a');
  });

  it('table 的字符串单元格也是文本,不是 HTML', () => {
    const node = table(['x'], [['<b>粗体</b>']]);
    expect(node.querySelector('b')).toBeNull();
    expect(node.querySelector('td')?.textContent).toBe('<b>粗体</b>');
  });

  it('table 把节点单元格包进 td,样式才生效', () => {
    // 样式全挂在 `tbody td` 上(padding、border-bottom、hover、首列加粗),
    // 节点单元格不包 td 的话这些样式就丢了 —— 列对不齐、只有最后一列有线。
    const span = el('span', { class: 'cell-mono', text: 'M001' });
    const btn = el('button', { class: 'btn', text: '退款' });
    const node = table(['订单号', '操作'], [[span, btn]]);
    const tds = node.querySelectorAll('tbody td');
    expect(tds.length).toBe(2);
    expect(tds[0].querySelector('span.cell-mono')?.textContent).toBe('M001');
    expect(tds[1].querySelector('button.btn')?.textContent).toBe('退款');
    // <tr> 里只能有 td/th
    for (const child of node.querySelectorAll('tbody tr > *')) {
      expect(['TD', 'TH']).toContain(child.tagName);
    }
  });

  it('table 已经是 td 的单元格不重复包', () => {
    const td = el('td', { class: 'cell-actions' }, el('button', { class: 'btn', text: 'x' }));
    const node = table(['操作'], [[td]]);
    const tds = node.querySelectorAll('tbody td');
    expect(tds.length).toBe(1);
    expect(tds[0].className).toBe('cell-actions');
  });
});
