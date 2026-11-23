/** 极简 DOM 辅助函数(不引入任何框架)。 */

type AttrValue = string | number | boolean | undefined | null | ((ev: Event) => unknown);
type Attrs = Record<string, AttrValue>;
type Child = Node | string | number | null | undefined | false;

/** 创建元素:el('div', {class:'card'}, '文本', childNode) */
export function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs?: Attrs,
  ...children: Child[]
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (attrs) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value === undefined || value === null || value === false) continue;
      if (key === 'class') node.className = String(value);
      else if (key === 'text') node.textContent = String(value);
      else if (key.startsWith('data-') || key.startsWith('aria-')) {
        node.setAttribute(key, String(value));
      } else if (typeof value === 'function') {
        // 事件名归一化:onclick → click。
        //
        // 之前直接 addEventListener(key, ...) 会把 "onclick" 当成事件类型注册,
        // 而 DOM 的 click 事件名叫 "click",结果这个监听器永远不会被触发 ——
        // 属于「写了没生效」这种最难查的坑。
        // 非 on 前缀的 key 视为自定义事件名,原样注册。
        const eventName = /^on[a-z]/.test(key) ? key.slice(2).toLowerCase() : key;
        node.addEventListener(eventName, value as EventListener);
      } else if (key in node) {
        (node as unknown as Record<string, unknown>)[key] = value;
      } else {
        node.setAttribute(key, String(value));
      }
    }
  }
  append(node, children);
  return node;
}

/** 给容器追加子节点。 */
export function append(parent: Node, children: Child[]): void {
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    parent.appendChild(typeof child === 'string' || typeof child === 'number'
      ? document.createTextNode(String(child))
      : child);
  }
}

/** 清空容器。 */
export function clear(node: Node): void {
  while (node.firstChild) node.removeChild(node.firstChild);
}

/** 查询元素(非空断言)。 */
export function $<T extends Element = HTMLElement>(selector: string, root: ParentNode = document): T {
  const found = root.querySelector<T>(selector);
  if (!found) throw new Error(`找不到元素: ${selector}`);
  return found;
}

/** 读取输入框的值。 */
export function inputValue(id: string): string {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.value.trim() : '';
}

/** 读取复选框状态。 */
export function checked(id: string): boolean {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.checked : false;
}

/** 读取下拉框的值。 */
export function selectValue(id: string): string {
  const node = document.getElementById(id) as HTMLSelectElement | null;
  return node ? node.value : '';
}

/** 创建一个「卡片」区块。 */
export function card(title: string, ...children: Child[]): HTMLElement {
  return el('div', { class: 'card' }, el('h2', { text: title }), ...children);
}

/**
 * 表格渲染(外面套一层 .table-wrap,负责横向滚动与边框圆角)。
 */
export function table(headers: string[], rows: (Node | string)[][]): HTMLElement {
  const thead = el('thead', {}, el('tr', {}, ...headers.map((h) => el('th', { text: h }))));
  const tbody = el('tbody');
  for (const row of rows) {
    tbody.appendChild(el('tr', {}, ...row.map((cell) => {
      if (typeof cell === 'string') return el('td', { text: cell });
      // 已经是单元格的就直接用;否则包一层 <td>。
      //
      // 不包的话,span/button 会直接落在 <tr> 里 —— 这是非法 HTML,
      // 而且样式全挂在 `tbody td` 上(padding、border-bottom、hover 背景、
      // 首列加粗),没被包住的那几列就全都没有:列对不齐、只有最后一列有线。
      if (cell instanceof HTMLElement &&
          (cell.tagName === 'TD' || cell.tagName === 'TH')) {
        return cell;
      }
      return el('td', {}, cell);
    })));
  }
  return el('div', { class: 'table-wrap' }, el('table', {}, thead, tbody));
}

/** 下拉框的一个选项。 */
export function option(value: string, label: string): HTMLOptionElement {
  return el('option', { value, text: label });
}
