/**
 * 测试环境准备。
 *
 * Node 26 自带一个实验性的 globalThis.localStorage(需要 --localstorage-file),
 * 没给那个参数时它是 undefined,并且会把 jsdom 自带的 localStorage 挡掉 ——
 * 于是测试里 localStorage.setItem 直接报「undefined is not an object」。
 *
 * 这里补一个内存版 Storage,行为跟浏览器一致(存字符串、getItem 读不存在的键
 * 返回 null 而不是 undefined、removeItem 后确实清掉)。
 */

class MemoryStorage implements Storage {
  private map = new Map<string, string>();

  get length(): number {
    return this.map.size;
  }

  clear(): void {
    this.map.clear();
  }

  getItem(key: string): string | null {
    const v = this.map.get(String(key));
    return v === undefined ? null : v;
  }

  key(index: number): string | null {
    const keys = [...this.map.keys()];
    return index < 0 || index >= keys.length ? null : keys[index];
  }

  removeItem(key: string): void {
    this.map.delete(String(key));
  }

  setItem(key: string, value: string): void {
    this.map.set(String(key), String(value));
  }
}

function ensureStorage(name: 'localStorage' | 'sessionStorage'): void {
  const g = globalThis as unknown as Record<string, unknown>;
  if (!g[name]) {
    Object.defineProperty(g, name, {
      value: new MemoryStorage(),
      configurable: true,
      writable: true,
    });
  }
}

ensureStorage('localStorage');
ensureStorage('sessionStorage');
