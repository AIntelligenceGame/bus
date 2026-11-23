/** 与后端交互的封装:统一带上用户令牌与敏感操作票据。 */

const TOKEN_KEY = 'pay_token';
const VERIFY_KEY = 'pay_verify_token';
const VERIFY_EXP_KEY = 'pay_verify_exp';

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export function getVerifyToken(): string {
  return localStorage.getItem(VERIFY_KEY) || '';
}

export function setVerifyToken(token: string, ttl: number): void {
  // 后端没给有效期(或给了 0)就别缓存。
  // 之前这里兜底成 600s:后端其实已经判定票据过期,前端却当它还有效,
  // 结果每次敏感操作都被拒、而且永远不再弹验证框。
  if (!token || !(ttl > 0)) {
    clearVerifyToken();
    return;
  }
  localStorage.setItem(VERIFY_KEY, token);
  localStorage.setItem(VERIFY_EXP_KEY, String(Date.now() + ttl * 1000));
}

export function clearVerifyToken(): void {
  localStorage.removeItem(VERIFY_KEY);
  localStorage.removeItem(VERIFY_EXP_KEY);
}

/** 敏感操作票据是否仍在有效期内。 */
export function verifyTokenValid(): boolean {
  const token = getVerifyToken();
  const exp = parseInt(localStorage.getItem(VERIFY_EXP_KEY) || '0', 10);
  return !!token && exp > Date.now();
}

/** 后端返回的错误。 */
export class ApiError extends Error {
  readonly status: number;
  /** 响应体里的附加字段(如 mfa_required),供前端调整交互。 */
  readonly data: Record<string, unknown>;

  constructor(message: string, status: number, data: Record<string, unknown> = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.data = data;
  }
}

interface ApiOptions {
  method?: string;
  body?: unknown;
  /** true 时 401 不触发「清令牌 + 跳登录页」(登录接口本身用)。 */
  silent401?: boolean;
  /** 内部用:标记「已经自动重新验证并重试过一次」,防止无限递归。 */
  retried?: boolean;
}

/**
 * 调用后端 JSON 接口。
 *
 * 自动附带 X-User-Token 与 X-Verify-Token(票据有效时)。
 */
export async function api<T>(path: string, options: ApiOptions = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-User-Token': getToken(),
  };
  if (verifyTokenValid()) {
    headers['X-Verify-Token'] = getVerifyToken();
  }
  const resp = await fetch(path, {
    method: options.method || 'GET',
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const text = await resp.text();
  let data: unknown = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = {};
    }
  }
  if (!resp.ok) {
    const body = data && typeof data === 'object' ? (data as Record<string, unknown>) : {};
    const msg = 'error' in body ? String(body.error) : 'HTTP ' + resp.status;
    // 401:令牌失效(过期、被轮换、账号被锁)。
    // 主动清掉本地状态并回登录页,否则页面会一直拿着废令牌反复请求。
    if (resp.status === 401 && !options.silent401) {
      handleUnauthorized();
    }
    // 403 + 票据相关:本地这张敏感操作票据已经废了
    // (后端过期、服务重启换了主密钥、票据不属于当前用户、
    //  或者票据「用后即焚」被上一笔操作消耗掉了)。
    // 清掉它,下一次操作才会重新弹验证框;不清的话用户会拿着废票据
    // 反复被拒,连「重新验证」的机会都没有。
    if (resp.status === 403 && msg.includes('票据')) {
      clearVerifyToken();
      // 票据是用后即焚的:很可能是上一笔操作刚好把它用掉了。
      // 这里自动重新验证一次再重试,免得用户白挨一次报错。
      //
      // 安全上没问题:403 一定发生在业务逻辑之前(票据校验在最前面),
      // 被拒的请求没有执行任何资金操作,重试是安全的。
      if (!options.retried && !path.startsWith('/api/account/verify')) {
        const { requireVerify } = await import('./verify');
        await requireVerify(true);
        return await api<T>(path, { ...options, retried: true });
      }
    }
    throw new ApiError(msg, resp.status, body);
  }
  return data as T;
}

/** 令牌失效时的统一处理:清理本地状态并跳回登录页。 */
function handleUnauthorized(): void {
  clearToken();
  clearVerifyToken();
  if (location.hash !== '#/' && location.hash !== '') {
    location.hash = '#/';
  } else {
    // hash 本来就在首页,改 hash 不会触发 hashchange —— 手动派发一次,
    // 否则已经渲染出来的控制台会带着废令牌一直停在那儿。
    window.dispatchEvent(new Event('hashchange'));
  }
}

/**
 * 下载二进制资源(二维码图片),返回可用于 <img src> 的 object URL。
 *
 * 用请求头带令牌,而不是把令牌拼进 URL:
 * URL 会进浏览器历史、Referer 和服务端访问日志。
 *
 * **调用方必须负责 revokeObjectURL**:object URL 会把 Blob 一直钉在内存里,
 * 不释放的话发几次支付就把内存吃满了(表现就是页面越来越卡)。
 */
export async function apiBlobURL(path: string): Promise<string> {
  const resp = await fetch(path, {
    method: 'GET',
    headers: { 'X-User-Token': getToken() },
  });
  if (!resp.ok) {
    if (resp.status === 401) handleUnauthorized();
    throw new ApiError('HTTP ' + resp.status, resp.status);
  }
  return URL.createObjectURL(await resp.blob());
}

/**
 * 释放一个 object URL(替换旧图片时用)。
 *
 * 页面里反复替换 <img> 时,先 revoke 旧的,否则旧的 Blob 一直占着内存。
 */
export function releaseBlobURL(url: string | null | undefined): void {
  if (!url) return;
  try {
    URL.revokeObjectURL(url);
  } catch {
    /* 忽略:释放失败不影响功能 */
  }
}

/**
 * 下载文件(导出 CSV 之类),带令牌但不带在 URL 上。
 */
export async function apiDownload(path: string, fallbackName: string): Promise<void> {
  const resp = await fetch(path, {
    method: 'GET',
    headers: { 'X-User-Token': getToken() },
  });
  if (!resp.ok) {
    if (resp.status === 401) handleUnauthorized();
    throw new ApiError('HTTP ' + resp.status, resp.status);
  }
  const blob = await resp.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  // 服务端的文件名带了时间戳(orders-20260101-120000.csv),
  // 比前端写死的 orders.csv 好用 —— 导多份时不会互相覆盖。
  a.download = filenameFromDisposition(resp.headers.get('Content-Disposition')) || fallbackName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // **不能同步 revoke**:部分浏览器(尤其 Firefox)里 a.click() 只是发起下载,
  // 真正读 blob 是异步的;同步 revoke 会让下载中途失败(得到 0 字节的空文件)。
  // 延后一拍再释放,既能保证下载完成,又不会把 Blob 长期钉在内存里。
  setTimeout(() => releaseBlobURL(url), 30000);
}

/**
 * 从 Content-Disposition 里取文件名。
 *
 * 服务端格式:`attachment; filename="orders-20260101-120000.csv"`。
 * 取不到就返回空串(让调用方用兜底名)。
 */
function filenameFromDisposition(header: string | null): string {
  if (!header) return '';
  const m = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(header);
  if (!m) return '';
  const name = m[1].trim();
  // 文件名里带路径分隔符的话,浏览器会当成目录结构,截掉。
  return name.replace(/[\\/]/g, '_');
}
