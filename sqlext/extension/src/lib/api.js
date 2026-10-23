// api.js —— 与 sqlextd 服务端通信的薄封装。
//
// 页面和 Service Worker 共用。
// 关键点：令牌只存在 chrome.storage.local，页面（主世界）永远拿不到。

export const DEFAULT_API_BASE = 'http://127.0.0.1:28081';

export const STORAGE_KEYS = {
  apiBase: 'apiBase',
  token: 'token',
  user: 'user',
  sites: 'sites'
};

/** 读服务端地址。 */
export async function getApiBase() {
  const s = await chrome.storage.local.get(STORAGE_KEYS.apiBase);
  return (s[STORAGE_KEYS.apiBase] || DEFAULT_API_BASE).replace(/\/+$/, '');
}

/** 写服务端地址。 */
export async function setApiBase(base) {
  await chrome.storage.local.set({ [STORAGE_KEYS.apiBase]: String(base || '').replace(/\/+$/, '') });
}

/** 读登录令牌（可能为空）。 */
export async function getToken() {
  const s = await chrome.storage.local.get(STORAGE_KEYS.token);
  return s[STORAGE_KEYS.token] || '';
}

/** 写登录令牌。 */
export async function setToken(token) {
  if (token) {
    await chrome.storage.local.set({ [STORAGE_KEYS.token]: token });
  } else {
    await chrome.storage.local.remove(STORAGE_KEYS.token);
  }
}

/** 读缓存的用户信息。 */
export async function getCachedUser() {
  const s = await chrome.storage.local.get(STORAGE_KEYS.user);
  return s[STORAGE_KEYS.user] || null;
}

/** 写缓存的用户信息。 */
export async function setCachedUser(user) {
  if (user) {
    await chrome.storage.local.set({ [STORAGE_KEYS.user]: user });
  } else {
    await chrome.storage.local.remove(STORAGE_KEYS.user);
  }
}

/** 读已授权站点列表。 */
export async function getSites() {
  const s = await chrome.storage.local.get(STORAGE_KEYS.sites);
  return Array.isArray(s[STORAGE_KEYS.sites]) ? s[STORAGE_KEYS.sites] : [];
}

/** 写已授权站点列表。 */
export async function setSites(sites) {
  await chrome.storage.local.set({ [STORAGE_KEYS.sites]: Array.isArray(sites) ? sites : [] });
}

/**
 * 调服务端接口。
 * @param {string} path 以 / 开头的路径
 * @param {{method?: string, body?: any, auth?: boolean}} [opts]
 */
export async function api(path, opts = {}) {
  const method = opts.method || 'GET';
  const base = await getApiBase();
  const headers = { 'Content-Type': 'application/json' };

  if (opts.auth !== false) {
    const token = await getToken();
    if (token) {
      headers.Authorization = 'Bearer ' + token;
    }
  }

  let res;
  try {
    res = await fetch(base + path, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body)
    });
  } catch (e) {
    const err = new Error('连不上服务端 ' + base + '：' + (e && e.message ? e.message : e));
    err.code = 'network_error';
    throw err;
  }

  let data = null;
  try {
    data = await res.json();
  } catch (_) {
    // 服务端返回了非 JSON
  }

  if (!res.ok) {
    const err = new Error((data && data.error) || ('HTTP ' + res.status));
    err.code = (data && data.code) || String(res.status);
    err.status = res.status;
    throw err;
  }
  return data;
}
