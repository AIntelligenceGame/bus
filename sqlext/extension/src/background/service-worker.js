// service-worker.js —— 扩展的"后台"。
//
// 它是唯一持有登录令牌、也唯一直接和服务端说话的地方。
//
// 关键安全设计：
//   1. 令牌只存在 chrome.storage.local，主世界的页面脚本永远拿不到
//   2. **按站点授权**：只有用户明确授权过的网站，才能调用解析接口。
//      否则任何网站都能白嫖用户的余额——这是这类插件最大的被滥用口子。
//   3. 内容脚本只注册在已授权的站点上（动态注册，不是 <all_urls>），
//      未授权的站点上页面根本拿不到 window.sqlext

import {
  api,
  getToken,
  setToken,
  setCachedUser,
  getCachedUser,
  getSites,
  setSites,
  getApiBase,
  setApiBase
} from '../lib/api.js';

const CONTENT_SCRIPT_ID = 'sqlext-host';

// ---------------------------------------------------------------- 安装

chrome.runtime.onInstalled.addListener(async () => {
  await syncSites();
  await ensureContentScript();
  // 打开设置页让用户填服务端地址
  chrome.tabs.create({ url: chrome.runtime.getURL('src/pages/settings.html') });
});

chrome.runtime.onStartup.addListener(async () => {
  await syncSites();
  await ensureContentScript();
});

/** 同步授权列表并按需注册内容脚本。 */
async function syncSites() {
  await fetchSites();
  await ensureContentScript();
}

// ---------------------------------------------------------------- 内容脚本注册

/**
 * 让内容脚本只在已授权的站点上运行。
 *
 * 用动态注册而不是 manifest 里写 <all_urls>：
 *   - 装插件时不申请"读取所有网站数据"权限，商店审核友好
 *   - 用户没授权的站点，页面里连 window.sqlext 都不存在
 */
async function ensureContentScript() {
  const sites = await getSites();
  const matches = sites.map(toMatchPattern).filter(Boolean);
  try {
    const existing = await chrome.scripting.getRegisteredContentScripts({ ids: [CONTENT_SCRIPT_ID] });
    if (existing && existing.length) {
      await chrome.scripting.updateContentScripts([
        { id: CONTENT_SCRIPT_ID, matches: matches.length ? matches : ['http://127.0.0.1/*'] }
      ]);
    } else {
      await chrome.scripting.registerContentScripts([
        {
          id: CONTENT_SCRIPT_ID,
          matches: matches.length ? matches : ['http://127.0.0.1/*'],
          js: ['src/content/host.js'],
          runAt: 'document_start',
          allFrames: false
        }
      ]);
    }
  } catch (e) {
    console.warn('[sqlext] 注册内容脚本失败：', e);
  }
}

/** 把 https://example.com 转成 Chrome 的匹配模式。 */
function toMatchPattern(origin) {
  try {
    const u = new URL(origin);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') {
      return null;
    }
    return u.protocol + '//' + u.hostname + (u.port ? ':' + u.port : '') + '/*';
  } catch (_) {
    return null;
  }
}

// ---------------------------------------------------------------- 消息路由

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || msg.kind !== 'sqlext') {
    return false;
  }
  handlePageMessage(msg, sender)
    .then(sendResponse)
    .catch((e) => sendResponse({ ok: false, error: String((e && e.message) || e) }));
  // 返回 true 表示异步回复
  return true;
});

async function handlePageMessage(msg, sender) {
  const origin = (sender && sender.origin) || msg.pageOrigin || '';

  // 登录态检查
  const token = await getToken();
  if (!token) {
    return { ok: false, error: '尚未登录，请先点击插件图标登录', code: 'not_logged_in' };
  }

  switch (msg.type) {
    case 'parse':
      return handleParse(msg, origin, token);
    case 'account':
      return handleAccount(token);
    case 'listSites': {
      const sites = await fetchSites();
      return { ok: true, result: { sites } };
    }
    case 'requestSite':
      return handleRequestSite(origin);
    default:
      return { ok: false, error: '未知请求类型: ' + msg.type, code: 'bad_request' };
  }
}

/** 解析请求：先查授权，再调服务端。 */
async function handleParse(msg, origin, token) {
  const sites = await getSites();
  // 本地缓存为空说明还没同步过（刚装 / 刚登录），此时交给服务端判定，
  // 否则会误拒掉本来就授权过的网站。
  if (sites.length > 0 && !isOriginAllowed(origin, sites)) {
    return {
      ok: false,
      error: '当前网站（' + origin + '）未获授权。请在插件里授权该网站后再调用。',
      code: 'site_not_authorized'
    };
  }
  const sql = (msg.payload && msg.payload.sql) || '';
  if (!sql.trim()) {
    return { ok: false, error: 'SQL 不能为空', code: 'empty_sql' };
  }
  const data = await api('/api/v1/parse', {
    method: 'POST',
    body: { sql, origin }
  });
  return { ok: true, result: data };
}

async function handleAccount(token) {
  const data = await api('/api/v1/auth/me');
  await setCachedUser(data.user);
  return { ok: true, result: { user: data.user, balance: data.balance } };
}

/** 页面请求授权当前网站：打开授权页让用户点确认。 */
async function handleRequestSite(origin) {
  if (!origin || origin === 'null') {
    return { ok: false, error: '无法识别当前网站', code: 'bad_origin' };
  }
  const url = chrome.runtime.getURL(
    'src/pages/authorize.html?origin=' + encodeURIComponent(origin)
  );
  await chrome.windows.create({ url, type: 'popup', width: 460, height: 420 });
  return { ok: true, result: { origin, message: '已打开授权页，请点击"允许"' } };
}

function isOriginAllowed(origin, sites) {
  if (!origin) {
    return false;
  }
  let host = origin;
  try {
    host = new URL(origin).host;
  } catch (_) {
    // 保留原样
  }
  return sites.some((s) => {
    if (s === origin) {
      return true;
    }
    try {
      return new URL(s).host === host;
    } catch (_) {
      return false;
    }
  });
}

// ---------------------------------------------------------------- 内部页面消息

// 扩展自己的页面（登录/注册/充值/设置/授权）发来的消息
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || msg.kind !== 'sqlext-ui') {
    return false;
  }
  handleUIMessage(msg)
    .then(sendResponse)
    .catch((e) => sendResponse({ ok: false, error: String((e && e.message) || e) }));
  return true;
});

async function handleUIMessage(msg) {
  switch (msg.type) {
    case 'login': {
      const data = await api('/api/v1/auth/login', {
        method: 'POST',
        auth: false,
        body: { email: msg.payload.email, password: msg.payload.password }
      });
      await setToken(data.token);
      await setCachedUser(data.user);
      return { ok: true, result: data };
    }
    case 'register': {
      const data = await api('/api/v1/auth/register', {
        method: 'POST',
        auth: false,
        body: { email: msg.payload.email, password: msg.payload.password }
      });
      await setToken(data.token);
      await setCachedUser(data.user);
      return { ok: true, result: data };
    }
    case 'logout': {
      try {
        await api('/api/v1/auth/logout', { method: 'POST' });
      } catch (_) {
        // 服务端不可达也要能本地登出
      }
      await setToken('');
      await setCachedUser(null);
      return { ok: true, result: {} };
    }
    case 'me': {
      const data = await api('/api/v1/auth/me');
      await setCachedUser(data.user);
      return { ok: true, result: data };
    }
    case 'balance': {
      const data = await api('/api/v1/billing/balance');
      return { ok: true, result: data };
    }
    case 'createOrder': {
      const data = await api('/api/v1/billing/orders', {
        method: 'POST',
        body: { amount: msg.payload.amount, channel: msg.payload.channel }
      });
      return { ok: true, result: data };
    }
    case 'listOrders': {
      const data = await api('/api/v1/billing/orders');
      return { ok: true, result: data };
    }
    case 'getApiBase': {
      return { ok: true, result: { apiBase: await getApiBase() } };
    }
    case 'setApiBase': {
      // 主机权限由调用方（扩展页面）申请——Service Worker 里没有用户手势，
      // chrome.permissions.request() 会静默失败。这里只负责存。
      await setApiBase(msg.payload.apiBase);
      return { ok: true, result: { apiBase: await getApiBase() } };
    }
    case 'listSites': {
      // 服务端是授权信息的唯一真相来源（跨设备同步），
      // 本地缓存只是为了快速判断和注册内容脚本。
      const sites = await fetchSites();
      await ensureContentScript();
      return { ok: true, result: { sites } };
    }
    case 'addSite': {
      const origin = normalizeOrigin(msg.payload.origin);
      if (!origin) {
        return { ok: false, error: '站点地址不合法' };
      }
      const res = await api('/api/v1/sites', { method: 'POST', body: { origin } });
      const sites = (res && res.sites) || [];
      await setSites(sites);
      await ensureContentScript();
      return { ok: true, result: { sites } };
    }
    case 'removeSite': {
      const origin = normalizeOrigin(msg.payload.origin);
      const res = await api(
        '/api/v1/sites?origin=' + encodeURIComponent(origin),
        { method: 'DELETE' }
      );
      const sites = (res && res.sites) || [];
      await setSites(sites);
      await ensureContentScript();
      return { ok: true, result: { sites } };
    }
    default:
      return { ok: false, error: '未知请求类型: ' + msg.type };
  }
}

function normalizeOrigin(origin) {
  try {
    const u = new URL(origin);
    return u.protocol + '//' + u.host;
  } catch (_) {
    return '';
  }
}

/**
 * 从服务端拉已授权站点列表。
 *
 * 拉不到（没登录 / 服务端不可达）时退回本地缓存，
 * 至少保证"上次授权过的网站还能用"。
 */
async function fetchSites() {
  try {
    const res = await api('/api/v1/sites');
    if (res && Array.isArray(res.sites)) {
      await setSites(res.sites);
      return res.sites;
    }
  } catch (_) {
    // 忽略：走下面的兜底
  }
  return getSites();
}
