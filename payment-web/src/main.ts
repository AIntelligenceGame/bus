/** 应用入口:路由 + 应用骨架(侧边栏 / 顶栏 / 内容区)。 */

// 样式在 index.html 里用 <link href="/src/styles.css"> 引入(Vite 会打进产物),
// 这里不再 import CSS,省掉「*.css 模块声明」在部分 IDE 里识别不了的问题。

import { el, clear } from './lib/dom';
import { api, ApiError } from './lib/api';
import { hasToken, login, logout, refreshMe, state } from './lib/store';
import { toast } from './lib/ui';
import { renderHome } from './pages/home';
import { renderChannels } from './pages/channels';
import { renderPlayground, releasePlaygroundResources } from './pages/playground';
import { renderOrders } from './pages/orders';
import { renderApps } from './pages/apps';
import { renderReports } from './pages/reports';
import { renderSecurity, releaseSecurityResources } from './pages/security';
import { renderKeys } from './pages/keys';

interface Route {
  path: string;
  title: string;
  desc: string;
  group: string;
  icon: string;
  render: (root: HTMLElement) => void;
  auth: boolean;
}

const ROUTES: Route[] = [
  {
    path: '/', title: '概览', desc: '账户与渠道的当前状态',
    group: '概览', icon: '◎',
    render: renderHome, auth: false,
  },
  {
    path: '/playground', title: '支付体验', desc: '发起一笔支付,查看二维码与客户端参数',
    group: '交易', icon: '▸',
    render: renderPlayground, auth: true,
  },
  {
    path: '/orders', title: '订单', desc: '订单查询、导出与退款',
    group: '交易', icon: '☰',
    render: renderOrders, auth: true,
  },
  {
    path: '/reports', title: '报表', desc: '按应用 / 渠道 / 天分组统计',
    group: '交易', icon: '▦',
    render: renderReports, auth: true,
  },
  {
    path: '/apps', title: '应用管理', desc: '产品维度:独立渠道配置与订单归类',
    group: '配置', icon: '▣',
    render: renderApps, auth: true,
  },
  {
    path: '/channels', title: '渠道配置', desc: '配置支付渠道密钥,加密入库',
    group: '配置', icon: '⚙',
    render: renderChannels, auth: true,
  },
  {
    path: '/keys', title: '开放接口', desc: 'API 密钥与签名接入',
    group: '配置', icon: '⇄',
    render: renderKeys, auth: true,
  },
  {
    path: '/security', title: '账户安全', desc: '口令、二次验证与审计日志',
    group: '账户', icon: '⛨',
    render: renderSecurity, auth: true,
  },
];

const appRoot = document.getElementById('app');
if (!appRoot) throw new Error('缺少 #app 容器');
const app: HTMLElement = appRoot;

/**
 * 回到概览页。
 *
 * hash 已经在 '#/' 时不会再触发 hashchange,需要主动 render 一次;
 * 否则交给 hashchange 监听器处理,避免重复渲染。
 */
function goHome(): void {
  if (location.hash === '#/' || location.hash === '' || location.hash === '#') {
    render();
  } else {
    location.hash = '#/';
  }
}

/** 取当前路由路径。 */
function currentPath(): string {
  const hash = location.hash.replace(/^#/, '');
  const raw = hash || location.pathname;
  if (!raw || raw === '/') return '/';
  const path = raw.split('?')[0];
  return path.endsWith('/') && path.length > 1 ? path.slice(0, -1) : path;
}

function routeOf(path: string): Route {
  return ROUTES.find((r) => r.path === path) || ROUTES[0];
}

// ---------------------------------------------------------------- 登录页

/**
 * 未登录时显示的独立登录页。
 *
 * 登录框是这一屏的主角,平台介绍只作为脚注,不能抢在前面 ——
 * 用户打开页面的第一件事是「登录」,不是「读产品简介」。
 *
 * 表单是**自适应**的:
 *
 *   - 「验证码」输入框默认不显示。没开二次验证的账户不该看到它,
 *     只有服务端回 mfa_required=true(说明口令已对、只差验证码)时才亮出来;
 *   - 「注册码」输入框默认不显示,只有平台设置了注册码时才显示
 *     (由公开的 /api/version 告知,这是平台级配置,不是账户级信息)。
 */
function renderAuth(): void {
  clear(app);
  const nameInput = el('input', {
    type: 'text', id: 'login-name', placeholder: '商户名称', autocomplete: 'username',
  });

  // 回填上次登录用的商户名。
  //
  // 二次验证是**按账户**绑定的。退出登录后如果商户名打错了一个字、或者换了一个,
  // 平台会当成新账户(首次登录按名称自动建号),新账户当然没开二次验证 ——
  // 表现出来就是「我明明开了二次验证,怎么一口令就进去了」。
  // 回填 + 明确提示,是为了让人一眼看出自己登的是不是同一个账户。
  const LAST_NAME_KEY = 'pay_last_name';
  const lastName = localStorage.getItem(LAST_NAME_KEY) || '';
  if (lastName) nameInput.value = lastName;
  const passInput = el('input', {
    type: 'password', id: 'login-password', placeholder: '登录口令', autocomplete: 'current-password',
  });
  const codeInput = el('input', {
    type: 'text', id: 'login-code', placeholder: '请输入验证码', autocomplete: 'one-time-code',
  });
  const regInput = el('input', {
    type: 'text', id: 'login-regcode', placeholder: '请输入注册码',
  });
  const err = el('div', { class: 'error-text' });

  const loginBtn = el('button', { class: 'btn btn-primary btn-block', text: '登录' }) as HTMLButtonElement;
  const codeBtn = el('button', { class: 'btn btn-secondary btn-block', text: '重新获取验证码' }) as HTMLButtonElement;

  // 备用验证方式:验证器 App 丢了 / 手机被偷 / App 被误删时的第二条腿。
  //
  // 绑了备用邮箱或手机号的账户,登录页会多一个入口:
  // 拿不到 App 里的动态码时,改用发到**备用地址**的验证码。
  const backupBtn = el('button', {
    class: 'btn btn-ghost btn-block', text: '验证器 App 丢了?用备用邮箱/短信',
  }) as HTMLButtonElement;
  const backupBox = el('div', { class: 'stack' }, backupBtn);
  backupBox.style.display = 'none';

  const field = (label: string, input: HTMLElement, extra?: Node): HTMLElement =>
    el('div', { class: 'field' },
      el('label', { class: 'label', text: label, for: input.id }),
      input,
      extra || null);

  // ---- 验证码区块(默认隐藏) ----
  const codeHint = el('div', { class: 'hint-text', text: '验证码已发送到该账户绑定的手机/邮箱,5 分钟内有效' });
  const codeField = field('验证码', codeInput, codeHint);
  codeField.style.display = 'none';
  const codeBtnBox = el('div', { class: 'stack' }, codeBtn);
  codeBtnBox.style.display = 'none';

  // ---- 注册码区块(默认隐藏,平台要求时才显示) ----
  const regField = field('注册码', regInput,
    el('div', { class: 'hint-text', text: '平台设置了注册码时需要填写' }));
  regField.style.display = 'none';

  /**
   * 显示验证码输入框(服务端说该账户开了二次验证时才调用)。
   *
   * mfaType 决定交互形态:
   *
   *   - email/sms:验证码由服务端下发,自动发一次,也能手动「重新获取」;
   *   - totp:动态码由验证器 App 本地算出,没有「下发」这回事,
   *     所以既不自动发送,也不给「重新获取验证码」按钮。
   */
  let mfaShown = false;
  let codeSent = false;
  /**
   * 显示验证码输入框(服务端说该账户开了二次验证时才调用)。
   *
   * mfaType 决定交互形态:
   *
   *   - email/sms:验证码由服务端下发,自动发一次,也能手动「重新获取」;
   *   - totp:动态码由验证器 App 本地算出,没有「下发」这回事,
   *     所以既不自动发送,也不给「重新获取验证码」按钮。
   *
   * hasBackup:账户绑了备用验证方式时,额外给一条「用备用邮箱/短信」的路
   * (验证器 App 丢了也不至于登不进来)。
   */
  const showMFA = (mfaType: string, hasBackup: boolean): void => {
    const isTOTP = mfaType === 'totp';
    if (!mfaShown) {
      mfaShown = true;
      codeField.style.display = '';
    }
    if (isTOTP) {
      codeBtnBox.style.display = 'none';
      codeField.querySelector('label')!.textContent = '验证器 App 动态码';
      codeInput.placeholder = 'App 里的 6 位动态码';
      codeHint.textContent = '用验证器 App 打开「支付配置选择平台」条目,输入里面的 6 位动态码';
    } else {
      codeBtnBox.style.display = '';
      codeField.querySelector('label')!.textContent = '验证码';
      codeInput.placeholder = '请输入验证码';
      codeHint.textContent = '验证码已发送到该账户绑定的手机/邮箱,5 分钟内有效';
      if (!codeSent) {
        codeSent = true;
        void sendLoginCode();
      }
    }
    backupBox.style.display = hasBackup ? '' : 'none';
    codeInput.focus();
  };

  /** 发送**备用**地址的登录验证码(验证器 App 丢了时用)。 */
  const sendBackupCode = async (): Promise<void> => {
    const name = nameInput.value.trim();
    if (!name) {
      err.textContent = '请先填写商户名称';
      return;
    }
    backupBtn.classList.add('is-busy');
    try {
      const res = await api<{ message?: string }>('/api/login/code', {
        method: 'POST',
        body: { name, backup: true },
        silent401: true,
      });
      toast(res.message || '如果该账户绑了备用验证方式,验证码已发送到备用地址', 'info', 4000);
      codeInput.placeholder = '备用地址收到的 6 位验证码';
      codeHint.textContent = '验证码已发到「备用接收地址」,5 分钟内有效,填在上面的输入框里;'
        + '没收到的话,检查一下账户里的邮件/短信通道配置(没配通道时码只写服务器日志)';
    } catch (e) {
      err.textContent = e instanceof Error ? e.message : String(e);
    } finally {
      backupBtn.classList.remove('is-busy');
    }
  };

  /** 发送登录验证码。 */
  const sendLoginCode = async (): Promise<void> => {
    const name = nameInput.value.trim();
    if (!name) {
      err.textContent = '请先填写商户名称';
      return;
    }
    codeBtn.classList.add('is-busy');
    try {
      const res = await api<{ message?: string }>('/api/login/code', {
        method: 'POST',
        body: { name },
        silent401: true,
      });
      toast(res.message || '如果该账户已开启二次验证,验证码已发送到其绑定的接收地址', 'info', 4000);
    } catch (e) {
      err.textContent = e instanceof Error ? e.message : String(e);
    } finally {
      codeBtn.classList.remove('is-busy');
    }
  };

  const submit = async (): Promise<void> => {
    err.textContent = '';
    const name = nameInput.value.trim();
    const password = passInput.value;
    if (!name || !password) {
      err.textContent = '请填写商户名称与登录口令';
      return;
    }
    loginBtn.classList.add('is-busy');
    try {
      const res = await login(name, password, codeInput.value.trim(), regInput.value.trim());
      // 记住这次用的商户名:下次退出后回来直接回填,免得换名字登成新账户。
      localStorage.setItem(LAST_NAME_KEY, name);
      if (res.created) {
        // 服务端是「按名称自动建号」的:名称不存在就新建一个账户。
        // 新账户什么设置都没有(二次验证、渠道、密钥全是空的),
        // 不说清楚的话,看着就像「我明明开了二次验证,怎么没生效」。
        toast('已创建新账户「' + name + '」:首次登录会自动建号,' +
          '二次验证等安全设置需要在这个账户里重新配置', 'info', 7000);
      } else if (res.mfa_skipped) {
        // 二次验证绑定还在、但验证码发不出去 → 平台登录时跳过了它。
        // 必须如实告诉用户,否则「我明明开了二次验证,怎么没拦我」会变成安全困惑。
        toast(res.mfa_note || '二次验证无效,本次登录已跳过', 'err', 6000);
      } else {
        toast('登录成功', 'ok');
      }
      goHome();
    } catch (e) {
      // 口令已对、只差验证码:把验证码框显示出来,而不是让用户对着一个
      // 不知道存在的框去猜。
      if (e instanceof ApiError && e.data.mfa_required === true) {
        showMFA(String(e.data.mfa_type || ''), e.data.mfa_backup === true);
      }
      err.textContent = e instanceof Error ? e.message : String(e);
    } finally {
      loginBtn.classList.remove('is-busy');
    }
  };

  loginBtn.addEventListener('click', () => void submit());
  codeBtn.addEventListener('click', () => void sendLoginCode());
  backupBtn.addEventListener('click', () => void sendBackupCode());

  app.appendChild(el('div', { class: 'auth' },
    el('div', { class: 'auth-card' },
      el('div', { class: 'auth-brand' },
        el('div', { class: 'brand-mark', text: '付' }),
        el('h1', { text: '支付配置选择平台' }),
        el('p', { text: '商户自助配置支付渠道,密钥加密入库' })),
      field('商户名称', nameInput),
      field('登录口令', passInput,
        el('div', { class: 'hint-text', text: '首次登录将自动创建账户,口令不少于 8 位' })),
      regField,
      codeField,
      err,
      el('div', { class: 'stack' },
        loginBtn,
        codeBtnBox,
        backupBox),
      el('div', { class: 'auth-foot', text: '首次使用?填一个商户名称与口令即可创建账户。二次验证按账户绑定,换一个商户名就是另一个账户。' }))));

  // 平台是否要求注册码(平台级配置,公开)。
  void api<{ registration_code_required?: boolean }>('/api/version')
    .then((v) => {
      if (v.registration_code_required) regField.style.display = '';
    })
    .catch(() => undefined);
}

// ---------------------------------------------------------------- 应用骨架

let shell: {
  sidebar: HTMLElement;
  content: HTMLElement;
  title: HTMLElement;
  desc: HTMLElement;
} | null = null;

function buildShell(): void {
  clear(app);

  const sidebar = el('aside', { class: 'sidebar' });
  sidebar.appendChild(el('div', { class: 'sidebar-brand' },
    el('div', { class: 'brand-mark', text: '付' }),
    el('div', {},
      el('div', { class: 'brand-name', text: '支付配置平台' }),
      el('div', { class: 'brand-sub', text: 'Payment Console' }))));

  // 分组导航
  const groups: string[] = [];
  for (const r of ROUTES) if (!groups.includes(r.group)) groups.push(r.group);
  for (const g of groups) {
    const group = el('div', { class: 'nav-group' },
      el('div', { class: 'nav-group-title', text: g }));
    for (const r of ROUTES.filter((x) => x.group === g)) {
      group.appendChild(el('a', {
        class: 'nav-item',
        href: '#' + r.path,
        'data-path': r.path,
      },
        el('span', { class: 'nav-icon', text: r.icon }),
        el('span', { text: r.title })));
    }
    sidebar.appendChild(group);
  }

  const userBox = el('div', { class: 'sidebar-foot' });
  sidebar.appendChild(userBox);

  const title = el('div', { class: 'topbar-title' });
  const desc = el('div', { class: 'topbar-desc' });
  const logoutBtn = el('button', { class: 'btn btn-secondary btn-sm', text: '退出登录' });
  logoutBtn.addEventListener('click', () => {
    void (async () => {
      logoutBtn.classList.add('is-busy');
      await logout();
      toast('已退出登录', 'info');
      goHome();
    })();
  });

  const topbar = el('header', { class: 'topbar' },
    el('div', {}, title, desc),
    el('div', { class: 'topbar-actions' }, logoutBtn));

  const content = el('div', { class: 'content' });
  const main = el('div', { class: 'main' }, topbar, content);
  app.appendChild(el('div', { class: 'shell' }, sidebar, main));

  shell = { sidebar, content, title, desc };
  renderUserBox(userBox);
}

function renderUserBox(box: HTMLElement): void {
  clear(box);
  box.appendChild(el('div', { class: 'sidebar-user', text: state.userName || '已登录' }));
  box.appendChild(el('div', { class: 'sidebar-user-sub', text: state.userId }));
}

function render(): void {
  const route = routeOf(currentPath());

  if (!hasToken()) {
    shell = null;
    renderAuth();
    return;
  }

  if (!shell) buildShell();

  // 侧边栏底部的用户信息(登录后 / 刷新后都要同步)
  const foot = shell!.sidebar.querySelector('.sidebar-foot');
  if (foot) renderUserBox(foot as HTMLElement);

  // 导航高亮
  const links = shell!.sidebar.querySelectorAll('.nav-item');
  links.forEach((link) => {
    const path = link.getAttribute('data-path');
    if (path === route.path) link.classList.add('active');
    else link.classList.remove('active');
  });

  shell!.title.textContent = route.title;
  shell!.desc.textContent = route.desc;

  // 离开「支付体验」页时释放二维码的 object URL:
  // object URL 会把 Blob 钉在内存里,不释放就一直涨(页面越来越卡)。
  releasePlaygroundResources();
  // 同理:离开「账户安全」页时释放 TOTP 二维码的 object URL。
  releaseSecurityResources();

  clear(shell!.content);
  try {
    route.render(shell!.content);
  } catch (err) {
    shell!.content.appendChild(el('div', { class: 'card' },
      el('h2', { text: '页面加载失败' }),
      el('p', { class: 'error-text', text: err instanceof Error ? err.message : String(err) })));
  }
}

window.addEventListener('hashchange', render);

if (hasToken()) {
  void refreshMe()
    .catch(() => undefined)
    .finally(render);
} else {
  render();
}
