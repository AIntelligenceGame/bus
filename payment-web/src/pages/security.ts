/** 账户安全页:口令、二次验证(邮箱/短信/验证器 App)、验证码下发通道、审计日志。 */

import { api, apiBlobURL, releaseBlobURL } from '../lib/api';
import { el, table } from '../lib/dom';
import { badge, card, copyText, emptyState, field, openModal, select, sel, setVal, toast, withBusy } from '../lib/ui';
import type { AuditLog, MFAChannel, PageResponse, SecurityResponse } from '../lib/types';
import { requireVerify } from '../lib/verify';

/**
 * TOTP 二维码的 object URL。
 *
 * object URL 会把 Blob 一直钉在内存里 —— 每点一次「重新生成密钥」就多一张
 * 永不释放的图,反复换绑几次内存就单调上涨(手机端更早出现卡顿)。
 * 所以换图之前先把旧的释放掉,离开本页时也要释放。
 */
let totpQRURL: string | null = null;

/** 释放 TOTP 二维码的 object URL(离开本页时调用)。 */
export function releaseSecurityResources(): void {
  releaseBlobURL(totpQRURL);
  totpQRURL = null;
}

/**
 * 二次验证方式。
 *
 * 第一项是「关闭」,而且是**默认值**:
 * 并不是每个商户都开通了邮箱、短信或验证器 App,平台不能默认替他们选一种
 * —— 也不能让「没开通任何一种」这件事变成一个走不通的死路。
 * 想开启就明确选一种,没选就是关闭。
 */
const MFA_TYPES: { value: string; label: string }[] = [
  { value: 'off', label: '关闭(不启用二次验证)' },
  { value: 'email', label: '邮箱' },
  { value: 'sms', label: '短信' },
  { value: 'totp', label: '验证器 App(TOTP)' },
];

export function renderSecurity(root: HTMLElement): void {
  // ---- 安全状态 ----
  const statusBox = el('div', { class: 'stat-grid' });
  root.appendChild(card('安全状态', '敏感操作需要「口令 + 验证码」换取的短时票据', null, statusBox));

  // 最近一次拉到的账户安全状态(提示文案要用)。
  let secStatus: SecurityResponse | null = null;

  // ---- 修改口令 ----
  const passBtn = el('button', { class: 'btn btn-primary', text: '更新口令' }) as HTMLButtonElement;
  root.appendChild(card('登录口令', '口令以 PBKDF2 散列保存,不会明文入库或回显',
    null,
    el('div', { class: 'grid grid-2' },
      field({ id: 'old-pass', label: '原口令', type: 'password', optional: '首次设置可留空' }),
      field({ id: 'new-pass', label: '新口令', type: 'password', hint: '不少于 8 位' })),
    el('div', { class: 'row' }, passBtn)));

  // ---- 二次验证 ----
  //
  // 三种方式:邮箱 / 短信 / 验证器 App(TOTP)。
  //
  // 关键设计:
  //   - 「验证方式」**始终可见**。已开启时它是「更换接收方式或地址」——
  //     后端 enable 接口就是覆盖绑定,所以换方式不需要先关闭再开。
  //   - TOTP 不需要任何下发通道,验证码由用户的验证器 App 本地算出,
  //     所以 TOTP 分支里没有「发送验证码」按钮,也不显示接收地址。
  //   - 关闭流程与开启流程用**各自独立**的输入框(验证码 purpose 不同),
  //     混用会让用户把绑定码填进关闭流程,后端判 purpose 不匹配,
  //     回一句「验证码无效或已过期」,用户会以为「关闭没生效」。
  const mfaType = el('select', { id: 'mfa-type' }) as HTMLSelectElement;
  for (const t of MFA_TYPES) mfaType.appendChild(el('option', { value: t.value, text: t.label }));

  const targetInput = el('input', {
    type: 'text', id: 'mfa-target', placeholder: '邮箱地址或手机号',
  });
  const bindPassword = el('input', {
    type: 'password', id: 'mfa-bind-password', placeholder: '登录口令', autocomplete: 'current-password',
  });
  const bindCode = el('input', {
    type: 'text', id: 'mfa-bind-code', placeholder: '6 位验证码', autocomplete: 'one-time-code',
  });
  const disablePassword = el('input', {
    type: 'password', id: 'mfa-disable-password', placeholder: '登录口令', autocomplete: 'current-password',
  });
  const disableCode = el('input', {
    type: 'text', id: 'mfa-disable-code', placeholder: '6 位验证码', autocomplete: 'one-time-code',
  });

  const bindBtn = el('button', { class: 'btn btn-secondary', text: '发送绑定验证码' }) as HTMLButtonElement;
  const disableCodeBtn = el('button', { class: 'btn btn-secondary', text: '发送关闭验证码' }) as HTMLButtonElement;
  const enableBtn = el('button', { class: 'btn btn-primary', text: '开启二次验证' }) as HTMLButtonElement;
  const disableBtn = el('button', { class: 'btn btn-danger', text: '关闭二次验证' }) as HTMLButtonElement;

  // 「当前绑定确认」改成**弹窗**,不再在页面上常驻一栏输入框。
  //
  // 为什么:换绑是一个**动作**(重新生成密钥 / 保存新绑定 / 绑定备用方式),
  // 确认码只在这个动作发生的那一刻才需要。摆在页面上常驻着,用户根本
  // 分不清「这个码」和「App 里新扫出来的码」是哪个 —— 十个有九个会把
  // **新 App 的动态码**填进去,于是「保存」一直失败。
  //
  // 弹窗把确认放进动作发生的那一刻,上下文只有一件事:确认你是当前绑定的人。
  // 页面里只留下**一个**验证码输入框(新绑定那一个)。
  //
  // 为什么要确认:换绑会让旧绑定当场失效。只要口令就能换绑的话,拿到口令
  // 的人可以在别人的页面上点「重新生成密钥」,把密钥换成自己那份 ——
  // 正主的验证器立刻作废,人也被挡在门外,二次验证反倒替对方开了门。

  // TOTP:生成密钥 → 扫码 → 输入动态码。
  //
  // 注意「二维码不显示」这件事的成因:img 没有 src 时浏览器会画一个空白框,
  // 看起来就像「二维码加载不出来」。所以这里:
  //   - 图片在**加载成功之前不显示**(不给人看空白框);
  //   - 选到「验证器 App」就**自动生成密钥**,而不是让人先找到按钮;
  //   - 图片失败时给出**可操作的替代方案**(手动输入密钥),并把原因说清楚。
  const totpGenBtn = el('button', { class: 'btn btn-secondary', text: '重新生成密钥' }) as HTMLButtonElement;
  const totpBox = el('div', { class: 'notice notice-info' });
  const totpSecret = el('code', { id: 'mfa-totp-secret', text: '' });
  const totpCopyBtn = el('button', { class: 'btn btn-ghost btn-sm', text: '复制密钥' }) as HTMLButtonElement;
  const totpQR = el('img', {
    alt: 'TOTP 二维码',
    style: 'width:180px;height:180px;background:#fff;padding:6px;border-radius:8px',
  });
  const totpHint = el('div', { class: 'hint-text' });
  totpQR.style.display = 'none';
  totpCopyBtn.style.display = 'none';
  totpBox.appendChild(el('div', { style: 'margin-bottom:6px', text: '用验证器 App 扫码,或手动输入下面的密钥' }));
  totpBox.appendChild(el('div', { class: 'row', style: 'margin-bottom:8px' }, totpQR, totpHint));
  totpBox.appendChild(el('div', { class: 'row' }, totpSecret, totpCopyBtn, el('span', { class: 'spacer' }), totpGenBtn));
  totpBox.style.display = 'none';

  // 当前页面里的 TOTP 密钥(生成过就不再自动重新生成,免得把用户刚扫的码作废)。
  let totpSecretValue = '';

  // 邮箱/短信分支的表单
  const targetField = el('div', { class: 'field' },
    el('label', { class: 'label', text: '接收地址', for: 'mfa-target' }),
    targetInput,
    el('div', { class: 'hint-text', text: '验证码会发到这个地址' }));

  const bindCodeField = el('div', { class: 'field' },
    el('label', { class: 'label', text: '绑定验证码', for: 'mfa-bind-code' }),
    bindCode,
    el('div', { class: 'hint-text', text: '点「发送绑定验证码」后填这里' }));

  // TOTP 分支的表单
  const totpCodeField = el('div', { class: 'field' },
    el('label', { class: 'label', text: '动态码', for: 'mfa-totp-code' }),
    el('input', { type: 'text', id: 'mfa-totp-code', placeholder: 'App 里的 6 位动态码', autocomplete: 'one-time-code' }),
    el('div', { class: 'hint-text', text: '验证器 App 每 30 秒刷新一次' }));

  const bindTitle = el('div', { class: 'section-title', text: '开启二次验证' });
  // 提示区:说明当前状态,以及选了邮箱/短信却没配下发通道的后果。
  const bindHint = el('div', { class: 'notice notice-info', style: 'margin-bottom:14px' });
  const bindSection = el('div', {},
    bindTitle,
    bindHint,
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '验证方式', for: 'mfa-type' }),
        mfaType),
      targetField),
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '登录口令', for: 'mfa-bind-password' }),
        bindPassword),
      bindCodeField),
    totpBox,
    totpCodeField,
    el('div', { class: 'row' }, bindBtn, enableBtn));

  const currentBox = el('div', { class: 'notice notice-info', style: 'margin-bottom:14px' });
  const disableCodeLabel = el('label', { class: 'label', text: '关闭验证码', for: 'mfa-disable-code' });
  const disableCodeField = el('div', { class: 'field' },
    disableCodeLabel,
    disableCode,
    el('div', { class: 'hint-text', text: '点「发送关闭验证码」后,验证码会发到已绑定的地址' }));
  const disableSection = el('div', {},
    el('div', { class: 'section-title', text: '关闭二次验证' }),
    currentBox,
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '登录口令', for: 'mfa-disable-password' }),
        disablePassword),
      disableCodeField),
    el('div', { class: 'row' }, disableCodeBtn, disableBtn));

  root.appendChild(card('二次验证(邮箱 / 短信 / 验证器 App)',
    '开启后,登录与敏感操作都需要额外输入验证码',
    null, bindSection, disableSection));

  // ---- 备用验证方式 ----
  //
  // 「验证器 App 丢了 / 手机被偷 / App 被误删」时的第二条腿:
  // 提前绑一个邮箱或手机号,登录页就能改用它收验证码,
  // 账户不至于因为丢了 App 就永久登不进来。
  //
  // 绑定它需要「当前绑定的人」确认(和换绑一样):
  // 否则知道口令的人能给自己绑一个备用地址,直接绕过验证器登录。
  const backupType = el('select', { id: 'mfa-backup-type' }) as HTMLSelectElement;
  backupType.appendChild(el('option', { value: 'email', text: '邮箱' }));
  backupType.appendChild(el('option', { value: 'sms', text: '短信' }));
  const backupTarget = el('input', {
    type: 'text', id: 'mfa-backup-target', placeholder: '邮箱地址或手机号',
  });
  const backupPassword = el('input', {
    type: 'password', id: 'mfa-backup-password', placeholder: '登录口令', autocomplete: 'current-password',
  });
  const backupCode = el('input', {
    type: 'text', id: 'mfa-backup-code', placeholder: '6 位验证码', autocomplete: 'one-time-code',
  });
  const backupStatus = el('div', { class: 'hint-text', style: 'margin-bottom:12px' });
  const backupSendBtn = el('button', { class: 'btn btn-secondary', text: '发送备用地址验证码' }) as HTMLButtonElement;
  const backupSaveBtn = el('button', { class: 'btn btn-primary', text: '保存备用验证方式' }) as HTMLButtonElement;
  const backupDelBtn = el('button', { class: 'btn btn-danger', text: '删除备用验证方式' }) as HTMLButtonElement;

  const backupSection = el('div', {},
    backupStatus,
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '备用验证方式', for: 'mfa-backup-type' }),
        backupType),
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '备用接收地址', for: 'mfa-backup-target' }),
        backupTarget)),
    el('div', { class: 'grid grid-2' },
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '登录口令', for: 'mfa-backup-password' }),
        backupPassword),
      el('div', { class: 'field' },
        el('label', { class: 'label', text: '备用地址验证码', for: 'mfa-backup-code' }),
        backupCode)),
    el('div', { class: 'row' }, backupSendBtn, backupSaveBtn, backupDelBtn),
    el('div', { class: 'hint-text', style: 'margin-top:10px',
      text: '绑定备用方式需要同时提供:登录口令、点「保存」时弹窗里的「当前绑定确认」(证明还是你在操作)、'
        + '以及发到备用地址的验证码 —— 否则知道口令的人就能给自己绑一个备用地址登录。' }));

  root.appendChild(card('备用验证方式(验证器 App 丢了时的退路)',
    '提前绑一个邮箱或手机号:App 丢了、被误删、手机被偷时,还能用它登录',
    null, backupSection));

  // ---- 验证码下发通道 ----
  root.appendChild(buildChannelCard());

  // ---- 审计日志 ----
  const logAction = el('select', { id: 'log-action' }) as HTMLSelectElement;
  for (const [value, label] of [
    ['', '全部动作'],
    ['account.login', '登录'],
    ['account.mfa', '二次验证'],
    ['account.password', '修改口令'],
    ['channel.save', '保存渠道配置'],
    ['channel.test', '渠道连通性测试'],
    ['channel.reveal', '查看密钥明文(已下线)'],
    ['order.pay', '发起支付'],
    ['order.refund', '退款'],
    ['order.close', '关闭订单'],
    ['order.charge', '钱包扣款'],
    ['order.export', '导出订单'],
    ['apikey.create', '创建密钥'],
    ['apikey.revoke', '吊销密钥'],
  ]) {
    logAction.appendChild(el('option', { value, text: label }));
  }
  const logRefresh = el('button', { class: 'btn btn-secondary', text: '刷新' }) as HTMLButtonElement;
  const logBody = el('div');
  const logPager = el('div', { class: 'pager' });

  root.appendChild(card('审计日志', '记录登录、配置变更、支付与退款等操作及来源 IP',
    el('div', { class: 'row' }, logRefresh),
    el('div', { class: 'field', style: 'max-width:220px' },
      el('label', { class: 'label', text: '动作', for: 'log-action' }),
      logAction),
    logBody,
    logPager));

  let logPage = 1;
  const logSize = 20;

  // ---------------------------------------------------------------- 事件

  passBtn.addEventListener('click', () =>
    void withBusy(passBtn, async () => {
      try {
        await api('/api/account/password', {
          method: 'POST',
          body: { old_password: elValue('old-pass'), new_password: elValue('new-pass') },
        });
        toast('口令已更新', 'ok');
        setVal('old-pass', '');
        setVal('new-pass', '');
        await loadStatus();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  /**
   * 绑定区的提示文案。
   *
   * 核心是**别让人以为开了就一定能收到码**:
   * 邮箱 / 短信方式依赖下单户自己配的下发通道,没配的话验证码只会写进日志。
   * 验证器 App(TOTP)则完全不需要任何通道 —— 手机上装个 App 就能用。
   */
  function updateBindHint(): void {
    const type = sel('mfa-type');
    const enabled = !!secStatus?.mfa_enabled;
    const lines: string[] = [];
    let kind = 'notice notice-info';

    if (type === 'off' || type === '') {
      if (enabled) {
        lines.push('当前已开启:' + mfaBindingText(secStatus!.mfa_type, secStatus!.mfa_target) + '。');
        if (secStatus!.mfa_deliverable === false) {
          lines.push('但这个绑定是无效的:验证码发不出去(没有可用下发通道),拦不住任何人,'
            + '平台登录时会跳过它。建议用下方「关闭二次验证」关掉它(通道不可用,只需口令),或改用验证器 App。');
        } else {
          lines.push('要关闭请用下方「关闭二次验证」;要换方式,直接在上面选新的方式再「保存新绑定」。');
        }
      } else {
        lines.push('当前未开启二次验证(平台默认关闭,不强制)。');
        lines.push('需要时在上面选一种方式,再点「开启二次验证」:');
        lines.push('· 验证器 App —— 手机装个验证器 App 扫码即可,不需要任何通道;');
        lines.push('· 邮箱 / 短信 —— 需要先在下方「验证码下发通道」里配好 SMTP 或短信网关,否则码只会写进日志、实际收不到。');
      }
    } else if (type === 'totp') {
      lines.push('验证器 App 方式不需要任何下发通道:密钥由平台生成并加密保存,动态码在你的手机上算出。');
      lines.push('点「生成密钥」→ 用验证器 App 扫码 → 输入 App 里的 6 位动态码 → 点「开启二次验证」。');
      if (enabled) {
        lines.push('换绑(重生成密钥)会让旧绑定作废,所以点「重新生成密钥」时会弹窗问一次:'
          + '输入旧验证器 App 里的动态码,证明还是原来那个人在操作。');
      }
    } else {
      // email / sms
      const channelOk = type === 'email'
        ? !!secStatus?.email_channel_configured
        : !!secStatus?.sms_channel_configured;
      if (!channelOk) {
        kind = 'notice notice-warn';
        lines.push('尚未配置' + (type === 'email' ? '邮件' : '短信') +
          '下发通道 —— 验证码只会写进服务器日志,用户实际收不到。');
        lines.push('请先到下方「验证码下发通道」里配置(配完可以用「发送测试邮件」实测);');
        lines.push('或者改用「验证器 App」,它不需要任何通道。');
      } else {
        lines.push('填好接收地址 → 点「发送绑定验证码」→ 把收到的 6 位码填进去 → 点「' +
          (enabled ? '保存新绑定' : '开启二次验证') + '」。');
        if (enabled) {
          lines.push('换绑时会弹窗问一次「当前绑定确认」:发到「已绑定地址」的验证码,证明还是原来那个人在操作。');
        }
      }
    }
    bindHint.className = kind;
    bindHint.replaceChildren(...lines.map((line) => el('div', { text: line })));
  }

  // 切换验证方式:
  //   - off:默认项。不显示任何绑定表单,「开启」按钮禁用 —— 不选方式就没法开启。
  //   - email/sms:要填接收地址并发验证码。
  //   - totp:不用地址、不用下发通道,动态码由验证器 App 本地算出。
  const syncMFAForm = (): void => {
    const type = sel('mfa-type');
    const isTOTP = type === 'totp';
    const isOff = type === 'off' || type === '';
    const isCode = type === 'email' || type === 'sms';

    targetField.style.display = isCode ? '' : 'none';
    bindCodeField.style.display = isCode ? '' : 'none';
    bindBtn.style.display = isCode ? '' : 'none';
    totpBox.style.display = isTOTP ? '' : 'none';
    totpCodeField.style.display = isTOTP ? '' : 'none';
    bindPassword.parentElement!.style.display = isOff ? 'none' : '';
    enableBtn.disabled = isOff;
    enableBtn.textContent = isOff
      ? '请先选择验证方式'
      : (secStatus && secStatus.mfa_enabled ? '保存新绑定' : '开启二次验证');
    // 选到「验证器 App」就把二维码生成出来 —— 让人自己再点一次
    // 「生成密钥」,页面在等待期间只有一个空白方框,看着就像二维码坏了。
    //
    // 但**已经绑定过**二次验证的账户例外:换绑必须先通过「当前绑定确认」,
    // 不能让任何人点一下就把新密钥拿到手(那等于把验证器换给别人,
    // 正主当场进不来)。这种情况让用户自己填完确认码再点「重新生成密钥」。
    if (isTOTP && !totpSecretValue && !(secStatus && secStatus.mfa_enabled)) {
      void loadTOTPSetup();
    }
    // 换绑时密钥不会自动生成:点「重新生成密钥」时会弹窗确认当前绑定,
    // 确认过了才把新密钥发出去。
    if (isTOTP && secStatus && secStatus.mfa_enabled && !totpSecretValue) {
      totpHint.textContent = '换绑不会自动生成密钥:点「重新生成密钥」,'
        + '按弹窗提示输入旧验证器 App 的动态码。';
    }
    updateBindHint();
  };
  mfaType.addEventListener('change', syncMFAForm);

  bindBtn.addEventListener('click', () =>
    void withBusy(bindBtn, async () => {
      try {
        const type = sel('mfa-type');
        if (type !== 'email' && type !== 'sms') {
          toast('请先选择邮箱或短信', 'err');
          return;
        }
        const res = await api<{ target?: string; delivered?: boolean; message?: string }>(
          '/api/account/mfa/code', {
            method: 'POST',
            body: {
              type,
              target: elValue('mfa-target'),
              purpose: 'bind',
              // 绑定新地址要先验口令。
              password: elValue('mfa-bind-password'),
            },
          });
        toast(res.message || ('验证码已发送到 ' + (res.target || '目标地址')), res.delivered === false ? 'err' : 'ok');
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  /**
   * 弹窗向**当前绑定的人**要一个确认码。
   *
   * 返回确认码;用户取消返回 null。
   *
   * 确认方式随**当前**绑定方式而变:
   *   - 当前是验证器 App:输入**旧 App** 里的 6 位动态码(不用下发);
   *   - 当前是邮箱/短信:点「发送到已绑定地址」把码发过去,填进来即可。
   *
   * 目标由后端强制为**已绑定的地址**(请求里指定别的地址会被拒),
   * 免得「把验证码发到攻击者邮箱」这种绕法。
   */
  const askCurrentBinding = (): Promise<string | null> => {
    return new Promise((resolve) => {
      const isTOTP = secStatus?.mfa_type === 'totp';
      const input = el('input', {
        type: 'text', id: 'mfa-current-code',
        placeholder: isTOTP ? '旧 App 里的 6 位动态码' : '6 位验证码',
        autocomplete: 'one-time-code',
      });
      const sendBtn = el('button', {
        class: 'btn btn-secondary btn-sm', text: '发送到已绑定地址',
      }) as HTMLButtonElement;
      sendBtn.style.display = isTOTP ? 'none' : '';
      const body = el('div', { class: 'field' },
        el('label', { class: 'label', text: '当前绑定确认' }),
        input,
        el('div', { class: 'row', style: 'margin-top:6px' }, sendBtn),
        el('div', { class: 'hint-text', text: isTOTP
          ? '请输入**当前**验证器 App 里的动态码 —— 还没扫码生成新密钥的话,填 App 里现有的那条。'
          : '验证码会发到已绑定的地址 ' + (secStatus?.mfa_target || '') + ',填收到的 6 位码。' }));

      let done = false;
      const handle = openModal({
        title: '确认你是当前绑定的人',
        desc: '换绑会让旧的二次验证立即失效,所以要先证明现在操作的人就是原来绑定的那个人。',
        body,
        okText: '确认',
        onOk: (h) => {
          const v = input.value.trim();
          if (!v) {
            h.error('请输入验证码');
            return;
          }
          done = true;
          h.close(true);
          resolve(v);
        },
      });
      // 取消(按钮 / 遮罩 / Esc)一律当作「不继续」。
      const rawClose = handle.close;
      handle.close = (ok: boolean): void => {
        rawClose(ok);
        if (!done) {
          done = true;
          resolve(null);
        }
      };
      sendBtn.addEventListener('click', () =>
        void withBusy(sendBtn, async () => {
          try {
            const res = await api<{ target?: string; delivered?: boolean; message?: string }>(
              '/api/account/mfa/code', {
                method: 'POST',
                body: { purpose: 'rebind' },
              });
            toast(res.message || ('验证码已发送到 ' + (res.target || '已绑定的接收地址')),
              res.delivered === false ? 'err' : 'ok');
          } catch (e) {
            toast(errText(e), 'err');
          }
        }));
    });
  };

  disableCodeBtn.addEventListener('click', () =>
    void withBusy(disableCodeBtn, async () => {
      try {
        const res = await api<{ target?: string; delivered?: boolean; message?: string }>(
          '/api/account/mfa/code', {
            method: 'POST',
            body: { purpose: 'disable' },
          });
        toast(res.message || ('验证码已发送到 ' + (res.target || '已绑定的接收地址')),
          res.delivered === false ? 'err' : 'ok');
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  /**
   * 生成(或重新生成)TOTP 密钥,并把二维码画出来。
   *
   * 关键点:
   *   - **选到「验证器 App」就自动调它**,页面上一进来二维码就该在,
   *     而不是让人先找到「生成密钥」按钮 —— 否则只有一个空白方框,
   *     看起来就是「二维码不显示」;
   *   - 密钥**先落地**再画图:图片失败也有密钥可手动输入;
   *   - 重新生成会让之前扫的码失效,提示里要说清楚。
   */
  const loadTOTPSetup = async (currentCode?: string): Promise<void> => {
    totpHint.textContent = '正在生成密钥…';
    totpQR.style.display = 'none';
    // 旧二维码不再显示了,顺手释放它的 object URL(换绑一次漏一张,内存就涨一截)。
    releaseBlobURL(totpQRURL);
    totpQRURL = null;
    try {
      // 已经绑定过的账户:换绑要带上「当前绑定确认」(旧验证器 App 的动态码),
      // 否则后端直接拒绝 —— 新密钥不能发给不是当前绑定的人。
      const body = (secStatus && secStatus.mfa_enabled)
        ? { current_code: currentCode || '' }
        : {};
      const res = await api<{ secret: string; qrcode_url: string }>('/api/account/mfa/totp/setup', {
        method: 'POST',
        body,
      });
      totpSecretValue = res.secret || '';
      totpSecret.textContent = totpSecretValue;
      totpCopyBtn.style.display = '';
      if (!totpSecretValue) {
        totpHint.textContent = '后端没有返回密钥,请点「重新生成密钥」重试';
        return;
      }
      try {
        const url = await apiBlobURL(res.qrcode_url);
        totpQR.src = url;
        totpQRURL = url;
        totpQR.style.display = '';
        const rebound = !!(secStatus && secStatus.mfa_enabled && secStatus.mfa_type === 'totp');
        totpHint.textContent = (rebound
          ? '当前已绑定验证器 App。下面二维码是**新密钥**:扫码后把 App 里的 6 位动态码'
            + '填到下面那一个输入框,再点「保存新绑定」才生效(旧密钥同时失效)。'
          : '扫完后验证器 App 里会出现一条「支付配置选择平台」,')
          + '把它的 6 位动态码填到下面的输入框(也可以手动输入上面的密钥)。';
      } catch (e) {
        // 图片挂了不要紧:密钥已经拿到,手动输入一样能用。
        totpHint.textContent = '二维码加载失败(' + errText(e) + ')。'
          + '请手动把下面的密钥填进验证器 App —— 效果完全一样。';
      }
    } catch (e) {
      totpHint.textContent = '生成密钥失败:' + errText(e)
        + (secStatus && secStatus.mfa_enabled ? '(换绑要通过弹窗里的「当前绑定确认」)' : '');
    }
  };

  totpGenBtn.addEventListener('click', () =>
    void withBusy(totpGenBtn, async () => {
      // 换绑(已经有绑定)时,先在弹窗里证明「我是当前绑定的那个人」。
      // 确认放在**这一刻**问,因为只有确认过了才能拿到新密钥;
      // 等扫完码再问就晚了 —— 旧条目可能已经被新条目覆盖,旧码取不到了。
      let currentCode = '';
      if (secStatus && secStatus.mfa_enabled) {
        const asked = await askCurrentBinding();
        if (asked === null) return;
        currentCode = asked;
      }
      const had = !!totpSecretValue;
      await loadTOTPSetup(currentCode);
      if (totpSecretValue) {
        toast(had
          ? '已生成新密钥,之前扫的码会失效,请重新扫码'
          : '密钥已生成,请用验证器 App 扫码后输入动态码', 'ok');
      }
    }));

  totpCopyBtn.addEventListener('click', () => void copyText(totpSecretValue));

  enableBtn.addEventListener('click', () =>
    void withBusy(enableBtn, async () => {
      try {
        const type = sel('mfa-type');
        if (type !== 'email' && type !== 'sms' && type !== 'totp') {
          toast('请先选择一种验证方式', 'err');
          return;
        }
        const isTOTP = type === 'totp';
        const target = isTOTP ? '' : elValue('mfa-target');
        // 换绑确认:
        //   - 验证器 App:已经在「重新生成密钥」那一步弹窗确认过了,
        //     这里不用再问(而且也问不出来 —— 扫完码旧条目可能已被覆盖);
        //   - 邮箱/短信:换地址时在这里弹窗要「发到已绑定地址」的验证码。
        let currentCode = '';
        if (!isTOTP && secStatus && secStatus.mfa_enabled
          && (secStatus.mfa_type !== type || secStatus.mfa_target !== target)) {
          const asked = await askCurrentBinding();
          if (asked === null) return;
          currentCode = asked;
        }
        await api('/api/account/mfa/enable', {
          method: 'POST',
          body: {
            type,
            target,
            password: elValue('mfa-bind-password'),
            code: isTOTP ? elValue('mfa-totp-code') : elValue('mfa-bind-code'),
            current_code: currentCode,
          },
        });
        toast('二次验证已开启', 'ok');
        setVal('mfa-bind-code', '');
        setVal('mfa-totp-code', '');
        if (isTOTP) {
          // 已生效的密钥不再留在页面上。
          //
          // 服务端在绑定成功后会把「待确认密钥」清掉,这时页面还留着旧的
          // 密钥和二维码,用户会以为还能再扫一次 —— 而再点「保存新绑定」
          // 只会换来一句「请先点击「生成密钥」并用验证器 App 扫码」。
          // 清掉之后页面回到「已绑定」状态:要换绑就点「重新生成密钥」。
          totpSecretValue = '';
          totpSecret.textContent = '';
          totpCopyBtn.style.display = 'none';
          totpQR.removeAttribute('src');
          totpQR.style.display = 'none';
          releaseBlobURL(totpQRURL);
          totpQRURL = null;
        }
        await loadStatus();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  disableBtn.addEventListener('click', () =>
    void withBusy(disableBtn, async () => {
      try {
        await api('/api/account/mfa/disable', {
          method: 'POST',
          body: {
            password: elValue('mfa-disable-password'),
            code: elValue('mfa-disable-code'),
          },
        });
        toast('二次验证已关闭,以后登录只需要口令', 'ok');
        setVal('mfa-disable-code', '');
        await loadStatus();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  backupSendBtn.addEventListener('click', () =>
    void withBusy(backupSendBtn, async () => {
      try {
        const res = await api<{ target?: string; delivered?: boolean; message?: string }>(
          '/api/account/mfa/code', {
            method: 'POST',
            body: {
              type: sel('mfa-backup-type'),
              target: elValue('mfa-backup-target'),
              purpose: 'bind',
              // 绑定新地址要先验口令。
              password: elValue('mfa-backup-password'),
            },
          });
        toast(res.message || ('验证码已发送到 ' + (res.target || '目标地址')),
          res.delivered === false ? 'err' : 'ok');
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  backupSaveBtn.addEventListener('click', () =>
    void withBusy(backupSaveBtn, async () => {
      try {
        // 绑定备用方式也必须证明「还是当前绑定的人」在操作,
        // 否则知道口令的人能给自己绑一个备用地址直接登录。
        // 确认放在弹窗里,点「保存备用验证方式」的这一刻问。
        let currentCode = '';
        if (secStatus && secStatus.mfa_enabled) {
          const asked = await askCurrentBinding();
          if (asked === null) return;
          currentCode = asked;
        }
        await api('/api/account/mfa/backup', {
          method: 'POST',
          body: {
            type: sel('mfa-backup-type'),
            target: elValue('mfa-backup-target'),
            password: elValue('mfa-backup-password'),
            code: elValue('mfa-backup-code'),
            current_code: currentCode,
          },
        });
        toast('备用验证方式已保存', 'ok');
        setVal('mfa-backup-code', '');
        await loadStatus();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  backupDelBtn.addEventListener('click', () =>
    void withBusy(backupDelBtn, async () => {
      try {
        // 删掉备用方式 = 把账户的退路也删了:要票据。
        await requireVerify();
        await api('/api/account/mfa/backup', {
          method: 'DELETE',
          body: { password: elValue('mfa-backup-password') },
        });
        toast('备用验证方式已删除', 'ok');
        await loadStatus();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  logRefresh.addEventListener('click', () => {
    logPage = 1;
    void loadLogs();
  });
  logAction.addEventListener('change', () => {
    logPage = 1;
    void loadLogs();
  });

  // ---------------------------------------------------------------- 加载

  async function loadStatus(): Promise<void> {
    try {
      const sec = await api<SecurityResponse>('/api/account/security');
      statusBox.replaceChildren(
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '商户' }),
          el('div', { class: 'stat-value sm', text: sec.name })),
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '商户 ID' }),
          el('div', { class: 'stat-value sm mono', text: sec.user_id })),
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '登录口令' }),
          el('div', { class: 'stat-value sm' }, sec.password_set ? badge('已设置', 'success') : badge('未设置', 'warning'))),
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '二次验证' }),
          el('div', { class: 'stat-value sm' },
            sec.mfa_enabled
              ? (sec.mfa_deliverable === false
                ? badge('已开启但无效', 'danger')
                : badge('已开启', 'success'))
              : badge('未开启', 'muted'),
            sec.mfa_enabled
              ? el('div', { class: 'stat-hint', text: mfaBindingText(sec.mfa_type, sec.mfa_target) })
              : el('div', { class: 'stat-hint', text: '默认关闭,需要时再开' }),
            sec.mfa_enabled && sec.mfa_deliverable === false
              ? el('div', { class: 'stat-hint', text: '验证码发不出去,登录时会被跳过' })
              : null)),
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '邮件通道' }),
          el('div', { class: 'stat-value sm' },
            sec.email_channel_configured ? badge('已配置', 'success') : badge('未配置', 'warning'),
            sec.email_channel_configured ? null
              : el('div', { class: 'stat-hint', text: '不配的话邮件验证码只写日志' }))),
        el('div', { class: 'stat' },
          el('div', { class: 'stat-label', text: '票据有效期' }),
          el('div', { class: 'stat-value sm', text: String(sec.verify_ttl_sec) }),
          el('div', { class: 'stat-hint', text: '秒' })),
      );
      secStatus = sec;
      // 默认项是「关闭」:没开二次验证的账户不要被默认按到邮箱/短信上 ——
      // 并不是每个商户都开通了邮箱或短信,默认替他们选一种只会让人白忙一场。
      mfaType.value = sec.mfa_enabled ? sec.mfa_type : 'off';
      syncMFAForm();

      // 「开启」区始终可见(已开启时它是「更换接收方式或地址」),
      // 「关闭」区只在已开启时出现 —— 没开的时候给「关闭」按钮纯属摆设。
      disableSection.style.display = sec.mfa_enabled ? '' : 'none';
      bindTitle.textContent = sec.mfa_enabled ? '更换接收方式或地址' : '开启二次验证';

      // 备用验证方式:绑了才显示「删除」按钮,并说清楚它是干什么的。
      const hasBackup = !!sec.mfa_backup;
      const backupOK = sec.mfa_backup_deliverable !== false;
      backupStatus.textContent = hasBackup
        ? '当前备用验证方式:' + mfaBindingText(sec.mfa_backup_type || 'email', sec.mfa_backup_target || '')
          + ' —— 登录时可以用它收验证码。'
          + (backupOK
            ? ''
            : ' **但这条通道发不出去**(没配 SMTP/短信网关,或平台以 -log-mfa 启动):'
              + '验证码只写进服务器日志,App 丢了照样登不进来。'
              + '请到下方「验证码下发通道」配好并用「发送测试邮件」实测。')
        : '还没绑备用验证方式。建议绑一个:验证器 App 丢了、被误删或手机被偷时,'
          + '没有它这个账户就再也登不进来了。';
      backupDelBtn.style.display = hasBackup ? '' : 'none';
      backupSaveBtn.textContent = hasBackup ? '更换备用验证方式' : '保存备用验证方式';

      if (sec.mfa_enabled) {
        const isTOTP = sec.mfa_type === 'totp';
        const deliverable = sec.mfa_deliverable !== false;
        currentBox.replaceChildren(
          el('div', { text: '当前绑定:' + mfaBindingText(sec.mfa_type, sec.mfa_target) }),
          el('div', { class: 'hint-text', style: 'margin-top:4px',
            text: !deliverable
              ? '这个绑定是无效的:验证码发不出去(没有可用下发通道),拦不住任何人,登录时会被平台跳过。建议关掉它,或改用验证器 App。'
              : isTOTP
                ? '更换验证方式:选好上面的「验证方式」再点「保存新绑定」;验证器 App 方式需要重新生成密钥并扫码。'
                : '更换接收方式或地址:在上面选好新的方式与地址,点「发送绑定验证码」再「保存新绑定」即可。' }),
        );
        // 关闭流程:
        //   - TOTP:用当前动态码,不需要下发验证码;
        //   - 通道不可用:那个码用户根本收不到,再要它就是死锁 —— 只留口令。
        if (isTOTP) {
          disableCodeLabel.textContent = '当前动态码';
          disableCode.placeholder = 'App 里的 6 位动态码';
          disableCodeBtn.style.display = 'none';
        } else if (!deliverable) {
          disableCodeLabel.textContent = '关闭验证码';
          disableCode.placeholder = '通道不可用,关闭时只需口令';
          disableCodeBtn.style.display = 'none';
          disableCode.disabled = true;
        } else {
          disableCodeLabel.textContent = '关闭验证码';
          disableCode.placeholder = '6 位验证码';
          disableCodeBtn.style.display = '';
          disableCode.disabled = false;
        }
      } else {
        disableCodeBtn.style.display = 'none';
        disableCode.disabled = false;
      }
    } catch (e) {
      statusBox.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  async function loadLogs(): Promise<void> {
    logBody.replaceChildren(el('p', { class: 'muted', text: '加载中…' }));
    try {
      const action = sel('log-action');
      const res = await api<{ logs: AuditLog[] } & PageResponse>(
        '/api/audit-logs?page=' + logPage + '&size=' + logSize
        + (action ? '&action=' + encodeURIComponent(action) : ''),
      );
      const logs = res.logs || [];
      logBody.replaceChildren();
      if (!logs.length) {
        logBody.appendChild(emptyState('暂无审计记录', '发生登录、配置变更、支付等操作后会记录在这里。'));
        return;
      }
      const rows = logs.map((l) => [
        l.created_at ? new Date(l.created_at).toLocaleString() : '—',
        badge(ACTION_LABELS[l.action] || l.action, 'info'),
        el('span', { class: 'cell-mono', text: l.target || '—' }),
        l.detail || '—',
        el('span', { class: 'cell-mono', text: l.client_ip || '—' }),
      ]);
      logBody.appendChild(table(['时间', '动作', '对象', '详情', '来源 IP'], rows));
      renderLogPager(logPager, res, (p) => {
        logPage = p;
        void loadLogs();
      });
    } catch (e) {
      logBody.replaceChildren(el('p', { class: 'error-text', text: errText(e) }));
    }
  }

  void loadStatus();
  void loadLogs();
}

// ---------------------------------------------------------------- 通道配置

/**
 * 验证码下发通道卡片。
 *
 * 没有这一步,「邮箱验证码」就只是个摆设:平台默认只把码写进日志,
 * 用户点了「发送」看到一句「已发送」,邮箱里什么都没有。
 *
 * 这一张卡的目标是「**填完就能用**」:
 *   - 把要填什么、常见邮箱厂商的参数直接写在页面上,不用去查文档;
 *   - 邮件通道支持 STARTTLS(587)与隐式 TLS(465)两种加密方式;
 *   - 短信网关的字段名可配(不同厂商叫法不一样);
 *   - 有「发送测试」按钮 —— 保存成功不等于真能发出去,必须实测。
 */
function buildChannelCard(): HTMLElement {
  const emailSave = el('button', { class: 'btn btn-primary', text: '保存邮件通道' }) as HTMLButtonElement;
  const emailTest = el('button', { class: 'btn btn-secondary', text: '发送测试邮件' }) as HTMLButtonElement;
  const emailDel = el('button', { class: 'btn btn-ghost', text: '删除' }) as HTMLButtonElement;
  const smsSave = el('button', { class: 'btn btn-primary', text: '保存短信通道' }) as HTMLButtonElement;
  const smsTest = el('button', { class: 'btn btn-secondary', text: '发送测试短信' }) as HTMLButtonElement;
  const smsDel = el('button', { class: 'btn btn-ghost', text: '删除' }) as HTMLButtonElement;
  const qqPreset = el('button', { class: 'btn btn-ghost btn-sm', text: '填入 QQ 邮箱参数' }) as HTMLButtonElement;
  const mail163Preset = el('button', { class: 'btn btn-ghost btn-sm', text: '填入 163 邮箱参数' }) as HTMLButtonElement;

  const emailStatus = el('div', { class: 'hint-text' });
  const smsStatus = el('div', { class: 'hint-text' });

  const guide = el('div', { class: 'notice notice-info', style: 'margin-bottom:16px' },
    el('div', { text: '配好通道 → 点「发送测试」确认真的收到 → 再去上面开启二次验证。' }),
    el('div', { class: 'hint-text', style: 'margin-top:6px',
      text: '邮件:在邮箱服务商后台开启 SMTP 服务并生成「授权码」(不是登录密码),填服务器/端口/账号/授权码即可。' }),
    el('div', { class: 'hint-text',
      text: '短信:填你自己的短信网关地址,平台会向它 POST {"code":"123456","target":"13800138000"};字段名可按厂商改。' }),
    el('div', { class: 'hint-text',
      text: '不想配通道也可以:用「验证器 App」方式开启二次验证,它不需要任何通道。' }));

  const cardEl = card('验证码下发通道', '平台不内置任何邮件/短信厂商;填自己的 SMTP 或网关地址,授权码加密入库',
    null,
    guide,
    el('div', { class: 'section-title', text: '邮件通道(SMTP)' }),
    emailStatus,
    el('div', { class: 'grid grid-3' },
      field({ id: 'ch-mail-host', label: 'SMTP 服务器', placeholder: 'smtp.qq.com' }),
      field({ id: 'ch-mail-port', label: '端口', type: 'number', value: '587' }),
      field({ id: 'ch-mail-user', label: '登录账号', placeholder: 'number@qq.com' })),
    el('div', { class: 'grid grid-2' },
      field({ id: 'ch-mail-pass', label: '授权码 / 密码', type: 'password', secret: true,
        hint: 'QQ / 163 邮箱填「授权码」,不是登录密码;加密入库,永不回显' }),
      field({ id: 'ch-mail-from', label: '发件人地址', optional: '留空则用登录账号' })),
    el('div', { class: 'grid grid-2' },
      select('ch-mail-tls', [
        { value: 'starttls', label: 'STARTTLS(先握手再加密,587 端口)' },
        { value: 'tls', label: '隐式 TLS(连接即加密,465 端口)' },
        { value: 'none', label: '不加密(仅限内网自建服务器)' },
      ], 'starttls', '加密方式'),
      field({ id: 'ch-mail-test-to', label: '测试收件地址', optional: '留空则发到登录账号' })),
    el('div', { class: 'row' }, qqPreset, mail163Preset),
    el('div', { class: 'row' }, emailSave, emailTest, emailDel),
    el('div', { class: 'section-title', style: 'margin-top:18px', text: '短信通道(HTTP 网关)' }),
    smsStatus,
    el('div', { class: 'grid grid-2' },
      field({ id: 'ch-sms-url', label: '网关地址', placeholder: 'https://sms.example.com/send' }),
      field({ id: 'ch-sms-token', label: '网关令牌', type: 'password', secret: true, optional: '没有可留空' })),
    el('div', { class: 'grid grid-3' },
      field({ id: 'ch-sms-code-field', label: '验证码字段名', value: 'code', optional: '默认 code' }),
      field({ id: 'ch-sms-target-field', label: '接收地址字段名', value: 'target', optional: '默认 target' }),
      field({ id: 'ch-sms-test-to', label: '测试手机号', optional: '留空则发到已绑定手机号' })),
    el('div', { class: 'row' }, smsSave, smsTest, smsDel));

  void loadChannels();

  async function loadChannels(): Promise<void> {
    try {
      const res = await api<{ channels: Record<string, MFAChannel> }>('/api/account/mfa/channels');
      const chans = res.channels || {};
      const mail = chans.email || {};
      const sms = chans.sms || {};
      setVal('ch-mail-host', str(mail.host));
      setVal('ch-mail-port', str(mail.port) || '587');
      setVal('ch-mail-user', str(mail.username));
      setVal('ch-mail-from', str(mail.from_addr));
      setVal('ch-mail-tls', str(mail.tls_mode) || 'starttls');
      setVal('ch-sms-url', str(sms.url));
      setVal('ch-sms-code-field', str(sms.code_field) || 'code');
      setVal('ch-sms-target-field', str(sms.target_field) || 'target');
      // 授权码 / 令牌永远不回显:已保存时把输入框清空,占位符说明「留空表示不修改」。
      if (mail.password_set === true) {
        setVal('ch-mail-pass', '');
        const node = document.getElementById('ch-mail-pass') as HTMLInputElement | null;
        if (node) node.placeholder = '已保存(留空表示不修改)';
      }
      if (sms.token_set === true) {
        const node = document.getElementById('ch-sms-token') as HTMLInputElement | null;
        if (node) node.placeholder = '已保存(留空表示不修改)';
      }
      emailStatus.textContent = mail.configured
        ? '已配置:' + str(mail.host) + ':' + str(mail.port) + ' 账号 ' + str(mail.username)
        : '未配置 —— 邮件验证码只会写进服务器日志,用户收不到。';
      smsStatus.textContent = sms.configured
        ? '已配置:' + str(sms.url)
        : '未配置 —— 短信验证码只会写进服务器日志,用户收不到。';
    } catch (e) {
      emailStatus.textContent = '读取通道配置失败:' + errText(e);
      smsStatus.textContent = '读取通道配置失败:' + errText(e);
    }
  }

  qqPreset.addEventListener('click', () => {
    setVal('ch-mail-host', 'smtp.qq.com');
    setVal('ch-mail-port', '587');
    setVal('ch-mail-tls', 'starttls');
    toast('已填入 QQ 邮箱参数:登录账号填你的 QQ 邮箱,授权码在 QQ 邮箱设置里生成', 'info');
  });
  mail163Preset.addEventListener('click', () => {
    setVal('ch-mail-host', 'smtp.163.com');
    setVal('ch-mail-port', '465');
    setVal('ch-mail-tls', 'tls');
    toast('已填入 163 邮箱参数:登录账号填你的 163 邮箱,授权码在 163 邮箱设置里生成', 'info');
  });

  // 保存邮件通道配置。
  // 授权码留空表示「不修改」(后端保留旧值)。
  const saveEmailChannel = async (): Promise<void> => {
    await api('/api/account/mfa/channels', {
      method: 'POST',
      body: {
        channel: 'email',
        config: {
          host: elValue('ch-mail-host'),
          port: Number(elValue('ch-mail-port') || '587'),
          username: elValue('ch-mail-user'),
          password: elValue('ch-mail-pass'),
          from_addr: elValue('ch-mail-from'),
          tls_mode: elValue('ch-mail-tls') || 'starttls',
        },
      },
    });
  };

  emailSave.addEventListener('click', () =>
    void withBusy(emailSave, async () => {
      try {
        await requireVerify();
        await saveEmailChannel();
        toast('邮件通道已保存(授权码加密入库)', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  emailTest.addEventListener('click', () =>
    void withBusy(emailTest, async () => {
      try {
        await requireVerify();
        // 先把表单里的配置存下去再测。
        // 不然「发送测试邮件」用的是库里那份(可能压根没保存过),
        // 用户填了半天点测试,后端却说「通道尚未配置」。
        await saveEmailChannel();
        const res = await api<{ message?: string }>('/api/account/mfa/channels/test', {
          method: 'POST',
          body: {
            channel: 'email',
            // 留空则由后端发到登录账号。
            target: elValue('ch-mail-test-to') || elValue('ch-mail-user'),
          },
        });
        toast(res.message || '测试邮件已发送,请查收', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  emailDel.addEventListener('click', () =>
    void withBusy(emailDel, async () => {
      try {
        await requireVerify();
        await api('/api/account/mfa/channels/delete', {
          method: 'POST',
          body: { channel: 'email' },
        });
        toast('邮件通道已删除', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  // 保存短信通道配置(令牌留空表示「不修改」)。
  const saveSMSChannel = async (): Promise<void> => {
    await api('/api/account/mfa/channels', {
      method: 'POST',
      body: {
        channel: 'sms',
        config: {
          url: elValue('ch-sms-url'),
          token: elValue('ch-sms-token'),
          code_field: elValue('ch-sms-code-field') || 'code',
          target_field: elValue('ch-sms-target-field') || 'target',
        },
      },
    });
  };

  smsSave.addEventListener('click', () =>
    void withBusy(smsSave, async () => {
      try {
        await requireVerify();
        await saveSMSChannel();
        toast('短信通道已保存(令牌加密入库)', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  smsTest.addEventListener('click', () =>
    void withBusy(smsTest, async () => {
      try {
        await requireVerify();
        // 同邮件:先保存再测,免得用户填了没保存、点测试却说「通道尚未配置」。
        await saveSMSChannel();
        const res = await api<{ message?: string }>('/api/account/mfa/channels/test', {
          method: 'POST',
          body: {
            channel: 'sms',
            target: elValue('ch-sms-test-to'),
          },
        });
        toast(res.message || '测试短信已发送,请查收', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  smsDel.addEventListener('click', () =>
    void withBusy(smsDel, async () => {
      try {
        await requireVerify();
        await api('/api/account/mfa/channels/delete', {
          method: 'POST',
          body: { channel: 'sms' },
        });
        toast('短信通道已删除', 'ok');
        await loadChannels();
      } catch (e) {
        toast(errText(e), 'err');
      }
    }));

  return cardEl;
}

// ---------------------------------------------------------------- 辅助

/** 二次验证方式的中文名。 */
function mfaTypeLabel(mfaType: string): string {
  switch (mfaType) {
    case 'sms': return '短信';
    case 'totp': return '验证器 App';
    default: return '邮箱';
  }
}

/** 当前绑定的描述(TOTP 没有接收地址,只说方式)。 */
function mfaBindingText(mfaType: string, target: string): string {
  if (mfaType === 'totp') return '验证器 App';
  return mfaTypeLabel(mfaType) + ' → ' + (target || '—');
}

function str(v: unknown): string {
  return v === undefined || v === null ? '' : String(v);
}

/** 审计动作的中文名。 */
const ACTION_LABELS: Record<string, string> = {
  'account.login': '登录',
  'account.mfa': '二次验证',
  'account.password': '修改口令',
  'channel.save': '保存渠道配置',
  'channel.test': '渠道连通性测试',
  'channel.reveal': '查看密钥明文(已下线)',
  'order.pay': '发起支付',
  'order.refund': '退款',
  'order.close': '关闭订单',
  'order.charge': '钱包扣款',
  'order.export': '导出订单',
  'apikey.create': '创建密钥',
  'apikey.revoke': '吊销密钥',
};

function elValue(id: string): string {
  const node = document.getElementById(id) as HTMLInputElement | null;
  return node ? node.value.trim() : '';
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** 渲染审计日志的分页条。 */
function renderLogPager(
  box: HTMLElement,
  res: { page: number; total: number; pages: number },
  goto: (page: number) => void,
): void {
  box.replaceChildren();
  const pages = res.pages || 0;
  if (pages <= 1) {
    box.appendChild(el('span', { class: 'muted', text: '共 ' + (res.total || 0) + ' 条' }));
    return;
  }
  const prev = el('button', { class: 'btn btn-secondary btn-sm', text: '上一页' }) as HTMLButtonElement;
  const next = el('button', { class: 'btn btn-secondary btn-sm', text: '下一页' }) as HTMLButtonElement;
  prev.disabled = res.page <= 1;
  next.disabled = res.page >= pages;
  prev.addEventListener('click', () => goto(res.page - 1));
  next.addEventListener('click', () => goto(res.page + 1));
  box.appendChild(prev);
  box.appendChild(next);
  box.appendChild(el('span', { class: 'muted', text: `第 ${res.page}/${pages} 页 · 共 ${res.total} 条` }));
}
