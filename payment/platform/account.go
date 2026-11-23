package platform

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// 账户安全相关接口:口令、二次验证(邮箱/短信)、敏感操作票据。
//
// 设计要点:
//   - 口令只存 PBKDF2 散列,永不出库、永不回显;
//   - 二次验证验证码只存散列,5 分钟有效、用一次即作废;
//   - 「敏感操作票据」是短时令牌(默认 10 分钟),由「口令 + 验证码」换取,
//     改渠道配置 / 发起支付 / 退款 / 查看密钥明文都必须带上;

// mfaTargetLabel 返回二次验证绑定目标的可展示文本。
//
// TOTP 没有「接收地址」,显示成「验证器 App」;其余类型做脱敏。
func mfaTargetLabel(mfaType, target string) string {
	if mfaType == MFATypeTOTP {
		return "验证器 App"
	}
	return maskTarget(target)
}

// handleAccountSecurity 返回当前账户的安全状态。
//
// GET /api/account/security
func (p *Platform) handleAccountSecurity(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	// mfa_deliverable:当前绑定**能不能真的把验证码发出去**。
	//
	// false 意味着:绑定还在(界面上显示「已开启」),但验证码只会写进日志,
	// 用户永远收不到 —— 这样的二次验证拦不住任何人,却能挡住正主。
	// 前端要如实显示,别让人以为"我明明开了二次验证,怎么没拦住"。
	mfaDeliverable := true
	if user.MFAEnabled && user.MFAType != MFATypeTOTP {
		if ready, cerr := p.mfaChannelReady(user.UserID, user.MFAType); cerr != nil {
			p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
			mfaDeliverable = true
		} else {
			mfaDeliverable = ready
		}
	}
	// 备用方式同理:「绑了备用邮箱」不等于「邮箱真能收到验证码」。
	// 通道只写日志(没配 SMTP / 平台用 -log-mfa 起的)时,备用方式就是个
	// 摆设 —— 用户丢了 App 点「用备用邮箱」,还是登不进来。
	// 前端要如实说:收不到码。
	backupDeliverable := true
	if user.MFABackupType != "" {
		if ready, cerr := p.mfaChannelReady(user.UserID, user.MFABackupType); cerr != nil {
			p.opts.Logger.Printf("检查备用验证方式下发通道失败: %v", cerr)
			backupDeliverable = true
		} else {
			backupDeliverable = ready
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":         user.UserID,
		"name":            user.UserName,
		"password_set":    user.PasswordHash != "",
		"mfa_enabled":     user.MFAEnabled,
		"mfa_type":        user.MFAType,
		"mfa_target":      mfaTargetLabel(user.MFAType, user.MFATarget),
		"mfa_deliverable": mfaDeliverable,
		// 备用验证方式:验证器 App 丢了时的第二条腿。
		"mfa_backup":        user.MFABackupType != "",
		"mfa_backup_type":   user.MFABackupType,
		"mfa_backup_target": maskTarget(user.MFABackupTarget),
		// 备用方式的码**能不能真的发出去**。
		"mfa_backup_deliverable": backupDeliverable,
		"verify_ttl_sec":         int(p.opts.VerifyTTL.Seconds()),
		// 通道配置状态:没配通道时前端要明确提示「验证码只会写进日志」,
		// 而不是让用户对着一个永远收不到的信封等。
		"email_channel_configured": p.channelConfigured(user.UserID, mfaChannelEmail),
		"sms_channel_configured":   p.channelConfigured(user.UserID, mfaChannelSMS),
	})
}

// channelConfigured 判断某个账户是否配了指定类型的下发通道。
func (p *Platform) channelConfigured(userID, channel string) bool {
	cfg, err := p.store.loadMFAChannel(userID, channel)
	if err != nil || cfg == nil {
		return false
	}
	return len(cfg) > 0
}

// handleSetPassword 设置/修改登录口令。
//
// POST /api/account/password {"old_password":"...","new_password":"..."}
func (p *Platform) handleSetPassword(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if len(req.NewPassword) < 8 {
		writeError(w, http.StatusBadRequest, "新口令不少于 8 位")
		return
	}
	if user.PasswordHash != "" {
		if !verifyPassword(user.PasswordHash, req.OldPassword) {
			p.audit(r, user.UserID, auditAccountPass, "password", "原口令错误", "")
			writeCredentialError(w, "原口令不正确")
			return
		}
	}
	hash, err := hashPassword(req.NewPassword)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := p.store.SetPassword(user.UserID, hash); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountPass, "password", "口令已更新", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleSendLoginCode 下发**登录用**的二次验证验证码(未登录场景)。
//
// POST /api/login/code {"name":"商户A"}
// POST /api/login/code {"name":"商户A","backup":true} —— 发到**备用**地址
//
// 这个接口必须是公开的:开了二次验证的账户在登录前没有令牌,
// 拿不到任何能发验证码的接口,结果就是「开了二次验证就再也登不进去」。
//
// 安全要点:
//
//  1. 只能发给**该账户已绑定的**接收地址,请求里不能指定目标
//     (否则等于「把验证码发到攻击者邮箱」,二次验证形同虚设);
//  2. 响应不区分「账户不存在 / 没开二次验证 / 已发送」,
//     避免被用来探测哪些商户名已注册;
//  3. 按 IP 与按账户名限流,挡「短信轰炸」与「枚举商户名」。
func (p *Platform) handleSendLoginCode(w http.ResponseWriter, r *http.Request) {
	ip := p.clientIPOf(r)
	if !p.loginLimiter.allow("logincode-ip:"+ip, loginCodeRateLimitPerIP, time.Minute) {
		writeRateLimited(w, "验证码发送过于频繁，请稍后再试")
		return
	}
	var req struct {
		Name string `json:"name"`
		// Backup true = 发到**备用**地址(验证器 App 丢了时的第二条腿)。
		Backup bool `json:"backup"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "请填写商户名称")
		return
	}
	if !p.loginLimiter.allow("logincode-name:"+name, loginCodeRateLimitPerAccount, time.Minute) {
		writeRateLimited(w, "验证码发送过于频繁，请稍后再试")
		return
	}
	user, err := p.store.UserByName(name)
	if err != nil {
		p.opts.Logger.Printf("查询用户失败: %v", err)
	}
	// 只有「已存在且开了二次验证」的账户才会真的收到验证码;
	// 其它情况一律返回同样的成功响应。
	//
	// TOTP(验证器 App)不需要下发任何东西:码是用户 App 本地算出来的 ——
	// 但账户若绑了**备用**邮箱/短信,那条路依然可以发码。
	mfaType, mfaTarget, purpose := "", "", mfaPurposeLogin
	if user != nil && user.MFAEnabled {
		if req.Backup {
			mfaType, mfaTarget, purpose = user.MFABackupType, user.MFABackupTarget, mfaPurposeLoginBackup
		} else {
			mfaType, mfaTarget = user.MFAType, user.MFATarget
		}
	}
	if mfaType != "" && mfaTarget != "" && mfaType != MFATypeTOTP {
		// 通道不可用就别发了:发了也只是写进日志,用户永远收不到 ——
		// 倒不如不发,免得日志里堆一堆没人能用的码。
		ready, cerr := p.mfaChannelReady(user.UserID, mfaType)
		if cerr != nil {
			p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
		} else if !ready {
			// 用户在页面上看到的是「已发送」,实际什么也没发出去 ——
			// 至少在日志里说清楚为什么,不然查起来只能靠猜。
			p.opts.Logger.Printf("[MFA] 登录验证码没有下发:账户 %s 的 %s 通道不可用"+
				"(没配 SMTP/短信网关,或平台以 -log-mfa 启动,验证码只写日志)", user.UserID, mfaType)
		} else {
			code, cerr := generateMFACode()
			if cerr == nil {
				if err := p.store.CreateMFACode(user.UserID, purpose, code, mfaCodeTTL*time.Second); err == nil {
					if _, err := p.sendMFACode(r, user.UserID, mfaType, mfaTarget, code); err != nil {
						p.opts.Logger.Printf("登录验证码下发失败: %v", err)
					}
				}
			}
		}
	} else if req.Backup && user != nil && user.MFAEnabled {
		// 点了「用备用邮箱/短信」,但这个账户没有备用方式 —— 说清楚,
		// 免得平台方以为「发了但用户没收到」。
		p.opts.Logger.Printf("[MFA] 账户 %s 请求了备用验证码,但没有绑定备用验证方式", user.UserID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "如果该账户已开启二次验证,验证码已发送到其绑定的接收地址,5 分钟内有效",
	})
}

// handleSendMFACode 下发二次验证验证码。
//
// POST /api/account/mfa/code {"type":"email|sms","target":"...","purpose":"bind|sensitive"}
//
// 下发频率受限(按用户与按 IP):否则这个接口会被当成短信/邮件轰炸器。
//
// 目标地址的规则(重要):
//
//   - purpose=bind(绑定新的接收地址):目标由请求指定,但必须先验口令;
//   - 其它用途(登录 / 敏感操作 / 关闭二次验证 / 更换绑定):目标**强制**为
//     已绑定的地址,不允许请求里换一个。
//
// 否则就出现了这样的绕过:攻击者知道口令、但拿不到受害者的邮箱,
// 只要把验证码发到自己邮箱,就能完成二次验证。
//
// TOTP(验证器 App)没有「下发」这回事:码是用户 App 本地算出来的,
// 这个接口对 TOTP 账户一律拒绝,免得前端按邮件/短信的流程去调它。
func (p *Platform) handleSendMFACode(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if !p.mfaLimiter.allow("user:"+user.UserID, mfaSendRateLimitPerUser, time.Minute) {
		writeRateLimited(w, "验证码发送过于频繁，请稍后再试")
		return
	}
	if !p.mfaLimiter.allow("ip:"+p.clientIPOf(r), mfaSendRateLimitPerIP, time.Minute) {
		writeRateLimited(w, "验证码发送过于频繁，请稍后再试")
		return
	}
	var req struct {
		Type    string `json:"type"`
		Target  string `json:"target"`
		Purpose string `json:"purpose"`
		Code    string `json:"code"`
		// Password:绑定用途下的登录口令(与 code 区分开,免得前端把口令塞进 code 字段)。
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	purpose := req.Purpose
	if purpose == "" {
		purpose = mfaPurposeSensitive
	}
	target := strings.TrimSpace(req.Target)
	mfaType := normalizeMFAType(req.Type)

	switch purpose {
	case mfaPurposeBind:
		// 绑定新目标:必须验口令,且要明确给出接收地址。
		bindPassword := req.Password
		if bindPassword == "" {
			// 兼容旧调用:口令曾经是塞在 code 字段里的。
			bindPassword = req.Code
		}
		if user.PasswordHash == "" || !verifyPassword(user.PasswordHash, bindPassword) {
			writeCredentialError(w, "口令不正确")
			return
		}
		if target == "" {
			writeError(w, http.StatusBadRequest, "请填写验证码接收地址")
			return
		}
		if mfaType == "" {
			writeError(w, http.StatusBadRequest, "type 必须是 email 或 sms")
			return
		}
		if mfaType == MFATypeTOTP {
			writeError(w, http.StatusBadRequest, "验证器 App 方式不需要下发验证码")
			return
		}
		if err := validateMFATarget(mfaType, target); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	case mfaPurposeLogin, mfaPurposeSensitive, mfaPurposeDisable, mfaPurposeRebind:
		// 其它用途:只能发到已绑定的接收地址。
		if !user.MFAEnabled {
			writeError(w, http.StatusBadRequest, "尚未开启二次验证")
			return
		}
		if user.MFAType == MFATypeTOTP {
			writeError(w, http.StatusBadRequest, "验证器 App 方式不需要下发验证码,请直接输入 App 里的动态码")
			return
		}
		if user.MFATarget == "" {
			writeError(w, http.StatusBadRequest, "尚未开启二次验证")
			return
		}
		if target != "" && target != user.MFATarget {
			writeError(w, http.StatusBadRequest, "验证码只能发送到已绑定的接收地址")
			return
		}
		target = user.MFATarget
		mfaType = user.MFAType
	default:
		writeError(w, http.StatusBadRequest, "不支持的 purpose: "+purpose)
		return
	}

	// 通道不可用就别发了:发了也只是写进日志,用户永远收不到,
	// 而页面还会显示一句「验证码已发送」—— 那是在骗人。
	ready, cerr := p.mfaChannelReady(user.UserID, mfaType)
	if cerr != nil {
		writeError(w, http.StatusInternalServerError, "检查验证码下发通道失败: "+cerr.Error())
		return
	}
	if !ready {
		writeError(w, http.StatusBadRequest, mfaChannelMissingHint(mfaType))
		return
	}

	code, err := generateMFACode()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := p.store.CreateMFACode(user.UserID, purpose, code, mfaCodeTTL*time.Second); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	delivered, err := p.sendMFACode(r, user.UserID, mfaType, target, code)
	if err != nil {
		writeError(w, http.StatusBadGateway, "验证码下发失败: "+err.Error())
		return
	}
	msg := "验证码已发送,5 分钟内有效"
	if !delivered {
		// 没配下发通道时,平台只能把码写进日志 —— 这时候必须说清楚,
		// 否则用户会一直等一个永远不会来的邮件。
		msg = "验证码已写入服务器日志(尚未配置邮件/短信通道,用户实际收不到);请在「账户安全」页配置下发通道"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"target":     maskTarget(target),
		"expires_in": mfaCodeTTL,
		"delivered":  delivered,
		"message":    msg,
	})
}

// handleEnableMFA 开启(或更换)二次验证。
//
// POST /api/account/mfa/enable
//
//   - email/sms:{"type":"email|sms","target":"...","password":"...","code":"..."}
//   - totp:{"type":"totp","password":"...","secret":"...","code":"123456"}
//
// 「更换接收方式」不需要先关闭:绑定新地址时验证码发到**新**地址,
// 校验通过后直接覆盖旧绑定(旧地址的验证码此后一律作废)。
//
// **换绑必须由当前绑定的人确认**(见 requireCurrentBinding):
// 账户已经开过二次验证时,再来一次 enable 就是换绑 —— 旧绑定会当场失效。
// 只要口令就能换绑的话,拿到口令的人(共用电脑、浏览器被窥、密码泄露)
// 可以把验证器换成自己的,正主直接被锁在门外,二次验证反倒替对方开了门。
// 所以已经绑定过的账户,请求里必须带 current_code:旧验证器 App 的动态码,
// 或发到**已绑定地址**的验证码。
//
// **验证器 App(TOTP)换绑是个例外**:它的「当前绑定确认」发生在
// 「生成密钥」那一步(见 handleTOTPSetup)—— 因为只有先确认了旧绑定,
// 才能把新密钥发出去;扫完码之后旧条目可能已被覆盖,旧码取不到了。
// 所以这里对 TOTP 只校验**新密钥**的动态码。
func (p *Platform) handleEnableMFA(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if !p.mfaLimiter.allow("mfa:"+user.UserID, mfaVerifyRateLimitPerUser, time.Minute) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	var req struct {
		Type   string `json:"type"`
		Target string `json:"target"`
		// Password 登录口令。
		Password string `json:"password"`
		// Code 新绑定的验证码(TOTP 动态码 / 发到新地址的绑定码)。
		Code string `json:"code"`
		// CurrentCode 「当前绑定的人」的确认码:换绑时必填。
		CurrentCode string `json:"current_code"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	mfaType := normalizeMFAType(req.Type)
	if mfaType == "" {
		writeError(w, http.StatusBadRequest, "type 必须是 email、sms 或 totp")
		return
	}
	if user.PasswordHash == "" || !verifyPassword(user.PasswordHash, req.Password) {
		writeCredentialError(w, "口令不正确")
		return
	}

	// 换绑确认:已经在用二次验证的账户,再 enable 一次就是换绑。
	//
	// 唯一例外:邮箱/短信方式且**地址没变** —— 这种情况发到该地址的
	// 绑定码本身就证明了「地址还在你手里」,不用再要一个确认码
	// (否则用户会收到两封一模一样的验证码,平白多点一次)。
	//
	// TOTP 例外:**验证器 App 不在这里要旧动态码**。
	//
	// 换绑 TOTP 的「当前绑定确认」已经在**生成密钥**那一步做过了
	// (handleTOTPSetup 里拿旧 App 的动态码验过,见 requireCurrentBinding)。
	// 这里再要一次旧动态码是有害的:用户扫完新二维码之后,验证器 App 里
	// 旧的那条往往已经被新条目覆盖,旧码根本取不出来 —— 于是「保存」永远失败。
	// 而新密钥算出来的动态码本身就是「你确实拿到了新密钥」的证明。
	if user.MFAEnabled && !sameMFABinding(user, mfaType, strings.TrimSpace(req.Target)) {
		if mfaType != MFATypeTOTP {
			if err := p.requireCurrentBinding(user, req.CurrentCode, true); err != nil {
				p.audit(r, user.UserID, auditAccountMFA, "rebind", "更换绑定被拒:未通过当前绑定确认", "")
				writeError(w, http.StatusForbidden, err.Error())
				return
			}
		}
	}

	switch mfaType {
	case MFATypeTOTP:
		// 密钥以**服务端暂存**的那份为准:
		// 前端回传的密钥不可信(用户可以随便编一个,那等于没开二次验证)。
		secret := p.store.MFATOTPPendingSecret(user.UserID)
		if secret == "" {
			writeError(w, http.StatusBadRequest, "请先点击「生成密钥」并用验证器 App 扫码")
			return
		}
		// 必须用同一份密钥算出同一个码,否则等于谁都能开二次验证。
		if !verifyTOTPCode(secret, req.Code, time.Now()) {
			writeError(w, http.StatusBadRequest, "动态码无效或已过期")
			return
		}
		if err := p.store.SetMFATOTPSecret(user.UserID, secret); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		_ = p.store.SetMFATOTPPendingSecret(user.UserID, "")
		if err := p.store.SetMFA(user.UserID, MFATypeTOTP, "", true); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		p.audit(r, user.UserID, auditAccountMFA, MFATypeTOTP, "开启二次验证(验证器 App)", "")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mfa_type": MFATypeTOTP})
		return
	default:
		target := strings.TrimSpace(req.Target)
		if target == "" {
			writeError(w, http.StatusBadRequest, "请填写验证码接收地址")
			return
		}
		if err := validateMFATarget(mfaType, target); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// 收不到码就别开:邮箱 / 短信方式必须有能用的下发通道,
		// 否则开启之后登录就要这个码 —— 用户永远收不到,等于被锁在门外。
		if err := p.ensureMFAChannelReady(user.UserID, mfaType); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if !p.store.ConsumeMFACode(user.UserID, mfaPurposeBind, req.Code) {
			writeError(w, http.StatusBadRequest, "验证码无效或已过期")
			return
		}
		if err := p.store.SetMFA(user.UserID, mfaType, target, true); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		p.audit(r, user.UserID, auditAccountMFA, mfaType, "开启二次验证 target="+maskTarget(target), "")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mfa_type": mfaType})
		return
	}
}

// handleBindMFABackup 绑定**备用**二次验证(邮箱或短信)。
//
// POST /api/account/mfa/backup {"type":"email|sms","target":"...","password":"...","code":"..."}
//
// 「备用验证方式」是给「验证器 App 丢了/被误删/手机被偷」准备的第二条腿:
// 绑好之后,登录页可以用发到备用地址的验证码登录,账户不至于永久失联。
//
// 绑它需要三样:
//   - 登录口令;
//   - **当前绑定的人**的确认(旧 App 动态码 / 发到已绑定地址的验证码)——
//     否则拿到口令的人可以给自己的邮箱绑一个「备用方式」,直接绕过验证器登录;
//   - 发到**新地址**的绑定验证码(证明这个地址在你手里)。
func (p *Platform) handleBindMFABackup(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if !p.mfaLimiter.allow("mfa:"+user.UserID, mfaVerifyRateLimitPerUser, time.Minute) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	var req struct {
		Type        string `json:"type"`
		Target      string `json:"target"`
		Password    string `json:"password"`
		Code        string `json:"code"`
		CurrentCode string `json:"current_code"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	mfaType := normalizeMFAType(req.Type)
	if mfaType != MFATypeEmail && mfaType != MFATypeSMS {
		writeError(w, http.StatusBadRequest, "备用验证方式只能是邮箱或短信")
		return
	}
	if user.PasswordHash == "" || !verifyPassword(user.PasswordHash, req.Password) {
		writeCredentialError(w, "口令不正确")
		return
	}
	target := strings.TrimSpace(req.Target)
	if target == "" {
		writeError(w, http.StatusBadRequest, "请填写备用接收地址")
		return
	}
	if err := validateMFATarget(mfaType, target); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 收不到码就别绑:备用方式的意义是「真能用」,绑一个发不出去的电话号
	// 等于给用户一个「我有退路」的错觉。
	if err := p.ensureMFAChannelReady(user.UserID, mfaType); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 必须是「当前绑定的人」在操作:否则知道口令的人可以给自己的邮箱
	// 绑一个备用方式,然后用它登录 —— 验证器 App 就形同虚设了。
	if user.MFAEnabled {
		if err := p.requireCurrentBinding(user, req.CurrentCode, true); err != nil {
			p.audit(r, user.UserID, auditAccountMFA, "backup", "绑定备用方式被拒:未通过当前绑定确认", "")
			writeError(w, http.StatusForbidden, err.Error())
			return
		}
	}
	if !p.store.ConsumeMFACode(user.UserID, mfaPurposeBind, req.Code) {
		writeError(w, http.StatusBadRequest, "验证码无效或已过期")
		return
	}
	if err := p.store.SetMFABackup(user.UserID, mfaType, target); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountMFA, "backup", "绑定备用二次验证 target="+maskTarget(target), "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mfa_backup_type": mfaType})
}

// handleDeleteMFABackup 删除备用二次验证。
//
// DELETE /api/account/mfa/backup {"password":"..."}
//
// 需要敏感操作票据:删掉备用方式等于把账户的退路也一起删了,
// 别让人顺手点一下就没了。
func (p *Platform) handleDeleteMFABackup(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if req.Password != "" && (user.PasswordHash == "" || !verifyPassword(user.PasswordHash, req.Password)) {
		writeCredentialError(w, "口令不正确")
		return
	}
	if err := p.store.ClearMFABackup(user.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountMFA, "backup", "删除备用二次验证", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// sameMFABinding 判断「新绑定」和「当前绑定」是不是同一个。
//
// 用于邮箱/短信方式:地址没变时,发到该地址的绑定码本身就是
// 「这个地址还在你手里」的证明,不必再要一个单独的确认码。
func sameMFABinding(user *User, mfaType, target string) bool {
	if !user.MFAEnabled || user.MFAType != mfaType {
		return false
	}
	if mfaType == MFATypeTOTP {
		// TOTP 没有地址可比:重新绑定验证器一定要旧 App 的动态码确认。
		return false
	}
	return target != "" && target == user.MFATarget
}

// requireCurrentBinding 校验「当前绑定的人」的确认。
//
// 场景:账户已经开过二次验证,现在要**换绑**(换验证器 / 换地址)。
// 换绑会让旧绑定立刻失效 —— 如果只要口令就能换,拿到口令的人
// (共用电脑、浏览器被窥、密码泄露)可以把验证器换成自己的,
// 正主当场被锁在门外,二次验证反而成了对方的门。
//
// 所以换绑必须由**当前绑定的人**确认:
//
//   - 当前是验证器 App:要旧 App 里的动态码(拿**生效**密钥校验,
//     不是新生成的那个待确认密钥);
//   - 当前是邮箱/短信:要发到**已绑定地址**的一次性验证码。
//
// consume=true 时校验通过会把这枚验证码作废(真正落地绑定时用),
// consume=false 时只校验不作废(生成密钥那一步用:那时还没绑定完,
// 验证码要留到提交绑定时再用一次)。
//
// 通道不可用(码发不出去)时跳过这一步:那种绑定本来就拦不住任何人,
// 卡死只会把账户锁死 —— 与「关闭二次验证」「敏感操作票据」的处理一致。
func (p *Platform) requireCurrentBinding(user *User, code string, consume bool) error {
	code = strings.TrimSpace(code)
	if user.MFAType == MFATypeTOTP {
		secret := p.store.MFATOTPSecret(user.UserID)
		if secret == "" {
			// 没有生效密钥 = 这条绑定本来就是坏的(登录、关闭都过不去),
			// 拿它当换绑凭证只会把账户锁死:跳过。
			p.opts.Logger.Printf("账户 %s 的 TOTP 绑定没有生效密钥,换绑时不再要求当前绑定确认", user.UserID)
			return nil
		}
		if !p.store.ConsumeTOTPCode(user.UserID, secret, code, totpNow()) {
			return fmt.Errorf("当前绑定确认失败:请输入旧验证器 App 里的 6 位动态码")
		}
		return nil
	}
	if user.MFATarget == "" {
		// 没有接收地址 = 没有「当前绑定」可证明。
		p.opts.Logger.Printf("账户 %s 的二次验证没有接收地址,换绑时不再要求当前绑定确认", user.UserID)
		return nil
	}
	needCode := true
	if ready, cerr := p.mfaChannelReady(user.UserID, user.MFAType); cerr != nil {
		p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
	} else if !ready {
		needCode = false
		p.opts.Logger.Printf("账户 %s 的二次验证(%s)没有可用下发通道,换绑时不再要求当前绑定确认",
			user.UserID, user.MFAType)
	}
	if !needCode {
		return nil
	}
	hint := fmt.Sprintf("当前绑定确认失败:请输入发到已绑定地址(%s)的验证码", maskTarget(user.MFATarget))
	if consume {
		if !p.store.ConsumeMFACode(user.UserID, mfaPurposeRebind, code) {
			return fmt.Errorf("%s", hint)
		}
		return nil
	}
	if !p.store.PeekMFACode(user.UserID, mfaPurposeRebind, code) {
		return fmt.Errorf("%s", hint)
	}
	return nil
}

// handleDisableMFA 关闭二次验证。
//
// POST /api/account/mfa/disable {"password":"...","code":"..."}
//
// code 的含义随方式而变:
//
//   - email/sms:发到已绑定地址的一次性验证码;
//   - totp:当前验证器 App 里的动态码(不需要下发任何东西)。
func (p *Platform) handleDisableMFA(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if !p.mfaLimiter.allow("mfa:"+user.UserID, mfaVerifyRateLimitPerUser, time.Minute) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	var req struct {
		Password string `json:"password"`
		Code     string `json:"code"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if user.PasswordHash == "" || !verifyPassword(user.PasswordHash, req.Password) {
		writeCredentialError(w, "口令不正确")
		return
	}
	if user.MFAType == MFATypeTOTP {
		secret := p.store.MFATOTPSecret(user.UserID)
		if secret == "" || !p.store.ConsumeTOTPCode(user.UserID, secret, req.Code, totpNow()) {
			writeError(w, http.StatusBadRequest, "动态码无效或已过期,请输入验证器 App 当前显示的 6 位码")
			return
		}
	} else {
		// 通道还在 → 要验证码(证明是本人在操作)。
		//
		// 通道没了 → **不要**要验证码:那个码用户根本收不到,
		// 硬要就等于让账户永远关不掉二次验证(死锁)。
		// 这种绑定本来就拦不住任何人,关掉它不损失安全性。
		needCode := true
		if ready, cerr := p.mfaChannelReady(user.UserID, user.MFAType); cerr != nil {
			p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
		} else if !ready {
			needCode = false
			p.opts.Logger.Printf("账户 %s 的二次验证(%s)没有可用下发通道,关闭时不再要求验证码",
				user.UserID, user.MFAType)
		}
		if needCode && !p.store.ConsumeMFACode(user.UserID, mfaPurposeDisable, req.Code) {
			writeError(w, http.StatusBadRequest, "验证码无效或已过期")
			return
		}
	}
	if err := p.store.SetMFA(user.UserID, "", "", false); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 一并清掉 TOTP 共享密钥:留着它就等于给下一个绑定者留了一把万能钥匙。
	if err := p.store.SetMFATOTPSecret(user.UserID, ""); err != nil {
		p.opts.Logger.Printf("清理 TOTP 密钥失败: %v", err)
	}
	p.audit(r, user.UserID, auditAccountMFA, "disable", "关闭二次验证", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleVerify 校验身份并签发敏感操作票据。
//
// POST /api/account/verify {"password":"...","code":"..."}
//
// code 仅在该账户已开启二次验证时需要。
func (p *Platform) handleVerify(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	// 校验频率受限:这里是口令 + 验证码的双重入口。
	if !p.mfaLimiter.allow("verify:"+user.UserID, mfaVerifyRateLimitPerUser, time.Minute) {
		writeError(w, http.StatusTooManyRequests, "验证过于频繁，请稍后再试")
		return
	}
	var req struct {
		Password string `json:"password"`
		Code     string `json:"code"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if user.PasswordHash == "" {
		writeError(w, http.StatusBadRequest, "请先在「账户安全」页设置登录口令")
		return
	}
	if !verifyPassword(user.PasswordHash, req.Password) {
		p.audit(r, user.UserID, auditAccountMFA, "verify", "口令错误", "")
		writeCredentialError(w, "口令不正确")
		return
	}
	if user.MFAEnabled {
		if user.MFAType == MFATypeTOTP {
			secret := p.store.MFATOTPSecret(user.UserID)
			if secret == "" || !p.store.ConsumeTOTPCode(user.UserID, secret, req.Code, totpNow()) {
				writeCredentialError(w, "动态码无效或已过期,请输入验证器 App 当前显示的 6 位码")
				return
			}
		} else {
			// 通道还在 → 要验证码(证明是本人在操作)。
			//
			// 通道没了 → **不要**要验证码:那个码用户根本收不到。
			// 而敏感操作票据是「配置通道 / 改口令 / 发起支付 / 退款」的唯一入口,
			// 在这里卡死就等于把账户彻底锁死 —— 用户连去配置通道都做不到。
			// 这种绑定本来就拦不住任何人(码只写日志),跳过它不损失安全性。
			needCode := true
			if ready, cerr := p.mfaChannelReady(user.UserID, user.MFAType); cerr != nil {
				p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
			} else if !ready {
				needCode = false
				p.audit(r, user.UserID, auditAccountMFA, "verify", "二次验证无可用下发通道,本次校验未要求验证码", "")
				p.opts.Logger.Printf("账户 %s 的二次验证(%s)没有可用下发通道,本次身份校验未要求验证码",
					user.UserID, user.MFAType)
			}
			if needCode && !p.store.ConsumeMFACode(user.UserID, mfaPurposeSensitive, req.Code) {
				writeCredentialError(w, "验证码无效或已过期")
				return
			}
		}
	}
	token, err := p.store.CreateVerifyToken(user.UserID, p.opts.VerifyTTL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountMFA, "verify", "签发敏感操作票据", "")
	writeJSON(w, http.StatusOK, map[string]any{
		"verify_token": token,
		"expires_in":   int(p.opts.VerifyTTL.Seconds()),
	})
}

// requireVerified 校验敏感操作票据。
//
// 票据来源:X-Verify-Token 头(不放 URL,理由同 currentUser)。
func (p *Platform) requireVerified(r *http.Request, user *User) error {
	token := r.Header.Get("X-Verify-Token")
	if token == "" {
		return fmt.Errorf("该操作需要二次验证：请先调用 POST /api/account/verify 获取票据")
	}
	owner, ok := p.store.VerifyToken(token)
	if !ok {
		return fmt.Errorf("验证票据无效或已过期，请重新验证身份")
	}
	if owner != user.UserID {
		return fmt.Errorf("验证票据不属于当前用户")
	}
	return nil
}

// normalizeMFAType 规范化二次验证类型。
func normalizeMFAType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case MFATypeEmail:
		return MFATypeEmail
	case MFATypeSMS, "phone", "mobile":
		return MFATypeSMS
	case MFATypeTOTP, "app", "authenticator", "mfa":
		return MFATypeTOTP
	default:
		return ""
	}
}
