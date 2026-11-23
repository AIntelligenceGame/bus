package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
	"github.com/AIntelligenceGame/bus/payment/qrcode"
)

// 二次验证用途。
const (
	// mfaPurposeLogin 登录二次验证。
	mfaPurposeLogin = "login"
	// mfaPurposeLoginBackup 登录时用**备用**方式(备用邮箱/短信)验证。
	//
	// 与主方式分开:主方式的码和备用方式的码不能互相顶替,
	// 也不共用同一个用途计数(免得主方式的错误次数把备用方式也锁掉)。
	mfaPurposeLoginBackup = "login_backup"
	// mfaPurposeBind 绑定/启用二次验证。
	mfaPurposeBind = "bind"
	// mfaPurposeDisable 关闭二次验证。
	mfaPurposeDisable = "disable"
	// mfaPurposeSensitive 敏感操作(改配置 / 发起支付 / 退款)。
	mfaPurposeSensitive = "sensitive"
	// mfaPurposeRebind 更换绑定时,证明「我是当前绑定的那个人」。
	//
	// 与其它用途分开:换绑的确认码和「关闭验证码」「敏感操作验证码」
	// 不能互相顶替 —— 用途分开才挡得住「拿关闭流程的码去换绑」这类绕法。
	mfaPurposeRebind = "rebind"
)

// 审计动作。
const (
	auditLogin         = "account.login"
	auditChannelSave   = "channel.save"
	auditChannelTest   = "channel.test"
	auditChannelReveal = "channel.reveal"
	auditOrderPay      = "order.pay"
	auditOrderRefund   = "order.refund"
	auditOrderClose    = "order.close"
	auditOrderCharge   = "order.charge"
	auditOrderExport   = "order.export"
	auditAccountMFA    = "account.mfa"
	auditAccountPass   = "account.password"
	// auditNotifyBase 修改商户自己的对外访问地址(回调地址前缀)。
	auditNotifyBase = "account.notify_base"
	// auditAPIKeyCreate 创建开放接口密钥。
	auditAPIKeyCreate = "apikey.create"
	// auditAPIKeyRevoke 吊销开放接口密钥。
	auditAPIKeyRevoke = "apikey.revoke"
	// auditAdminMerchant 平台管理员启用/禁用商户。
	auditAdminMerchant = "admin.merchant"
	// auditAppSave 创建 / 修改应用,以及保存应用专属渠道配置。
	auditAppSave = "app.save"
	// auditAppDelete 删除应用。
	auditAppDelete = "app.delete"
	// auditAppRequireBinding 开关「订单必须绑定应用」。
	auditAppRequireBinding = "app.require_binding"
)

// currentUser 从请求中解析当前用户(令牌来源:请求头或 Cookie)。
//
// 刻意**不支持** ?token= 查询参数:URL 会进浏览器历史、Referer、
// 反向代理日志与监控看板,令牌出现在 URL 里等于公开令牌。
func (p *Platform) currentUser(r *http.Request) (*User, error) {
	token := r.Header.Get("X-User-Token")
	if token == "" {
		if cookie, err := r.Cookie("pay_token"); err == nil {
			token = cookie.Value
		}
	}
	if token == "" {
		return nil, fmt.Errorf("缺少用户令牌，请先在首页登录")
	}
	user, err := p.store.UserByToken(token)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("用户令牌无效")
	}
	return user, nil
}

// ownOrder 取当前商户**自己**的订单。
//
// 跨商户改单是资金安全问题:任何按 out_trade_no 定位订单的写操作
// (退款 / 关单 / 查单 / 钱包扣款)都必须先过这一关。
// 别人的订单一律按「不存在」处理,不泄露它的存在。
func (p *Platform) ownOrder(user *User, outTradeNo string) (*OrderRecord, error) {
	if outTradeNo == "" {
		return nil, fmt.Errorf("缺少 out_trade_no")
	}
	record, err := p.store.OrderByTradeNo(user.UserID, outTradeNo)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("订单不存在: %s", outTradeNo)
	}
	return record, nil
}

// handleLogin 登录(或按名称创建用户),返回访问令牌。
//
// POST /api/login {"name": "商户A", "password": "...", "code": "123456"}
//
// code 仅在该账户已开启二次验证时需要。
//
// 防暴力破解的三道闸:
//
//  1. 限流:同一 IP / 同一账户的登录频率受限;
//  2. 账户锁定:连续输错口令(或验证码)达到上限后锁定一段时间;
//  3. 令牌轮换:每次登录都换新令牌,旧令牌立即失效(防会话固定)。
func (p *Platform) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Password string `json:"password"`
		Code     string `json:"code"`
		// RegistrationCode 注册码(平台设置了注册码时必填)。
		RegistrationCode string `json:"registration_code"`
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
	// 注册码也可以从请求头带(方便脚本调用)。
	if req.RegistrationCode == "" {
		req.RegistrationCode = r.Header.Get("X-Registration-Code")
	}

	// 限流:同一 IP 与同一账户都限。
	ip := p.clientIPOf(r)
	if !p.loginLimiter.allow("ip:"+ip, loginRateLimitPerIP, time.Minute) {
		writeRateLimited(w, "登录过于频繁，请稍后再试")
		return
	}
	if !p.loginLimiter.allow("name:"+name, loginRateLimitPerAccount, time.Minute) {
		writeRateLimited(w, "登录尝试过于频繁，请稍后再试")
		return
	}

	// 注册码:防止别人抢注商户名称(账号抢注)。
	//
	// 在查用户**之前**校验:否则「账户不存在 → 403」「账户存在但口令错 → 401」
	// 的差异会变成一个探测商户名称是否已注册的口子。
	if p.opts.RegistrationCode != "" && req.RegistrationCode != p.opts.RegistrationCode {
		writeError(w, http.StatusForbidden, "注册码不正确")
		return
	}

	user, err := p.store.UserByName(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	created := false
	// mfaSkipped:二次验证绑定在、但下发通道没了,本次登录跳过了它。
	mfaSkipped := false
	if user == nil {
		// 平台关闭了自助注册:建号只能由平台管理员来做。
		//
		// 否则任何人挑一个商户名就能抢先注册(账号抢注),
		// 受害者再也无法用这个名字登录。
		if p.opts.DisableSelfRegistration {
			writeError(w, http.StatusForbidden,
				"该商户名称尚未开通,请联系平台方创建账户(平台已关闭自助注册)")
			return
		}
		if len(req.Password) < 8 {
			writeError(w, http.StatusBadRequest, "首次登录请设置不少于 8 位的登录口令")
			return
		}
		hash, err := hashPassword(req.Password)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		user, err = p.store.CreateUser(name, hash)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		created = true
	} else {
		// 被平台禁用的商户:一律拒绝登录。
		if user.Disabled {
			writeError(w, http.StatusForbidden, "该商户已被平台禁用")
			return
		}
		// 账户被锁定:直接拒绝,不再校验口令(避免继续消耗 PBKDF2)。
		if locked, until := p.lockedUntil(user); locked {
			p.audit(r, user.UserID, auditLogin, name, "账户锁定中", "")
			writeRateLimited(w, "登录失败次数过多，账户已锁定，请于 "+until+" 后重试")
			return
		}
		if user.PasswordHash == "" {
			writeError(w, http.StatusForbidden, "该账户尚未设置口令，请通过管理接口重置")
			return
		}
		if !verifyPassword(user.PasswordHash, req.Password) {
			// 口令错误**不锁账户**:否则知道商户名的人就能把别人的账户锁死
			// (拒绝服务)。这里只记审计,靠 IP / 账户维度的限流挡住暴力破解。
			p.registerLoginFailure(r, user, name, "口令错误", false)
			writeError(w, http.StatusUnauthorized, "商户名称或口令不正确")
			return
		}
		if user.MFAEnabled {
			// 绑定还在,但**下发通道没了**(通道被删 / 从没配 / 平台没注入下发器):
			// 这种二次验证根本拦不住任何人 —— 验证码只会写进服务器日志,
			// 用户永远收不到。而登录又要这个码,于是账户被永久锁在门外。
			//
			// 所以这种「失效的绑定」不再强制校验:它本来就等于没开,
			// 拦不住攻击者,却能挡住正主。跳过时如实告诉用户。
			if user.MFAType != MFATypeTOTP {
				ready, cerr := p.mfaChannelReady(user.UserID, user.MFAType)
				switch {
				case cerr != nil:
					// 判断本身出错(比如查库失败)→ 保守起见仍然要求验证码。
					p.opts.Logger.Printf("检查二次验证下发通道失败: %v", cerr)
				case !ready:
					mfaSkipped = true
					p.opts.Logger.Printf("账户 %s 的二次验证(%s)没有可用下发通道,本次登录跳过二次验证",
						user.UserID, user.MFAType)
					p.audit(r, user.UserID, auditLogin, name, "二次验证无可用下发通道,已跳过", "")
				}
			}
			if !mfaSkipped {
				// 缺验证码 ≠ 验证码错:前者只是「还没填」,不该计入失败次数,
				// 否则用户第一次登录忘了填验证码就被计一次,连错几次直接锁死。
				//
				// 回一个 mfa_required 标志,前端据此把验证码输入框显示出来 ——
				// 没开二次验证的账户不会看到这个框。
				//
				// mfa_type 一并回传:前端据此区分「收到的短信/邮件里的码」
				// 和「验证器 App 里的动态码」,并决定要不要显示「重新获取验证码」。
				if strings.TrimSpace(req.Code) == "" {
					writeErrorFields(w, http.StatusUnauthorized,
						"该账户已开启二次验证,请输入验证码",
						map[string]any{
							"mfa_required": true,
							"mfa_type":     user.MFAType,
							// 备用方式:验证器 App 丢了还能走邮箱/短信。
							"mfa_backup": user.MFABackupType != "",
						})
					return
				}
				// 主方式先验;不行再看**备用**方式(验证器 App 丢了时的第二条腿)。
				passed := false
				if user.MFAType == MFATypeTOTP {
					secret := p.store.MFATOTPSecret(user.UserID)
					// 用 ConsumeTOTPCode 而不是 verifyTOTPCode:动态码必须一次性
					// (RFC 6238 §5.2),同一个码不能被重放。
					passed = secret != "" &&
						p.store.ConsumeTOTPCode(user.UserID, secret, req.Code, totpNow())
				} else {
					passed = p.store.ConsumeMFACode(user.UserID, mfaPurposeLogin, req.Code)
				}
				if !passed && user.MFABackupType != "" {
					passed = p.store.ConsumeMFACode(user.UserID, mfaPurposeLoginBackup, req.Code)
				}
				if !passed {
					// 口令已经正确、只是没过二次验证:说明对方**已经掌握了口令**,
					// 这种才值得锁定账户。
					p.registerLoginFailure(r, user, name, "验证码错误", true)
					msg := "验证码无效或已过期"
					if user.MFAType == MFATypeTOTP {
						msg = "动态码无效或已过期,请输入验证器 App 当前显示的 6 位码"
					}
					writeErrorFields(w, http.StatusUnauthorized, msg,
						map[string]any{
							"mfa_required": true,
							"mfa_type":     user.MFAType,
							"mfa_backup":   user.MFABackupType != "",
						})
					return
				}
			}
		}
	}

	// 登录成功:换新令牌(旧令牌立即失效),清零失败计数。
	token, err := p.store.RotateAccessToken(user.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := p.store.ResetLoginFailures(user.UserID); err != nil {
		p.opts.Logger.Printf("重置登录失败计数失败: %v", err)
	}
	p.loginLimiter.reset("name:" + name)

	http.SetCookie(w, &http.Cookie{
		Name:     "pay_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		// HTTPS 下加 Secure,避免令牌在明文连接里裸奔。
		Secure: isHTTPS(r),
	})
	p.audit(r, user.UserID, auditLogin, name, map[bool]string{true: "创建账户", false: "登录成功"}[created], "")
	resp := map[string]any{
		"user_id":     user.UserID,
		"name":        user.UserName,
		"token":       token,
		"created":     created,
		"mfa_enabled": user.MFAEnabled,
		"mfa_type":    user.MFAType,
		"mfa_target":  maskTarget(user.MFATarget),
	}
	if mfaSkipped {
		// 如实告诉用户:绑定还在,但它拦不住任何人(验证码发不出去),
		// 所以这次登录跳过了它。免得用户以为"我明明关了二次验证"。
		resp["mfa_skipped"] = true
		resp["mfa_note"] = "二次验证仍在绑定中,但验证码下发通道不可用(收不到),本次登录已跳过二次验证;请到「账户安全」页关闭它或改用验证器 App"
	}
	writeJSON(w, http.StatusOK, resp)
}

// lockedUntil 判断账户是否处于锁定状态。
func (p *Platform) lockedUntil(user *User) (bool, string) {
	if user == nil || user.LockedUntil == "" {
		return false, ""
	}
	exp, err := parseTime(user.LockedUntil)
	if err != nil {
		return false, ""
	}
	if time.Now().After(exp) {
		return false, ""
	}
	return true, exp.Format(time.RFC3339)
}

// registerLoginFailure 记一次登录失败。
//
// lock 为 true 时连续失败达到上限会锁定账户;为 false 时只累计次数。
func (p *Platform) registerLoginFailure(r *http.Request, user *User, name, detail string, lock bool) {
	until, err := p.store.RegisterLoginFailure(user.UserID, maxLoginFailures, loginLockDuration*time.Second, lock)
	if err != nil {
		p.opts.Logger.Printf("记录登录失败失败: %v", err)
	}
	p.audit(r, user.UserID, auditLogin, name, detail, "")
	if until != "" {
		p.opts.Logger.Printf("账户 %s 连续登录失败,已锁定至 %s", user.UserID, until)
	}
}

// handleLogout 退出登录(令牌作废)。
//
// POST /api/logout
func (p *Platform) handleLogout(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	// 轮换令牌:旧令牌立刻失效。
	if _, err := p.store.RotateAccessToken(user.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: "pay_token", Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: isHTTPS(r),
	})
	p.audit(r, user.UserID, auditLogin, user.UserName, "退出登录", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleMe 返回当前用户信息与已配置的渠道(密钥已脱敏)。
//
// GET /api/me
func (p *Platform) handleMe(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	channels, err := p.store.Channels(user.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defaultChannel, err := p.store.DefaultChannel(user.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	views := make([]map[string]any, 0, len(channels))
	for _, cfg := range channels {
		views = append(views, maskChannelConfig(cfg))
	}
	// 用户对象要脱敏后再下发:mfa_target 是邮箱/手机号,
	// 没有必要让前端拿到完整值(接口日志、截图都可能泄露出去)。
	me := *user
	me.MFATarget = mfaTargetLabel(user.MFAType, user.MFATarget)
	writeJSON(w, http.StatusOK, map[string]any{
		"user":            &me,
		"channels":        views,
		"default_channel": defaultChannel,
		// notify_base_url 是商户自己配的对外地址(空 = 用平台默认);
		// notify_base_default 是平台默认值(-base),页面据此提示回退后是什么。
		"notify_base_url":     user.NotifyBaseURL,
		"notify_base_default": p.defaultBase(),
		"notify_urls":         p.notifyURLMap(user),
	})
}

// handleSaveNotifyBase 保存商户自己的对外访问地址(回调地址前缀)。
//
// POST /api/account/notify-base
// {"notify_base_url":"https://pay.example.com"}
//
// 传空字符串表示「清除」,回到平台默认地址(启动参数 -base)。
//
// 需要二次验证票据(X-Verify-Token):这个设置决定了「渠道把支付结果告诉谁」,
// 不能只凭一个会话令牌就改掉。
func (p *Platform) handleSaveNotifyBase(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var body struct {
		NotifyBaseURL string `json:"notify_base_url"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	base, err := normalizeBaseURL(body.NotifyBaseURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := p.store.SetNotifyBaseURL(user.UserID, base); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	detail := "恢复平台默认回调地址"
	if base != "" {
		detail = "回调地址改为 " + base
	}
	p.audit(r, user.UserID, auditNotifyBase, "notify_base_url", detail, "")

	// 回显新的回调地址:前端保存后直接把地址框刷新成生效后的值。
	user.NotifyBaseURL = base
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                  true,
		"notify_base_url":     base,
		"notify_base_default": p.defaultBase(),
		"notify_urls":         p.notifyURLMap(user),
	})
}

// maskChannelConfig 返回脱敏后的渠道配置(密钥只显示「已保存」标记)。
func maskChannelConfig(cfg ChannelConfig) map[string]any {
	m, err := configToMap(cfg)
	if err != nil {
		return map[string]any{"channel": cfg.Channel, "enabled": cfg.Enabled, "is_default": cfg.IsDefault}
	}
	masked := maskSecrets(m)
	masked["channel"] = cfg.Channel
	masked["enabled"] = cfg.Enabled
	masked["is_default"] = cfg.IsDefault
	return masked
}

// handleSaveChannel 保存某个渠道的配置。
//
// POST /api/channels/wechat
// {"enabled":true,"is_default":true,"wechat":{"app_id":"...","mch_id":"..."}}
//
// 需要二次验证票据(X-Verify-Token)。
func (p *Platform) handleSaveChannel(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	channel := channelFromRequest(r)
	if !isKnownChannel(channel) {
		writeError(w, http.StatusBadRequest, "未知渠道: "+channel)
		return
	}
	var incoming ChannelConfig
	if err := decodeJSON(r, &incoming); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	incoming.Channel = channel

	// 应用维度:带 app_id 时保存的是**该应用专属**的渠道配置
	//(没配过的渠道仍然回退到全局配置)。
	appID := strings.TrimSpace(r.URL.Query().Get("app_id"))
	if appID != "" {
		owned, err := p.store.AppByID(user.UserID, appID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if owned == nil {
			writeError(w, http.StatusNotFound, "应用不存在: "+appID)
			return
		}
	}

	// 敏感字段为空(前端拿到的是掩码)时保留已保存的值
	//
	// 注意:appID 非空时,existing 是**合并后**的视图(应用专属覆盖全局),
	// 所以第一次给应用存专属配置时,全局那套密钥会被一并复制进应用那行。
	// 这是有意为之:应用专属的行必须是一份**完整**的配置(合并逻辑是
	// 「整行覆盖」,不是逐字段覆盖),少一个密钥那套配置就用不了。
	// 代价是:之后全局换密钥,这个应用的专属配置不会跟着变 ——
	// 需要的话重新进这个应用的配置页保存一次即可。
	existing, err := p.effectiveChannelsFor(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	incomingMap, err := configToMap(incoming)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, cfg := range existing {
		if cfg.Channel != channel {
			continue
		}
		oldMap, err := configToMap(cfg)
		if err != nil {
			continue
		}
		incomingMap = mergeSecrets(oldMap, incomingMap)
		break
	}
	var merged ChannelConfig
	if err := configFromMap(incomingMap, &merged); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	merged.Channel = channel
	// 证书缓存目录由平台统一分配:商户自填目录等于给了「往任意目录写文件」的能力。
	if merged.Wechat != nil {
		merged.Wechat.CertDir = p.certDirFor(user.UserID, appID)
	}

	if appID != "" {
		if err := p.store.SaveAppChannel(user.UserID, appID, merged); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else if err := p.store.SaveChannel(user.UserID, merged); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.invalidateUser(user.UserID)
	// 注意 audit 的最后一个参数是**客户端 IP**,不是随便什么字符串。
	// 应用维度要记进 detail 里,不然审计日志里会出现「IP = app_xxx」这种鬼话。
	auditDetail := "保存渠道配置"
	if appID != "" {
		auditDetail += " 应用=" + appID
	}
	p.audit(r, user.UserID, auditChannelSave, channel, auditDetail, "")

	// 保存后立即尝试构建,尽早暴露配置错误
	if merged.Enabled {
		if _, err := buildProvider(merged, p.certDirFor(user.UserID, appID)); err != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":    false,
				"error": err.Error(),
			})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// effectiveChannelsFor 取某用户在指定应用下生效的渠道配置。
//
// appID 为空 = 全局配置;非空 = 应用专属覆盖全局之后的合并结果。
func (p *Platform) effectiveChannelsFor(userID, appID string) ([]ChannelConfig, error) {
	if appID == "" {
		return p.store.Channels(userID)
	}
	return p.store.EffectiveChannels(userID, appID)
}

// handleRevealChannel 不再提供渠道密钥的明文回显。
//
// 以前这个接口会把渠道配置(含密钥明文)整个返回给浏览器。密钥明文一旦
// 到了浏览器,就可能被截屏、被扩展插件读走、被代理缓存下来 ——
// 密钥在库里是密文,回显出来就等于白加密。
//
// 现在要换密钥只能「重新填写 + 保存」,配置是否可用用「测试连通性」验证。
//
// POST /api/channels/wechat/reveal
func (p *Platform) handleRevealChannel(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	p.audit(r, user.UserID, auditChannelReveal, channelFromRequest(r),
		"查看渠道密钥明文(功能已下线)", "")
	writeError(w, http.StatusGone,
		"出于安全考虑不再提供密钥明文回显:密钥在服务端加密保存,不会下发到浏览器。"+
			"如需更换密钥请重新填写后保存,并用「测试连通性」确认配置可用。")
}

// handleTestChannel 测试渠道连通性。
//
// POST /api/channels/wechat/test
func (p *Platform) handleTestChannel(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	channel := channelFromRequest(r)
	appID := strings.TrimSpace(r.URL.Query().Get("app_id"))
	if appID != "" {
		// 应用必须属于当前用户(和保存渠道配置时一样)。
		// 不校验的话,虽然按 user_id 过滤的数据不会泄露,
		// 但会拿别人的 app_id 去拼证书目录,凭空多出一堆没意义的目录。
		owned, err := p.store.AppByID(user.UserID, appID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if owned == nil {
			writeError(w, http.StatusNotFound, "应用不存在: "+appID)
			return
		}
	}
	channels, err := p.effectiveChannelsFor(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var target *ChannelConfig
	for i := range channels {
		if channels[i].Channel == channel {
			target = &channels[i]
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, "该渠道尚未配置")
		return
	}
	provider, err := buildProvider(*target, p.certDirFor(user.UserID, appID))
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	// 微信:尝试下载平台证书,验证商户私钥与 APIv3 密钥是否正确。
	//
	// 回包带上**实际请求的地址**:「测试连通性」最没用的一种形态就是
	// 只给一句「校验通过」——用户根本不知道平台替他去哪儿问了谁。
	if wcp, ok := provider.(*wechat.Provider); ok {
		base := wechat.DefaultBaseURL
		if target.Wechat != nil && target.Wechat.BaseURL != "" {
			base = target.Wechat.BaseURL
		}
		endpoint := strings.TrimRight(base, "/") + "/v3/certificates"
		if err := wcp.FetchPlatformCerts(r.Context()); err != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"ok": false, "error": err.Error(), "endpoint": endpoint,
			})
			return
		}
		p.audit(r, user.UserID, auditChannelTest, channel,
			"渠道校验通过 endpoint="+endpoint, "")
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"message":  "微信渠道校验通过(平台证书已下载)",
			"endpoint": endpoint,
		})
		return
	}
	// 支付宝:拿一个不存在的单号去查一次。
	//
	// 「查不到这个单号」恰恰说明链路是通的:网关能通、签名也对
	// (密钥格式、AppID 填错的话,这里会直接报签名 / 应用错误)。
	// 只做本地格式校验的话,给的是一种假的安全感。
	if alp, ok := provider.(*alipay.Provider); ok {
		gateway := alipay.DefaultGateway
		if target.Alipay != nil && target.Alipay.Gateway != "" {
			gateway = target.Alipay.Gateway
		}
		endpoint := gateway
		testTradeNo := "TEST" + fmt.Sprint(time.Now().UnixNano())
		_, err := alp.Query(r.Context(), testTradeNo)
		switch {
		case err == nil:
			p.audit(r, user.UserID, auditChannelTest, channel,
				"渠道校验通过 endpoint="+endpoint, "")
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":       true,
				"message":  "支付宝网关可达、签名正确",
				"endpoint": endpoint,
			})
		case errors.Is(err, payment.ErrOrderNotFound):
			p.audit(r, user.UserID, auditChannelTest, channel,
				"渠道校验通过 endpoint="+endpoint, "")
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":       true,
				"message":  "支付宝网关可达、签名正确(测试单号 " + testTradeNo + " 不存在,属正常)",
				"endpoint": endpoint,
			})
		default:
			writeJSON(w, http.StatusOK, map[string]any{
				"ok": false, "error": err.Error(), "endpoint": endpoint,
			})
			return
		}
		return
	}
	p.audit(r, user.UserID, auditChannelTest, channel, "渠道校验通过", "")
	// 钱包渠道这边只做了本地校验(密钥能不能解析、必填项齐不齐),并没有联调收单机构。
	// 文案要说实话,不然「测试连通性」通过给了用户一个假的安全感。
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "渠道配置校验通过(仅本地校验)"})
}

// payRequest 发起支付的请求体。
type payRequest struct {
	Channel    string `json:"channel"`
	TradeType  string `json:"trade_type"`
	OutTradeNo string `json:"out_trade_no"`
	Subject    string `json:"subject"`
	Amount     int64  `json:"amount"`
	PayerID    string `json:"payer_id"`
	ClientIP   string `json:"client_ip"`
	Attach     string `json:"attach"`
	NotifyURL  string `json:"notify_url"`
	// AppID 订单归属的应用(空 = 未分类,用全局渠道配置)。
	//
	// 有了它才能「按产品分开看订单、分开统计、分开出报表」;
	// 同一个商户的不同产品也能各自配一套渠道密钥(不同的微信 AppID / 商户号)。
	AppID string `json:"app_id"`
}

// handlePay 发起支付。
//
// POST /api/pay(需要二次验证票据)
func (p *Platform) handlePay(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req payRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.corePay(r, user, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// qrcodeURL 生成二维码图片地址。
func qrcodeURL(content string) string {
	if content == "" {
		return ""
	}
	return "/api/qrcode?content=" + url.QueryEscape(content)
}

// usageGuide 返回面向用户的接入说明。
func usageGuide(channel, tradeType string) []string {
	switch channel {
	case string(payment.ChannelWechat):
		switch tradeType {
		case string(payment.TradeTypeApp):
			return []string{
				"APP 端:把 session.app_params 原样传给微信 SDK 的 pay() 方法。",
				"字段含义:appid / partnerid / prepayid / package / noncestr / timestamp / sign。",
				"服务端收到回调后调用 /api/orders 查询订单状态。",
			}
		case string(payment.TradeTypeJSAPI):
			return []string{
				"公众号/小程序:把 session.app_params 传给 wx.requestPayment。",
				"字段含义:appId / timeStamp / nonceStr / package / signType / paySign。",
			}
		case string(payment.TradeTypeH5):
			return []string{
				"H5 端:直接跳转 session.redirect_url 即可完成支付。",
			}
		default:
			return []string{
				"扫码支付:把 session.qrcode 渲染成二维码图片给用户扫码。",
				"二维码图片地址:session.qrcode_url。",
				"用户在微信「扫一扫」完成付款后,平台会自动收到异步回调。",
			}
		}
	case string(payment.ChannelAlipay):
		switch tradeType {
		case string(payment.TradeTypeApp):
			return []string{
				"APP 端:把 session.app_params.orderString 交给支付宝 SDK 调起。",
			}
		default:
			return []string{
				"扫码支付(当面付):把 session.qrcode 渲染成二维码给用户扫码。",
				"二维码图片地址:session.qrcode_url。",
				"网页支付:直接跳转 session.redirect_url。",
			}
		}
	default:
		return []string{
			"钱包支付:把 session.token(client_secret)与 session.app_params 交给客户端钱包 SDK。",
			"客户端完成支付后回传支付令牌,服务端再调用收单网关完成扣款。",
		}
	}
}

// handleRefund 发起退款。
//
// POST /api/refund {"out_trade_no":"...","out_refund_no":"...","refund_amount":0}
//
// refund_amount 为 0 表示全额退款。
//
// 平台自己记账:退款金额要受「订单金额」和「已退累计」双重约束,
// 否则「退 1 元、再退 1 元」能一直退到超过订单金额。
func (p *Platform) handleRefund(w http.ResponseWriter, r *http.Request) {
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
		Channel      string `json:"channel"`
		OutTradeNo   string `json:"out_trade_no"`
		OutRefundNo  string `json:"out_refund_no"`
		RefundAmount int64  `json:"refund_amount"`
		Reason       string `json:"reason"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreRefund(r, user, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleClose 关闭订单。
//
// POST /api/close {"out_trade_no":"..."}
func (p *Platform) handleClose(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	// 关单会影响渠道侧的资金状态,和退款一样要求二次验证票据。
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		Channel    string `json:"channel"`
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if err := p.coreClose(r, user, req); err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleWalletCharge 钱包渠道扣款(Apple Pay / Google Pay 的最后一步)。
//
// POST /api/wallet/charge
//
//	{"out_trade_no":"...","channel":"applepay","payment_token":"...","amount":19900}
//
// 流程:客户端先用 /api/pay 拿到钱包会话参数 → 用户在系统钱包里授权(Face ID / 密码)
// → 客户端把支付令牌回传到这里 → 服务端用令牌完成真实扣款。
//
// 客户端拿到的「已授权」不等于支付成功,最终入账以渠道异步回调为准。
func (p *Platform) handleWalletCharge(w http.ResponseWriter, r *http.Request) {
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
		Channel      string `json:"channel"`
		OutTradeNo   string `json:"out_trade_no"`
		PaymentToken string `json:"payment_token"`
		Amount       int64  `json:"amount"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreWalletCharge(r, user, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleQuery 查询订单状态(向渠道侧查询后返回最新状态)。
//
// POST /api/query {"out_trade_no":"...","channel":"wechat"}
func (p *Platform) handleQuery(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Channel    string `json:"channel"`
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreQuery(r, user, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOrdersAPI 返回当前用户的订单列表(支持筛选与分页)。
//
// GET /api/orders?page=1&size=20&status=SUCCESS&channel=wechat&keyword=xxx&from=&to=
func (p *Platform) handleOrdersAPI(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	filter := parseOrderFilter(r)
	orders, total, err := p.store.OrdersPage(user.UserID, filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	page, size := normalizePage(filter.Page, filter.Size)
	writeJSON(w, http.StatusOK, map[string]any{
		"orders": orders,
		"page":   page,
		"size":   size,
		"total":  total,
		"pages":  totalPages(total, size),
	})
}

// handleRefundsAPI 返回当前用户的退款流水。
//
// GET /api/refunds?limit=100
func (p *Platform) handleRefundsAPI(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	limit := intParam(r, "limit", 100)
	refunds, err := p.store.Refunds(user.UserID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"refunds": refunds})
}

// handleStatsAPI 返回当前用户的交易统计。
//
// GET /api/stats
// GET /api/stats?app_id=xxx            只看某个应用
// GET /api/stats?group_by=app|channel|day  按维度分组(报表)
//
// group_by 为空时返回原来的统计对象(前端概览页用),不改变老行为;
// 带 group_by 时返回 {stats, group_by, groups}。
func (p *Platform) handleStatsAPI(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	appID := strings.TrimSpace(q.Get("app_id"))
	if appID != "" {
		// 应用必须属于当前用户,否则会看到一个空统计(而且暴露「别人的 app_id 存在」)。
		app, err := p.store.AppByID(user.UserID, appID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if app == nil {
			writeError(w, http.StatusBadRequest, "应用不存在: "+appID)
			return
		}
	}
	stats, err := p.store.StatsForApp(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	groupBy := strings.TrimSpace(q.Get("group_by"))
	if groupBy == "" {
		writeJSON(w, http.StatusOK, stats)
		return
	}
	groups, truncated, err := p.store.StatGroups(user.UserID, groupBy, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":    stats,
		"group_by": normalizeGroupBy(groupBy),
		"groups":   groups,
		// 被截断时前端要显式提示,不能给一份看着完整的数据。
		"truncated": truncated,
	})
}

// handleAuditLogs 返回当前用户的审计日志(支持分页与按动作过滤)。
//
// GET /api/audit-logs?page=1&size=50&action=account.login
func (p *Platform) handleAuditLogs(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	page := intParam(r, "page", 1)
	size := intParam(r, "size", 50)
	action := strings.TrimSpace(r.URL.Query().Get("action"))
	logs, total, err := p.store.AuditLogsPage(user.UserID, size, page, action)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs":  logs,
		"page":  page,
		"size":  size,
		"total": total,
		"pages": totalPages(total, size),
	})
}

// handleQRCode 输出二维码 PNG。
//
// GET /api/qrcode?content=...
//
// 三道防滥用:
//   - **限流**:渲染一张二维码要 5~35 ms CPU,循环请求就能把 CPU 打满。
//   - **长度上限**:content 超过二维码容量时直接拒,不给「超长输入」烧 CPU 的机会。
//   - **服务端缓存**:同一个 content 反复渲染是纯浪费,缓存住直接返回。
func (p *Platform) handleQRCode(w http.ResponseWriter, r *http.Request) {
	content := r.URL.Query().Get("content")
	if content == "" {
		writeError(w, http.StatusBadRequest, "缺少 content 参数")
		return
	}
	// 二维码字节模式最多 666 字节(版本 20)。超长的直接拒掉:
	// 继续渲染既没意义(扫不出东西),又白烧 CPU。
	if len(content) > qrcodeMaxContentBytes {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("二维码内容过长(最多 %d 字节)", qrcodeMaxContentBytes))
		return
	}
	ip := p.clientIPOf(r)
	if !p.qrcodeLimiter.allow("qrcode:"+ip, qrcodeRateLimitPerIP, time.Minute) {
		writeRateLimited(w, "二维码请求过于频繁,请稍后再试")
		return
	}
	data, ok := p.qrcodeCache.get(content)
	if !ok {
		rendered, err := qrcode.PNG(content, 8)
		if err != nil {
			writeError(w, http.StatusBadRequest, "二维码内容无法编码: "+err.Error())
			return
		}
		p.qrcodeCache.put(content, rendered)
		data = rendered
	}
	w.Header().Set("Content-Type", "image/png")
	// 二维码内容是**支付链接**,不能被共享缓存长期持有;但同一个链接短时间内
	// 反复取是正常的(用户刷新页面),给个短 TTL。
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.Write(data)
}

// qrcodeMaxContentBytes 二维码内容最大字节数。
const qrcodeMaxContentBytes = 666

// handleNotify 接收支付平台异步回调。
//
// POST /notify/{channel}/{user_id}
//
// 回调地址里带 user_id,平台据此找到该商户的渠道配置完成验签。
func (p *Platform) handleNotify(w http.ResponseWriter, r *http.Request) {
	channel := r.PathValue("channel")
	userID := r.PathValue("user_id")
	// 应用维度:回调地址里带了 app_id,就用**该应用**的渠道配置验签。
	//
	// 不能一律用全局配置:一个商户的不同产品可能用不同的微信 AppID / 商户号,
	// 验签必须用当初下单那一套,否则订单状态永远停在「待支付」。
	appID := strings.TrimSpace(r.PathValue("app_id"))
	body := readBody(r)

	providers, err := p.appProviders(userID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		writeError(w, http.StatusBadRequest, "渠道 "+channel+" 未启用或配置无效")
		return
	}
	headers := make(map[string]string, len(r.Header))
	for key := range r.Header {
		headers[key] = r.Header.Get(key)
	}
	query := make(map[string]string, len(r.URL.Query()))
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			query[key] = values[0]
		}
	}
	notify, err := provider.ParseNotify(r.Context(), &payment.NotifyRequest{
		Method:  r.Method,
		URL:     r.URL.String(),
		Headers: headers,
		Body:    body,
		Query:   query,
	})
	if err != nil {
		p.opts.Logger.Printf("回调验签失败(%s/%s): %v", channel, userID, err)
		writeError(w, http.StatusBadRequest, "验签失败: "+err.Error())
		return
	}
	if notify.OutTradeNo == "" {
		writeError(w, http.StatusBadRequest, "回调缺少 out_trade_no")
		return
	}
	// 1) 回调的订单必须是本商户的订单(回调地址里带 user_id,回调的验签
	//    已经用该商户的渠道配置做过,这里再确认一次归属)。
	record, err := p.store.OrderByTradeNo(userID, notify.OutTradeNo)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if record == nil {
		p.opts.Logger.Printf("回调订单不存在(%s/%s): %s", channel, userID, notify.OutTradeNo)
		writeError(w, http.StatusNotFound, "订单不存在: "+notify.OutTradeNo)
		return
	}
	// 1.5) 回调地址里的应用标识必须与订单归属的应用一致。
	//
	// 地址是平台在下单时生成的,渠道原样回调回来 —— 两者对不上说明
	// 「这笔单用的密钥」和「验签用的密钥」不是同一套,验签即使过了也不能算数。
	if record.AppID != appID {
		p.opts.Logger.Printf("回调应用不匹配(%s/%s/%s): 订单 %s 属于应用 %s",
			channel, userID, appID, notify.OutTradeNo, record.AppID)
		writeError(w, http.StatusBadRequest, "回调地址与订单归属的应用不一致")
		return
	}
	// 2) 金额必须与订单一致:金额对不上,说明这不是这一笔单的回调。
	//    这种单子绝不能写进账 —— 写了就是「订单显示已支付,但付的不是这个金额」。
	if notify.Amount > 0 && notify.Amount != record.Amount {
		p.opts.Logger.Printf("回调金额不一致(%s/%s): 订单 %s 元,回调 %s 元",
			channel, userID, record.AmountYuan, payment.CentsToYuan(notify.Amount))
		writeError(w, http.StatusBadRequest, "回调金额与订单金额不一致")
		return
	}
	// 3) 应用号 / 商户号必须是自己配的那一套:验签只能证明报文没被篡改,
	//    不能证明「这笔钱是付给这个商户的」。
	if err := checkNotifyMerchant(provider, notify); err != nil {
		p.opts.Logger.Printf("回调商户校验失败(%s/%s): %v", channel, userID, err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 4) 状态机:状态只能向前走。一次迟到的回调不能把已退款的订单改回「成功」。
	//
	//    写回用**条件更新**:读到什么状态,就只在「状态还是那个」时写。
	//    并发的关单 / 查单不会把「钱到账了」这个结果盖掉
	//    (以前是读-改-写,关单正好插在中间就会写成已关闭)。
	//
	//    钱包渠道的退款要分「全额 / 部分」:Stripe 的 charge.refunded 事件里
	//    amount 是**原扣款金额**,amount_refunded 才是已经退了多少。
	//    只退了一部分却把订单标成「已退款」,报表口径就是错的。
	notifyStatus := notify.Status
	if notifyStatus == payment.TradeStatusRefunded {
		if raw := notify.Raw["amount_refunded"]; raw != "" {
			if refunded, perr := strconv.ParseInt(raw, 10, 64); perr == nil &&
				refunded > 0 && refunded < record.Amount {
				notifyStatus = payment.TradeStatusPartiallyRefunded
			}
		}
	}
	notifyRaw := ""
	if raw, merr := json.Marshal(notify); merr == nil {
		notifyRaw = string(raw)
	}
	effective, err := p.applyOrderStatus(userID, notify.OutTradeNo,
		func(current string) (string, bool) {
			next, ok := advanceStatus(payment.TradeStatus(current), notifyStatus)
			if !ok {
				p.opts.Logger.Printf("回调状态未生效: 订单 %s 当前 %s,回调 %s",
					notify.OutTradeNo, current, notifyStatus)
			}
			return string(next), true
		},
		func(current string) map[string]any {
			out := map[string]any{}
			if notify.TransactionID != "" {
				out["transaction_id"] = notify.TransactionID
			}
			if notifyRaw != "" {
				out["notify_raw"] = notifyRaw
			}
			return out
		})
	if err != nil {
		p.opts.Logger.Printf("回调写回订单状态失败(%s/%s): %v", channel, userID, err)
		writeError(w, http.StatusConflict, "订单状态更新失败: "+err.Error())
		return
	}
	p.opts.Logger.Printf("收到回调 %s 订单 %s 状态 %s", channel, notify.OutTradeNo, effective)
	// 支付宝要求回**纯文本 success**(正好 7 个字符,不能带空格换行、不能是 JSON),
	// 否则它认为通知没收到:先立刻重发 3 次,再按 4m/10m/10m/1h/2h/6h/15h 重发,
	// 跨度 24 小时 22 分。官方文档:opendocs.alipay.com/support/01raw4
	// 微信/Stripe 只看 HTTP 状态码,回 JSON 即可。
	if payment.Channel(channel) == payment.ChannelAlipay {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// checkNotifyMerchant 校验回调里的应用号 / 商户号是不是本商户自己配的那一套。
//
// 为什么要查:验签只保证报文没被篡改,不保证「钱付给了这个商户」。
// 商户配了多个应用、或回调被重放时,没有这道校验就会记错账。
func checkNotifyMerchant(provider payment.Provider, notify *payment.Notify) error {
	switch p := provider.(type) {
	case *wechat.Provider:
		if got := notify.Raw["appid"]; got != "" && got != p.AppID() {
			return fmt.Errorf("回调 appid(%s)与本商户配置(%s)不一致", got, p.AppID())
		}
		if got := notify.Raw["mchid"]; got != "" && got != p.MchID() {
			return fmt.Errorf("回调 mchid(%s)与本商户配置(%s)不一致", got, p.MchID())
		}
	case *alipay.Provider:
		if got := notify.Raw["app_id"]; got != "" && got != p.AppID() {
			return fmt.Errorf("回调 app_id(%s)与本商户配置(%s)不一致", got, p.AppID())
		}
	}
	return nil
}

// readBody 读取请求体。
func readBody(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
		if len(buf) > 4<<20 {
			break
		}
	}
	return buf
}

// isKnownChannel 判断渠道是否受支持。
func isKnownChannel(channel string) bool {
	switch channel {
	case string(payment.ChannelWechat), string(payment.ChannelAlipay),
		string(payment.ChannelApplePay), string(payment.ChannelGooglePay):
		return true
	}
	return false
}

// channelFromRequest 从请求里取渠道标识。
//
// r.PathValue("channel") 只有经过 ServeMux 路由时才有值。直接调用
// handler(单测、或者平台挂在别的路由上)时它是空的 —— 于是渠道变成
// 空串,不是「未知渠道」,而是后面拿空渠道去查配置、去构建 Provider,
// 报出来的错跟真实原因完全对不上。
//
// 所以按「路径值 → 查询参数 → 从路径本身解析」的顺序再找一遍,
// 和应用维度那几个 handler 的兜底顺序保持一致。
func channelFromRequest(r *http.Request) string {
	if v := strings.TrimSpace(r.PathValue("channel")); v != "" {
		return v
	}
	if v := strings.TrimSpace(r.URL.Query().Get("channel")); v != "" {
		return v
	}
	return pathSegment(r.URL.Path, "/api/channels/")
}

// audit 写审计日志(失败只记日志,不阻断业务)。
func (p *Platform) audit(r *http.Request, userID, action, target, detail, clientIP string) {
	if clientIP == "" {
		clientIP = p.clientIPOf(r)
	}
	if err := p.store.AddAudit(userID, action, target, detail, clientIP); err != nil {
		p.opts.Logger.Printf("写审计日志失败: %v", err)
	}
}
