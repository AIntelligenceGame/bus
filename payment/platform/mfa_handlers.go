package platform

import (
	"net/http"
	"strings"
	"time"
)

// 二次验证的补充接口:TOTP 绑定准备、验证码下发通道配置。
//
// 这一组接口解决两个「看起来是功能、其实没通」的坑:
//
//  1. TOTP 绑定需要「先生成密钥 → 用户扫码 → 输入动态码 → 启用」四步,
//     中间的密钥必须由服务端生成并加密暂存,不能让前端自己编一个。
//  2. 邮件/短信验证码要有真能发出去的下发通道。通道参数(授权码、网关令牌)
//     是敏感信息,加密入库,读取接口只回显「配没配」。

// handleTOTPSetup 生成 TOTP 密钥,供验证器 App 扫码绑定。
//
// POST /api/account/mfa/totp/setup
// body(可选):{"current_code":"..."} —— 换绑时的「当前绑定确认」。
//
// 返回 secret(base32)、otpauth URI 与二维码地址。
// 密钥在**本次会话内**暂存于服务端(加密),启用时用同一份密钥校验动态码。
//
// **已经绑定过二次验证的账户,这一步也要当前绑定的人确认**:
// 「重新生成密钥」会让旧绑定失效,拿到口令的人只要在别人的页面
// 点一下就能把密钥换成自己那份 —— 正主的验证器当场作废,人也被挡在门外。
// 所以这里必须带上旧验证器 App 的动态码(或发到已绑定地址的验证码)。
func (p *Platform) handleTOTPSetup(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if !p.mfaLimiter.allow("totp:"+user.UserID, mfaVerifyRateLimitPerUser, time.Minute) {
		writeError(w, http.StatusTooManyRequests, "操作过于频繁，请稍后再试")
		return
	}
	// 请求体可选:首绑时什么都不用带。
	var req struct {
		CurrentCode string `json:"current_code"`
	}
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	// 换绑确认:先把「你确实是当前绑定的人」这件事确认掉,
	// 再把新密钥发出去。
	if user.MFAEnabled {
		if err := p.requireCurrentBinding(user, req.CurrentCode, false); err != nil {
			p.audit(r, user.UserID, auditAccountMFA, "totp-setup", "生成密钥被拒:未通过当前绑定确认", "")
			writeError(w, http.StatusForbidden, err.Error())
			return
		}
	}
	secret, err := generateTOTPSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 暂存:先加密存起来,启用时按这个密钥校验动态码。
	// 存在「待确认」列而不是生效列 —— 用户点一下「生成密钥」不该把
	// 正在用的绑定冲掉(否则旧 App 立刻失效,账户直接进不去)。
	if err := p.store.SetMFATOTPPendingSecret(user.UserID, secret); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	uri := totpURI(secret, user.UserName)
	writeJSON(w, http.StatusOK, map[string]any{
		"secret": secret,
		"uri":    uri,
		// 二维码走平台自己的接口渲染(前端用带令牌的请求取,令牌不进 URL)。
		"qrcode_url": qrcodeURL(uri),
		"digits":     totpDigits,
		"period":     totpPeriod,
	})
}

// handleMFAChannels 读取当前账户的验证码下发通道配置。
//
// GET /api/account/mfa/channels
//
// 口令 / 授权码 / 网关令牌**不回显**(只回显是否已配置)。
func (p *Platform) handleMFAChannels(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	out := map[string]any{}
	for _, channel := range []string{mfaChannelEmail, mfaChannelSMS} {
		raw, err := p.store.loadMFAChannel(user.UserID, channel)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if raw == nil {
			out[channel] = map[string]any{"configured": false}
			continue
		}
		item := map[string]any{"configured": true}
		for k, v := range raw {
			// 敏感字段只回显「有没有」,不回显内容。
			switch k {
			case "password", "token":
				if s, ok := v.(string); ok && s != "" {
					item[k+"_set"] = true
				} else {
					item[k+"_set"] = false
				}
			default:
				item[k] = v
			}
		}
		out[channel] = item
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": out})
}

// handleSaveMFAChannel 保存验证码下发通道配置。
//
// POST /api/account/mfa/channels {"channel":"email","config":{...}}
//
// 需要敏感操作票据(「口令 + 验证码」换取的短时票据)。
// body 里的 "password" 是可选的:带了就再校验一次,不带就只认票据。
//
// 口令 / 令牌留空表示「保持原值不变」(前端拿到的是掩码,不会回传明文)。
func (p *Platform) handleSaveMFAChannel(w http.ResponseWriter, r *http.Request) {
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
		Channel  string         `json:"channel"`
		Config   map[string]any `json:"config"`
		Password string         `json:"password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if req.Password != "" && (user.PasswordHash == "" || !verifyPassword(user.PasswordHash, req.Password)) {
		writeCredentialError(w, "口令不正确")
		return
	}
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	switch channel {
	case mfaChannelEmail, mfaChannelSMS:
	default:
		writeError(w, http.StatusBadRequest, "channel 只能是 email 或 sms")
		return
	}
	cfg := req.Config
	if cfg == nil {
		cfg = map[string]any{}
	}
	// 敏感字段留空 => 保留旧值(和渠道密钥的处理一致)。
	old, err := p.store.loadMFAChannel(user.UserID, channel)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, key := range []string{"password", "token"} {
		cur, _ := cfg[key].(string)
		if strings.TrimSpace(cur) != "" {
			continue
		}
		if old == nil {
			continue
		}
		prev, ok := old[key].(string)
		if !ok || prev == "" {
			continue
		}
		cfg[key] = prev
	}
	// 基本校验:邮件通道至少要有服务器地址,短信通道至少要有网关地址。
	switch channel {
	case mfaChannelEmail:
		host, _ := cfg["host"].(string)
		if strings.TrimSpace(host) == "" {
			writeError(w, http.StatusBadRequest, "请填写 SMTP 服务器地址")
			return
		}
		// 地址当场校验(只查协议/主机名/IP 字面量,不查 DNS):
		// 平台会替商户去连这个地址,内网地址一律拒绝
		// (否则等于给了「借平台探测内网」的跳板)。
		if err := p.outbound.validateSMTPHostSyntax(host); err != nil {
			writeError(w, http.StatusBadRequest, "SMTP 服务器地址不可用: "+err.Error())
			return
		}
	case mfaChannelSMS:
		url, _ := cfg["url"].(string)
		if strings.TrimSpace(url) == "" {
			writeError(w, http.StatusBadRequest, "请填写短信网关地址")
			return
		}
		if _, err := p.outbound.validateOutboundURLSyntax(url); err != nil {
			writeError(w, http.StatusBadRequest, "短信网关地址不可用: "+err.Error())
			return
		}
	}
	if err := p.store.saveMFAChannel(user.UserID, channel, cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountMFA, "channel."+channel, "保存验证码下发通道", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "configured": true})
}

// handleDeleteMFAChannel 删除验证码下发通道配置。
//
// POST /api/account/mfa/channels/delete {"channel":"email"}
func (p *Platform) handleDeleteMFAChannel(w http.ResponseWriter, r *http.Request) {
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
		Channel  string `json:"channel"`
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
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	switch channel {
	case mfaChannelEmail, mfaChannelSMS:
	default:
		writeError(w, http.StatusBadRequest, "channel 只能是 email 或 sms")
		return
	}
	if err := p.store.deleteMFAChannel(user.UserID, channel); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAccountMFA, "channel."+channel, "删除验证码下发通道", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleTestMFAChannel 测试验证码下发通道(真的发一封/一条出去)。
//
// POST /api/account/mfa/channels/test {"channel":"email","target":"..."}
//
// 有了这个按钮,用户才能确认「通道配好了」而不是「保存成功了但收不到」。
func (p *Platform) handleTestMFAChannel(w http.ResponseWriter, r *http.Request) {
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
		Channel  string `json:"channel"`
		Target   string `json:"target"`
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
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	if channel != mfaChannelEmail && channel != mfaChannelSMS {
		writeError(w, http.StatusBadRequest, "channel 只能是 email 或 sms")
		return
	}
	target := strings.TrimSpace(req.Target)
	if target == "" {
		target = user.MFATarget
	}
	if target == "" {
		writeError(w, http.StatusBadRequest, "请填写测试接收地址")
		return
	}
	// 先验格式:邮件通道填个手机号进来,SMTP 那边只会回一句
	// 「missing '@' or angle-addr」,用户根本看不懂是自己填错了。
	if err := validateMFATarget(channel, target); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	code, err := generateMFACode()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	delivered, err := p.sendMFACode(r, user.UserID, channel, target, code)
	if err != nil {
		writeError(w, http.StatusBadGateway, "测试发送失败: "+err.Error())
		return
	}
	if !delivered {
		writeError(w, http.StatusBadGateway,
			"该通道尚未配置(或只写进了服务器日志),实际收不到;请先保存通道配置")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"target":  maskTarget(target),
		"message": "测试验证码已发送,请查收",
	})
}
