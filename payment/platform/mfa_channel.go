package platform

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/AIntelligenceGame/gapi/mail"
)

// 验证码下发通道(每个账户自己配)。
//
// 为什么要有这一层:
//   - 平台默认的 MFASender 是 LogMFASender —— 只把验证码打进日志,
//     「能收到验证码」这件事根本没发生,用户却在页面上看到「验证码已发送」。
//   - 短信通道各家厂商不一样,与其在平台里写死某一家,不如让每个账户
//     填自己的通道参数(SMTP / HTTP 网关),平台只负责按配置发送。
//
// 配置**加密入库**(SecretBox),口令 / 授权码 / 网关令牌都存密文,
// 读取接口只回显「是否已配置」,不回显明文。

const (
	// mfaChannelEmail 邮件通道(SMTP)。
	mfaChannelEmail = "email"
	// mfaChannelSMS 短信通道(HTTP 网关)。
	mfaChannelSMS = "sms"
)

// aadMFAChannel 验证码通道配置的附加认证数据(与渠道配置分开,防止密文互相搬运)。
const aadMFAChannel = "pay-platform/mfa-channel/v1"

// SMTPChannelConfig SMTP 邮件通道配置。
type SMTPChannelConfig struct {
	// Host SMTP 服务器地址(如 smtp.qq.com)。
	Host string `json:"host"`
	// Port SMTP 端口(QQ/163 的 465 是隐式 TLS,587 是 STARTTLS)。
	Port int `json:"port"`
	// Username 登录账号(QQ 邮箱是整个邮箱地址)。
	Username string `json:"username"`
	// Password 授权码 / 密码(加密入库,永不回显)。
	Password string `json:"password"`
	// FromAddr 发件人地址(留空则用 Username)。
	FromAddr string `json:"from_addr"`
	// TLSMode 加密方式(页面下拉框选择):
	//
	//	"starttls" —— 先明文握手再升级加密(QQ 587 / 163 25);
	//	"tls"     —— 隐式 TLS,连接即加密(QQ/163 465);
	//	"none"    —— 不加密(仅内网自建服务器)。
	TLSMode string `json:"tls_mode"`
	// TLS 旧字段(早期配置用的是它):TLSMode 为空时按端口推断。
	TLS bool `json:"tls"`
}

// mode 归一化 SMTP 加密方式。
//
// 没显式指定时按端口推断:465/994 是隐式 TLS,其余按 STARTTLS 处理。
func (c SMTPChannelConfig) mode() string {
	switch strings.ToLower(strings.TrimSpace(c.TLSMode)) {
	case "tls", "ssl", "implicit":
		return "tls"
	case "none", "off", "plain":
		return "none"
	case "starttls", "":
		if c.Port == 465 || c.Port == 994 {
			return "tls"
		}
		return "starttls"
	default:
		return "starttls"
	}
}

// HTTPGatewayConfig 短信 / HTTP 网关通道配置。
type HTTPGatewayConfig struct {
	// URL 网关地址,平台会 POST {"code":"...","target":"..."}。
	URL string `json:"url"`
	// Token 访问令牌(加密入库,永不回显)。
	Token string `json:"token"`
	// CodeField 网关取验证码的字段名,默认 "code"。
	CodeField string `json:"code_field"`
	// TargetField 网关取接收地址的字段名,默认 "target"。
	TargetField string `json:"target_field"`
}

// mfaChannelConfig 一个账户的通道配置(邮件 + 短信各一份)。
type mfaChannelConfig struct {
	Email *SMTPChannelConfig `json:"email,omitempty"`
	SMS   *HTTPGatewayConfig `json:"sms,omitempty"`
}

// ---------------------------------------------------------------- 发送调度

// sendMFACode 把验证码发给 target。
//
// 优先用**账户自己配的通道**(SMTP / 短信网关):
// 平台内置的 LogMFASender 只把码写进日志,那不叫「发送」——
// 用户在页面上看到「验证码已发送」,邮箱里却什么都没有。
//
// delivered 返回 false 表示「只写进了日志,用户实际收不到」,
// 调用方要如实告诉用户,别让人对着一个永远收不到的信封干等。
func (p *Platform) sendMFACode(r *http.Request, userID, mfaType, target, code string) (bool, error) {
	sender, err := p.mfaSender(userID, mfaType)
	if err != nil {
		return false, err
	}
	if err := sender.Send(r.Context(), mfaType, target, code); err != nil {
		return false, err
	}
	if _, isLog := sender.(LogMFASender); isLog {
		return false, nil
	}
	return true, nil
}

// mfaSender 解析某个账户某个类型(邮件 / 短信)的下发通道。
//
// 账户没配通道时退回平台默认通道(LogMFASender 或使用方注入的实现)。
func (p *Platform) mfaSender(userID, mfaType string) (MFASender, error) {
	switch mfaType {
	case mfaChannelEmail:
		raw, err := p.store.loadMFAChannel(userID, mfaChannelEmail)
		if err != nil {
			return nil, err
		}
		if raw != nil {
			var cfg SMTPChannelConfig
			if err := configFromMap(raw, &cfg); err != nil {
				return nil, err
			}
			if strings.TrimSpace(cfg.Host) != "" {
				return smtpSender{cfg: cfg, policy: p.outbound}, nil
			}
		}
	case mfaChannelSMS:
		raw, err := p.store.loadMFAChannel(userID, mfaChannelSMS)
		if err != nil {
			return nil, err
		}
		if raw != nil {
			var cfg HTTPGatewayConfig
			if err := configFromMap(raw, &cfg); err != nil {
				return nil, err
			}
			if strings.TrimSpace(cfg.URL) != "" {
				return gatewaySender{cfg: cfg, policy: p.outbound, logger: p.opts.Logger}, nil
			}
		}
	}
	if p.opts.MFASender == nil {
		return LogMFASender{Logger: p.opts.Logger}, nil
	}
	return p.opts.MFASender, nil
}

// mfaChannelReady 判断该账户的验证码**能不能真的发出去**。
//
// 返回 (true, nil) 表示能发;(false, nil) 表示只能写进日志 ——
// 也就是「用户永远收不到」;(false, err) 表示判断本身出错(数据库问题),
// 这种情况下要**保守处理**(宁可当成有通道),不能因为查库失败就放水。
func (p *Platform) mfaChannelReady(userID, mfaType string) (bool, error) {
	sender, err := p.mfaSender(userID, mfaType)
	if err != nil {
		return false, err
	}
	if _, isLog := sender.(LogMFASender); isLog {
		// 只写日志的通道:默认**不算**「有通道」——
		// 用户在页面上点了发送,邮箱里什么都没有,而开启二次验证之后
		// 登录又要这个码,账户就被锁在门外了。
		//
		// 开发 / 内网联调可以显式打开 Options.AllowLogOnlyMFA,
		// 让平台方直接从服务器日志里取码。
		return p.opts.AllowLogOnlyMFA, nil
	}
	return true, nil
}

// ensureMFAChannelReady 检查该账户有没有**能用的**验证码下发通道。
//
// 邮箱 / 短信方式如果只能落到日志,就不该让用户把二次验证开起来:
// 码在服务器日志里,用户永远收不到 —— 而开启之后登录就需要这个码,
// 等于把自己锁在门外(只能找平台方翻日志救)。
//
// 两种情况算「有通道」:
//   - 账户自己配了 SMTP / 短信网关(页面里配,加密入库);
//   - 平台方注入了真正的下发器(Options.MFASender 不是 LogMFASender)。
//
// 验证器 App(TOTP)根本不需要通道,不走这个检查。
func (p *Platform) ensureMFAChannelReady(userID, mfaType string) error {
	ready, err := p.mfaChannelReady(userID, mfaType)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}
	return fmt.Errorf("%s;开启后登录就要这个码,等于把账户锁在门外", mfaChannelMissingHint(mfaType))
}

// mfaChannelMissingHint 生成「通道没配」时的统一提示。
//
// 同一个意思在好几个地方出现(发码、绑定、登录、校验票据),
// 文案统一放在这里,免得各处说法不一致,用户以为说的是不同的事。
func mfaChannelMissingHint(mfaType string) string {
	what := "短信"
	if mfaType == mfaChannelEmail {
		what = "邮件"
	}
	return "尚未配置" + what + "下发通道:验证码只会写进服务器日志,实际收不到," +
		"所以平台不会给你一个收不到的通知;请先在「账户安全 → 验证码下发通道」里配置" +
		"(配完点「发送测试邮件」实测),或改用验证器 App(不需要任何通道)"
}

// validateMFATarget 校验接收地址格式。
//
// 格式不对的地址会让验证码「发不出去」,而且往往查半天查不出原因
// (SMTP 会直接拒收,短信网关会报「号码不存在」),所以在绑定前就拦下来。
func validateMFATarget(mfaType, target string) error {
	t := strings.TrimSpace(target)
	if t == "" {
		return fmt.Errorf("请填写验证码接收地址")
	}
	switch mfaType {
	case MFATypeEmail:
		at := strings.Index(t, "@")
		if at <= 0 || at == len(t)-1 || !strings.Contains(t[at+1:], ".") ||
			strings.ContainsAny(t, " \t\r\n") || strings.Count(t, "@") != 1 {
			return fmt.Errorf("邮箱格式不正确:%s", t)
		}
	case MFATypeSMS:
		digits := 0
		for _, r := range t {
			if r >= '0' && r <= '9' {
				digits++
				continue
			}
			if r != '+' && r != '-' && r != ' ' {
				return fmt.Errorf("手机号格式不正确:%s", t)
			}
		}
		if digits < 5 || digits > 15 {
			return fmt.Errorf("手机号格式不正确:%s", t)
		}
	}
	return nil
}

// ---------------------------------------------------------------- 存储

// loadMFAChannel 读取并解密某个账户的通道配置。
//
// 返回 (nil, nil) 表示该通道还没配置。
func (s *Store) loadMFAChannel(userID, channel string) (map[string]any, error) {
	var model MFAChannelModel
	err := s.db.Where("user_id = ? AND channel = ?", userID, channel).First(&model).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("platform: 查询验证码通道失败: %w", err)
	}
	if model.CfgEnc == "" {
		return nil, nil
	}
	plain := model.CfgEnc
	if s.box != nil {
		decoded, derr := s.box.DecryptWithAAD(model.CfgEnc, aadMFAChannel)
		if derr != nil {
			return nil, fmt.Errorf("platform: 解密验证码通道失败: %w", derr)
		}
		plain = decoded
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(plain), &out); err != nil {
		return nil, fmt.Errorf("platform: 解析验证码通道失败: %w", err)
	}
	return out, nil
}

// saveMFAChannel 保存(加密)某个账户的通道配置。
func (s *Store) saveMFAChannel(userID, channel string, cfg map[string]any) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("platform: 序列化验证码通道失败: %w", err)
	}
	enc := string(raw)
	if s.box != nil {
		enc, err = s.box.EncryptWithAAD(raw, aadMFAChannel)
		if err != nil {
			return err
		}
	}
	model := MFAChannelModel{
		UserID:    userID,
		Channel:   channel,
		CfgEnc:    enc,
		UpdatedAt: formatTime(time.Now()),
	}
	err = s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "channel"}},
		DoUpdates: clause.AssignmentColumns([]string{"cfg_enc", "updated_at"}),
	}).Create(&model).Error
	if err != nil {
		return fmt.Errorf("platform: 保存验证码通道失败: %w", err)
	}
	return nil
}

// deleteMFAChannel 删除某个账户的通道配置。
func (s *Store) deleteMFAChannel(userID, channel string) error {
	err := s.db.Where("user_id = ? AND channel = ?", userID, channel).
		Delete(&MFAChannelModel{}).Error
	if err != nil {
		return fmt.Errorf("platform: 删除验证码通道失败: %w", err)
	}
	return nil
}

// smtpAuthHint 把常见的「认证失败」翻译成人话。
//
// QQ / 163 的 SMTP 只认「授权码」,不认邮箱登录密码;
// 而服务器回的 535 一句话里塞了好几种可能(服务没开、密码错、频率限制、被锁),
// 用户基本看不懂。这里按错误特征补一句「接下来该干什么」。
func smtpAuthHint(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "535"),
		strings.Contains(msg, "authentication failed"),
		strings.Contains(msg, "auth error"),
		strings.Contains(msg, "login fail"),
		strings.Contains(msg, "535 "):
		return "；SMTP 认证被拒:QQ / 163 邮箱要填「授权码」(邮箱设置里生成,不是登录密码)," +
			"并确认已经开启 SMTP 服务(QQ 的授权码一般是 16 位)"
	case strings.Contains(msg, "530"):
		return "；服务器要求先升级加密:把「加密方式」改成 STARTTLS(587)或隐式 TLS(465)"
	}
	return ""
}

// ---------------------------------------------------------------- 发送实现

// smtpSender 按 SMTP 通道配置发送验证码。
type smtpSender struct {
	cfg SMTPChannelConfig
	// policy 出站地址策略(SSRF 防护)。
	policy outboundPolicy
}

// Send 发送验证码邮件(mfaType 固定为 email)。
//
// 邮件的组装与投递交给 gapi 的 mail 包(底层 gomail),平台不再自己拼 MIME:
//
//   - 自己拼的报文缺 Date / Message-ID,base64 正文也没按 76 字符折行,
//     网易 163 这类收件方会**静默丢弃**(SMTP 那边照样回 250 已入队,用户就是收不到);
//   - 现成的库能发,就别自己造轮子。
//
// 加密方式沿用页面上的「加密方式」下拉框(tls_mode):
//
//	tls      —— 隐式 TLS(QQ / 163 的 465);
//	starttls —— 先明文握手再升级(QQ 587 / 163 25);服务器不支持就直接报错,
//	            绝不让验证码**明文**发出去;
//	none     —— 不强制加密(内网自建服务器)。
func (s smtpSender) Send(_ context.Context, _ string, target, code string) error {
	host := strings.TrimSpace(s.cfg.Host)
	if host == "" {
		return fmt.Errorf("platform: 未配置 SMTP 服务器")
	}
	mode := s.cfg.mode()
	port := s.cfg.Port
	if port <= 0 {
		switch mode {
		case "tls":
			port = 465
		case "none":
			port = 25
		default:
			port = 587
		}
	}
	from := strings.TrimSpace(s.cfg.FromAddr)
	if from == "" {
		from = strings.TrimSpace(s.cfg.Username)
	}
	if from == "" {
		return fmt.Errorf("platform: 未配置发件人地址")
	}
	username := strings.TrimSpace(s.cfg.Username)

	// SMTP 服务器地址是商户自己填的,平台会照着去连 —— 和短信网关是同一类风险:
	// 不校验的话,商户就能借平台去连内网的任意端口(探测服务、读内部协议)。
	// 这里做一次预检(域名解析出来的 IP 必须不是内网地址)。
	if err := s.policy.validateSMTPHost(host); err != nil {
		return fmt.Errorf("platform: SMTP 服务器地址不可用: %w", err)
	}

	// 证书校验:加密通道要验服务器证书(gapi 默认是跳过校验的开发行为),
	// 验证码不该发给一个证书对不上的服务器;
	// 「不加密」模式给内网自建服务器留后路,不强求证书有效。
	tlsConfig := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	if mode == "none" {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	err := mail.Email(mail.EmailParams{
		To:    target,
		Code:  code,
		Name:  "支付平台",
		Value: "支付平台验证码",
		SMTPInfo: mail.SMTPConfig{
			Host:      host,
			Port:      port,
			User:      username,
			Auth:      s.cfg.Password,
			Addr:      from,
			TLSMode:   mode,
			TLSConfig: tlsConfig,
		},
	})
	if err != nil {
		return fmt.Errorf("platform: 发送验证码邮件失败: %w%s", err, smtpAuthHint(err))
	}
	return nil
}

// gatewaySender 按 HTTP 网关配置发送短信验证码。
type gatewaySender struct {
	cfg    HTTPGatewayConfig
	client *http.Client
	// policy 出站地址策略(SSRF 防护)。
	policy outboundPolicy
	// logger 记录网关返回的原始内容(不给浏览器看)。
	logger *log.Logger
}

// Send 调用网关发送验证码(mfaType 固定为 sms)。
func (s gatewaySender) Send(ctx context.Context, _ string, target, code string) error {
	url := strings.TrimSpace(s.cfg.URL)
	if url == "" {
		return fmt.Errorf("platform: 未配置短信网关地址")
	}
	// 发之前再校验一次地址:配置是加密存库的,但策略可能在两次之间变过
	// (平台方收紧了白名单),所以不只在保存时校验。
	if _, err := s.policy.validateOutboundURL(url); err != nil {
		return fmt.Errorf("platform: 短信网关地址不可用: %w", err)
	}
	codeField := s.cfg.CodeField
	if codeField == "" {
		codeField = "code"
	}
	targetField := s.cfg.TargetField
	if targetField == "" {
		targetField = "target"
	}
	payload := map[string]string{codeField: code, targetField: target}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("platform: 序列化短信请求失败: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(raw)))
	if err != nil {
		return fmt.Errorf("platform: 构造短信请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	}
	client := s.client
	if client == nil {
		client = s.policy.newOutboundClient(15 * time.Second)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("platform: 调用短信网关失败: %w", err)
	}
	defer resp.Body.Close()
	// 读一点响应体:很多短信网关即使发送失败也返回 200,错误码在 body 里。
	// 不看 body 的话,页面会显示「测试发送成功」,而短信根本没发出去。
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// **只把状态码带回给用户**,原始响应体只写进服务器日志。
		//
		// 为什么要这么小心:网关地址是商户自己填的,平台替它发请求。
		// 如果把响应体原样回显,商户就能借平台去读任意内部服务的响应
		// (拿网关地址指向内网服务,再看错误信息里的内容)—— 这是 SSRF
		// 的「读」那一半,比「打」那一半危险得多。
		if s.logger != nil {
			s.logger.Printf("[MFA] 短信网关返回状态码 %d,响应体: %s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return fmt.Errorf("platform: 短信网关返回状态码 %d(响应内容见服务器日志)", resp.StatusCode)
	}
	if msg := gatewayErrorMessage(body); msg != "" {
		return fmt.Errorf("platform: 短信网关返回失败: %s", msg)
	}
	return nil
}

// gatewayErrorMessage 从短信网关的响应体里识别「发送失败」。
//
// 兼容几种常见形态:
//
//	{"code": 1001, "msg": "余额不足"}
//	{"errcode": "FAIL", "errmsg": "..."}
//	{"success": false, "message": "..."}
//	{"status": "error", "desc": "..."}
//
// 认不出来(空 body、非 JSON、字段缺失)一律当作成功 —— 宁可放过,不要误报。
func gatewayErrorMessage(body []byte) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	msg := firstString(payload, "message", "msg", "errmsg", "err_msg", "desc", "description", "error")
	// 数值型错误码(阿里云/腾讯云风格):0 表示成功。
	for _, key := range []string{"code", "errcode", "err_code"} {
		v, ok := payload[key]
		if !ok {
			continue
		}
		switch num := v.(type) {
		case float64:
			if num != 0 {
				if msg != "" {
					return fmt.Sprintf("错误码 %v(%s)", int64(num), msg)
				}
				return fmt.Sprintf("错误码 %v", int64(num))
			}
		case string:
			low := strings.ToLower(strings.TrimSpace(num))
			if low != "" && low != "0" && low != "ok" && low != "success" {
				if msg != "" {
					return fmt.Sprintf("错误码 %s(%s)", num, msg)
				}
				return "错误码 " + num
			}
		}
	}
	if v, ok := payload["success"]; ok {
		if b, isBool := v.(bool); isBool && !b {
			if msg != "" {
				return msg
			}
			return "网关返回 success=false"
		}
	}
	if v, ok := payload["status"]; ok {
		if s, isStr := v.(string); isStr {
			switch strings.ToLower(strings.TrimSpace(s)) {
			case "error", "failed", "fail", "failure":
				if msg != "" {
					return msg
				}
				return "网关返回 status=" + s
			}
		}
	}
	return ""
}

// firstString 取 payload 里第一个非空的字符串字段。
func firstString(payload map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := payload[k].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
