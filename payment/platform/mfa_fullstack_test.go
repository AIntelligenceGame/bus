package platform

import (
	"bytes"
	"encoding/json"
	"image/png"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// codePattern 邮件正文里的 6 位验证码。
var codePattern = regexp.MustCompile(`\b\d{6}\b`)

// codeAnchoredPattern 按邮件模板里的固定文案定位验证码。
//
// gapi/mail 的正文模板是「验证码:%s」—— 用文案定位比「找第一个 6 位数字串」稳:
// 正文里只要出现时间戳、订单号、金额之类的 6 位数字,前者就会取错。
var codeAnchoredPattern = regexp.MustCompile(`验证码[:：]\s*(\d{6})`)

// mailCode 从一封验证码邮件里取出 6 位码。
func mailCode(t *testing.T, raw string) string {
	t.Helper()
	header, body := mailRaw(t, raw)
	text := mailBody(t, header, body)
	// 先按「验证码:」这个锚点找。
	if m := codeAnchoredPattern.FindStringSubmatch(text); m != nil {
		return m[1]
	}
	// 兜底:取**最后一个** 6 位数字串(验证码通常在正文靠前的位置,
	// 但正文后面可能有别的数字)。宁可取最后一个,也不要取开头的。
	all := codePattern.FindAllString(text, -1)
	if len(all) == 0 {
		t.Fatalf("正文里没有验证码: %q", text)
	}
	return all[len(all)-1]
}

// TestMFAPageFlowEndToEnd 走**真实路由 + 真实数据库 + 真实 SMTP** 的全流程。
//
// 这是「在页面上配好通道 → 填了就能用」的端到端证明:
//
//	登录 → 换票据 → 配邮件通道 → 发绑定码(真的收到邮件) → 开启邮箱二次验证
//	→ 退出 → 重新登录(要验证码,且验证码能从邮件里取到) → 关闭二次验证
func TestMFAPageFlowEndToEnd(t *testing.T) {
	host, port, messages := newFakeSMTP(t)

	p := &Platform{
		opts: Options{
			Logger:     log.New(io.Discard, "", 0),
			VerifyTTL:  10 * time.Minute,
			SessionTTL: time.Hour,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
		apiLimiter:   newRateLimiter(),
		// 假的 SMTP / 短信服务器都在 127.0.0.1 上,放开出站防护的内网拦截。
		outbound: outboundPolicy{allowPrivate: true},
	}
	h := p.Handler()

	// 1) 首次登录(自动创建账户)。
	code, resp := muxPost(t, h, http.MethodPost, "/api/login", "", "",
		map[string]any{"name": "商户全流程", "password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)
	if token == "" {
		t.Fatal("首次登录没有返回令牌")
	}

	// 2) 通道没配时,安全状态要如实说「不可下发」。
	code, resp = muxGet(t, h, "/api/account/security", token)
	if code != http.StatusOK {
		t.Fatalf("读取安全状态失败: %d %v", code, resp)
	}
	if resp["email_channel_configured"] != false {
		t.Fatalf("还没配通道,不该回已配置: %v", resp)
	}

	// 3) 身份校验(此时没有二次验证,只要口令)→ 票据。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/verify", token, "",
		map[string]any{"password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("身份校验失败: %d %v", code, resp)
	}
	verifyToken, _ := resp["verify_token"].(string)
	if verifyToken == "" {
		t.Fatal("没有拿到票据")
	}

	// 4) 保存邮件通道配置(页面上那一栏)。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{
			"channel": "email",
			"config": map[string]any{
				"host": host, "port": port,
				"from_addr": "noreply@example.com",
				"tls_mode":  "none",
			},
		})
	if code != http.StatusOK {
		t.Fatalf("保存邮件通道失败: %d %v", code, resp)
	}

	// 5) 发绑定验证码 —— 邮件要真的送到。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "email", "target": "user@example.com",
			"purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusOK {
		t.Fatalf("发送绑定验证码失败: %d %v", code, resp)
	}
	if resp["delivered"] != true {
		t.Fatalf("配好通道后应当真的发出去: %v", resp)
	}
	var raw string
	select {
	case raw = <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到邮件")
	}
	bindCode := mailCode(t, raw)

	// 6) 开启邮箱二次验证。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/enable", token, "",
		map[string]any{
			"type": "email", "target": "user@example.com",
			"password": "supersecret", "code": bindCode,
		})
	if code != http.StatusOK {
		t.Fatalf("开启二次验证失败: %d %v", code, resp)
	}

	// 7) 安全状态:已开启且可下发。
	code, resp = muxGet(t, h, "/api/account/security", token)
	if code != http.StatusOK {
		t.Fatalf("读取安全状态失败: %d %v", code, resp)
	}
	if resp["mfa_enabled"] != true {
		t.Fatalf("应当已开启二次验证: %v", resp)
	}
	if resp["mfa_deliverable"] != true {
		t.Fatalf("通道可用时应当可下发: %v", resp)
	}

	// 8) 重新登录:这次**要**验证码 —— 而且验证码要从邮件里取。
	code, resp = muxPost(t, h, http.MethodPost, "/api/login", "", "",
		map[string]any{"name": "商户全流程", "password": "supersecret"})
	if code != http.StatusUnauthorized {
		t.Fatalf("开启二次验证后登录应当要验证码,实际 %d: %v", code, resp)
	}
	if resp["mfa_required"] != true {
		t.Fatalf("应当回 mfa_required=true: %v", resp)
	}
	// 先取一次登录验证码(公开接口)。
	code, resp = muxPost(t, h, http.MethodPost, "/api/login/code", "", "",
		map[string]any{"name": "商户全流程"})
	if code != http.StatusOK {
		t.Fatalf("取登录验证码失败: %d %v", code, resp)
	}
	select {
	case raw = <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到登录验证码邮件")
	}
	loginCode := mailCode(t, raw)
	code, resp = muxPost(t, h, http.MethodPost, "/api/login", "", "",
		map[string]any{"name": "商户全流程", "password": "supersecret", "code": loginCode})
	if code != http.StatusOK {
		t.Fatalf("带验证码登录失败: %d %v", code, resp)
	}
	token, _ = resp["token"].(string)
	if resp["mfa_skipped"] == true {
		t.Fatalf("通道可用时不该跳过二次验证: %v", resp)
	}

	// 9) 关闭二次验证:同样要一个真的邮件码。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/verify", token, "",
		map[string]any{"password": "supersecret"})
	// 403(凭据不对)而不是 401(会话失效):前端收到 401 会把用户登出,
	// 「没填验证码」这种正常反馈不该把人踢回登录页。
	if code != http.StatusForbidden {
		t.Fatalf("身份校验应当要验证码,实际 %d: %v", code, resp)
	}
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/code", token, "",
		map[string]any{"purpose": "sensitive"})
	if code != http.StatusOK {
		t.Fatalf("发送敏感操作验证码失败: %d %v", code, resp)
	}
	select {
	case raw = <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到敏感操作验证码邮件")
	}
	sensitiveCode := mailCode(t, raw)
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/verify", token, "",
		map[string]any{"password": "supersecret", "code": sensitiveCode})
	if code != http.StatusOK {
		t.Fatalf("带验证码的身份校验失败: %d %v", code, resp)
	}
	verifyToken, _ = resp["verify_token"].(string)

	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/code", token, "",
		map[string]any{"purpose": "disable"})
	if code != http.StatusOK {
		t.Fatalf("发送关闭验证码失败: %d %v", code, resp)
	}
	select {
	case raw = <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到关闭验证码邮件")
	}
	disableCode := mailCode(t, raw)
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/disable", token, "",
		map[string]any{"password": "supersecret", "code": disableCode})
	if code != http.StatusOK {
		t.Fatalf("关闭二次验证失败: %d %v", code, resp)
	}
	code, resp = muxGet(t, h, "/api/account/security", token)
	if code != http.StatusOK {
		t.Fatalf("读取安全状态失败: %d %v", code, resp)
	}
	if resp["mfa_enabled"] != false {
		t.Fatalf("应当已经关闭二次验证: %v", resp)
	}
}

// TestTOTPQRCodeImage 验证器 App 的二维码必须真的能取到,而不是一张白图。
//
// 页面上的二维码走的是「先取密钥 → 再按 /api/qrcode?content=... 取 PNG」,
// 任何一环断了,用户在页面上就只看到一个空白方框(看起来像「二维码不显示」)。
func TestTOTPQRCodeImage(t *testing.T) {
	p := &Platform{
		opts: Options{
			Logger:     log.New(io.Discard, "", 0),
			VerifyTTL:  10 * time.Minute,
			SessionTTL: time.Hour,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
		apiLimiter:   newRateLimiter(),
		// 假的 SMTP / 短信服务器都在 127.0.0.1 上,放开出站防护的内网拦截。
		outbound: outboundPolicy{allowPrivate: true},
	}
	h := p.Handler()

	code, resp := muxPost(t, h, http.MethodPost, "/api/login", "", "",
		map[string]any{"name": "商户二维码", "password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)

	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	qrURL, _ := resp["qrcode_url"].(string)
	if qrURL == "" {
		t.Fatalf("响应里没有二维码地址: %v", resp)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, qrURL, nil)
	req.Header.Set("X-User-Token", token)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("取二维码失败: %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
		t.Fatalf("Content-Type 应当是 image/png,实际 %q", ct)
	}
	img, err := png.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("不是合法的 PNG: %v", err)
	}
	bounds := img.Bounds()
	if bounds.Dx() <= 0 || bounds.Dy() <= 0 {
		t.Fatalf("图片尺寸异常: %v", bounds)
	}
	// 不能是一张全白的图(那就等于「二维码不显示」)。
	dark := 0
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, _ := img.At(x, y).RGBA()
			if r < 0x8000 && g < 0x8000 && b < 0x8000 {
				dark++
			}
		}
	}
	if dark == 0 {
		t.Fatal("二维码图片是全白的,扫不了")
	}
}

// muxGet 走真实路由发一个 GET 请求。
func muxGet(t *testing.T, h http.Handler, path, token string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("X-User-Token", token)
	}
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}
