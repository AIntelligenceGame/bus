package platform

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// mfaPost 调一个平台接口,返回状态码与解析后的响应体。
func mfaPost(t *testing.T, handler http.HandlerFunc, path, token, verifyToken string, body any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("序列化请求失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-User-Token", token)
	}
	if verifyToken != "" {
		req.Header.Set("X-Verify-Token", verifyToken)
	}
	handler(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// ---------------------------------------------------------------- TOTP 单元

// TestTOTPIssueAndVerify 动态码的生成与校验。
func TestTOTPIssueAndVerify(t *testing.T) {
	secret, err := generateTOTPSecret()
	if err != nil {
		t.Fatalf("生成密钥失败: %v", err)
	}
	if len(secret) != 32 {
		t.Fatalf("密钥长度异常: %q", secret)
	}
	now := time.Now()
	code, err := totpCodeAt(secret, now)
	if err != nil {
		t.Fatalf("计算动态码失败: %v", err)
	}
	if len(code) != totpDigits {
		t.Fatalf("动态码长度应为 %d,实际 %q", totpDigits, code)
	}
	if !verifyTOTPCode(secret, code, now) {
		t.Fatal("刚生成的动态码应当校验通过")
	}
	// 造一个「格式合法、但和真码不同」的错误码。
	//
	// 之前只测了 "000000" 这一个字面量,而且用的是 `&&`:
	// 本轮真码恰好是 "000000" 时(1/10^6)整个断言会被静默跳过。
	// 更常见���失效路径是「校验器压根没把 secret 参与运算」,
	// 那种情况下"000000" 也可能碰巧不被验 —— 得用「和真码不同但格式合法」的码才测得出来。
	wrong := []byte(code)
	if wrong[0] == '0' {
		wrong[0] = '1'
	} else {
		wrong[0] = '0'
	}
	wrongCode := string(wrong)
	if wrongCode == code {
		t.Fatalf("没能造出不同的错误码: %q", code)
	}
	if verifyTOTPCode(secret, wrongCode, now) {
		t.Fatalf("错误的动态码不应通过校验: %q", wrongCode)
	}
	// 超出允许的时钟偏移(±1 步 = 30 秒)应当校验失败。
	far := now.Add(5 * time.Minute)
	farCode, err := totpCodeAt(secret, far)
	if err != nil {
		t.Fatal(err)
	}
	if verifyTOTPCode(secret, farCode, now) {
		t.Fatal("超出偏移窗口的动态码不应通过")
	}
	// 另一个密钥算出的码不应通过。
	other, err := generateTOTPSecret()
	if err != nil {
		t.Fatal(err)
	}
	otherCode, err := totpCodeAt(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if verifyTOTPCode(secret, otherCode, now) {
		t.Fatal("别的密钥算出的动态码不应通过")
	}
	// otpauth URI 要能被验证器 App 识别。
	uri := totpURI(secret, "merchant@example.com")
	if !strings.HasPrefix(uri, "otpauth://totp/") || !strings.Contains(uri, "secret="+secret) {
		t.Fatalf("otpauth URI 异常: %q", uri)
	}
}

// ---------------------------------------------------------------- TOTP 流程

// TestTOTPEnableLoginDisable TOTP 全流程:绑定 → 登录 → 关闭。
func TestTOTPEnableLoginDisable(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户TOTP", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, err := p.store.UserByName("商户TOTP")
	if err != nil || user == nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	// 1. 生成密钥
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	if secret == "" {
		t.Fatal("响应里应有 secret")
	}
	if resp["qrcode_url"] == nil || resp["qrcode_url"] == "" {
		t.Fatal("响应里应有二维码地址")
	}
	// 密钥只存在于服务端暂存列,不能污染生效列。
	if p.store.MFATOTPSecret(user.UserID) != "" {
		t.Fatal("还没绑定就不该有生效密钥")
	}

	// 2. 用动态码绑定
	dyn, err := totpCodeAt(secret, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("绑定 TOTP 失败: %d %v", code, resp)
	}
	u, _ := p.store.UserByName("商户TOTP")
	if !u.MFAEnabled || u.MFAType != MFATypeTOTP {
		t.Fatalf("绑定后状态异常: enabled=%v type=%q", u.MFAEnabled, u.MFAType)
	}
	if p.store.MFATOTPPendingSecret(user.UserID) != "" {
		t.Fatal("绑定成功后待确认密钥应当被清掉")
	}

	// 3. 登录:不带码 → 要求验证码,并回传 mfa_type
	code, resp = loginWithBody(t, p, map[string]string{"name": "商户TOTP", "password": "supersecret"})
	if code != http.StatusUnauthorized {
		t.Fatalf("开启后登录应要求验证码,实际 %d: %v", code, resp)
	}
	if resp["mfa_required"] != true {
		t.Fatalf("响应里应有 mfa_required: %v", resp)
	}
	if resp["mfa_type"] != MFATypeTOTP {
		t.Fatalf("响应里应回传 mfa_type=totp: %v", resp)
	}

	// 4. 登录:带动态码 → 成功
	dyn, _ = totpCodeAt(secret, time.Now())
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户TOTP", "password": "supersecret", "code": dyn,
	})
	if code != http.StatusOK {
		t.Fatalf("带动态码登录应成功,实际 %d: %v", code, resp)
	}
	// 登录会轮换令牌,后续操作要用新令牌。
	token = resp["token"].(string)

	// 5. 关闭:需要当前动态码。
	//
	// 动态码**一次性**(RFC 6238 §5.2):第 4 步登录已经用过这一片了,
	// 同一片里的码再用会被判成重放 —— 所以这里把时间往前推一片,
	// 模拟用户等下一个码(真实场景就是等下一次刷新的 6 位码)。
	advanceTOTPClock(t, time.Duration(totpPeriod)*time.Second)
	dyn, _ = totpCodeAt(secret, totpNow())
	code, resp = mfaPost(t, p.handleDisableMFA, "/api/account/mfa/disable", token, "",
		map[string]any{"password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("关闭二次验证失败: %d %v", code, resp)
	}
	u, _ = p.store.UserByName("商户TOTP")
	if u.MFAEnabled {
		t.Fatal("关闭后 MFAEnabled 应为 false")
	}
	if p.store.MFATOTPSecret(user.UserID) != "" {
		t.Fatal("关闭后应清掉 TOTP 密钥")
	}
	// 关闭之后登录不再要验证码。
	code, resp = loginWithBody(t, p, map[string]string{"name": "商户TOTP", "password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("关闭后登录应成功,实际 %d: %v", code, resp)
	}
}

// TestTOTPSwitchWithoutDisable 换方式不需要先关闭:直接覆盖绑定。
//
// 但覆盖前必须由**当前绑定的人**确认(发到已绑定地址的验证码):
// 否则拿到口令的人就能把正主的验证器换成自己的,正主当场进不来。
func TestTOTPSwitchWithoutDisable(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户换", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户换")

	// 先绑邮箱
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定邮箱失败: %d %v", code, resp)
	}

	// 换绑确认码:发到已绑定地址的那枚。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeRebind, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}

	// 直接换成 TOTP(不关闭)
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "",
		map[string]any{"current_code": "222222"})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn, _ := totpCodeAt(secret, time.Now())
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn, "current_code": "222222"})
	if code != http.StatusOK {
		t.Fatalf("切换到 TOTP 失败: %d %v", code, resp)
	}
	u, _ := p.store.UserByName("商户换")
	if u.MFAType != MFATypeTOTP {
		t.Fatalf("切换后类型应为 totp,实际 %q", u.MFAType)
	}
	if u.MFATarget != "" {
		t.Fatalf("TOTP 不该有接收地址,实际 %q", u.MFATarget)
	}
}

// ---------------------------------------------------------------- 通道配置

// TestMFAChannelSecretEncrypted 通道配置必须加密入库,且读取接口不回显密钥。
func TestMFAChannelSecretEncrypted(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户通道", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户通道")

	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const secret = "super-secret-auth-code"
	code, resp = mfaPost(t, p.handleSaveMFAChannel, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{
			"channel": "email",
			"config": map[string]any{
				"host": "smtp.qq.com", "port": 587,
				"username": "sender@example.com", "password": secret,
			},
		})
	if code != http.StatusOK {
		t.Fatalf("保存通道失败: %d %v", code, resp)
	}

	// 库里不能有明文。
	var raw string
	if err := p.store.db.Raw("SELECT cfg_enc FROM pay_mfa_channels WHERE user_id = ? AND channel = ?",
		user.UserID, mfaChannelEmail).Scan(&raw).Error; err != nil {
		t.Fatal(err)
	}
	if raw == "" {
		t.Fatal("通道配置没有入库")
	}
	if strings.Contains(raw, secret) {
		t.Fatalf("库里出现了明文授权码: %q", raw)
	}

	// 读取接口只回显「配没配」,不回显内容。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/account/mfa/channels", nil)
	req.Header.Set("X-User-Token", token)
	p.handleMFAChannels(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, secret) {
		t.Fatalf("读取接口回显了授权码: %s", body)
	}
	out := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	channels, _ := out["channels"].(map[string]any)
	mail, _ := channels["email"].(map[string]any)
	if mail["configured"] != true {
		t.Fatalf("应回显 configured=true: %v", mail)
	}
	if mail["password_set"] != true {
		t.Fatalf("应回显 password_set=true: %v", mail)
	}
	if _, ok := mail["password"]; ok {
		t.Fatalf("不该回显 password 字段: %v", mail)
	}
	if mail["host"] != "smtp.qq.com" {
		t.Fatalf("非敏感字段应当回显: %v", mail)
	}

	// 授权码留空保存 → 保留原值(不会把授权码清掉)。
	code, resp = mfaPost(t, p.handleSaveMFAChannel, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{
			"channel": "email",
			"config":  map[string]any{"host": "smtp.qq.com", "port": 587, "username": "sender@example.com"},
		})
	if code != http.StatusOK {
		t.Fatalf("二次保存失败: %d %v", code, resp)
	}
	cfg, err := p.store.loadMFAChannel(user.UserID, mfaChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if cfg["password"] != secret {
		t.Fatalf("留空保存应保留原授权码,实际 %v", cfg["password"])
	}

	// 没有票据 → 拒绝。
	code, resp = mfaPost(t, p.handleSaveMFAChannel, "/api/account/mfa/channels", token, "",
		map[string]any{"channel": "email", "config": map[string]any{"host": "smtp.qq.com"}})
	if code != http.StatusForbidden {
		t.Fatalf("没有票据应拒绝,实际 %d: %v", code, resp)
	}

	// 删除。
	code, resp = mfaPost(t, p.handleDeleteMFAChannel, "/api/account/mfa/channels/delete", token, verifyToken,
		map[string]any{"channel": "email"})
	if code != http.StatusOK {
		t.Fatalf("删除通道失败: %d %v", code, resp)
	}
	cfg, err = p.store.loadMFAChannel(user.UserID, mfaChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatalf("删除后不应还有配置: %v", cfg)
	}
}

// TestMFACodeDeliveredFlag 「只写日志」必须如实告诉用户。
func TestMFACodeDeliveredFlag(t *testing.T) {
	// 平台默认通道(LogMFASender):只写日志 → delivered=false。
	// AllowLogOnlyMFA 打开,表示平台方承认「从服务器日志里取码」这条开发流程。
	p := &Platform{
		opts: Options{
			Logger:          log.New(io.Discard, "", 0),
			VerifyTTL:       10 * time.Minute,
			AllowLogOnlyMFA: true,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
	}
	code, resp := login(t, p, "商户日志", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)

	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "email", "target": "a@b.com", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusOK {
		t.Fatalf("发送绑定验证码失败: %d %v", code, resp)
	}
	if resp["delivered"] != false {
		t.Fatalf("只写日志时应回 delivered=false: %v", resp)
	}
	msg, _ := resp["message"].(string)
	if !strings.Contains(msg, "日志") {
		t.Fatalf("提示语应当说清楚「只写日志」: %q", msg)
	}

	// 注入了真实发送器 → delivered=true。
	p2, sender := newTestPlatform(t)
	code, resp = login(t, p2, "商户真发", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token2 := resp["token"].(string)
	code, resp = mfaPost(t, p2.handleSendMFACode, "/api/account/mfa/code", token2, "",
		map[string]any{
			"type": "email", "target": "a@b.com", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusOK {
		t.Fatalf("发送绑定验证码失败: %d %v", code, resp)
	}
	if resp["delivered"] != true {
		t.Fatalf("真实发送时应回 delivered=true: %v", resp)
	}
	if len(sender.codes) != 1 {
		t.Fatalf("应当真的下发了 1 条验证码,实际 %d", len(sender.codes))
	}
}

// TestEnableMFARequiresWorkingChannel 邮箱 / 短信方式必须有能用的下发通道。
//
// 没有通道就开启 = 验证码只写日志、用户永远收不到,而登录又要这个码 ——
// 等于把账户锁在门外。这种情况要**拒绝开启**,而不是开了再让人登不进来。
func TestEnableMFARequiresWorkingChannel(t *testing.T) {
	// 平台只有日志通道(默认的 LogMFASender)。
	p := &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
	}
	code, resp := login(t, p, "商户无通道", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户无通道")

	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusBadRequest {
		t.Fatalf("没有下发通道时不应允许开启,实际 %d: %v", code, resp)
	}
	msg, _ := resp["error"].(string)
	if !strings.Contains(msg, "尚未配置") {
		t.Fatalf("错误提示要说清楚原因: %q", msg)
	}
	u, _ := p.store.UserByName("商户无通道")
	if u.MFAEnabled {
		t.Fatal("被拒绝后不该开启二次验证")
	}

	// 配了通道 → 允许开启。
	if err := p.store.saveMFAChannel(user.UserID, mfaChannelEmail, map[string]any{
		"host": "smtp.example.com", "port": 587, "username": "u@example.com", "password": "x",
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "222222"})
	if code != http.StatusOK {
		t.Fatalf("配好通道后应允许开启,实际 %d: %v", code, resp)
	}
}

// ---------------------------------------------------------------- 无通道时的自愈

// newLogOnlyPlatform 一个「只有日志通道」的平台(平台方没注入真实下发器)。
func newLogOnlyPlatform(t *testing.T) *Platform {
	t.Helper()
	return &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
	}
}

// TestMFACodeRefusedWithoutChannel 没有可用通道时,不要发一个收不到的验证码。
//
// 平台默认只有 LogMFASender(只写日志)。这时点「发送绑定验证码」,
// 页面如果说「已发送」就是在骗人 —— 用户在等一个永远不会来的邮件,
// 而开启二次验证之后登录又要这个码,等于把自己锁在门外。
//
// 所以:没有可用通道 → 明确拒绝,并说清楚去哪儿配置。
func TestMFACodeRefusedWithoutChannel(t *testing.T) {
	p := newLogOnlyPlatform(t)
	code, resp := login(t, p, "商户无通道发码", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)

	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "email", "target": "a@b.com", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusBadRequest {
		t.Fatalf("没有通道时不应发码,实际 %d: %v", code, resp)
	}
	msg, _ := resp["error"].(string)
	if !strings.Contains(msg, "下发通道") {
		t.Fatalf("错误提示要说清楚是通道没配: %q", msg)
	}
}

// TestUndeliverableBindingSelfHeal 「失效的绑定」不能把账户锁死。
//
// 场景:账户里已经有一条邮箱二次验证绑定,但通道没了(从没配 / 被删)。
// 这种绑定拦不住任何人(码只写日志),却能挡住正主 ——
// 登录要一个他永远收不到的码,敏感操作票据也要,连「去把通道配上」都做不到。
//
// 期望:登录跳过它并如实告知;身份校验只要口令;关闭它也只要口令。
func TestUndeliverableBindingSelfHeal(t *testing.T) {
	p := newLogOnlyPlatform(t)
	code, resp := login(t, p, "商户失效绑定", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户失效绑定")
	// 手工造一条「开着邮箱二次验证、但没有通道」的绑定。
	if err := p.store.SetMFA(user.UserID, MFATypeEmail, "a@b.com", true); err != nil {
		t.Fatal(err)
	}

	// 1) 安全状态要如实报「不可下发」。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/account/security", nil)
	req.Header.Set("X-User-Token", token)
	p.handleAccountSecurity(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["mfa_deliverable"] != false {
		t.Fatalf("通道不可用时应回 mfa_deliverable=false: %v", out)
	}

	// 2) 登录不需要验证码(跳过并告知)。
	code, resp = login(t, p, "商户失效绑定", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("失效绑定不应挡住登录: %d %v", code, resp)
	}
	if resp["mfa_skipped"] != true {
		t.Fatalf("应当回 mfa_skipped=true: %v", resp)
	}
	if note, _ := resp["mfa_note"].(string); !strings.Contains(note, "跳过") {
		t.Fatalf("应当说明为什么跳过: %q", note)
	}

	// 3) 身份校验只要口令(否则连通道都配不了)。
	token = resp["token"].(string)
	code, resp = mfaPost(t, p.handleVerify, "/api/account/verify", token, "",
		map[string]any{"password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("失效绑定时身份校验不应要求验证码: %d %v", code, resp)
	}
	if resp["verify_token"] == "" {
		t.Fatal("应当签发票据")
	}

	// 4) 关闭二次验证只要口令。
	code, resp = mfaPost(t, p.handleDisableMFA, "/api/account/mfa/disable", token, "",
		map[string]any{"password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("失效绑定应当可以只凭口令关闭: %d %v", code, resp)
	}
	u, _ := p.store.UserByName("商户失效绑定")
	if u.MFAEnabled {
		t.Fatal("应当已经关闭二次验证")
	}
}

// TestMFATargetValidation 接收地址格式不对要当场拦下来。
//
// 格式错的地址会让验证码「发不出去」,而且往往查不出原因:
// SMTP 直接拒收,短信网关报「号码不存在」。
func TestMFATargetValidation(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户地址校验", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户地址校验")

	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "email", "target": "not-an-email", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusBadRequest {
		t.Fatalf("非法邮箱地址应被拒绝,实际 %d: %v", code, resp)
	}
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "sms", "target": "abc", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusBadRequest {
		t.Fatalf("非法手机号应被拒绝,实际 %d: %v", code, resp)
	}
	// 正常手机号(含 +86 前缀)应当通过格式校验。
	// 这里网关地址是假的,发不出去是正常的 —— 关键是不能被判成「格式不正确」。
	if err := p.store.saveMFAChannel(user.UserID, mfaChannelSMS, map[string]any{
		"url": "https://sms.example.com/send",
	}); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "sms", "target": "+8613800138000", "purpose": "bind", "password": "supersecret",
		})
	msg, _ := resp["error"].(string)
	if strings.Contains(msg, "格式不正确") {
		t.Fatalf("合法手机号不该被判非法: %d %v", code, resp)
	}
}

// ---------------------------------------------------------------- 真实路由

// muxPost 走平台真实路由发一个请求(带中间件:鉴权 + 安全响应头)。
func muxPost(t *testing.T, h http.Handler, method, path, token, verifyToken string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("序列化请求失败: %v", err)
		}
		reader = strings.NewReader(string(raw))
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-User-Token", token)
	}
	if verifyToken != "" {
		req.Header.Set("X-Verify-Token", verifyToken)
	}
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestMFAEndpointsOverRealRoutes 走真实路由跑一遍新接口:
// 未登录被拦、带令牌能调、敏感操作要票据。
func TestMFAEndpointsOverRealRoutes(t *testing.T) {
	p, _ := newTestPlatform(t)
	h := p.Handler()

	// 未登录 → 401
	code, _ := muxPost(t, h, http.MethodGet, "/api/account/mfa/channels", "", "", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("未登录应被拒绝,实际 %d", code)
	}

	code, resp := login(t, p, "商户路由", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户路由")

	// TOTP 绑定:生成密钥 → 动态码绑定
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn, _ := totpCodeAt(secret, time.Now())
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("绑定 TOTP 失败: %d %v", code, resp)
	}

	// 通道配置:没有票据 → 403
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/channels", token, "",
		map[string]any{"channel": "email", "config": map[string]any{"host": "smtp.qq.com"}})
	if code != http.StatusForbidden {
		t.Fatalf("没有票据应被拒绝,实际 %d: %v", code, resp)
	}

	// 有票据 → 成功
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{"channel": "email", "config": map[string]any{
			"host": "smtp.qq.com", "port": 587, "username": "u@example.com", "password": "auth-code",
		}})
	if code != http.StatusOK {
		t.Fatalf("保存通道失败: %d %v", code, resp)
	}

	// 读取:只回显配置状态
	code, resp = muxPost(t, h, http.MethodGet, "/api/account/mfa/channels", token, "", nil)
	if code != http.StatusOK {
		t.Fatalf("读取通道失败: %d %v", code, resp)
	}
	channels, _ := resp["channels"].(map[string]any)
	mail, _ := channels["email"].(map[string]any)
	if mail["configured"] != true || mail["password_set"] != true {
		t.Fatalf("通道状态异常: %v", mail)
	}
	if _, ok := mail["password"]; ok {
		t.Fatalf("不该回显授权码: %v", mail)
	}

	// 安全状态里带上通道状态
	code, resp = muxPost(t, h, http.MethodGet, "/api/account/security", token, "", nil)
	if code != http.StatusOK {
		t.Fatalf("读取安全状态失败: %d %v", code, resp)
	}
	if resp["email_channel_configured"] != true {
		t.Fatalf("安全状态应显示邮件通道已配置: %v", resp)
	}
	if resp["mfa_type"] != MFATypeTOTP {
		t.Fatalf("安全状态的二次验证方式应为 totp: %v", resp)
	}
}
