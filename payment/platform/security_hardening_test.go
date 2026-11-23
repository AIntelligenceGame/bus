package platform

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
	"gorm.io/gorm/schema"
)

// recordingSender 记录下发的验证码(测试用)。
type recordingSender struct {
	codes []string
}

func (s *recordingSender) Send(_ context.Context, _, _, code string) error {
	s.codes = append(s.codes, code)
	return nil
}

// newTestPlatform 造一个测试用的平台(不落主密钥文件、不监听端口)。
func newTestPlatform(t *testing.T) (*Platform, *recordingSender) {
	t.Helper()
	store := newTestStore(t)
	sender := &recordingSender{}
	p := &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
			MFASender: sender,
		},
		store:        store,
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
		// 测试里的 mock 服务器(SMTP / 短信网关)都在 127.0.0.1 上,
		// 出站防护默认会拦内网地址,所以测试里放开。
		outbound: outboundPolicy{allowPrivate: true},
	}
	return p, sender
}

// login 调用登录接口,返回状态码与响应体。
func login(t *testing.T, p *Platform, name, password string) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"name": name, "password": password})
	if err != nil {
		t.Fatalf("序列化登录请求失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	p.handleLogin(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestAccessTokenNeverStoredPlaintext 令牌入库必须是散列,不能有明文。
func TestAccessTokenNeverStoredPlaintext(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户甲", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录应成功,实际 %d: %v", code, resp)
	}
	token, _ := resp["token"].(string)
	if token == "" {
		t.Fatalf("登录没有返回令牌: %v", resp)
	}

	// 库里的明文列必须为空,散列列必须非空且不等于令牌。
	var model UserModel
	if err := p.store.db.Where("user_name = ?", "商户甲").First(&model).Error; err != nil {
		t.Fatalf("读用户失败: %v", err)
	}
	if model.AccessToken != "" {
		t.Fatalf("库里仍然存着明文令牌: %q", model.AccessToken)
	}
	if model.AccessTokenHash == "" {
		t.Fatalf("库里没有令牌的散列")
	}
	if model.AccessTokenHash == token {
		t.Fatalf("散列与明文令牌相同,等于没散列")
	}

	// 令牌本身仍然可用。
	user, err := p.store.UserByToken(token)
	if err != nil {
		t.Fatalf("按令牌查用户失败: %v", err)
	}
	if user == nil {
		t.Fatalf("令牌查不到用户")
	}
	// 散列列不出现在任何对外 JSON 里。
	if strings.Contains(user.AccessToken, token) {
		t.Fatalf("User 里带着明文令牌")
	}
}

// TestLoginRotatesToken 每次登录都换新令牌,旧令牌立即失效。
func TestLoginRotatesToken(t *testing.T) {
	p, _ := newTestPlatform(t)
	hash, err := hashPassword("supersecret")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	user, err := p.store.CreateUser("商户乙", hash)
	if err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	_ = user

	code, first := login(t, p, "商户乙", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录应成功,实际 %d: %v", code, first)
	}
	firstToken := first["token"].(string)

	code, second := login(t, p, "商户乙", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("第二次登录应成功,实际 %d: %v", code, second)
	}
	secondToken := second["token"].(string)
	if firstToken == secondToken {
		t.Fatalf("两次登录返回了同一个令牌,没有轮换")
	}
	// 旧令牌必须失效。
	if u, err := p.store.UserByToken(firstToken); err != nil {
		t.Fatalf("查令牌失败: %v", err)
	} else if u != nil {
		t.Fatalf("旧令牌仍然有效")
	}
}

// TestLoginBruteForceBlocked 连续输错口令会被限流/锁定,不会一直试下去。
func TestLoginBruteForceBlocked(t *testing.T) {
	p, _ := newTestPlatform(t)
	hash, err := hashPassword("supersecret")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if _, err := p.store.CreateUser("商户丙", hash); err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	// 一直用错误口令试。
	var lastCode int
	for i := 0; i < 12; i++ {
		lastCode, _ = login(t, p, "商户丙", "wrong-password")
		if lastCode == http.StatusTooManyRequests {
			break
		}
	}
	if lastCode != http.StatusTooManyRequests {
		t.Fatalf("连续错误登录应被限流或锁定,最后一次状态码 %d", lastCode)
	}
	// 正确口令此时也不能登录(账户被锁)。
	if code, _ := login(t, p, "商户丙", "supersecret"); code == http.StatusOK {
		t.Fatalf("账户锁定期间竟然登录成功了")
	}
}

// TestMFACodeBruteForceLimit 验证码错误尝试过多就作废,挡 6 位数字枚举。
func TestMFACodeBruteForceLimit(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户丁")
	if err := store.CreateMFACode(user.UserID, mfaPurposeSensitive, "123456", time.Minute); err != nil {
		t.Fatalf("创建验证码失败: %v", err)
	}
	// 连续错误尝试。
	for i := 0; i < maxMFAAttempts+2; i++ {
		if store.ConsumeMFACode(user.UserID, mfaPurposeSensitive, "000000") {
			t.Fatalf("错误验证码竟然校验通过")
		}
	}
	// 正确验证码这时也应该失效了。
	if store.ConsumeMFACode(user.UserID, mfaPurposeSensitive, "123456") {
		t.Fatalf("验证码在多次错误尝试后仍然可用(可被暴力枚举)")
	}
}

// TestRateLimiter 限流器本身的行为。
func TestRateLimiter(t *testing.T) {
	l := newRateLimiter()
	for i := 0; i < 3; i++ {
		if !l.allow("k", 3, time.Minute) {
			t.Fatalf("第 %d 次应该放行", i+1)
		}
	}
	if l.allow("k", 3, time.Minute) {
		t.Fatalf("超过上限仍然放行")
	}
	// 换个 key 不受影响。
	if !l.allow("other", 3, time.Minute) {
		t.Fatalf("别的 key 被误伤")
	}
	// reset 之后恢复。
	l.reset("k")
	if !l.allow("k", 3, time.Minute) {
		t.Fatalf("reset 后仍然不放行")
	}
}

// TestSecurityHeaders 响应必须带安全响应头。
func TestSecurityHeaders(t *testing.T) {
	p, _ := newTestPlatform(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	p.Handler().ServeHTTP(rec, req)

	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
		"Referrer-Policy":         "no-referrer",
		"Content-Security-Policy": "",
	}
	for key, value := range want {
		got := rec.Header().Get(key)
		if got == "" {
			t.Fatalf("缺少安全响应头 %s", key)
		}
		if value != "" && got != value {
			t.Fatalf("安全响应头 %s = %q,期望 %q", key, got, value)
		}
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Fatalf("CSP 不完整: %q", rec.Header().Get("Content-Security-Policy"))
	}
}

// TestTokenInURLRejected 令牌不再允许从 URL 查询参数读取。
func TestTokenInURLRejected(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户戊", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录应成功,实际 %d: %v", code, resp)
	}
	token := resp["token"].(string)

	// 用查询参数带令牌:必须被拒。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/me?token="+token, nil)
	p.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("URL 里的令牌应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 用请求头带令牌:应该通过。
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.Header.Set("X-User-Token", token)
	p.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("请求头里的令牌应被接受,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestVerifyTokenInURLRejected 敏感操作票据同样不放进 URL。
func TestVerifyTokenInURLRejected(t *testing.T) {
	p, _ := newTestPlatform(t)
	hash, err := hashPassword("supersecret")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	user, err := p.store.CreateUser("商户己", hash)
	if err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/pay?verify_token="+verifyToken, nil)
	if err := p.requireVerified(req, user); err == nil {
		t.Fatalf("URL 里的验证票据竟然被接受了")
	}
	req = httptest.NewRequest(http.MethodPost, "/api/pay", nil)
	req.Header.Set("X-Verify-Token", verifyToken)
	if err := p.requireVerified(req, user); err != nil {
		t.Fatalf("请求头里的验证票据应被接受: %v", err)
	}
}

// TestSecretsNeverEchoedToBrowser 读取渠道配置时密钥不能原样回显。
func TestSecretsNeverEchoedToBrowser(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户庚")
	cfg := ChannelConfig{
		Channel: string(payment.ChannelWechat),
		Enabled: true,
		Wechat: &wechat.Config{
			AppID:         "wx123",
			MchID:         "1900000109",
			SerialNo:      "ABC",
			APIv3Key:      "0123456789abcdef0123456789abcdef",
			PrivateKeyPEM: "-----BEGIN PRIVATE KEY-----secret-----END PRIVATE KEY-----",
		},
	}
	if err := store.SaveChannel(user.UserID, cfg); err != nil {
		t.Fatalf("保存渠道配置失败: %v", err)
	}
	channels, err := store.Channels(user.UserID)
	if err != nil {
		t.Fatalf("读取渠道配置失败: %v", err)
	}
	if len(channels) != 1 {
		t.Fatalf("应有 1 条渠道配置,实际 %d", len(channels))
	}
	// 服务端内部能拿到明文(用于调渠道)。
	if channels[0].Wechat == nil || channels[0].Wechat.APIv3Key == "" {
		t.Fatalf("服务端读不到明文密钥")
	}
	// 返回浏览器的那份必须被掩码。
	view := maskChannelConfig(channels[0])
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	if strings.Contains(string(raw), "0123456789abcdef") {
		t.Fatalf("掩码后的配置里还有 APIv3 密钥明文: %s", raw)
	}
	if strings.Contains(string(raw), "BEGIN PRIVATE KEY") {
		t.Fatalf("掩码后的配置里还有私钥明文: %s", raw)
	}
}

// TestModelColumnsMatchSchema 确认模型里的列名与实际建出来的表一致。
//
// 改了 gorm:"column:xxx" 却忘了同步写查询里的原始 SQL 列名(或者反过来),
// 编译期看不出来,运行时才报 "no such column"。
// 这里把「模型字段 → DB 列名」和「库里真实的列」对一遍。
func TestModelColumnsMatchSchema(t *testing.T) {
	store := newTestStore(t)
	models := []any{
		&UserModel{}, &ChannelModel{}, &AppModel{}, &AppChannelModel{},
		&OrderModel{}, &MFACodeModel{}, &MFAChannelModel{}, &RefundModel{},
		&VerifyTokenModel{}, &APIKeyModel{}, &AuditLogModel{},
	}
	for _, m := range models {
		s, err := schema.Parse(m, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("解析模型失败: %v", err)
		}
		cols, err := store.db.Migrator().ColumnTypes(m)
		if err != nil {
			t.Fatalf("读取表 %s 的列失败: %v", s.Table, err)
		}
		got := make(map[string]bool, len(cols))
		for _, c := range cols {
			got[c.Name()] = true
		}
		for _, f := range s.Fields {
			if f.DBName == "" {
				continue
			}
			if !got[f.DBName] {
				t.Fatalf("表 %s 的字段 %s 映射到列 %s,但库里没有这一列", s.Table, f.Name, f.DBName)
			}
		}
	}
}
