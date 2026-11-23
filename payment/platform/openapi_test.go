package platform

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 开放接口(API 密钥 + HMAC 签名)的回归测试。
//
// 覆盖:签名往返、篡改拒绝、重放拒绝、权限边界、吊销失效、商户隔离、禁用商户。

// newOpenPlatform 造一个用于开放接口测试的平台。
func newOpenPlatform(t *testing.T) (*Platform, *Store) {
	t.Helper()
	store := newTestStore(t)
	p := &Platform{
		opts: Options{
			Logger:            log.New(io.Discard, "", 0),
			VerifyTTL:         10 * time.Minute,
			PublicBaseURL:     "https://pay.example.com",
			SessionTTL:        time.Hour,
			TrustForwardedFor: false,
		},
		store:        store,
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
		apiLimiter:   newRateLimiter(),
		nonces:       &nonceCache{},
	}
	return p, store
}

// signedRequest 用密钥给一个请求签名。
func signedRequest(t *testing.T, secret, method, path, query, body string, keyID string) *http.Request {
	t.Helper()
	target := path
	if query != "" {
		target += "?" + query
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := randomHex(8)
	sig := signRequest(secret, method, target, ts, nonce, []byte(body))
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(apiKeyHeaderKey, keyID)
	req.Header.Set(apiKeyHeaderTimestamp, ts)
	req.Header.Set(apiKeyHeaderNonce, nonce)
	req.Header.Set(apiKeyHeaderSignature, sig)
	req.Header.Set("Content-Type", "application/json")
	return req
}

// TestOpenAPISignatureRoundTrip 签名正确的请求被接受,错误信息为空。
func TestOpenAPISignatureRoundTrip(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户甲")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	principal, raw, err := p.openPrincipal(req, apiKeyScopePay)
	if err != nil {
		t.Fatalf("签名校验应通过: %v", err)
	}
	if principal.User.UserID != user.UserID {
		t.Fatalf("认证主体商户不对: %s", principal.User.UserID)
	}
	if string(raw) != body {
		t.Fatalf("原始请求体不对: %s", string(raw))
	}
}

// TestOpenAPIRejectsTamperedBody 改了请求体但复用签名 → 拒绝。
func TestOpenAPIRejectsTamperedBody(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户乙")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	// 换掉请求体,签名保持不变。
	req.Body = io.NopCloser(strings.NewReader(`{"out_trade_no":"ORDER-2"}`))
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err == nil {
		t.Fatal("篡改请求体后必须拒绝")
	}
}

// TestOpenAPIRejectsReplay 同一个随机串重放 → 拒绝。
func TestOpenAPIRejectsReplay(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丙")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err != nil {
		t.Fatalf("第一次请求应通过: %v", err)
	}
	// 同样的头再来一次。
	req2 := httptest.NewRequest(http.MethodPost, "/api/open/query", strings.NewReader(body))
	for _, h := range []string{apiKeyHeaderKey, apiKeyHeaderTimestamp, apiKeyHeaderNonce, apiKeyHeaderSignature} {
		req2.Header.Set(h, req.Header.Get(h))
	}
	if _, _, err := p.openPrincipal(req2, apiKeyScopePay); err == nil {
		t.Fatal("重放请求必须拒绝")
	}
}

// TestOpenAPIRejectsStaleTimestamp 时间戳超出窗口 → 拒绝。
func TestOpenAPIRejectsStaleTimestamp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丁")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	old := strconv.FormatInt(time.Now().Unix()-3600, 10)
	nonce := randomHex(8)
	body := `{"out_trade_no":"ORDER-1"}`
	sig := signRequest(secret, http.MethodPost, "/api/open/query", old, nonce, []byte(body))
	req := httptest.NewRequest(http.MethodPost, "/api/open/query", strings.NewReader(body))
	req.Header.Set(apiKeyHeaderKey, rec.KeyID)
	req.Header.Set(apiKeyHeaderTimestamp, old)
	req.Header.Set(apiKeyHeaderNonce, nonce)
	req.Header.Set(apiKeyHeaderSignature, sig)
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err == nil {
		t.Fatal("过期时间戳必须拒绝")
	}
}

// TestOpenAPIScopeEnforcement 只有 refund 权限的密钥不能下单,也不能退款。
func TestOpenAPIScopeEnforcement(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户戊")
	rec, secret, err := store.CreateAPIKey(user.UserID, "只读", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/refund", "", body, rec.KeyID)
	if _, _, err := p.openPrincipal(req, apiKeyScopeRefund); err == nil {
		t.Fatal("没有 refund 权限的密钥必须被拒绝")
	}
	// 有 pay 权限:下单类接口放行。
	req = signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", body, rec.KeyID)
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err != nil {
		t.Fatalf("有 pay 权限时不应被拒绝: %v", err)
	}
}

// TestOpenAPIRevokedKeyRejected 吊销后的密钥立即失效。
func TestOpenAPIRevokedKeyRejected(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户己")
	rec, secret, err := store.CreateAPIKey(user.UserID, "待吊销", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	if err := store.RevokeAPIKey(user.UserID, rec.KeyID); err != nil {
		t.Fatalf("吊销密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err == nil {
		t.Fatal("已吊销的密钥必须被拒绝")
	}
}

// TestOpenAPICannotTouchOtherMerchantOrder 密钥不能碰别的商户的订单。
func TestOpenAPICannotTouchOtherMerchantOrder(t *testing.T) {
	p, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "ALICE-ORDER",
		UserID:     alice.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string("SUCCESS"),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(bob.UserID, "商户B的密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	// 商户 B 用自己的密钥去查商户 A 的订单 → 查不到。
	body := `{"out_trade_no":"ALICE-ORDER"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/order", "", body, rec.KeyID)
	principal, raw, err := p.openPrincipal(req, apiKeyScopePay)
	if err != nil {
		t.Fatalf("签名校验应通过: %v", err)
	}
	var out struct {
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeRaw(raw, &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if _, err := p.ownOrder(principal.User, out.OutTradeNo); err == nil {
		t.Fatal("商户 B 不能查到商户 A 的订单")
	}
}

// TestOpenAPIDisabledMerchantRejected 被平台禁用的商户,开放接口也用不了。
func TestOpenAPIDisabledMerchantRejected(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户辛")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	if err := store.SetUserDisabled(user.UserID, true); err != nil {
		t.Fatalf("禁用商户失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	if _, _, err := p.openPrincipal(req, apiKeyScopePay); err == nil {
		t.Fatal("被禁用的商户必须被拒绝")
	}
}

// TestAPIKeySecretStoredEncrypted 密钥明文不入库。
func TestAPIKeySecretStoredEncrypted(t *testing.T) {
	_, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户壬")
	key, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	var model APIKeyModel
	// 按真实的 key_id 精确查(之前写的是 "pk_" 前缀,永远查不到,那段是死代码)。
	if err := store.DB().Where("key_id = ?", key.KeyID).First(&model).Error; err != nil {
		t.Fatalf("按 key_id 查密钥失败: %v", err)
	}
	if model.SecretEnc == "" {
		t.Fatal("密钥密文不能为空")
	}
	if strings.Contains(model.SecretEnc, secret) {
		t.Fatal("密钥明文不能入库")
	}
	if model.SecretEnc == secret {
		t.Fatal("密钥原文不能入库")
	}
}

// TestCreateAPIKeyRequiresVerifyToken 创建密钥必须二次验证。
func TestCreateAPIKeyRequiresVerifyToken(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户癸")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/keys", strings.NewReader(`{"name":"x","scopes":["pay"]}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleCreateAPIKey(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无票据创建密钥应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestOpenAPIStatsGroupByApp 开放接口的分组统计要能用应用维度。
func TestOpenAPIStatsGroupByApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户甲")
	app, err := store.SaveApp(user.UserID, "", "iOS 客户端", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "APP-ORDER-1",
		UserID:     user.UserID,
		AppID:      app.AppID,
		Channel:    "wechat",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     string("SUCCESS"),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	req := signedRequest(t, secret, http.MethodGet, "/api/open/stats", "group_by=app", "", rec.KeyID)
	rw := httptest.NewRecorder()
	p.handleOpenStats(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("分组统计应成功,实际 %d: %s", rw.Code, rw.Body.String())
	}
	var out struct {
		GroupBy string `json:"group_by"`
		Groups  []struct {
			Key         string `json:"key"`
			Label       string `json:"label"`
			TotalAmount int64  `json:"total_amount"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(rw.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.GroupBy != "app" {
		t.Fatalf("group_by 不对: %q", out.GroupBy)
	}
	found := false
	for _, g := range out.Groups {
		if g.Key == app.AppID {
			found = true
			if g.Label != "iOS 客户端" {
				t.Fatalf("分组显示名不对: %q", g.Label)
			}
			if g.TotalAmount != 19900 {
				t.Fatalf("分组金额不对: %d", g.TotalAmount)
			}
		}
	}
	if !found {
		t.Fatalf("没有找到应用分组: %+v", out.Groups)
	}
}

// TestOpenAPIOrdersFilterByApp 开放接口的订单列表能按应用过滤。
func TestOpenAPIOrdersFilterByApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户甲")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	for _, o := range []struct {
		no    string
		appID string
	}{
		{"IN-APP", app.AppID},
		{"NO-APP", ""},
	} {
		if err := store.AddOrder(OrderRecord{
			OutTradeNo: o.no,
			UserID:     user.UserID,
			AppID:      o.appID,
			Channel:    "wechat",
			Amount:     100,
			AmountYuan: "1.00",
			Status:     string("SUCCESS"),
		}); err != nil {
			t.Fatalf("写入订单失败: %v", err)
		}
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	query := "app_id=" + app.AppID
	req := signedRequest(t, secret, http.MethodGet, "/api/open/orders", query, "", rec.KeyID)
	rw := httptest.NewRecorder()
	p.handleOpenOrders(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("订单列表应成功,实际 %d: %s", rw.Code, rw.Body.String())
	}
	var out struct {
		Orders []OrderRecord `json:"orders"`
		Total  int64         `json:"total"`
	}
	if err := json.Unmarshal(rw.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.Total != 1 || len(out.Orders) != 1 {
		t.Fatalf("过滤结果不对: total=%d orders=%d", out.Total, len(out.Orders))
	}
	if out.Orders[0].OutTradeNo != "IN-APP" {
		t.Fatalf("返回的订单不对: %q", out.Orders[0].OutTradeNo)
	}
	if out.Orders[0].AppID != app.AppID {
		t.Fatalf("订单里没有应用 ID: %q", out.Orders[0].AppID)
	}
}

// TestAPIKeyListNeverReturnsSecret 密钥列表里没有明文。
func TestAPIKeyListNeverReturnsSecret(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户子")
	_, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleAPIKeys(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("列出密钥应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatal("密钥列表不能返回明文")
	}
}

// TestRevokeAPIKeyOnlyOwnKeys 不能吊销别人的密钥。
func TestRevokeAPIKeyOnlyOwnKeys(t *testing.T) {
	_, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	rec, _, err := store.CreateAPIKey(alice.UserID, "A的密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	// 商户 B 吊销商户 A 的密钥 → 失败。
	if err := store.RevokeAPIKey(bob.UserID, rec.KeyID); err == nil {
		t.Fatal("不能吊销别人的密钥")
	}
	// 密钥仍然是启用状态。
	keys, err := store.APIKeys(alice.UserID)
	if err != nil {
		t.Fatalf("查询密钥失败: %v", err)
	}
	if len(keys) != 1 || !keys[0].Enabled {
		t.Fatalf("密钥状态不应改变: %+v", keys)
	}
}

// TestAdminEndpointRequiresAdminToken 平台管理接口必须带管理员令牌。
func TestAdminEndpointRequiresAdminToken(t *testing.T) {
	p, _ := newOpenPlatform(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/admin/merchants", nil)
	p.handleAdminMerchants(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无管理员令牌应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	// 配了令牌之后,错的令牌仍然拒绝。
	p.opts.AdminToken = "admin-secret"
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/admin/merchants", nil)
	req.Header.Set("X-Admin-Token", "wrong")
	p.handleAdminMerchants(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("错误管理员令牌应被拒绝,实际 %d", rec.Code)
	}
	// 正确的令牌放行。
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/admin/merchants", nil)
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminMerchants(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("正确管理员令牌应放行,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAdminDisableMerchant 管理员禁用商户后,该商户不能登录、令牌失效。
func TestAdminDisableMerchant(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丑")
	p.opts.AdminToken = "admin-secret"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants/"+user.UserID+"/status",
		strings.NewReader(`{"disabled":true}`))
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminSetMerchant(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("禁用商户应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	// 令牌立即失效。
	found, err := store.UserByToken(user.AccessToken)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}
	if found != nil {
		t.Fatal("被禁用商户的令牌必须立即失效")
	}
	// 登录也被拒绝。
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/login",
		strings.NewReader(`{"name":"商户丑","password":"whatever"}`))
	p.handleLogin(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("被禁用商户登录应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestOpenAPIForgedSignatureDoesNotBurnNonce 伪造签名的请求不能「用掉」随机串。
//
// 否则任何知道密钥 ID 的人都能提前占住随机串,
// 把商户自己的合法请求挤成「重放」。
func TestOpenAPIForgedSignatureDoesNotBurnNonce(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户戌")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"ORDER-1"}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := randomHex(8)

	// 伪造签名(用错误的密钥算)。
	forged := signedRequest(t, "wrong-secret", http.MethodPost, "/api/open/query", "", body, rec.KeyID)
	forged.Header.Set(apiKeyHeaderTimestamp, ts)
	forged.Header.Set(apiKeyHeaderNonce, nonce)
	if _, _, err := p.openPrincipal(forged, apiKeyScopePay); err == nil {
		t.Fatal("伪造签名必须被拒绝")
	}

	// 同一组随机串 + 正确签名 → 仍然可用。
	good := httptest.NewRequest(http.MethodPost, "/api/open/query", strings.NewReader(body))
	good.Header.Set(apiKeyHeaderKey, rec.KeyID)
	good.Header.Set(apiKeyHeaderTimestamp, ts)
	good.Header.Set(apiKeyHeaderNonce, nonce)
	good.Header.Set(apiKeyHeaderSignature, signRequest(secret, http.MethodPost, "/api/open/query", ts, nonce, []byte(body)))
	if _, _, err := p.openPrincipal(good, apiKeyScopePay); err != nil {
		t.Fatalf("伪造请求不应消耗随机串: %v", err)
	}
}

// TestOpenAPICanonicalStringStable 待签名字符串的算法固定(防止实现漂移)。
func TestOpenAPICanonicalStringStable(t *testing.T) {
	got := canonicalString("post", "/api/open/pay?a=1", "1700000000", "n1", []byte(`{"x":1}`))
	// 写死已知答案:sha256(`{"x":1}`) = 5041bf1f...
	//
	// 之前这条只验「摘要长度是 64」—— 摘要算法被换成任何同样 64 字符的东西
	// (换算法、换编码、换字段顺序)都能通过,等于没测。
	want := strings.Join([]string{
		"POST",
		"/api/open/pay?a=1",
		"1700000000",
		"n1",
		"5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22",
	}, "\n")
	if got != want {
		t.Fatalf("待签名字符串变了:\n期望 %q\n实际 %q", want, got)
	}
	// 再补一条:body 变了摘要必须跟着变(不能是写死的常量)。
	other := canonicalString("post", "/api/open/pay?a=1", "1700000000", "n1", []byte(`{"x":2}`))
	if other == got {
		t.Fatal("body 变了摘要必须跟着变")
	}
}

// TestOpenAPISignTargetIncludesQuery 查询串参与签名。
func TestOpenAPISignTargetIncludesQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/open/orders?page=2&size=10", nil)
	if got := signTarget(req); got != "/api/open/orders?page=2&size=10" {
		t.Fatalf("签名目标不对: %s", got)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/open/stats", nil)
	if got := signTarget(req); got != "/api/open/stats" {
		t.Fatalf("签名目标不对: %s", got)
	}
}

// TestOpenAPIOrdersScopedToMerchant 开放接口的订单列表只包含自己的订单。
func TestOpenAPIOrdersScopedToMerchant(t *testing.T) {
	p, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	for _, o := range []OrderRecord{
		{OutTradeNo: "A-1", UserID: alice.UserID, Channel: "wechat", Amount: 100, AmountYuan: "1.00", Status: "SUCCESS"},
		{OutTradeNo: "B-1", UserID: bob.UserID, Channel: "wechat", Amount: 200, AmountYuan: "2.00", Status: "SUCCESS"},
	} {
		if err := store.AddOrder(o); err != nil {
			t.Fatalf("写入订单失败: %v", err)
		}
	}
	rec, secret, err := store.CreateAPIKey(alice.UserID, "A的密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := signedRequest(t, secret, http.MethodGet, "/api/open/orders", "page=1&size=10", "", rec.KeyID)
	p.handleOpenOrders(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("订单列表应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var res struct {
		Orders []OrderRecord `json:"orders"`
		Total  int64         `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if res.Total != 1 {
		t.Fatalf("商户 A 只应看到 1 条订单,实际 %d", res.Total)
	}
	for _, o := range res.Orders {
		if o.UserID != alice.UserID {
			t.Fatalf("出现了别的商户的订单: %+v", o)
		}
	}
}
