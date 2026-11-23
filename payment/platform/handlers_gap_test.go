package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 补齐此前没有测试覆盖的 handler 与 store 方法。
//
// 这些接口以前「能跑但没人验过」:改坏了不会有人红。
// 这里逐个把它们的行为钉死 —— 尤其是几条边界:
//   - 开放接口的下单 / 查单 / 关单 / 钱包扣款;
//   - 密钥吊销(只能吊销自己的);
//   - 平台管理建号 / 平台统计;
//   - 应用专属渠道配置的删除(有订单时必须拒绝);
//   - 退款流水、审计日志、退出登录。

// ---------------------------------------------------------------- 开放接口下单

// TestOpenPayHandlerCreatesOrder 开放接口下单要真的落一条订单。
func TestOpenPayHandlerCreatesOrder(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户甲")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"OPEN-1","subject":"iPhone","amount":19900}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", body, rec.KeyID)
	w := httptest.NewRecorder()
	p.handleOpenPay(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("开放下单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		OrderNo string `json:"order_no"`
		Amount  int64  `json:"amount"`
		Yuan    string `json:"yuan"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.OrderNo != "OPEN-1" || out.Amount != 19900 || out.Yuan != "199.00" {
		t.Fatalf("下单结果不对: %+v", out)
	}
	// 订单必须真的在库里。
	saved, err := store.OrderByTradeNo(user.UserID, "OPEN-1")
	if err != nil || saved == nil {
		t.Fatalf("订单没落库: %v", err)
	}
	if saved.Status != string(payment.TradeStatusPending) {
		t.Fatalf("订单状态不对: %s", saved.Status)
	}
}

// TestOpenPayRejectsUnknownApp 应用不存在时不能下单(否则订单挂到空气上)。
func TestOpenPayRejectsUnknownApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户乙")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"OPEN-2","amount":100,"app_id":"app_nonexistent"}`
	req := signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", body, rec.KeyID)
	w := httptest.NewRecorder()
	p.handleOpenPay(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("不存在的应用应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
	// 订单不该落库。
	if saved, _ := store.OrderByTradeNo(user.UserID, "OPEN-2"); saved != nil {
		t.Fatal("下单失败时不该留下订单记录")
	}
}

// TestOpenPayRejectsDuplicateTradeNo 重复的单号不能重复下单。
func TestOpenPayRejectsDuplicateTradeNo(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丙")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"OPEN-DUP","amount":100}`
	// 第一次:成功。
	w := httptest.NewRecorder()
	p.handleOpenPay(w, signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("第一次下单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	// 第二次:409。
	w = httptest.NewRecorder()
	p.handleOpenPay(w, signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", body, rec.KeyID))
	if w.Code != http.StatusConflict {
		t.Fatalf("重复单号应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestOpenPayRejectsBadBody 请求体不合法时不能下单。
func TestOpenPayRejectsBadBody(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丁")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	w := httptest.NewRecorder()
	p.handleOpenPay(w, signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", `{"amount":`, rec.KeyID))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("坏请求体应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------- 开放接口查单

// TestOpenQueryHandler 开放接口查单。
func TestOpenQueryHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户戊")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "Q-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"Q-1"}`
	w := httptest.NewRecorder()
	p.handleOpenQuery(w, signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("查单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		Status  string `json:"status"`
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.Status == "" {
		t.Fatalf("查单结果没有状态: %+v", out)
	}
}

// TestOpenQueryRequiresTradeNo 缺订单号要报错。
func TestOpenQueryRequiresTradeNo(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户己")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	w := httptest.NewRecorder()
	p.handleOpenQuery(w, signedRequest(t, secret, http.MethodPost, "/api/open/query", "", `{"channel":"wechat"}`, rec.KeyID))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("缺订单号应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestOpenOrderHandler 开放接口查订单详情。
func TestOpenOrderHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户庚")
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "O-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"O-1"}`
	w := httptest.NewRecorder()
	p.handleOpenOrder(w, signedRequest(t, secret, http.MethodPost, "/api/open/order", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("查订单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		OutTradeNo string `json:"out_trade_no"`
		Status     string `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.OutTradeNo != "O-1" || out.Status != "SUCCESS" {
		t.Fatalf("订单详情不对: %+v", out)
	}
}

// TestOpenOrderNotFound 查不到订单返回 404。
func TestOpenOrderNotFound(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户辛")
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"NOPE"}`
	w := httptest.NewRecorder()
	p.handleOpenOrder(w, signedRequest(t, secret, http.MethodPost, "/api/open/order", "", body, rec.KeyID))
	if w.Code != http.StatusNotFound {
		t.Fatalf("查不到订单应返回 404,实际 %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------- 开放接口关单

// TestOpenCloseHandler 开放接口关单:待支付订单能关掉。
func TestOpenCloseHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户壬")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo:     "C-1",
		UserID:         user.UserID,
		Channel:        "wechat",
		Amount:         100,
		AmountYuan:     "1.00",
		Status:         string(payment.TradeStatusPending),
		ChannelCreated: true,
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"C-1"}`
	w := httptest.NewRecorder()
	p.handleOpenClose(w, signedRequest(t, secret, http.MethodPost, "/api/open/close", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("关单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	saved, _ := store.OrderByTradeNo(user.UserID, "C-1")
	if saved == nil || saved.Status != string(payment.TradeStatusClosed) {
		t.Fatalf("订单没被关掉: %+v", saved)
	}
	if fake.closeCalls == 0 {
		t.Fatal("渠道侧没有建单时不该调渠道关单")
	}
}

// TestOpenCloseRejectsPaidOrder 已支付的订单不能关。
func TestOpenCloseRejectsPaidOrder(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户癸")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "C-2",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"C-2"}`
	w := httptest.NewRecorder()
	p.handleOpenClose(w, signedRequest(t, secret, http.MethodPost, "/api/open/close", "", body, rec.KeyID))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("已支付订单不该能关,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestOpenCloseWithoutChannelCreated 渠道侧没建单时关单不去调渠道。
func TestOpenCloseWithoutChannelCreated(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户子")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "C-3",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusPending),
		// ChannelCreated 默认 false:上次下单没成功。
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"wechat","out_trade_no":"C-3"}`
	w := httptest.NewRecorder()
	p.handleOpenClose(w, signedRequest(t, secret, http.MethodPost, "/api/open/close", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("关单应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	if fake.closeCalls != 0 {
		t.Fatal("渠道侧没有这笔单,不该去调渠道关单")
	}
	saved, _ := store.OrderByTradeNo(user.UserID, "C-3")
	if saved == nil || saved.Status != string(payment.TradeStatusClosed) {
		t.Fatalf("订单没被关掉: %+v", saved)
	}
}

// ---------------------------------------------------------------- 开放接口钱包扣款

// TestOpenWalletHandler 开放接口的钱包扣款要走 WalletProvider。
func TestOpenWalletHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丑")
	fake := &fakeWalletProvider{}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelApplePay: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "W-1",
		UserID:     user.UserID,
		Channel:    "applepay",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试",
		[]string{apiKeyScopePay, apiKeyScopeWallet}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"applepay","out_trade_no":"W-1","payment_token":"tok-1"}`
	w := httptest.NewRecorder()
	p.handleOpenWallet(w, signedRequest(t, secret, http.MethodPost, "/api/open/wallet/charge", "", body, rec.KeyID))
	if w.Code != http.StatusOK {
		t.Fatalf("钱包扣款应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	if fake.chargedToken != "tok-1" || fake.chargedAmount != 19900 {
		t.Fatalf("扣款参数不对: %+v", fake)
	}
	saved, _ := store.OrderByTradeNo(user.UserID, "W-1")
	if saved == nil || saved.Status != string(payment.TradeStatusSuccess) {
		t.Fatalf("订单状态没更新: %+v", saved)
	}
}

// TestOpenWalletRejectsTamperedAmount 客户端传了不一样的金额要拒绝。
func TestOpenWalletRejectsTamperedAmount(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户寅")
	fake := &fakeWalletProvider{}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelApplePay: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "W-2",
		UserID:     user.UserID,
		Channel:    "applepay",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	rec, secret, err := store.CreateAPIKey(user.UserID, "测试",
		[]string{apiKeyScopePay, apiKeyScopeWallet}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"channel":"applepay","out_trade_no":"W-2","payment_token":"tok","amount":1}`
	w := httptest.NewRecorder()
	p.handleOpenWallet(w, signedRequest(t, secret, http.MethodPost, "/api/open/wallet/charge", "", body, rec.KeyID))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("金额不一致应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
	if fake.chargedToken != "" {
		t.Fatal("金额被拒时不该真的扣款")
	}
}

// TestOpenWalletRequiresWalletScope 没有 wallet 权限的密钥不能扣款。
func TestOpenWalletRequiresWalletScope(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户卯")
	rec, secret, err := store.CreateAPIKey(user.UserID, "只下单",
		[]string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"W-3","payment_token":"tok"}`
	w := httptest.NewRecorder()
	p.handleOpenWallet(w, signedRequest(t, secret, http.MethodPost, "/api/open/wallet/charge", "", body, rec.KeyID))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("没 wallet 权限应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------- 密钥吊销

// TestRevokeAPIKeyHandler 吊销自己的密钥:能吊销,之后密钥失效。
func TestRevokeAPIKeyHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户辰")
	rec, secret, err := store.CreateAPIKey(user.UserID, "待吊销", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/keys/"+rec.KeyID+"/revoke", strings.NewReader(""))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleRevokeAPIKey(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("吊销应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	// 吊销后这把密钥不能再用。
	body := `{"out_trade_no":"X"}`
	w2 := httptest.NewRecorder()
	p.handleOpenQuery(w2, signedRequest(t, secret, http.MethodPost, "/api/open/query", "", body, rec.KeyID))
	if w2.Code == http.StatusOK {
		t.Fatal("吊销后的密钥不能再调接口")
	}
}

// TestRevokeAPIKeyRequiresVerifyToken 吊销密钥要二次验证票据。
func TestRevokeAPIKeyRequiresVerifyToken(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户巳")
	rec, _, err := store.CreateAPIKey(user.UserID, "测试", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/keys/"+rec.KeyID+"/revoke", strings.NewReader(""))
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleRevokeAPIKey(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("无票据吊销应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestRevokeAPIKeyCannotRevokeOthers 不能吊销别人的密钥。
func TestRevokeAPIKeyCannotRevokeOthers(t *testing.T) {
	p, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	rec, _, err := store.CreateAPIKey(alice.UserID, "A的密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(bob.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/keys/"+rec.KeyID+"/revoke", strings.NewReader(""))
	req.Header.Set("X-User-Token", bob.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleRevokeAPIKey(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("吊销别人的密钥应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
	// A 的密钥必须还是启用状态。
	keys, err := store.APIKeys(alice.UserID)
	if err != nil {
		t.Fatalf("查询密钥失败: %v", err)
	}
	if len(keys) != 1 || !keys[0].Enabled {
		t.Fatalf("别人的密钥不该被吊销: %+v", keys)
	}
}

// TestDescribeTTL 有效期的描述文案。
func TestDescribeTTL(t *testing.T) {
	if got := describeTTL(0); got != "长期有效" {
		t.Fatalf("TTL 描述不对: %s", got)
	}
	if got := describeTTL(30 * 24 * time.Hour); got != "30 天" {
		t.Fatalf("TTL 描述不对: %s", got)
	}
}

// ---------------------------------------------------------------- 平台管理

// TestAdminCreateMerchant 管理员建号。
func TestAdminCreateMerchant(t *testing.T) {
	p, store := newOpenPlatform(t)
	p.opts.AdminToken = "admin-secret"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants",
		strings.NewReader(`{"name":"新商户","password":"至少8位的口令"}`))
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminCreateMerchant(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("建号应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	// 账户确实存在了。
	found, err := store.UserByName("新商户")
	if err != nil || found == nil {
		t.Fatalf("商户没建出来: %v", err)
	}
	// 口令能用。
	if !verifyPassword(found.PasswordHash, "至少8位的口令") {
		t.Fatal("建号时的口令校验不过")
	}
}

// TestAdminCreateMerchantRejectsShortPassword 口令太短要拒绝。
func TestAdminCreateMerchantRejectsShortPassword(t *testing.T) {
	p, _ := newOpenPlatform(t)
	p.opts.AdminToken = "admin-secret"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants",
		strings.NewReader(`{"name":"新商户","password":"123"}`))
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminCreateMerchant(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("短口令应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestAdminCreateMerchantRejectsDuplicateName 重名要返回 409。
func TestAdminCreateMerchantRejectsDuplicateName(t *testing.T) {
	p, store := newOpenPlatform(t)
	p.opts.AdminToken = "admin-secret"
	newTestUser(t, store, "已有商户")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants",
		strings.NewReader(`{"name":"已有商户","password":"至少8位的口令"}`))
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminCreateMerchant(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("重名应返回 409,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestAdminCreateMerchantRequiresAdminToken 没令牌不能建号。
func TestAdminCreateMerchantRequiresAdminToken(t *testing.T) {
	p, _ := newOpenPlatform(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants",
		strings.NewReader(`{"name":"新商户","password":"至少8位的口令"}`))
	p.handleAdminCreateMerchant(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("没令牌应被拒,实际 %d", w.Code)
	}
}

// TestAdminStats 平台整体统计。
func TestAdminStats(t *testing.T) {
	p, store := newOpenPlatform(t)
	p.opts.AdminToken = "admin-secret"
	user := newTestUser(t, store, "商户午")
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "PS-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/admin/stats", nil)
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminStats(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("平台统计应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		MerchantCount int64 `json:"merchant_count"`
		OrderCount    int64 `json:"order_count"`
		TotalAmount   int64 `json:"total_amount"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.MerchantCount != 1 || out.OrderCount != 1 || out.TotalAmount != 19900 {
		t.Fatalf("平台统计不对: %+v", out)
	}
}

// TestAdminStatsRequiresAdminToken 没令牌看不到平台统计。
func TestAdminStatsRequiresAdminToken(t *testing.T) {
	p, _ := newOpenPlatform(t)
	p.opts.AdminToken = "admin-secret"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/admin/stats", nil)
	req.Header.Set("X-Admin-Token", "wrong")
	p.handleAdminStats(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("错令牌应被拒,实际 %d", w.Code)
	}
}

// ---------------------------------------------------------------- 应用专属渠道

// TestDeleteAppChannelHandler 删除应用专属渠道配置。
func TestDeleteAppChannelHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户未")
	app, err := store.SaveApp(user.UserID, "", "iOS", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat", Enabled: true, IsDefault: true,
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/apps/"+app.AppID+"/channels/wechat", strings.NewReader(""))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteAppChannel(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("删除应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	// 配置真的没了。
	specific, err := store.AppChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("查询应用渠道失败: %v", err)
	}
	if len(specific) != 0 {
		t.Fatalf("专属配置没删掉: %+v", specific)
	}
}

// TestDeleteAppChannelRejectedWhenOrdersExist 应用下有该渠道订单时不能删配置。
//
// 那批订单是用这套密钥下的,删了配置就退不了款了。
func TestDeleteAppChannelRejectedWhenOrdersExist(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户申")
	app, err := store.SaveApp(user.UserID, "", "iOS", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat", Enabled: true, IsDefault: true,
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "AC-1",
		UserID:     user.UserID,
		AppID:      app.AppID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("写入订单失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/apps/"+app.AppID+"/channels/wechat", strings.NewReader(""))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteAppChannel(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("有订单时删除应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
	// 配置必须还在。
	specific, _ := store.AppChannels(user.UserID, app.AppID)
	if len(specific) != 1 {
		t.Fatalf("被拒后配置不该被删: %+v", specific)
	}
}

// TestDeleteAppChannelUnknownChannel 未知渠道要拒绝。
func TestDeleteAppChannelUnknownChannel(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户酉")
	app, err := store.SaveApp(user.UserID, "", "iOS", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/apps/"+app.AppID+"/channels/notachannel", strings.NewReader(""))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteAppChannel(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("未知渠道应被拒,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestDeleteAppChannelOtherMerchantApp 不能删别的商户的应用配置。
func TestDeleteAppChannelOtherMerchantApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	app, err := store.SaveApp(alice.UserID, "", "A的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(bob.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/apps/"+app.AppID+"/channels/wechat", strings.NewReader(""))
	req.Header.Set("X-User-Token", bob.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteAppChannel(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("删别人的应用应返回 404,实际 %d: %s", w.Code, w.Body.String())
	}
}

// TestStoreDefaultChannelFor 应用专属的默认渠道覆盖全局的。
func TestStoreDefaultChannelFor(t *testing.T) {
	_, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户戌")
	app, err := store.SaveApp(user.UserID, "", "iOS", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	// 全局:微信是默认。
	if err := store.SaveChannel(user.UserID, ChannelConfig{
		Channel: "wechat", Enabled: true, IsDefault: true,
	}); err != nil {
		t.Fatalf("保存全局渠道失败: %v", err)
	}
	got, err := store.DefaultChannelFor(user.UserID, "")
	if err != nil {
		t.Fatalf("取默认渠道失败: %v", err)
	}
	if got != "wechat" {
		t.Fatalf("全局默认渠道不对: %q", got)
	}
	// 应用专属:支付宝是默认。
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "alipay", Enabled: true, IsDefault: true,
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	got, err = store.DefaultChannelFor(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("取默认渠道失败: %v", err)
	}
	if got != "alipay" {
		t.Fatalf("应用专属默认渠道没覆盖全局: %q", got)
	}
	// 没配专属的应用仍然用全局的。
	other, err := store.SaveApp(user.UserID, "", "安卓", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	got, err = store.DefaultChannelFor(user.UserID, other.AppID)
	if err != nil {
		t.Fatalf("取默认渠道失败: %v", err)
	}
	if got != "wechat" {
		t.Fatalf("没配专属的应用应该用全局默认: %q", got)
	}
}

// TestStoreDefaultChannelForNoChannel 什么渠道都没配时返回空串。
func TestStoreDefaultChannelForNoChannel(t *testing.T) {
	_, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户亥")
	got, err := store.DefaultChannelFor(user.UserID, "")
	if err != nil {
		t.Fatalf("取默认渠道失败: %v", err)
	}
	if got != "" {
		t.Fatalf("没配渠道时应返回空串,实际 %q", got)
	}
}

// TestStoreDeleteAppChannelRejectsEmptyApp 不能删「默认应用」的渠道配置。
func TestStoreDeleteAppChannelRejectsEmptyApp(t *testing.T) {
	_, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户子")
	if err := store.DeleteAppChannel(user.UserID, "", "wechat"); err == nil {
		t.Fatal("app_id 为空时不能删渠道配置")
	}
}

// ---------------------------------------------------------------- 控制台接口

// TestRefundsAPI 退款流水接口。
func TestRefundsAPI(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丑")
	if err := store.AddRefund(RefundRecord{
		RefundID:    "RF-1",
		UserID:      user.UserID,
		OutTradeNo:  "O-1",
		OutRefundNo: "R-1",
		Channel:     "wechat",
		Amount:      5000,
		AmountYuan:  "50.00",
		Status:      "SUCCESS",
	}); err != nil {
		t.Fatalf("写入退款流水失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/refunds?limit=10", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleRefundsAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("退款流水应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		Refunds []RefundRecord `json:"refunds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	// RefundID 是入库时由 store 自己生成的(newID("r")),
	// 调用方传进来的 "RF-1" 会被覆盖 —— 所以这里只能断言商户自己填的
	// 商户退款单号 out_refund_no 和金额。
	if len(out.Refunds) != 1 {
		t.Fatalf("应有 1 条退款流水,实际 %d 条: %+v", len(out.Refunds), out.Refunds)
	}
	if out.Refunds[0].OutRefundNo != "R-1" {
		t.Fatalf("商户退款单号不对: %+v", out.Refunds[0])
	}
	if out.Refunds[0].Amount != 5000 {
		t.Fatalf("退款金额不对: %+v", out.Refunds[0])
	}
}

// TestRefundsAPINeedsToken 退款流水要令牌。
func TestRefundsAPINeedsToken(t *testing.T) {
	p, _ := newOpenPlatform(t)
	w := httptest.NewRecorder()
	p.handleRefundsAPI(w, httptest.NewRequest(http.MethodGet, "/api/refunds", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("没令牌应被拒,实际 %d", w.Code)
	}
}

// TestAuditLogsAPI 审计日志接口:能分页、能按动作过滤。
func TestAuditLogsAPI(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户寅")
	if err := store.AddAudit(user.UserID, "order.pay", "O-1", "x", "127.0.0.1"); err != nil {
		t.Fatalf("写入审计日志失败: %v", err)
	}
	if err := store.AddAudit(user.UserID, "account.login", "-", "y", "127.0.0.1"); err != nil {
		t.Fatalf("写入审计日志失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/audit-logs?page=1&size=10", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleAuditLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("审计日志应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	var out struct {
		Logs  []AuditLog `json:"logs"`
		Total int64      `json:"total"`
		Page  int        `json:"page"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.Total != 2 || len(out.Logs) != 2 {
		t.Fatalf("审计日志条数不对: total=%d logs=%d", out.Total, len(out.Logs))
	}
	// 按动作过滤。
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/audit-logs?action=order.pay", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleAuditLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("审计日志应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	out.Logs = nil
	out.Total = 0
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.Total != 1 || len(out.Logs) != 1 || out.Logs[0].Action != "order.pay" {
		t.Fatalf("按动作过滤不对: %+v", out)
	}
}

// TestAuditLogsOnlyOwnLogs 审计日志只返回自己的。
func TestAuditLogsOnlyOwnLogs(t *testing.T) {
	p, store := newOpenPlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	if err := store.AddAudit(alice.UserID, "order.pay", "O-1", "", "127.0.0.1"); err != nil {
		t.Fatalf("写入审计日志失败: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/audit-logs", nil)
	req.Header.Set("X-User-Token", bob.AccessToken)
	p.handleAuditLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("审计日志应成功,实际 %d", w.Code)
	}
	var out struct {
		Logs  []AuditLog `json:"logs"`
		Total int64      `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if out.Total != 0 || len(out.Logs) != 0 {
		t.Fatalf("不该看到别人的日志: %+v", out)
	}
}

// TestLogoutHandler 退出登录:旧令牌立刻失效。
func TestLogoutHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户卯")
	oldToken := user.AccessToken
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/logout", strings.NewReader(""))
	req.Header.Set("X-User-Token", oldToken)
	p.handleLogout(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("退出登录应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	// 旧令牌不能再用。
	found, err := store.UserByToken(oldToken)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}
	if found != nil {
		t.Fatal("退出登录后旧令牌必须失效")
	}
}

// TestLogoutNeedsToken 没令牌不能退出。
func TestLogoutNeedsToken(t *testing.T) {
	p, _ := newOpenPlatform(t)
	w := httptest.NewRecorder()
	p.handleLogout(w, httptest.NewRequest(http.MethodPost, "/api/logout", strings.NewReader("")))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("没令牌应被拒,实际 %d", w.Code)
	}
}

// ---------------------------------------------------------------- 备用二次验证

// TestDeleteMFABackupHandler 删除备用二次验证。
func TestDeleteMFABackupHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户辰")
	if err := store.SetMFABackup(user.UserID, MFATypeEmail, "backup@example.com"); err != nil {
		t.Fatalf("设置备用验证失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/account/mfa/backup",
		strings.NewReader(`{"password":""}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteMFABackup(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("删除备用验证应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	got, err := store.UserByID(user.UserID)
	if err != nil || got == nil {
		t.Fatalf("查询用户失败: %v", err)
	}
	if got.MFABackupType != "" || got.MFABackupTarget != "" {
		t.Fatalf("备用验证没删掉: %+v", got)
	}
}

// TestDeleteMFABackupWrongPassword 口令不对时不能删(403,不是 401)。
func TestDeleteMFABackupWrongPassword(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户巳")
	hash, err := hashPassword("supersecret")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if err := store.SetPassword(user.UserID, hash); err != nil {
		t.Fatalf("设置口令失败: %v", err)
	}
	if err := store.SetMFABackup(user.UserID, MFATypeEmail, "backup@example.com"); err != nil {
		t.Fatalf("设置备用验证失败: %v", err)
	}
	verify, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/account/mfa/backup",
		strings.NewReader(`{"password":"wrong-password"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verify)
	p.handleDeleteMFABackup(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("口令错误应返回 403,实际 %d: %s", w.Code, w.Body.String())
	}
	// 备用验证必须还在。
	got, _ := store.UserByID(user.UserID)
	if got.MFABackupType == "" {
		t.Fatal("口令错误时不该删掉备用验证")
	}
}
