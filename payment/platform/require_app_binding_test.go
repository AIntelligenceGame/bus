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

// 「订单必须绑定应用」模式。
//
// 用户的原话:「反正就是必须指定应用,不然无法区分是哪些应用的支付」。
//
// 不强制的时候,app_id 为空的下单会被放行,订单落进「未分类」——
// 钱进来了,但事后没人说得清是哪个产品的。这条测试覆盖强制模式下的行为。

// newForcedUser 建一个**开启了强制绑定**的商户。
func newForcedUser(t *testing.T, store *Store, name string) *User {
	t.Helper()
	user, err := store.CreateUser(name, "hash")
	if err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	if err := store.SetRequireAppBinding(user.UserID, true); err != nil {
		t.Fatalf("开启强制绑定失败: %v", err)
	}
	return user
}

// TestNewAccountRequiresAppBindingByDefault 新建的账户默认强制绑定应用。
//
// 这条是「从今天起不再产生未分类订单」的保证。
func TestNewAccountRequiresAppBindingByDefault(t *testing.T) {
	store := newTestStore(t)
	user, err := store.CreateUser("新商户", "hash")
	if err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	if !user.RequireAppBinding {
		t.Fatal("新建账户应当默认强制绑定应用")
	}
	// 再从库里读一遍,确认真的落库了(不是只在内存里)。
	got, err := store.UserByID(user.UserID)
	if err != nil {
		t.Fatalf("读用户失败: %v", err)
	}
	if got == nil || !got.RequireAppBinding {
		t.Fatalf("库里没记下强制绑定: %+v", got)
	}
}

// TestForcedModeRejectsPayWithoutApp 强制模式下不带 app_id 的下单被拒。
func TestForcedModeRejectsPayWithoutApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	fake := &countingProvider{fakeProvider: fakeProvider{channel: payment.ChannelWechat}}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	// 不带 app_id → 拒绝。
	req := httptest.NewRequest(http.MethodPost, "/api/pay",
		strings.NewReader(`{"out_trade_no":"FORCE-1","channel":"wechat","amount":100}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handlePay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("强制模式下不带 app_id 的下单应该被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "必须绑定应用") {
		t.Fatalf("错误信息没说明是强制绑定应用: %s", rec.Body.String())
	}
	if fake.created != 0 {
		t.Fatalf("被拒的下单不该调渠道: %d", fake.created)
	}
	if order, _ := store.OrderByTradeNo(user.UserID, "FORCE-1"); order != nil {
		t.Fatal("被拒的下单不该留下订单")
	}

	// 带上 app_id → 放行。
	req2 := httptest.NewRequest(http.MethodPost, "/api/pay",
		strings.NewReader(`{"out_trade_no":"FORCE-2","channel":"wechat","amount":100,"app_id":"`+app.AppID+`"}`))
	req2.Header.Set("X-User-Token", user.AccessToken)
	req2.Header.Set("X-Verify-Token", verifyToken)
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	p.handlePay(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("带 app_id 的下单应该成功,实际 %d: %s", rec2.Code, rec2.Body.String())
	}
	order, _ := store.OrderByTradeNo(user.UserID, "FORCE-2")
	if order == nil || order.AppID != app.AppID {
		t.Fatalf("订单归属的应用不对: %+v", order)
	}
}

// TestForcedModeRejectsUnboundKey 强制模式下没绑应用的密钥不能下单。
//
// 这是「客户端必须绑定应用」在开放接口上的落点:
// 商户的后端拿到的密钥没绑应用 → 下单直接被拒,不会静默进未分类。
func TestForcedModeRejectsUnboundKey(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	fake := &countingProvider{fakeProvider: fakeProvider{channel: payment.ChannelWechat}}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}

	// 没绑应用的密钥:下单被拒。
	unbound, unboundSecret, err := store.CreateAPIKey(user.UserID, "没绑的",
		[]string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	body := `{"out_trade_no":"FORCE-KEY-1","channel":"wechat","amount":100}`
	req := signedRequest(t, unboundSecret, http.MethodPost, "/api/open/pay", "", body, unbound.KeyID)
	rec := httptest.NewRecorder()
	p.handleOpenPay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("没绑应用的密钥下单应该被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.created != 0 {
		t.Fatalf("被拒的下单不该调渠道: %d", fake.created)
	}

	// 绑了应用的密钥:不用传 app_id 也能下单。
	bound, boundSecret, err := store.CreateAPIKeyForApp(user.UserID, "绑了的",
		[]string{apiKeyScopePay}, app.AppID, 0)
	if err != nil {
		t.Fatalf("创建绑定密钥失败: %v", err)
	}
	body2 := `{"out_trade_no":"FORCE-KEY-2","channel":"wechat","amount":100}`
	req2 := signedRequest(t, boundSecret, http.MethodPost, "/api/open/pay", "", body2, bound.KeyID)
	rec2 := httptest.NewRecorder()
	p.handleOpenPay(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("绑了应用的密钥下单应该成功,实际 %d: %s", rec2.Code, rec2.Body.String())
	}
	order, _ := store.OrderByTradeNo(user.UserID, "FORCE-KEY-2")
	if order == nil || order.AppID != app.AppID {
		t.Fatalf("订单归属的应用不对: %+v", order)
	}
}

// TestForcedModeRefusesUnboundKeyCreation 强制模式下建密钥必须选应用。
func TestForcedModeRefusesUnboundKeyCreation(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	if _, err := store.SaveApp(user.UserID, "", "小程序", ""); err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	// 不带 app_id → 拒绝。
	req := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"没绑的","scopes":["pay"]}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleCreateAPIKey(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("强制模式下建不绑应用的密钥应该被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
	keys, err := store.APIKeys(user.UserID)
	if err != nil {
		t.Fatalf("取密钥列表失败: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("不该建出密钥: %d 把", len(keys))
	}
}

// TestRequireBindingToggleNeedsVerifyToken 开关要二次验证。
//
// 这个开关一开,没带 app_id 的下单全被拒 —— 商户的业务可能当场中断,
// 不能只凭会话令牌就改。
func TestRequireBindingToggleNeedsVerifyToken(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	req := httptest.NewRequest(http.MethodPost, "/api/apps/require-binding",
		strings.NewReader(`{"require":false}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleSetRequireAppBinding(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无票据改开关应被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestRequireBindingToggleNeedsAnApp 一个应用都没有时不允许开启。
//
// 开了等于自断手脚:所有下单都会被拒,而商户没地方可去。
func TestRequireBindingToggleNeedsAnApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/apps/require-binding",
		strings.NewReader(`{"require":true}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleSetRequireAppBinding(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("没有应用时开启应被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestRequireBindingToggleRoundTrip 开关能来回切,并且立刻改变行为。
func TestRequireBindingToggleRoundTrip(t *testing.T) {
	p, store := newOpenPlatform(t)
	// 老账户:默认不强制。
	user := newTestUser(t, store, "老商户")
	if user.RequireAppBinding {
		t.Fatal("测试账户应当默认不强制")
	}
	if _, err := store.SaveApp(user.UserID, "", "小程序", ""); err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	set := func(require bool) *httptest.ResponseRecorder {
		body := `{"require":false}`
		if require {
			body = `{"require":true}`
		}
		req := httptest.NewRequest(http.MethodPost, "/api/apps/require-binding",
			strings.NewReader(body))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("X-Verify-Token", verifyToken)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		p.handleSetRequireAppBinding(rec, req)
		return rec
	}

	rec := set(true)
	if rec.Code != http.StatusOK {
		t.Fatalf("开启失败: %d %s", rec.Code, rec.Body.String())
	}
	got, err := store.UserByID(user.UserID)
	if err != nil {
		t.Fatalf("读用户失败: %v", err)
	}
	if got == nil || !got.RequireAppBinding {
		t.Fatal("开关没生效")
	}

	// 再关掉。
	rec = set(false)
	if rec.Code != http.StatusOK {
		t.Fatalf("关闭失败: %d %s", rec.Code, rec.Body.String())
	}
	got, err = store.UserByID(user.UserID)
	if err != nil {
		t.Fatalf("读用户失败: %v", err)
	}
	if got == nil || got.RequireAppBinding {
		t.Fatal("关闭没生效")
	}
}

// TestForcedModeBlocksDeletingLastApp 强制模式下不能删掉最后一个应用。
//
// 删了最后一个应用 → 建不了绑应用的密钥 → 下单也过不了 → 账户直接锁死。
func TestForcedModeBlocksDeletingLastApp(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	app, err := store.SaveApp(user.UserID, "", "唯一应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	req := httptest.NewRequest(http.MethodDelete, "/api/apps/"+app.AppID, nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	rec := httptest.NewRecorder()
	p.handleDeleteApp(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("删最后一个应用应该被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
	// 应用还在。
	if _, err := store.AppByID(user.UserID, app.AppID); err != nil {
		t.Fatalf("查应用失败: %v", err)
	} else if gone, _ := store.AppByID(user.UserID, app.AppID); gone == nil {
		t.Fatal("应用不该被删掉")
	}
}

// TestMeExposesRequireAppBinding 控制台要能看到当前是不是强制模式。
func TestMeExposesRequireAppBinding(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newForcedUser(t, store, "强制商户")
	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	rec := httptest.NewRecorder()
	p.handleMe(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("读取账户信息失败: %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		User struct {
			RequireAppBinding bool `json:"require_app_binding"`
		} `json:"user"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if !resp.User.RequireAppBinding {
		t.Fatal("/api/me 没有把强制绑定状态带出来")
	}
}
