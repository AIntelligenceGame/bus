package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 开放接口密钥与应用绑定(方案 A 的落地方式)。
//
// 商户的业务后端只有一把密钥,却要同时服务好几个产品。
// 不绑定应用时,客户端忘了传 app_id 会**静默**落到全局渠道配置上,
// 订单全进了「未分类」,报表对不上,当场还没有任何报错。
// 绑定之后 app_id 变成「密钥的一部分」——
//   - 不传 → 自动落到密钥绑定的应用;
//   - 传了别的应用 → 直接 400。
//
// 这些都在**同一个服务进程内**完成,不需要为每个产品单独起一套服务端。

// countingProvider 记下被调用了几次的假渠道。
type countingProvider struct {
	fakeProvider
	created int
}

func (f *countingProvider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	f.created++
	return &payment.PaymentSession{
		Channel:    f.channel,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		QRCode:     "pay://bound=1",
	}, nil
}

// boundKeyFixture 一次测试要用的全套东西。
type boundKeyFixture struct {
	p     *Platform
	store *Store
	user  *User
	app   *App
	// globalFake 全局渠道那套假渠道。
	globalFake *countingProvider
	// appFake 这个应用专属的那套假渠道。
	appFake *countingProvider
}

// newBoundKeyFixture 造一个商户 + 一个应用,并给「全局」和「这个应用」
// 各装一套**不同的**假渠道:哪一套被调用,就能证明钱走的是哪套配置。
func newBoundKeyFixture(t *testing.T) *boundKeyFixture {
	t.Helper()
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	globalFake := &countingProvider{fakeProvider: fakeProvider{channel: payment.ChannelWechat}}
	appFake := &countingProvider{fakeProvider: fakeProvider{channel: payment.ChannelWechat}}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: globalFake,
	}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: appFake,
	}
	return &boundKeyFixture{
		p: p, store: store, user: user, app: app,
		globalFake: globalFake, appFake: appFake,
	}
}

// openPay 用密钥调一次开放接口下单。
func (f *boundKeyFixture) openPay(t *testing.T, keyID, secret string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("序列化请求体失败: %v", err)
	}
	req := signedRequest(t, secret, http.MethodPost, "/api/open/pay", "", string(raw), keyID)
	rec := httptest.NewRecorder()
	f.p.handleOpenPay(rec, req)
	return rec
}

// TestAPIKeyBoundAppOrderLandsOnApp 绑定应用的密钥:不传 app_id 也落到那个应用。
//
// 这条是整套设计的核心断言:
//   - 订单的 app_id = 密钥绑定的应用(不是空、不是别的应用);
//   - 走的是**这个应用自己的**渠道配置(appFake 被调用,globalFake 没被调用)。
func TestAPIKeyBoundAppOrderLandsOnApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	rec, secret, err := f.store.CreateAPIKeyForApp(f.user.UserID, "小程序专用",
		[]string{apiKeyScopePay}, f.app.AppID, 0)
	if err != nil {
		t.Fatalf("创建绑定密钥失败: %v", err)
	}
	if rec.AppID != f.app.AppID {
		t.Fatalf("密钥记录里的应用不对: %q", rec.AppID)
	}

	res := f.openPay(t, rec.KeyID, secret, map[string]any{
		"out_trade_no": "BOUND-1",
		"channel":      "wechat",
		"amount":       100,
		"subject":      "绑定测试",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("下单失败: %d %s", res.Code, res.Body.String())
	}

	order, err := f.store.OrderByTradeNo(f.user.UserID, "BOUND-1")
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order == nil {
		t.Fatal("订单没落库")
	}
	if order.AppID != f.app.AppID {
		t.Fatalf("订单归属的应用不对: 期望 %q,实际 %q", f.app.AppID, order.AppID)
	}
	if f.appFake.created != 1 {
		t.Fatalf("没有用应用专属的渠道下单: appFake=%d", f.appFake.created)
	}
	if f.globalFake.created != 0 {
		t.Fatalf("竟然用了全局的渠道下单: globalFake=%d", f.globalFake.created)
	}
}

// TestOpenPayRejectsMismatchedApp 密钥绑了 A,请求却传 B → 直接拒绝。
//
// 传错应用比不传更危险:不传只是没归类,传错会把别的产品的钱记到 A 上。
func TestOpenPayRejectsMismatchedApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	other, err := f.store.SaveApp(f.user.UserID, "", "另一个产品", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	rec, secret, err := f.store.CreateAPIKeyForApp(f.user.UserID, "小程序专用",
		[]string{apiKeyScopePay}, f.app.AppID, 0)
	if err != nil {
		t.Fatalf("创建绑定密钥失败: %v", err)
	}

	res := f.openPay(t, rec.KeyID, secret, map[string]any{
		"out_trade_no": "BOUND-2",
		"channel":      "wechat",
		"amount":       100,
		"app_id":       other.AppID,
	})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("传错应用应该被拒绝,实际 %d: %s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "不能用于应用") {
		t.Fatalf("错误信息没说明是应用不匹配: %s", res.Body.String())
	}
	// 不能留下半成品订单。
	if order, _ := f.store.OrderByTradeNo(f.user.UserID, "BOUND-2"); order != nil {
		t.Fatal("被拒绝的下单不该留下订单记录")
	}
	if f.appFake.created != 0 || f.globalFake.created != 0 {
		t.Fatalf("被拒绝的下单不该真的去调渠道: app=%d global=%d",
			f.appFake.created, f.globalFake.created)
	}
}

// TestOpenPayBoundKeyAcceptsSameApp 密钥绑了 A,请求传的也是 A → 放行。
func TestOpenPayBoundKeyAcceptsSameApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	rec, secret, err := f.store.CreateAPIKeyForApp(f.user.UserID, "小程序专用",
		[]string{apiKeyScopePay}, f.app.AppID, 0)
	if err != nil {
		t.Fatalf("创建绑定密钥失败: %v", err)
	}
	res := f.openPay(t, rec.KeyID, secret, map[string]any{
		"out_trade_no": "BOUND-3",
		"channel":      "wechat",
		"amount":       100,
		"app_id":       f.app.AppID,
	})
	if res.Code != http.StatusOK {
		t.Fatalf("同一个应用应该放行: %d %s", res.Code, res.Body.String())
	}
	order, _ := f.store.OrderByTradeNo(f.user.UserID, "BOUND-3")
	if order == nil || order.AppID != f.app.AppID {
		t.Fatalf("订单归属的应用不对: %+v", order)
	}
}

// TestUnboundKeyKeepsGlobalBehavior 不绑定的密钥:行为和以前一模一样。
//
// 这条是「不改老行为」的保险:老的商户一把密钥管所有产品,
// 订单进「未分类」,报表里也能看到。
func TestUnboundKeyKeepsGlobalBehavior(t *testing.T) {
	f := newBoundKeyFixture(t)
	rec, secret, err := f.store.CreateAPIKey(f.user.UserID, "老密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	if rec.AppID != "" {
		t.Fatalf("不该绑定任何应用: %q", rec.AppID)
	}
	res := f.openPay(t, rec.KeyID, secret, map[string]any{
		"out_trade_no": "UNBOUND-1",
		"channel":      "wechat",
		"amount":       100,
	})
	if res.Code != http.StatusOK {
		t.Fatalf("下单失败: %d %s", res.Code, res.Body.String())
	}
	order, _ := f.store.OrderByTradeNo(f.user.UserID, "UNBOUND-1")
	if order == nil {
		t.Fatal("订单没落库")
	}
	if order.AppID != "" {
		t.Fatalf("不绑定的密钥不该把订单挂到某个应用下: %q", order.AppID)
	}
	if f.globalFake.created != 1 {
		t.Fatalf("应该走全局渠道: globalFake=%d", f.globalFake.created)
	}
}

// TestCreateAPIKeyForAppRejectsUnknownApp 绑一个不存在的应用 → 失败。
func TestCreateAPIKeyForAppRejectsUnknownApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	if _, _, err := f.store.CreateAPIKeyForApp(f.user.UserID, "坏密钥",
		[]string{apiKeyScopePay}, "NO-SUCH-APP", 0); err == nil {
		t.Fatal("绑不存在的应用竟然创建成功了")
	}
	// 别的商户的应用也不能绑。
	other := newTestUser(t, f.store, "别的商户")
	otherApp, err := f.store.SaveApp(other.UserID, "", "别人的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if _, _, err := f.store.CreateAPIKeyForApp(f.user.UserID, "坏密钥",
		[]string{apiKeyScopePay}, otherApp.AppID, 0); err == nil {
		t.Fatal("绑别的商户的应用竟然创建成功了")
	}
}

// TestAPIKeyListCarriesAppID 密钥列表里要能看到绑定的是哪个应用。
func TestAPIKeyListCarriesAppID(t *testing.T) {
	f := newBoundKeyFixture(t)
	if _, _, err := f.store.CreateAPIKeyForApp(f.user.UserID, "绑了的",
		[]string{apiKeyScopePay}, f.app.AppID, 0); err != nil {
		t.Fatalf("创建绑定密钥失败: %v", err)
	}
	if _, _, err := f.store.CreateAPIKey(f.user.UserID, "没绑的",
		[]string{apiKeyScopePay}, 0); err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	keys, err := f.store.APIKeys(f.user.UserID)
	if err != nil {
		t.Fatalf("取密钥列表失败: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("密钥数量不对: %d", len(keys))
	}
	bound := 0
	for _, k := range keys {
		if k.AppID == f.app.AppID {
			bound++
		} else if k.AppID != "" {
			t.Fatalf("不该有的应用 ID: %q", k.AppID)
		}
	}
	if bound != 1 {
		t.Fatalf("绑定的密钥数量不对: %d", bound)
	}
}

// TestCreateAPIKeyHandlerBindsApp 控制台建密钥时能选应用。
func TestCreateAPIKeyHandlerBindsApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	verifyToken, err := f.store.CreateVerifyToken(f.user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	body := `{"name":"绑定的","scopes":["pay"],"app_id":"` + f.app.AppID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/keys", strings.NewReader(body))
	req.Header.Set("X-User-Token", f.user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.p.handleCreateAPIKey(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("创建密钥失败: %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Key APIKeyRecord `json:"key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if resp.Key.AppID != f.app.AppID {
		t.Fatalf("返回的密钥没带应用 ID: %q", resp.Key.AppID)
	}
	// 库里也确认一下(光看响应不够,那是回显)。
	keys, err := f.store.APIKeys(f.user.UserID)
	if err != nil {
		t.Fatalf("取密钥列表失败: %v", err)
	}
	if len(keys) != 1 || keys[0].AppID != f.app.AppID {
		t.Fatalf("库里的应用 ID 不对: %+v", keys)
	}
}

// TestCreateAPIKeyHandlerRejectsUnknownApp 控制台建密钥时绑了不存在的应用 → 400。
func TestCreateAPIKeyHandlerRejectsUnknownApp(t *testing.T) {
	f := newBoundKeyFixture(t)
	verifyToken, err := f.store.CreateVerifyToken(f.user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"坏的","scopes":["pay"],"app_id":"NO-SUCH-APP"}`))
	req.Header.Set("X-User-Token", f.user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.p.handleCreateAPIKey(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("绑不存在的应用应该 400,实际 %d: %s", rec.Code, rec.Body.String())
	}
	// 不能真的建出密钥来。
	keys, err := f.store.APIKeys(f.user.UserID)
	if err != nil {
		t.Fatalf("取密钥列表失败: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("不该建出密钥: %d 把", len(keys))
	}
}
