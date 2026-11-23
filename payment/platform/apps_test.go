package platform

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

// wechatCfgForTest 造一个只有 AppID 的微信配置(测试里只关心「是不是那套」)。
func wechatCfgForTest(appID string) *wechat.Config {
	return &wechat.Config{AppID: appID, MchID: "1900000109"}
}

// TestAppLifecycle 建 / 改 / 删应用的完整流程。
func TestAppLifecycle(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")

	app, err := store.SaveApp(user.UserID, "", "iOS 客户端", "主 App")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if app.AppID == "" {
		t.Fatal("应用 ID 为空")
	}
	if app.Name != "iOS 客户端" {
		t.Fatalf("应用名不对: %q", app.Name)
	}

	// 改名。
	app2, err := store.SaveApp(user.UserID, app.AppID, "iOS 客户端 v2", "")
	if err != nil {
		t.Fatalf("改应用失败: %v", err)
	}
	if app2.Name != "iOS 客户端 v2" {
		t.Fatalf("改名没生效: %q", app2.Name)
	}

	// 别的商户改不了这个应用。
	other := newTestUser(t, store, "别的商户")
	if _, err := store.SaveApp(other.UserID, app.AppID, "抢走", ""); err == nil {
		t.Fatal("别的商户不该能改这个应用")
	}

	// 没有订单的应用可以删。
	if err := store.DeleteApp(user.UserID, app.AppID); err != nil {
		t.Fatalf("删除应用失败: %v", err)
	}
}

// TestDeleteAppRefusedWhenOrdersExist 应用下有订单时不许删。
//
// 订单是钱的事实:删了应用,那批订单就变成查不到、算不出报表的孤儿。
func TestDeleteAppRefusedWhenOrdersExist(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "有订单的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "APP-ORDER-1",
		UserID:     user.UserID,
		AppID:      app.AppID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	err = store.DeleteApp(user.UserID, app.AppID)
	if err == nil {
		t.Fatal("有订单的应用不该能删")
	}
	if !strings.Contains(err.Error(), "订单") {
		t.Fatalf("报错没说清楚: %v", err)
	}
}

// TestDeleteAppRefusedWhenKeysBound 还有密钥绑在这个应用上时不许删。
//
// 删了应用,那把密钥就指向一个不存在的应用:它列表里看着正常,
// 但每次下单都会被拒(「应用不存在」),商户完全想不到是删应用删掉了。
func TestDeleteAppRefusedWhenKeysBound(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "有密钥的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if _, _, err := store.CreateAPIKeyForApp(user.UserID, "小程序专用",
		[]string{apiKeyScopePay}, app.AppID, 0); err != nil {
		t.Fatalf("建密钥失败: %v", err)
	}
	err = store.DeleteApp(user.UserID, app.AppID)
	if err == nil {
		t.Fatal("还有密钥绑着的应用不该能删")
	}
	if !errors.Is(err, ErrAppHasKeys) {
		t.Fatalf("报错类型不对: %v", err)
	}
	// 吊销密钥之后就能删了。
	keys, err := store.APIKeys(user.UserID)
	if err != nil {
		t.Fatalf("列密钥失败: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("密钥数量不对: %d", len(keys))
	}
	if err := store.RevokeAPIKey(user.UserID, keys[0].KeyID); err != nil {
		t.Fatalf("吊销密钥失败: %v", err)
	}
	if err := store.DeleteApp(user.UserID, app.AppID); err != nil {
		t.Fatalf("吊销密钥后应能删应用: %v", err)
	}
}

// TestAppChannelOverridesGlobal 应用专属的渠道配置覆盖全局的。
func TestAppChannelOverridesGlobal(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	// 全局:微信启用、支付宝启用。
	for _, ch := range []string{"wechat", "alipay"} {
		if err := store.SaveChannel(user.UserID, ChannelConfig{
			Channel: ch,
			Enabled: true,
		}); err != nil {
			t.Fatalf("保存全局渠道失败: %v", err)
		}
	}
	// 应用专属:微信用另一套密钥(appid 不同),支付宝在这个产品上禁用。
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
		Wechat:  wechatCfgForTest("wx-app-specific"),
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "alipay",
		Enabled: false, // 这个产品先别用支付宝
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}

	merged, err := store.EffectiveChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("EffectiveChannels: %v", err)
	}
	byChannel := map[string]ChannelConfig{}
	for _, c := range merged {
		byChannel[c.Channel] = c
	}
	w := byChannel["wechat"]
	if w.Wechat == nil || w.Wechat.AppID != "wx-app-specific" {
		t.Fatalf("应用专属的微信配置没生效: %+v", w.Wechat)
	}
	if byChannel["alipay"].Enabled {
		t.Fatal("应用专属的「不启用」应该覆盖全局的启用")
	}

	// 全局那边不受影响。
	//
	// 这里必须点名道姓地查:之前是个「for 循环 + if alipay 且被禁用才失败」,
	// 如果全局列表里压根没有 alipay,循环一次都不进,测试照样绿 ——
	// 「全局被污染」这种回归根本测不出来。
	global, err := store.EffectiveChannels(user.UserID, "")
	if err != nil {
		t.Fatalf("EffectiveChannels(全局): %v", err)
	}
	globalByChannel := map[string]ChannelConfig{}
	for _, c := range global {
		globalByChannel[c.Channel] = c
	}
	alipay, ok := globalByChannel["alipay"]
	if !ok {
		t.Fatalf("全局列表里没有支付宝: %+v", global)
	}
	if !alipay.Enabled {
		t.Fatal("全局的支付宝配置被应用配置改了")
	}
	gw, ok := globalByChannel["wechat"]
	if !ok {
		t.Fatalf("全局列表里没有微信: %+v", global)
	}
	// 全局的微信 AppID 不能被换成应用专属那套密钥。
	if gw.Wechat != nil && gw.Wechat.AppID == "wx-app-specific" {
		t.Fatalf("全局的微信配置被应用配置污染了: %+v", gw.Wechat)
	}
}

// TestAppChannelOrderCount 应用专属渠道的订单数统计。
//
// 注意:删除保护(有订单不许删专属配置)在 handler 里,见
// handlers_gap_test.go 的 TestDeleteAppChannelRejectedWhenOrdersExist(断言 409)。
func TestAppChannelOrderCount(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "APP-CH-1",
		UserID:     user.UserID,
		AppID:      app.AppID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	count, err := store.AppChannelOrderCount(user.UserID, app.AppID, "wechat")
	if err != nil {
		t.Fatalf("AppChannelOrderCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("订单数不对: %d", count)
	}
}

// TestNotifyURLCarriesAppID 回调地址要带应用标识。
//
// 验签用的是**下单那套**密钥,地址里不带应用标识的话,平台会用全局那套
// 密钥去验 —— 签名必然对不上,订单永远停在待支付。
func TestNotifyURLCarriesAppID(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	got := p.notifyURLFor(payment.ChannelWechat, user, user.UserID, app.AppID)
	if !strings.Contains(got, app.AppID) {
		t.Fatalf("回调地址里没有应用 ID: %s", got)
	}
	if !strings.HasPrefix(got, "https://pay.example.com/notify/wechat/") {
		t.Fatalf("回调地址前缀不对: %s", got)
	}
	// 全局的地址不带应用标识。
	global := p.notifyURLForUser(payment.ChannelWechat, user, user.UserID)
	if strings.Contains(global, app.AppID) {
		t.Fatalf("全局回调地址不该带应用 ID: %s", global)
	}
}

// TestProviderCacheKeyedByApp 不同应用要用各自的 provider 缓存。
//
// 两个微信 App 各配各的商户号 —— 缓存键如果不带应用,第二个应用会拿到
// 第一个应用的 provider,收款直接进错账户。
func TestProviderCacheKeyedByApp(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	globalFake := &fakeProvider{channel: payment.ChannelWechat}
	appFake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: globalFake,
	}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: appFake,
	}

	got, err := p.appProviders(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("appProviders: %v", err)
	}
	if got[payment.ChannelWechat] != appFake {
		t.Fatal("应用维度取到了全局的 provider")
	}
	gotGlobal, err := p.appProviders(user.UserID, "")
	if err != nil {
		t.Fatalf("appProviders(全局): %v", err)
	}
	if gotGlobal[payment.ChannelWechat] != globalFake {
		t.Fatal("全局维度取错了 provider")
	}
}

// TestInvalidateUserClearsAppProviders 清缓存时要把应用维度的缓存一起清掉。
//
// 不清的话,商户改了某个应用的渠道配置,那个应用还在用旧的 provider ——
// 页面显示「已保存」,但收款还是走的旧商户号。
func TestInvalidateUserClearsAppProviders(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: &fakeProvider{channel: payment.ChannelWechat},
	}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: &fakeProvider{channel: payment.ChannelWechat},
	}
	p.invalidateUser(user.UserID)
	if len(p.providers) != 0 {
		t.Fatalf("缓存没清干净: %d 条", len(p.providers))
	}
}

// TestRefundUsesOrderApp 退款必须用**订单归属的应用**那套凭证。
//
// 用别的商户号去退这笔单,渠道那边根本认不出来 —— 钱退不掉,
// 商户却以为已经退了。
func TestRefundUsesOrderApp(t *testing.T) {
	p, store, user, globalFake := newRefundTestPlatform(t, "APP-REFUND-1", 10000)
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	appFake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[providerCacheKey(user.UserID, app.AppID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: appFake,
	}
	// 这笔订单属于 app。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "APP-REFUND-2",
		UserID:     user.UserID,
		AppID:      app.AppID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: "100.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}

	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "APP-REFUND-2",
		"out_refund_no": "R-APP-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("退款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if appFake.refundCalls != 1 {
		t.Fatalf("应用专属的 provider 没被调用: %d", appFake.refundCalls)
	}
	if globalFake.refundCalls != 0 {
		t.Fatalf("退款用错了 provider(走了全局那套): %d", globalFake.refundCalls)
	}
}

// TestPayRejectsForeignApp 不能把订单挂到别人家的应用上。
func TestPayRejectsForeignApp(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户甲")
	other := newTestUser(t, store, "商户乙")
	otherApp, err := store.SaveApp(other.UserID, "", "乙的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	raw, _ := json.Marshal(map[string]any{
		"app_id":       otherApp.AppID,
		"amount":       100,
		"out_trade_no": "FOREIGN-1",
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/pay", strings.NewReader(string(raw)))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handlePay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("挂到别人的应用上应被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
}
