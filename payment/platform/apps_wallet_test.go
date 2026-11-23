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

// 钱包渠道(Apple Pay / Google Pay)在**应用维度**下的行为。
//
// 用户的原话:「我看只有微信和支付宝,别的支付,好像没有设置应用指定?」
//
// 也就是说:应用专属的渠道配置这一套(选应用 → 配自己的一套密钥 →
// 这个应用的订单走这套密钥),钱包渠道是不是也能用?
//
// 这条测试把钱包渠道当成和微信/支付宝**完全平级**的渠道来对待:
//   - 能在应用下保存专属配置;
//   - 专属配置覆盖全局;
//   - 应用下的订单用专属配置下的收单密钥。
//
// 如果钱包渠道真的只支持全局,那这条测试会红 —— 那就是要修的缺陷。

// walletCfgForTest 造一份钱包收单配置。
func walletCfgForTest(merchantID string) *WalletSettings {
	return &WalletSettings{
		MerchantID:           merchantID,
		DisplayName:          "收银台",
		DomainName:           "pay.example.com",
		Currency:             "CNY",
		StripeSecretKey:      "sk_test_" + merchantID,
		StripePublishableKey: "pk_test_" + merchantID,
		StripeWebhookSecret:  "whsec_" + merchantID,
	}
}

// TestWalletChannelSavesAppSpecificConfig 钱包渠道能存在应用专属配置里。
func TestWalletChannelSavesAppSpecificConfig(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "iOS 客户端", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	// 全局先配一套钱包(收单商户号 A)。
	body := `{"enabled":true,"is_default":false,"wallet":{"merchant_id":"merchant-global",` +
		`"display_name":"全局","domain_name":"pay.example.com","currency":"CNY",` +
		`"stripe_secret_key":"sk_global","stripe_publishable_key":"pk_global"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/channels/applepay",
		strings.NewReader(body))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleSaveChannel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("保存全局钱包配置失败: %d %s", rec.Code, rec.Body.String())
	}

	// 应用下配专属的一套(收单商户号 B)。
	body2 := `{"enabled":true,"is_default":false,"wallet":{"merchant_id":"merchant-app",` +
		`"display_name":"应用","domain_name":"pay.example.com","currency":"CNY",` +
		`"stripe_secret_key":"sk_app","stripe_publishable_key":"pk_app"}}`
	req2 := httptest.NewRequest(http.MethodPost,
		"/api/channels/applepay?app_id="+app.AppID, strings.NewReader(body2))
	req2.Header.Set("X-User-Token", user.AccessToken)
	req2.Header.Set("X-Verify-Token", verifyToken)
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	p.handleSaveChannel(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("保存应用专属钱包配置失败: %d %s", rec2.Code, rec2.Body.String())
	}

	// 应用专属那一行必须真的落库了。
	specific, err := store.AppChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("AppChannels: %v", err)
	}
	if len(specific) != 1 {
		t.Fatalf("应用下应该有 1 条专属渠道配置,实际 %d", len(specific))
	}
	if specific[0].Channel != "applepay" {
		t.Fatalf("专属配置的渠道不对: %s", specific[0].Channel)
	}
	if specific[0].Wallet == nil {
		t.Fatal("专属配置里没有钱包配置")
	}
	if specific[0].Wallet.MerchantID != "merchant-app" {
		t.Fatalf("专属配置的收单商户号不对: %s", specific[0].Wallet.MerchantID)
	}

	// 合并结果:应用下走专属那套,全局下走全局那套。
	merged, err := store.EffectiveChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("EffectiveChannels: %v", err)
	}
	var found bool
	for _, c := range merged {
		if c.Channel != "applepay" {
			continue
		}
		found = true
		if c.Wallet == nil || c.Wallet.MerchantID != "merchant-app" {
			t.Fatalf("应用下的钱包配置没生效: %+v", c.Wallet)
		}
	}
	if !found {
		t.Fatal("合并结果里没有 applepay")
	}
	global, err := store.EffectiveChannels(user.UserID, "")
	if err != nil {
		t.Fatalf("EffectiveChannels(全局): %v", err)
	}
	for _, c := range global {
		if c.Channel == "applepay" && (c.Wallet == nil || c.Wallet.MerchantID != "merchant-global") {
			t.Fatalf("全局的钱包配置被应用配置污染了: %+v", c.Wallet)
		}
	}
}

// TestWalletChannelAppScopeSavedViaHandler 走 HTTP 处理器保存应用专属钱包配置。
//
// 上面那条测的是 store 层;这条测的是**接口层**:
// 控制台的「渠道配置」页选了应用之后,点「保存 Apple Pay 配置」,
// 请求是 POST /api/channels/applepay?app_id=xxx。
//
// 这条路径如果没带上 app_id(或者带了但后端不认),用户的配置就白存了 ——
// 而且是静默存到全局去了,用户根本发现不了。
func TestWalletChannelAppScopeSavedViaHandler(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "安卓 App", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	body := `{"enabled":true,"is_default":false,"wallet":{"merchant_id":"merchant-handler",` +
		`"display_name":"应用","domain_name":"pay.example.com","currency":"CNY",` +
		`"stripe_secret_key":"sk_handler","stripe_publishable_key":"pk_handler"}}`
	req := httptest.NewRequest(http.MethodPost,
		"/api/channels/googlepay?app_id="+app.AppID, strings.NewReader(body))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleSaveChannel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("保存失败: %d %s", rec.Code, rec.Body.String())
	}

	// 必须存成**应用专属**,而不是全局。
	specific, err := store.AppChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("AppChannels: %v", err)
	}
	if len(specific) != 1 || specific[0].Channel != "googlepay" {
		t.Fatalf("应用专属配置没落库: %+v", specific)
	}
	globalSpecific, err := store.AppChannels(user.UserID, "")
	if err != nil {
		t.Fatalf("AppChannels(全局): %v", err)
	}
	if len(globalSpecific) != 0 {
		t.Fatalf("不该存成全局配置: %+v", globalSpecific)
	}
}

// TestWalletChannelAppProviderUsesAppConfig 下单时用的 provider 必须来自应用专属配置。
//
// 前面几条测的是「存进去了」「接口能看到」。这条测的是**真正干活的那一层**:
// appProviders(userID, appID) 返回的 provider,是拿应用专属配置建的,
// 而不是拿全局那套。
//
// 验证手法:全局**什么都不配**,只给应用配一套钱包。
//   - 应用维度必须能拿到 applepay 的 provider(有专属配置);
//   - 全局维度必须拿不到(全局确实没配)。
//
// 如果代码里哪里偷懒、直接用全局配置,这两条断言就会红。
func TestWalletChannelAppProviderUsesAppConfig(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "iOS 客户端", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	// 只配应用,不配全局。
	body := `{"enabled":true,"is_default":false,"wallet":{"merchant_id":"merchant-only-app",` +
		`"display_name":"应用","domain_name":"pay.example.com","currency":"CNY",` +
		`"stripe_secret_key":"sk_only_app","stripe_publishable_key":"pk_only_app"}}`
	req := httptest.NewRequest(http.MethodPost,
		"/api/channels/applepay?app_id="+app.AppID, strings.NewReader(body))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleSaveChannel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("保存应用专属钱包配置失败: %d %s", rec.Code, rec.Body.String())
	}

	// 应用维度:必须有 applepay。
	appProviders, err := p.appProviders(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("appProviders: %v", err)
	}
	if _, ok := appProviders[payment.ChannelApplePay]; !ok {
		t.Fatalf("应用维度没有 applepay provider: %+v", appProviders)
	}
	// 全局维度:全局没配,就不该有。
	globalProviders, err := p.appProviders(user.UserID, "")
	if err != nil {
		t.Fatalf("appProviders(全局): %v", err)
	}
	if _, ok := globalProviders[payment.ChannelApplePay]; ok {
		t.Fatal("全局本来没配钱包,却冒出了 applepay provider")
	}
}

// TestAppChannelsResponseIncludesWallet 应用维度接口要能看到钱包渠道。
//
// GET /api/apps/{app_id}/channels 返回的是**合并后**的渠道列表,
// 控制台「应用详情」用它来显示「专属配置 / 继承全局」。
// 钱包渠道也得在里头,不然用户看不到自己配的东西。
func TestAppChannelsResponseIncludesWallet(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "applepay",
		Enabled: true,
		Wallet:  walletCfgForTest("merchant-detail"),
	}); err != nil {
		t.Fatalf("保存应用钱包配置失败: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/apps/"+app.AppID+"/channels", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	rec := httptest.NewRecorder()
	p.handleAppChannels(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("读取失败: %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Channels []struct {
			Channel     string `json:"channel"`
			AppSpecific bool   `json:"app_specific"`
			Wallet      *struct {
				MerchantID string `json:"merchant_id"`
			} `json:"wallet"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	var found bool
	for _, c := range resp.Channels {
		if c.Channel != "applepay" {
			continue
		}
		found = true
		if !c.AppSpecific {
			t.Fatal("钱包渠道没被标成「专属配置」")
		}
		if c.Wallet == nil || c.Wallet.MerchantID != "merchant-detail" {
			t.Fatalf("钱包配置摘要没带回来: %+v", c.Wallet)
		}
	}
	if !found {
		t.Fatalf("应用维度接口没返回 applepay: %s", rec.Body.String())
	}
}
