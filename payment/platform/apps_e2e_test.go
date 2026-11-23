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

// 端到端:走真实路由(mux),把「建应用 → 配应用专属渠道 → 下单 → 查订单 →
// 报表」串一遍。
//
// 这一层能抓到只在 HTTP 层才暴露的问题:路由没注册、查询参数没解析、
// 响应结构与前端期望对不上 —— 单元测试直接调 handler 是抓不到的。

// e2ePlatform 造一个走真实 mux 的测试平台。
func e2ePlatform(t *testing.T) (*Platform, *Store) {
	t.Helper()
	store := newTestStore(t)
	p, _, _, _ := newFeaturePlatform(t)
	p.store = store
	p.providers = map[string]map[payment.Channel]payment.Provider{}
	return p, store
}

// e2eCall 发一个带令牌(可选票据)的请求,返回状态码与解析后的 JSON。
func e2eCall(t *testing.T, p *Platform, method, path, body, token, verify string) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-User-Token", token)
	}
	if verify != "" {
		req.Header.Set("X-Verify-Token", verify)
	}
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

// TestE2EAppsFlow 端到端:应用维度全流程。
func TestE2EAppsFlow(t *testing.T) {
	p, store := e2ePlatform(t)
	user := newTestUser(t, store, "商户甲")

	// 1) 建应用(要票据)。
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	code, out := e2eCall(t, p, http.MethodPost, "/api/apps",
		`{"name":"iOS 客户端","remark":"主 App"}`, user.AccessToken, verifyToken)
	if code != http.StatusOK {
		t.Fatalf("建应用应成功,实际 %d: %v", code, out)
	}
	appID, _ := out["app"].(map[string]any)["app_id"].(string)
	if appID == "" {
		t.Fatalf("没有返回应用 ID: %v", out)
	}

	// 2) 重名要报错(409),并且说人话。
	code, out = e2eCall(t, p, http.MethodPost, "/api/apps",
		`{"name":"iOS 客户端"}`, user.AccessToken, verifyToken)
	if code != http.StatusConflict {
		t.Fatalf("重名应被拒,实际 %d: %v", code, out)
	}
	if !strings.Contains(toString(out["error"]), "已存在") {
		t.Fatalf("重名报错没说清楚: %v", out)
	}

	// 3) 应用列表:应用 + 未分类汇总都要在。
	code, out = e2eCall(t, p, http.MethodGet, "/api/apps", "", user.AccessToken, "")
	if code != http.StatusOK {
		t.Fatalf("应用列表应成功,实际 %d: %v", code, out)
	}
	apps, _ := out["apps"].([]any)
	if len(apps) != 1 {
		t.Fatalf("应用列表条数不对: %v", out)
	}
	if _, ok := out["unclassified"].(map[string]any); !ok {
		t.Fatalf("缺少未分类汇总: %v", out)
	}

	// 4) 应用渠道配置:没配过就是继承全局。
	code, out = e2eCall(t, p, http.MethodGet, "/api/apps/"+appID+"/channels", "",
		user.AccessToken, "")
	if code != http.StatusOK {
		t.Fatalf("应用渠道应成功,实际 %d: %v", code, out)
	}
	chans, _ := out["channels"].([]any)
	if len(chans) != 0 {
		t.Fatalf("没有专属配置时不该有渠道: %v", chans)
	}
	notify, _ := out["notify_urls"].(map[string]any)
	if notify == nil {
		t.Fatalf("缺少回调地址: %v", out)
	}
	if url, _ := notify["wechat"].(string); !strings.Contains(url, appID) {
		t.Fatalf("回调地址里没有应用 ID: %q", url)
	}

	// 5) 下单(带 app_id),订单要归到那个应用下。
	//    这里用假 provider,免得真去调微信。
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[providerCacheKey(user.UserID, appID)] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	verifyToken2, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	code, out = e2eCall(t, p, http.MethodPost, "/api/pay",
		`{"app_id":"`+appID+`","channel":"wechat","amount":100,"out_trade_no":"E2E-001"}`,
		user.AccessToken, verifyToken2)
	if code != http.StatusOK {
		t.Fatalf("下单应成功,实际 %d: %v", code, out)
	}

	// 6) 订单列表:带 app_id 过滤能查到这笔单。
	code, out = e2eCall(t, p, http.MethodGet, "/api/orders?app_id="+appID, "",
		user.AccessToken, "")
	if code != http.StatusOK {
		t.Fatalf("订单列表应成功,实际 %d: %v", code, out)
	}
	orders, _ := out["orders"].([]any)
	if len(orders) != 1 {
		t.Fatalf("按应用过滤的订单数不对: %v", out)
	}
	order, _ := orders[0].(map[string]any)
	if order["app_id"] != appID {
		t.Fatalf("订单里的应用 ID 不对: %v", order)
	}

	// 7) 报表:按应用分组能查到这一档。
	code, out = e2eCall(t, p, http.MethodGet, "/api/stats?group_by=app", "",
		user.AccessToken, "")
	if code != http.StatusOK {
		t.Fatalf("分组统计应成功,实际 %d: %v", code, out)
	}
	if out["group_by"] != "app" {
		t.Fatalf("group_by 不对: %v", out)
	}
	groups, _ := out["groups"].([]any)
	found := false
	for _, g := range groups {
		gm, _ := g.(map[string]any)
		if gm["key"] == appID {
			found = true
		}
	}
	if !found {
		t.Fatalf("分组里没有这个应用: %v", groups)
	}

	// 8) 删应用:有订单必须被拒。
	code, out = e2eCall(t, p, http.MethodDelete, "/api/apps/"+appID, "",
		user.AccessToken, verifyToken2)
	if code != http.StatusConflict {
		t.Fatalf("有订单的应用不该能删,实际 %d: %v", code, out)
	}
	if !strings.Contains(toString(out["error"]), "订单") {
		t.Fatalf("删除报错没说清楚: %v", out)
	}
}

// TestE2EAppsRequireAuth 应用接口要令牌与票据。
func TestE2EAppsRequireAuth(t *testing.T) {
	p, store := e2ePlatform(t)
	user := newTestUser(t, store, "商户甲")

	// 没令牌 → 401。
	code, _ := e2eCall(t, p, http.MethodGet, "/api/apps", "", "", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("没令牌应被拒,实际 %d", code)
	}

	// 有令牌但没票据 → 403(建应用要二次验证)。
	code, _ = e2eCall(t, p, http.MethodPost, "/api/apps", `{"name":"x"}`, user.AccessToken, "")
	if code != http.StatusForbidden {
		t.Fatalf("无票据建应用应被拒,实际 %d", code)
	}
}

// TestE2EStatsAppFilterIsolation 应用过滤不能串到别的商户。
func TestE2EStatsAppFilterIsolation(t *testing.T) {
	p, store := e2ePlatform(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")
	bobApp, err := store.SaveApp(bob.UserID, "", "乙的应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	// 商户 A 查商户 B 的应用 → 400(应用不存在),不能拿到 B 的统计。
	code, out := e2eCall(t, p, http.MethodGet, "/api/stats?app_id="+bobApp.AppID, "",
		alice.AccessToken, "")
	if code != http.StatusBadRequest {
		t.Fatalf("查别人的应用应被拒,实际 %d: %v", code, out)
	}
}

// toString 安全地把 any 转成字符串。
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
