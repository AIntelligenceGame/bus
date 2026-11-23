package platform

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/platform/openapi"
)

// TestOpenAPIClientSignatureMatchesPlatform 客户端的签名算法必须和平台端一致。
//
// 两边各写一份实现,只要有一处对不上(比如 body 摘要、换行、十六进制大小写),
// 商户就会得到「签名不正确」,而这个错误很难自己看出来。
func TestOpenAPIClientSignatureMatchesPlatform(t *testing.T) {
	body := []byte(`{"channel":"wechat","amount":19900}`)
	cases := []struct {
		method, target, ts, nonce string
		body                      []byte
	}{
		{"POST", "/api/open/pay", "1700000000", "nonce123", body},
		{"GET", "/api/open/orders?page=1&size=20", "1700000001", "nonce456", nil},
		{"POST", "/api/open/refund", "1700000002", "nonce789", []byte("")},
	}
	for _, c := range cases {
		got := openapi.Sign("sk_test", c.method, c.target, c.ts, c.nonce, c.body)
		want := signRequest("sk_test", c.method, c.target, c.ts, c.nonce, c.body)
		if got != want {
			t.Fatalf("签名不一致(%s %s):\n客户端 %s\n平台   %s", c.method, c.target, got, want)
		}
	}
}

// TestOpenAPIClientEndToEnd 拿真实 SDK 打真实平台(httptest),确认能验签通过。
func TestOpenAPIClientEndToEnd(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户乙")
	key, secret, err := store.CreateAPIKey(user.UserID, "测试密钥",
		[]string{apiKeyScopePay, apiKeyScopeRefund}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	// 种一笔成功订单,后面的统计才有具体数字可断言。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "E2E-001",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     19900,
		AmountYuan: payment.CentsToYuan(19900),
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("造订单失败: %v", err)
	}
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	c := openapi.New(srv.URL, key.KeyID, secret)
	ctx := context.Background()

	orders, err := c.Orders(ctx, openapi.OrdersQuery{Page: 1, Size: 10})
	if err != nil {
		t.Fatalf("订单列表调用失败: %v", err)
	}
	if orders.Page != 1 || orders.Size != 10 {
		t.Fatalf("分页参数没回显: %+v", orders)
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("统计调用失败: %v", err)
	}
	// 断言具体数字,不能只断言「不为 nil」——
	// 之前那条只查了 stats == nil,统计算错算成 0 也照样绿。
	if stats.OrderCount != 1 {
		t.Fatalf("订单笔数不对: 期望 1,实际 %d", stats.OrderCount)
	}
	if stats.TotalAmount != 19900 {
		t.Fatalf("订单总额不对: 期望 19900,实际 %d", stats.TotalAmount)
	}
	if stats.SuccessCount != 1 {
		t.Fatalf("成功笔数不对: 期望 1,实际 %d", stats.SuccessCount)
	}
	if stats.NetAmount != 19900 {
		t.Fatalf("净收入不对: 期望 19900,实际 %d", stats.NetAmount)
	}
}

// TestOpenAPIClientRejectsBadSignature 密钥不对时必须被拒绝。
func TestOpenAPIClientRejectsBadSignature(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户丙")
	key, _, err := store.CreateAPIKey(user.UserID, "测试密钥", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	bad := openapi.New(srv.URL, key.KeyID, "sk_wrong_secret")
	if _, err := bad.Orders(context.Background(), openapi.OrdersQuery{Page: 1}); err == nil {
		t.Fatal("错误的密钥必须被拒绝")
	}
}

// TestOpenAPIClientScopeIsolation 只有 pay 权限的密钥不能退款。
//
// 关键:必须有一笔**真实存在**的订单。
// 之前这里退的是 "NO-SUCH-ORDER" —— 就算把 requireScope 整段删掉,
// 退款也会因为「订单不存在」失败,断言照样通过,权限校验形同虚设。
func TestOpenAPIClientScopeIsolation(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户丁")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "SCOPE-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: payment.CentsToYuan(10000),
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("造订单失败: %v", err)
	}
	key, secret, err := store.CreateAPIKey(user.UserID, "只下单", []string{apiKeyScopePay}, 0)
	if err != nil {
		t.Fatalf("创建密钥失败: %v", err)
	}
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	c := openapi.New(srv.URL, key.KeyID, secret)
	_, err = c.Refund(context.Background(), openapi.RefundRequest{
		OutTradeNo:   "SCOPE-1",
		OutRefundNo:  "R-1",
		RefundAmount: 5000,
	})
	if err == nil {
		t.Fatal("没有 refund 权限的密钥不能退款")
	}
	// 拒绝的**理由**必须是权限不足 —— 别的原因(订单不存在之类)不算数。
	if !strings.Contains(err.Error(), "权限") {
		t.Fatalf("拒绝原因应是权限不足,实际: %v", err)
	}
	if fake.refundCalls != 0 {
		t.Fatalf("被拒的退款不该调渠道,实际调了 %d 次", fake.refundCalls)
	}
	total, err := store.RefundedAmount(user.UserID, "SCOPE-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 0 {
		t.Fatalf("被拒的退款不该产生退款流水,实际已退 %d", total)
	}
}
