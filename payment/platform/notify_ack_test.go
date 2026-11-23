package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// TestAlipayNotifyAckIsPlainSuccess 支付宝回调必须回纯文本 success。
//
// 官方要求:商家收到异步通知后要输出 success(正好 7 个字符、纯文本、
// 不能带空格换行、不能是 JSON),否则支付宝认为通知没收到 ——
// 先立刻重发 3 次,再按 4m/10m/10m/1h/2h/6h/15h 重发,跨度 24 小时 22 分。
// 官方文档:opendocs.alipay.com/support/01raw4
//
// 以前这里回的是 {"ok":true} —— 支付宝会当成失败,一直重发。
func TestAlipayNotifyAckIsPlainSuccess(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户测试")
	fake := &fakeProvider{channel: payment.ChannelAlipay}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelAlipay: fake,
	}
	const orderNo = "ALI-ACK-1"
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: orderNo,
		UserID:     user.UserID,
		Channel:    "alipay",
		Amount:     10000,
		AmountYuan: payment.CentsToYuan(10000),
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelAlipay,
		OutTradeNo: orderNo,
		Amount:     10000,
		Status:     payment.TradeStatusSuccess,
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/notify/alipay/"+user.UserID, strings.NewReader(`{"trade_status":"TRADE_SUCCESS"}`))
	req.SetPathValue("channel", "alipay")
	req.SetPathValue("user_id", user.UserID)
	p.handleNotify(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("回调应答状态码不对: %d", rec.Code)
	}
	if got := rec.Body.String(); got != "success" {
		t.Fatalf("支付宝回调应答必须是纯文本 success,实际 %q", got)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("支付宝回调应答的 Content-Type 不该是 JSON: %q", ct)
	}
}

// TestWechatNotifyAckIsJSON 微信回调只看 HTTP 状态码,回 JSON 即可。
func TestWechatNotifyAckIsJSON(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户测试")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	const orderNo = "WX-ACK-1"
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: orderNo,
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: payment.CentsToYuan(10000),
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: orderNo,
		Amount:     10000,
		Status:     payment.TradeStatusSuccess,
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/notify/wechat/"+user.UserID, strings.NewReader(`{"event_type":"TRANSACTION.SUCCESS"}`))
	req.SetPathValue("channel", "wechat")
	req.SetPathValue("user_id", user.UserID)
	p.handleNotify(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("回调应答状态码不对: %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("微信回调应答应当是 JSON: %q", rec.Body.String())
	}
}
