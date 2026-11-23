package platform

import (
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// TestNotifyPartialRefundStaysPartial 部分退款不能把订单标成「已退款」。
//
// 钱包渠道(Stripe)的 charge.refunded 事件里,amount 是**原扣款金额**,
// amount_refunded 才是已经退了多少(官方 Charge 对象的字段)。
// 只退了一半却标成「已退款」,报表口径就是错的 —— 商户会以为整单都退完了。
func TestNotifyPartialRefundStaysPartial(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "PARTIAL-1", 10000)
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: "PARTIAL-1",
		Amount:     10000,
		Status:     payment.TradeStatusRefunded,
		Raw:        map[string]string{"amount_refunded": "5000"},
	}

	notifyCall(t, p, user)

	var order OrderModel
	if err := store.db.Where("user_id = ? AND out_trade_no = ?", user.UserID, "PARTIAL-1").
		First(&order).Error; err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusPartiallyRefunded) {
		t.Fatalf("只退了一半,订单状态应该是 %s,实际 %s",
			payment.TradeStatusPartiallyRefunded, order.Status)
	}
}

// TestNotifyFullRefundMarksRefunded 全额退款才标成「已退款」。
func TestNotifyFullRefundMarksRefunded(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "FULL-1", 10000)
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: "FULL-1",
		Amount:     10000,
		Status:     payment.TradeStatusRefunded,
		Raw:        map[string]string{"amount_refunded": "10000"},
	}

	notifyCall(t, p, user)

	var order OrderModel
	if err := store.db.Where("user_id = ? AND out_trade_no = ?", user.UserID, "FULL-1").
		First(&order).Error; err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusRefunded) {
		t.Fatalf("全额退款后订单状态应该是 %s,实际 %s",
			payment.TradeStatusRefunded, order.Status)
	}
}
