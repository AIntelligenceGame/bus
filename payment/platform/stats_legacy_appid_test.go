package platform

import (
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// TestStatGroupsLegacyNullAppID 老库里的订单(app_id = NULL)不能从报表里消失。
//
// 背景:多产品维度是后加的,给订单表加 app_id 列用的是 AutoMigrate。
// AutoMigrate 加列**不会给存量行填默认值** —— 老行的 app_id 是 NULL,
// 不是空串。
//
// 而 SQL 的 GROUP BY 把 NULL 和空串当成**两组**,GORM 把两组的键都扫成空串,
// 在 Go 的 map 里后一组会把前一组**覆盖掉**。结果就是:
// 老订单那笔钱在「未分类」那一档里凭空少一块,而且不报任何错。
//
// 这个用例直接造一个「老表 + AutoMigrate 加列」的库复现这个坑,
// 断言 NULL 与空串的订单必须落在同一组、金额必须对得上。
func TestStatGroupsLegacyNullAppID(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")

	// 新写的订单:app_id 是空串(当前代码写出来的就是空串)。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "NEW-1",
		UserID:     user.UserID,
		AppID:      "",
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "新订单",
		Amount:     200,
		AmountYuan: "2.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}

	// 模拟老库:把这一行的 app_id 改成 NULL(等价于 AutoMigrate 加列后
	// 存量行没被填默认值的样子)。
	if err := store.db.Model(&OrderModel{}).
		Where("out_trade_no = ? AND user_id = ?", "NEW-1", user.UserID).
		Update("app_id", nil).Error; err != nil {
		t.Fatalf("构造 NULL app_id 失败: %v", err)
	}

	// 再补一笔正常的新订单(app_id = ''),模拟「建了应用之后又下的单」。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "NEW-2",
		UserID:     user.UserID,
		AppID:      "",
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "又一笔",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}

	groups, _, err := store.StatGroups(user.UserID, "app", "")
	if err != nil {
		t.Fatalf("StatGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("应该只有「未分类」一组,实际 %d 组: %+v", len(groups), groups)
	}
	g := groups[0]
	if g.Key != "" {
		t.Fatalf("分组键不对: %q", g.Key)
	}
	if g.OrderCount != 2 {
		t.Fatalf("订单数不对(NULL 那笔被吞了?): %+v", g)
	}
	if g.TotalAmount != 300 {
		t.Fatalf("成功金额不对(NULL 那笔的钱丢了?): %+v", g)
	}
	if g.Label != "未分类" {
		t.Fatalf("展示名不对: %q", g.Label)
	}

	// 退款侧同样不能丢:退款表也有 app_id 列,老库那批退款的 app_id 也是 NULL。
	if err := store.AddRefund(RefundRecord{
		UserID:      user.UserID,
		OutTradeNo:  "NEW-1",
		OutRefundNo: "R-NULL",
		Channel:     "wechat",
		AppID:       "",
		Amount:      50,
		Status:      refundStatusSuccess,
	}); err != nil {
		t.Fatalf("记录退款失败: %v", err)
	}
	if err := store.db.Model(&RefundModel{}).
		Where("out_refund_no = ? AND user_id = ?", "R-NULL", user.UserID).
		Update("app_id", nil).Error; err != nil {
		t.Fatalf("构造 NULL app_id 失败: %v", err)
	}

	groups, _, err = store.StatGroups(user.UserID, "app", "")
	if err != nil {
		t.Fatalf("StatGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("应该只有「未分类」一组,实际 %d 组: %+v", len(groups), groups)
	}
	if groups[0].RefundCount != 1 || groups[0].RefundAmount != 50 {
		t.Fatalf("退款不该被吞: %+v", groups[0])
	}
}
