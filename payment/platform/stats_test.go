package platform

import (
	"fmt"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// seedStatsData 造一点数据:两个应用 + 一个未分类,渠道有微信和支付宝。
func seedStatsData(t *testing.T, store *Store, userID string) {
	t.Helper()
	type ord struct {
		tradeNo string
		appID   string
		channel string
		amount  int64
		status  string
	}
	orders := []ord{
		{"A-1", "app_a", "wechat", 1000, string(payment.TradeStatusSuccess)},
		{"A-2", "app_a", "wechat", 2000, string(payment.TradeStatusSuccess)},
		{"A-3", "app_a", "alipay", 3000, string(payment.TradeStatusPending)},
		{"B-1", "app_b", "alipay", 4000, string(payment.TradeStatusSuccess)},
		{"U-1", "", "wechat", 5000, string(payment.TradeStatusSuccess)},
	}
	for _, o := range orders {
		if err := store.AddOrder(OrderRecord{
			OutTradeNo: o.tradeNo,
			UserID:     userID,
			AppID:      o.appID,
			Channel:    o.channel,
			TradeType:  "native",
			Subject:    "测试",
			Amount:     o.amount,
			AmountYuan: "0.01",
			Status:     o.status,
		}); err != nil {
			t.Fatalf("下单失败: %v", err)
		}
	}
	// 一笔成功退款(app_a),一笔失败退款(app_b)。
	// 失败的那笔**不该**计入已退金额 —— 那笔钱还在商户手里。
	if err := store.AddRefund(RefundRecord{
		UserID:      userID,
		OutTradeNo:  "A-1",
		OutRefundNo: "R-1",
		Channel:     "wechat",
		AppID:       "app_a",
		Amount:      500,
		Status:      refundStatusSuccess,
	}); err != nil {
		t.Fatalf("记录退款失败: %v", err)
	}
	if err := store.AddRefund(RefundRecord{
		UserID:      userID,
		OutTradeNo:  "B-1",
		OutRefundNo: "R-2",
		Channel:     "alipay",
		AppID:       "app_b",
		Amount:      600,
		Status:      refundStatusFailed,
	}); err != nil {
		t.Fatalf("记录退款失败: %v", err)
	}
}

// TestStatGroupsByApp 按应用分组:金额、笔数、退款口径都要对。
func TestStatGroupsByApp(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	seedStatsData(t, store, user.UserID)

	groups, truncated, err := store.StatGroups(user.UserID, "app", "")
	if err != nil {
		t.Fatalf("StatGroups: %v", err)
	}
	if truncated {
		t.Fatalf("按应用分组不该被截断")
	}
	byKey := map[string]StatGroup{}
	for _, g := range groups {
		byKey[g.Key] = g
	}
	a, ok := byKey["app_a"]
	if !ok {
		t.Fatalf("缺少 app_a 分组: %+v", groups)
	}
	if a.OrderCount != 3 || a.SuccessCount != 2 {
		t.Fatalf("app_a 笔数不对: %+v", a)
	}
	if a.TotalAmount != 3000 {
		t.Fatalf("app_a 成功金额不对: %d", a.TotalAmount)
	}
	if a.RefundCount != 1 || a.RefundAmount != 500 {
		t.Fatalf("app_a 退款不对: %+v", a)
	}
	if a.NetAmount != 2500 {
		t.Fatalf("app_a 净收入不对: %d", a.NetAmount)
	}
	// app_b 那笔退款是失败的,不计入。
	b := byKey["app_b"]
	if b.RefundCount != 0 || b.RefundAmount != 0 {
		t.Fatalf("失败退款不应计入: %+v", b)
	}
	if b.TotalAmount != 4000 {
		t.Fatalf("app_b 成功金额不对: %d", b.TotalAmount)
	}
	// 未分类那批老订单也要在。
	u, ok := byKey[""]
	if !ok {
		t.Fatalf("缺少未分类分组: %+v", groups)
	}
	if u.TotalAmount != 5000 {
		t.Fatalf("未分类金额不对: %d", u.TotalAmount)
	}
	if u.Label != "未分类" {
		t.Fatalf("未分类展示名不对: %q", u.Label)
	}
}

// TestStatGroupsReconcileWithStats 分组汇总必须和总数对得上。
//
// 概览页显示一个总数、报表页显示一堆分组,两组数字对不上是商户最常见的
// 「平台算错了」投诉来源。这里直接断言相等。
func TestStatGroupsReconcileWithStats(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	seedStatsData(t, store, user.UserID)

	stats, err := store.Stats(user.UserID)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	for _, dim := range []string{"app", "channel", "day"} {
		groups, truncated, err := store.StatGroups(user.UserID, dim, "")
		if err != nil {
			t.Fatalf("StatGroups(%s): %v", dim, err)
		}
		if truncated {
			t.Fatalf("%s 分组被截断了(数据量很小,不该触发上限)", dim)
		}
		var orderCount, successCount, refundCount, total, refundAmount int64
		for _, g := range groups {
			orderCount += g.OrderCount
			successCount += g.SuccessCount
			refundCount += g.RefundCount
			total += g.TotalAmount
			refundAmount += g.RefundAmount
		}
		if orderCount != stats.OrderCount {
			t.Fatalf("%s 订单数对不上: 分组 %d vs 总数 %d", dim, orderCount, stats.OrderCount)
		}
		if successCount != stats.SuccessCount {
			t.Fatalf("%s 成功数对不上: 分组 %d vs 总数 %d", dim, successCount, stats.SuccessCount)
		}
		if total != stats.TotalAmount {
			t.Fatalf("%s 成功金额对不上: 分组 %d vs 总数 %d", dim, total, stats.TotalAmount)
		}
		if refundCount != stats.RefundCount || refundAmount != stats.RefundAmount {
			t.Fatalf("%s 退款对不上: 分组 %d/%d vs 总数 %d/%d",
				dim, refundCount, refundAmount, stats.RefundCount, stats.RefundAmount)
		}
	}
}

// TestStatsForApp 单应用口径的总数要只算那个应用。
func TestStatsForApp(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	seedStatsData(t, store, user.UserID)

	stats, err := store.StatsForApp(user.UserID, "app_a")
	if err != nil {
		t.Fatalf("StatsForApp: %v", err)
	}
	if stats.OrderCount != 3 || stats.SuccessCount != 2 {
		t.Fatalf("app_a 统计不对: %+v", stats)
	}
	if stats.TotalAmount != 3000 || stats.RefundAmount != 500 || stats.NetAmount != 2500 {
		t.Fatalf("app_a 金额不对: %+v", stats)
	}
}

// TestStatGroupsByDay 按天分组:今天下的单要落在今天。
func TestStatGroupsByDay(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	seedStatsData(t, store, user.UserID)

	groups, truncated, err := store.StatGroups(user.UserID, "day", "")
	if err != nil {
		t.Fatalf("StatGroups(day): %v", err)
	}
	if truncated {
		t.Fatalf("按天分组不该被截断")
	}
	if len(groups) == 0 {
		t.Fatal("按天分组没有任何结果")
	}
	today := time.Now().Local().Format("2006-01-02")
	found := false
	for _, g := range groups {
		if g.Key == today {
			found = true
			if g.OrderCount != 5 {
				t.Fatalf("今天的订单数不对: %+v", g)
			}
		}
	}
	if !found {
		t.Fatalf("没有今天的分组(%s): %+v", today, groups)
	}
}

// TestStatGroupsByDaySortedDescending 按天分组必须按日期**倒序**返回。
//
// 之前这里是把 map 的遍历结果简单倒过来 —— map 遍历顺序是随机的,
// 倒过来也只是把随机顺序反了个个儿,每次刷新报表行的先后都是乱的。
// 前端是照后端给的顺序渲染的(自己不再排),所以这个乱序会直接显示出来。
func TestStatGroupsByDaySortedDescending(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")

	// 造三笔横跨不同日期的订单(本地时区)。
	// 直接写 model,因为 AddOrder 会把 CreatedAt 覆盖成「现在」。
	base := time.Now().Local()
	dates := []time.Time{
		base.AddDate(0, 0, -5),
		base.AddDate(0, 0, -3),
		base.AddDate(0, 0, -1),
	}
	for i, d := range dates {
		day := time.Date(d.Year(), d.Month(), d.Day(), 12, 0, 0, 0, time.Local)
		model := OrderModel{
			OutTradeNo: fmt.Sprintf("D-%d", i),
			UserID:     user.UserID,
			Channel:    "wechat",
			TradeType:  "native",
			Subject:    "按天统计",
			Amount:     100,
			AmountYuan: "1.00",
			Status:     string(payment.TradeStatusSuccess),
			CreatedAt:  formatTime(day),
			UpdatedAt:  formatTime(day),
		}
		if err := store.db.Create(&model).Error; err != nil {
			t.Fatalf("写入订单失败: %v", err)
		}
	}

	groups, _, err := store.StatGroups(user.UserID, "day", "")
	if err != nil {
		t.Fatalf("StatGroups(day): %v", err)
	}
	if len(groups) != len(dates) {
		t.Fatalf("应该 %d 组,实际 %d 组: %+v", len(dates), len(groups), groups)
	}
	for i := 1; i < len(groups); i++ {
		if groups[i-1].Key < groups[i].Key {
			t.Fatalf("按天分组必须按日期倒序,第 %d 组(%s)排在第 %d 组(%s)前面了: %+v",
				i-1, groups[i-1].Key, i, groups[i].Key, groups)
		}
	}
	// 最新的那天应该在最前面。
	wantFirst := dates[len(dates)-1].Format("2006-01-02")
	if groups[0].Key != wantFirst {
		t.Fatalf("最近的一天(%s)应该在最前面,实际第一组是 %s", wantFirst, groups[0].Key)
	}
}
