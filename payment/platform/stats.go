package platform

import (
	"fmt"
	"sort"
	"strings"

	"github.com/AIntelligenceGame/bus/payment"
)

// 按维度分组的统计(报表)。
//
// 有了这一层,商户就能问「我这个月 iOS 端赚了多少」「微信渠道的成功率是多少」
// 「哪一天流水最高」—— 而不必自己把 CSV 拉下来用 Excel 透视。
//
// 分组维度:
//   - app:按应用(产品)。一个商户多个产品时要能分开算。
//   - channel:按渠道。
//   - day:按天(本地时区的自然日)。
//
// 口径必须和 Stats() 完全一致:
//   - 「成功金额」只算 order_status = SUCCESS 的订单;
//   - 「已退金额」只算 refundCountedStatuses 里的退款
//     (占位与处理中也算 —— 钱可能已经出去了;失败的不算 —— 那笔钱还在商户手里)。
// 不一致的话,概览页的数字和报表页的数字会对不上,商户会以为平台算错了。

// groupBy 取分组维度;非法值归一成 channel。
func normalizeGroupBy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "app":
		return "app"
	case "day", "date":
		return "day"
	default:
		return "channel"
	}
}

// StatGroups 按维度分组统计。
//
// appID 非空 = 只统计该应用(不传则统计全部,含「未分类」)。
//
// truncated = true 表示结果**不完整**:按天分组时扫描的行数超过了
// maxStatGroupRows 上限,更早的数据没被统计进来。调用方要把这个标志
// 透给前端 —— 宁可明说「被截断了」,也不要给一份看着完整的错数据。
func (s *Store) StatGroups(userID, groupBy, appID string) ([]StatGroup, bool, error) {
	switch normalizeGroupBy(groupBy) {
	case "app":
		groups, err := s.statGroupsByApp(userID, appID)
		return groups, false, err
	case "day":
		return s.statGroupsByDay(userID, appID)
	default:
		groups, err := s.statGroupsByChannel(userID, appID)
		return groups, false, err
	}
}

// statGroupsByApp 按应用分组。
func (s *Store) statGroupsByApp(userID, appID string) ([]StatGroup, error) {
	groups, err := s.orderGroups(userID, "app_id", appID)
	if err != nil {
		return nil, err
	}
	refunds, err := s.refundGroups(userID, "app_id", appID)
	if err != nil {
		return nil, err
	}
	for key, g := range refunds {
		target, ok := groups[key]
		if !ok {
			target = StatGroup{Key: key}
			groups[key] = target
		}
		target.RefundCount += g.RefundCount
		target.RefundAmount += g.RefundAmount
		groups[key] = target
	}
	// 应用名做展示名。
	names := map[string]string{}
	apps, err := s.Apps(userID)
	if err != nil {
		return nil, err
	}
	for _, a := range apps {
		names[a.AppID] = a.Name
	}
	out := make([]StatGroup, 0, len(groups))
	for _, g := range groups {
		g.NetAmount = g.TotalAmount - g.RefundAmount
		g.Label = appLabel(names, g.Key)
		out = append(out, g)
	}
	sortStatGroups(out)
	return out, nil
}

// statGroupsByChannel 按渠道分组。
func (s *Store) statGroupsByChannel(userID, appID string) ([]StatGroup, error) {
	groups, err := s.orderGroups(userID, "channel", appID)
	if err != nil {
		return nil, err
	}
	refunds, err := s.refundGroups(userID, "channel", appID)
	if err != nil {
		return nil, err
	}
	for key, g := range refunds {
		target, ok := groups[key]
		if !ok {
			target = StatGroup{Key: key}
			groups[key] = target
		}
		target.RefundCount += g.RefundCount
		target.RefundAmount += g.RefundAmount
		groups[key] = target
	}
	out := make([]StatGroup, 0, len(groups))
	for _, g := range groups {
		g.NetAmount = g.TotalAmount - g.RefundAmount
		g.Label = channelLabel(g.Key)
		out = append(out, g)
	}
	sortStatGroups(out)
	return out, nil
}

// statGroupsByDay 按天分组。
//
// 「天」按**本地时区**的自然日算 —— 商户看的是自己日历上的那天,
// 不是 UTC 的那天(东八区凌晨 0 点到 8 点的单会被算到前一天)。
//
// 实现上不能直接 GROUP BY substr(created_at, 1, 10):那是 UTC 日期。
// 所以这里按本地日期在 Go 里归组 —— 但要**限制扫描的行数**,
// 否则一个有几百万订单的商户点一次报表就要扫全表。
//
// 上限用 maxStatGroupRows 兜着,超了就在结果里标出来
// (和导出一样:宁可明说「被截断了」,也不要给一份看着完整的错数据)。
//
// 判定办法:多取一行(Limit(maxStatGroupRows+1)),
// 拿到 maxStatGroupRows+1 行就说明确实被截断了,只保留前 maxStatGroupRows 行。
func (s *Store) statGroupsByDay(userID, appID string) ([]StatGroup, bool, error) {
	type row struct {
		CreatedAt string
		Amount    int64
		Status    string
	}
	q := s.db.Model(&OrderModel{}).Where("user_id = ?", userID)
	if appID != "" {
		q = q.Where("app_id = ?", appID)
	}
	var rows []row
	if err := q.Select("created_at, amount, order_status AS status").
		Order("created_at DESC").Limit(maxStatGroupRows + 1).Find(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("platform: query orders for day stats: %w", err)
	}
	truncated := len(rows) > maxStatGroupRows
	if truncated {
		rows = rows[:maxStatGroupRows]
	}
	groups := map[string]*StatGroup{}
	for _, r := range rows {
		t, err := parseTime(r.CreatedAt)
		if err != nil {
			continue
		}
		key := t.Local().Format("2006-01-02")
		g, ok := groups[key]
		if !ok {
			g = &StatGroup{Key: key, Label: key}
			groups[key] = g
		}
		g.OrderCount++
		if r.Status == string(payment.TradeStatusSuccess) {
			g.SuccessCount++
			g.TotalAmount += r.Amount
		}
	}
	// 退款也按天归组(退款记录自带 app_id 与 channel,但没有订单的创建时间;
	// 这里按退款自己的创建时间归组,和订单分组是同一个口径的近似 ——
	// 跨天退款时两边会差一天,这是有意为之:退款发生在哪天就算哪天)。
	refundRows, refundTruncated, err := s.refundRowsForStats(userID, appID)
	if err != nil {
		return nil, false, err
	}
	if refundTruncated {
		truncated = true
	}
	for _, r := range refundRows {
		t, err := parseTime(r.CreatedAt)
		if err != nil {
			continue
		}
		key := t.Local().Format("2006-01-02")
		g, ok := groups[key]
		if !ok {
			g = &StatGroup{Key: key, Label: key}
			groups[key] = g
		}
		g.RefundCount++
		g.RefundAmount += r.Amount
	}
	out := make([]StatGroup, 0, len(groups))
	for _, g := range groups {
		g.NetAmount = g.TotalAmount - g.RefundAmount
		out = append(out, *g)
	}
	// 按日期倒序:最近的在前,报表页默认看最近几天。
	//
	// 必须**真的排序**:上面那个 map 是 Go 的 map,遍历顺序是随机的,
	// 直接把遍历结果倒过来只是把随机顺序反了个个儿 —— 每次刷新报表,
	// 行的先后都是乱的(前端是照后端给的顺序渲染的,不会自己再排一次)。
	// key 是 "2006-01-02" 格式,字典序就是时间序,直接比字符串即可。
	sort.Slice(out, func(i, j int) bool { return out[i].Key > out[j].Key })
	return out, truncated, nil
}

// maxStatGroupRows 单次报表扫描的最大订单行数。
const maxStatGroupRows = 20000

// orderGroups 按指定列分组统计订单。
func (s *Store) orderGroups(userID, column, appID string) (map[string]StatGroup, error) {
	type agg struct {
		// 分组键。别名用 group_key 而不是 key ——
		// KEY 是 MySQL 的保留字(`SELECT app_id AS key` 直接语法报错),
		// 平台是支持 MySQL 的,别名不能想当然。
		Key          string `gorm:"column:group_key"`
		OrderCount   int64
		SuccessCount int64
		TotalAmount  int64
	}
	q := s.db.Model(&OrderModel{}).Where("user_id = ?", userID)
	if appID != "" {
		q = q.Where("app_id = ?", appID)
	}
	// 分组列一律用 COALESCE(列, '') 包一层。
	//
	// 原因:老库加 app_id 列时,存量行的值是 **NULL**(不是空串)——
	// AutoMigrate 加列不会给老行填默认值。GROUP BY 的话 NULL 与 '' 是**两组**,
	// 而 GORM 把两组的键都扫成 "" ,在 Go 的 map 里后一组会**覆盖**前一组:
	// 报表上「未分类」那一档的金额会凭空少一块,还查不出来。
	// 包一层 COALESCE 之后,NULL 与 '' 合成同一组,数字才对得上。
	groupExpr := "COALESCE(" + column + ", '')"
	var rows []agg
	if err := q.Select(groupExpr+" AS group_key, COUNT(*) AS order_count, "+
		"SUM(CASE WHEN order_status = ? THEN 1 ELSE 0 END) AS success_count, "+
		"COALESCE(SUM(CASE WHEN order_status = ? THEN amount ELSE 0 END), 0) AS total_amount",
		string(payment.TradeStatusSuccess), string(payment.TradeStatusSuccess)).
		Group(groupExpr).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: group orders by %s: %w", column, err)
	}
	out := make(map[string]StatGroup, len(rows))
	for _, r := range rows {
		out[r.Key] = StatGroup{
			Key:          r.Key,
			OrderCount:   r.OrderCount,
			SuccessCount: r.SuccessCount,
			TotalAmount:  r.TotalAmount,
		}
	}
	return out, nil
}

// refundGroups 按指定列分组统计退款。
func (s *Store) refundGroups(userID, column, appID string) (map[string]StatGroup, error) {
	type agg struct {
		Key          string `gorm:"column:group_key"`
		RefundCount  int64
		RefundAmount int64
	}
	q := s.db.Model(&RefundModel{}).Where("user_id = ? AND "+refundCountedWhere,
		userID, refundCountedStatuses)
	if appID != "" {
		q = q.Where("app_id = ?", appID)
	}
	// 同 orderGroups:NULL 与 '' 必须合成一组(见那里的说明)。
	groupExpr := "COALESCE(" + column + ", '')"
	var rows []agg
	if err := q.Select(groupExpr + " AS group_key, COUNT(*) AS refund_count, " +
		"COALESCE(SUM(amount), 0) AS refund_amount").
		Group(groupExpr).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: group refunds by %s: %w", column, err)
	}
	out := make(map[string]StatGroup, len(rows))
	for _, r := range rows {
		out[r.Key] = StatGroup{
			Key:          r.Key,
			RefundCount:  r.RefundCount,
			RefundAmount: r.RefundAmount,
		}
	}
	return out, nil
}

// refundStatRow 退款统计用的一行。
type refundStatRow struct {
	CreatedAt string
	Amount    int64
}

// refundRowsForStats 取退款行(按天归组用)。
//
// 第二个返回值 truncated:行数超上限时为 true(口径同 statGroupsByDay)。
func (s *Store) refundRowsForStats(userID, appID string) ([]refundStatRow, bool, error) {
	q := s.db.Model(&RefundModel{}).
		Where("user_id = ? AND "+refundCountedWhere, userID, refundCountedStatuses)
	if appID != "" {
		q = q.Where("app_id = ?", appID)
	}
	var rows []refundStatRow
	if err := q.Select("created_at, amount").
		Order("created_at DESC").Limit(maxStatGroupRows + 1).Find(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("platform: query refunds for day stats: %w", err)
	}
	if len(rows) > maxStatGroupRows {
		return rows[:maxStatGroupRows], true, nil
	}
	return rows, false, nil
}

// sortStatGroups 给分组结果排序:金额高的在前,金额相同按订单数。
func sortStatGroups(groups []StatGroup) {
	// 简单插入排序:分组数量很小(应用 / 渠道都是个位数)。
	for i := 1; i < len(groups); i++ {
		for j := i; j > 0; j-- {
			if groups[j].TotalAmount > groups[j-1].TotalAmount ||
				(groups[j].TotalAmount == groups[j-1].TotalAmount &&
					groups[j].OrderCount > groups[j-1].OrderCount) {
				groups[j], groups[j-1] = groups[j-1], groups[j]
				continue
			}
			break
		}
	}
}

// channelLabel 渠道的展示名。
func channelLabel(channel string) string {
	switch channel {
	case string(payment.ChannelWechat):
		return "微信支付"
	case string(payment.ChannelAlipay):
		return "支付宝"
	case string(payment.ChannelApplePay):
		return "Apple Pay"
	case string(payment.ChannelGooglePay):
		return "Google Pay"
	case "":
		return "未分类"
	default:
		return channel
	}
}
