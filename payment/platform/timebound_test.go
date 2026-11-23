package platform

import (
	"testing"
	"time"
)

// TestDateOnlyBoundUsesLocalDay 「只有日期」的时间边界按本地时区解释。
//
// 平台统计「今日订单」用的是本地零点(见 store.go 的 todayStart),
// 报表按天分组也用 Local()。如果日期筛选这边按 UTC 解释,两边口径就不一致:
// 东八区整整差 8 小时 —— 概览页含 0:00~8:00 的订单,订单页按今天筛选却从
// 8:00 才开始,商户看着两个对不上的数字。
func TestDateOnlyBoundUsesLocalDay(t *testing.T) {
	// 造一个非 UTC 的本地时区,免得测试在 UTC 机器上跑不出差异。
	//
	// 用带时区参数的版本,不改全局的 time.Local ——
	// 那是进程级共享状态,同包内任何别的测试将来加个 t.Parallel() 就会互相踩。
	loc := time.FixedZone("CST", 8*3600)

	day := "2026-11-20"

	start := normalizeTimeBoundIn(day, false, loc)
	end := normalizeTimeBoundIn(day, true, loc)

	// 下界 = 本地当天零点,换算成 UTC 是前一天 16:00。
	wantStart := time.Date(2026, 11, 20, 0, 0, 0, 0, loc).UTC().Format(timeLayout)
	if start != wantStart {
		t.Fatalf("下界应是本地零点,实际 %s,期望 %s", start, wantStart)
	}

	// 上界 = 本地当天最后一刻,换算成 UTC 是当天 15:59:59.999...
	wantEnd := time.Date(2026, 11, 20, 23, 59, 59, int(time.Second-time.Nanosecond), loc).
		UTC().Format(timeLayout)
	if end != wantEnd {
		t.Fatalf("上界应是本地当天最后一刻,实际 %s,期望 %s", end, wantEnd)
	}

	// 上下界必须落在同一天里(这是筛选能用的前提)。
	startT, err := time.Parse(timeLayout, start)
	if err != nil {
		t.Fatalf("解析下界失败: %v", err)
	}
	endT, err := time.Parse(timeLayout, end)
	if err != nil {
		t.Fatalf("解析上界失败: %v", err)
	}
	if !endT.After(startT) {
		t.Fatalf("上界必须晚于下界: %s vs %s", start, end)
	}
	if endT.Sub(startT) < 23*time.Hour {
		t.Fatalf("一天的下界到上界应至少 23 小时,实际 %s", endT.Sub(startT))
	}
}

// TestDateOnlyBoundMatchesTodayStat 日期筛选的下界要和「今日订单」的口径一致。
//
// 这是上面那条的「业务后果」版本:两个数字必须指向同一个零点,
// 否则商户在概览页和订单页会看到互相矛盾的数据。
func TestDateOnlyBoundMatchesTodayStat(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)

	now := time.Now().In(loc)
	today := now.Format("2006-01-02")

	// 直接调生产代码里的那个函数(store.go 的 todayStart)。
	//
	// 之前这里是**在测试里复刻了一遍**算法,拿复刻品和另一边的结果比 ——
	// 真把 store.go 的口径改成 UTC,这个「口径一致」的测试照样绿。
	wantStart := todayStart(now)

	// 订单页「开始日期 = 今天」的下界。
	filterStart := normalizeTimeBoundIn(today, false, loc)

	if filterStart != wantStart {
		t.Fatalf("按今天筛选的下界(%s)必须和「今日订单」的下界(%s)一致", filterStart, wantStart)
	}
}

// TestTimeBoundWithOffsetUnaffected 带时区偏移的完整时间戳不受影响。
func TestTimeBoundWithOffsetUnaffected(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)

	// "2026-11-20T18:00:00+08:00" = "2026-11-20T10:00:00Z"
	got := normalizeTimeBoundIn("2026-11-20T18:00:00+08:00", false, loc)
	want := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC).Format(timeLayout)
	if got != want {
		t.Fatalf("带偏移的时间戳应按自己的偏移解析,实际 %s,期望 %s", got, want)
	}

	// UTC 的完整时间戳同样不变。
	got = normalizeTimeBoundIn("2026-11-20T10:00:00Z", false, loc)
	if got != want {
		t.Fatalf("UTC 时间戳解析不对,实际 %s,期望 %s", got, want)
	}
}

// TestTimeBoundEmptyAndGarbage 空值和解析不了的串不崩。
func TestTimeBoundEmptyAndGarbage(t *testing.T) {
	if got := normalizeTimeBound("", false); got != "" {
		t.Fatalf("空串应返回空,实际 %q", got)
	}
	if got := normalizeTimeBound("   ", true); got != "" {
		t.Fatalf("空白串应返回空,实际 %q", got)
	}
	// 解析不了就原样返回(查询按字符串比,查不到,但不打挂请求)。
	if got := normalizeTimeBound("不是时间", false); got != "不是时间" {
		t.Fatalf("解析不了的串应原样返回,实际 %q", got)
	}
}
