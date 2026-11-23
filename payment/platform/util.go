package platform

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// newID 生成带前缀的随机标识。
func newID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s%d", prefix, 0)
	}
	return prefix + hex.EncodeToString(buf)
}

// newShortID 生成指定字节数的随机标识(十六进制)。
//
// 用途:渠道的单号有长度上限(微信 out_trade_no 最长 32 位),
// 又必须够随机(单号撞号 = 两笔不同的交易被当成同一笔)。
func newShortID(n int) string {
	if n <= 0 {
		n = 8
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// writeJSON 输出 JSON 响应。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return
	}
}

// writeError 输出 JSON 错误响应。
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

// writeCredentialError 输出「凭据不对」的 JSON 错误响应(口令错、验证码错)。
//
// **用 403,不用 401**:这套 API 里 401 的含义是「会话没了」
// (令牌缺失/过期/无效,见 currentUser),前端收到 401 会清掉本地令牌
// 并跳回登录页。
//
// 而「敏感操作弹窗里口令打错了一次」绝不能当成会话失效 ——
// 那会把用户直接踢出控制台,正在填的表单和已输入的密钥全丢,
// 用户还以为是系统故障,重登一遍才发现是自己手滑。
//
// 403 在本项目里的既有含义就是「你是登录状态,但你给的这张凭据不对」
// (requireVerified 的票据问题一直用 403),所以这里是自洽的。
func writeCredentialError(w http.ResponseWriter, message string) {
	writeError(w, http.StatusForbidden, message)
}

// writeErrorFields 输出带附加字段的 JSON 错误响应。
//
// 用途:让前端能据此调整交互,而不是靠猜。例如登录时返回 mfa_required=true,
// 前端才把「验证码」输入框显示出来 —— 没开二次验证的账户根本不该看到这个框。
func writeErrorFields(w http.ResponseWriter, status int, message string, extra map[string]any) {
	body := map[string]any{"error": message}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, body)
}

// ---------------------------------------------------------------- 时间列格式

// timeLayout 时间列在数据库里的统一格式。
//
// 为什么不用 time.RFC3339Nano:
//
//   - **时区**:RFC3339Nano 会带上本地偏移("+08:00"),而开放接口的调用方
//     习惯传 UTC("Z")。字符串比较("created_at >= ?")是**逐字符**比,
//     两种时区混在一起比的是「墙上时间」而不是「时刻」→ 边界能差 8 小时。
//   - **宽度**:RFC3339Nano 会把末尾的 0 去掉("...:00Z" vs "...:00.5Z"),
//     宽度不一致 → 字典序不等于时间序。
//
// 所以统一成:**固定宽度 + 永远 UTC**。
// 固定宽度 → 字典序 = 时间序,可以直接用 SQL 的 >= / <= 比较和 ORDER BY;
// 永远 UTC → 和调用方传什么时区无关。
//
// 老库里存的是本地时区的 RFC3339Nano,由 migrateTimeColumns() 一次性改写。
const timeLayout = "2006-01-02T15:04:05.000000000Z"

// formatTime 把时间转成数据库里存的样子(UTC、固定宽度)。
func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// parseTime 解析数据库里的时间串。
//
// 兼容三种:新的固定宽度 UTC、老版的 RFC3339Nano(可能带时区偏移)、
// 以及只有日期的老数据(2026-11-20)。
func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("时间串为空")
	}
	for _, layout := range []string{timeLayout, time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间: %s", s)
}

// parseNanoTime 解析时间串,失败返回零值。
func parseNanoTime(s string) time.Time {
	t, err := parseTime(s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// normalizeTimeBound 把客户端传来的时间边界归一成数据库里的格式。
//
// 客户端可能给三种东西:
//
//	"2026-11-20"                 —— 只有日期
//	"2026-11-20T10:00:00Z"       —— UTC
//	"2026-11-20T18:00:00+08:00" —— 带时区偏移
//
// 全部转成 UTC 之后才能和库里比:库里存的是 UTC,直接拿字符串比
// 会因为时区不同而错 8 小时(而且是「静默少算/多算」,最难发现的那种错)。
//
// endOfDay=true 时(用于上界),只有日期就取那天的最后一刻 ——
// 「to=2026-11-20」按字面理解应该是「含 11-20 当天」。
func normalizeTimeBound(raw string, endOfDay bool) string {
	return normalizeTimeBoundIn(raw, endOfDay, time.Local)
}

// normalizeTimeBoundIn 按指定时区归整时间边界。
//
// 带时区的版本只给测试用(生产代码走 normalizeTimeBound,用的是本地时区)。
func normalizeTimeBoundIn(raw string, endOfDay bool, loc *time.Location) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	t, err := parseTimeBoundIn(raw, loc)
	if err != nil {
		// 解析不了就原样返回(查询会按字符串比较,大概率查不到 —— 但总比把请求打挂好)。
		return raw
	}
	if endOfDay && len(raw) == len("2006-01-02") {
		// 取那天的最后一刻:「to=2026-11-20」按字面理解应该含 11-20 当天。
		//
		// 用 Date 而不是 Add(24h):有夏令时的时区里,本地的一天不一定是 24 小时,
		// Add(24h) 会跨到第二天的 1 点(或者反过来少一小时)。
		t = time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, int(time.Second-time.Nanosecond), t.Location())
	}
	return formatTime(t)
}

// parseTimeBound 解析客户端传来的时间边界。
//
// 与 parseTime 的唯一区别:**只有日期的串按本地时区解释**
// ("2026-11-20" = 本地的 2026-11-20 零点),而不是 UTC。
//
// 为什么必须这样:
//
//   - 平台统计「今日订单」用的是**本地零点**(见 store.go 里 todayStart 的算法,
//     注释里明确写了「不能用 Truncate(24*time.Hour),那是按 UTC 对齐的,
//     在东八区要等到早上 8 点才换日」)。
//   - 报表按天分组也用 `t.Local()`。
//
// 如果日期筛选这边按 UTC 解释,两边口径就不一致了:
// 东八区会整整差 8 小时 —— 概览页的「今日订单」含 0:00~8:00,
// 订单页按今天筛选却从 8:00 才开始,商户看着两个对不上的数字。
//
// 带时区偏移的完整时间戳仍按它自己的偏移解析,不受影响。
func parseTimeBound(raw string) (time.Time, error) {
	return parseTimeBoundIn(raw, time.Local)
}

// parseTimeBoundIn 按指定时区解析时间边界。
//
// 单独拆一个带时区参数的版本,是为了让测试不用去改全局的 time.Local ——
// 那是进程级的共享状态,任何人加一条 t.Parallel() 就会互相踩。
func parseTimeBoundIn(raw string, loc *time.Location) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if len(s) == len("2006-01-02") {
		return time.ParseInLocation("2006-01-02", s, loc)
	}
	return parseTime(s)
}

// boolToInt32 把 bool 转成数据库里的 0/1。
func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// maxRequestBody 请求体大小上限(1MB)。
//
// 为什么要限:控制台的接口都是 POST JSON,不限制的话一个几百 MB 的请求体
// 能直接把内存吃满(一个连接就够)。
const maxRequestBody = 1 << 20

// decodeOptionalJSON 解析请求体,但**允许请求体为空**。
//
// 用在哪:有些接口的请求体是可有可无的(比如「生成密钥」——
// 首绑时不需要带任何东西,换绑时才带「当前绑定确认」)。
// 用 decodeJSON 的话空 body 会直接报「unexpected end of JSON input」,
// 而前端「没有参数就不发 body」是很自然的写法,不该为此报错。
func decodeOptionalJSON(r *http.Request, v any) error {
	if r == nil || r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > maxRequestBody {
		return fmt.Errorf("请求体超过上限 %d 字节", maxRequestBody)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}

// decodeJSON 解析请求体(带大小上限)。
func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return fmt.Errorf("请求体为空")
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > maxRequestBody {
		return fmt.Errorf("请求体超过上限 %d 字节", maxRequestBody)
	}
	return json.Unmarshal(raw, v)
}
