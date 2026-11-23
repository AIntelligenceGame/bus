package platform

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// 控制台/运维相关的接口:订单导出、探针,以及一些共用的参数解析helpers。
//
// 这些接口是「平台该有的」基本盘:
//   - 订单要能查、能筛、能导出(财务对账要拿明细);
//   - 要有健康检查(部署时的探针);
//   - 要有版本信息(排查问题时先确认跑的是哪个版本)。

// handleOrdersExport 把订单导出成 CSV。
//
// GET /api/orders/export?status=&channel=&keyword=&from=&to=
//
// 带 UTF-8 BOM:Excel 打开中文 CSV 不乱码。
//
// **流式输出**:边查边写,不把整份结果攒在内存里。
// 一次最多导 maxExportRows 条;超出的部分会写一行提示(而不是静默截断 ——
// 商户拿一份「少了一半」的账单去对账,比报错难查得多)。
func (p *Platform) handleOrdersExport(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	// 导出很贵(一次扫几千行、拼几 MB 的 CSV),限流防止把库和带宽打满。
	if !p.exportLimiter.allow("export:"+user.UserID, exportRateLimitPerUser, time.Minute) {
		writeRateLimited(w, "导出太频繁,请稍后再试")
		return
	}
	filter := parseOrderFilter(r)
	filename := fmt.Sprintf("orders-%s.csv", time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	// 导出的是资金明细,不允许被中间缓存长期持有。
	w.Header().Set("Cache-Control", "no-store")

	// 应用名映射:导出里要写「应用」这一列(光有 app_id,财务看不懂)。
	appNames := map[string]string{}
	apps, aerr := p.store.Apps(user.UserID)
	if aerr != nil {
		writeError(w, http.StatusInternalServerError, aerr.Error())
		return
	}
	for _, app := range apps {
		appNames[app.AppID] = app.Name
	}

	_, _ = w.Write([]byte{0xEF, 0xBB, 0xBF})
	cw := csv.NewWriter(w)
	defer cw.Flush()
	_ = cw.Write([]string{
		"订单号", "应用", "渠道", "交易类型", "标题", "金额(元)", "状态",
		"渠道流水号", "创建时间", "更新时间",
	})

	count := 0
	streamErr := p.store.StreamOrdersForExport(user.UserID, filter, func(o OrderRecord) error {
		count++
		return cw.Write([]string{
			csvSafe(o.OutTradeNo),
			csvSafe(appLabel(appNames, o.AppID)),
			csvSafe(o.Channel),
			csvSafe(o.TradeType),
			csvSafe(o.Subject),
			o.AmountYuan,
			csvSafe(o.Status),
			csvSafe(o.TransactionID),
			o.CreatedAt.Format(time.RFC3339),
			o.UpdatedAt.Format(time.RFC3339),
		})
	})
	if streamErr != nil {
		// 响应已经开始,没法再改状态码了:记一条审计,让服务端日志能追到。
		p.opts.Logger.Printf("导出订单失败(用户 %s): %v", user.UserID, streamErr)
		p.audit(r, user.UserID, auditOrderExport, "orders",
			fmt.Sprintf("导出中断,已写出 %d 条", count), "")
		return
	}
	// 到达上限时明确告诉用户「被截断了」,而不是让商户以为这就是全部。
	if count >= maxExportRows {
		_ = cw.Write([]string{
			fmt.Sprintf("(提示:本次导出已达上限 %d 条,更早的订单请用时间条件分段导出)", maxExportRows),
		})
	}
	p.audit(r, user.UserID, auditOrderExport, "orders", fmt.Sprintf("导出 %d 条", count), "")
}

// csvSafe 转义 CSV 单元格里的公式注入。
//
// 商户可以把订单标题写成「=cmd|'/c calc'!A1」,导出后财务用 Excel 打开,
// Excel 会把它当成公式执行(CSV 注入)。以 = + - @ 开头的单元格一律加单引号前缀。
func csvSafe(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + s
	}
	return s
}

// handleHealthz 健康检查(公开)。
//
// GET /healthz
func (p *Platform) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"status": "UP",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// handleVersion 版本信息(公开)。
//
// GET /api/version
//
// registration_code_required 是平台级配置(不是账户级),公开它没有风险,
// 但前端靠它决定要不要显示「注册码」输入框。
func (p *Platform) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":                       PlatformName,
		"version":                    PlatformVersion,
		"registration_code_required": p.opts.RegistrationCode != "",
	})
}

// PlatformName 平台名称。
const PlatformName = "bus-payment-platform"

// PlatformVersion 平台版本号。
const PlatformVersion = "1.1.0"

// parseOrderFilter 从查询参数解析订单筛选条件。
func parseOrderFilter(r *http.Request) OrderFilter {
	q := r.URL.Query()
	return OrderFilter{
		Page:    intParam(r, "page", 1),
		Size:    intParam(r, "size", 20),
		Status:  strings.TrimSpace(q.Get("status")),
		Channel: strings.TrimSpace(q.Get("channel")),
		AppID:   strings.TrimSpace(q.Get("app_id")),
		Keyword: strings.TrimSpace(q.Get("keyword")),
		From:    strings.TrimSpace(q.Get("from")),
		To:      strings.TrimSpace(q.Get("to")),
	}
}

// intParam 读取整数查询参数,非法或缺省时返回默认值。
func intParam(r *http.Request, name string, def int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// totalPages 计算总页数。
func totalPages(total int64, size int) int {
	if size <= 0 {
		return 0
	}
	return int((total + int64(size) - 1) / int64(size))
}

// normalizeBaseURL 规整商户填写的对外访问地址。
//
// 规则:
//   - 空字符串 = 清除,回到平台默认地址;
//   - 只接受 http / https;
//   - 必须带主机名;
//   - 去掉结尾的斜杠(拼回调路径时不会变成双斜杠);
//   - 不接受带账号密码 / 查询串 / 锚点的地址(那些东西在回调地址里没有意义,
//     带着走反而容易让人填错)。
//
// 允许带路径前缀(例如 https://example.com/pay),部署在子路径后面时用得上。
func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("地址格式不对: %v", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("只支持 http / https 地址,当前是 %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("地址里要带域名,例如 https://pay.example.com")
	}
	if u.User != nil {
		return "", fmt.Errorf("地址里不要带账号密码")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("地址里不要带 ?query 或 #锚点")
	}
	u.Scheme = scheme
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String(), nil
}

// sameOrigin 判断两个 URL 是否同源(协议 + 主机 + 端口)。
//
// 用于校验「商户自定义回调地址」:只允许与平台自己的地址同源。
func sameOrigin(raw, base string) bool {
	if base == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	b, err := url.Parse(base)
	if err != nil {
		return false
	}
	if u.Scheme == "" || b.Scheme == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, b.Scheme) &&
		strings.EqualFold(u.Host, b.Host)
}
