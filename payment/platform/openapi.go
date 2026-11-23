package platform

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 开放接口(机器对机器)与平台管理接口。
//
// 三类调用方,三种鉴权:
//
//	控制台    浏览器会话令牌 + 敏感操作票据(口令 / 验证码)
//	开放接口  API 密钥 + HMAC-SHA256 签名(给商户自己的业务后端用)
//	平台管理  管理员令牌(平台运营方看全平台数据、禁用违规商户)
//
// 开放接口**不是**控制台的复制品:它按 scope 授权,
// 只有显式授予 refund 权限的密钥才能退款。

// 开放接口的限流阈值(窗口 1 分钟)。
const (
	// apiRateLimitPerKey 单把密钥每分钟最多请求次数。
	apiRateLimitPerKey = 600
	// apiRateLimitPerIP 同一 IP 每分钟最多请求次数。
	apiRateLimitPerIP = 1200
)

// 开放接口的路径前缀。
const openAPIPrefix = "/api/open/"

// 开放接口路径。
const (
	openPathPay        = "/api/open/pay"
	openPathQuery      = "/api/open/query"
	openPathRefund     = "/api/open/refund"
	openPathClose      = "/api/open/close"
	openPathWallet     = "/api/open/wallet/charge"
	openPathOrders     = "/api/open/orders"
	openPathStats      = "/api/open/stats"
	openPathQueryOrder = "/api/open/order"
)

// ---------------------------------------------------------------- 开放接口

// openPrincipal 认证开放接口请求,返回认证主体。
//
// 签名覆盖请求体,所以必须**先读原始字节**再验签,
// 之后业务代码从这份字节里解析 JSON(不能二次读流)。
func (p *Platform) openPrincipal(r *http.Request, scope string) (*Principal, []byte, error) {
	// 限流:按密钥与 IP 各算一次。
	// 开放接口是机器流量,天然比控制台频繁得多,阈值给得宽;
	// 但还是要限——否则一把泄露的密钥就能把渠道接口打爆(钱是商户的,额度是平台的)。
	ip := p.clientIPOf(r)
	if !p.apiLimiter.allow("ip:"+ip, apiRateLimitPerIP, time.Minute) {
		return nil, nil, fmt.Errorf("请求过于频繁，请稍后再试")
	}
	keyID := strings.TrimSpace(r.Header.Get(apiKeyHeaderKey))
	if keyID != "" && !p.apiLimiter.allow("key:"+keyID, apiRateLimitPerKey, time.Minute) {
		return nil, nil, fmt.Errorf("请求过于频繁，请稍后再试")
	}

	body := readBody(r)
	if len(body) > apiKeyMaxBodySize {
		return nil, nil, fmt.Errorf("请求体过大")
	}
	principal, err := p.authenticateAPIKey(r, body)
	if err != nil {
		return nil, nil, err
	}
	if err := p.requireScope(principal, scope); err != nil {
		return nil, nil, err
	}
	return principal, body, nil
}

// decodeRaw 解析已经读出来的 JSON 字节。
func decodeRaw(raw []byte, v any) error {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return fmt.Errorf("请求体不能为空")
	}
	return json.Unmarshal(raw, v)
}

// handleOpenPay 开放接口:下单。
//
// POST /api/open/pay
//
// 需要 pay 权限。
func (p *Platform) handleOpenPay(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req payRequest
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	// 密钥绑定了应用 → app_id 由密钥决定,不用客户端传。
	//
	// 这样客户端「忘了传 app_id」不再是静默降级(悄悄走全局渠道、订单进未分类),
	// 而是不可能发生;真传错了(传了别的应用)当场 400,不会把钱记到别的产品上。
	if principal.Key != nil && principal.Key.AppID != "" {
		req.AppID = strings.TrimSpace(req.AppID)
		if req.AppID == "" {
			req.AppID = principal.Key.AppID
		} else if req.AppID != principal.Key.AppID {
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("该密钥绑定应用 %s,不能用于应用 %s", principal.Key.AppID, req.AppID))
			return
		}
	}
	result, err := p.corePay(r, principal.User, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOpenQuery 开放接口:查单。
//
// POST /api/open/query
//
// 需要 pay 权限。
func (p *Platform) handleOpenQuery(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Channel    string `json:"channel"`
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreQuery(r, principal.User, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOpenOrder 开放接口:查订单详情(不向渠道发请求,查平台自己的记录)。
//
// POST /api/open/order
//
// 需要 pay 权限。
func (p *Platform) handleOpenOrder(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	record, err := p.ownOrder(principal.User, req.OutTradeNo)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, record)
}

// handleOpenRefund 开放接口:退款。
//
// POST /api/open/refund
//
// 需要 **refund** 权限:退款是资金操作,不能靠一把只用来下单的密钥顺手做掉。
func (p *Platform) handleOpenRefund(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopeRefund)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Channel      string `json:"channel"`
		OutTradeNo   string `json:"out_trade_no"`
		OutRefundNo  string `json:"out_refund_no"`
		RefundAmount int64  `json:"refund_amount"`
		Reason       string `json:"reason"`
	}
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreRefund(r, principal.User, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOpenClose 开放接口:关闭订单。
//
// POST /api/open/close
//
// 需要 pay 权限。
func (p *Platform) handleOpenClose(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Channel    string `json:"channel"`
		OutTradeNo string `json:"out_trade_no"`
	}
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if err := p.coreClose(r, principal.User, req); err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleOpenWallet 开放接口:钱包渠道扣款。
//
// POST /api/open/wallet/charge
//
// 需要 wallet 权限。
func (p *Platform) handleOpenWallet(w http.ResponseWriter, r *http.Request) {
	principal, body, err := p.openPrincipal(r, apiKeyScopeWallet)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Channel      string `json:"channel"`
		OutTradeNo   string `json:"out_trade_no"`
		PaymentToken string `json:"payment_token"`
		Amount       int64  `json:"amount"`
	}
	if err := decodeRaw(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	result, err := p.coreWalletCharge(r, principal.User, req)
	if err != nil {
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOpenOrders 开放接口:订单列表。
//
// GET /api/open/orders?page=1&size=20&status=&channel=&keyword=&from=&to=
//
// 需要 pay 权限。查询串也参与签名(见 signTarget),所以翻页参数同样防篡改。
func (p *Platform) handleOpenOrders(w http.ResponseWriter, r *http.Request) {
	principal, _, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	filter := parseOrderFilter(r)
	orders, total, err := p.store.OrdersPage(principal.User.UserID, filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	page, size := normalizePage(filter.Page, filter.Size)
	writeJSON(w, http.StatusOK, map[string]any{
		"orders": orders,
		"page":   page,
		"size":   size,
		"total":  total,
		"pages":  totalPages(total, size),
	})
}

// handleOpenStats 开放接口:交易统计。
//
// GET /api/open/stats
// GET /api/open/stats?app_id=xxx
// GET /api/open/stats?group_by=app|channel|day
//
// 需要 pay 权限。
func (p *Platform) handleOpenStats(w http.ResponseWriter, r *http.Request) {
	principal, _, err := p.openPrincipal(r, apiKeyScopePay)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	appID := strings.TrimSpace(q.Get("app_id"))
	if appID != "" {
		app, err := p.store.AppByID(principal.User.UserID, appID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if app == nil {
			writeError(w, http.StatusBadRequest, "应用不存在: "+appID)
			return
		}
	}
	stats, err := p.store.StatsForApp(principal.User.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	groupBy := strings.TrimSpace(q.Get("group_by"))
	if groupBy == "" {
		writeJSON(w, http.StatusOK, stats)
		return
	}
	groups, truncated, err := p.store.StatGroups(principal.User.UserID, groupBy, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":     stats,
		"group_by":  normalizeGroupBy(groupBy),
		"groups":    groups,
		"truncated": truncated,
	})
}

// ---------------------------------------------------------------- 密钥管理

// handleAPIKeys 列出本商户的开放接口密钥(不含密钥明文)。
//
// GET /api/keys
//
// 密钥明文只在创建时返回一次,列表里永远拿不到。
func (p *Platform) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	keys, err := p.store.APIKeys(user.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

// handleCreateAPIKey 创建一把开放接口密钥。
//
// POST /api/keys {"name":"订单服务","scopes":["pay","refund"],"ttl_days":0}
//
// 需要二次验证票据:密钥能直接动钱,不能只凭会话令牌就发出去。
//
// 响应里的 secret **只出现这一次**,之后库里只有密文。
func (p *Platform) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		Name string `json:"name"`
		// Scopes 权限列表:pay / refund / wallet。
		Scopes []string `json:"scopes"`
		// TTLDays 有效期(天),0 表示长期有效。
		TTLDays int `json:"ttl_days"`
		// AppID 绑定到哪个应用(空 = 不绑定)。
		//
		// 绑定后,用这把密钥下单会自动落到该应用,客户端不必再传 app_id。
		AppID string `json:"app_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "未命名密钥"
	}
	if len(name) > 60 {
		writeError(w, http.StatusBadRequest, "备注名过长")
		return
	}
	appID := strings.TrimSpace(req.AppID)
	if appID == "" && user.RequireAppBinding {
		// 该商户开了「订单必须绑定应用」:一把不绑应用的密钥下单一定被拒,
		// 与其建出来就 unusable,不如当场说清楚。
		writeError(w, http.StatusBadRequest, requireAppBindingKeyMessage())
		return
	}
	var ttl time.Duration
	if req.TTLDays > 0 {
		ttl = time.Duration(req.TTLDays) * 24 * time.Hour
	}
	record, secret, err := p.store.CreateAPIKeyForApp(user.UserID, name, req.Scopes,
		appID, ttl)
	if err != nil {
		// 应用不存在属于调用方的问题,不是服务端故障。
		if strings.Contains(err.Error(), "应用不存在") {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAPIKeyCreate, record.KeyID,
		fmt.Sprintf("权限=%s 有效期=%s 应用=%s", strings.Join(record.Scopes, ","),
			describeTTL(ttl), auditAppLabel(record.AppID)), "")
	writeJSON(w, http.StatusOK, map[string]any{
		"key":    record,
		"secret": secret,
		"notice": "密钥明文只显示这一次,请立即保存;之后只能重新创建或吊销。",
	})
}

// handleRevokeAPIKey 吊销一把密钥。
//
// POST /api/keys/{key_id}/revoke
//
// 需要二次验证票据。
func (p *Platform) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	keyID := strings.TrimSpace(r.PathValue("key_id"))
	if keyID == "" {
		keyID = pathSegment(r.URL.Path, "/api/keys/")
	}
	if keyID == "" {
		writeError(w, http.StatusBadRequest, "缺少密钥 ID")
		return
	}
	// 只能吊销自己的密钥:RevokeAPIKey 里带了 user_id 条件。
	if err := p.store.RevokeAPIKey(user.UserID, keyID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAPIKeyRevoke, keyID, "吊销密钥", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// describeTTL 把有效期描述成人话。
func describeTTL(ttl time.Duration) string {
	if ttl <= 0 {
		return "长期有效"
	}
	return strconv.Itoa(int(ttl.Hours()/24)) + " 天"
}

// pathSegment 取路径里某个固定片段之后的值。
//
// ServeMux 的 PathValue 只在经过路由时才有值;直接调用 handler(测试、
// 或把平台挂到别的路由里)时它可能是空的,所以这里再从路径本身解析一次。
//
// marker 是固定的路径片段,例如 "/api/keys/"。
func pathSegment(path, marker string) string {
	idx := strings.Index(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	if i := strings.Index(rest, "/"); i >= 0 {
		rest = rest[:i]
	}
	return strings.TrimSpace(rest)
}

// adminUserID 取被操作商户的 ID。
func adminUserID(r *http.Request) string {
	if v := strings.TrimSpace(r.PathValue("user_id")); v != "" {
		return v
	}
	return pathSegment(r.URL.Path, "/api/admin/merchants/")
}

// ---------------------------------------------------------------- 平台管理

// requireAdmin 校验平台管理员令牌。
//
// 没配置管理员令牌时,平台管理接口**整体不开放**。
func (p *Platform) requireAdmin(r *http.Request) error {
	if p.opts.AdminToken == "" {
		return fmt.Errorf("平台管理接口未启用")
	}
	got := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
	if got == "" {
		return fmt.Errorf("缺少管理员令牌")
	}
	// 常量时间比较:避免用比较耗时反推令牌。
	if subtle.ConstantTimeCompare([]byte(got), []byte(p.opts.AdminToken)) != 1 {
		return fmt.Errorf("管理员令牌不正确")
	}
	return nil
}

// handleAdminMerchants 商户列表(平台管理员用)。
//
// GET /api/admin/merchants
//
// 只返回非敏感信息:口令散列、令牌、二次验证目标都不下发。
func (p *Platform) handleAdminMerchants(w http.ResponseWriter, r *http.Request) {
	if err := p.requireAdmin(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	users, err := p.store.AllUsers()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(users))
	for i := range users {
		u := users[i]
		out = append(out, map[string]any{
			"user_id":     u.UserID,
			"user_name":   u.UserName,
			"mfa_enabled": u.MFAEnabled,
			"disabled":    u.Disabled,
			"created_at":  u.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"merchants": out})
}

// handleAdminResetMFA 重置某个商户的二次验证(平台管理员用)。
//
// POST /api/admin/merchants/{user_id}/mfa/reset
//
// **为什么要留这个后门**:换绑二次验证必须由「当前绑定的人」确认
// (旧验证器 App 的动态码 / 发到已绑定地址的验证码),这挡住了
// 「拿到口令就能把正主的验证器换掉」这类接管。
//
// 但正主**真的**把手机丢了/换号了的时候,他既给不出旧动态码,
// 又收不到旧地址的验证码 —— 这时只能由平台方核实身份后重置。
// 没有这条恢复路径,加了确认之后就是「丢手机 = 账户永久失联」。
//
// 重置后该商户没有二次验证,登录只要口令 —— 平台方要确认对方确实是本人
// (线下核验、核对订单信息等),再调用这个接口。
func (p *Platform) handleAdminResetMFA(w http.ResponseWriter, r *http.Request) {
	if err := p.requireAdmin(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	userID := adminUserID(r)
	if userID == "" {
		writeError(w, http.StatusBadRequest, "缺少商户 ID")
		return
	}
	user, err := p.store.UserByID(userID)
	if err != nil || user == nil {
		writeError(w, http.StatusNotFound, "商户不存在")
		return
	}
	if err := p.store.SetMFA(userID, "", "", false); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 一并清掉 TOTP 密钥:留着它就等于给下一个绑定者留了一把万能钥匙。
	if err := p.store.SetMFATOTPSecret(userID, ""); err != nil {
		p.opts.Logger.Printf("清理 TOTP 密钥失败: %v", err)
	}
	if err := p.store.SetMFATOTPPendingSecret(userID, ""); err != nil {
		p.opts.Logger.Printf("清理待确认 TOTP 密钥失败: %v", err)
	}
	p.audit(r, "", auditAdminMerchant, userID, "重置商户二次验证", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user_id": userID, "mfa_enabled": false})
}

// handleAdminCreateMerchant 平台管理员创建商户(平台管理员用)。
//
// POST /api/admin/merchants {"name":"商户A","password":"至少8位"}
//
// 什么时候需要它:平台关掉了自助注册(Options.DisableSelfRegistration)之后,
// 商户没法靠「首次登录自动建号」开户,只能由平台方在这里建。
// 建号时直接把口令设成商户拿到的那一串,商户登录后可以自己改。
func (p *Platform) handleAdminCreateMerchant(w http.ResponseWriter, r *http.Request) {
	if err := p.requireAdmin(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "请填写商户名称")
		return
	}
	if len(req.Password) < 8 {
		writeError(w, http.StatusBadRequest, "登录口令不少于 8 位")
		return
	}
	if existing, err := p.store.UserByName(name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	} else if existing != nil {
		writeError(w, http.StatusConflict, "商户名称已存在: "+name)
		return
	}
	hash, err := hashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	user, err := p.store.CreateUser(name, hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, "", auditAdminMerchant, user.UserID, "创建商户 "+name, "")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "user_id": user.UserID, "name": user.UserName,
	})
}

// handleAdminStats 平台整体统计(平台管理员用)。
//
// GET /api/admin/stats
func (p *Platform) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	if err := p.requireAdmin(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	stats, err := p.store.PlatformStats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleAdminSetMerchant 启用 / 禁用某个商户。
//
// POST /api/admin/merchants/{user_id}/status {"disabled":true}
//
// 禁用后:该商户不能登录、已有会话令牌立即失效、开放接口密钥也用不了。
func (p *Platform) handleAdminSetMerchant(w http.ResponseWriter, r *http.Request) {
	if err := p.requireAdmin(r); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	userID := adminUserID(r)
	if userID == "" {
		writeError(w, http.StatusBadRequest, "缺少商户 ID")
		return
	}
	var req struct {
		Disabled bool `json:"disabled"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if err := p.store.SetUserDisabled(userID, req.Disabled); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	action := "启用"
	if req.Disabled {
		action = "禁用"
	}
	p.audit(r, "", auditAdminMerchant, userID, action+"商户", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user_id": userID, "disabled": req.Disabled})
}
