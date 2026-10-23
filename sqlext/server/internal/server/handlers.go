package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/sqlext/server/internal/auth"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/store"
	"github.com/AIntelligenceGame/bus/sqltree"
)

// ---------------------------------------------------------------- 认证

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type authResponse struct {
	Token     string      `json:"token"`
	ExpiresAt time.Time   `json:"expires_at"`
	User      *store.User `json:"user"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.SignupEnabled {
		writeErr(w, http.StatusForbidden, "signup_disabled", "当前未开放注册")
		return
	}
	var req registerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "请求体格式不对: "+err.Error())
		return
	}
	email := strings.TrimSpace(req.Email)
	if !strings.Contains(email, "@") || len(email) < 5 {
		writeErr(w, http.StatusBadRequest, "bad_email", "邮箱格式不对")
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_password", err.Error())
		return
	}
	u, err := s.store.CreateUser(r.Context(), email, hash, s.cfg.FreeQuota)
	if err == store.ErrEmailTaken {
		writeErr(w, http.StatusConflict, "email_taken", "该邮箱已注册")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "创建用户失败")
		return
	}
	s.issueToken(w, r, u)
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "请求体格式不对: "+err.Error())
		return
	}
	u, err := s.store.UserByEmail(r.Context(), req.Email)
	if err != nil {
		// 不区分"不存在"和"口令错"，避免枚举邮箱
		writeErr(w, http.StatusUnauthorized, "invalid_credentials", "邮箱或口令不对")
		return
	}
	if err := auth.VerifyPassword(u.PasswordHash, req.Password); err != nil {
		writeErr(w, http.StatusUnauthorized, "invalid_credentials", "邮箱或口令不对")
		return
	}
	s.issueToken(w, r, u)
}

// issueToken 生成登录态并返回。
func (s *Server) issueToken(w http.ResponseWriter, r *http.Request, u *store.User) {
	token, err := auth.NewToken()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "生成令牌失败")
		return
	}
	expires := time.Now().Add(s.cfg.TokenTTL)
	if err := s.store.CreateSession(r.Context(), token, u.ID, expires); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "创建会话失败")
		return
	}
	writeJSON(w, http.StatusOK, authResponse{Token: token, ExpiresAt: expires, User: u})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	_, token, err := s.currentUser(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", "未登录")
		return
	}
	_ = s.store.DeleteSession(r.Context(), token)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		writeJSON(w, http.StatusOK, map[string]any{"user": u, "balance": u.Balance})
	})(w, r)
}

// ---------------------------------------------------------------- 余额与充值

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		writeJSON(w, http.StatusOK, map[string]any{
			"balance":        u.Balance,
			"cost_per_parse": s.cfg.CostPerParse,
		})
	})(w, r)
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	names := []string{}
	for _, n := range []string{"manual", "wechat", "alipay", "stripe"} {
		if _, ok := s.channels.Get(n); ok {
			names = append(names, n)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"channels": names, "default": s.channels.Default().Name()})
}

type createOrderRequest struct {
	Amount  int64  `json:"amount"`
	Channel string `json:"channel"`
}

func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		var req createOrderRequest
		if err := decodeJSON(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", "请求体格式不对: "+err.Error())
			return
		}
		if req.Amount <= 0 {
			writeErr(w, http.StatusBadRequest, "bad_amount", "充值金额必须大于 0")
			return
		}
		chName := req.Channel
		if chName == "" {
			chName = s.channels.Default().Name()
		}
		ch, ok := s.channels.Get(chName)
		if !ok {
			writeErr(w, http.StatusBadRequest, "bad_channel", "不支持的支付渠道: "+chName)
			return
		}
		order := &store.Order{
			UserID:  u.ID,
			Amount:  req.Amount,
			Channel: ch.Name(),
			Status:  store.OrderPending,
		}
		if err := s.store.CreateOrder(r.Context(), order); err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "创建订单失败")
			return
		}
		checkout, err := ch.CreateCheckout(r.Context(), order, s.cfg.BaseURL)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "生成支付信息失败: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"order": order, "checkout": checkout})
	})(w, r)
}

func (s *Server) handleListOrders(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		limit := atoiDefault(r.URL.Query().Get("limit"), 20)
		orders, err := s.store.ListOrders(r.Context(), u.ID, limit)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "查询订单失败")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
	})(w, r)
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		id := r.PathValue("id")
		o, err := s.store.Order(r.Context(), id)
		if err != nil || o.UserID != u.ID {
			writeErr(w, http.StatusNotFound, "not_found", "订单不存在")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"order": o})
	})(w, r)
}

// ---------------------------------------------------------------- 站点授权

func (s *Server) handleListSites(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		sites, err := s.store.Sites(r.Context(), u.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "查询站点失败")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"sites": sites})
	})(w, r)
}

// addSiteRequest 是新增授权站点的请求体。
type addSiteRequest struct {
	Origin string `json:"origin"`
}

func (s *Server) handleAddSite(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		var req addSiteRequest
		if err := decodeJSON(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", "请求体格式不对: "+err.Error())
			return
		}
		origin := normalizeOrigin(req.Origin)
		if origin == "" {
			writeErr(w, http.StatusBadRequest, "bad_origin", "站点地址不合法")
			return
		}
		if err := s.store.AddSite(r.Context(), u.ID, origin); err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "保存站点失败")
			return
		}
		sites, _ := s.store.Sites(r.Context(), u.ID)
		writeJSON(w, http.StatusOK, map[string]any{"sites": sites})
	})(w, r)
}

func (s *Server) handleRemoveSite(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		origin := normalizeOrigin(r.URL.Query().Get("origin"))
		if origin == "" {
			writeErr(w, http.StatusBadRequest, "bad_origin", "站点地址不合法")
			return
		}
		if err := s.store.RemoveSite(r.Context(), u.ID, origin); err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "移除站点失败")
			return
		}
		sites, _ := s.store.Sites(r.Context(), u.ID)
		writeJSON(w, http.StatusOK, map[string]any{"sites": sites})
	})(w, r)
}

// containsSite 判断站点是否在已授权列表里。
//
// 只比 host，不比 scheme——用户授权了 https://a.com 之后，
// http://a.com 不应该因为协议不同就再拦一次（内容不同但风险相同）。
func containsSite(sites []string, origin string) bool {
	host := origin
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	for _, s := range sites {
		h := s
		if i := strings.Index(h, "://"); i >= 0 {
			h = h[i+3:]
		}
		if h == host {
			return true
		}
	}
	return false
}

// normalizeOrigin 归一化站点来源，只保留 scheme://host[:port]。
func normalizeOrigin(origin string) string {
	o := strings.TrimSpace(strings.ToLower(origin))
	if i := strings.Index(o, "://"); i >= 0 {
		o = o[i+3:]
	}
	if i := strings.IndexAny(o, "/?#"); i >= 0 {
		o = o[:i]
	}
	if o == "" {
		return ""
	}
	// 补回 scheme：只认 http/https
	if strings.HasPrefix(o, "localhost") || strings.Contains(o, ":") {
		return "http://" + o
	}
	return "https://" + o
}

// ---------------------------------------------------------------- 解析

type parseRequest struct {
	SQL    string `json:"sql"`
	Origin string `json:"origin,omitempty"`
}

type parseResponse struct {
	Statement  *sqltree.SQLStatement `json:"statement"`
	Balance    int64                 `json:"balance"`
	Cost       int64                 `json:"cost"`
	DurationMS int64                 `json:"duration_ms"`
}

// handleParse 是核心接口：解析一条 SQL，扣费，返回结构化结果。
//
// 计费顺序刻意是"先扣后算"：余额不足就直接拒绝，不给白嫖机会。
// 解析失败**不扣费**——用户拿到的是错误信息，不是结果，
// 扣了钱又没结果会引发纠纷。
func (s *Server) handleParse(w http.ResponseWriter, r *http.Request) {
	s.requireAuth(func(w http.ResponseWriter, r *http.Request, u *store.User) {
		if !s.limiter.allow(u.ID) {
			writeErr(w, http.StatusTooManyRequests, "rate_limited",
				"调用过于频繁，请稍后再试")
			return
		}
		var req parseRequest
		if err := decodeJSON(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", "请求体格式不对: "+err.Error())
			return
		}
		sqlStr := strings.TrimSpace(req.SQL)
		if sqlStr == "" {
			writeErr(w, http.StatusBadRequest, "empty_sql", "SQL 不能为空")
			return
		}

		// 站点授权校验：没授权过的网站不许用我的额度。
		//
		// 这一层放在服务端是有意为之——扩展端的校验可以被绕过
		// （改扩展、注入脚本、伪造 postMessage），服务端是最后一道闸。
		if origin := normalizeOrigin(req.Origin); origin != "" {
			sites, err := s.store.Sites(r.Context(), u.ID)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "internal", "查询站点失败")
				return
			}
			if !containsSite(sites, origin) {
				writeErr(w, http.StatusForbidden, "site_not_authorized",
					"该网站未获授权（"+origin+"）")
				return
			}
		}

		cost := s.cfg.CostPerParse
		if err := s.store.DeductBalance(r.Context(), u.ID, cost); err == store.ErrInsufficientBalance {
			writeErr(w, http.StatusPaymentRequired, "insufficient_balance",
				"余额不足，请先充值")
			return
		} else if err != nil {
			writeErr(w, http.StatusInternalServerError, "internal", "扣费失败")
			return
		}

		start := time.Now()
		stmt, err := sqltree.Parse(sqlStr)
		if err != nil {
			// 解析失败退费
			_ = s.store.AddBalance(r.Context(), u.ID, cost)
			writeErr(w, http.StatusBadRequest, "parse_failed", "SQL 解析失败: "+err.Error())
			return
		}
		elapsed := time.Since(start)

		// 重新读一次余额（并发下可能已经被别的请求扣过）
		cur, err := s.store.UserByID(r.Context(), u.ID)
		balance := int64(0)
		if err == nil {
			balance = cur.Balance
		}
		writeJSON(w, http.StatusOK, parseResponse{
			Statement:  stmt,
			Balance:    balance,
			Cost:       cost,
			DurationMS: elapsed.Milliseconds(),
		})
	})(w, r)
}
