// Package server 实现 sqlextd 的 HTTP 接口。
//
// 设计要点：
//   - 认证走 Bearer 令牌（服务端可吊销），不用 JWT
//   - 余额扣减在服务端做，插件端永远不做可信判断
//   - 解析走 bus/sqltree，插件端不碰解析逻辑
package server

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AIntelligenceGame/bus/sqlext/server/internal/billing"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/config"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/store"
)

// Server 是 HTTP 服务。
type Server struct {
	cfg      config.Config
	store    store.Store
	channels *billing.Registry
	limiter  *rateLimiter
	logger   *log.Logger
}

// New 创建服务。
func New(cfg config.Config, st store.Store, channels *billing.Registry, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	if channels == nil {
		channels = billing.NewRegistry(billing.ManualChannel{})
	}
	return &Server{
		cfg:      cfg,
		store:    st,
		channels: channels,
		limiter:  newRateLimiter(cfg.RateLimitPerMin),
		logger:   logger,
	}
}

// Handler 返回路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 健康检查
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// 认证
	mux.HandleFunc("POST /api/v1/auth/register", s.handleRegister)
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("GET /api/v1/auth/me", s.handleMe)

	// 余额与充值
	mux.HandleFunc("GET /api/v1/billing/balance", s.handleBalance)
	mux.HandleFunc("GET /api/v1/billing/channels", s.handleChannels)
	mux.HandleFunc("POST /api/v1/billing/orders", s.handleCreateOrder)
	mux.HandleFunc("GET /api/v1/billing/orders", s.handleListOrders)
	mux.HandleFunc("GET /api/v1/billing/orders/{id}", s.handleGetOrder)

	// 站点授权
	mux.HandleFunc("GET /api/v1/sites", s.handleListSites)
	mux.HandleFunc("POST /api/v1/sites", s.handleAddSite)
	mux.HandleFunc("DELETE /api/v1/sites", s.handleRemoveSite)

	// 解析
	mux.HandleFunc("POST /api/v1/parse", s.handleParse)

	// 静态页面（落地页、隐私政策）。
	// Chrome Web Store 要求隐私政策有公网 URL，直接由本服务托管最省事。
	if h := webHandler(s.cfg.WebDir); h != nil {
		mux.Handle("GET /", h)
	}

	return s.withMiddleware(mux)
}

// withMiddleware 套上 CORS、日志、panic 恢复。
func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Printf("panic: %v %s", rec, r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error": "internal error",
				})
			}
		}()

		// CORS：扩展页面是 chrome-extension:// 源，本地调试还会用 http://localhost。
		origin := r.Header.Get("Origin")
		if origin != "" && s.originAllowed(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func (s *Server) originAllowed(origin string) bool {
	if len(s.cfg.CORSOrigins) == 0 {
		// 没配置就只放开本地开发用的地址；生产环境请显式配置。
		return strings.HasPrefix(origin, "http://localhost") ||
			strings.HasPrefix(origin, "http://127.0.0.1") ||
			strings.HasPrefix(origin, "chrome-extension://")
	}
	for _, o := range s.cfg.CORSOrigins {
		if o == "*" || strings.EqualFold(o, origin) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- 请求/响应工具

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: msg, Code: code})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// currentUser 从 Authorization 头取令牌，查出用户。
func (s *Server) currentUser(r *http.Request) (*store.User, string, error) {
	token := bearerToken(r)
	if token == "" {
		return nil, "", errors.New("missing token")
	}
	sess, err := s.store.Session(r.Context(), token)
	if err != nil {
		return nil, "", errors.New("invalid token")
	}
	u, err := s.store.UserByID(r.Context(), sess.UserID)
	if err != nil {
		return nil, "", errors.New("invalid token")
	}
	return u, token, nil
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	parts := strings.SplitN(h, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

// requireAuth 是一个中间件包装。
func (s *Server) requireAuth(h func(http.ResponseWriter, *http.Request, *store.User)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, err := s.currentUser(r)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "未登录或登录已过期")
			return
		}
		h(w, r, u)
	}
}

// ---------------------------------------------------------------- 限流

// rateLimiter 是一个简单的每用户每分钟固定窗口限流器。
type rateLimiter struct {
	mu      sync.Mutex
	limit   int
	windows map[string]*window
}

type window struct {
	start time.Time
	count int
}

func newRateLimiter(limit int) *rateLimiter {
	if limit <= 0 {
		limit = 0 // 0 = 不限
	}
	return &rateLimiter{limit: limit, windows: map[string]*window{}}
}

// allow 判断本次调用是否放行。
func (l *rateLimiter) allow(key string) bool {
	if l == nil || l.limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	w, ok := l.windows[key]
	if !ok || now.Sub(w.start) >= time.Minute {
		l.windows[key] = &window{start: now, count: 1}
		return true
	}
	if w.count >= l.limit {
		return false
	}
	w.count++
	return true
}

// ---------------------------------------------------------------- 小工具

func atoiDefault(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return n
}
