package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/sqlext/server/internal/billing"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/config"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/store"
)

// newTestServer 起一个内存态的测试服务。
func newTestServer(t *testing.T, cost int64) *Server {
	t.Helper()
	cfg := config.Config{
		TokenTTL:        time.Hour,
		CostPerParse:    cost,
		FreeQuota:       10,
		SignupEnabled:   true,
		RateLimitPerMin: 1000,
	}
	return New(cfg, store.NewMemory(), billing.NewRegistry(billing.ManualChannel{}), nil)
}

// do 发一个请求并把响应解到 out（可为 nil）。
func do(t *testing.T, s *Server, method, path, token string, body any, out any) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if out != nil && rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), out)
	}
	return rec
}

// register 注册并返回令牌。
func register(t *testing.T, s *Server, email string) string {
	t.Helper()
	var resp struct {
		Token string `json:"token"`
	}
	rec := do(t, s, "POST", "/api/v1/auth/register", "", map[string]string{
		"email": email, "password": "passw0rd123",
	}, &resp)
	if rec.Code != http.StatusOK {
		t.Fatalf("注册失败: %d %s", rec.Code, rec.Body.String())
	}
	if resp.Token == "" {
		t.Fatal("注册没返回令牌")
	}
	return resp.Token
}

func TestRegisterAndLogin(t *testing.T) {
	s := newTestServer(t, 1)
	register(t, s, "a@b.com")

	// 重复注册应被拒
	rec := do(t, s, "POST", "/api/v1/auth/register", "", map[string]string{
		"email": "a@b.com", "password": "passw0rd123",
	}, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("重复注册期望 409，实际 %d", rec.Code)
	}

	// 口令错
	rec = do(t, s, "POST", "/api/v1/auth/login", "", map[string]string{
		"email": "a@b.com", "password": "wrong-pass",
	}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("错误口令期望 401，实际 %d", rec.Code)
	}

	// 正确口令
	var resp struct {
		Token string `json:"token"`
	}
	rec = do(t, s, "POST", "/api/v1/auth/login", "", map[string]string{
		"email": "a@b.com", "password": "passw0rd123",
	}, &resp)
	if rec.Code != http.StatusOK || resp.Token == "" {
		t.Errorf("登录失败: %d %s", rec.Code, rec.Body.String())
	}
}

func TestParseDeductsBalance(t *testing.T) {
	s := newTestServer(t, 1)
	token := register(t, s, "a@b.com")

	var out struct {
		Statement struct {
			Type    string `json:"type"`
			Details struct {
				From string `json:"from"`
			} `json:"details"`
		} `json:"statement"`
		Balance int64 `json:"balance"`
		Cost    int64 `json:"cost"`
	}
	rec := do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT id, name FROM users WHERE status = 1",
	}, &out)

	if rec.Code != http.StatusOK {
		t.Fatalf("解析失败: %d %s", rec.Code, rec.Body.String())
	}
	if out.Statement.Type != "Select" {
		t.Errorf("类型不对: %q", out.Statement.Type)
	}
	if out.Statement.Details.From != "users" {
		t.Errorf("表名不对: %q", out.Statement.Details.From)
	}
	if out.Cost != 1 {
		t.Errorf("扣费不对: %d", out.Cost)
	}
	if out.Balance != 9 {
		t.Errorf("余额不对: %d", out.Balance)
	}
}

func TestParseFailureRefunds(t *testing.T) {
	s := newTestServer(t, 1)
	token := register(t, s, "a@b.com")

	rec := do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT FROM WHERE ###",
	}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("非法 SQL 期望 400，实际 %d", rec.Code)
	}

	var bal struct {
		Balance int64 `json:"balance"`
	}
	do(t, s, "GET", "/api/v1/billing/balance", token, nil, &bal)
	if bal.Balance != 10 {
		t.Errorf("解析失败应该退费，余额应为 10，实际 %d", bal.Balance)
	}
}

func TestParseInsufficientBalance(t *testing.T) {
	s := newTestServer(t, 100) // 一次解析 100 积分，赠送只有 10
	token := register(t, s, "a@b.com")

	rec := do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT 1 FROM t",
	}, nil)
	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("余额不足期望 402，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "insufficient_balance") {
		t.Errorf("错误码不对: %s", rec.Body.String())
	}
}

func TestParseRequiresAuth(t *testing.T) {
	s := newTestServer(t, 1)
	rec := do(t, s, "POST", "/api/v1/parse", "", map[string]string{
		"sql": "SELECT 1 FROM t",
	}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("未登录期望 401，实际 %d", rec.Code)
	}
}

// TestSiteAuthorization 站点没授权时不能花用户的额度。
func TestSiteAuthorization(t *testing.T) {
	s := newTestServer(t, 1)
	token := register(t, s, "a@b.com")

	// 没授权的站点
	rec := do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT 1 FROM t", "origin": "https://evil.example.com",
	}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("未授权站点期望 403，实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 授权后放行
	rec = do(t, s, "POST", "/api/v1/sites", token, map[string]string{
		"origin": "https://good.example.com",
	}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("授权站点失败: %d %s", rec.Code, rec.Body.String())
	}

	rec = do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT id FROM t", "origin": "https://good.example.com",
	}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("已授权站点应放行，实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 撤销授权后重新拒绝
	rec = do(t, s, "DELETE", "/api/v1/sites?origin=https%3A%2F%2Fgood.example.com", token, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("撤销授权失败: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT id FROM t", "origin": "https://good.example.com",
	}, nil)
	if rec.Code != http.StatusForbidden {
		t.Errorf("撤销后应拒绝，实际 %d", rec.Code)
	}
}

func TestCommentSQLIsParsedCleanly(t *testing.T) {
	s := newTestServer(t, 1)
	token := register(t, s, "a@b.com")

	// 带各种注释的多行 SQL，结果里不该残留注释
	var out struct {
		Statement struct {
			Details struct {
				From          string   `json:"from"`
				SelectedExprs []string `json:"selected_exprs"`
			} `json:"details"`
		} `json:"statement"`
	}
	rec := do(t, s, "POST", "/api/v1/parse", token, map[string]string{
		"sql": "SELECT `id`, name -- 这是注释\nFROM users /* 块注释 */ WHERE id = 1",
	}, &out)
	if rec.Code != http.StatusOK {
		t.Fatalf("解析失败: %d %s", rec.Code, rec.Body.String())
	}
	if out.Statement.Details.From != "users" {
		t.Errorf("表名不对: %q", out.Statement.Details.From)
	}
	if len(out.Statement.Details.SelectedExprs) != 2 {
		t.Errorf("字段数不对: %v", out.Statement.Details.SelectedExprs)
	}
}

func TestCreateOrder(t *testing.T) {
	s := newTestServer(t, 1)
	token := register(t, s, "a@b.com")

	var out struct {
		Order struct {
			ID     string `json:"id"`
			Amount int64  `json:"amount"`
			Status string `json:"status"`
		} `json:"order"`
		Checkout struct {
			Channel string `json:"channel"`
			Method  string `json:"method"`
		} `json:"checkout"`
	}
	rec := do(t, s, "POST", "/api/v1/billing/orders", token, map[string]any{
		"amount": 100,
	}, &out)
	if rec.Code != http.StatusOK {
		t.Fatalf("下单失败: %d %s", rec.Code, rec.Body.String())
	}
	if out.Order.ID == "" || out.Order.Amount != 100 || out.Order.Status != store.OrderPending {
		t.Errorf("订单不对: %+v", out.Order)
	}
	if out.Checkout.Channel != "manual" {
		t.Errorf("渠道不对: %+v", out.Checkout)
	}
}
