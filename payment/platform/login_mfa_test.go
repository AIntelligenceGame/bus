package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// loginWithBody 调用登录接口,请求体可自定义(用于带验证码/注册码的场景)。
func loginWithBody(t *testing.T, p *Platform, body map[string]string) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("序列化登录请求失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	p.handleLogin(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// 造一个开了二次验证的账户。
func newMFAUser(t *testing.T, p *Platform, name, password string) *User {
	t.Helper()
	if code, resp := login(t, p, name, password); code != http.StatusOK {
		t.Fatalf("首次登录应成功,实际 %d: %v", code, resp)
	}
	user, err := p.store.UserByName(name)
	if err != nil || user == nil {
		t.Fatalf("查询用户失败: %v", err)
	}
	if err := p.store.SetMFA(user.UserID, MFATypeEmail, "ops@example.com", true); err != nil {
		t.Fatalf("开启二次验证失败: %v", err)
	}
	return user
}

// TestLoginMFARequiredFlag 开了二次验证但没填验证码时,登录要回 mfa_required,
// 让前端据此把验证码输入框显示出来(没开二次验证的账户不该看到那个框)。
func TestLoginMFARequiredFlag(t *testing.T) {
	p, _ := newTestPlatform(t)
	user := newMFAUser(t, p, "商户MFA", "supersecret")

	code, resp := loginWithBody(t, p, map[string]string{
		"name": "商户MFA", "password": "supersecret",
	})
	if code != http.StatusUnauthorized {
		t.Fatalf("缺验证码时应返回 401,实际 %d: %v", code, resp)
	}
	if resp["mfa_required"] != true {
		t.Fatalf("响应应带 mfa_required=true,实际: %v", resp)
	}

	// 关键:「忘了填验证码」不算登录失败,不能计入失败次数,更不能因此锁定账户。
	got, err := p.store.UserByName("商户MFA")
	if err != nil || got == nil {
		t.Fatalf("查询用户失败: %v", err)
	}
	if got.FailedLogins != 0 {
		t.Fatalf("缺验证码不应计入失败次数,实际 FailedLogins=%d", got.FailedLogins)
	}
	if locked, _ := p.lockedUntil(got); locked {
		t.Fatal("缺验证码不应导致账户锁定")
	}
	if got.UserID != user.UserID {
		t.Fatal("用户不应被替换")
	}
}

// TestLoginMFAWithCodeStillWorks 补上验证码后应当正常登录。
func TestLoginMFAWithCodeStillWorks(t *testing.T) {
	p, _ := newTestPlatform(t)
	user := newMFAUser(t, p, "商户MFA2", "supersecret")
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeLogin, "123456", time.Minute); err != nil {
		t.Fatalf("写入验证码失败: %v", err)
	}
	code, resp := loginWithBody(t, p, map[string]string{
		"name": "商户MFA2", "password": "supersecret", "code": "123456",
	})
	if code != http.StatusOK {
		t.Fatalf("带正确验证码应登录成功,实际 %d: %v", code, resp)
	}
	if resp["token"] == "" || resp["token"] == nil {
		t.Fatalf("登录成功应返回令牌: %v", resp)
	}
}

// TestLoginNoMFAHasNoFlag 没开二次验证的账户,登录响应里不该有 mfa_required,
// 前端就是靠这个字段决定要不要显示验证码框的。
func TestLoginNoMFAHasNoFlag(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := loginWithBody(t, p, map[string]string{
		"name": "商户普通", "password": "supersecret",
	})
	if code != http.StatusOK {
		t.Fatalf("登录应成功,实际 %d: %v", code, resp)
	}
	if _, ok := resp["mfa_required"]; ok {
		t.Fatalf("未开二次验证不应返回 mfa_required 字段: %v", resp)
	}
}

// TestVersionTellsRegistrationCodeRequired /api/version 要告诉前端平台是否要注册码。
func TestVersionTellsRegistrationCodeRequired(t *testing.T) {
	p, _ := newTestPlatform(t)
	p.opts.RegistrationCode = "REG-123"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/version", nil)
	p.handleVersion(rec, req)
	out := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析版本响应失败: %v", err)
	}
	if out["registration_code_required"] != true {
		t.Fatalf("应返回 registration_code_required=true: %v", out)
	}

	p2, _ := newTestPlatform(t)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/version", nil)
	p2.handleVersion(rec2, req2)
	out2 := map[string]any{}
	if err := json.Unmarshal(rec2.Body.Bytes(), &out2); err != nil {
		t.Fatalf("解析版本响应失败: %v", err)
	}
	if out2["registration_code_required"] != false {
		t.Fatalf("未设注册码时应返回 false: %v", out2)
	}
}
