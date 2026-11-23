package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func postJSON(t *testing.T, p *Platform, path, token string, body map[string]string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-User-Token", token)
	}
	switch path {
	case "/api/account/mfa/enable":
		p.handleEnableMFA(rec, req)
	case "/api/account/mfa/disable":
		p.handleDisableMFA(rec, req)
	case "/api/account/mfa/code":
		p.handleSendMFACode(rec, req)
	default:
		t.Fatalf("未知路径 %s", path)
	}
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestReproDisableMFAThenLogin(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户关", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户关")

	// 开启二次验证
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = postJSON(t, p, "/api/account/mfa/enable", token, map[string]string{
		"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111",
	})
	if code != http.StatusOK {
		t.Fatalf("开启二次验证失败: %d %v", code, resp)
	}
	u, _ := p.store.UserByName("商户关")
	if !u.MFAEnabled {
		t.Fatal("开启后 MFAEnabled 应为 true")
	}

	// 关闭二次验证
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeDisable, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = postJSON(t, p, "/api/account/mfa/disable", token, map[string]string{
		"password": "supersecret", "code": "222222",
	})
	if code != http.StatusOK {
		t.Fatalf("关闭二次验证失败: %d %v", code, resp)
	}
	u, _ = p.store.UserByName("商户关")
	t.Logf("关闭后 MFAEnabled=%v MFAType=%q MFATarget=%q", u.MFAEnabled, u.MFAType, u.MFATarget)
	if u.MFAEnabled {
		t.Fatal("关闭后 MFAEnabled 仍为 true")
	}

	// 再登录:不应该再要验证码
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户关", "password": "supersecret",
	})
	if code != http.StatusOK {
		t.Fatalf("关闭后登录应成功,实际 %d: %v", code, resp)
	}
	if resp["mfa_required"] == true {
		t.Fatalf("关闭后不应再要求验证码: %v", resp)
	}
}
