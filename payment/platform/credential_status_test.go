package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mustHashPassword 算一个口令哈希,失败就直接让测试挂掉。
func mustHashPassword(t *testing.T, password string) string {
	t.Helper()
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword 失败: %v", err)
	}
	return hash
}

// TestCredentialFailureIsNotSessionExpiry 「凭据不对」不能返回 401。
//
// 401 在这套 API 里的含义是「会话没了」:前端收到后会清掉本地令牌
// 并跳回登录页。如果「敏感操作弹窗里口令打错一次」也返回 401,用户就会被
// 直接踢出控制台 —— 正在填的表单、已输入的密钥全丢,还得重新登录。
//
// 所以凭据类失败一律 403(与 requireVerified 的票据失败保持一致),
// 401 只留给「令牌缺失/过期/无效」。
func TestCredentialFailureIsNotSessionExpiry(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户丁")
	if err := store.SetPassword(user.UserID, mustHashPassword(t, "rightpass")); err != nil {
		t.Fatalf("设置口令失败: %v", err)
	}

	cases := []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		path    string
		body    string
	}{
		{
			name:    "身份验证",
			handler: p.handleVerify,
			path:    "/api/account/verify",
			body:    `{"password":"wrongpass"}`,
		},
		{
			name:    "修改口令",
			handler: p.handleSetPassword,
			path:    "/api/account/password",
			body:    `{"old_password":"wrongpass","new_password":"newpass123"}`,
		},
		{
			name:    "开启二次验证",
			handler: p.handleEnableMFA,
			path:    "/api/account/mfa/enable",
			body:    `{"type":"email","target":"a@example.com","password":"wrongpass"}`,
		},
		{
			name:    "关闭二次验证",
			handler: p.handleDisableMFA,
			path:    "/api/account/mfa/disable",
			body:    `{"password":"wrongpass"}`,
		},
		{
			name:    "绑定备用验证方式",
			handler: p.handleBindMFABackup,
			path:    "/api/account/mfa/backup",
			body:    `{"type":"email","target":"a@example.com","password":"wrongpass"}`,
		},
		{
			name:    "下发绑定验证码",
			handler: p.handleSendMFACode,
			path:    "/api/account/mfa/code",
			body:    `{"type":"email","target":"a@example.com","purpose":"bind","code":"wrongpass"}`,
		},
		{
			name:    "保存验证通道",
			handler: p.handleSaveMFAChannel,
			path:    "/api/account/mfa/channels",
			body:    `{"channel":"email","password":"wrongpass","config":{}}`,
		},
		{
			name:    "删除验证通道",
			handler: p.handleDeleteMFAChannel,
			path:    "/api/account/mfa/channels/delete",
			body:    `{"channel":"email","password":"wrongpass"}`,
		},
		{
			name:    "测试验证通道",
			handler: p.handleTestMFAChannel,
			path:    "/api/account/mfa/channels/test",
			body:    `{"channel":"email","target":"a@example.com","password":"wrongpass"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("X-User-Token", user.AccessToken)
			req.Header.Set("Content-Type", "application/json")
			tc.handler(rec, req)

			if rec.Code == http.StatusUnauthorized {
				t.Fatalf("凭据错误不能返回 401(会被前端当成会话失效而登出),实际 %d: %s",
					rec.Code, rec.Body.String())
			}
			if rec.Code != http.StatusForbidden {
				t.Fatalf("凭据错误应返回 403,实际 %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestSessionExpiryStillReturns401 反过来:令牌真的失效时必须还是 401。
//
// 上面那条测试把「凭据错」改成 403 之后,不能顺手把「会话没了」也放跑 ——
// 那会让失效的令牌一直留在前端,页面拿着废令牌反复请求。
func TestSessionExpiryStillReturns401(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户戊")
	if err := store.SetPassword(user.UserID, mustHashPassword(t, "rightpass")); err != nil {
		t.Fatalf("设置口令失败: %v", err)
	}

	t.Run("令牌无效", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/account/verify",
			strings.NewReader(`{"password":"rightpass"}`))
		req.Header.Set("X-User-Token", "not-a-real-token")
		req.Header.Set("Content-Type", "application/json")
		p.handleVerify(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("令牌无效应返回 401,实际 %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("没有令牌", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/account/verify",
			strings.NewReader(`{"password":"rightpass"}`))
		req.Header.Set("Content-Type", "application/json")
		p.handleVerify(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("没有令牌应返回 401,实际 %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("令牌有效且口令正确时不该被拒", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/account/verify",
			strings.NewReader(`{"password":"rightpass"}`))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		p.handleVerify(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("口令正确应通过,实际 %d: %s", rec.Code, rec.Body.String())
		}
	})
}
