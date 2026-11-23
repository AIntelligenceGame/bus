package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// verifyTokenFor 走「口令 + 验证码」换一张敏感操作票据(未开二次验证时只要口令)。
func verifyTokenFor(t *testing.T, p *Platform, token, password string) string {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"password": password})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/account/verify", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Token", token)
	p.handleVerify(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	vt, _ := out["verify_token"].(string)
	if vt == "" {
		t.Fatalf("获取票据失败: %d %v", rec.Code, out)
	}
	return vt
}

// saveChannelViaAPI 调「保存渠道配置」接口。
func saveChannelViaAPI(t *testing.T, p *Platform, token, verifyToken, channel string, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/channels/"+channel, strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Token", token)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.SetPathValue("channel", channel)
	p.handleSaveChannel(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestSaveChannelKeepsExistingSecrets 保存渠道配置时,前端拿到的密钥是掩码/空,
// 回传空值不应该把**已保存的密钥**清掉。
//
// 这是真实踩过的坑:前端为了不把明文显示在页面上,敏感字段一律留空提交,
// 如果后端不合并旧值,用户每保存一次配置就把密钥擦掉一次 ——
// 页面还提示「配置已保存(密钥已加密入库)」,钱却收不了了。
func TestSaveChannelKeepsExistingSecrets(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户密钥", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)
	user, _ := p.store.UserByName("商户密钥")
	vt := verifyTokenFor(t, p, token, "supersecret")

	// 第一次保存:带上密钥
	code, resp = saveChannelViaAPI(t, p, token, vt, "wechat", map[string]any{
		"enabled": false, "is_default": false,
		"wechat": map[string]any{
			"app_id": "wx123", "mch_id": "1900000109", "serial_no": "SERIAL",
			"private_key_pem": "-----BEGIN PRIVATE KEY-----REAL-----END PRIVATE KEY-----",
			"api_v3_key":      "REAL_V3_KEY",
		},
	})
	if code != http.StatusOK || resp["ok"] != true {
		t.Fatalf("首次保存失败: %d %v", code, resp)
	}

	// 第二次保存:模拟前端回传(敏感字段留空)
	code, resp = saveChannelViaAPI(t, p, token, vt, "wechat", map[string]any{
		"enabled": false, "is_default": false,
		"wechat": map[string]any{
			"app_id": "wx123", "mch_id": "1900000109", "serial_no": "SERIAL",
			"private_key_pem": "", "api_v3_key": "",
		},
	})
	if code != http.StatusOK || resp["ok"] != true {
		t.Fatalf("二次保存失败: %d %v", code, resp)
	}

	cfgs, err := p.store.Channels(user.UserID)
	if err != nil {
		t.Fatalf("读取渠道配置失败: %v", err)
	}
	var found *ChannelConfig
	for i := range cfgs {
		if cfgs[i].Channel == "wechat" {
			found = &cfgs[i]
		}
	}
	if found == nil || found.Wechat == nil {
		t.Fatal("微信配置不存在")
	}
	// 必须是**原来那一份**:
	// 只断言「非空」的话,存错字段、存了旧值、被换成另一份密钥都能通过。
	if found.Wechat.PrivateKeyPEM != "-----BEGIN PRIVATE KEY-----REAL-----END PRIVATE KEY-----" {
		t.Fatalf("商户私钥内容不对: %q", found.Wechat.PrivateKeyPEM)
	}
	if found.Wechat.APIv3Key != "REAL_V3_KEY" {
		t.Fatalf("APIv3 密钥内容不对: %q", found.Wechat.APIv3Key)
	}
}
