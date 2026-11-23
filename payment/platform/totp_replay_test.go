package platform

import (
	"net/http"
	"testing"
	"time"
)

// advanceTOTPClock 把「动态码的当前时间」往前推,并在测试结束后还原。
//
// 动态码一次性(RFC 6238 §5.2),一个时间片(30 秒)里只能用一次;
// 测试要连续做多次校验,就得能推进时间,而不是干等 30 秒。
func advanceTOTPClock(t *testing.T, d time.Duration) {
	t.Helper()
	prev := totpNow
	base := prev()
	totpNow = func() time.Time { return base.Add(d) }
	t.Cleanup(func() { totpNow = prev })
}

// TestTOTPCodeCannotBeReplayed 同一个动态码不能用第二次。
//
// RFC 6238 §5.2:「The verifier MUST NOT accept the second attempt of the OTP
// after the successful validation has been issued for the first OTP,
// which ensures one-time only use of an OTP」。
//
// 不做一次性处理的话,一个 6 位码在它的时间片(允许 ±1 片,最长 90 秒)里
// 可以被反复重放 —— 被瞄一眼、被抓一次包就能用第二次。
func TestTOTPCodeCannotBeReplayed(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户重放", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)

	// 绑定 TOTP。
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn, err := totpCodeAt(secret, totpNow())
	if err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("绑定 TOTP 失败: %d %v", code, resp)
	}

	// 第一次用这个码:成功。
	dyn, _ = totpCodeAt(secret, totpNow())
	if !p.store.ConsumeTOTPCode(totpTestUserID(t, p, "商户重放"), secret, dyn, totpNow()) {
		t.Fatal("第一次校验应当通过")
	}
	// 第二次用**同一个码**:必须被拒。
	if p.store.ConsumeTOTPCode(totpTestUserID(t, p, "商户重放"), secret, dyn, totpNow()) {
		t.Fatal("同一个动态码不能用第二次(RFC 6238 §5.2 要求一次性使用)")
	}
	// 下一片的码可以用。
	advanceTOTPClock(t, time.Duration(totpPeriod)*time.Second)
	nextDyn, _ := totpCodeAt(secret, totpNow())
	if !p.store.ConsumeTOTPCode(totpTestUserID(t, p, "商户重放"), secret, nextDyn, totpNow()) {
		t.Fatal("下一个时间片的动态码应当通过")
	}
}

// TestTOTPReplayBlockedOnLogin 重放保护要落在**登录**这条路上。
func TestTOTPReplayBlockedOnLogin(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户重放登录", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)

	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn, _ := totpCodeAt(secret, totpNow())
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("绑定 TOTP 失败: %d %v", code, resp)
	}

	// 第一次带码登录:成功。
	dyn, _ = totpCodeAt(secret, totpNow())
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户重放登录", "password": "supersecret", "code": dyn,
	})
	if code != http.StatusOK {
		t.Fatalf("带动态码登录应成功,实际 %d: %v", code, resp)
	}

	// 再用同一个码登录:必须失败。
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户重放登录", "password": "supersecret", "code": dyn,
	})
	if code == http.StatusOK {
		t.Fatal("同一个动态码不该能登录第二次")
	}
	if resp["mfa_required"] != true {
		t.Fatalf("应当提示需要验证码: %v", resp)
	}
}

// totpTestUserID 取测试用户的 ID。
func totpTestUserID(t *testing.T, p *Platform, name string) string {
	t.Helper()
	user, err := p.store.UserByName(name)
	if err != nil || user == nil {
		t.Fatalf("查用户 %s 失败: %v", name, err)
	}
	return user.UserID
}
