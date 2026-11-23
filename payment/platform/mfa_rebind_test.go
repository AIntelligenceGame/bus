package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------- 换绑确认
//
// 背景:账户已经开过二次验证时,再来一次「开启 / 保存新绑定」就是**换绑** ——
// 旧绑定会当场失效。
//
// 如果只要口令就能换绑,那么:
//   - 拿到口令的人(共用电脑、浏览器被窥、密码泄露)可以在别人的页面
//     上点「重新生成密钥」,把验证器换成自己那份;
//   - 正主的验证器当场作废,人也直接被挡在门外 ——
//     二次验证反而替攻击者开了门。
//
// 所以换绑必须由**当前绑定的人**确认:
//   - 当前是验证器 App → 旧 App 里的动态码(拿**生效**密钥校验);
//   - 当前是邮箱/短信 → 发到**已绑定地址**的一次性验证码(purpose=rebind)。
//
// 验证器 App 的确认发生在**生成密钥**那一步(而不是「保存」那一步):
// 只有先确认了旧绑定才能把新密钥发出去;扫完新码之后旧条目可能已被覆盖,
// 旧动态码就再也取不到了 —— 那时候再要一次旧码,换绑永远做不完。

// bindTOTP 给账户绑定一个验证器 App,返回生效密钥。
func bindTOTP(t *testing.T, p *Platform, token string) string {
	t.Helper()
	code, resp := mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("生成密钥失败: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn, err := totpCodeAt(secret, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("绑定验证器 App 失败: %d %v", code, resp)
	}
	return secret
}

// TestRebindTOTPNeedsCurrentHolder 换绑验证器 App 必须由旧 App 确认。
//
// 这条用例对应真实的坑:用户 A 已经绑过一次验证器,别人拿着 A 的口令
// 在页面上点一下「重新生成密钥」,再把新密钥绑到自己手机上 ——
// A 的验证器当场作废,A 再也登不进来。
//
// 确认**只有一次**,而且必须发生在「生成密钥」之前:
//   - 密钥没发出去之前,正主的旧 App 还是好的,旧动态码取得到;
//   - 扫完新码之后旧条目可能已被覆盖,旧码就再也取不到了,
//     这时候再要一次旧码,换绑就永远做不完。
func TestRebindTOTPNeedsCurrentHolder(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户换绑", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户换绑")

	// 首绑:不需要「当前绑定确认」(本来就没有当前绑定)。
	secret := bindTOTP(t, p, token)
	if p.store.MFATOTPSecret(user.UserID) != secret {
		t.Fatal("绑定后生效密钥应当就是刚生成的那份")
	}

	// 攻击者:只有会话 + 口令,没有旧 App 的动态码。
	// 1) 不能再生成新密钥(密钥不该发给不是当前绑定的人)。
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusForbidden {
		t.Fatalf("换绑生成密钥应被拒绝,实际 %d: %v", code, resp)
	}
	if resp["secret"] != nil {
		t.Fatalf("被拒绝时不该返回密钥: %v", resp)
	}
	// 被拒时也不能留下任何「待确认密钥」,否则下一次 enable 就把它捡起来了。
	if p.store.MFATOTPPendingSecret(user.UserID) != "" {
		t.Fatal("被拒后不该留下待确认密钥")
	}

	// 2) 旧绑定必须原封不动(正主还能登录)。
	if p.store.MFATOTPSecret(user.UserID) != secret {
		t.Fatal("被拒后旧绑定必须原封不动(正主还能登录)")
	}
	if !verifyTOTPCode(p.store.MFATOTPSecret(user.UserID), mustTOTPCode(t, secret), time.Now()) {
		t.Fatal("旧验证器应当仍然可用")
	}

	// 3) 正主带着旧 App 的动态码来换绑 → 允许;
	//    绑定这一步只要**新密钥**的动态码(旧码这时可能已经取不到了)。
	oldCode := mustTOTPCode(t, secret)
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "",
		map[string]any{"current_code": oldCode})
	if code != http.StatusOK {
		t.Fatalf("带当前绑定确认生成密钥应成功: %d %v", code, resp)
	}
	newSecret, _ := resp["secret"].(string)
	if newSecret == "" || newSecret == secret {
		t.Fatalf("应当返回一把新密钥: %q", newSecret)
	}
	newCode := mustTOTPCode(t, newSecret)
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": newCode})
	if code != http.StatusOK {
		t.Fatalf("带新动态码换绑应成功: %d %v", code, resp)
	}
	if p.store.MFATOTPSecret(user.UserID) != newSecret {
		t.Fatal("换绑成功后生效密钥应当是新密钥")
	}
}

// TestStalePendingSecretIsVoid 绑定之前拿到的待确认密钥,绑定之后必须作废。
//
// 换绑验证器 App 的确认只做一次(在生成密钥那一步),所以「待确认密钥」
// 这条通道必须自己站得住:它在**绑定成立之前**被拿到,就不能在绑定
// 成立之后被用来把验证器换成自己的。
func TestStalePendingSecretIsVoid(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户旧密钥", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户旧密钥")

	// 攻击者:账户还没开二次验证时,拿一份待确认密钥(这时候不需要确认)。
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("未开二次验证时生成密钥应成功: %d %v", code, resp)
	}
	staleSecret, _ := resp["secret"].(string)
	if staleSecret == "" {
		t.Fatal("应当返回密钥")
	}

	// 之后绑定成立了(邮箱绑定)—— 待确认密钥随之作废。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定邮箱失败: %d %v", code, resp)
	}
	if p.store.MFATOTPPendingSecret(user.UserID) != "" {
		t.Fatal("绑定成立后,之前那份待确认密钥必须作废")
	}

	// 攻击者拿旧密钥来绑验证器 → 没有待确认密钥可用,换不了绑。
	staleCode, err := totpCodeAt(staleSecret, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": staleCode})
	if code == http.StatusOK {
		t.Fatal("绑定之前拿到的密钥不该能换绑")
	}
	u, _ := p.store.UserByName("商户旧密钥")
	if u.MFAType != MFATypeEmail || u.MFATarget != "a@b.com" {
		t.Fatalf("原绑定必须保留,实际 %s/%s", u.MFAType, u.MFATarget)
	}
}

// TestRebindTOTPWithoutCurrentCodeRejected 没有确认码时,连生成密钥都不行。
//
// 这一条单独列出来,是因为它是「别人在页面上点一下就能看到新密钥」
// 这个坑的最小复现:接口必须挡住,不能只是「绑定那一步挡住」。
func TestRebindTOTPWithoutCurrentCodeRejected(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户看密钥", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	bindTOTP(t, p, token)

	// 错码也不行。
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "",
		map[string]any{"current_code": "000000"})
	if code != http.StatusForbidden {
		t.Fatalf("错误的当前绑定确认应被拒绝,实际 %d: %v", code, resp)
	}
	msg, _ := resp["error"].(string)
	if !strings.Contains(msg, "当前绑定") {
		t.Fatalf("错误提示要说清楚是「当前绑定确认」没过: %q", msg)
	}
}

// TestRebindEmailNeedsCurrentHolder 换接收地址也要旧地址的验证码。
func TestRebindEmailNeedsCurrentHolder(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户换地址", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户换地址")

	// 首绑邮箱。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定邮箱失败: %d %v", code, resp)
	}

	// 换地址:新地址的绑定码 + 旧地址的确认码,缺一不可。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "c@d.com", "password": "supersecret", "code": "222222"})
	if code != http.StatusForbidden {
		t.Fatalf("换地址没有当前绑定确认应被拒绝,实际 %d: %v", code, resp)
	}
	u, _ := p.store.UserByName("商户换地址")
	if u.MFATarget != "a@b.com" {
		t.Fatalf("被拒后旧地址必须保留,实际 %q", u.MFATarget)
	}

	// 带上旧地址的确认码 → 允许换绑。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeRebind, "333333", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{
			"type": "email", "target": "c@d.com", "password": "supersecret",
			"code": "222222", "current_code": "333333",
		})
	if code != http.StatusOK {
		t.Fatalf("带当前绑定确认换地址应成功: %d %v", code, resp)
	}
	u, _ = p.store.UserByName("商户换地址")
	if u.MFATarget != "c@d.com" {
		t.Fatalf("换绑后地址应当更新,实际 %q", u.MFATarget)
	}
}

// TestRebindSameAddressNeedsNoSecondCode 地址没变时不用再要一个确认码。
//
// 同一个地址重新绑定一次,发到该地址的绑定码本身就证明了
// 「这个地址还在你手里」—— 再收一封一模一样的验证码只是折腾用户。
func TestRebindSameAddressNeedsNoSecondCode(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户同址", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户同址")

	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定邮箱失败: %d %v", code, resp)
	}
	// 同一个地址再绑一次:只要绑定码。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "222222"})
	if code != http.StatusOK {
		t.Fatalf("同址重新绑定不该再要确认码: %d %v", code, resp)
	}
}

// TestRebindCodeOnlyGoesToBoundAddress 换绑确认码只能发到已绑定的地址。
//
// 否则就成了经典绕法:把确认码发到攻击者自己的邮箱。
func TestRebindCodeOnlyGoesToBoundAddress(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户确认码", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户确认码")

	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "a@b.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定邮箱失败: %d %v", code, resp)
	}

	// 指定别的地址 → 拒绝。
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{"purpose": "rebind", "target": "attacker@evil.com"})
	if code != http.StatusBadRequest {
		t.Fatalf("确认码只能发到已绑定地址,实际 %d: %v", code, resp)
	}
	// 不指定地址 → 发到已绑定的那个。
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{"purpose": "rebind"})
	if code != http.StatusOK {
		t.Fatalf("发送换绑确认码失败: %d %v", code, resp)
	}
	if resp["target"] != maskTarget("a@b.com") {
		t.Fatalf("确认码应当发到已绑定地址,实际 %v", resp["target"])
	}
}

// TestRebindUndeliverableBindingSelfHeal 通道不可用的旧绑定不卡死换绑。
//
// 老规矩:那种绑定本来就拦不住任何人(码只写日志),
// 卡死只会让账户再也改不了绑定。
func TestRebindUndeliverableBindingSelfHeal(t *testing.T) {
	p := newLogOnlyPlatform(t)
	code, resp := login(t, p, "商户失效换绑", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户失效换绑")
	// 手工造一条「开着邮箱二次验证、但没有通道」的绑定。
	if err := p.store.SetMFA(user.UserID, MFATypeEmail, "a@b.com", true); err != nil {
		t.Fatal(err)
	}
	// 换成验证器 App:通道不可用 → 不要求当前绑定确认,直接放行。
	code, resp = mfaPost(t, p.handleTOTPSetup, "/api/account/mfa/totp/setup", token, "", map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("失效绑定不该卡死换绑: %d %v", code, resp)
	}
	secret, _ := resp["secret"].(string)
	dyn := mustTOTPCode(t, secret)
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "totp", "password": "supersecret", "code": dyn})
	if code != http.StatusOK {
		t.Fatalf("失效绑定不该卡死换绑: %d %v", code, resp)
	}
}

// mustTOTPCode 算当前时刻的动态码。
func mustTOTPCode(t *testing.T, secret string) string {
	t.Helper()
	code, err := totpCodeAt(secret, time.Now())
	if err != nil {
		t.Fatalf("计算动态码失败: %v", err)
	}
	return code
}

// TestAdminResetMFA 平台管理员可以重置商户的二次验证。
//
// 换绑要「当前绑定的人」确认,那正主真把手机丢了怎么办?
// 那时他给不出旧动态码、也收不到旧地址的验证码 ——
// 只能由平台方核实身份后重置。没有这条兜底,加了确认就是
// 「丢手机 = 账户永久失联」。
func TestAdminResetMFA(t *testing.T) {
	p, store := newOpenPlatform(t)
	user := newTestUser(t, store, "商户丢手机")
	if err := store.SetMFA(user.UserID, MFATypeTOTP, "", true); err != nil {
		t.Fatal(err)
	}
	secret, err := generateTOTPSecret()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetMFATOTPSecret(user.UserID, secret); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMFATOTPPendingSecret(user.UserID, secret); err != nil {
		t.Fatal(err)
	}

	// 没有管理员令牌 → 拒绝。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/merchants/"+user.UserID+"/mfa/reset", nil)
	p.handleAdminResetMFA(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无管理员令牌应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 有管理员令牌 → 重置。
	p.opts.AdminToken = "admin-secret"
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/admin/merchants/"+user.UserID+"/mfa/reset", nil)
	req.Header.Set("X-Admin-Token", "admin-secret")
	p.handleAdminResetMFA(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("重置二次验证应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	u, _ := store.UserByID(user.UserID)
	if u.MFAEnabled {
		t.Fatal("重置后应当不再有二次验证")
	}
	if store.MFATOTPSecret(user.UserID) != "" {
		t.Fatal("重置后必须清掉生效的 TOTP 密钥")
	}
	if store.MFATOTPPendingSecret(user.UserID) != "" {
		t.Fatal("重置后必须清掉待确认的 TOTP 密钥")
	}
}
