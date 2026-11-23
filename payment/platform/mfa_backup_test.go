package platform

import (
	"net/http"
	"testing"
	"time"
)

// ---------------------------------------------------------------- 备用验证方式
//
// 「验证器 App 丢了 / 被误删 / 手机被偷」时的第二条腿。
//
// 没有它,账户就是永久失联:登录必须要 App 里的动态码,拿不到就登不进来,
// 而换绑又要旧 App 的动态码 —— 死锁。
//
// 所以要能**提前**绑一个备用邮箱/手机号:登录时改用它收验证码。
//
// 安全底线:备用方式只是「多一条路」,不能变成「绕过主方式的路」——
//   - 绑定备用方式必须由**当前绑定的人**确认(否则知道口令的人
//     可以给自己的邮箱绑一个备用方式,直接绕过验证器登录);
//   - 备用码只认备用用途(login_backup),不能顶替别的用途;

// bindBackupFactor 给账户绑一个备用验证方式(邮箱)。
func bindBackupFactor(t *testing.T, p *Platform, token, userID, currentCode string) {
	t.Helper()
	// 发到备用地址的绑定码。
	if err := p.store.CreateMFACode(userID, mfaPurposeBind, "654321", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp := mfaPost(t, p.handleBindMFABackup, "/api/account/mfa/backup", token, "",
		map[string]any{
			"type": "email", "target": "backup@example.com",
			"password": "supersecret", "code": "654321", "current_code": currentCode,
		})
	if code != http.StatusOK {
		t.Fatalf("绑定备用验证方式失败: %d %v", code, resp)
	}
}

// bindBackupFactorWith 给账户绑一个备用验证方式(指定绑定码,便于避开同库同码冲突)。
func bindBackupFactorWith(t *testing.T, p *Platform, token, userID, currentCode, bindCode string) {
	t.Helper()
	if err := p.store.CreateMFACode(userID, mfaPurposeBind, bindCode, time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp := mfaPost(t, p.handleBindMFABackup, "/api/account/mfa/backup", token, "",
		map[string]any{
			"type": "email", "target": "backup@example.com",
			"password": "supersecret", "code": bindCode, "current_code": currentCode,
		})
	if code != http.StatusOK {
		t.Fatalf("绑定备用验证方式失败: %d %v", code, resp)
	}
}

// TestBackupFactorLetsYouInWhenAuthenticatorIsLost 验证器没了还能登录。
//
// 这就是「不小心删了 MFA App」的解药:绑过备用方式之后,
// 登录页可以改用发到备用地址的验证码。
func TestBackupFactorLetsYouInWhenAuthenticatorIsLost(t *testing.T) {
	p, sender := newTestPlatform(t)
	code, resp := login(t, p, "商户备用", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户备用")

	// 主方式:验证器 App。
	secret := bindTOTP(t, p, token)

	// 提前绑一个备用邮箱。
	bindBackupFactor(t, p, token, user.UserID, mustTOTPCode(t, secret))
	u, _ := p.store.UserByID(user.UserID)
	if u.MFABackupType != MFATypeEmail || u.MFABackupTarget != "backup@example.com" {
		t.Fatalf("备用方式没有绑上: %+v", u)
	}

	// 登录时向备用地址要一个验证码。
	sender.codes = nil
	code, resp = mfaPost(t, p.handleSendLoginCode, "/api/login/code", "", "",
		map[string]any{"name": "商户备用", "backup": true})
	if code != http.StatusOK {
		t.Fatalf("请求备用验证码失败: %d %v", code, resp)
	}
	if len(sender.codes) != 1 {
		t.Fatalf("应当下发 1 个备用验证码,实际 %d", len(sender.codes))
	}
	backupCode := sender.codes[0]

	// App 没了(动态码给不出来),用备用验证码登录 → 成功。
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户备用", "password": "supersecret", "code": backupCode,
	})
	if code != http.StatusOK {
		t.Fatalf("用备用验证码登录应成功,实际 %d: %v", code, resp)
	}

	// 备用码不能顶替主方式之外的用途:再登一次(同一个码已作废)。
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户备用", "password": "supersecret", "code": backupCode,
	})
	if code == http.StatusOK {
		t.Fatal("备用验证码是一次性的,不该能再用")
	}
}

// TestBackupFactorRequiresCurrentHolder 绑定备用方式也要当前绑定的人确认。
//
// 否则:攻击者知道口令 → 给自己的邮箱绑一个「备用方式」→
// 用它登录 → 验证器 App 就形同虚设了。
func TestBackupFactorRequiresCurrentHolder(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户抢备用", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户抢备用")
	secret := bindTOTP(t, p, token)

	// 攻击者:只有口令,没有旧 App 的动态码。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "654321", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleBindMFABackup, "/api/account/mfa/backup", token, "",
		map[string]any{
			"type": "email", "target": "attacker@evil.com",
			"password": "supersecret", "code": "654321",
		})
	if code != http.StatusForbidden {
		t.Fatalf("没有当前绑定确认时绑定备用方式应被拒绝,实际 %d: %v", code, resp)
	}
	u, _ := p.store.UserByID(user.UserID)
	if u.MFABackupType != "" {
		t.Fatalf("被拒后不该留下备用方式: %q", u.MFABackupType)
	}

	// 正主带着旧 App 的动态码 → 允许。
	bindBackupFactorWith(t, p, token, user.UserID, mustTOTPCode(t, secret), "777777")
}

// TestBackupCodeCannotBeUsedAsPrimaryCode 备用码不能顶替主方式的码。
//
// 用途是分开的:登录备用码是 login_backup,主方式是 login。
func TestBackupCodeCannotBeUsedAsPrimaryCode(t *testing.T) {
	p, sender := newTestPlatform(t)
	code, resp := login(t, p, "商户分用途", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户分用途")

	// 主方式:邮箱;备用方式:另一个邮箱。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "111111", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleEnableMFA, "/api/account/mfa/enable", token, "",
		map[string]any{"type": "email", "target": "main@example.com", "password": "supersecret", "code": "111111"})
	if code != http.StatusOK {
		t.Fatalf("绑定主方式失败: %d %v", code, resp)
	}
	// 备用方式:主方式是邮箱,所以要发到已绑定地址的确认码。
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeRebind, "222222", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := p.store.CreateMFACode(user.UserID, mfaPurposeBind, "333333", time.Minute); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleBindMFABackup, "/api/account/mfa/backup", token, "",
		map[string]any{
			"type": "email", "target": "backup@example.com",
			"password": "supersecret", "code": "333333", "current_code": "222222",
		})
	if code != http.StatusOK {
		t.Fatalf("绑定备用方式失败: %d %v", code, resp)
	}

	// 拿备用地址的登录码:它只认「备用登录」这个用途。
	sender.codes = nil
	code, resp = mfaPost(t, p.handleSendLoginCode, "/api/login/code", "", "",
		map[string]any{"name": "商户分用途", "backup": true})
	if code != http.StatusOK {
		t.Fatalf("请求备用验证码失败: %d %v", code, resp)
	}
	if len(sender.codes) == 0 {
		t.Fatal("没有收到备用验证码")
	}
	backupCode := sender.codes[len(sender.codes)-1]
	// 备用码不能被当成敏感操作(关二次验证 / 换绑)的验证码用。
	if p.store.ConsumeMFACode(user.UserID, mfaPurposeSensitive, backupCode) {
		t.Fatal("备用登录码不能当敏感操作验证码用")
	}
	if p.store.ConsumeMFACode(user.UserID, mfaPurposeLogin, backupCode) {
		t.Fatal("备用登录码不能顶替主方式的登录验证码")
	}
	// 但登录时它就是用来登录的 → 应当能登进去。
	code, resp = loginWithBody(t, p, map[string]string{
		"name": "商户分用途", "password": "supersecret", "code": backupCode,
	})
	if code != http.StatusOK {
		t.Fatalf("用备用验证码登录应成功,实际 %d: %v", code, resp)
	}
}
