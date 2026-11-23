package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestVerifyTokenIsSingleUse 验证票据用后即焚(超出复用窗口就作废)。
//
// 票据是「口令 + 验证码」换来的:一枚能反复用 10 分钟的票据一旦泄露,
// 攻击者就能在 10 分钟内反复改配置、退款。
func TestVerifyTokenIsSingleUse(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户票据")

	token, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	// 第一次用:通过。
	owner, ok := store.VerifyToken(token)
	if !ok || owner != user.UserID {
		t.Fatalf("首次校验应通过,实际 ok=%v owner=%s", ok, owner)
	}
	// 复用窗口内:连续操作还能用(控制台「保存 → 立刻测试」这类连招)。
	if _, ok := store.VerifyToken(token); !ok {
		t.Fatal("复用窗口内应还能通过")
	}
	// 把 used_at 挪到窗口之外:再拿来用必须被拒。
	old := time.Now().Add(-2 * verifyReuseGrace).Format(time.RFC3339Nano)
	res := store.DB().Model(&VerifyTokenModel{}).
		Where("token_hash = ?", store.hasher.sum(token)).
		Update("used_at", old)
	if res.Error != nil {
		t.Fatalf("改 used_at 失败: %v", res.Error)
	}
	if _, ok := store.VerifyToken(token); ok {
		t.Fatal("超出复用窗口的票据必须作废")
	}
}

// TestVerifyTokenExpired 过期票据不能用来做敏感操作。
func TestVerifyTokenExpired(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户过期票据")
	token, err := store.CreateVerifyToken(user.UserID, -time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	if _, ok := store.VerifyToken(token); ok {
		t.Fatal("过期票据必须作废")
	}
}

// TestRequestBodySizeLimit 请求体超过上限要被拒。
//
// 不限大小的话,一个几百 MB 的请求体就能把内存吃满。
func TestRequestBodySizeLimit(t *testing.T) {
	big := strings.Repeat("x", maxRequestBody+1024)
	req := httptest.NewRequest(http.MethodPost, "/api/refund", strings.NewReader(`{"a":"`+big+`"}`))
	req.Header.Set("Content-Type", "application/json")
	var target map[string]any
	if err := decodeJSON(req, &target); err == nil {
		t.Fatal("超大的请求体必须报错")
	}
}
