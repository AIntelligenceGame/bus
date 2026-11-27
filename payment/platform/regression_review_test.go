package platform

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// 这组测试是「平台代码复查」那一轮的回归用例,每条对应一个被修掉的缺陷。
// 放在一个文件里,方便一眼看全这轮修了什么。

// TestRefundWithNullStatusCountsAsRefunded refund_status 为 NULL 的老数据
// 必须同时被「超额退款保护」和「报表」当成已退。
//
// 背景:refund_status 是后加的列,AutoMigrate 给老行填的是 NULL 而不是空串。
// 口径不一致的话,商户看着报表说「还能再退 100 元」,真去退的时候被平台拦掉 ——
// 钱和账对不上,还得反过来查平台是不是有 bug。
func TestRefundWithNullStatusCountsAsRefunded(t *testing.T) {
	p, store, user, _ := newRefundTestPlatform(t, "NULL-1", 10000)

	// 手工插一行 refund_status 为 NULL 的退款(模拟老库数据)。
	now := formatTime(time.Now())
	if err := store.db.Exec(
		"INSERT INTO pay_refunds (user_id, out_trade_no, out_refund_no, amount, refund_status, created_at, updated_at) "+
			"VALUES (?, ?, ?, ?, NULL, ?, ?)",
		user.UserID, "NULL-1", "R-NULL", 6000, now, now,
	).Error; err != nil {
		t.Fatalf("插入 NULL 状态的退款失败: %v", err)
	}

	// 1) 报表口径:已退金额必须算上这 6000。
	amt, err := store.RefundedAmount(user.UserID, "NULL-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if amt != 6000 {
		t.Fatalf("NULL 状态的退款没被计入已退金额: %d", amt)
	}

	// 2) 超额退款保护:再退 5000(合计 11000 > 10000)必须被拦下。
	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "NULL-1",
		"out_refund_no": "R-2",
		"refund_amount": 5000,
	})
	if rec.Code == http.StatusOK {
		t.Fatalf("超额退款应该被拦下,实际放行了: %s", rec.Body.String())
	}
}

// TestSaveChannelRejectsInvalidEnabledConfig 启用状态下保存一份**通不过校验**的
// 渠道配置,不能把 enabled=true 的行写进库。
//
// 以前是「先存再构建」:构建失败时接口回 ok:false,前端提示「保存失败」,
// 可库里那一行已经写好了、enabled 还是 true —— 前后端说的不是一回事。
func TestSaveChannelRejectsInvalidEnabledConfig(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户校验", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)
	user, _ := p.store.UserByName("商户校验")
	vt := verifyTokenFor(t, p, token, "supersecret")

	// 微信配置里 api_v3_key 必须是 32 字节,这里故意写错。
	code, resp = saveChannelViaAPI(t, p, token, vt, "wechat", map[string]any{
		"enabled": true, "is_default": false,
		"wechat": map[string]any{
			"app_id": "wx123", "mch_id": "1900000109", "serial_no": "SERIAL",
			"private_key_pem": "-----BEGIN PRIVATE KEY-----REAL-----END PRIVATE KEY-----",
			"api_v3_key":      "TOO-SHORT",
		},
	})
	if code != http.StatusOK {
		t.Fatalf("接口应返回 200(带 ok:false),实际 %d: %v", code, resp)
	}
	if resp["ok"] != false {
		t.Fatalf("配置没过校验就该拒绝保存,实际返回: %v", resp)
	}

	// 库里不能有 enabled=true 的微信配置。
	cfgs, err := p.effectiveChannelsFor(user.UserID, "")
	if err != nil {
		t.Fatalf("effectiveChannelsFor: %v", err)
	}
	for _, c := range cfgs {
		if c.Channel == "wechat" && c.Enabled {
			t.Fatal("没通过校验的配置不该以 enabled=true 落库")
		}
	}
}

// TestSaveChannelAllowsDisabledInvalidConfig 同一份配置改成「停用」应该能存下来。
//
// 停用状态不构建渠道,配置对不对无所谓 —— 商户可能正想先把配置填进去,
// 校验通过后再启用。上一条测试不能把这个正常用法一起堵死。
func TestSaveChannelAllowsDisabledInvalidConfig(t *testing.T) {
	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户停用", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)
	user, _ := p.store.UserByName("商户停用")
	vt := verifyTokenFor(t, p, token, "supersecret")

	code, resp = saveChannelViaAPI(t, p, token, vt, "wechat", map[string]any{
		"enabled": false, "is_default": false,
		"wechat": map[string]any{
			"app_id": "wx123", "mch_id": "1900000109", "serial_no": "SERIAL",
			"private_key_pem": "-----BEGIN PRIVATE KEY-----REAL-----END PRIVATE KEY-----",
			"api_v3_key":      "TOO-SHORT",
		},
	})
	if code != http.StatusOK || resp["ok"] != true {
		t.Fatalf("停用状态下应该能保存: %d %v", code, resp)
	}
	cfgs, err := p.effectiveChannelsFor(user.UserID, "")
	if err != nil {
		t.Fatalf("effectiveChannelsFor: %v", err)
	}
	found := false
	for _, c := range cfgs {
		if c.Channel == "wechat" {
			found = true
			if c.Enabled {
				t.Fatal("保存的应该是停用状态")
			}
		}
	}
	if !found {
		t.Fatal("配置没保存下来")
	}
}

// TestSPAPathsArePublic 控制台的各个页面路径必须能直接访问(不撞鉴权中间件)。
//
// 现象:用路径访问某个页面就 401,走 #/xxx 哈希路由又正常 ——
// 因为路由注册和免登录白名单是两处各写一份,加页面时漏改白名单。
func TestSPAPathsArePublic(t *testing.T) {
	if !spaAvailable() {
		t.Skip("前端尚未构建,跳过 SPA 路径测试")
	}
	p, _ := newTrafficPlatform(t)
	for _, path := range spaPaths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		p.Handler().ServeHTTP(rec, req)
		if rec.Code == http.StatusUnauthorized {
			t.Fatalf("页面路径 %s 不该要求登录,实际 401", path)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("页面路径 %s 返回状态码不对: %d", path, rec.Code)
		}
	}
}

// TestNonceReplayRejected 同一个随机串在时间窗口内只能用一次。
func TestNonceReplayRejected(t *testing.T) {
	c := &nonceCache{}
	if !c.allow("k:n1", 200*time.Millisecond) {
		t.Fatal("第一次用应该放行")
	}
	if c.allow("k:n1", 200*time.Millisecond) {
		t.Fatal("窗口内的重复随机串必须被拒")
	}
	// 换一个随机串 -> 放行。
	if !c.allow("k:n2", 200*time.Millisecond) {
		t.Fatal("不同随机串应该放行")
	}
}

// TestNonceAllowedAfterWindowExpires 窗口外的随机串可以再用。
//
// 超过时间窗口之后,同一个随机串再次出现不算重放(否则客户端重试会被永久拒)。
func TestNonceAllowedAfterWindowExpires(t *testing.T) {
	c := &nonceCache{}
	if !c.allow("k:n1", 30*time.Millisecond) {
		t.Fatal("第一次用应该放行")
	}
	if c.allow("k:n1", 30*time.Millisecond) {
		t.Fatal("窗口内的重复随机串必须被拒")
	}
	time.Sleep(80 * time.Millisecond)
	if !c.allow("k:n1", 30*time.Millisecond) {
		t.Fatal("窗口过期后同一个随机串应该能再用")
	}
}

// TestNonceCacheIsBounded 缓存必须有上限:随机串是**调用方给的**,理论上无限多。
//
// 没有上限的话,攻击者拿着合法密钥疯狂换随机串,内存就被撑爆了。
func TestNonceCacheIsBounded(t *testing.T) {
	c := &nonceCache{}
	for i := 0; i < nonceCacheMaxEntries+1000; i++ {
		c.allow("k:"+strconv.Itoa(i), time.Minute)
	}
	if got := c.seen.Len(); got > nonceCacheMaxEntries {
		t.Fatalf("随机串缓存超过了上限: %d > %d", got, nonceCacheMaxEntries)
	}
}
