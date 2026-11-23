package platform

import (
	"io"
	"log"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestDefaultVerifyTTL 防止「敏感操作票据有效期」又被写成秒数而不是 time.Duration。
//
// 回归背景:defaultVerifyTTL 曾写成 10 * 60(= 600 纳秒),
// 票据签发瞬间就过期;前端却按 expires_in=0 兜底缓存 10 分钟,
// 于是无论怎么重新验证都只得到一句「验证票据无效或已过期」。
func TestDefaultVerifyTTL(t *testing.T) {
	if defaultVerifyTTL != 10*time.Minute {
		t.Fatalf("defaultVerifyTTL 应该是 10 分钟,实际是 %v", defaultVerifyTTL)
	}
}

// TestVerifyTicketUsableWithDefaultTTL 用默认配置跑一遍:
// 登录 → 换票据 → 用票据做敏感操作。
//
// 票据必须「签发后还能用」,而且 expires_in 要跟后端一致(前端靠它决定缓存多久)。
func TestVerifyTicketUsableWithDefaultTTL(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	// 走 New() 才能拿到「默认」的 VerifyTTL;MasterKey 显式给,
	// 免得测试去写仓库里的 payment-data/master.key。
	p, err := New(Options{
		DB:        db,
		MasterKey: "test-master-key",
		Logger:    log.New(io.Discard, "", 0),
		// 测试用的通道地址指向本机,放开出站防护的内网拦截。
		OutboundAllowPrivate: true,
	})
	if err != nil {
		t.Fatalf("创建平台失败: %v", err)
	}
	if p.opts.VerifyTTL != 10*time.Minute {
		t.Fatalf("默认票据有效期应该是 10 分钟,实际是 %v", p.opts.VerifyTTL)
	}
	h := p.Handler()

	// 1) 首次登录(自动建账户)。
	code, resp := muxPost(t, h, http.MethodPost, "/api/login", "", "",
		map[string]any{"name": "票据有效期", "password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token, _ := resp["token"].(string)

	// 2) 换票据。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/verify", token, "",
		map[string]any{"password": "supersecret"})
	if code != http.StatusOK {
		t.Fatalf("身份校验失败: %d %v", code, resp)
	}
	verifyToken, _ := resp["verify_token"].(string)
	if verifyToken == "" {
		t.Fatal("没有拿到票据")
	}
	// expires_in 会告诉前端票据能放多久,不能是 0(前端会兜底成 600s,与后端对不上)。
	if ttl, _ := resp["expires_in"].(float64); ttl < 60 {
		t.Fatalf("expires_in 应该是像样的秒数(>=60),实际是 %v", resp["expires_in"])
	}

	// 3) 用票据做一次敏感操作 —— 这里就能验出「票据是不是签发即过期」。
	code, resp = muxPost(t, h, http.MethodPost, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{
			"channel": "email",
			"config":  map[string]any{"host": "smtp.example.com", "port": 587},
		})
	if code != http.StatusOK {
		t.Fatalf("带票据保存通道失败: %d %v", code, resp)
	}
}
