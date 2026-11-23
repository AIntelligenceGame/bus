package platform

import (
	"net/http"
	"sync"
	"time"
)

// 简单的固定窗口限流器(进程内内存态)。
//
// 用途:挡住「暴力试口令 / 暴力试验证码 / 疯狂下发验证码」这三类请求。
// 它不是分布式限流:多实例部署时每个实例各算各的,总配额会被放大。
// 需要全局配额时,请在网关层(Nginx / 网关 / Redis)再做一层。

// 限流阈值(窗口都是 1 分钟)。
const (
	// loginRateLimitPerIP 同一 IP 每分钟最多登录次数。
	loginRateLimitPerIP = 10
	// loginRateLimitPerAccount 同一账户每分钟最多登录次数。
	loginRateLimitPerAccount = 5
	// mfaSendRateLimitPerUser 同一用户每分钟最多下发验证码次数。
	mfaSendRateLimitPerUser = 3
	// mfaSendRateLimitPerIP 同一 IP 每分钟最多下发验证码次数。
	mfaSendRateLimitPerIP = 10
	// mfaVerifyRateLimitPerUser 同一用户每分钟最多校验验证码次数。
	mfaVerifyRateLimitPerUser = 20
	// loginCodeRateLimitPerIP 同一 IP 每分钟最多请求登录验证码次数。
	loginCodeRateLimitPerIP = 5
	// loginCodeRateLimitPerAccount 同一账户每分钟最多请求登录验证码次数。
	loginCodeRateLimitPerAccount = 3
	// exportRateLimitPerUser 同一商户每分钟最多导出次数。
	//
	// 导出一次要扫几千行订单、拼几 MB 的 CSV,是平台里最贵的读操作。
	exportRateLimitPerUser = 5
	// qrcodeRateLimitPerIP 同一 IP 每分钟最多请求二维码图片次数。
	//
	// 渲染一张二维码要编码 + 光栅化(5~35 ms CPU),不限流就能被用来打满 CPU。
	qrcodeRateLimitPerIP = 60
)

// writeRateLimited 返回 429,并带上 Retry-After(秒)。
//
// 带上 Retry-After 是标准做法:客户端(以及网关)据此知道什么时候可以重试,
// 而不是自己猜一个间隔反复重试,把限流变成压测。
func writeRateLimited(w http.ResponseWriter, message string) {
	w.Header().Set("Retry-After", "60")
	writeError(w, http.StatusTooManyRequests, message)
}

// rateLimiter 固定窗口限流器。
type rateLimiter struct {
	mu   sync.Mutex
	hits map[string]*rateWindow
	// lastSweep 上次清理过期条目的时间。
	lastSweep time.Time
}

// rateWindow 一个窗口的计数。
type rateWindow struct {
	count  int
	reset  time.Time
	lastAt time.Time
}

// newRateLimiter 创建限流器。
func newRateLimiter() *rateLimiter {
	return &rateLimiter{
		hits:      make(map[string]*rateWindow),
		lastSweep: time.Now(),
	}
}

// allow 判断是否放行,并累加一次计数。
//
// key 一般是「IP」或「用户 ID + 用途」;limit 是窗口内允许的最大次数。
// 超限返回 false,并在窗口结束后自动恢复。
func (l *rateLimiter) allow(key string, limit int, window time.Duration) bool {
	if l == nil || key == "" || limit <= 0 || window <= 0 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	// 顺带清理过期条目,避免长期运行内存增长。
	if now.Sub(l.lastSweep) > time.Minute {
		for k, w := range l.hits {
			if now.After(w.reset) {
				delete(l.hits, k)
			}
		}
		l.lastSweep = now
	}

	w, ok := l.hits[key]
	if !ok || now.After(w.reset) {
		l.hits[key] = &rateWindow{count: 1, reset: now.Add(window), lastAt: now}
		return true
	}
	w.lastAt = now
	if w.count >= limit {
		return false
	}
	w.count++
	return true
}

// reset 清掉某个 key 的计数(登录成功等场景用)。
func (l *rateLimiter) reset(key string) {
	if l == nil || key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.hits, key)
}

// retryAfter 返回该 key 距离可以再次尝试还有多久(用于提示信息)。
func (l *rateLimiter) retryAfter(key string) time.Duration {
	if l == nil || key == "" {
		return 0
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w, ok := l.hits[key]
	if !ok {
		return 0
	}
	if now.After(w.reset) {
		return 0
	}
	return w.reset.Sub(now)
}
