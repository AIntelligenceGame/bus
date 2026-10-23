// Package config 读取 sqlextd 的运行配置。
//
// 全部走环境变量，方便容器化部署。
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是服务端配置。
type Config struct {
	// Addr 是监听地址，如 ":8080"。
	Addr string
	// BaseURL 是对外可访问的地址，用于充值二维码里回填订单号。
	BaseURL string
	// TokenTTL 是登录态有效期。
	TokenTTL time.Duration
	// CostPerParse 是每次解析扣多少积分。
	CostPerParse int64
	// SignupEnabled 为 false 时关闭注册（防止机器人注册白嫖）。
	SignupEnabled bool
	// CORSOrigins 允许跨域访问的来源（扩展页面用 chrome-extension:// 源，
	// 但本地调试时需要放开）。
	CORSOrigins []string
	// FreeQuota 是新用户赠送的初始积分。
	FreeQuota int64
	// RateLimitPerMin 是每个用户每分钟最多解析多少次。
	RateLimitPerMin int
}

// Load 从环境变量装载配置。
func Load() Config {
	return Config{
		Addr:            env("SQLEXT_ADDR", ":8080"),
		BaseURL:         env("SQLEXT_BASE_URL", "http://127.0.0.1:8080"),
		TokenTTL:        time.Duration(envInt("SQLEXT_TOKEN_TTL_HOURS", 24*30)) * time.Hour,
		CostPerParse:    int64(envInt("SQLEXT_COST_PER_PARSE", 1)),
		SignupEnabled:   envBool("SQLEXT_SIGNUP_ENABLED", true),
		CORSOrigins:     envList("SQLEXT_CORS_ORIGINS"),
		FreeQuota:       int64(envInt("SQLEXT_FREE_QUOTA", 20)),
		RateLimitPerMin: envInt("SQLEXT_RATE_LIMIT_PER_MIN", 60),
	}
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envList(key string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
