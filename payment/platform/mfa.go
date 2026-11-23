package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// 二次验证验证码的下发通道。
//
// 平台不内置任何短信/邮件厂商 SDK(避免绑死供应商、也避免多一堆依赖),
// 由使用方实现 MFASender 注入即可;默认实现把验证码打到日志(仅适合开发/内网)。
type MFASender interface {
	// Send 下发验证码到 target(邮箱地址或手机号)。
	Send(ctx context.Context, mfaType, target, code string) error
}

// LogMFASender 默认实现:把验证码写入日志。
//
// 生产环境请替换成真正的短信/邮件网关(见 HTTPMFASender)。
type LogMFASender struct {
	// Logger 日志,默认标准错误。
	Logger *log.Logger
}

// Send 写日志。
func (s LogMFASender) Send(_ context.Context, mfaType, target, code string) error {
	logger := s.Logger
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("[MFA] type=%s target=%s code=%s", mfaType, maskTarget(target), code)
	return nil
}

// HTTPMFASender 通过自建网关下发验证码(对接企业内部的短信/邮件服务)。
//
// 网关需接收 JSON:{"type":"sms|email","target":"...","code":"123456"}
// 并返回 2xx。
type HTTPMFASender struct {
	// URL 网关地址。
	URL string
	// Token 访问令牌(可选),作为 Authorization 头发送。
	Token string
	// Client HTTP 客户端,默认 10 秒超时。
	Client *http.Client
}

// Send 调用网关。
func (s HTTPMFASender) Send(ctx context.Context, mfaType, target, code string) error {
	if s.URL == "" {
		return fmt.Errorf("platform: MFA 网关地址为空")
	}
	body, err := json.Marshal(map[string]string{
		"type":   mfaType,
		"target": target,
		"code":   code,
	})
	if err != nil {
		return fmt.Errorf("platform: 序列化验证码请求失败: %w", err)
	}
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("platform: 构造验证码请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("platform: 调用验证码网关失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("platform: 验证码网关返回状态码 %d", resp.StatusCode)
	}
	return nil
}

// maskTarget 日志里对手机号/邮箱做脱敏。
func maskTarget(target string) string {
	runes := []rune(target)
	if len(runes) <= 4 {
		return "****"
	}
	keep := len(runes) / 4
	return string(runes[:keep]) + "****" + string(runes[len(runes)-keep:])
}
