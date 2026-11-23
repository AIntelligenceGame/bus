package platform

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// 账户安全:口令散列、二次验证(邮箱/短信验证码)、敏感操作票据、审计日志。
//
// 口令散列用 PBKDF2-HMAC-SHA256(600000 次迭代,OWASP 推荐值),只存盐+散列。
// 二次验证一次性验证码用 6 位数字,数据库里只存散列 + 有效期 + 使用标记。

const (
	// pbkdf2Iterations 口令散列迭代次数。
	pbkdf2Iterations = 600000
	// pbkdf2KeyLen 派生密钥长度(字节)。
	pbkdf2KeyLen = 32
	// mfaCodeTTL 验证码有效期。
	mfaCodeTTL = 5 * 60 // 秒
	// defaultVerifyTTL 敏感操作票据有效期。
	//
	// 注意:这里是 time.Duration,必须写 10 * time.Minute。
	// 写成 10 * 60 会被当成 600 纳秒,票据签发瞬间就过期,
	// 前端却会缓存 10 分钟,表现为「验证票据无效或已过期」且不再弹验证框。
	defaultVerifyTTL = 10 * time.Minute
	// maxMFAAttempts 单个验证码允许的错误尝试次数,超过就作废重来。
	//
	// 6 位数字只有 100 万种组合,不限次数的话 5 分钟内可以试遍。
	maxMFAAttempts = 5
	// maxLoginFailures 连续登录失败多少次后锁定账户。
	maxLoginFailures = 5
	// loginLockDuration 账户锁定时长。
	loginLockDuration = 15 * 60 // 秒
	// defaultSessionTTL 登录会话默认有效期(24 小时)。
	defaultSessionTTL = 24 * time.Hour
	// defaultCertDir 平台托管的证书缓存根目录。
	defaultCertDir = "./payment-data/certs"
)

// 二次验证类型。
const (
	// MFATypeEmail 邮箱验证码。
	MFATypeEmail = "email"
	// MFATypeSMS 短信验证码。
	MFATypeSMS = "sms"
	// MFATypeTOTP 验证器 App(基于时间的动态口令)。
	//
	// 与邮箱/短信的区别:不需要任何下发通道,码由用户的验证器 App 本地算出,
	// 通道被劫持也拿不到码 —— 这是三种方式里最安全的。
	MFATypeTOTP = "totp"
)

// secretFieldNames 需要加密存储、且**不允许原样返回给浏览器**的字段名。
//
// 这些字段在任何读取接口里都会被清空,页面只能看到「已保存」标记;
// 明文只在服务端内存中短暂存在,或经二次验证后按需单独取回。
var secretFieldNames = map[string]bool{
	"private_key_pem":       true,
	"api_v3_key":            true,
	"stripe_secret_key":     true,
	"stripe_webhook_secret": true,
	"webhook_secret":        true,
	"password":              true,
	"password_hash":         true,
}

// hashPassword 生成口令散列。
//
// 返回值格式:pbkdf2-sha256$<iterations>$<base64 salt>$<base64 hash>
func hashPassword(password string) (string, error) {
	if password == "" {
		return "", fmt.Errorf("platform: 口令不能为空")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("platform: 生成盐失败: %w", err)
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, pbkdf2KeyLen)
	if err != nil {
		return "", fmt.Errorf("platform: 派生密钥失败: %w", err)
	}
	return strings.Join([]string{
		"pbkdf2-sha256",
		strconv.Itoa(pbkdf2Iterations),
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(key),
	}, "$"), nil
}

// verifyPassword 校验口令。
//
// stored 为空(用户还没设置口令)时返回 false。
func verifyPassword(stored, password string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" {
		return false
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter <= 0 {
		return false
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	want, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, password, salt, iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// generateMFACode 生成 6 位数字验证码。
func generateMFACode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", fmt.Errorf("platform: 生成验证码失败: %w", err)
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

// maskSecrets 清空配置中的敏感字段(返回副本,不修改入参)。
//
// 用于「读取渠道配置」接口:页面只能知道「密钥已保存」,拿不到明文。
func maskSecrets(cfg map[string]any) map[string]any {
	out, _ := maskSecretsIn(cfg).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

// maskSecretsIn 递归清空敏感字段。
func maskSecretsIn(node any) any {
	switch v := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			if secretFieldNames[key] {
				// 用固定掩码占位,让前端知道「这里有值」
				if s, ok := val.(string); ok && s != "" {
					out[key] = secretMask
				} else {
					out[key] = ""
				}
				continue
			}
			out[key] = maskSecretsIn(val)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = maskSecretsIn(item)
		}
		return out
	default:
		return node
	}
}

// secretMask 敏感字段的掩码占位符。
const secretMask = "********"

// mergeSecrets 合并「新提交的配置」与「已保存的配置」。
//
// 规则:敏感字段提交为空(前端拿到的是掩码,不会回传明文)时,保留已保存的值。
// 非敏感字段一律以新提交为准。
//
// **必须递归**:渠道配置是嵌套结构(wechat / alipay / wallet),
// 密钥字段(private_key_pem、api_v3_key…)都在嵌套里面。
// 只合并顶层的话,用户每保存一次配置就把密钥擦掉一次 ——
// 页面还提示「配置已保存(密钥已加密入库)」,支付却已经收不了钱。
func mergeSecrets(existing, incoming map[string]any) map[string]any {
	if existing == nil {
		existing = map[string]any{}
	}
	merged := make(map[string]any, len(incoming))
	for key, val := range incoming {
		merged[key] = val
	}
	for key, oldVal := range existing {
		cur := merged[key]
		// 嵌套配置(wechat / alipay / wallet):递归合并。
		if oldMap, ok := oldVal.(map[string]any); ok {
			if curMap, ok2 := cur.(map[string]any); ok2 {
				merged[key] = mergeSecrets(oldMap, curMap)
				continue
			}
			// 新提交没有这一段(或不是对象):整段保留旧值。
			merged[key] = oldVal
			continue
		}
		if !secretFieldNames[key] {
			continue
		}
		curStr, ok := cur.(string)
		// 掩码占位符等于「没填」:不能让 "********" 被当成真值存进库。
		if ok && curStr != "" && curStr != secretMask {
			continue // 新提交有值,用新值
		}
		if s, ok := oldVal.(string); ok && s != "" {
			merged[key] = s // 新提交为空,保留旧值
		}
	}
	// 新增的嵌套配置也要合并(例如 wallet 段)
	for key, val := range existing {
		if _, ok := merged[key]; ok {
			continue
		}
		merged[key] = val
	}
	return merged
}

// configToMap 把结构化配置转成 map。
func configToMap(cfg any) (map[string]any, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("platform: 序列化配置失败: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("platform: 解析配置失败: %w", err)
	}
	return out, nil
}

// configFromMap 把 map 转回结构化配置。
func configFromMap(m map[string]any, target any) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("platform: 序列化配置失败: %w", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("platform: 解析配置失败: %w", err)
	}
	return nil
}
