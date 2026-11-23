package platform

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

// 令牌与验证码的带密钥散列。
//
// 为什么不能用普通 SHA-256:
//   - 访问令牌本身是高熵随机串,SHA-256 够用,但只要库被拖走,
//     「能不能反查」这件事就不该只靠熵;
//   - 一次性验证码只有 6 位数字(100 万种),SHA-256 的散列可以直接穷举,
//     拿到库就等于拿到了验证码,5 分钟内可以试遍所有组合。
//
// 所以这里统一用 HMAC-SHA256,密钥从「渠道密钥加密主密钥」派生:
// 拿到数据库而没有主密钥,就算知道算法也解不开令牌 / 验证码。
//
// 边界说明:这仍然不是「服务器被完全攻陷」的保护——主密钥必须与数据库分开存放。

// pepperLabel 派生标签:不同用途用不同标签,避免同一段密钥在不同场景下撞车。
const pepperLabel = "pay-platform/token-pepper/v1"

// tokenHasher 带密钥散列器(HMAC-SHA256)。
type tokenHasher struct {
	key []byte
}

// newTokenHasher 从加密盒派生散列密钥。
//
// box 为空时退化为固定密钥(仅限本地调试,进程重启后令牌仍然有效,
// 但拿到数据库就能离线校验令牌——生产环境务必配置主密钥)。
func newTokenHasher(box *SecretBox) *tokenHasher {
	if box == nil || len(box.key) == 0 {
		sum := sha256.Sum256([]byte(pepperLabel + "/default"))
		return &tokenHasher{key: sum[:]}
	}
	mac := hmac.New(sha256.New, box.key)
	mac.Write([]byte(pepperLabel))
	return &tokenHasher{key: mac.Sum(nil)}
}

// sum 返回 token 的带密钥散列(base64url,43 字符)。
func (h *tokenHasher) sum(token string) string {
	if h == nil || len(h.key) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, h.key)
	mac.Write([]byte(token))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
