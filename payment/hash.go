package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

// SHA256Hex 返回数据的 SHA256 十六进制摘要(通用摘要工具)。
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// SHA256Base64 返回数据的 SHA256 Base64 摘要。
func SHA256Base64(data []byte) string {
	sum := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// HMACSHA256 返回 HMAC-SHA256 原始字节。
func HMACSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// HMACSHA256Hex 返回 HMAC-SHA256 的十六进制摘要。
func HMACSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(HMACSHA256(key, data))
}

// HMACSHA256Base64 返回 HMAC-SHA256 的 Base64 摘要。
func HMACSHA256Base64(key, data []byte) string {
	return base64.StdEncoding.EncodeToString(HMACSHA256(key, data))
}

// HMACEqual 常量时间比较两个摘要,避免时序攻击。
func HMACEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
