// Package auth 提供口令散列与登录令牌。
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

// ErrInvalidCredentials 表示口令不对。刻意不区分"用户不存在"和"口令错"，
// 避免通过响应时间/错误信息枚举出注册过的邮箱。
var ErrInvalidCredentials = errors.New("auth: invalid credentials")

// HashPassword 生成口令散列。
func HashPassword(password string) (string, error) {
	if len(password) < 8 {
		return "", errors.New("auth: password too short (min 8)")
	}
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// VerifyPassword 校验口令。
func VerifyPassword(hash, password string) error {
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return ErrInvalidCredentials
	}
	return nil
}

// NewToken 生成一个新的登录令牌（32 字节随机，hex 编码）。
//
// 用随机串而不是 JWT：令牌由服务端存储，随时可吊销——
// 插件是长期装在用户浏览器里的，可吊销性比无状态更重要。
func NewToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
