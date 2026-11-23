package platform

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// TOTP(基于时间的动态口令,RFC 6238)——「验证器 App」二次验证。
//
// 为什么要有这一种:
//  邮箱/短信验证码都依赖「把码送到用户手上」,通道被劫持(短信被 SIM 劫持、
//  邮箱被钓鱼)时二次验证就形同虚设;TOTP 完全离线,码由用户的验证器 App
//  根据共享密钥 + 本地时间算出,平台侧不需要任何下发通道。
//
// 参数(RFC 6238 推荐值):
//   - 算法 HMAC-SHA1;
//   - 步长 30 秒;
//   - 长度 6 位;
//   - 允许 ±1 个时间片的偏差(手机时间不准 / 网络延迟时的容错)。
//
// 安全约束:
//   - 共享密钥**加密入库**(SecretBox),库里不存明文;
//   - 校验用常量时间比较;
//   - 校验失败不泄露「哪一步错了」(统一「验证码无效或已过期」)。

const (
	// totpPeriod 时间步长(秒)。
	totpPeriod = 30
	// totpDigits 动态口令位数。
	totpDigits = 6
	// totpSkew 允许的时间片偏差(前后各 1 个)。
	totpSkew = 1
	// totpIssuer otpauth URI 里的发行者名。
	totpIssuer = PlatformName
)

// totpSecretSize 共享密钥字节数(160 bit,HMAC-SHA1 推荐)。
const totpSecretSize = 20

// totpNow 取「当前时间」。
//
// 单独抽出来是因为动态码有时间片:同一个码只能用一次(RFC 6238 §5.2),
// 而一个时间片只有 30 秒 —— 测试里要连续做多次校验就得能推进时间。
// 生产代码不要改它。
var totpNow = time.Now

// generateTOTPSecret 生成一个新的 TOTP 共享密钥(base32,无填充)。
func generateTOTPSecret() (string, error) {
	buf := make([]byte, totpSecretSize)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("platform: 生成 TOTP 密钥失败: %w", err)
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf), nil
}

// decodeTOTPSecret 解析 base32 密钥。
func decodeTOTPSecret(secret string) ([]byte, error) {
	s := strings.ToUpper(strings.TrimSpace(secret))
	s = strings.ReplaceAll(s, " ", "")
	s = strings.TrimRight(s, "=")
	if s == "" {
		return nil, fmt.Errorf("platform: TOTP 密钥为空")
	}
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("platform: TOTP 密钥格式错误: %w", err)
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("platform: TOTP 密钥为空")
	}
	return key, nil
}

// totpCodeAt 计算某个时刻的动态口令。
func totpCodeAt(secret string, at time.Time) (string, error) {
	key, err := decodeTOTPSecret(secret)
	if err != nil {
		return "", err
	}
	counter := uint64(at.Unix()) / totpPeriod
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], counter)
	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 0x0f
	value := (uint32(sum[offset]&0x7f) << 24) |
		(uint32(sum[offset+1]) << 16) |
		(uint32(sum[offset+2]) << 8) |
		uint32(sum[offset+3])
	mod := uint32(1)
	for i := 0; i < totpDigits; i++ {
		mod *= 10
	}
	return fmt.Sprintf("%0*d", totpDigits, value%mod), nil
}

// verifyTOTPCode 校验动态口令(允许 ±totpSkew 个时间片)。
//
// 失败一律返回 false —— 调用方不要据此区分「密钥不对」还是「码过期」。
//
// 注意:这个函数**不**保证一次性使用。校验「生效中的密钥」时要用
// Store.ConsumeTOTPCode,否则同一个码在它的时间片内可以被重放
// (RFC 6238 §5.2 要求验证方在成功校验后不得再接受同一个 OTP)。
func verifyTOTPCode(secret, code string, at time.Time) bool {
	_, ok := verifyTOTPCodeCounter(secret, code, at)
	return ok
}

// verifyTOTPCodeCounter 校验动态口令,并返回**命中的时间片**。
//
// 时间片(counter)就是 RFC 6238 里的 T = (unix 秒 / 步长),
// 记录它就能做到「同一个码只用一次」。
func verifyTOTPCodeCounter(secret, code string, at time.Time) (uint64, bool) {
	code = strings.TrimSpace(code)
	if code == "" || len(code) != totpDigits {
		return 0, false
	}
	for skew := -totpSkew; skew <= totpSkew; skew++ {
		when := at.Add(time.Duration(skew) * totpPeriod * time.Second)
		want, err := totpCodeAt(secret, when)
		if err != nil {
			return 0, false
		}
		if subtle.ConstantTimeCompare([]byte(want), []byte(code)) == 1 {
			return uint64(when.Unix()) / totpPeriod, true
		}
	}
	return 0, false
}

// totpURI 生成 otpauth:// URI(验证器 App 扫这个二维码即可添加账号)。
func totpURI(secret, account string) string {
	label := url.PathEscape(totpIssuer + ":" + account)
	q := url.Values{}
	q.Set("secret", secret)
	q.Set("issuer", totpIssuer)
	q.Set("algorithm", "SHA1")
	q.Set("digits", fmt.Sprint(totpDigits))
	q.Set("period", fmt.Sprint(totpPeriod))
	return "otpauth://totp/" + label + "?" + q.Encode()
}
