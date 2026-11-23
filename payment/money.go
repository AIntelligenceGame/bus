package payment

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// CentsToYuan 把「分」转换成「元」字符串,例如 1234 -> "12.34"(内部金额单位为分)。
//
// 取绝对值时不能直接写 cents = -cents:math.MinInt64 取负会溢出,
// 结果仍然是 math.MinInt64,除以 100 之后会显示成一个毫无意义的巨额负数。
// 这里改成在 uint64 上取绝对值,再按符号拼回去。
func CentsToYuan(cents int64) string {
	neg := ""
	u := uint64(cents)
	if cents < 0 {
		neg = "-"
		u = uint64(-(cents + 1)) + 1 // 等价于 |cents|,且不会溢出
	}
	return fmt.Sprintf("%s%d.%02d", neg, u/100, u%100)
}

// YuanToCents 把「元」字符串转换成「分」,例如 "12.34" -> 1234。
// 使用字符串切分避免浮点误差,最多支持两位小数。
func YuanToCents(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("payment: empty amount")
	}
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	if len(fracPart) > 2 {
		return 0, fmt.Errorf("payment: amount %q has more than 2 decimals", s)
	}
	for len(fracPart) < 2 {
		fracPart += "0"
	}
	yi, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("payment: invalid amount %q: %w", s, err)
	}
	fe, err := strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("payment: invalid amount %q: %w", s, err)
	}
	// 乘 100 会溢出:ParseInt 只保证 intPart 本身不超 int64,
	// 但 92233720368547758 * 100 已经远超 int64 上限,静默回绕会得到
	// 一个看起来正常、实际完全错误的金额 —— 支付场景下这是致命的。
	// 所以先做除法判界,再乘。
	const maxCents = int64(^uint64(0) >> 1)
	if yi > maxCents/100 || (yi == maxCents/100 && fe > maxCents%100) {
		return 0, fmt.Errorf("payment: amount %q overflows int64 cents", s)
	}
	total := yi*100 + fe
	if neg {
		total = -total
	}
	return total, nil
}

// FormatCents 格式化金额用于日志展示。
func FormatCents(cents int64) string {
	return CentsToYuan(cents) + " CNY"
}
