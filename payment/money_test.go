package payment

import (
	"math"
	"testing"
)

func TestYuanToCents(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"1", 100, true},
		{"12.3", 1230, true},
		{"12.34", 1234, true},
		{"12.345", 0, false},
		{"", 0, false},
		{"abc", 0, false},
		{"-1.50", -150, true},
	}
	for _, c := range cases {
		got, err := YuanToCents(c.in)
		if c.ok && err != nil {
			t.Fatalf("YuanToCents(%q) unexpected error: %v", c.in, err)
		}
		if !c.ok {
			if err == nil {
				t.Fatalf("YuanToCents(%q) expected error", c.in)
			}
			continue
		}
		if got != c.want {
			t.Fatalf("YuanToCents(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestCentsToYuan(t *testing.T) {
	if got := CentsToYuan(1234); got != "12.34" {
		t.Fatalf("CentsToYuan(1234) = %q, want 12.34", got)
	}
	if got := CentsToYuan(5); got != "0.05" {
		t.Fatalf("CentsToYuan(5) = %q, want 0.05", got)
	}
}

// TestCentsToYuanExtremes 边界:CentsToYuan 取绝对值时不能溢出。
func TestCentsToYuanExtremes(t *testing.T) {
	// math.MinInt64 取负会溢出,必须照常显示为负数而不是乱码。
	got := CentsToYuan(math.MinInt64)
	if want := "-92233720368547758.08"; got != want {
		t.Fatalf("CentsToYuan(MinInt64) = %q, want %q", got, want)
	}
	if got := CentsToYuan(math.MaxInt64); got != "92233720368547758.07" {
		t.Fatalf("CentsToYuan(MaxInt64) = %q, want 92233720368547758.07", got)
	}
	if got := CentsToYuan(math.MinInt64 + 1); got != "-92233720368547758.07" {
		t.Fatalf("CentsToYuan(MinInt64+1) = %q", got)
	}
}

// TestYuanToCentsOverflow 超大金额必须报错,不能静默回绕成另一个金额。
func TestYuanToCentsOverflow(t *testing.T) {
	for _, s := range []string{
		"92233720368547759.00",  // 乘 100 溢出
		"92233720368547758.08",  // 分位溢出
		"99999999999999999999",  // int64 之外
		"-92233720368547758.08", // 负数同样要判界
	} {
		if _, err := YuanToCents(s); err == nil {
			t.Fatalf("YuanToCents(%q) 应当报错(溢出),却返回成功", s)
		}
	}
	// 最大可表示金额仍要能正常转换。
	if v, err := YuanToCents("92233720368547758.07"); err != nil || v != math.MaxInt64 {
		t.Fatalf("YuanToCents(最大金额) = %d, err=%v", v, err)
	}
}
