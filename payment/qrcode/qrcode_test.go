package qrcode

import (
	"strings"
	"testing"
)

func TestEncodeStructure(t *testing.T) {
	code, err := Encode("https://pay.example.com/qr?token=abc123")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if code.Size != 17+4*code.Version {
		t.Fatalf("size = %d, version = %d", code.Size, code.Version)
	}
	// 三个定位图形的中心必须是深色
	corners := [][2]int{{3, 3}, {code.Size - 4, 3}, {3, code.Size - 4}}
	for _, c := range corners {
		if !code.IsDark(c[0], c[1]) {
			t.Fatalf("finder center (%d,%d) should be dark", c[0], c[1])
		}
	}
	// 定位图形外圈(距离中心 3)也是深色
	if !code.IsDark(0, 3) {
		t.Fatal("finder outer ring should be dark")
	}
	// 静默区之外不存在模块
	if code.IsDark(-1, 0) || code.IsDark(0, code.Size) {
		t.Fatal("out of range module should be light")
	}
}

func TestEncodeTooLong(t *testing.T) {
	if _, err := Encode(strings.Repeat("a", 1000)); err == nil {
		t.Fatal("expected error for over-long content")
	}
}

func TestRender(t *testing.T) {
	code, err := Encode("hello")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	pngBytes, err := code.PNG(4)
	if err != nil {
		t.Fatalf("png: %v", err)
	}
	if len(pngBytes) == 0 {
		t.Fatal("empty png")
	}
	svgBytes, err := code.SVG(4)
	if err != nil {
		t.Fatalf("svg: %v", err)
	}
	if !strings.Contains(string(svgBytes), "<svg") {
		t.Fatal("svg output looks wrong")
	}
}

// TestRSRemainder 校验 RS 纠错码:数据 + 纠错码 应可被生成多项式整除。
func TestRSRemainder(t *testing.T) {
	degree := 10
	divisor := computeDivisor(degree)
	data := []byte{0x10, 0x20, 0x0c, 0x56, 0x61, 0x80}
	ec := computeRemainder(data, divisor)

	combined := append(append([]byte{}, data...), ec...)
	remainder := computeRemainder(combined, divisor)
	for i, b := range remainder {
		if b != 0 {
			t.Fatalf("remainder[%d] = %d, want 0", i, b)
		}
	}
}

// TestRSGeneratorPolynomialKnownAnswer 生成多项式必须是 QR 规范里的那一个。
//
// 上面那条 TestRSRemainder 只验「数据 + 纠错码 能被生成多项式整除」,
// 两边都用的是生产实现 —— 生成多项式本身算错了(比如用了 AES 的 0x11B
// 而不是 QR 规范的 0x11D),余数照样是 0,那条测试发现不了。
//
// 这里的期望值是 QR 规范(ISO/IEC 18004)的生成多项式系数(去掉最高位的 1):
//
//	7 个纠错码字:7f 7a 9a a4 0b 44 75
//	10 个纠错码字:d8 c2 9f 6f c7 5e 5f 71 9d c1
//	13 个纠错码字:89 49 e3 11 b1 11 34 0d 2e 2b 53 84 78
func TestRSGeneratorPolynomialKnownAnswer(t *testing.T) {
	cases := []struct {
		degree int
		want   []byte
	}{
		{7, []byte{0x7f, 0x7a, 0x9a, 0xa4, 0x0b, 0x44, 0x75}},
		{10, []byte{0xd8, 0xc2, 0x9f, 0x6f, 0xc7, 0x5e, 0x5f, 0x71, 0x9d, 0xc1}},
		{13, []byte{0x89, 0x49, 0xe3, 0x11, 0xb1, 0x11, 0x34, 0x0d, 0x2e, 0x2b, 0x53, 0x84, 0x78}},
	}
	for _, c := range cases {
		got := computeDivisor(c.degree)
		if len(got) != len(c.want) {
			t.Fatalf("degree=%d 的生成多项式长度不对: 期望 %d,实际 %d", c.degree, len(c.want), len(got))
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Fatalf("degree=%d 的生成多项式第 %d 个系数不对: 期望 %#x,实际 %#x",
					c.degree, i, c.want[i], got[i])
			}
		}
	}
}

// TestGFMul 校验 GF(256) 乘法基本性质。
func TestGFMul(t *testing.T) {
	if got := gfMul(1, 123); got != 123 {
		t.Fatalf("gfMul(1,123) = %d, want 123", got)
	}
	if got := gfMul(0, 123); got != 0 {
		t.Fatalf("gfMul(0,123) = %d, want 0", got)
	}
	if got := gfMul(2, 128); got != gfMul(128, 2) {
		t.Fatal("gfMul should be commutative")
	}
	// 已知答案向量:QR 用的是 0x11D 这个本原多项式。
	//
	// 只验单位元/零元/交换律的话,多项式换成 0x11B(AES 的)也全都通过。
	if got := gfMul(0x02, 0x80); got != 0x1d {
		t.Fatalf("gfMul(0x02,0x80) = %#x, want 0x1d", got)
	}
	if got := gfMul(0x53, 0xca); got != 0x8f {
		t.Fatalf("gfMul(0x53,0xca) = %#x, want 0x8f", got)
	}
	// 0x80 * 0x80 在 0x11D 下应当折回到 0x13(0x80 溢出后要乘 0x11D)。
	if got := gfMul(0x80, 0x80); got != 0x13 {
		t.Fatalf("gfMul(0x80,0x80) = %#x, want 0x13", got)
	}
}
