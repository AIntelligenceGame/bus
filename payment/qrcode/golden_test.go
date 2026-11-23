package qrcode

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// golden 用例:固定内容 → 固定矩阵。
//
// 这些向量不是「拿自己跟自己对」——生成之后用一份**独立实现**
// 按 ISO/IEC 18004 逐模块校验过:
//   - 格式信息两份拷贝都能用 BCH(0x537,异或 0x5412)纠正回来,
//     纠错等级 M、掩码号与矩阵一致;
//   - 按标准的之字形走位顺序取出的码字,反交织分块后,
//     每块在 α^0..α^(ec-1) 处的 RS 校验因子全为 0(多项式 0x11D);
//   - 数据段解出来的模式、长度、内容与输入完全一致。
//
// 也就是说:这四个矩阵是**符合国标的合法二维码**,不是「自洽但错」的结果。
// 把它固化成回归向量,以后任何人改编码器(掩码评分、纠错码、分块、
// 版本选择)导致结果漂移,测试会立刻失败。
var golden = []struct {
	name    string
	content string
}{
	{"hello", "HELLO WORLD"},
	{"url", "https://pay.example.com/qr?token=abcdef0123456789"},
	{"utf8", "支付二维码:订单 OT202601010001"},
	{"long300", strings.Repeat("A", 300)},
}

func TestGoldenVectors(t *testing.T) {
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			code, err := Encode(g.content)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want, err := os.ReadFile(filepath.Join("testdata", "golden_"+g.name+".txt"))
			if err != nil {
				t.Fatalf("读 golden: %v", err)
			}
			// golden 文件第一行是头部(version=… size=…),后面才是矩阵。
			_, body, ok := strings.Cut(string(want), "\n")
			if !ok {
				t.Fatalf("golden 文件格式不对")
			}
			if got := matrixText(code.Matrix()); got != body {
				t.Fatalf("矩阵与 golden 不一致\n--- 期望 ---\n%s\n--- 实际 ---\n%s", body, got)
			}
		})
	}
}

// TestGoldenHeaderVersion 顺带校验 golden 文件头里记的版本号/尺寸和实际一致。
func TestGoldenHeaderVersion(t *testing.T) {
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			code, err := Encode(g.content)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			raw, err := os.ReadFile(filepath.Join("testdata", "golden_"+g.name+".txt"))
			if err != nil {
				t.Fatalf("读 golden: %v", err)
			}
			lines := strings.SplitN(string(raw), "\n", 2)
			want := "version=" + strconv.Itoa(code.Version) + " size=" + strconv.Itoa(code.Size)
			if lines[0] != want {
				t.Fatalf("golden 头 = %q, 实际 %q", lines[0], want)
			}
		})
	}
}

func matrixText(m [][]bool) string {
	var sb strings.Builder
	for _, row := range m {
		for _, v := range row {
			if v {
				sb.WriteByte('1')
			} else {
				sb.WriteByte('0')
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}
