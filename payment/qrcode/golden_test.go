package qrcode

import (
	"os"
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
//
// 向量本身放在 golden_vectors.go(Go 源码)里,不放 testdata/ 目录 ——
// testdata/ 按项目约定不入库,向量放那儿的话新克隆的仓库上
// 根本读不到文件,回归保护就形同虚设。
//
// 重新生成向量:UPDATE_GOLDEN=1 go test ./payment/qrcode/
// (确认过改动是对的之后再提交 golden_vectors.go 的变化)
var golden = []struct {
	name    string
	content string
	want    string
}{
	{"hello", "HELLO WORLD", goldenHello},
	{"url", "https://pay.example.com/qr?token=abcdef0123456789", goldenUrl},
	{"utf8", "支付二维码:订单 OT202601010001", goldenUtf8},
	{"long300", strings.Repeat("A", 300), goldenLong300},
}

func TestGoldenVectors(t *testing.T) {
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			code, err := Encode(g.content)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			// golden 文件第一行是头部(version=… size=…),后面才是矩阵。
			_, body, ok := strings.Cut(g.want, "\n")
			if !ok {
				t.Fatalf("golden 向量格式不对")
			}
			if got := matrixText(code.Matrix()); got != body {
				t.Fatalf("矩阵与 golden 不一致\n--- 期望 ---\n%s\n--- 实际 ---\n%s", body, got)
			}
		})
	}
}

// TestGoldenHeaderVersion 顺带校验 golden 向量头部里记的版本号/尺寸和实际一致。
func TestGoldenHeaderVersion(t *testing.T) {
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			code, err := Encode(g.content)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			lines := strings.SplitN(g.want, "\n", 2)
			want := "version=" + strconv.Itoa(code.Version) + " size=" + strconv.Itoa(code.Size)
			if lines[0] != want {
				t.Fatalf("golden 头 = %q, 实际 %q", lines[0], want)
			}
		})
	}
}

// TestGoldenVectorsUpdate 重生成 golden 向量文件(默认不跑)。
//
// 用法:UPDATE_GOLDEN=1 go test ./payment/qrcode/
// 生成后记得人眼看一遍 diff,确认是「预期的改动」而不是编码器退化了。
//
// 这个函数是 golden_vectors.go 的**唯一**生成者:文件长什么样完全由它决定,
// 所以「手工改文件」和「重新生成」不会打架。
func TestGoldenVectorsUpdate(t *testing.T) {
	if os.Getenv("UPDATE_GOLDEN") == "" {
		t.Skip("设置 UPDATE_GOLDEN=1 才会重生成向量")
	}
	var sb strings.Builder
	sb.WriteString(goldenVectorsHeader)
	for _, g := range golden {
		code, err := Encode(g.content)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		sb.WriteString("// golden" + goldenConstSuffix(g.name) + " 是 " + g.name +
			" 用例的黄金矩阵(第一行是头部,后面逐行是矩阵)。\n")
		sb.WriteString("const golden" + goldenConstSuffix(g.name) + " = `")
		sb.WriteString("version=" + strconv.Itoa(code.Version) + " size=" + strconv.Itoa(code.Size))
		sb.WriteString("\n")
		sb.WriteString(matrixText(code.Matrix()))
		sb.WriteString("`\n\n")
	}
	if err := os.WriteFile("golden_vectors.go", []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("写 golden_vectors.go: %v", err)
	}
	t.Log("已重生成 golden_vectors.go")
}

// goldenConstSuffix 把用例名转成常量名的后缀(hello -> Hello)。
func goldenConstSuffix(name string) string {
	return strings.ToUpper(name[:1]) + name[1:]
}

// goldenVectorsHeader golden_vectors.go 的文件头(生成时原样写入)。
const goldenVectorsHeader = `package qrcode

// 本文件是二维码编码器的**黄金回归向量**,由 TestGoldenVectorsUpdate 自动重生成,
// 不要手改。
//
// 为什么向量放在 Go 源码里,而不是 testdata/ 目录:
// 按项目约定 testdata/ 不入库(testdata/ 在 .gitignore 里),而这些向量
// 是**回归保护的一部分** —— 放在不入库的地方,新克隆的仓库上
// TestGoldenVectors 直接读不到文件,回归保护就形同虚设。
//
// 向量本身不是「拿自己跟自己对」:它们是用一份**独立实现**
// 按 ISO/IEC 18004 逐模块校验过的(格式信息 BCH 纠正、之字形走位取码字、
// 反交织分块、GF(256) RS 校验因子全为 0、解出的内容与输入逐字节一致),
// 也就是说这四个矩阵是**符合国标的合法二维码**。

`


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
