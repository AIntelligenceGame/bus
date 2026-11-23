// Package qrcode 提供纯标准库实现的二维码编码与渲染。
//
// 特性:
//   - 字节模式(UTF-8 内容直接编码),纠错等级 M;
//   - 版本 1~20 自动选择(字节模式最大 666 字节),足以容纳支付二维码链接;
//   - 自动选择掩码(按 ISO/IEC 18004 惩罚分规则);
//   - 支持 PNG(位图)与 SVG(矢量)两种渲染。
//
// 用法:
//
//	code, err := qrcode.Encode("https://pay.example.com/qr?token=xxx")
//	png, err := code.PNG(8)   // 每个模块 8 像素
//	svg, err := code.SVG(8)
package qrcode

import (
	"errors"
	"fmt"
)

// ErrContentTooLong 内容超出当前支持的版本容量。
var ErrContentTooLong = errors.New("qrcode: content too long")

// 各版本的总码字数(纠错等级无关)。
var totalCodewords = [20]int{
	26, 44, 70, 100, 134, 172, 196, 242, 292, 346,
	404, 466, 532, 581, 655, 733, 815, 901, 991, 1085,
}

// 各版本的数据码字数(纠错等级 M)。
var dataCodewordsM = [20]int{
	16, 28, 44, 64, 86, 108, 124, 154, 182, 216,
	254, 290, 334, 365, 415, 453, 507, 563, 627, 669,
}

// 各版本每块的纠错码字数(纠错等级 M)。
var ecCodewordsM = [20]int{
	10, 16, 26, 18, 24, 16, 18, 22, 22, 26,
	30, 22, 22, 24, 24, 28, 28, 26, 26, 26,
}

// 各版本的校正图形中心坐标。
var alignmentPositions = [20][]int{
	nil,
	{6, 18},
	{6, 22},
	{6, 26},
	{6, 30},
	{6, 34},
	{6, 22, 38},
	{6, 24, 42},
	{6, 26, 46},
	{6, 28, 50},
	{6, 30, 54},
	{6, 32, 58},
	{6, 34, 62},
	{6, 26, 46, 66},
	{6, 26, 48, 70},
	{6, 26, 50, 74},
	{6, 30, 54, 78},
	{6, 30, 56, 82},
	{6, 30, 58, 86},
	{6, 34, 62, 90},
}

// formatBitsM 纠错等级 M 的格式信息位。
const formatBitsM = 0

// Code 一张二维码。
type Code struct {
	// Version 版本号(1~20)。
	Version int
	// Size 边长模块数。
	Size int

	modules [][]bool
}

// Encode 把内容编码为二维码(字节模式 + M 级纠错)。
func Encode(content string) (*Code, error) {
	if content == "" {
		return nil, errors.New("qrcode: empty content")
	}
	data := []byte(content)
	version, err := chooseVersion(len(data))
	if err != nil {
		return nil, err
	}
	e := newEncoder(version)
	e.drawFunctionPatterns()

	codewords, err := e.buildCodewords(data)
	if err != nil {
		return nil, err
	}
	e.drawCodewords(codewords)

	best := 0
	bestPenalty := -1
	for mask := 0; mask < 8; mask++ {
		e.applyMask(mask)
		e.drawFormatBits(mask)
		p := e.penaltyScore()
		if bestPenalty < 0 || p < bestPenalty {
			bestPenalty = p
			best = mask
		}
		e.applyMask(mask) // 再异或一次即还原
	}
	e.applyMask(best)
	e.drawFormatBits(best)

	return &Code{
		Version: version,
		Size:    e.size,
		modules: e.modules,
	}, nil
}

// Matrix 返回模块矩阵,true 表示深色模块。
func (c *Code) Matrix() [][]bool {
	out := make([][]bool, len(c.modules))
	for i := range c.modules {
		out[i] = append([]bool(nil), c.modules[i]...)
	}
	return out
}

// IsDark 判断指定坐标是否为深色模块。
func (c *Code) IsDark(x, y int) bool {
	if y < 0 || y >= c.Size || x < 0 || x >= c.Size {
		return false
	}
	return c.modules[y][x]
}

// chooseVersion 选择能容纳数据的最小版本。
func chooseVersion(dataLen int) (int, error) {
	for version := 1; version <= len(totalCodewords); version++ {
		countBits := 8
		if version >= 10 {
			countBits = 16
		}
		need := 4 + countBits + 8*dataLen
		if need <= 8*dataCodewordsM[version-1] {
			return version, nil
		}
	}
	return 0, fmt.Errorf("%w: %d bytes exceeds version 20 capacity", ErrContentTooLong, dataLen)
}

// encoder 二维码编码器内部状态。
type encoder struct {
	version    int
	size       int
	modules    [][]bool
	isFunction [][]bool
}

// newEncoder 创建指定版本的编码器。
func newEncoder(version int) *encoder {
	size := 17 + 4*version
	e := &encoder{
		version:    version,
		size:       size,
		modules:    make([][]bool, size),
		isFunction: make([][]bool, size),
	}
	for y := 0; y < size; y++ {
		e.modules[y] = make([]bool, size)
		e.isFunction[y] = make([]bool, size)
	}
	return e
}

// setFunctionModule 设置功能模块。
func (e *encoder) setFunctionModule(x, y int, dark bool) {
	e.modules[y][x] = dark
	e.isFunction[y][x] = true
}

// drawFunctionPatterns 绘制定位图形、校正图形、时序图形与格式信息占位。
func (e *encoder) drawFunctionPatterns() {
	// 时序图形
	for i := 0; i < e.size; i++ {
		e.setFunctionModule(6, i, i%2 == 0)
		e.setFunctionModule(i, 6, i%2 == 0)
	}
	// 三个定位图形(含分隔符)
	e.drawFinderPattern(3, 3)
	e.drawFinderPattern(e.size-4, 3)
	e.drawFinderPattern(3, e.size-4)
	// 校正图形
	positions := alignmentPositions[e.version-1]
	if len(positions) > 0 {
		last := len(positions) - 1
		for i, cy := range positions {
			for j, cx := range positions {
				// 跳过与定位图形重叠的位置
				if (i == 0 && j == 0) || (i == 0 && j == last) || (i == last && j == 0) {
					continue
				}
				e.drawAlignmentPattern(cx, cy)
			}
		}
	}
	// 先写一次占位格式信息,保证数据区不会占用这些位置
	e.drawFormatBits(0)
	e.drawVersion()
}

// drawFinderPattern 绘制 7x7 定位图形及其分隔符,中心为 (x, y)。
func (e *encoder) drawFinderPattern(x, y int) {
	for dy := -4; dy <= 4; dy++ {
		for dx := -4; dx <= 4; dx++ {
			xx, yy := x+dx, y+dy
			if xx < 0 || xx >= e.size || yy < 0 || yy >= e.size {
				continue
			}
			dist := abs(dx)
			if abs(dy) > dist {
				dist = abs(dy)
			}
			e.setFunctionModule(xx, yy, dist != 2 && dist != 4)
		}
	}
}

// drawAlignmentPattern 绘制 5x5 校正图形,中心为 (x, y)。
func (e *encoder) drawAlignmentPattern(x, y int) {
	for dy := -2; dy <= 2; dy++ {
		for dx := -2; dx <= 2; dx++ {
			dist := abs(dx)
			if abs(dy) > dist {
				dist = abs(dy)
			}
			e.setFunctionModule(x+dx, y+dy, dist != 1)
		}
	}
}

// drawVersion 写入版本信息(版本 7 及以上)。
func (e *encoder) drawVersion() {
	if e.version < 7 {
		return
	}
	rem := e.version
	for i := 0; i < 12; i++ {
		rem = (rem << 1) ^ ((rem >> 11) * 0x1F25)
	}
	bits := e.version<<12 | rem
	for i := 0; i < 18; i++ {
		dark := (bits>>i)&1 == 1
		a := e.size - 11 + i%3
		b := i / 3
		e.setFunctionModule(a, b, dark)
		e.setFunctionModule(b, a, dark)
	}
}

// drawFormatBits 写入格式信息(纠错等级 + 掩码编号)。
func (e *encoder) drawFormatBits(mask int) {
	data := formatBitsM<<3 | mask
	rem := data
	for i := 0; i < 10; i++ {
		rem = (rem << 1) ^ ((rem >> 9) * 0x537)
	}
	bits := (data<<10 | rem) ^ 0x5412

	// 第一份
	for i := 0; i <= 5; i++ {
		e.setFunctionModule(8, i, (bits>>i)&1 == 1)
	}
	e.setFunctionModule(8, 7, (bits>>6)&1 == 1)
	e.setFunctionModule(8, 8, (bits>>7)&1 == 1)
	e.setFunctionModule(7, 8, (bits>>8)&1 == 1)
	for i := 9; i < 15; i++ {
		e.setFunctionModule(14-i, 8, (bits>>i)&1 == 1)
	}
	// 第二份
	for i := 0; i < 8; i++ {
		e.setFunctionModule(e.size-1-i, 8, (bits>>i)&1 == 1)
	}
	for i := 8; i < 15; i++ {
		e.setFunctionModule(8, e.size-15+i, (bits>>i)&1 == 1)
	}
	// 固定的深色模块
	e.setFunctionModule(8, e.size-8, true)
}

// bitBuffer 位缓冲区,用于拼接数据码字。
type bitBuffer struct {
	bits []byte
}

// appendBits 追加 n 位(bits 为高位在前)。
func (b *bitBuffer) appendBits(value int, n int) {
	for i := n - 1; i >= 0; i-- {
		b.bits = append(b.bits, byte((value>>i)&1))
	}
}

// length 返回位数。
func (b *bitBuffer) length() int { return len(b.bits) }

// toBytes 按字节对齐输出,高位在前。
func (b *bitBuffer) toBytes() []byte {
	out := make([]byte, len(b.bits)/8)
	for i := 0; i < len(b.bits); i++ {
		if b.bits[i] == 1 {
			out[i/8] |= 1 << (7 - uint(i%8))
		}
	}
	return out
}

// buildCodewords 构造数据码字与纠错码字,并做交织。
func (e *encoder) buildCodewords(data []byte) ([]byte, error) {
	dataCount := dataCodewordsM[e.version-1]
	ecCount := ecCodewordsM[e.version-1]
	total := totalCodewords[e.version-1]
	blocks := (total - dataCount) / ecCount
	if blocks <= 0 || blocks*ecCount+dataCount != total {
		return nil, fmt.Errorf("qrcode: invalid version %d table", e.version)
	}

	// 1) 比特流:模式指示符 + 字符计数 + 数据 + 结束符 + 填充
	var buf bitBuffer
	buf.appendBits(0b0100, 4) // 字节模式
	if e.version < 10 {
		buf.appendBits(len(data), 8)
	} else {
		buf.appendBits(len(data), 16)
	}
	for _, b := range data {
		buf.appendBits(int(b), 8)
	}
	capacityBits := dataCount * 8
	if buf.length() < capacityBits {
		terminate := capacityBits - buf.length()
		if terminate > 4 {
			terminate = 4
		}
		buf.appendBits(0, terminate)
	}
	for buf.length()%8 != 0 {
		buf.appendBits(0, 1)
	}
	allData := buf.toBytes()
	if len(allData) > dataCount {
		return nil, fmt.Errorf("%w: encoded data overflow", ErrContentTooLong)
	}
	// 填充字节 0xEC / 0x11 交替
	pad := []byte{0xEC, 0x11}
	base := len(allData)
	for i := base; i < dataCount; i++ {
		allData = append(allData, pad[(i-base)%2])
	}

	// 2) 分块
	shortLen := dataCount / blocks
	numLong := dataCount % blocks
	dataBlocks := make([][]byte, 0, blocks)
	offset := 0
	for i := 0; i < blocks; i++ {
		size := shortLen
		if i >= blocks-numLong {
			size = shortLen + 1
		}
		dataBlocks = append(dataBlocks, allData[offset:offset+size])
		offset += size
	}

	// 3) 计算纠错码
	divisor := computeDivisor(ecCount)
	ecBlocks := make([][]byte, 0, blocks)
	for _, block := range dataBlocks {
		ecBlocks = append(ecBlocks, computeRemainder(block, divisor))
	}

	// 4) 交织
	result := make([]byte, 0, total)
	maxLen := shortLen
	if numLong > 0 {
		maxLen = shortLen + 1
	}
	for i := 0; i < maxLen; i++ {
		for _, block := range dataBlocks {
			if i < len(block) {
				result = append(result, block[i])
			}
		}
	}
	for i := 0; i < ecCount; i++ {
		for _, block := range ecBlocks {
			result = append(result, block[i])
		}
	}
	return result, nil
}

// drawCodewords 把码字按之字形填入数据区。
func (e *encoder) drawCodewords(codewords []byte) {
	bitIndex := 0
	totalBits := len(codewords) * 8
	for right := e.size - 1; right >= 1; right -= 2 {
		if right == 6 {
			right = 5 // 跳过垂直时序图形所在列
		}
		for vert := 0; vert < e.size; vert++ {
			for j := 0; j < 2; j++ {
				x := right - j
				upward := ((right + 1) & 2) == 0
				y := vert
				if upward {
					y = e.size - 1 - vert
				}
				if e.isFunction[y][x] {
					continue
				}
				if bitIndex >= totalBits {
					continue
				}
				bit := (codewords[bitIndex/8]>>uint(7-bitIndex%8))&1 == 1
				e.modules[y][x] = bit
				bitIndex++
			}
		}
	}
}

// applyMask 对数据区应用掩码(异或操作,可重复调用以还原)。
func (e *encoder) applyMask(mask int) {
	for y := 0; y < e.size; y++ {
		for x := 0; x < e.size; x++ {
			if e.isFunction[y][x] {
				continue
			}
			if maskBit(mask, x, y) {
				e.modules[y][x] = !e.modules[y][x]
			}
		}
	}
}

// maskBit 判断坐标是否被掩码命中。
func maskBit(mask, x, y int) bool {
	switch mask {
	case 0:
		return (x+y)%2 == 0
	case 1:
		return y%2 == 0
	case 2:
		return x%3 == 0
	case 3:
		return (x+y)%3 == 0
	case 4:
		return (x/3+y/2)%2 == 0
	case 5:
		return x*y%2+x*y%3 == 0
	case 6:
		return (x*y%2+x*y%3)%2 == 0
	case 7:
		return ((x+y)%2+x*y%3)%2 == 0
	}
	return false
}

// penaltyScore 计算掩码惩罚分,越小越好。
func (e *encoder) penaltyScore() int {
	const (
		penaltyN1 = 3
		penaltyN2 = 3
		penaltyN3 = 40
		penaltyN4 = 10
	)
	score := 0

	// 规则 1: 同色连续 5 个及以上
	for y := 0; y < e.size; y++ {
		run := 1
		for x := 1; x < e.size; x++ {
			if e.modules[y][x] == e.modules[y][x-1] {
				run++
			} else {
				if run >= 5 {
					score += penaltyN1 + (run - 5)
				}
				run = 1
			}
		}
		if run >= 5 {
			score += penaltyN1 + (run - 5)
		}
	}
	for x := 0; x < e.size; x++ {
		run := 1
		for y := 1; y < e.size; y++ {
			if e.modules[y][x] == e.modules[y-1][x] {
				run++
			} else {
				if run >= 5 {
					score += penaltyN1 + (run - 5)
				}
				run = 1
			}
		}
		if run >= 5 {
			score += penaltyN1 + (run - 5)
		}
	}

	// 规则 2: 2x2 同色块
	for y := 0; y < e.size-1; y++ {
		for x := 0; x < e.size-1; x++ {
			c := e.modules[y][x]
			if c == e.modules[y][x+1] && c == e.modules[y+1][x] && c == e.modules[y+1][x+1] {
				score += penaltyN2
			}
		}
	}

	// 规则 3: 类似定位图形的 1:1:3:1:1 模式
	score += e.countFinderLikePatterns() * penaltyN3

	// 规则 4: 深色模块比例偏离 50%
	dark := 0
	for y := 0; y < e.size; y++ {
		for x := 0; x < e.size; x++ {
			if e.modules[y][x] {
				dark++
			}
		}
	}
	total := e.size * e.size
	k := (abs(dark*20-total*10)+total-1)/total - 1
	score += k * penaltyN4
	return score
}

// countFinderLikePatterns 统计行/列中与定位图形相似的模式。
func (e *encoder) countFinderLikePatterns() int {
	patterns := [][]bool{
		{true, false, true, true, true, false, true, false, false, false, false},
		{false, false, false, false, true, false, true, true, true, false, true},
	}
	count := 0
	for y := 0; y < e.size; y++ {
		for x := 0; x+11 <= e.size; x++ {
			for _, p := range patterns {
				if e.matchRow(y, x, p) {
					count++
				}
			}
		}
	}
	for x := 0; x < e.size; x++ {
		for y := 0; y+11 <= e.size; y++ {
			for _, p := range patterns {
				if e.matchColumn(x, y, p) {
					count++
				}
			}
		}
	}
	return count
}

// matchRow 判断行中是否匹配给定模式。
func (e *encoder) matchRow(y, x int, pattern []bool) bool {
	for i, want := range pattern {
		if e.modules[y][x+i] != want {
			return false
		}
	}
	return true
}

// matchColumn 判断列中是否匹配给定模式。
func (e *encoder) matchColumn(x, y int, pattern []bool) bool {
	for i, want := range pattern {
		if e.modules[y+i][x] != want {
			return false
		}
	}
	return true
}

// computeDivisor 生成 RS 生成多项式(不含最高位系数)。
func computeDivisor(degree int) []byte {
	result := make([]byte, degree)
	result[degree-1] = 1
	root := 1
	for i := 0; i < degree; i++ {
		for j := 0; j < degree; j++ {
			result[j] = byte(gfMul(int(result[j]), root))
			if j+1 < degree {
				result[j] ^= result[j+1]
			}
		}
		root = gfMul(root, 2)
	}
	return result
}

// computeRemainder 计算数据码字的 RS 纠错码。
func computeRemainder(data, divisor []byte) []byte {
	result := make([]byte, len(divisor))
	for _, b := range data {
		factor := int(b ^ result[0])
		copy(result, result[1:])
		result[len(result)-1] = 0
		for i := range result {
			result[i] ^= byte(gfMul(int(divisor[i]), factor))
		}
	}
	return result
}

// gfMul GF(256) 上的乘法,本原多项式为 0x11D。
func gfMul(x, y int) int {
	z := 0
	for i := 7; i >= 0; i-- {
		z = (z << 1) ^ ((z >> 7) * 0x11D)
		z ^= ((y >> i) & 1) * x
	}
	return z
}

// abs 取绝对值。
func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
