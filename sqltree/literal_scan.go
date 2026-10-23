package sqltree

import "strings"

// 本文件提供"文本级扫描"的基础设施：判断某个位置是不是处在
// 字符串字面量、标识符引号或注释里。
//
// 为什么需要它：语法补丁（syntax_fixup.go）要在**原文**里找关键字、
// 摘掉、替换。如果不区分关键字和字面量，就会把用户 SQL 里的
// 字符串内容改掉——比如 `WHERE name='zerofill'` 里的 'zerofill'
// 会被当成列修饰剥掉，条件值被静默改写。
//
// 所以：**任何在原文里按关键字做替换的地方，都必须先过一遍这里的判断。**

// literalRegions 标出原文里"不是代码"的部分。
type literalRegions struct {
	// text 是被扫描的原文。
	text string
	// mask[i] 为 true 表示 text[i] 处在字面量或注释里。
	mask []bool
}

// scanLiteralRegions 扫描原文，标出所有字面量与注释区域。
//
// 识别的区域：
//   - 单引号字符串 '...'（含 ” 与 \' 转义）
//   - 双引号字符串 "..."
//   - 反引号标识符 `...`
//   - 行注释 -- ... 到行尾
//   - 块注释 /* ... */
func scanLiteralRegions(text string) *literalRegions {
	regions := &literalRegions{text: text, mask: make([]bool, len(text))}
	i := 0
	for i < len(text) {
		c := text[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			end := skipQuoted(text, i)
			for j := i; j < end; j++ {
				regions.mask[j] = true
			}
			i = end
		case c == '-' && i+1 < len(text) && text[i+1] == '-':
			// 行注释
			end := i
			for end < len(text) && text[end] != '\n' {
				regions.mask[end] = true
				end++
			}
			i = end
		case c == '/' && i+1 < len(text) && text[i+1] == '*':
			// 块注释
			end := i + 2
			for end < len(text) {
				if text[end] == '*' && end+1 < len(text) && text[end+1] == '/' {
					end += 2
					break
				}
				end++
			}
			if end > len(text) {
				end = len(text)
			}
			for j := i; j < end; j++ {
				regions.mask[j] = true
			}
			i = end
		default:
			i++
		}
	}
	return regions
}

// inLiteral 判断位置 pos 是否处在字面量或注释里。
func (r *literalRegions) inLiteral(pos int) bool {
	if pos < 0 || pos >= len(r.mask) {
		return false
	}
	return r.mask[pos]
}

// findKeyword 在原文中查找关键字 keyword，返回它的起始位置。
//
// 只返回**不在字面量/注释里**、且前后都不是标识符字符的匹配。
// 找不到时返回 -1。
func (r *literalRegions) findKeyword(keyword string) int {
	lower := strings.ToLower(r.text)
	needle := strings.ToLower(keyword)
	from := 0
	for {
		idx := strings.Index(lower[from:], needle)
		if idx < 0 {
			return -1
		}
		idx += from
		from = idx + 1
		// 前后不能是标识符字符（避免命中 identifier 的一部分）
		if idx > 0 && isIdentChar(r.text[idx-1]) {
			continue
		}
		end := idx + len(keyword)
		if end < len(r.text) && isIdentChar(r.text[end]) {
			continue
		}
		// 不能落在字面量或注释里
		if r.inLiteral(idx) {
			continue
		}
		return idx
	}
}

// findKeywordAfter 在 pos 之后查找关键字 keyword。
func (r *literalRegions) findKeywordAfter(keyword string, pos int) int {
	saved := &literalRegions{text: r.text[pos:], mask: r.mask[pos:]}
	idx := saved.findKeyword(keyword)
	if idx < 0 {
		return -1
	}
	return pos + idx
}
