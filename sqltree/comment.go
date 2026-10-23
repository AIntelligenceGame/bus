package sqltree

import "strings"

// 本文件负责"注释归一化"：在进入解析器之前，把 SQL 文本里的注释统一处理掉。
//
// 为什么要在入口统一处理，而不是让每个扫描器各自小心：
//
//  1. vitess 语法层虽然能识别注释，但注释内容会原样留在 AST 节点的文本里。
//     `SELECT a FROM (SELECT /* c */ 1) s` 的 From 字段会变成
//     "(select /* c */ 1) as s"——注释漏进了输出。
//
//  2. 我们自己有一批"按关键字扫描原文"的逻辑：找 PARTITION 子句、切分
//     ALTER 动作、找 WITH ROLLUP、剥 ZEROFILL…… 只要注释里恰好出现了
//     这些关键字（`/* PARTITION BY HASH(id) */`、`-- with rollup`），
//     扫描器就会把注释当成真的结构。
//
//  3. 注释可能出现在**任何**位置：关键字中间、逗号之间、括号里、字符串外……
//     逐个扫描器打补丁不可能补全，入口统一处理才是根治。
//
// 归一化规则（与 MySQL 一致）：
//
//   - `'...'` `"..."` `...` 里的内容原样保留——那不是注释。
//   - `-- ` 行注释：MySQL 要求 "--" 后面跟空白或控制字符才算注释；
//     `a--b` 是减号运算符（a - (-b)），不能当注释。
//   - `#` 行注释：MySQL 特有。
//   - `/* ... */` 块注释：整段去掉，替换成一个空格（避免把两段代码粘在一起）。
//   - `/*! ... */` 版本注释：**MySQL 会执行里面的内容**，所以不能当注释丢掉，
//     要把内容还原成真正的 SQL（并去掉开头的 5 位版本号）。
//   - `/*+ ... */` 优化器提示：当成普通注释去掉（对解析结果没有影响）。
//
// 归一化之后，vitess 和所有自研扫描器看到的是同一份"干净"的文本，
// 输出结果里也不会带注释——和 MySQL 实际执行时的行为一致。

// stripSQLComments 去掉 SQL 文本里的注释，返回归一化后的 SQL。
func stripSQLComments(sql string) string {
	if !mayContainComment(sql) {
		return sql
	}
	var sb strings.Builder
	sb.Grow(len(sql))

	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			// 字符串/标识符引号：整段原样保留
			end := skipQuoted(sql, i)
			sb.WriteString(sql[i:end])
			i = end

		case c == '#':
			// 井号行注释（MySQL 特有）
			end := i
			for end < len(sql) && sql[end] != '\n' {
				end++
			}
			sb.WriteByte(' ')
			i = end

		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			if !isCommentDashDash(sql, i) {
				// "--" 后面不是空白：这是减号运算符，MySQL 里 a--b == a - (-b)
				sb.WriteString("- -")
				i += 2
				continue
			}
			end := i
			for end < len(sql) && sql[end] != '\n' {
				end++
			}
			sb.WriteByte(' ')
			i = end

		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			body, end := readBlockComment(sql, i)
			if body == "" {
				// 普通注释/提示：替换成空格
				sb.WriteByte(' ')
			} else {
				// 版本注释：MySQL 会执行里面的内容
				sb.WriteString(body)
			}
			i = end

		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// mayContainComment 粗判文本里有没有可能出现注释。
// 绝大多数 SQL 一个注释都没有，直接原样返回，省掉一次完整扫描。
func mayContainComment(sql string) bool {
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '#':
			return true
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				return true
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				return true
			}
		}
	}
	return false
}

// isCommentDashDash 判断 pos 处的 "--" 是不是行注释。
//
// MySQL 规则："--" 后面必须跟空白字符或控制字符才算注释，
// 否则它是减号运算符。`1--2` 在 MySQL 里是 1 - (-2) = 3。
func isCommentDashDash(sql string, pos int) bool {
	next := pos + 2
	if next >= len(sql) {
		// 结尾的 "--"：按注释处理
		return true
	}
	c := sql[next]
	return isSpace(c) || isControl(c)
}

// isControl 判断是否为控制字符。
func isControl(c byte) bool {
	return c < 0x20 || c == 0x7f
}

// readBlockComment 读取 pos 处开始的块注释。
//
// 返回注释体的内容与结束后的位置：
//   - 普通注释 `/* ... */` 与提示 `/*+ ... */`：内容返回空串。
//   - 版本注释 `/*!40101 SET NAMES utf8 */`：内容返回去掉版本号后的
//     "SET NAMES utf8 "（MySQL 会执行它）。
//   - 注释没有闭合：视为一直到结尾都是注释。
func readBlockComment(sql string, pos int) (string, int) {
	end := pos + 2
	closed := false
	for end+1 < len(sql) {
		if sql[end] == '*' && sql[end+1] == '/' {
			end += 2
			closed = true
			break
		}
		end++
	}
	if !closed {
		// 没闭合的块注释：MySQL 报语法错，这里整段丢掉，
		// 让后面的错误信息更接近真实原因。
		return "", len(sql)
	}

	body := sql[pos+2 : end-2]
	// 版本注释：/*! 后跟 5 位版本号
	if strings.HasPrefix(body, "!") {
		inner := body[1:]
		// 去掉版本号（5 位数字）
		if len(inner) >= 5 && isAllDigits(inner[:5]) {
			inner = inner[5:]
			// 版本号后面通常还有一个空格
			if len(inner) > 0 && isSpace(inner[0]) {
				inner = inner[1:]
			}
		}
		return " " + inner + " ", end
	}
	return "", end
}

// isAllDigits 判断字符串是否全部由数字组成。
func isAllDigits(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return len(text) > 0
}
