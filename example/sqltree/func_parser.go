package main

import (
	"fmt"
	"strings"
)

// CREATE FUNCTION 在 vitess 语法层完全不支持（go-mysql-server 也没有对应的 plan 节点），
// 因此这里手写一个轻量解析器。MySQL 的语法是：
//
//	CREATE [AGGREGATE] FUNCTION sp_name ([func_parameter[,func_parameter]...])
//	    RETURNS type
//	    [characteristic ...]
//	    function_body
//
// 其中 characteristic 取值为：
//	CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA |
//	DETERMINISTIC | NOT DETERMINISTIC | COMMENT 'string'

// FunctionDetails 表示 CREATE FUNCTION 语句的解析结果。
type FunctionDetails struct {
	Name            string           `json:"name"`
	Aggregate       bool             `json:"aggregate,omitempty"`
	Params          []ProcedureParam `json:"params,omitempty"`
	Returns         string           `json:"returns,omitempty"`
	Characteristics []string         `json:"characteristics,omitempty"`
	Comment         string           `json:"comment,omitempty"`
	Body            string           `json:"body,omitempty"`
	Statements      []string         `json:"statements,omitempty"`
}

// functionCharacteristics 是 MySQL 中合法的 function characteristic 关键字集合。
// 这些关键字都是多词的，需要按空白分隔的词组匹配，不能只匹配单个词。
var functionCharacteristics = []string{
	"contains sql",
	"no sql",
	"reads sql data",
	"modifies sql data",
	"deterministic",
	"not deterministic",
	"comment",
}

// matchCharacteristic 在文本开头匹配一个 function characteristic。
// 返回匹配到的关键字与剩余文本；未匹配时返回空串。
func matchCharacteristic(text string) (string, string) {
	lower := strings.ToLower(text)
	// "not deterministic" 必须先于 "deterministic" 匹配
	for _, keyword := range functionCharacteristics {
		if !strings.HasPrefix(lower, keyword) {
			continue
		}
		// 关键字后面必须跟空白，避免 "deterministicx" 误匹配
		rest := text[len(keyword):]
		if rest != "" && !isSpace(rest[0]) {
			continue
		}
		return strings.ToUpper(keyword), strings.TrimSpace(rest)
	}
	return "", text
}

// parseCreateFunction 尝试把 SQL 解析为 CREATE FUNCTION 语句。
// 若不是 CREATE FUNCTION，返回 nil。
func parseCreateFunction(sqlStr string) *FunctionDetails {
	rest := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(rest)

	if !strings.HasPrefix(lower, "create") {
		return nil
	}
	rest = strings.TrimSpace(rest[len("create"):])
	lower = strings.ToLower(rest)

	// CREATE OR REPLACE FUNCTION
	if strings.HasPrefix(lower, "or replace") {
		rest = strings.TrimSpace(rest[len("or replace"):])
		lower = strings.ToLower(rest)
	}
	// CREATE AGGREGATE FUNCTION
	aggregate := false
	if strings.HasPrefix(lower, "aggregate") {
		aggregate = true
		rest = strings.TrimSpace(rest[len("aggregate"):])
		lower = strings.ToLower(rest)
	}
	if !strings.HasPrefix(lower, "function") {
		return nil
	}
	rest = strings.TrimSpace(rest[len("function"):])

	details := &FunctionDetails{
		Aggregate:       aggregate,
		Params:          []ProcedureParam{},
		Characteristics: []string{},
	}

	// 函数名
	name, rest, ok := scanIdentifier(rest)
	if !ok {
		return nil
	}
	details.Name = name

	// 参数列表
	if !strings.HasPrefix(strings.TrimSpace(rest), "(") {
		return nil
	}
	paramText, rest, ok := scanBalanced(rest, '(', ')')
	if !ok {
		return nil
	}
	details.Params = parseFunctionParams(paramText)

	rest = strings.TrimSpace(rest)
	lower = strings.ToLower(rest)

	// RETURNS type
	if !strings.HasPrefix(lower, "returns") {
		return nil
	}
	rest = strings.TrimSpace(rest[len("returns"):])

	returnType, rest, ok := scanType(rest)
	if !ok {
		return nil
	}
	details.Returns = returnType

	// 特性列表，直到遇到 body 起始关键字
	rest = strings.TrimSpace(rest)
	for {
		lower = strings.ToLower(rest)
		if strings.HasPrefix(lower, "return") || strings.HasPrefix(lower, "begin") {
			break
		}
		keyword, remainder := matchCharacteristic(rest)
		if keyword == "" {
			// 不是 characteristic，说明已经越过特性列表
			break
		}
		rest = remainder
		if keyword == "COMMENT" {
			comment, remainder2, ok := scanQuoted(rest)
			if ok {
				details.Comment = comment
				rest = strings.TrimSpace(remainder2)
			}
		}
		details.Characteristics = append(details.Characteristics, keyword)
	}

	// 函数体
	body := strings.TrimSpace(rest)
	details.Body = body
	details.Statements = splitFunctionBody(body)

	return details
}

// parseFunctionParams 解析函数参数列表文本。
// 形如 "a INT, b VARCHAR(10)"，每项为 "name type"（MySQL 函数参数不支持方向与默认值）。
func parseFunctionParams(text string) []ProcedureParam {
	params := []ProcedureParam{}
	for _, part := range splitTopLevel(text, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, typ, ok := splitNameAndType(part)
		if !ok {
			continue
		}
		params = append(params, ProcedureParam{Name: name, Type: typ})
	}
	return params
}

// splitNameAndType 把 "name TYPE" 拆成 name 与 TYPE。
func splitNameAndType(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	idx := strings.IndexAny(text, " \t\n")
	if idx <= 0 {
		return "", "", false
	}
	name := text[:idx]
	typ := strings.TrimSpace(text[idx+1:])
	if typ == "" {
		return "", "", false
	}
	return name, typ, true
}

// splitFunctionBody 把函数体拆成语句列表。
// "RETURN expr" 是单语句；"BEGIN ... END" 需要按分号拆分（忽略字符串/注释内的分号）。
func splitFunctionBody(body string) []string {
	body = strings.TrimSpace(body)
	if body == "" {
		return []string{}
	}
	lower := strings.ToLower(body)
	if strings.HasPrefix(lower, "begin") {
		inner := strings.TrimSpace(body[len("begin"):])
		if end := findMatchingEnd(inner); end >= 0 {
			inner = inner[:end]
		}
		return splitStatements(inner)
	}
	return []string{body}
}

// findMatchingEnd 在 "BEGIN ... END" 文本中找到与外层 BEGIN 配对的 END 位置。
// text 是已经剥掉外层 BEGIN 之后的部分，因此这里从深度 1 开始计数。
// 返回 END 的起始下标，未找到返回 -1。
func findMatchingEnd(text string) int {
	// 已经剥掉了外层 BEGIN，所以当前深度为 1
	depth := 1
	lower := strings.ToLower(text)
	for i := 0; i < len(text); {
		// 跳过字符串字面量
		if text[i] == '\'' || text[i] == '"' || text[i] == '`' {
			quote := text[i]
			i++
			for i < len(text) {
				if text[i] == '\\' {
					i += 2
					continue
				}
				if text[i] == quote {
					i++
					break
				}
				i++
			}
			continue
		}
		if i+5 <= len(text) && lower[i:i+5] == "begin" && isWordBoundary(text, i, i+5) {
			depth++
			i += 5
			continue
		}
		if i+3 <= len(text) && lower[i:i+3] == "end" && isWordBoundary(text, i, i+3) {
			depth--
			if depth == 0 {
				return i
			}
			i += 3
			continue
		}
		i++
	}
	return -1
}

// isWordBoundary 判断 text[start:end] 是否为一个独立单词。
// 避免把 "endpoint" 里的 "end" 当作块结束关键字。
func isWordBoundary(text string, start, end int) bool {
	if start > 0 && isIdentChar(text[start-1]) {
		return false
	}
	if end < len(text) && isIdentChar(text[end]) {
		return false
	}
	return true
}

// splitStatements 按顶层分号切分语句列表。
func splitStatements(text string) []string {
	stmts := []string{}
	var current strings.Builder
	for i := 0; i < len(text); {
		c := text[i]
		if c == '\'' || c == '"' || c == '`' {
			quote := c
			current.WriteByte(c)
			i++
			for i < len(text) {
				if text[i] == '\\' && i+1 < len(text) {
					current.WriteByte(text[i])
					current.WriteByte(text[i+1])
					i += 2
					continue
				}
				current.WriteByte(text[i])
				if text[i] == quote {
					i++
					break
				}
				i++
			}
			continue
		}
		if c == ';' {
			if stmt := strings.TrimSpace(current.String()); stmt != "" {
				stmts = append(stmts, stmt)
			}
			current.Reset()
			i++
			continue
		}
		current.WriteByte(c)
		i++
	}
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		stmts = append(stmts, stmt)
	}
	return stmts
}

// splitTopLevel 按顶层分隔符切分文本，忽略括号与引号内的分隔符。
func splitTopLevel(text string, sep byte) []string {
	parts := []string{}
	var current strings.Builder
	depth := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			quote := c
			current.WriteByte(c)
			i++
			for i < len(text) {
				if text[i] == '\\' && i+1 < len(text) {
					current.WriteByte(text[i])
					current.WriteByte(text[i+1])
					i += 2
					continue
				}
				current.WriteByte(text[i])
				if text[i] == quote {
					break
				}
				i++
			}
		case c == '(':
			depth++
			current.WriteByte(c)
		case c == ')':
			depth--
			current.WriteByte(c)
		case c == sep && depth == 0:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(c)
		}
	}
	parts = append(parts, current.String())
	return parts
}

// scanIdentifier 读取一个标识符（允许 `反引号` 包裹与 db.table 形式）。
func scanIdentifier(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", "", false
	}
	var parts []string
	for {
		if text[0] == '`' {
			end := strings.IndexByte(text[1:], '`')
			if end < 0 {
				return "", "", false
			}
			parts = append(parts, text[1:end+1])
			text = strings.TrimSpace(text[end+2:])
		} else {
			i := 0
			for i < len(text) && (isIdentChar(text[i])) {
				i++
			}
			if i == 0 {
				return "", "", false
			}
			parts = append(parts, text[:i])
			text = text[i:]
		}
		text = strings.TrimSpace(text)
		if strings.HasPrefix(text, ".") {
			text = strings.TrimSpace(text[1:])
			continue
		}
		break
	}
	return strings.Join(parts, "."), text, true
}

// scanBalanced 读取以 open 开头、close 结束的平衡片段，返回片段内容与剩余文本。
func scanBalanced(text string, open, close byte) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" || text[0] != open {
		return "", "", false
	}
	depth := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c == '\'' || c == '"' || c == '`' {
			quote := c
			i++
			for i < len(text) {
				if text[i] == '\\' && i+1 < len(text) {
					i += 2
					continue
				}
				if text[i] == quote {
					break
				}
				i++
			}
			continue
		}
		if c == open {
			depth++
			continue
		}
		if c == close {
			depth--
			if depth == 0 {
				return text[1:i], text[i+1:], true
			}
		}
	}
	return "", "", false
}

// scanType 读取一个类型定义（如 INT、VARCHAR(20)、DECIMAL(10,2)）。
func scanType(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	end := 0
	for end < len(text) && isIdentChar(text[end]) {
		end++
	}
	if end == 0 {
		return "", "", false
	}
	name := text[:end]
	rest := text[end:]
	// 可能的精度参数
	if strings.HasPrefix(strings.TrimSpace(rest), "(") {
		if inner, remainder, ok := scanBalanced(rest, '(', ')'); ok {
			name = fmt.Sprintf("%s(%s)", name, inner)
			rest = remainder
		}
	}
	// UNSIGNED / ZEROFILL 等修饰
	trimmed := strings.TrimSpace(rest)
	for {
		lower := strings.ToLower(trimmed)
		if strings.HasPrefix(lower, "unsigned") {
			name += " UNSIGNED"
			trimmed = strings.TrimSpace(trimmed[len("unsigned"):])
			continue
		}
		if strings.HasPrefix(lower, "zerofill") {
			name += " ZEROFILL"
			trimmed = strings.TrimSpace(trimmed[len("zerofill"):])
			continue
		}
		break
	}
	return name, trimmed, true
}

// scanWord 读取一个空白分隔的单词。
func scanWord(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	end := 0
	for end < len(text) && !isSpace(text[end]) {
		end++
	}
	if end == 0 {
		return "", "", false
	}
	return text[:end], text[end:], true
}

// scanQuoted 读取一个引号包裹的字符串。
func scanQuoted(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", "", false
	}
	quote := text[0]
	if quote != '\'' && quote != '"' {
		return "", "", false
	}
	var sb strings.Builder
	for i := 1; i < len(text); i++ {
		if text[i] == '\\' && i+1 < len(text) {
			sb.WriteByte(text[i+1])
			i++
			continue
		}
		if text[i] == quote {
			return sb.String(), text[i+1:], true
		}
		sb.WriteByte(text[i])
	}
	return "", "", false
}

// isIdentChar 判断是否为标识符字符。
func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// isSpace 判断是否为空白字符。
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
