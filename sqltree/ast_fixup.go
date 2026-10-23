package sqltree

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// 本文件处理 vitess 语法层的几个"硬伤"：
//
//  1. vitess 不支持函数索引（MySQL 8.0.13+ 的 `KEY idx ((expr))`），
//     一旦建表语句里出现它，整条语句会解析失败。
//     这里先把表达式替换成占位列名解析，解析完再还原成真正的函数索引。
//
//  2. vitess 的 TableExprs 在多表 JOIN 时是一棵 JoinTableExpr 树，
//     简单的 type switch 取不到表名，需要递归收集。
//
//  3. LIMIT 的 OFFSET 是独立的 Expr 字段，容易被漏掉。

// fnIndexPlaceholderPrefix 是函数索引占位列名前缀。
const fnIndexPlaceholderPrefix = "__vitess_fnidx_"

// functionalIndexInfo 描述一个函数索引。
type functionalIndexInfo struct {
	Name     string `json:"name"`
	Expr     string `json:"expression"`
	Unique   bool   `json:"unique,omitempty"`
	FullText bool   `json:"fulltext,omitempty"`
	Spatial  bool   `json:"spatial,omitempty"`
	Using    string `json:"using,omitempty"`

	// index 是该函数索引在列表中的序号，解析阶段用来把占位列名映射回来。
	index int
}

// placeholder 返回该函数索引对应的占位列名。
func (f functionalIndexInfo) placeholder() string {
	return fmt.Sprintf("%s%d__", fnIndexPlaceholderPrefix, f.index)
}

// functionalIndexInfoList 是函数索引列表。
type functionalIndexInfoList []functionalIndexInfo

// lookup 按占位列名查找对应的函数索引。
func (l functionalIndexInfoList) lookup(column string) (functionalIndexInfo, bool) {
	for _, info := range l {
		if info.placeholder() == column {
			return info, true
		}
	}
	return functionalIndexInfo{}, false
}

// rewriteFunctionalIndexes 把 CREATE TABLE 中的函数索引
// （`KEY idx ((expr))`）改写成普通索引，因为 vitess 语法层不支持表达式索引。
//
// 返回改写后的 SQL 与解析出的函数索引列表。
// 若语句里没有函数索引，原样返回。
func rewriteFunctionalIndexes(sqlStr string) (string, functionalIndexInfoList) {
	open := findTableSpecOpen(sqlStr)
	if open < 0 {
		return sqlStr, nil
	}
	end := matchingParen(sqlStr, open)
	if end < 0 {
		return sqlStr, nil
	}

	content := sqlStr[open+1 : end]
	items := splitTopLevelItems(content)

	var infos functionalIndexInfoList
	var sb strings.Builder
	pos := 0
	for _, item := range items {
		sb.WriteString(content[pos:item.start])
		text := content[item.start:item.end]
		newText, info, ok := rewriteFunctionalIndexItem(text, len(infos))
		if ok {
			info.index = len(infos)
			infos = append(infos, info)
		}
		sb.WriteString(newText)
		pos = item.end
	}
	sb.WriteString(content[pos:])

	if len(infos) == 0 {
		return sqlStr, nil
	}
	return sqlStr[:open+1] + sb.String() + sqlStr[end:], infos
}

// findTableSpecOpen 找到建表语句中表定义括号 '(' 的位置。
// 找不到（例如 CREATE TABLE t LIKE u）返回 -1。
//
// 注释里的括号不算——`CREATE TABLE t (id INT) /* (note) */` 里
// 表定义括号是第一个，注释里的那个不能算。
func findTableSpecOpen(sqlStr string) int {
	regions := scanLiteralRegions(sqlStr)
	for i := 0; i < len(sqlStr); i++ {
		if sqlStr[i] == '(' && !regions.inLiteral(i) {
			return i
		}
	}
	return -1
}

// matchingParen 返回与 open 处 '(' 配对的 ')' 的位置，找不到返回 -1。
//
// 注释里的括号不参与配对。
func matchingParen(text string, open int) int {
	regions := scanLiteralRegions(text)
	depth := 0
	for i := open; i < len(text); i++ {
		if regions.inLiteral(i) {
			continue
		}
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// textRange 表示文本中一段的范围 [start, end)。
type textRange struct {
	start int
	end   int
}

// splitTopLevelItems 按顶层逗号切分文本，返回各项的范围。
// 括号内与引号内的逗号不切分。
func splitTopLevelItems(text string) []textRange {
	ranges := []textRange{}
	regions := scanLiteralRegions(text)
	depth := 0
	start := 0
	for i := 0; i < len(text); i++ {
		// 字面量与注释里的括号、逗号都不参与切分
		if regions.inLiteral(i) {
			continue
		}
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth != 0 {
				continue
			}
			if trimmed := strings.TrimSpace(text[start:i]); trimmed != "" {
				ranges = append(ranges, textRange{start: start, end: i})
			}
			start = i + 1
		}
	}
	if trimmed := strings.TrimSpace(text[start:]); trimmed != "" {
		ranges = append(ranges, textRange{start: start, end: len(text)})
	}
	return ranges
}

// rewriteFunctionalIndexItem 判断一个建表项是否是函数索引，
// 是则改写成普通索引（列名用占位符），并返回函数索引信息。
func rewriteFunctionalIndexItem(text string, idx int) (string, functionalIndexInfo, bool) {
	info := functionalIndexInfo{}
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)

	// 索引类型前缀：UNIQUE / FULLTEXT / SPATIAL
	prefix := ""
	for _, kw := range []struct {
		name string
		set  func(*functionalIndexInfo)
	}{
		{"unique", func(f *functionalIndexInfo) { f.Unique = true }},
		{"fulltext", func(f *functionalIndexInfo) { f.FullText = true }},
		{"spatial", func(f *functionalIndexInfo) { f.Spatial = true }},
	} {
		if strings.HasPrefix(lower, kw.name+" ") {
			set := kw.set
			set(&info)
			prefix = kw.name + " "
			trimmed = strings.TrimSpace(trimmed[len(kw.name):])
			lower = strings.ToLower(trimmed)
			break
		}
	}

	// KEY / INDEX 关键字
	if !strings.HasPrefix(lower, "key ") && !strings.HasPrefix(lower, "index ") {
		return text, info, false
	}
	trimmed = strings.TrimSpace(trimmed[strings.IndexByte(trimmed, ' ')+1:])
	lower = strings.ToLower(trimmed)

	// 索引名（可省略）
	name := ""
	if !strings.HasPrefix(trimmed, "(") {
		var ok bool
		var rest string
		name, rest, ok = scanIdentifier(trimmed)
		if !ok {
			return text, info, false
		}
		info.Name = name
		trimmed = strings.TrimSpace(rest)
		lower = strings.ToLower(trimmed)
	}

	// USING BTREE / USING HASH
	usingPart := ""
	if strings.HasPrefix(lower, "using ") {
		using, rest, ok := scanWord(trimmed[len("using "):])
		if ok {
			info.Using = using
			usingPart = "USING " + using
			trimmed = strings.TrimSpace(rest)
		}
	}

	// 索引列清单
	if !strings.HasPrefix(trimmed, "(") {
		return text, info, false
	}
	inner, rest, ok := scanBalanced(trimmed, '(', ')')
	if !ok {
		return text, info, false
	}
	innerTrim := strings.TrimSpace(inner)
	// 函数索引的特征：整个列清单被一对额外的括号包住
	if !strings.HasPrefix(innerTrim, "(") || !strings.HasSuffix(innerTrim, ")") {
		return text, info, false
	}
	expr := strings.TrimSpace(innerTrim[1 : len(innerTrim)-1])
	if expr == "" {
		return text, info, false
	}
	info.Expr = expr

	placeholder := fmt.Sprintf("%s%d__", fnIndexPlaceholderPrefix, idx)
	keyword := "KEY "
	if prefix != "" {
		keyword = prefix + keyword
	}
	namePart := ""
	if name != "" {
		namePart = name + " "
	}
	trailing := strings.TrimSpace(rest)

	newText := keyword + namePart + usingPart + " (" + placeholder + ")"
	if trailing != "" {
		newText += " " + trailing
	}
	return newText, info, true
}

// collectTableNames 递归收集表表达式中的所有表名（支持 JOIN 树与派生表）。
func collectTableNames(exprs sqlparser.TableExprs) []string {
	names := []string{}
	for _, expr := range exprs {
		collectTableNamesFromExpr(expr, &names)
	}
	return names
}

// collectTableNamesFromExpr 递归收集单个表表达式里的表名。
func collectTableNamesFromExpr(expr sqlparser.TableExpr, names *[]string) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := e.Expr.(sqlparser.TableName); ok {
			*names = append(*names, tableFullName(tableName))
		}
	case *sqlparser.JoinTableExpr:
		collectTableNamesFromExpr(e.LeftExpr, names)
		collectTableNamesFromExpr(e.RightExpr, names)
	case *sqlparser.ParenTableExpr:
		for _, sub := range e.Exprs {
			collectTableNamesFromExpr(sub, names)
		}
	}
}

// limitOffset 提取 LIMIT 的 OFFSET 值（非整数字面量时返回 0）。
func limitOffset(limit *sqlparser.Limit) int {
	if limit == nil || limit.Offset == nil {
		return 0
	}
	literal, ok := limit.Offset.(*sqlparser.SQLVal)
	if !ok || literal.Type != sqlparser.IntVal {
		return 0
	}
	value, err := strconv.Atoi(string(literal.Val))
	if err != nil {
		return 0
	}
	return value
}

// tableOptionKeywords 是 ALTER TABLE 允许的表选项关键字。
// vitess 的 DDL 节点没有承载它们的字段，只能从原文里取回。
var tableOptionKeywords = []string{
	"engine", "charset", "character set", "collate", "comment",
	"auto_increment", "row_format", "avg_row_length", "key_block_size",
	"max_rows", "min_rows", "pack_keys", "stats_persistent", "tablespace",
	"storage", "insert_method", "checksum", "delay_key_write",
	"compression", "encryption", "default character set", "default charset",
	"default collate", "default row_format", "default tablespace",
	"algorithm",
}

// enrichAlterActions 用原始 SQL 补回 vitess 丢失的 ALTER 动作信息。
//
// vitess 会把 `ALTER TABLE t ENGINE=InnoDB`、`ALTER COLUMN c SET DEFAULT 1`
// 这类语句解析成一个没有任何字段的 DDL 节点，动作内容全丢。
// 这里把原文按顶层逗号切开，与 AST 解析出的动作一一对应后回填。
//
// 若原文切分出的动作数与 AST 解析出的数量不一致（说明 vitess 的切分规则不同），
// 就放弃回填，避免张冠李戴。
func enrichAlterActions(actions []AlterAction, sqlStr string) []AlterAction {
	segments := splitAlterTableBody(sqlStr)
	if len(segments) != len(actions) {
		return actions
	}
	for i, segment := range segments {
		switch classifyAlterAction(segment) {
		case "table_option":
			actions[i] = AlterAction{Action: "option", Detail: segment}
		case "alter_column":
			actions[i] = AlterAction{Action: "alter_column", Detail: segment}
		case "rename_table":
			actions[i] = AlterAction{Action: "rename", Detail: segment}
		}
	}
	return actions
}

// splitTableOptionRun 把一条"纯表选项"的动作按空格拆成若干个选项。
//
// MySQL 允许 `ALTER TABLE t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4` 这样
// 用空格分隔多个表选项，而 vitess 只认单个选项——整条语句会报语法错误。
// 这里把选项拆开，交给调用方改写成逗号分隔的形式再喂给 vitess。
//
// 只有当这一段**以表选项关键字开头**时才拆：
// `ADD COLUMN a INT COMMENT 'x'` 里的 COMMENT 是列注释，不是表选项，
// 拆开就张冠李戴了。
func splitTableOptionRun(segment string) []string {
	trimmed := strings.TrimSpace(segment)
	if trimmed == "" || !startsWithTableOption(trimmed) {
		return []string{trimmed}
	}
	// 扫描出各个选项的起始位置
	var starts []int
	i := 0
	for i < len(trimmed) {
		if isQuoteChar(trimmed[i]) {
			i = skipQuoted(trimmed, i)
			continue
		}
		if i > 0 && !isSpace(trimmed[i-1]) {
			i++
			continue
		}
		if kw, ok := matchTableOptionKeyword(trimmed, i); ok {
			starts = append(starts, i)
			i += len(kw)
			continue
		}
		i++
	}
	if len(starts) <= 1 {
		return []string{trimmed}
	}
	parts := make([]string, 0, len(starts))
	for idx, start := range starts {
		end := len(trimmed)
		if idx+1 < len(starts) {
			end = starts[idx+1]
		}
		parts = append(parts, strings.TrimSpace(trimmed[start:end]))
	}
	return parts
}

// startsWithTableOption 判断文本是否以表选项关键字开头。
func startsWithTableOption(text string) bool {
	_, ok := matchTableOptionKeyword(text, 0)
	return ok
}

// matchTableOptionKeyword 在 text 的 pos 处匹配一个表选项关键字。
// 返回匹配到的关键字与是否匹配成功。
//
// 关键字后面必须跟 "=" 或空白（否则只是恰好同名的普通词），并且取最长匹配——
// "DEFAULT CHARACTER SET" 要比 "CHARACTER SET" 优先。
func matchTableOptionKeyword(text string, pos int) (string, bool) {
	lower := strings.ToLower(text)
	best := ""
	for _, keyword := range tableOptionKeywords {
		if len(keyword) <= len(best) {
			continue
		}
		if !strings.HasPrefix(lower[pos:], keyword) {
			continue
		}
		after := pos + len(keyword)
		if after < len(text) {
			// 关键字后面只能是 "=" 或空白
			if text[after] != '=' && !isSpace(text[after]) {
				continue
			}
		}
		best = keyword
	}
	if best == "" {
		return "", false
	}
	return best, true
}

// isQuoteChar 判断是否是引号字符。
func isQuoteChar(c byte) bool {
	return c == '\'' || c == '"' || c == '`'
}

// skipQuoted 跳过从 pos 处开始的引号字符串，返回结束后的位置。
func skipQuoted(text string, pos int) int {
	quote := text[pos]
	i := pos + 1
	for i < len(text) {
		if text[i] == '\\' {
			i += 2
			continue
		}
		if text[i] == quote {
			// 连续两个引号是转义
			if i+1 < len(text) && text[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(text)
}

// splitAlterTableBody 把 ALTER TABLE 语句的正文按顶层逗号切成若干条动作原文。
// 不是 ALTER TABLE 或无法定位正文时返回 nil。
func splitAlterTableBody(sqlStr string) []string {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "alter") {
		return nil
	}
	rest := strings.TrimSpace(trimmed[len("alter"):])
	if !strings.HasPrefix(strings.ToLower(rest), "table") {
		return nil
	}
	rest = strings.TrimSpace(rest[len("table"):])

	// 跳过表名（可能带 db. 前缀与反引号）
	if _, remainder, ok := scanIdentifier(rest); ok {
		rest = strings.TrimSpace(remainder)
	}
	body := strings.TrimSpace(strings.TrimSuffix(rest, ";"))
	if body == "" {
		return nil
	}

	segments := []string{}
	for _, item := range splitTopLevelItems(body) {
		text := strings.TrimSpace(body[item.start:item.end])
		// 用空格分隔的表选项要拆成多条——改写后的 SQL 里它们是逗号分隔的，
		// 两边条数必须一致，否则回填时会对不上。
		if startsWithTableOption(text) {
			segments = append(segments, splitTableOptionRun(text)...)
			continue
		}
		segments = append(segments, text)
	}
	return segments
}

// classifyAlterAction 判断一条 ALTER 动作原文的种类。
func classifyAlterAction(text string) string {
	lower := strings.ToLower(strings.TrimSpace(text))

	verb := ""
	rest := lower
	for _, v := range []string{"add ", "drop ", "modify ", "change ", "rename ", "alter "} {
		if strings.HasPrefix(rest, v) {
			verb = strings.TrimSpace(v)
			rest = strings.TrimSpace(rest[len(v):])
			break
		}
	}

	// ALTER COLUMN ... 只可能来自 ALTER 动词
	if verb == "alter" {
		return "alter_column"
	}
	switch {
	case strings.HasPrefix(rest, "column "):
		return "column"
	case strings.HasPrefix(rest, "index "), strings.HasPrefix(rest, "key "):
		return "index"
	case strings.HasPrefix(rest, "constraint "), strings.HasPrefix(rest, "primary key"),
		strings.HasPrefix(rest, "foreign key "), strings.HasPrefix(rest, "check "):
		return "constraint"
	}
	if verb == "rename" {
		return "rename_table"
	}
	if isTableOptionText(rest) {
		return "table_option"
	}
	return "other"
}

// isTableOptionText 判断文本是否是 ALTER TABLE 的表选项。
func isTableOptionText(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	for _, keyword := range tableOptionKeywords {
		if trimmed == keyword || strings.HasPrefix(trimmed, keyword+" ") || strings.HasPrefix(trimmed, keyword+"=") {
			return true
		}
	}
	return false
}
