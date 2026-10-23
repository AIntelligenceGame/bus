package sqltree

import (
	"strings"
)

// vitess 的语法层覆盖了绝大多数 MySQL 语法，但仍有若干 MySQL 合法语法它不支持，
// 一旦出现整条语句就报语法错误：
//
//	INSERT [LOW_PRIORITY | HIGH_PRIORITY | DELAYED] ...
//	SELECT ... GROUP BY ... WITH ROLLUP
//	TRUNCATE TABLE t PARTITION p0
//	DROP VIEW v CASCADE
//	DO expr
//
// 这些语法的信息量都很小，做一次"剥离 + 记录"的改写即可：
// 把 vitess 不认得的部分摘掉让它去解析，解析结果里再把摘掉的内容带回去。

// leadingWhitespace 返回字符串的前导空白。
func leadingWhitespace(text string) string {
	return text[:len(text)-len(strings.TrimLeft(text, " \t\n\r"))]
}

// syntaxFixup 记录一次针对 vitess 语法缺陷的改写。
type syntaxFixup struct {
	// Kind 标识改写类型。
	Kind string `json:"kind"`
	// Original 是被剥离的原文。
	Original string `json:"original"`
	// Note 是给人看的说明。
	Note string `json:"note"`
}

// syntaxFixupKind 是各类改写的标识。
const (
	fixupKindInsertPriority = "insert_priority"
	fixupKindWithRollup     = "group_by_rollup"
	fixupKindTruncatePart   = "truncate_partition"
	fixupKindDropViewCasc   = "drop_view_cascade"
	fixupKindPartition      = "partition_clause"
	fixupKindExplainFormat  = "explain_format"
	fixupKindZerofill       = "zerofill"
	fixupKindAlterOptions   = "alter_table_options"
)

// applySyntaxFixups 针对 vitess 不支持的语法做剥离改写。
// 返回改写后的 SQL 与改写记录。
func applySyntaxFixups(sqlStr string) (string, []syntaxFixup) {
	fixups := []syntaxFixup{}
	rest := sqlStr

	rest, fixup := fixupInsertPriority(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupGroupByRollup(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupTruncatePartition(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupDropViewCascade(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupExplainFormat(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupZerofill(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	rest, fixup = fixupAlterTableOptions(rest)
	if fixup != nil {
		fixups = append(fixups, *fixup)
	}
	return rest, fixups
}

// fixupInsertPriority 摘掉 INSERT 的优先级关键字。
// vitess 支持 INSERT IGNORE，但不支持 LOW_PRIORITY / HIGH_PRIORITY / DELAYED。
func fixupInsertPriority(sqlStr string) (string, *syntaxFixup) {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "insert ") {
		return sqlStr, nil
	}
	rest := strings.TrimSpace(trimmed[len("insert"):])
	restLower := strings.ToLower(rest)
	for _, keyword := range []string{"low_priority", "high_priority", "delayed"} {
		if !strings.HasPrefix(restLower, keyword+" ") {
			continue
		}
		remainder := strings.TrimSpace(rest[len(keyword):])
		// 保持原语句的前导空白
		prefix := leadingWhitespace(sqlStr)
		return prefix + "INSERT " + remainder, &syntaxFixup{
			Kind:     fixupKindInsertPriority,
			Original: strings.ToUpper(keyword),
			Note:     "INSERT 优先级关键字 " + strings.ToUpper(keyword) + "（vitess 不支持，已剥离）",
		}
	}
	return sqlStr, nil
}

// fixupGroupByRollup 摘掉 GROUP BY 后面的 WITH ROLLUP。
//
// 查找只在**代码区**进行——字面量里的 'group by ... with rollup' 不能命中，
// 否则用户的字符串值会被静默改写。
func fixupGroupByRollup(sqlStr string) (string, *syntaxFixup) {
	regions := scanLiteralRegions(sqlStr)
	groupIdx := regions.findKeyword("group by")
	if groupIdx < 0 {
		return sqlStr, nil
	}
	rollupIdx := regions.findKeywordAfter("with rollup", groupIdx)
	if rollupIdx < 0 {
		return sqlStr, nil
	}
	// WITH ROLLUP 后面可能还有别的内容（ORDER BY / LIMIT），只摘关键字本身
	end := rollupIdx + len("with rollup")
	return sqlStr[:rollupIdx] + sqlStr[end:], &syntaxFixup{
		Kind:     fixupKindWithRollup,
		Original: "WITH ROLLUP",
		Note:     "GROUP BY ... WITH ROLLUP（vitess 不支持，已剥离）",
	}
}

// fixupTruncatePartition 摘掉 TRUNCATE TABLE t PARTITION p0 里的分区名。
func fixupTruncatePartition(sqlStr string) (string, *syntaxFixup) {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "truncate ") {
		return sqlStr, nil
	}
	rest := strings.TrimSpace(trimmed[len("truncate"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "table ") {
		rest = strings.TrimSpace(rest[len("table"):])
	} else {
		return sqlStr, nil
	}
	// 跳过表名
	tableName, remainder, ok := scanIdentifier(rest)
	if !ok {
		return sqlStr, nil
	}
	remainder = strings.TrimSpace(remainder)
	if !strings.HasPrefix(strings.ToLower(remainder), "partition ") {
		return sqlStr, nil
	}
	partition, after, ok := scanWord(remainder[len("partition "):])
	if !ok {
		return sqlStr, nil
	}
	// 表名之后若还有别的内容就不处理（不是单纯的 PARTITION 形式）
	if strings.TrimSpace(after) != "" {
		return sqlStr, nil
	}
	prefix := leadingWhitespace(sqlStr)
	return prefix + "TRUNCATE TABLE " + tableName, &syntaxFixup{
		Kind:     fixupKindTruncatePart,
		Original: "PARTITION " + partition,
		Note:     "TRUNCATE 的分区名 " + partition + "（vitess 不支持，已剥离）",
	}
}

// fixupDropViewCascade 摘掉 DROP VIEW 结尾的 CASCADE / RESTRICT。
// vitess 对 DROP TABLE 支持这两个关键字，对 DROP VIEW 不支持。
func fixupDropViewCascade(sqlStr string) (string, *syntaxFixup) {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "drop view ") {
		return sqlStr, nil
	}
	for _, keyword := range []string{"cascade", "restrict"} {
		suffix := " " + keyword
		if !strings.HasSuffix(lower, suffix) {
			continue
		}
		prefix := leadingWhitespace(sqlStr)
		body := strings.TrimSpace(trimmed[len("drop view ") : len(trimmed)-len(keyword)])
		return prefix + "DROP VIEW " + body, &syntaxFixup{
			Kind:     fixupKindDropViewCasc,
			Original: strings.ToUpper(keyword),
			Note:     "DROP VIEW 的 " + strings.ToUpper(keyword) + "（vitess 不支持，已剥离）",
		}
	}
	return sqlStr, nil
}

// fixupExplainFormat 把 EXPLAIN FORMAT=xxx 里的格式值换成普通标识符。
//
// vitess 的语法里 FORMAT 后面只能跟一个标识符，而 MySQL 允许
// EXPLAIN FORMAT=JSON 这种关键字写法（JSON 在 vitess 里是保留字），
// 直接解析会报语法错误。这里把格式值替换成占位标识符，
// 解析结果里再把原值换回去。
func fixupExplainFormat(sqlStr string) (string, *syntaxFixup) {
	trimmed := strings.TrimSpace(sqlStr)
	if !strings.HasPrefix(strings.ToLower(trimmed), "explain") {
		return sqlStr, nil
	}
	// 只在代码区找 FORMAT=——字面量里的 'format=json' 不能命中
	regions := scanLiteralRegions(sqlStr)
	idx := regions.findKeyword("format")
	if idx < 0 {
		return sqlStr, nil
	}
	// FORMAT 之后必须紧跟等号
	after := strings.TrimLeft(sqlStr[idx+len("format"):], " \t\n\r")
	if !strings.HasPrefix(after, "=") {
		return sqlStr, nil
	}
	// 定位等号后的格式值起点
	i := idx + len("format")
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	if i >= len(sqlStr) || sqlStr[i] != '=' {
		return sqlStr, nil
	}
	i++
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	if i >= len(sqlStr) {
		return sqlStr, nil
	}
	// 读取格式值
	var value string
	end := i
	if sqlStr[i] == '\'' || sqlStr[i] == '"' {
		quote := sqlStr[i]
		e := strings.IndexByte(sqlStr[i+1:], quote)
		if e < 0 {
			return sqlStr, nil
		}
		value = sqlStr[i+1 : i+1+e]
		end = i + 1 + e + 1
	} else {
		for end < len(sqlStr) && isIdentChar(sqlStr[end]) {
			end++
		}
		if end == i {
			return sqlStr, nil
		}
		value = sqlStr[i:end]
	}
	placeholder := "vitess_explain_fmt"
	return sqlStr[:i] + placeholder + sqlStr[end:], &syntaxFixup{
		Kind:     fixupKindExplainFormat,
		Original: value,
		Note:     "EXPLAIN 的 FORMAT=" + value + "（vitess 只接受标识符，已替换）",
	}
}

// fixupZerofill 摘掉列定义里的 ZEROFILL 关键字。
//
// vitess 只在**数值类型**上支持 ZEROFILL（`INT(5) UNSIGNED ZEROFILL` 可以），
// 但 `VARCHAR(128) ZEROFILL` 会报语法错误；同时 "zerofill" 本身是保留字，
// 用作列名（`SELECT zerofill FROM t`）也会报错。
//
// 这里把列定义里的 ZEROFILL 摘掉，并记下它属于哪一列，解析结果里再补回去。
//
// 关键约束：**只在代码区找关键字**。字面量里的 'zerofill'、
// 注释里的 zerofill 都必须原样保留——否则用户的字符串值会被静默改写。
func fixupZerofill(sqlStr string) (string, *syntaxFixup) {
	regions := scanLiteralRegions(sqlStr)
	idx := regions.findKeyword("zerofill")
	if idx < 0 {
		return sqlStr, nil
	}
	column := columnNameBefore(sqlStr, idx)
	// 去掉关键字及其前面的空白
	start := idx
	for start > 0 && isSpace(sqlStr[start-1]) {
		start--
	}
	end := idx + len("zerofill")
	original := "ZEROFILL"
	if column != "" {
		original = column + " ZEROFILL"
	}
	return sqlStr[:start] + sqlStr[end:], &syntaxFixup{
		Kind:     fixupKindZerofill,
		Original: original,
		Note:     "列 " + column + " 的 ZEROFILL 修饰（vitess 不支持，已剥离）",
	}
}

// fixupAlterTableOptions 改写 ALTER TABLE 的表选项。
//
// MySQL 允许一条 ALTER 里用**空格**分隔多个表选项：
//
//	ALTER TABLE t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
//
// 而 vitess 只认单个选项（且不认 CHARSET 这个简写），整条语句会报语法错。
//
// 这里做两件事：
//  1. 把用空格分隔的表选项拆成逗号分隔；
//  2. 把 CHARSET= 改写成 CHARACTER SET=（两者在 MySQL 里完全等价，
//     vitess 只认后者）。
//
// 只改写表选项，不碰 ADD/DROP/MODIFY 这类动作——`ADD COLUMN comment VARCHAR(10)`
// 里的 comment 是列注释，拆开就张冠李戴了。
func fixupAlterTableOptions(sqlStr string) (string, *syntaxFixup) {
	start, end, ok := alterTableBodyRange(sqlStr)
	if !ok {
		return sqlStr, nil
	}
	body := sqlStr[start:end]

	parts := []string{}
	changed := false
	for _, segment := range splitTopLevelItems(body) {
		// 切分出来的片段可能带前导空白，先去掉——
		// 否则表选项关键字不在开头，识别不出来。
		text := strings.TrimSpace(body[segment.start:segment.end])
		if !startsWithTableOption(text) {
			parts = append(parts, text)
			continue
		}
		options := splitTableOptionRun(text)
		if len(options) > 1 {
			changed = true
		}
		for _, option := range options {
			rewritten := rewriteCharsetKeyword(option)
			if rewritten != option {
				changed = true
			}
			parts = append(parts, rewritten)
		}
	}
	if !changed {
		return sqlStr, nil
	}

	newBody := strings.Join(parts, ", ")
	return sqlStr[:start] + newBody + sqlStr[end:], &syntaxFixup{
		Kind:     fixupKindAlterOptions,
		Original: strings.TrimSpace(body),
		Note:     "ALTER TABLE 的表选项（vitess 只认单个选项，已拆分）",
	}
}

// rewriteCharsetKeyword 把表选项里的 CHARSET 简写改写成 CHARACTER SET。
//
// MySQL 里 `CHARSET=utf8mb4` 与 `CHARACTER SET=utf8mb4` 等价，
// `DEFAULT CHARSET=` 与 `DEFAULT CHARACTER SET=` 也等价；
// vitess 的 ALTER TABLE 只认后者。
func rewriteCharsetKeyword(option string) string {
	lower := strings.ToLower(option)
	switch {
	case strings.HasPrefix(lower, "default charset"):
		return "DEFAULT CHARACTER SET" + option[len("default charset"):]
	case strings.HasPrefix(lower, "charset"):
		return "CHARACTER SET" + option[len("charset"):]
	}
	return option
}

// alterTableBodyRange 定位 ALTER TABLE 正文（表名之后、结尾分号之前）的范围。
//
// 不是 ALTER TABLE 或找不到正文时返回 false。
func alterTableBodyRange(sqlStr string) (int, int, bool) {
	i := 0
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	if !strings.HasPrefix(strings.ToLower(sqlStr[i:]), "alter") {
		return 0, 0, false
	}
	i += len("alter")
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	if !strings.HasPrefix(strings.ToLower(sqlStr[i:]), "table") {
		return 0, 0, false
	}
	i += len("table")
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	// 跳过表名（可能带库名前缀与反引号）
	if _, remainder, ok := scanIdentifier(sqlStr[i:]); ok {
		i = len(sqlStr) - len(remainder)
	}
	for i < len(sqlStr) && isSpace(sqlStr[i]) {
		i++
	}
	// 去掉结尾的分号与空白
	end := len(sqlStr)
	for end > i && isSpace(sqlStr[end-1]) {
		end--
	}
	if end > i && sqlStr[end-1] == ';' {
		end--
		for end > i && isSpace(sqlStr[end-1]) {
			end--
		}
	}
	if i >= end {
		return 0, 0, false
	}
	return i, end, true
}

// columnNameBefore 找出 text 中 pos 位置之前那一列的列名。
//
// 形如 "col_name INT(10) ZEROFILL"，需要跳过类型与类型参数，
// 一直往回找到紧邻的标识符。
func columnNameBefore(text string, pos int) string {
	i := pos - 1
	// 跳过空白
	for i >= 0 && isSpace(text[i]) {
		i--
	}
	// 跳过类型参数 (10,2)
	if i >= 0 && text[i] == ')' {
		depth := 0
		for ; i >= 0; i-- {
			if text[i] == ')' {
				depth++
			} else if text[i] == '(' {
				depth--
				if depth == 0 {
					i--
					break
				}
			}
		}
		for i >= 0 && isSpace(text[i]) {
			i--
		}
	}
	// 跳过类型名（i 停在类型名之前的那一个字符上）
	for i >= 0 && isIdentChar(text[i]) {
		i--
	}
	// 跳过空白，落在列名的最后一个字符上
	for i >= 0 && isSpace(text[i]) {
		i--
	}
	if i < 0 || !isIdentChar(text[i]) {
		return ""
	}
	end := i
	for end >= 0 && isIdentChar(text[end]) {
		end--
	}
	return text[end+1 : i+1]
}
