package sqltree

import (
	"strconv"
	"strings"
)

// vitess 的语法层只支持 `PARTITION BY HASH(col) PARTITIONS n` 这一种最简形式，
// MySQL 里更常见的 `PARTITION BY RANGE(...) (...)`（带分区定义列表）与
// `SUBPARTITION BY` 都会直接报语法错误。
//
// 这里自己扫一遍 PARTITION 子句：先把子句从建表语句里摘掉让 vitess 去解析表体，
// 再用下面的扫描器把分区信息解析出来。

// partitionClause 表示一段 PARTITION 子句的原文。
type partitionClause struct {
	// Text 是子句全文（从 "PARTITION BY" 开始）。
	Text string
	// Start/End 是子句在原文中的位置。
	Start int
	End   int
}

// isCreateTableStatement 判断语句是否是 CREATE TABLE。
//
// 必须先确认是建表语句：否则 SELECT 里窗口函数的
// `OVER (PARTITION BY ...)` 会被误当成建表的分区子句。
func isCreateTableStatement(sqlStr string) bool {
	rest := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(rest)
	if !strings.HasPrefix(lower, "create ") {
		return false
	}
	rest = strings.TrimSpace(rest[len("create"):])
	lower = strings.ToLower(rest)
	for _, keyword := range []string{"or replace ", "temporary ", "temp "} {
		if strings.HasPrefix(lower, keyword) {
			rest = strings.TrimSpace(rest[len(keyword):])
			lower = strings.ToLower(rest)
		}
	}
	return strings.HasPrefix(lower, "table ")
}

// findPartitionClause 在建表语句里找出 PARTITION 子句。
// 找不到返回 nil。
//
// PARTITION 子句位于表定义括号之后（MySQL 语法里它在 table_options 之后），
// 因此只需要在表定义右括号之后找。
func findPartitionClause(sqlStr string) *partitionClause {
	if !isCreateTableStatement(sqlStr) {
		return nil
	}
	open := findTableSpecOpen(sqlStr)
	if open < 0 {
		return nil
	}
	end := matchingParen(sqlStr, open)
	if end < 0 {
		return nil
	}

	rest := sqlStr[end+1:]
	// 只在代码区找 PARTITION BY——注释里的 "PARTITION BY" 不能命中，
	// 否则 `CREATE TABLE t (id INT) /* PARTITION BY HASH(id) */`
	// 会把注释内容当成真的分区子句剥掉。
	idx := scanLiteralRegions(rest).findKeyword("partition by")
	if idx < 0 {
		return nil
	}

	start := end + 1 + idx
	text := strings.TrimSpace(rest[idx:])
	// 去掉结尾分号
	text = strings.TrimSpace(strings.TrimSuffix(text, ";"))
	return &partitionClause{Text: text, Start: start, End: start + len(text)}
}

// removePartitionClause 从建表语句里摘掉 PARTITION 子句。
func removePartitionClause(sqlStr string, clause *partitionClause) string {
	if clause == nil {
		return sqlStr
	}
	return strings.TrimSpace(sqlStr[:clause.Start] + " " + sqlStr[clause.End:])
}

// parsePartitionClause 解析 PARTITION 子句文本，返回分区信息。
//
// 支持的语法：
//
//	PARTITION BY [LINEAR] {HASH|KEY|RANGE|LIST} (expr_or_cols)
//	    [PARTITIONS n]
//	    [SUBPARTITION BY [LINEAR] {HASH|KEY} (expr_or_cols) [SUBPARTITIONS n]]
//	    [ (partition_definition [, ...] ) ]
//
// partition_definition 形如：
//
//	PARTITION name [VALUES LESS THAN ((expr) | MAXVALUE)] [SUBPARTITION (name, ...)]
func parsePartitionClause(text string) *PartitionDetails {
	details := &PartitionDetails{
		Columns:     []string{},
		Definitions: []PartitionDef{},
	}
	rest := strings.TrimSpace(text)
	lower := strings.ToLower(rest)

	if !strings.HasPrefix(lower, "partition by") {
		return details
	}
	rest = strings.TrimSpace(rest[len("partition by"):])
	lower = strings.ToLower(rest)

	// LINEAR
	if strings.HasPrefix(lower, "linear ") {
		details.Linear = true
		rest = strings.TrimSpace(rest[len("linear"):])
		lower = strings.ToLower(rest)
	}

	// 分区类型
	for _, kind := range []string{"hash", "key", "range", "list"} {
		if strings.HasPrefix(lower, kind) {
			details.Type = strings.ToUpper(kind)
			rest = strings.TrimSpace(rest[len(kind):])
			lower = strings.ToLower(rest)
			break
		}
	}
	if details.Type == "" {
		return details
	}

	// RANGE COLUMNS / LIST COLUMNS：MySQL 8 的多列分区类型。
	// 没有这一层时会被当成普通的 RANGE，列清单与分区定义全丢。
	if strings.HasPrefix(lower, "columns ") {
		details.Type += " COLUMNS"
		rest = strings.TrimSpace(rest[len("columns"):])
		lower = strings.ToLower(rest)
	}

	// KEY ALGORITHM = n
	if details.Type == "KEY" && strings.HasPrefix(lower, "algorithm") {
		rest = strings.TrimSpace(rest[len("algorithm"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
		}
		if value, remainder, ok := scanWord(rest); ok {
			details.KeyAlgorithm = value
			rest = strings.TrimSpace(remainder)
			lower = strings.ToLower(rest)
		}
	}

	// 分区键：表达式或列清单
	if strings.HasPrefix(rest, "(") {
		inner, remainder, ok := scanBalanced(rest, '(', ')')
		if ok {
			details.Columns, details.Expr = applyPartitionKey(inner)
			rest = strings.TrimSpace(remainder)
			lower = strings.ToLower(rest)
		}
	}

	// PARTITIONS n
	if strings.HasPrefix(lower, "partitions ") {
		rest = strings.TrimSpace(rest[len("partitions"):])
		if value, remainder, ok := scanWord(rest); ok {
			if number, err := strconv.Atoi(value); err == nil {
				details.NumPartitions = number
			}
			rest = strings.TrimSpace(remainder)
			lower = strings.ToLower(rest)
		}
	}

	// SUBPARTITION BY
	if strings.HasPrefix(lower, "subpartition by") {
		rest = strings.TrimSpace(rest[len("subpartition by"):])
		lower = strings.ToLower(rest)
		sub, remainder := parseSubPartition(rest)
		details.SubPartition = sub
		rest = remainder
		lower = strings.ToLower(rest)
	}

	// 分区定义列表
	if strings.HasPrefix(rest, "(") {
		inner, _, ok := scanBalanced(rest, '(', ')')
		if ok {
			details.Definitions = parsePartitionDefinitions(inner)
		}
	}

	return details
}

// parseSubPartition 解析 SUBPARTITION BY 子句。
func parseSubPartition(text string) (sub *SubPartitionDetail, rest string) {
	details := &SubPartitionDetail{Columns: []string{}}
	rest = strings.TrimSpace(text)
	lower := strings.ToLower(rest)

	if strings.HasPrefix(lower, "linear ") {
		rest = strings.TrimSpace(rest[len("linear"):])
		lower = strings.ToLower(rest)
	}
	for _, kind := range []string{"hash", "key"} {
		if strings.HasPrefix(lower, kind) {
			details.Type = strings.ToUpper(kind)
			rest = strings.TrimSpace(rest[len(kind):])
			lower = strings.ToLower(rest)
			break
		}
	}
	if strings.HasPrefix(rest, "(") {
		inner, remainder, ok := scanBalanced(rest, '(', ')')
		if ok {
			details.Columns, details.Expr = applyPartitionKey(inner)
			rest = strings.TrimSpace(remainder)
			lower = strings.ToLower(rest)
		}
	}
	if strings.HasPrefix(lower, "subpartitions ") {
		rest = strings.TrimSpace(rest[len("subpartitions"):])
		if value, remainder, ok := scanWord(rest); ok {
			if number, err := strconv.Atoi(value); err == nil {
				details.NumPartitions = number
			}
			rest = strings.TrimSpace(remainder)
		}
	}
	return details, rest
}

// applyPartitionKey 判断分区键是列清单还是表达式。
// 返回列清单与表达式文本（两者互斥）。
func applyPartitionKey(inner string) (columns []string, expr string) {
	trimmed := strings.TrimSpace(inner)
	// 含运算符或函数调用的是表达式，否则是列清单
	if strings.ContainsAny(trimmed, "()*/+-") {
		return nil, trimmed
	}
	columns = []string{}
	for _, part := range splitTopLevel(trimmed, ',') {
		if name := strings.TrimSpace(part); name != "" {
			columns = append(columns, name)
		}
	}
	return columns, ""
}

// parsePartitionDefinitions 解析分区定义列表。
//
// 支持 MySQL 的两种分区定义：
//
//	PARTITION p0 VALUES LESS THAN (100)      -- RANGE
//	PARTITION p1 VALUES LESS THAN MAXVALUE    -- RANGE（标准写法，不带括号）
//	PARTITION p0 VALUES IN (1, 2, 3)           -- LIST
//	PARTITION p0 ... SUBPARTITION (s0, s1)     -- 子分区名
func parsePartitionDefinitions(text string) []PartitionDef {
	definitions := []PartitionDef{}
	for _, part := range splitTopLevel(text, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lower := strings.ToLower(part)
		if !strings.HasPrefix(lower, "partition ") {
			continue
		}
		rest := strings.TrimSpace(part[len("partition"):])
		name, rest, ok := scanIdentifier(rest)
		if !ok {
			continue
		}
		def := PartitionDef{Name: name}
		rest = strings.TrimSpace(rest)
		lower = strings.ToLower(rest)

		switch {
		case strings.HasPrefix(lower, "values less than"):
			rest = strings.TrimSpace(rest[len("values less than"):])
			// MySQL 的标准写法是 `VALUES LESS THAN MAXVALUE`——**不带括号**。
			// 带括号的 `VALUES LESS THAN (MAXVALUE)` 反而不是 MySQL 允许的写法，
			// 但两种都要认。
			if strings.HasPrefix(strings.ToLower(rest), "maxvalue") {
				def.Maxvalue = true
				rest = strings.TrimSpace(rest[len("maxvalue"):])
			} else if strings.HasPrefix(rest, "(") {
				inner, remainder, ok := scanBalanced(rest, '(', ')')
				if ok {
					limit := strings.TrimSpace(inner)
					if strings.EqualFold(limit, "maxvalue") {
						def.Maxvalue = true
					} else {
						def.Limit = limit
					}
					rest = strings.TrimSpace(remainder)
				}
			}
		case strings.HasPrefix(lower, "values in"):
			rest = strings.TrimSpace(rest[len("values in"):])
			if strings.HasPrefix(rest, "(") {
				inner, remainder, ok := scanBalanced(rest, '(', ')')
				if ok {
					for _, value := range splitTopLevel(inner, ',') {
						if value = strings.TrimSpace(value); value != "" {
							def.Values = append(def.Values, value)
						}
					}
					rest = strings.TrimSpace(remainder)
				}
			}
		}

		// SUBPARTITION (name, ...)
		if strings.HasPrefix(strings.ToLower(rest), "subpartition ") {
			rest = strings.TrimSpace(rest[len("subpartition "):])
			if strings.HasPrefix(rest, "(") {
				inner, remainder, ok := scanBalanced(rest, '(', ')')
				if ok {
					for _, name := range splitTopLevel(inner, ',') {
						if name = strings.TrimSpace(name); name != "" {
							def.SubPartitions = append(def.SubPartitions, name)
						}
					}
					rest = strings.TrimSpace(remainder)
				}
			}
		}

		definitions = append(definitions, def)
	}
	return definitions
}
