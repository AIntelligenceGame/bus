package main

import (
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/parser"
	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// 定义SQL解析结果结构体
// 用于存储解析后的表名、字段、分组、排序等信息
type SqlParseResult struct {
	Tables     []string       // 表名列表
	Columns    []ColumnInfo   // 字段信息列表
	GroupBy    []string       // GROUP BY 字段
	OrderBy    []OrderByInfo  // ORDER BY 信息
	Limit      *LimitInfo     // LIMIT 信息
	Where      []string       // WHERE 条件
	Having     []string       // HAVING 条件
	SubQueries []SubQueryInfo // 子查询信息
	Unions     []UnionInfo    // UNION 信息
	WithClause []WithInfo     // WITH 子句信息
	JoinInfo   []JoinInfo     // JOIN 信息
}

// 字段信息
type ColumnInfo struct {
	Name       string // 字段名
	Alias      string // 别名
	Table      string // 表名
	Expression string // 完整表达式
}

// ORDER BY 信息
type OrderByInfo struct {
	Column     string // 字段名
	Direction  string // 排序方向 (ASC/DESC)
	Expression string // 完整表达式
}

// LIMIT 信息
type LimitInfo struct {
	Offset     int    // 偏移量
	Limit      int    // 限制数量
	Expression string // 完整表达式
}

// 子查询信息
type SubQueryInfo struct {
	Type    string // 子查询类型 (FROM/WHERE/HAVING等)
	Content string // 子查询内容
	Alias   string // 别名
}

// UNION 信息
type UnionInfo struct {
	Type    string // UNION类型 (UNION/UNION ALL/INTERSECT/EXCEPT)
	Content string // UNION内容
}

// WITH 子句信息
type WithInfo struct {
	Name    string   // CTE名称
	Columns []string // CTE字段
	Content string   // CTE内容
}

// JOIN 信息
type JoinInfo struct {
	Type      string // JOIN类型 (LEFT/RIGHT/INNER/OUTER)
	Table     string // 关联表
	Condition string // 关联条件
}

// 定义一个自定义的监听器，用于处理解析事件
type MyListener struct {
	*parser.BaseMySqlParserListener
	tableNames map[string]struct{} // 用于存储表名称的映射
	columns    []ColumnInfo        // 用于存储字段列的切片
	groupBy    []string            // 存储 group by 字段
	orderBy    []OrderByInfo       // 存储 order by 字段
	limit      *LimitInfo          // 存储 limit 信息
	where      []string            // 存储 where 条件
	having     []string            // 存储 having 条件
	subQueries []SubQueryInfo      // 存储子查询
	unions     []UnionInfo         // 存储 union 信息
	withClause []WithInfo          // 存储 with 子句
	joinInfo   []JoinInfo          // 存储 join 信息
	result     *SqlParseResult     // 解析结果结构体
}

// 重写EnterTableName方法，处理表名称
func (l *MyListener) EnterTableName(ctx *parser.TableNameContext) {
	tableName := strings.ToLower(ctx.GetText())
	l.tableNames[tableName] = struct{}{}
}

func (l *MyListener) GetTableNames() []string {
	arr := make([]string, 0)
	if l.tableNames != nil {
		for k := range l.tableNames {
			arr = append(arr, k)
		}
	}
	return arr
}

// 处理 SELECT 字段
func (l *MyListener) EnterSelectElements(ctx *parser.SelectElementsContext) {
	// 清空之前的列
	l.columns = make([]ColumnInfo, 0)
	elements := ctx.AllSelectElement()
	for _, element := range elements {
		columnText := element.GetText()
		columnInfo := l.parseColumnInfo(columnText)
		l.columns = append(l.columns, columnInfo)
	}
	if l.result != nil {
		l.result.Columns = append(l.result.Columns, l.columns...)
	}
}

// 解析字段信息
func (l *MyListener) parseColumnInfo(columnText string) ColumnInfo {
	columnInfo := ColumnInfo{
		Expression: strings.TrimSpace(columnText),
	}

	// 处理字段别名（支持有 AS 和没有 AS 的情况）
	// 1. 先检查是否有 AS 关键字（注意大小写）
	upperText := strings.ToUpper(columnText)
	asIndex := strings.Index(upperText, " AS ")

	if asIndex != -1 {
		// 找到 AS 关键字，分离表达式和别名
		expression := strings.TrimSpace(columnText[:asIndex])
		alias := strings.TrimSpace(columnText[asIndex+4:]) // 4 = len(" AS ")

		columnInfo.Expression = expression
		columnInfo.Alias = alias
		l.parseFieldAndTable(expression, &columnInfo)
	} else {
		// 2. 没有 AS 关键字，尝试识别别名
		// 检查是否包含空格，可能表示有别名
		trimmedText := strings.TrimSpace(columnText)
		words := strings.Fields(trimmedText)

		if len(words) >= 2 {
			// 最后一个单词可能是别名
			potentialAlias := words[len(words)-1]
			// 检查别名是否合法（不以数字开头，不包含特殊字符等）
			if l.isValidAlias(potentialAlias) {
				// 前面的部分作为表达式
				expression := strings.Join(words[:len(words)-1], " ")
				columnInfo.Expression = expression
				columnInfo.Alias = potentialAlias
				l.parseFieldAndTable(expression, &columnInfo)
			} else {
				// 没有有效别名，整个作为表达式
				l.parseFieldAndTable(trimmedText, &columnInfo)
			}
		} else {
			// 只有一个单词，没有别名
			l.parseFieldAndTable(trimmedText, &columnInfo)
		}
	}

	return columnInfo
}

// 解析字段名和表名
func (l *MyListener) parseFieldAndTable(expression string, columnInfo *ColumnInfo) {
	// 移除可能的括号和引号
	expression = strings.Trim(expression, "`'\"")

	// 检查是否包含表名（有 . 分隔符）
	if strings.Contains(expression, ".") {
		parts := strings.Split(expression, ".")
		if len(parts) == 2 {
			columnInfo.Table = strings.TrimSpace(parts[0])
			columnInfo.Name = strings.TrimSpace(parts[1])
		} else if len(parts) > 2 {
			// 处理多级表名，如 database.table.column
			columnInfo.Table = strings.Join(parts[:len(parts)-1], ".")
			columnInfo.Name = strings.TrimSpace(parts[len(parts)-1])
		}
	} else {
		// 没有表名，整个作为字段名
		columnInfo.Name = expression
	}
}

// 检查是否为有效的别名
func (l *MyListener) isValidAlias(alias string) bool {
	if alias == "" {
		return false
	}

	// 移除引号
	alias = strings.Trim(alias, "`'\"")

	// 检查是否以数字开头
	if len(alias) > 0 && alias[0] >= '0' && alias[0] <= '9' {
		return false
	}

	// 检查是否包含明显的 SQL 关键字
	upperAlias := strings.ToUpper(alias)
	sqlKeywords := []string{"SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "ASC", "DESC", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MAX", "MIN"}
	for _, keyword := range sqlKeywords {
		if upperAlias == keyword {
			return false
		}
	}

	return true
}

// 处理 GROUP BY
func (l *MyListener) EnterGroupByClause(ctx *parser.GroupByClauseContext) {
	groupByText := ctx.GetText()
	// 移除 "GROUP BY" 关键字
	groupByText = strings.TrimPrefix(strings.ToUpper(groupByText), "GROUP BY")
	groupByText = strings.TrimSpace(groupByText)

	fields := strings.Split(groupByText, ",")
	for _, f := range fields {
		field := strings.TrimSpace(f)
		if field != "" {
			l.groupBy = append(l.groupBy, field)
		}
	}
	if l.result != nil {
		l.result.GroupBy = append(l.result.GroupBy, l.groupBy...)
	}
}

// 处理 ORDER BY
func (l *MyListener) EnterOrderByClause(ctx *parser.OrderByClauseContext) {
	orderByText := ctx.GetText()
	// 移除 "ORDER BY" 关键字
	orderByText = strings.TrimPrefix(strings.ToUpper(orderByText), "ORDER BY")
	orderByText = strings.TrimSpace(orderByText)

	fields := strings.Split(orderByText, ",")
	for _, f := range fields {
		field := strings.TrimSpace(f)
		if field != "" {
			orderInfo := l.parseOrderByInfo(field)
			l.orderBy = append(l.orderBy, orderInfo)
		}
	}
	if l.result != nil {
		l.result.OrderBy = append(l.result.OrderBy, l.orderBy...)
	}
}

// 解析 ORDER BY 信息
func (l *MyListener) parseOrderByInfo(field string) OrderByInfo {
	orderInfo := OrderByInfo{
		Expression: field,
	}

	// 检查是否有排序方向
	upperField := strings.ToUpper(field)
	if strings.HasSuffix(upperField, " ASC") {
		orderInfo.Column = strings.TrimSpace(strings.TrimSuffix(field, " ASC"))
		orderInfo.Direction = "ASC"
	} else if strings.HasSuffix(upperField, " DESC") {
		orderInfo.Column = strings.TrimSpace(strings.TrimSuffix(field, " DESC"))
		orderInfo.Direction = "DESC"
	} else {
		orderInfo.Column = field
		orderInfo.Direction = "ASC" // 默认升序
	}

	return orderInfo
}

// 处理 LIMIT
func (l *MyListener) EnterLimitClause(ctx *parser.LimitClauseContext) {
	limitText := ctx.GetText()
	limitInfo := l.parseLimitInfo(limitText)
	l.limit = &limitInfo
	if l.result != nil {
		l.result.Limit = &limitInfo
	}
}

// 解析 LIMIT 信息
func (l *MyListener) parseLimitInfo(limitText string) LimitInfo {
	limitInfo := LimitInfo{
		Expression: limitText,
	}

	// 移除 "LIMIT" 关键字
	limitText = strings.TrimPrefix(strings.ToUpper(limitText), "LIMIT")
	limitText = strings.TrimSpace(limitText)

	// 处理 "LIMIT offset, count" 格式
	if strings.Contains(limitText, ",") {
		parts := strings.Split(limitText, ",")
		if len(parts) == 2 {
			fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &limitInfo.Offset)
			fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &limitInfo.Limit)
		}
	} else {
		// 只有 count 的情况
		fmt.Sscanf(limitText, "%d", &limitInfo.Limit)
	}

	return limitInfo
}

// 处理 HAVING 子句
func (l *MyListener) EnterHavingClause(ctx *parser.HavingClauseContext) {
	havingText := ctx.GetText()
	// 移除 "HAVING" 关键字
	havingText = strings.TrimPrefix(strings.ToUpper(havingText), "HAVING")
	havingText = strings.TrimSpace(havingText)

	if havingText != "" {
		l.having = append(l.having, havingText)
	}
	if l.result != nil {
		l.result.Having = append(l.result.Having, l.having...)
	}
}

// 处理 UNION
func (l *MyListener) EnterUnionStatement(ctx *parser.UnionStatementContext) {
	unionText := ctx.GetText()
	unionInfo := UnionInfo{
		Content: unionText,
		Type:    "UNION", // 可以根据具体关键字确定类型
	}
	l.unions = append(l.unions, unionInfo)
	if l.result != nil {
		l.result.Unions = append(l.result.Unions, unionInfo)
	}
}

// 处理 WITH 子句
func (l *MyListener) EnterWithClause(ctx *parser.WithClauseContext) {
	withText := ctx.GetText()
	withInfo := WithInfo{
		Content: withText,
	}
	l.withClause = append(l.withClause, withInfo)
	if l.result != nil {
		l.result.WithClause = append(l.result.WithClause, withInfo)
	}
}

// 处理 JOIN
// func (l *MyListener) EnterJoinClause(ctx *parser.JoinClauseContext) {
// 	joinText := ctx.GetText()
// 	joinInfo := JoinInfo{
// 		Type:      "JOIN", // 可以根据上下文确定类型
// 		Condition: joinText,
// 	}
// 	l.joinInfo = append(l.joinInfo, joinInfo)
// 	if l.result != nil {
// 		l.result.JoinInfo = append(l.result.JoinInfo, joinInfo)
// 	}
// }

// 处理子查询
// func (l *MyListener) EnterSubquery(ctx *parser.SubqueryContext) {
// 	subQueryText := ctx.GetText()
// 	subQueryInfo := SubQueryInfo{
// 		Content: subQueryText,
// 		Type:    "SUBQUERY", // 可以根据上下文确定具体类型
// 	}
// 	l.subQueries = append(l.subQueries, subQueryInfo)
// 	if l.result != nil {
// 		l.result.SubQueries = append(l.result.SubQueries, subQueryInfo)
// 	}
// }

// 处理 WHERE 子句
// func (l *MyListener) EnterWhereClause(ctx *parser.WhereClauseContext) {
// 	whereText := ctx.GetText()
// 	// 移除 "WHERE" 关键字
// 	whereText = strings.TrimPrefix(strings.ToUpper(whereText), "WHERE")
// 	whereText = strings.TrimSpace(whereText)
//
// 	if whereText != "" {
// 		l.where = append(l.where, whereText)
// 	}
// 	if l.result != nil {
// 		l.result.Where = append(l.result.Where, l.where...)
// 	}
// }

// github.com/akito0107/xsqlparser 支持with 语法
func main() {
	// 创建一个ANTLR输入流
	// 测试各种别名情况
	input := antlr.NewInputStream("SELECT t1.id AS user_id, t1.name username, t2.age, COUNT(*) total_count FROM users t1 LEFT JOIN profiles t2 ON t1.id = t2.user_id GROUP BY t1.id ORDER BY t1.name ASC LIMIT 10")

	// 创建一个词法分析器
	lexer := parser.NewMySqlLexer(input)

	// 创建一个词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建一个语法分析器
	p := parser.NewMySqlParser(stream)

	// 创建一个解析结果结构体
	result := &SqlParseResult{}
	// 创建一个自定义的监听器，并初始化表名称映射
	listener := &MyListener{
		tableNames: make(map[string]struct{}),
		columns:    make([]ColumnInfo, 0),
		groupBy:    make([]string, 0),
		orderBy:    make([]OrderByInfo, 0),
		where:      make([]string, 0),
		having:     make([]string, 0),
		subQueries: make([]SubQueryInfo, 0),
		unions:     make([]UnionInfo, 0),
		withClause: make([]WithInfo, 0),
		joinInfo:   make([]JoinInfo, 0),
		result:     result,
	}

	// 创建一个语法树遍历器，并注册监听器
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Root())

	// 获取解析到的表名称并打印
	result.Tables = listener.GetTableNames()

	// 格式化输出结果
	fmt.Printf("\n=== SQL 解析结果 ===\n")
	fmt.Printf("表名: %v\n", result.Tables)
	fmt.Printf("字段数量: %d\n", len(result.Columns))
	for i, col := range result.Columns {
		fmt.Printf("  字段%d: %s (别名: %s, 表: %s)\n", i+1, col.Name, col.Alias, col.Table)
	}
	fmt.Printf("GROUP BY: %v\n", result.GroupBy)
	fmt.Printf("ORDER BY: %v\n", result.OrderBy)
	if result.Limit != nil {
		fmt.Printf("LIMIT: %+v\n", result.Limit)
	}
	if len(result.Where) > 0 {
		fmt.Printf("WHERE: %v\n", result.Where)
	}
	if len(result.Having) > 0 {
		fmt.Printf("HAVING: %v\n", result.Having)
	}
	if len(result.SubQueries) > 0 {
		fmt.Printf("子查询数量: %d\n", len(result.SubQueries))
	}
	if len(result.Unions) > 0 {
		fmt.Printf("UNION数量: %d\n", len(result.Unions))
	}
	if len(result.WithClause) > 0 {
		fmt.Printf("WITH子句数量: %d\n", len(result.WithClause))
	}
	if len(result.JoinInfo) > 0 {
		fmt.Printf("JOIN数量: %d\n", len(result.JoinInfo))
	}
}
