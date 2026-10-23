package sqltree

import (
	"fmt"
	"strconv"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// SQLStatement represents a parsed SQL statement with its type and details.
type SQLStatement struct {
	Type    string `json:"type"`
	Details any    `json:"details"`
}

// SelectDetails represents the details of a SELECT statement.
type SelectDetails struct {
	SelectedExprs []string `json:"selected_exprs,omitempty"`
	Distinct      bool     `json:"distinct,omitempty"`
	From          string   `json:"from,omitempty"`
	Where         string   `json:"where,omitempty"`
	GroupBy       []string `json:"group_by,omitempty"`
	// WithRollup 表示 GROUP BY ... WITH ROLLUP（MySQL 8 之前的老语法）。
	WithRollup bool      `json:"with_rollup,omitempty"`
	Having     string    `json:"having,omitempty"`
	OrderBy    []string  `json:"order_by,omitempty"`
	Limit      int       `json:"limit,omitempty"`
	Offset     int       `json:"offset,omitempty"`
	CTEs       []CTEInfo `json:"ctes,omitempty"`
	SetOp      string    `json:"set_op,omitempty"`
	// SetOpBranches 是集合运算（UNION/INTERSECT/EXCEPT）左右两侧的完整细节。
	//
	// 没有它的话，`SELECT a FROM t1 WHERE x=1 UNION SELECT b FROM t2 WHERE y=2`
	// 只会剩下投影列，两个分支的 FROM/WHERE 全部丢失。
	SetOpBranches []SelectDetails `json:"set_op_branches,omitempty"`
}

// UpdateDetails represents the details of an UPDATE statement.
type UpdateDetails struct {
	Table      string   `json:"table"`
	From       string   `json:"from,omitempty"`
	SetClauses []string `json:"set_clauses"`
	Where      string   `json:"where,omitempty"`
	OrderBy    []string `json:"order_by,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

// InsertDetails represents the details of an INSERT statement.
type InsertDetails struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns,omitempty"`
	Values  []string `json:"values,omitempty"`
	// Priority 是插入优先级：low_priority / high_priority / delayed。
	// vitess 不支持这几个关键字，解析前剥离、解析后补回。
	Priority    string         `json:"priority,omitempty"`
	OnDuplicate []string       `json:"on_duplicate,omitempty"`
	SelectData  *SelectDetails `json:"select_data,omitempty"`
}

// DeleteDetails represents the details of a DELETE statement.
type DeleteDetails struct {
	Table   string   `json:"table"`
	From    string   `json:"from,omitempty"`
	Where   string   `json:"where,omitempty"`
	OrderBy []string `json:"order_by,omitempty"`
	Limit   int      `json:"limit,omitempty"`
	Offset  int      `json:"offset,omitempty"`
}

// parseSQL 解析 SQL 并返回结构化结果。
//
// 分派策略（**AST 层是唯一的输出源**）：
//   - AST 层（vitess 语法层）：纯语法解析，不需要 catalog，能解析任意语句，
//     包括 catalog 里根本不存在的表。这是主力路径。
//   - 手写解析器：vitess 没有语法的语句（CREATE/DROP FUNCTION、DO、
//     OPTIMIZE/CHECK/CHECKSUM/REPAIR TABLE、SET PASSWORD）。
//   - plan 层（analyzer 语义层）：**只作为可选增强**——在传入了 catalog 时
//     把 SELECT * 展开成具体列。这是 AST 层唯一补不上的信息。
//     没有 catalog 时完全不用它，engine 传 nil 即可。
//
// 为什么不让 plan 层做主力：它强依赖 catalog（表必须真实存在且有主键），
// 而真实场景是用户随手给一条 SQL，解析器既不知道表结构也没权限查库；
// 而且 plan 层的输出更粗——会丢掉 DISTINCT / OFFSET / CTE / 集合运算，
// JOIN 的 from 还会渲染成树形 dump。输出格式必须与表是否存在无关。
// Parse 纯语法解析一条 SQL，返回结构化结果。
//
// 这是最常用的入口：**不需要 catalog、不需要表结构、不需要任何权限**，
// 用户给什么 SQL 就解析什么——包括表根本不存在的语句。
//
//	stmt, err := sqltree.Parse("SELECT id FROM t WHERE a = 1")
//	if err != nil { ... }
//	fmt.Println(stmt.Type)              // "Select"
//	fmt.Println(stmt.Details)          // *sqltree.SelectDetails
//
// 需要更多能力时用 ParseSQL（可传 engine 打开 SELECT * 展开等增强）。
func Parse(sqlStr string) (*SQLStatement, error) {
	return ParseSQL(nil, nil, sqlStr)
}

func ParseSQL(ctx *sql.Context, engine *sqle.Engine, sqlStr string) (*SQLStatement, error) {
	// 归一化：去掉注释。
	//
	// 注释可以出现在任何位置、任何写法（-- / # / /* */ / /*! */），
	// MySQL 执行时不会把它们当语句，所以解析结果里也不该有。
	// 统一在入口处理一次，下游的 vitess 与自研扫描器看到的都是同一份干净文本。
	sqlStr = stripSQLComments(sqlStr)

	// plan 层（语义分析）必须要有 ctx：go-mysql-server 内部会调用
	// sql.LoadSqlMode(ctx)，ctx 为 nil 时直接空指针 panic。
	// 调用方若只想用 AST 层，传 nil ctx 即可；这里直接把 engine 也一并忽略，
	// 避免"传了 engine 但没传 ctx"这种半截调用把进程搞崩。
	if ctx == nil {
		engine = nil
	}

	// vitess 语法层不支持的语句（CREATE/DROP FUNCTION、DO、
	// OPTIMIZE/CHECK/CHECKSUM/REPAIR TABLE、SET PASSWORD）走自研解析器。
	//
	// 注意：多语句脚本里的每一条也走同一套（见 parseSingleStatement），
	// 保证"单独解析"与"放进脚本里"行为一致。
	if stmt := parseHandWritten(sqlStr); stmt != nil {
		return stmt, nil
	}

	// 多语句：vitess 的 Parse 只接受单条语句
	if statements, ok := parseMultiStatement(sqlStr); ok {
		return statements, nil
	}

	ast, astCtx, astErr := parseAST(sqlStr)
	var astStmt *ASTStatement
	if astErr == nil {
		astStmt = extractASTStatement(ast, astCtx)
	}
	if astStmt != nil {
		stmt := &SQLStatement{Type: astStmt.Type, Details: astStmt.Details}
		// 有 catalog 时用 plan 层补上 SELECT * 的展开结果；没有 catalog 就跳过
		if engine != nil {
			enrichStarColumns(ctx, engine, sqlStr, stmt)
		}
		return stmt, nil
	}

	// AST 层不支持，回退到 plan 层（需要 catalog）
	if engine == nil {
		return nil, fmt.Errorf("无法解析该语句：AST 层不支持，且未提供 catalog 无法做语义分析")
	}
	stmt, err := parseViaPlan(ctx, engine, sqlStr)
	if err != nil {
		if astErr != nil {
			return nil, fmt.Errorf("AST 解析失败: %v; 语义分析失败: %v", astErr, err)
		}
		return nil, err
	}
	return stmt, nil
}

// enrichStarColumns 用 plan 层把 SELECT * 展开成具体的列。
//
// 这是 AST 层唯一补不上的信息：AST 是纯语法解析，不知道表里有哪些列。
// 只有传入了 catalog 时才做；没有 catalog 时保持 "*" 原样。
//
// 只处理"投影列表恰好只有一个 *"的情况——多列混合时 plan 层的展开结果
// 与 AST 的投影项对不上位，硬拼会错位，宁可不动。
func enrichStarColumns(ctx *sql.Context, engine *sqle.Engine, sqlStr string, stmt *SQLStatement) {
	// 先看有没有 "*" 要展开，没有就别去跑语义分析了
	var target *SelectDetails
	switch details := stmt.Details.(type) {
	case *SelectDetails:
		target = details
	case *InsertDetails:
		target = details.SelectData
	}
	if target == nil || !hasStarProjection(target) {
		return
	}

	planStmt, err := parseViaPlan(ctx, engine, sqlStr)
	if err != nil || planStmt == nil {
		return
	}
	switch details := stmt.Details.(type) {
	case *SelectDetails:
		if planDetails, ok := planStmt.Details.(*SelectDetails); ok {
			applyStarExpansion(details, planDetails)
		}
	case *InsertDetails:
		// INSERT ... SELECT 的子查询
		if details.SelectData == nil {
			return
		}
		planDetails, ok := planStmt.Details.(*InsertDetails)
		if !ok || planDetails.SelectData == nil {
			return
		}
		applyStarExpansion(details.SelectData, planDetails.SelectData)
	}
}

// hasStarProjection 判断投影列表里是否只有一个 "*"。
func hasStarProjection(details *SelectDetails) bool {
	return len(details.SelectedExprs) == 1 && details.SelectedExprs[0] == "*"
}

// applyStarExpansion 把 details 里唯一的 "*" 换成 plan 层展开出的列。
func applyStarExpansion(details *SelectDetails, planDetails *SelectDetails) {
	if len(details.SelectedExprs) != 1 || details.SelectedExprs[0] != "*" {
		return
	}
	if len(planDetails.SelectedExprs) == 0 {
		return
	}
	details.SelectedExprs = planDetails.SelectedExprs
}

// parseViaPlan 走 plan 层（语义分析）解析 SQL。
func parseViaPlan(ctx *sql.Context, engine *sqle.Engine, sqlStr string) (*SQLStatement, error) {
	analyzedNode, err := engine.AnalyzeQuery(ctx, sqlStr)
	if err != nil {
		return nil, err
	}

	var sqlStmt SQLStatement
	switch node := analyzedNode.(type) {
	case *plan.Update:
		sqlStmt = SQLStatement{
			Type:    "Update",
			Details: extractUpdateDetails(node),
		}
	case *plan.InsertInto:
		sqlStmt = SQLStatement{
			Type:    "Insert",
			Details: extractInsertDetails(node),
		}
	case *plan.DeleteFrom:
		sqlStmt = SQLStatement{
			Type:    "Delete",
			Details: extractDeleteDetails(node),
		}
	default:
		sqlStmt = planNodeToStatement(analyzedNode)
	}

	return &sqlStmt, nil
}

// planNodeToStatement 把 plan 层的节点转成结构化描述。
//
// 这是 AST 层不支持、只能回退到 plan 层时的最后一档。
//
// 关键点：**不能一律标成 "Select"**。旧实现把除 DML 之外的所有节点
// 都标成 Select，于是 `CREATE DATABASE d` 在有 catalog 时会返回
// {"type":"Select","details":{}}——调用方会把 DDL 当查询处理，
// 而且同一条语句"无 catalog 报错、有 catalog 变成 Select"，行为不一致。
func planNodeToStatement(node sql.Node) SQLStatement {
	switch n := node.(type) {
	case *plan.CreateDB:
		return SQLStatement{Type: "CreateDatabase", Details: map[string]string{
			"database": n.DbName,
		}}
	case *plan.CreateTable:
		return SQLStatement{Type: "CreateTable", Details: map[string]string{
			"table": planNodeName(n),
		}}
	case *plan.DropTable:
		return SQLStatement{Type: "Drop", Details: map[string]string{
			"kind": "table",
		}}
	case *plan.CreateView:
		return SQLStatement{Type: "CreateView", Details: map[string]string{
			"name": n.Name,
		}}
	case *plan.CreateIndex:
		return SQLStatement{Type: "CreateIndex", Details: map[string]string{
			"name":  n.Name,
			"table": planNodeName(n.Table),
		}}
	case *plan.DropIndex:
		return SQLStatement{Type: "DropIndex", Details: map[string]string{
			"name":  n.Name,
			"table": planNodeName(n.Table),
		}}
	case *plan.Truncate:
		return SQLStatement{Type: "Truncate", Details: map[string]string{
			"table": planNodeName(n.Child),
		}}
	case *plan.ShowTables:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "tables",
		}}
	case *plan.ShowDatabases:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "databases",
		}}
	case *plan.ShowColumns:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "columns",
		}}
	case *plan.ShowVariables:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "variables",
		}}
	case *plan.ShowStatus:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "status",
		}}
	case *plan.ShowCreateTable:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show":  "create table",
			"table": planNodeName(n.Child),
		}}
	case *plan.ShowCreateDatabase:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "create database",
		}}
	case *plan.ShowCreateProcedure:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "create procedure",
			"name": n.ProcedureName,
		}}
	case *plan.ShowCreateTrigger:
		return SQLStatement{Type: "Show", Details: map[string]string{
			"show": "create trigger",
			"name": n.TriggerName,
		}}
	case *plan.Set:
		return SQLStatement{Type: "Set", Details: map[string]string{
			"set": node.String(),
		}}
	case *plan.Use:
		name := ""
		if db := n.Database(); db != nil {
			name = db.Name()
		}
		return SQLStatement{Type: "Use", Details: map[string]string{
			"database": name,
		}}
	case *plan.StartTransaction:
		return SQLStatement{Type: "Begin", Details: map[string]string{}}
	case *plan.Commit:
		return SQLStatement{Type: "Commit", Details: map[string]string{}}
	case *plan.Rollback:
		return SQLStatement{Type: "Rollback", Details: map[string]string{}}
	default:
		// 认不出来的节点：当作查询处理，但保留原始文本，
		// 至少让调用方知道语句原文，而不是拿到一个空壳。
		details := extractSelectDetails(node)
		if details == nil {
			details = &SelectDetails{}
		}
		return SQLStatement{Type: "Select", Details: details}
	}
}

// planNodeName 尽力从 plan 节点里取表名/库名。
//
// go-mysql-server 的很多节点把名字放在未导出的字段里，
// 拿不到就退回节点的字符串表示（至少不是空的）。
func planNodeName(node sql.Node) string {
	if node == nil {
		return ""
	}
	if table, ok := node.(*plan.ResolvedTable); ok {
		return table.Name()
	}
	return node.String()
}

func extractSelectDetails(node sql.Node) *SelectDetails {
	visitor := &selectVisitor{
		Details: SelectDetails{},
		found:   make(map[string]bool),
	}
	visitNode(node, visitor)
	// 过滤掉空的 selected_exprs
	var filteredExprs []string
	for _, expr := range visitor.Details.SelectedExprs {
		if expr != "" {
			filteredExprs = append(filteredExprs, expr)
		}
	}
	visitor.Details.SelectedExprs = filteredExprs
	return &visitor.Details
}

func extractUpdateDetails(node *plan.Update) *UpdateDetails {
	details := &UpdateDetails{
		SetClauses: []string{},
		OrderBy:    []string{},
	}

	// 检查是否是 UPDATE with JOIN
	if updateJoin, ok := node.Child.(*plan.UpdateJoin); ok {
		// 获取所有要更新的表
		var tables []string
		for name := range updateJoin.Updaters {
			tables = append(tables, name)
		}
		details.Table = strings.Join(tables, ", ")

		// 遍历获取 SET, WHERE 等
		visitor := &updateVisitor{Details: details}
		visitUpdateNode(updateJoin, visitor)
	} else {
		visitor := &updateVisitor{Details: details}
		visitUpdateNode(node, visitor)
	}

	return details
}

type updateVisitor struct {
	Details *UpdateDetails
}

func visitUpdateNode(node sql.Node, visitor *updateVisitor) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *plan.UpdateJoin:
		// UPDATE with JOIN - SET 子句可能需要从其他地方获取
		// 先继续遍历
		visitUpdateNode(n.Child, visitor)
	case *plan.UpdateSource:
		for _, expr := range n.UpdateExprs {
			visitor.Details.SetClauses = append(visitor.Details.SetClauses, normalizeSQL(expr.String()))
		}
		visitUpdateNode(n.Child, visitor)
	case *plan.ResolvedTable:
		if visitor.Details.Table == "" {
			visitor.Details.Table = n.Name()
		}
	case *plan.TableAlias:
		if visitor.Details.Table == "" {
			visitor.Details.Table = n.Name()
		}
	case *plan.Filter:
		visitor.Details.Where = normalizeSQL(n.Expression.String())
		visitUpdateNode(n.Child, visitor)
	case *plan.Sort:
		for _, field := range n.SortFields {
			visitor.Details.OrderBy = append(visitor.Details.OrderBy, field.Column.String())
		}
		visitUpdateNode(n.Child, visitor)
	case *plan.Limit:
		if limitExpr, ok := n.Limit.(*expression.Literal); ok {
			if rowCount, err := strconv.Atoi(fmt.Sprintf("%v", limitExpr.Value())); err == nil {
				visitor.Details.Limit = rowCount
			}
		}
		visitUpdateNode(n.Child, visitor)
	default:
		for _, child := range node.Children() {
			visitUpdateNode(child, visitor)
		}
	}
}

func extractInsertDetails(node *plan.InsertInto) *InsertDetails {
	details := &InsertDetails{
		Columns: []string{},
		Values:  []string{},
	}

	for _, col := range node.ColumnNames {
		details.Columns = append(details.Columns, col)
	}

	visitor := &tableVisitor{}
	visitTableNode(node.Destination, visitor)
	details.Table = visitor.tableName

	if node.Source != nil {
		if node.LiteralValueSource {
			valuesVisitor := &valuesVisitor{}
			findValues(node.Source, valuesVisitor)
			details.Values = valuesVisitor.values
		} else {
			details.SelectData = extractSelectDetails(node.Source)
		}
	}

	return details
}

type tableVisitor struct {
	tableName string
}

type valuesVisitor struct {
	values []string
}

func findValues(node sql.Node, visitor *valuesVisitor) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *plan.Values:
		for _, tuple := range n.ExpressionTuples {
			var tupleValues []string
			for _, expr := range tuple {
				tupleValues = append(tupleValues, expr.String())
			}
			visitor.values = append(visitor.values, "("+strings.Join(tupleValues, ", ")+")")
		}
	default:
		for _, child := range node.Children() {
			findValues(child, visitor)
		}
	}
}

func visitTableNode(node sql.Node, visitor *tableVisitor) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *plan.InsertDestination:
		visitor.tableName = n.DestinationName
	case *plan.ResolvedTable:
		if visitor.tableName == "" {
			visitor.tableName = n.Name()
		}
	case *plan.TableAlias:
		if visitor.tableName == "" {
			visitor.tableName = n.Name()
		}
	default:
		for _, child := range node.Children() {
			visitTableNode(child, visitor)
		}
	}
}

func extractDeleteDetails(node *plan.DeleteFrom) *DeleteDetails {
	details := &DeleteDetails{
		OrderBy: []string{},
	}

	// 获取删除的目标表
	var tables []string
	if node.HasExplicitTargets() {
		// 多表 DELETE - explicitTargets 包含所有要删除的表
		for _, target := range node.GetDeleteTargets() {
			if t, ok := target.(*plan.ResolvedTable); ok {
				tables = append(tables, t.Name())
			} else if a, ok := target.(*plan.TableAlias); ok {
				tables = append(tables, a.Name())
			}
		}
	} else {
		// 单表 DELETE
		visitor := &tableVisitor{}
		visitTableNode(node.Child, visitor)
		if visitor.tableName != "" {
			tables = append(tables, visitor.tableName)
		}
	}
	details.Table = strings.Join(tables, ", ")

	deleteVisitor := &deleteVisitor{Details: details}
	visitDeleteNode(node, deleteVisitor)

	return details
}

type deleteVisitor struct {
	Details *DeleteDetails
}

// normalizeSQL 还原分析器优化痕迹，还原为原始 SQL 语义。
// 目前 go-mysql-server 会把 `IN (...)` 优化成 `HashInTuple`，
// 其 String() 输出带 "HASH " 前缀，这里去掉以贴近原始 SQL。
func normalizeSQL(s string) string {
	return strings.ReplaceAll(s, " HASH IN ", " IN ")
}

func visitDeleteNode(node sql.Node, visitor *deleteVisitor) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *plan.Filter:
		visitor.Details.Where = normalizeSQL(n.Expression.String())
		visitDeleteNode(n.Child, visitor)
	case *plan.Sort:
		for _, field := range n.SortFields {
			visitor.Details.OrderBy = append(visitor.Details.OrderBy, field.Column.String())
		}
		visitDeleteNode(n.Child, visitor)
	case *plan.Limit:
		if limitExpr, ok := n.Limit.(*expression.Literal); ok {
			if rowCount, err := strconv.Atoi(fmt.Sprintf("%v", limitExpr.Value())); err == nil {
				visitor.Details.Limit = rowCount
			}
		}
		visitDeleteNode(n.Child, visitor)
	default:
		for _, child := range node.Children() {
			visitDeleteNode(child, visitor)
		}
	}
}

// selectVisitor 访问者模式收集 SELECT 语句的各部分
type selectVisitor struct {
	Details SelectDetails
	found   map[string]bool
}

const (
	foundProject = "project"
	foundGroupBy = "groupby"
	foundHaving  = "having"
	foundOrderBy = "orderby"
	foundLimit   = "limit"
)

func visitNode(node sql.Node, visitor *selectVisitor) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *plan.Project:
		if !visitor.found[foundProject] {
			visitor.found[foundProject] = true
			for _, expr := range n.Projections {
				visitor.Details.SelectedExprs = append(visitor.Details.SelectedExprs, expr.String())
			}
		}
		visitNode(n.Child, visitor)

	case *plan.Filter:
		if visitor.Details.Where == "" {
			visitor.Details.Where = normalizeSQL(n.Expression.String())
		}
		visitNode(n.Child, visitor)

	case *plan.GroupBy:
		if !visitor.found[foundGroupBy] {
			visitor.found[foundGroupBy] = true
			for _, expr := range n.GroupByExprs {
				visitor.Details.GroupBy = append(visitor.Details.GroupBy, expr.String())
			}
		}
		visitNode(n.Child, visitor)

	case *plan.Having:
		if !visitor.found[foundHaving] {
			visitor.found[foundHaving] = true
			visitor.Details.Having = n.Cond.String()
		}
		visitNode(n.Child, visitor)

	case *plan.Sort:
		if !visitor.found[foundOrderBy] {
			visitor.found[foundOrderBy] = true
			for _, field := range n.SortFields {
				visitor.Details.OrderBy = append(visitor.Details.OrderBy, field.Column.String())
			}
		}
		visitNode(n.Child, visitor)

	case *plan.Limit:
		if !visitor.found[foundLimit] {
			if limitExpr, ok := n.Limit.(*expression.Literal); ok {
				if rowCount, err := strconv.Atoi(fmt.Sprintf("%v", limitExpr.Value())); err == nil {
					visitor.found[foundLimit] = true
					visitor.Details.Limit = rowCount
				}
			}
		}
		visitNode(n.Child, visitor)

	case *plan.ResolvedTable:
		if visitor.Details.From == "" {
			visitor.Details.From = n.Name()
		}

	case *plan.TableAlias:
		if visitor.Details.From == "" {
			visitor.Details.From = n.Name()
		}

	case *plan.SubqueryAlias:
		if visitor.Details.From == "" {
			visitor.Details.From = n.Name()
		}
		// 继续遍历子查询内部
		visitNode(n.Child, visitor)

	case *plan.RecursiveCte:
		// 递归 CTE 节点 - 遍历其子节点
		visitNode(n.Left(), visitor)
		visitNode(n.Right(), visitor)

	case *plan.JoinNode:
		if visitor.Details.From == "" {
			visitor.Details.From = node.String()
		}
		visitNode(n.Left(), visitor)
		visitNode(n.Right(), visitor)

	case *plan.Distinct:
		// DISTINCT 需要遍历子节点
		visitNode(n.Child, visitor)

	case *plan.Offset:
		// OFFSET 通常跟在 LIMIT 后面，继续遍历
		visitNode(n.Child, visitor)

	case *plan.SetOp:
		// UNION, UNION ALL, INTERSECT, EXCEPT
		// SetOpType: 0=Union, 1=Intersect, 2=Except
		// 需要收集左右两个子查询的列信息，但需要分别去重
		// 先遍历左子节点
		visitNode(n.Left(), visitor)
		// 遍历右子节点前，需要重新允许收集投影（因为是不同分支）
		savedFoundProject := visitor.found[foundProject]
		visitor.found[foundProject] = false
		visitNode(n.Right(), visitor)
		visitor.found[foundProject] = savedFoundProject
	}
}
