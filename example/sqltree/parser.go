package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SQLStatement represents a parsed SQL statement with its type and details.
type SQLStatement struct {
	Type    string `json:"type"`
	Details any    `json:"details"`
}

// SelectDetails represents the details of a SELECT statement.
type SelectDetails struct {
	SelectedExprs []string `json:"selected_exprs,omitempty"`
	From          string   `json:"from,omitempty"`
	Where         string   `json:"where,omitempty"`
	GroupBy       []string `json:"group_by,omitempty"`
	Having        string   `json:"having,omitempty"`
	OrderBy       []string `json:"order_by,omitempty"`
	Limit         int      `json:"limit,omitempty"`
}

// UpdateDetails represents the details of an UPDATE statement.
type UpdateDetails struct {
	Table      string   `json:"table"`
	SetClauses []string `json:"set_clauses"`
	Where      string   `json:"where,omitempty"`
	OrderBy    []string `json:"order_by,omitempty"`
	Limit      int      `json:"limit,omitempty"`
}

// InsertDetails represents the details of an INSERT statement.
type InsertDetails struct {
	Table      string         `json:"table"`
	Columns    []string       `json:"columns,omitempty"`
	Values     []string       `json:"values,omitempty"`
	SelectData *SelectDetails `json:"select_data,omitempty"`
}

// DeleteDetails represents the details of a DELETE statement.
type DeleteDetails struct {
	Table   string   `json:"table"`
	Where   string   `json:"where,omitempty"`
	OrderBy []string `json:"order_by,omitempty"`
	Limit   int      `json:"limit,omitempty"`
}

func main() {
	// 创建内存数据库 provider
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	// 创建 SQL 引擎
	engine := sqle.NewDefault(pro)

	// 创建基础会话
	baseSession := sql.NewBaseSession()
	session := memory.NewSession(baseSession, pro)

	// 创建解析上下文
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("test")

	// 创建测试表 - 带主键
	createTestTables(db)

	// 测试各种复杂 SQL
	testCases := []struct {
		name string
		sql  string
	}{
		// DML - UPDATE
		{"Simple UPDATE", "UPDATE t1 SET name = 'new_name' WHERE id = 1"},
		{"UPDATE with subquery in SET", "UPDATE t1 SET name = (SELECT name FROM t2 WHERE t2.id = t1.id), value = 100 WHERE status = 1"},
		{"UPDATE with JOIN", "UPDATE t1 a JOIN t2 b ON a.id = b.id SET a.name = b.name WHERE b.status = 1"},

		// DML - INSERT
		{"INSERT VALUES", "INSERT INTO t1 (id, name, value) VALUES (1, 'test', 100)"},
		{"INSERT SELECT", "INSERT INTO t1 (id, name, value) SELECT id, name, value FROM t2 WHERE status = 1"},
		{"INSERT multiple VALUES", "INSERT INTO t1 (id, name, value) VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)"},
		{"INSERT with CTE", "INSERT INTO t1 (id, name) WITH cte AS (SELECT id, name FROM t2) SELECT id, name FROM cte"},

		// DML - DELETE
		{"Simple DELETE", "DELETE FROM t1 WHERE id = 1"},
		{"DELETE with ORDER BY LIMIT", "DELETE FROM t1 WHERE status = 0 ORDER BY id DESC LIMIT 10"},
		{"DELETE with subquery", "DELETE FROM t1 WHERE id IN (SELECT id FROM t2 WHERE value < 10)"},
		{"Multi-table DELETE", "DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.status = 0"},

		// DQL - SELECT
		{"Simple SELECT", "SELECT id, name, value FROM t1 WHERE status = 1"},
		{"SELECT with JOIN", "SELECT a.id, a.name, b.value FROM t1 a JOIN t2 b ON a.id = b.id WHERE a.status = 1"},
		{"SELECT with subquery in FROM", "SELECT * FROM (SELECT id, name FROM t1 WHERE x > 10) AS sub WHERE sub.id > 5"},
		{"SELECT with GROUP BY", "SELECT category, COUNT(*) as cnt, SUM(value) as total FROM t1 GROUP BY category HAVING cnt > 1"},
		{"SELECT with ORDER BY and LIMIT", "SELECT * FROM t1 ORDER BY id DESC LIMIT 10 OFFSET 5"},
		{"SELECT DISTINCT", "SELECT DISTINCT name, value FROM t1 WHERE status = 1"},
		{"SELECT with subquery in WHERE (IN)", "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE value > 50)"},
		{"SELECT with subquery in WHERE (EXISTS)", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"},
		{"SELECT with subquery in WHERE (scalar)", "SELECT * FROM t1 WHERE value > (SELECT AVG(value) FROM t2)"},

		// SET Operations
		{"SELECT UNION", "SELECT id, name FROM t1 UNION SELECT id, name FROM t2"},
		{"SELECT UNION ALL", "SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2"},
		{"SELECT INTERSECT", "SELECT id FROM t1 INTERSECT SELECT id FROM t2"},
		{"SELECT EXCEPT", "SELECT id FROM t1 EXCEPT SELECT id FROM t2"},

		// CTE (Common Table Expression)
		{"WITH RECURSIVE CTE", "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte"},
		{"WITH CTE multiple", "WITH cte1 AS (SELECT id, name FROM t1), cte2 AS (SELECT id, value FROM t2) SELECT c1.id, c1.name, c2.value FROM cte1 c1 JOIN cte2 c2 ON c1.id = c2.id"},
		{"WITH CTE in subquery", "SELECT * FROM (WITH cte AS (SELECT id FROM t1) SELECT * FROM cte) AS sub"},
	}

	for _, tc := range testCases {
		fmt.Println("=" + tc.name + ": " + tc.sql)
		parseAndPrint(ctx, engine, tc.sql)
		fmt.Println()
	}
}

func createTestTables(db *memory.Database) {
	// t1 表 - 带主键
	t1 := memory.NewTable(db.BaseDatabase, "t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "t1", PrimaryKey: true},
		{Name: "name", Type: types.Text, Source: "t1"},
		{Name: "value", Type: types.Int64, Source: "t1"},
		{Name: "status", Type: types.Int64, Source: "t1"},
		{Name: "x", Type: types.Int64, Source: "t1"},
		{Name: "category", Type: types.Text, Source: "t1"},
	}), db.GetForeignKeyCollection())
	db.AddTable("t1", t1)

	// t2 表 - 带主键
	t2 := memory.NewTable(db.BaseDatabase, "t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "t2", PrimaryKey: true},
		{Name: "name", Type: types.Text, Source: "t2"},
		{Name: "value", Type: types.Int64, Source: "t2"},
		{Name: "status", Type: types.Int64, Source: "t2"},
	}), db.GetForeignKeyCollection())
	db.AddTable("t2", t2)
}

func parseAndPrint(ctx *sql.Context, engine *sqle.Engine, sqlStr string) {
	analyzedNode, err := engine.AnalyzeQuery(ctx, sqlStr)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
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
		sqlStmt = SQLStatement{
			Type:    "Select",
			Details: extractSelectDetails(analyzedNode),
		}
	}

	jsonOutput, err := json.MarshalIndent(sqlStmt, "", "  ")
	if err != nil {
		fmt.Printf("JSON error: %v\n", err)
		return
	}
	fmt.Println(string(jsonOutput))
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
