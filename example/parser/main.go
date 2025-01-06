package main

import (
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/parser"
	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// 定义一个自定义的监听器，用于处理解析事件
type MyListener struct {
	*parser.BaseMySqlParserListener
	tableNames map[string]struct{} // 用于存储表名称的映射
	columns    []string            // 用于存储字段列的切片
}

// 重写EnterTableName方法，处理表名称
func (l *MyListener) EnterTableName(ctx *parser.TableNameContext) {
	tableName := strings.ToLower(ctx.GetText())
	l.tableNames[tableName] = struct{}{} // 将表名称添加到映射中
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

// 重写EnterSelectStatement方法，处理SELECT语句
func (l *MyListener) EnterSelectStatement(ctx *parser.SelectStatementContext) {
	fmt.Println("Enter SELECT statement")
	fmt.Println(ctx.GetText())

	// 解析并存储字段列
	l.parseSelectColumns(ctx)

	fmt.Println("Columns:", l.columns)
	fmt.Println()
}

// 解析SELECT语句中的列名
func (l *MyListener) parseSelectColumns(ctx *parser.SelectStatementContext) {
	// 获取SELECT语句的子节点
	children := ctx.GetChildren()

	// 遍历子节点，查找列名
	for _, child := range children {
		if columnCtx, ok := child.(*parser.SelectElementContext); ok {
			// 获取列名
			column := strings.TrimSpace(columnCtx.GetText())
			l.columns = append(l.columns, column)
		}
	}
}

// 重写EnterFromClause方法，处理FROM子句
func (l *MyListener) EnterFromClause(ctx *parser.FromClauseContext) {
	fmt.Println("Enter FROM clause")
	fmt.Println(ctx.GetText())
	fmt.Println()
}

// 重写EnterGroupByClause方法，处理GROUP BY子句
func (l *MyListener) EnterGroupByClause(ctx *parser.GroupByClauseContext) {
	fmt.Println("Enter GROUP BY clause")
	fmt.Println(ctx.GetText())
	fmt.Println()
}

// 重写EnterOrderByClause方法，处理ORDER BY子句
func (l *MyListener) EnterOrderByClause(ctx *parser.OrderByClauseContext) {
	fmt.Println("Enter ORDER BY clause")
	fmt.Println(ctx.GetText())
	fmt.Println()
}

// github.com/akito0107/xsqlparser 支持with 语法
func main() {
	// 创建一个ANTLR输入流
	input := antlr.NewInputStream("SELECT inv.id AS id, inv.version AS version, inv.im_organization AS imOrganization, inv.sku_code AS skuCode, inv.inv_status_code AS invStatusCode , inv.location_code AS locationCode, inv.cw_code AS cwCode, inv.quota_interval AS quotaInterval, inv.create_time AS createTime, inv.last_modify_time AS lastModifyTime , inv.`qty` AS qty, SUM(CASE WHEN occ.current_occupy_qty IS NULL THEN 0 ELSE occ.current_occupy_qty END) AS occupyQty , inv.qty + SUM(CASE WHEN occ.current_occupy_qty IS NULL THEN 0 ELSE occ.current_occupy_qty END) AS availableQty, inv.saas_tenant_code AS saasTenantCode FROM ( SELECT * FROM t_cloud_location_inventory_6 inv1 WHERE inv1.im_organization = 'JackWolfskinGG' AND inv1.saas_tenant_code = 'JACKWOLFSKIN' ORDER BY inv1.id ASC LIMIT 0, 50000 ) inv LEFT JOIN t_cloud_inventory_occupy_6 occ ON inv.im_organization = occ.im_organization AND inv.sku_code = occ.sku_code AND inv.inv_status_code = occ.inv_status_code AND inv.cw_code = occ.cw_code AND inv.location_code = occ.location_code AND inv.quota_interval = occ.quota_interval AND occ.saas_tenant_code = 'JACKWOLFSKIN' GROUP BY inv.im_organization, inv.sku_code, inv.inv_status_code, inv.cw_code, inv.location_code, inv.quota_interval ORDER BY inv.id ASC")

	// 创建一个词法分析器
	lexer := parser.NewMySqlLexer(input)

	// 创建一个词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建一个语法分析器
	p := parser.NewMySqlParser(stream)

	// 创建一个自定义的监听器，并初始化表名称映射
	listener := &MyListener{
		tableNames: make(map[string]struct{}),
		columns:    make([]string, 0),
	}

	// 创建一个语法树遍历器，并注册监听器
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Root())

	// 获取解析到的表名称并打印
	fmt.Println("Table names:", listener.GetTableNames())
}
