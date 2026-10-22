package main

import (
	"context"
	"fmt"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func setupEngineWithRuleLinkTable(t *testing.T) (*sql.Context, *sqle.Engine) {
	t.Helper()

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	engine := sqle.NewDefault(pro)

	baseSession := sql.NewBaseSession()
	session := memory.NewSession(baseSession, pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("test")

	tbl := memory.NewTable(db.BaseDatabase, "im_sku_sync_rule_link", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "im_sku_sync_rule_link", PrimaryKey: true},
		{Name: "saas_tenant_code", Type: types.Text, Source: "im_sku_sync_rule_link"},
		{Name: "sku_code", Type: types.Text, Source: "im_sku_sync_rule_link"},
		{Name: "end_time", Type: types.Datetime, Source: "im_sku_sync_rule_link"},
	}), db.GetForeignKeyCollection())
	db.AddTable("im_sku_sync_rule_link", tbl)

	return ctx, engine
}

func TestDeleteWithLargeInList(t *testing.T) {
	sqlStr := `delete from im_sku_sync_rule_link  where saas_tenant_code = 'Apple_Distributor'
and sku_code in ('195951407364_P_APPLE',
'195951407364_195951572024_P_APPLE',
'195951407364_195951571720_P_APPLE',
'195951407371_P_APPLE',
'195951407371_195951572024_P_APPLE',
'195951407371_195951571720_P_APPLE',
'195951407388_P_APPLE',
'195951407388_195951572024_P_APPLE',
'195951407388_195951571720_P_APPLE',
'195951407395_P_APPLE',
'195951407395_195951572024_P_APPLE',
'195951407395_195951571720_P_APPLE',
'195951407401_P_APPLE',
'195951407401_195951572024_P_APPLE',
'195951407401_195951571720_P_APPLE',
'195951407418_P_APPLE',
'195951407418_195951572024_P_APPLE',
'195951407418_195951571720_P_APPLE',
'195951407425_P_APPLE',
'195951407425_195951572024_P_APPLE',
'195951407425_195951571720_P_APPLE',
'195951407432_P_APPLE',
'195951407432_195951572024_P_APPLE',
'195951407432_195951571720_P_APPLE')
and end_time = '2026-10-23 08:00:00';`

	ctx, engine := setupEngineWithRuleLinkTable(t)

	node, err := engine.AnalyzeQuery(ctx, sqlStr)
	if err != nil {
		t.Fatalf("Analyze error: %v", err)
	}

	fmt.Printf("Root node type: %T\n\n", node)
	fmt.Println("=== 节点树 ===")
	printTree(node, 0)

	// 用现有提取逻辑
	fmt.Println("\n=== 提取结果 ===")
	parseAndPrint(ctx, engine, sqlStr)

	// 检查 WHERE 表达式细节
	fmt.Println("\n=== WHERE 表达式细节 ===")
	if del, ok := node.(*plan.DeleteFrom); ok {
		if filter, ok := del.Child.(*plan.Filter); ok {
			expr := filter.Expression
			fmt.Printf("表达式类型: %T\n", expr)
			fmt.Printf("表达式内容: %s\n", expr.String())
			if and, ok := expr.(*expression.And); ok {
				fmt.Printf("\nAND 左操作数: %s\n", and.LeftChild.String())
				fmt.Printf("AND 右操作数: %s\n", and.RightChild.String())
			}
		}
	}
}

func printTree(node sql.Node, indent int) {
	if node == nil {
		return
	}
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}
	fmt.Printf("%s%T\n", prefix, node)
	for _, child := range node.Children() {
		printTree(child, indent+1)
	}
}
