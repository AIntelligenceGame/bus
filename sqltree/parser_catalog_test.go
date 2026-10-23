package sqltree

import (
	"context"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// setupCatalogEngine 建一个带 t1 表的内存 catalog。
func setupCatalogEngine(t *testing.T) (*sql.Context, *sqle.Engine) {
	t.Helper()
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	engine := sqle.NewDefault(pro)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("test")

	tbl := memory.NewTable(db.BaseDatabase, "t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "t1", PrimaryKey: true},
		{Name: "name", Type: types.Text, Source: "t1"},
	}), db.GetForeignKeyCollection())
	db.AddTable("t1", tbl)
	return ctx, engine
}

// TestOutputSchemaIndependentOfCatalog 验证**输出格式与表是否存在无关**。
//
// 这是历史需求里最关键的一条：用户随手给一条 SQL，解析器不知道表结构。
// 因此同一类语句，无论表在不在 catalog 里，输出的 JSON 结构必须一致，
// 否则调用方就要写两套解析逻辑。
func TestOutputSchemaIndependentOfCatalog(t *testing.T) {
	ctx, engine := setupCatalogEngine(t)

	// 同一批语句，分别在"无 catalog"和"有 catalog（但表不存在）"两种情况下解析
	queries := []string{
		"SELECT * FROM unknown_table WHERE a = 1",
		"SELECT DISTINCT id, name FROM unknown_table LIMIT 5 OFFSET 2",
		"UPDATE unknown_table SET a = 1 WHERE b = 2",
		"DELETE FROM unknown_table WHERE a = 1",
		"INSERT INTO unknown_table (a) VALUES (1)",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			noCatalog, err := ParseSQL(nil, nil, q)
			if err != nil {
				t.Fatalf("无 catalog 解析失败: %v", err)
			}
			withCatalog, err := ParseSQL(ctx, engine, q)
			if err != nil {
				t.Fatalf("有 catalog 解析失败: %v", err)
			}
			if noCatalog.Type != withCatalog.Type {
				t.Errorf("类型不一致: 无 catalog=%q, 有 catalog=%q", noCatalog.Type, withCatalog.Type)
			}
			noCatalogJSON := renderDetails(noCatalog.Details)
			withCatalogJSON := renderDetails(withCatalog.Details)
			if noCatalogJSON != withCatalogJSON {
				t.Errorf("输出不一致\n无 catalog: %s\n有 catalog: %s", noCatalogJSON, withCatalogJSON)
			}
		})
	}
}

// TestStarExpansionWithCatalog 验证有 catalog 时 SELECT * 会被展开成具体列。
//
// 这是 AST 层（纯语法）唯一补不上的信息，所以走 plan 层，
// 但只影响 selected_exprs，其余字段仍以 AST 层为准。
func TestStarExpansionWithCatalog(t *testing.T) {
	ctx, engine := setupCatalogEngine(t)

	// 无 catalog：保持 "*"
	stmt, err := ParseSQL(nil, nil, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("details 类型 = %T", stmt.Details)
	}
	if len(details.SelectedExprs) != 1 || details.SelectedExprs[0] != "*" {
		t.Errorf("无 catalog 时应保持 \"*\", 实际 = %v", details.SelectedExprs)
	}

	// 有 catalog：展开成 id/name
	stmt, err = ParseSQL(ctx, engine, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok = stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("details 类型 = %T", stmt.Details)
	}
	if len(details.SelectedExprs) != 2 {
		t.Errorf("有 catalog 时应展开成 2 列, 实际 = %v", details.SelectedExprs)
	}
}

// TestCatalogKeepsAstOnlyFields 验证有 catalog 时 AST 层独有的信息不会丢。
//
// 旧实现里 DML 一律走 plan 层，导致 DISTINCT / OFFSET / CTE / 集合运算被丢掉。
func TestCatalogKeepsAstOnlyFields(t *testing.T) {
	ctx, engine := setupCatalogEngine(t)

	stmt, err := ParseSQL(ctx, engine, "SELECT DISTINCT id FROM t1 LIMIT 5 OFFSET 2")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("details 类型 = %T", stmt.Details)
	}
	if !details.Distinct {
		t.Errorf("DISTINCT 丢失")
	}
	if details.Offset != 2 {
		t.Errorf("OFFSET = %d, 期望 2", details.Offset)
	}
	if details.Limit != 5 {
		t.Errorf("LIMIT = %d, 期望 5", details.Limit)
	}
}

// TestNilCtxDoesNotPanic 验证 ctx 为 nil 时不会 panic。
//
// go-mysql-server 的 plan 层在 ctx 为 nil 时会空指针 panic
// （sql.LoadSqlMode(ctx) 直接解引用 ctx.Session）。
// 调用方"只想要 AST 结果"时很可能会图省事传 nil ctx，
// 所以这里必须保证不崩——退回纯 AST 解析即可。
func TestNilCtxDoesNotPanic(t *testing.T) {
	_, engine := setupCatalogEngine(t)

	stmt, err := ParseSQL(nil, engine, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("details 类型 = %T", stmt.Details)
	}
	// 没有 ctx 就没法做语义分析，* 保持不展开
	if len(details.SelectedExprs) != 1 || details.SelectedExprs[0] != "*" {
		t.Errorf("nil ctx 时应保持 \"*\", 实际 = %v", details.SelectedExprs)
	}
}

// TestDescribeTableWithoutCatalog 验证 DescribeTable 没有 catalog 时返回错误而不是 panic。
func TestDescribeTableWithoutCatalog(t *testing.T) {
	if _, err := DescribeTable(nil, nil, "db", "t"); err == nil {
		t.Errorf("没有 catalog 时 DescribeTable 应返回错误")
	}
}
