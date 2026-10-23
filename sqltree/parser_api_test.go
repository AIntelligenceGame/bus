package sqltree

import (
	"testing"
)

// TestPublicAPIShape 锁定对外 API 的形状。
//
// 这些是"库使用者"直接依赖的约定，改动会让调用方的类型断言全部失效，
// 所以必须有测试守着。
func TestPublicAPIShape(t *testing.T) {
	stmt, err := Parse("SELECT id FROM t")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if stmt.Type != "Select" {
		t.Errorf("Type = %q, 期望 %q", stmt.Type, "Select")
	}
	// Details 一律是指针，调用方统一用 `stmt.Details.(*SelectDetails)`
	if _, ok := stmt.Details.(*SelectDetails); !ok {
		t.Errorf("Details 类型 = %T, 期望 *SelectDetails", stmt.Details)
	}
}

// TestDetailsArePointers 检查所有语句类型的 Details 都是指针。
func TestDetailsArePointers(t *testing.T) {
	cases := []struct {
		sql   string
		check func(any) bool
	}{
		{"SELECT id FROM t", func(d any) bool { _, ok := d.(*SelectDetails); return ok }},
		{"INSERT INTO t (a) VALUES (1)", func(d any) bool { _, ok := d.(*InsertDetails); return ok }},
		{"UPDATE t SET a = 1", func(d any) bool { _, ok := d.(*UpdateDetails); return ok }},
		{"DELETE FROM t WHERE a = 1", func(d any) bool { _, ok := d.(*DeleteDetails); return ok }},
		{"CREATE TABLE t (id INT)", func(d any) bool { _, ok := d.(*CreateTableDetails); return ok }},
		{"CREATE VIEW v AS SELECT 1", func(d any) bool { _, ok := d.(*ViewDetails); return ok }},
		{"CREATE PROCEDURE p() BEGIN END", func(d any) bool { _, ok := d.(*ProcedureDetails); return ok }},
		{"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1", func(d any) bool { _, ok := d.(*TriggerDetails); return ok }},
		{"CREATE FUNCTION f() RETURNS INT RETURN 1", func(d any) bool { _, ok := d.(*FunctionDetails); return ok }},
		{"DROP TABLE t", func(d any) bool { _, ok := d.(*DropDetails); return ok }},
		{"RENAME TABLE a TO b", func(d any) bool { _, ok := d.(*RenameTableDetails); return ok }},
		{"TRUNCATE TABLE t", func(d any) bool { _, ok := d.(*TruncateDetails); return ok }},
		{"SET @a = 1", func(d any) bool { _, ok := d.(map[string]string); return ok }},
		{"USE db1", func(d any) bool { _, ok := d.(map[string]string); return ok }},
		{"SHOW TABLES", func(d any) bool { _, ok := d.(map[string]string); return ok }},
		{"COMMIT", func(d any) bool { _, ok := d.(map[string]string); return ok }},
		{"LOCK TABLES t READ", func(d any) bool { _, ok := d.(*LockTablesDetails); return ok }},
		{"PREPARE s FROM 'SELECT 1'", func(d any) bool { _, ok := d.(*PrepareDetails); return ok }},
		{"EXECUTE s", func(d any) bool { _, ok := d.(*ExecuteDetails); return ok }},
		{"DEALLOCATE PREPARE s", func(d any) bool { _, ok := d.(*DeallocateDetails); return ok }},
		{"EXPLAIN SELECT 1", func(d any) bool { _, ok := d.(*ExplainDetails); return ok }},
		{"FLUSH TABLES", func(d any) bool { _, ok := d.(*FlushDetails); return ok }},
		{"ANALYZE TABLE t", func(d any) bool { _, ok := d.(*TableMaintenanceDetails); return ok }},
		{"KILL 1", func(d any) bool { _, ok := d.(*KillDetails); return ok }},
		{"SAVEPOINT sp", func(d any) bool { _, ok := d.(*SavepointDetails); return ok }},
		{"DO 1", func(d any) bool { _, ok := d.(*DoDetails); return ok }},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			if !tc.check(stmt.Details) {
				t.Errorf("Details 类型 = %T, 与预期不符", stmt.Details)
			}
		})
	}
}

// TestColumnTypeIsComplete 验证列类型不丢精度与 UNSIGNED/ZEROFILL。
func TestColumnTypeIsComplete(t *testing.T) {
	stmt, err := Parse(`CREATE TABLE t (
		a DECIMAL(20,4) UNSIGNED NOT NULL,
		b VARCHAR(128) ZEROFILL,
		c TINYINT(1) NOT NULL DEFAULT 0,
		d ENUM('a','b','c') NOT NULL
	)`)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*CreateTableDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	want := map[string]string{
		"a": "DECIMAL(20,4) unsigned",
		"b": "VARCHAR(128) zerofill",
		"c": "TINYINT(1)",
		"d": "ENUM",
	}
	for _, col := range details.Columns {
		expected, ok := want[col.Name]
		if !ok {
			continue
		}
		if col.Type != expected {
			t.Errorf("列 %s 的类型 = %q, 期望 %q", col.Name, col.Type, expected)
		}
	}
	// ENUM 的取值也要保留
	for _, col := range details.Columns {
		if col.Name != "d" {
			continue
		}
		if len(col.EnumValues) != 3 {
			t.Errorf("ENUM 取值 = %v, 期望 3 个", col.EnumValues)
		}
	}
}

// TestColumnKeyOptionMapping 锁定 vitess 的 ColumnKeyOption 序号含义。
//
// sqltree 用 KeyOpt 字段判断主键/唯一键，但 vitess 没有导出这些常量，
// 只能按序号取值（见 ast_parser.go 里的 vitessKey* 常量）。
// 这个测试一旦失败，说明 vitess 改了常量顺序，必须同步更新。
func TestColumnKeyOptionMapping(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (pk INT PRIMARY KEY, u INT UNIQUE, uk INT UNIQUE KEY, plain INT)")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*CreateTableDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	want := map[string]struct{ pk, unique bool }{
		"pk":    {pk: true},
		"u":     {unique: true},
		"uk":    {unique: true},
		"plain": {},
	}
	for _, col := range details.Columns {
		expected, ok := want[col.Name]
		if !ok {
			t.Fatalf("出现了意料之外的列 %q", col.Name)
		}
		if col.PrimaryKey != expected.pk {
			t.Errorf("列 %s 的 primary_key = %v, 期望 %v", col.Name, col.PrimaryKey, expected.pk)
		}
		if col.Unique != expected.unique {
			t.Errorf("列 %s 的 unique = %v, 期望 %v", col.Name, col.Unique, expected.unique)
		}
	}
}

// TestColumnCommentNotMistakenForKey 验证列注释里的 "unique"/"primary key"
// 不会被误判成键约束。
func TestColumnCommentNotMistakenForKey(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INT COMMENT 'unique and primary key')")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details := stmt.Details.(*CreateTableDetails)
	for _, col := range details.Columns {
		if col.PrimaryKey {
			t.Errorf("列 %s 被误判成主键（来自列注释）", col.Name)
		}
		if col.Unique {
			t.Errorf("列 %s 被误判成唯一键（来自列注释）", col.Name)
		}
	}
}
