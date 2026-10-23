package sqltree

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestParseWithoutCatalog 验证核心能力：
// 不提供任何 catalog / 表结构，纯粹给一条 SQL，也能正确解析。
//
// 这是真实场景——用户随手给一条语句，我们既不知道表结构也没有权限查库。
func TestParseWithoutCatalog(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantType string
		// 断言 details 中必须包含的字段（子串匹配）
		wantContains []string
	}{
		{
			name:     "DELETE 不存在的表",
			sql:      "delete from unknown_table where saas_tenant_code = 'Apple' and sku_code in ('a','b') and end_time = '2026-10-23 08:00:00'",
			wantType: "Delete",
			wantContains: []string{
				"unknown_table",
				"sku_code",
				"Apple",
			},
		},
		{
			name:     "SELECT 不存在的表",
			sql:      "SELECT id, name FROM some_unknown_table WHERE status = 1 ORDER BY id DESC LIMIT 10",
			wantType: "Select",
			wantContains: []string{
				"some_unknown_table",
				"status",
			},
		},
		{
			name:     "UPDATE 不存在的表",
			sql:      "UPDATE unknown_tbl SET name = 'x', value = 100 WHERE id = 1",
			wantType: "Update",
			wantContains: []string{
				"unknown_tbl",
				"name",
			},
		},
		{
			name:     "INSERT 不存在的表",
			sql:      "INSERT INTO unknown_tbl (id, name) VALUES (1, 'a'), (2, 'b')",
			wantType: "Insert",
			wantContains: []string{
				"unknown_tbl",
				"'a'",
			},
		},
		{
			name:     "JOIN 不存在的表",
			sql:      "SELECT a.id FROM tbl_a a JOIN tbl_b b ON a.id = b.id WHERE b.x = 1",
			wantType: "Select",
			wantContains: []string{
				"tbl_a",
				"tbl_b",
			},
		},
		{
			name:     "子查询不存在的表",
			sql:      "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE v > 5)",
			wantType: "Select",
			wantContains: []string{
				"t1",
				"t2",
			},
		},
		{
			name:     "CTE",
			sql:      "WITH cte AS (SELECT id FROM src) SELECT id FROM cte",
			wantType: "Select",
			wantContains: []string{
				"cte",
				"src",
			},
		},
		{
			name:     "UNION",
			sql:      "SELECT id FROM a UNION SELECT id FROM b",
			wantType: "Select",
			wantContains: []string{
				"union",
			},
		},
		{
			name:     "CREATE TABLE",
			sql:      "CREATE TABLE new_tbl (id INT PRIMARY KEY AUTO_INCREMENT, nm VARCHAR(64) NOT NULL DEFAULT 'x' COMMENT 'c', KEY k (nm)) ENGINE=InnoDB COMMENT='tbl'",
			wantType: "CreateTable",
			wantContains: []string{
				"new_tbl",
				"id",
				"nm",
				"VARCHAR",
				"auto_increment",
			},
		},
		{
			name:     "ALTER TABLE",
			sql:      "ALTER TABLE some_tbl ADD COLUMN c INT NOT NULL DEFAULT 0, DROP COLUMN d",
			wantType: "Alter",
			wantContains: []string{
				"some_tbl",
				"add",
				"drop",
			},
		},
		{
			name:     "CREATE VIEW",
			sql:      "CREATE VIEW v AS SELECT id FROM base_tbl WHERE x > 1",
			wantType: "CreateView",
			wantContains: []string{
				"v",
				"base_tbl",
			},
		},
		{
			name:     "CREATE PROCEDURE",
			sql:      "CREATE PROCEDURE p(IN a INT, OUT b VARCHAR(20)) BEGIN SET b = 'x'; SELECT a FROM t; END",
			wantType: "CreateProcedure",
			wantContains: []string{
				"p",
				"a",
				"b",
				"VARCHAR(20)",
			},
		},
		{
			name:     "CREATE FUNCTION",
			sql:      "CREATE FUNCTION f(p INT) RETURNS INT DETERMINISTIC BEGIN RETURN p + 1; END",
			wantType: "CreateFunction",
			wantContains: []string{
				"f",
				"p",
				"INT",
				"DETERMINISTIC",
			},
		},
		{
			name:     "CREATE TRIGGER",
			sql:      "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.c = 'x'",
			wantType: "CreateTrigger",
			wantContains: []string{
				"trg",
				"before",
				"insert",
			},
		},
		{
			name:     "CALL",
			sql:      "CALL my_proc(1, @x)",
			wantType: "Call",
			wantContains: []string{
				"my_proc",
			},
		},
		{
			name:     "DROP TABLE",
			sql:      "DROP TABLE IF EXISTS a, b",
			wantType: "Drop",
			wantContains: []string{
				"a",
				"b",
			},
		},
		{
			name:     "TRUNCATE",
			sql:      "TRUNCATE TABLE t",
			wantType: "Truncate",
			wantContains: []string{
				"t",
			},
		},
		{
			name:     "RENAME TABLE",
			sql:      "RENAME TABLE a TO b, c TO d",
			wantType: "RenameTable",
			wantContains: []string{
				"a",
				"b",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 关键：不建任何 engine / catalog，engine 传 nil
			stmt, err := ParseSQL(nil, nil, tc.sql)
			if err != nil {
				t.Fatalf("解析失败: %v\nSQL: %s", err, tc.sql)
			}
			if stmt.Type != tc.wantType {
				t.Errorf("类型 = %q, 期望 %q\nSQL: %s", stmt.Type, tc.wantType, tc.sql)
			}
			rendered := renderDetails(stmt.Details)
			for _, want := range tc.wantContains {
				if !strings.Contains(rendered, want) {
					t.Errorf("输出中缺少 %q\n实际输出: %s", want, rendered)
				}
			}
		})
	}
}

// renderDetails 把 details 渲染成字符串便于断言。
func renderDetails(details any) string {
	out, err := json.MarshalIndent(details, "", "  ")
	if err != nil {
		return ""
	}
	return string(out)
}

// TestSetOpBranchesKeepDetails 验证集合运算各分支的表名与条件不会丢。
//
// 旧实现里 UNION 只保留了合并后的投影列，
// `SELECT a FROM t1 WHERE x=1 UNION SELECT b FROM t2 WHERE y=2`
// 里两个分支的 FROM/WHERE 全丢了——调用方根本不知道查了哪两张表。
func TestSetOpBranchesKeepDetails(t *testing.T) {
	stmt, err := Parse("SELECT a FROM t1 WHERE x = 1 UNION SELECT b FROM t2 WHERE y = 2")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("details 类型 = %T", stmt.Details)
	}
	if details.SetOp != "union" {
		t.Errorf("SetOp = %q, 期望 %q", details.SetOp, "union")
	}
	if len(details.SetOpBranches) != 2 {
		t.Fatalf("SetOpBranches 长度 = %d, 期望 2", len(details.SetOpBranches))
	}
	left, right := details.SetOpBranches[0], details.SetOpBranches[1]
	if left.From != "t1" {
		t.Errorf("左分支 From = %q, 期望 %q", left.From, "t1")
	}
	if right.From != "t2" {
		t.Errorf("右分支 From = %q, 期望 %q", right.From, "t2")
	}
	if !strings.Contains(left.Where, "x = 1") {
		t.Errorf("左分支 Where = %q, 期望包含 %q", left.Where, "x = 1")
	}
	if !strings.Contains(right.Where, "y = 2") {
		t.Errorf("右分支 Where = %q, 期望包含 %q", right.Where, "y = 2")
	}
}
