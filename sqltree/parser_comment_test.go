package sqltree

import (
	"strings"
	"testing"
)

// parser_comment_test.go —— 注释处理回归测试。
//
// 需求：开发人员写 SQL 时注释可能出现在任何位置，格式也因人而异
// （-- / # / /* */ / /*! */）。只要这份 SQL 在 MySQL 里能执行，
// sqltree 就必须能解析，并且解析结果里不能带注释。

// TestCommentsAnywhereParsable 覆盖"注释出现在各种位置"的情形。
//
// 这里只断言"能解析且类型正确"，不断言注释在结果里被清掉
// （那由 TestCommentsStrippedFromResult 单独验证）。
func TestCommentsAnywhereParsable(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantType string
	}{
		// 行注释
		{"行首", "-- 这是注释\nSELECT 1", "Select"},
		{"行尾", "SELECT 1 -- 这是注释", "Select"},
		{"井号行首", "# 这是注释\nSELECT 1", "Select"},
		{"井号行尾", "SELECT 1 # 这是注释", "Select"},
		{"无空格双横线是运算符", "SELECT 1--2", "Select"},

		// 块注释
		{"块注释开头", "/* 这是注释 */ SELECT 1", "Select"},
		{"块注释多行", "/* 注释\n第二行\n第三行 */ SELECT 1", "Select"},
		{"块注释含单引号", "/* it's a comment */ SELECT 1", "Select"},
		{"块注释含逗号", "/* a, b, c */ SELECT 1", "Select"},
		{"块注释含分号", "/* a; b; c */ SELECT 1", "Select"},
		{"块注释含关键字", "SELECT 1 /* where select from */", "Select"},
		{"连续多个注释", "/* a */ /* b */ -- c\n/* d */ SELECT 1", "Select"},

		// 注释夹在 SQL 结构中间
		{"select列表中", "SELECT a, /* c */ b FROM t", "Select"},
		{"select关键字后", "SELECT /* c */ a FROM t", "Select"},
		{"from后", "SELECT a FROM /* c */ t", "Select"},
		{"where中", "SELECT a FROM t WHERE /* c */ id = 1", "Select"},
		{"等号后", "SELECT a FROM t WHERE id /* c */ = 1", "Select"},
		{"逗号前", "SELECT a /* c */, b FROM t", "Select"},
		{"函数名后", "SELECT COUNT(/* c */ id) FROM t", "Select"},
		{"括号内", "SELECT (/* c */ 1+2) FROM t", "Select"},
		{"order中", "SELECT a FROM t ORDER /* c */ BY a", "Select"},
		{"group中", "SELECT a FROM t GROUP /* c */ BY a", "Select"},
		{"join中", "SELECT * FROM a /* c */ JOIN b ON a.id = b.id", "Select"},
		{"子查询中", "SELECT * FROM (SELECT /* c */ 1 AS x) AS s", "Select"},
		{"cte中", "WITH c AS (SELECT /* c */ 1) SELECT * FROM c", "Select"},
		{"union中", "SELECT /* c */ 1 UNION SELECT 2", "Select"},

		// DML
		{"insert列中", "INSERT INTO t (a, /* c */ b) VALUES (1, 2)", "Insert"},
		{"insert值中", "INSERT INTO t (a, b) VALUES (1, /* c */ 2)", "Insert"},
		{"update中", "UPDATE t SET a = 1 /* c */ WHERE id = 2", "Update"},
		{"delete中", "DELETE FROM t /* c */ WHERE id = 1", "Delete"},
		{"replace中", "REPLACE INTO t (a) VALUES (1) /* c */", "Insert"},
		{"truncate中", "TRUNCATE TABLE t /* c */", "Truncate"},

		// DDL
		{"建表列间", "CREATE TABLE t ( /* c */ id INT, -- c2\n name VARCHAR(20) )", "CreateTable"},
		{"建表列后", "CREATE TABLE t (id INT /* c */, name VARCHAR(20))", "CreateTable"},
		{"建表末尾", "CREATE TABLE t (id INT) /* c */", "CreateTable"},
		{"建表选项间", "CREATE TABLE t (id INT) ENGINE=InnoDB /* c */ DEFAULT CHARSET=utf8mb4", "CreateTable"},
		{"alter动作间", "ALTER TABLE t ADD COLUMN a INT /* c */, DROP COLUMN b", "Alter"},
		{"alter选项后", "ALTER TABLE t ENGINE=InnoDB /* c */", "Alter"},
		{"索引定义中", "CREATE TABLE t (a INT, KEY idx (/* c */ a))", "CreateTable"},
		{"视图定义中", "CREATE VIEW v AS SELECT /* c */ 1 AS x", "CreateView"},
		{"load data中", "LOAD DATA INFILE 'a.csv' /* c */ INTO TABLE t FIELDS TERMINATED BY ','", "Load"},

		// 分区：注释里的 PARTITION 不能被当真
		{"注释里的分区", "CREATE TABLE t (id INT) /* PARTITION BY HASH(id) */", "CreateTable"},

		// 字符串里的注释符不是注释
		{"字符串含双横线", "SELECT 'a -- b' AS x", "Select"},
		{"字符串含块注释符", "SELECT 'a /* b */ c' AS x", "Select"},
		{"字符串含分号", "SELECT 'a; b' AS x", "Select"},
		{"反引号含双横线", "SELECT `a -- b` FROM t", "Select"},

		// 版本注释：MySQL 会执行里面的内容
		{"版本注释带分号", "/*!40101 SET NAMES utf8 */;", "Set"},
		{"版本注释在select里", "SELECT /*! SQL_NO_CACHE */ a FROM t", "Select"},
		{"版本注释带版本号", "SELECT /*!40001 SQL_NO_CACHE */ a FROM t", "Select"},
		{"版本注释表选项", "CREATE TABLE t (id INT) /*!50100 PARTITION BY HASH(id) */", "CreateTable"},

		// 优化器提示
		{"优化器提示", "SELECT /*+ MAX_EXECUTION_TIME(1000) */ a FROM t", "Select"},
	}

	for _, c := range cases {
		stmt, err := Parse(c.sql)
		if err != nil {
			t.Errorf("FAIL %-18s %q -> %v", c.name, c.sql, err)
			continue
		}
		if stmt.Type != c.wantType {
			t.Errorf("FAIL %-18s %q -> type=%s, want=%s", c.name, c.sql, stmt.Type, c.wantType)
		}
	}
}

// TestCommentsStrippedFromResult 验证注释不会残留在解析结果里。
func TestCommentsStrippedFromResult(t *testing.T) {
	cases := []string{
		"SELECT a FROM t WHERE id = 1 /* 过滤条件 */",
		"SELECT * FROM (SELECT /* c */ 1 AS x) AS s",
		"CREATE VIEW v AS SELECT /* c */ 1 AS x",
		"ALTER TABLE t ENGINE=InnoDB /* c */",
		"SELECT a FROM t WHERE id = 1 -- 尾部注释",
	}

	for _, sqlStr := range cases {
		stmt, err := Parse(sqlStr)
		if err != nil {
			t.Errorf("FAIL %q -> %v", sqlStr, err)
			continue
		}
		rendered := renderDetails(stmt.Details)
		for _, marker := range []string{"/*", "*/", "--"} {
			if strings.Contains(rendered, marker) {
				t.Errorf("FAIL %q -> 结果里残留注释标记 %q: %s", sqlStr, marker, rendered)
			}
		}
	}
}

// TestCommentsInvalidInMySQL 记录"MySQL 本身也不接受"的写法。
//
// 这些不是 sqltree 的缺陷：MySQL 同样会拒绝，所以保持"解析失败"是对的。
func TestCommentsInvalidInMySQL(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		// 嵌套块注释：MySQL 的块注释不嵌套，遇到第一个 */ 就结束，
		// 剩下的 " c */" 是语法错误。
		{"嵌套块注释", "/* a /* b */ c */ SELECT 1"},
		// 版本注释里的内容是一条完整语句，后面又跟了另一条语句，
		// 中间没有分号——MySQL 同样报语法错。
		{"版本注释后缺分号", "/*!40101 SET NAMES utf8 */ SELECT 1"},
		// STRAIGHT_JOIN 是查询选项，不是独立语句。
		{"版本注释当语句", "/*! STRAIGHT_JOIN */ SELECT 1"},
	}

	for _, c := range cases {
		if _, err := Parse(c.sql); err == nil {
			t.Errorf("FAIL %-18s %q -> 本应解析失败", c.name, c.sql)
		}
	}
}

// TestDashDashWithoutSpaceIsOperator 验证 `a--b` 按减号处理，
// 与 MySQL 一致（MySQL 要求 "--" 后跟空白才算注释）。
func TestDashDashWithoutSpaceIsOperator(t *testing.T) {
	stmt, err := Parse("SELECT 1--2")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("类型不对: %T", stmt.Details)
	}
	if len(details.SelectedExprs) != 1 {
		t.Fatalf("投影项数量不对: %v", details.SelectedExprs)
	}
	// 期望是 1 - -2，而不是把 --2 当注释吃掉
	if !strings.Contains(details.SelectedExprs[0], "-") {
		t.Errorf("`1--2` 应按减号解析，实际是 %q", details.SelectedExprs[0])
	}
}

// TestAlterTableOptions 验证 ALTER TABLE 的表选项的各种写法。
//
// MySQL 允许一条 ALTER 里用空格分隔多个表选项，也允许 CHARSET 简写；
// vitess 两样都不认，会整条报语法错。
func TestAlterTableOptions(t *testing.T) {
	cases := []struct {
		sql       string
		wantCount int
	}{
		{"ALTER TABLE t ENGINE=InnoDB", 1},
		{"ALTER TABLE t DEFAULT CHARSET=utf8mb4", 1},
		{"ALTER TABLE t DEFAULT CHARACTER SET=utf8mb4", 1},
		{"ALTER TABLE t CHARSET=utf8mb4", 1},
		{"ALTER TABLE t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", 2},
		{"ALTER TABLE t ENGINE=InnoDB, DEFAULT CHARSET=utf8mb4", 2},
		{"ALTER TABLE t ADD COLUMN a INT, ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", 3},
		{"ALTER TABLE t COMMENT='hello world' DEFAULT CHARSET=utf8mb4", 2},
		// 列注释不能被当成表选项拆开
		{"ALTER TABLE t ADD COLUMN comment VARCHAR(10)", 1},
	}

	for _, c := range cases {
		stmt, err := Parse(c.sql)
		if err != nil {
			t.Errorf("FAIL %q -> %v", c.sql, err)
			continue
		}
		details, ok := stmt.Details.(*AlterTableDetails)
		if !ok {
			t.Errorf("FAIL %q -> 类型不对: %T", c.sql, stmt.Details)
			continue
		}
		if len(details.Actions) != c.wantCount {
			t.Errorf("FAIL %q -> 动作数=%d, want=%d (%+v)",
				c.sql, len(details.Actions), c.wantCount, details.Actions)
		}
	}
}

// TestAlterTableOptionsKeepOriginalSpelling 验证解析结果里保留用户写的原文字样
// （DEFAULT CHARSET 而不是被改写后的 DEFAULT CHARACTER SET）。
func TestAlterTableOptionsKeepOriginalSpelling(t *testing.T) {
	stmt, err := Parse("ALTER TABLE t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details := stmt.Details.(*AlterTableDetails)
	if len(details.Actions) != 2 {
		t.Fatalf("动作数不对: %+v", details.Actions)
	}
	if details.Actions[1].Detail != "DEFAULT CHARSET=utf8mb4" {
		t.Errorf("选项原文不对: %q", details.Actions[1].Detail)
	}
}

// TestStripSQLComments 单元测试归一化函数本身。
func TestStripSQLComments(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"SELECT 1", "SELECT 1"},
		{"SELECT 1 -- x", "SELECT 1  "},
		{"SELECT 1 # x", "SELECT 1  "},
		{"SELECT /* x */ 1", "SELECT   1"},
		{"SELECT 'a -- b' FROM t", "SELECT 'a -- b' FROM t"},
		{"SELECT `a /* b */` FROM t", "SELECT `a /* b */` FROM t"},
		{"SELECT 1--2", "SELECT 1- -2"},
		{"/*!40101 SET NAMES utf8 */", " SET NAMES utf8  "},
		{"/*! SQL_NO_CACHE */", "  SQL_NO_CACHE  "},
		{"/*+ hint */", " "},
		// 没闭合的块注释：整段丢掉，不能 panic
		{"/*", " "},
		{"/*/", " "},
		{"/* a", " "},
		{"SELECT 1 /*", "SELECT 1  "},
	}
	for _, c := range cases {
		got := stripSQLComments(c.in)
		if got != c.want {
			t.Errorf("stripSQLComments(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestUnterminatedBlockCommentDoesNotPanic 回归测试。
//
// 之前 readBlockComment 用 "切片后看结尾是不是 */" 的方式判断注释有没有闭合，
// 遇到 "/*/" 这种贴边的情况会算出非法的切片下标，直接 panic。
func TestUnterminatedBlockCommentDoesNotPanic(t *testing.T) {
	for _, sqlStr := range []string{"/*", "/*/", "/* a", "SELECT 1 /*", "/*!40101"} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PANIC %q: %v", sqlStr, r)
				}
			}()
			_ = stripSQLComments(sqlStr)
		}()
	}
}
