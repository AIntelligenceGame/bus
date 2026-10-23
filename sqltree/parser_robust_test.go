package sqltree

import (
	"math/rand"
	"strings"
	"testing"
)

// parser_robust_test.go —— 健壮性回归测试。
//
// 解析器面对的是用户随手贴过来的 SQL：可能有没闭合的引号、没闭合的注释、
// 少半个关键字。**任何情况下都不能 panic**——进程崩了比解析失败严重得多。

// TestMalformedSQLDoesNotPanic 用一堆畸形 SQL 验证解析器不会崩。
func TestMalformedSQLDoesNotPanic(t *testing.T) {
	cases := []string{
		// 没闭合的注释
		"/*", "/*/", "/* a", "SELECT 1 /*", "/*!",
		"/*!40101", "/*! */", "/*!40101 SET",
		// 没闭合的引号
		"'", "'abc", "`abc", `"abc`, "SELECT 'abc",
		"SELECT `abc FROM t", "COMMENT 'abc",
		// 截断的关键字
		"SELECT", "SELECT *", "SELECT * FROM", "FROM t", "WHERE a =",
		"CREATE TABLE", "CREATE TABLE t", "CREATE TABLE t (",
		"CREATE TABLE t (a", "CREATE TABLE t (a INT",
		"ALTER TABLE", "ALTER TABLE t", "ALTER TABLE t ADD",
		"INSERT", "INSERT INTO", "INSERT INTO t", "INSERT INTO t (",
		"UPDATE", "UPDATE t", "UPDATE t SET", "DELETE FROM",
		"DROP", "DROP TABLE", "CREATE VIEW", "CREATE INDEX",
		// 只有一个符号
		"(", ")", ",", ";", "=", "*",
		// 分区/选项的残缺形式
		"CREATE TABLE t (a INT) PARTITION BY",
		"CREATE TABLE t (a INT) PARTITION BY RANGE",
		"CREATE TABLE t (a INT) PARTITION BY RANGE (a)",
		"CREATE TABLE t (a INT) PARTITION BY RANGE (a) (PARTITION p0",
		"CREATE TABLE t (a INT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN",
		"ALTER TABLE t ENGINE", "ALTER TABLE t ENGINE=", "ALTER TABLE t ENGINE=x DEFAULT",
		// 空与纯空白
		"", " ", "\n", "\t", ";",
		// 复制/管理语句的残缺形式
		"RESET", "START", "STOP", "CHANGE", "CHANGE MASTER", "CHANGE MASTER TO",
		"SHOW", "SHOW ENGINE", "SHOW ENGINE INNODB", "PURGE BINARY LOGS",
		// 深层嵌套
		strings.Repeat("(", 200) + "1" + strings.Repeat(")", 200),
		strings.Repeat("SELECT * FROM (", 30) + "t" + strings.Repeat(")", 30),
	}

	for _, sqlStr := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PANIC %q: %v", sqlStr, r)
				}
			}()
			_, _ = Parse(sqlStr)
		}()
	}
}

// TestRandomSQLDoesNotPanic 随机拼 SQL，验证解析器不会崩。
//
// 用固定种子，失败可以复现。
func TestRandomSQLDoesNotPanic(t *testing.T) {
	pieces := []string{
		"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "JOIN", "ON",
		"(", ")", ",", ";", "'", "\"", "`", "/*", "*/", "--", "#", "=", "*",
		"t", "a", "1", " ", "\n", "CREATE TABLE", "PARTITION BY", "VALUES LESS THAN",
		"MAXVALUE", "ENGINE=", "INNODB", "ALTER TABLE", "INSERT INTO", "VALUES",
	}
	rng := rand.New(rand.NewSource(20261023))

	for i := 0; i < 3000; i++ {
		n := 1 + rng.Intn(12)
		parts := make([]string, n)
		for j := range parts {
			parts[j] = pieces[rng.Intn(len(pieces))]
		}
		sqlStr := strings.Join(parts, " ")

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("PANIC %q: %v", sqlStr, r)
				}
			}()
			_, _ = Parse(sqlStr)
		}()
	}
}
