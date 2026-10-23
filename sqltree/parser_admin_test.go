package sqltree

import (
	"strings"
	"testing"
)

// parser_admin_test.go —— 管理/复制/库管理类语句的回归测试。
//
// 这类语句在 vitess 的 AST 层里大多没有可展示的字段，
// 有些（START SLAVE、CHANGE MASTER TO、SHOW ENGINE INNODB STATUS）
// vitess 干脆连语法都不支持，由自研解析器兜底。

// TestAdminAndReplicationStatements 验证管理/复制类语句都能解析。
func TestAdminAndReplicationStatements(t *testing.T) {
	cases := []struct {
		sql      string
		wantType string
	}{
		// 复制控制
		{"RESET MASTER", "Admin"},
		{"RESET SLAVE", "Admin"},
		{"RESET SLAVE ALL", "Admin"},
		{"RESET REPLICA ALL", "Admin"},
		{"START SLAVE", "Admin"},
		{"START REPLICA", "Admin"},
		{"STOP SLAVE", "Admin"},
		{"STOP REPLICA", "Admin"},
		{"CHANGE MASTER TO MASTER_HOST='h'", "Admin"},
		{"CHANGE REPLICATION SOURCE TO SOURCE_HOST='h'", "Admin"},
		{"CHANGE REPLICATION FILTER FILTER_NAME='f'", "Admin"},

		// 运维查询
		{"SHOW ENGINE INNODB STATUS", "Admin"},
		{"SHOW BINLOG EVENTS", "Admin"},
		{"SHOW OPEN TABLES", "Admin"},
		{"PURGE BINARY LOGS TO 'mysql-bin.000001'", "Admin"},
		{"PURGE BINARY LOGS BEFORE '2020-01-01'", "Admin"},

		// 库管理
		{"CREATE DATABASE d", "Database"},
		{"CREATE DATABASE IF NOT EXISTS d", "Database"},
		{"DROP DATABASE d", "Database"},
		{"DROP DATABASE IF EXISTS d", "Database"},
		{"ALTER DATABASE d CHARACTER SET utf8mb4", "Database"},
	}

	for _, c := range cases {
		stmt, err := Parse(c.sql)
		if err != nil {
			t.Errorf("FAIL %-46q -> %v", c.sql, err)
			continue
		}
		if stmt.Type != c.wantType {
			t.Errorf("FAIL %-46q -> type=%s, want=%s", c.sql, stmt.Type, c.wantType)
		}
	}
}

// TestDatabaseDetails 验证 CREATE/DROP DATABASE 的细节。
func TestDatabaseDetails(t *testing.T) {
	stmt, err := Parse("CREATE DATABASE IF NOT EXISTS mydb")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*DatabaseDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	if details.Action != "create" {
		t.Errorf("action = %q, 期望 create", details.Action)
	}
	if details.Name != "mydb" {
		t.Errorf("name = %q, 期望 mydb", details.Name)
	}
	if !details.IfNotExists {
		t.Error("if_not_exists 应该是 true")
	}
}

// TestReplicationPasswordIsMasked 验证复制语句里的口令被打码。
//
// 解析结果经常要往外发（前端、日志），不能把口令带出去。
func TestReplicationPasswordIsMasked(t *testing.T) {
	stmt, err := Parse("CHANGE MASTER TO MASTER_HOST='h', MASTER_PASSWORD='topsecret', MASTER_USER='repl'")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	details, ok := stmt.Details.(*AdminStatementDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	rendered := strings.Join(details.Options, " ")
	if strings.Contains(rendered, "topsecret") {
		t.Errorf("口令没有被打码: %v", details.Options)
	}
	if !strings.Contains(rendered, "MASTER_USER='repl'") {
		t.Errorf("非口令选项丢了: %v", details.Options)
	}
}

// TestStrippedSyntaxIsRestored 验证"解析前剥离、解析后补回"的信息没有丢。
//
// vitess 不支持这几种语法，解析前要摘掉；摘掉的东西必须回到结果里，
// 否则调用方看到的语句跟用户写的不是一回事。
func TestStrippedSyntaxIsRestored(t *testing.T) {
	// INSERT 的优先级
	for keyword, want := range map[string]string{
		"LOW_PRIORITY":  "low_priority",
		"HIGH_PRIORITY": "high_priority",
		"DELAYED":       "delayed",
	} {
		stmt, err := Parse("INSERT " + keyword + " INTO t (a) VALUES (1)")
		if err != nil {
			t.Errorf("FAIL INSERT %s -> %v", keyword, err)
			continue
		}
		details, ok := stmt.Details.(*InsertDetails)
		if !ok {
			t.Errorf("FAIL INSERT %s -> Details 类型 = %T", keyword, stmt.Details)
			continue
		}
		if details.Priority != want {
			t.Errorf("FAIL INSERT %s -> priority = %q, 期望 %q", keyword, details.Priority, want)
		}
	}

	// GROUP BY ... WITH ROLLUP
	stmt, err := Parse("SELECT a, COUNT(*) FROM t GROUP BY a WITH ROLLUP")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	sel, ok := stmt.Details.(*SelectDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	if !sel.WithRollup {
		t.Error("with_rollup 应该是 true")
	}

	// TRUNCATE 的分区名
	stmt, err = Parse("TRUNCATE TABLE t PARTITION p0")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	trunc, ok := stmt.Details.(*TruncateDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	if trunc.Partition != "p0" {
		t.Errorf("partition = %q, 期望 p0", trunc.Partition)
	}

	// DROP VIEW ... CASCADE
	stmt, err = Parse("DROP VIEW v CASCADE")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	drop, ok := stmt.Details.(*DropDetails)
	if !ok {
		t.Fatalf("Details 类型 = %T", stmt.Details)
	}
	if !drop.Cascade {
		t.Error("cascade 应该是 true")
	}

	// RESTRICT 不是 CASCADE
	stmt, err = Parse("DROP VIEW v RESTRICT")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	drop = stmt.Details.(*DropDetails)
	if drop.Cascade {
		t.Error("RESTRICT 不应该被当成 CASCADE")
	}
}
