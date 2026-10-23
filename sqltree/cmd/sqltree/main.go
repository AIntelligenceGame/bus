package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/AIntelligenceGame/bus/sqltree"
)

// 这个文件是 sqltree 的命令行演示。
//
// 设计前提（来自实际需求）：用户随手给一条 SQL，解析器**不知道表结构、
// 也没有权限查 catalog**。所以默认路径完全不建表、不建引擎，
// 纯语法解析（vitess AST 层），engine 传 nil。
//
// 可选开关：
//   --catalog            建一个内存 catalog，用来演示"SELECT * 展开成具体列"
//                        这一个 AST 层补不上的增强
//   --describe <库>.<表> 从 catalog 读出真实表结构（表结构反查）
//
// 换句话说：**表结构不是解析的前提**，只是可选的增强信息。

// usageTemplate 是帮助文本。
//
// 模板里的 @BT@ 代表一个反引号（MySQL 的标识符引用符号）——
// Go 的原始字符串里放不进反引号，所以用占位符，运行时再替换。
const usageTemplate = `sqltree —— SQL 结构化解析器（无需 catalog）

用法:
  sqltree [选项] [SQL ...]

选项:
  --demo                运行内置示例语句集
  --catalog             建一个内存 catalog，用于把 SELECT * 展开成具体列
  --describe <库>.<表>  从 catalog 读取表结构（需要 --catalog）
  --file <路径>         从文件读取 SQL（复杂语句推荐用这个）
  -                     从标准输入读取 SQL（等价于不传参数）
  -h, --help            显示本帮助

示例:
  sqltree "SELECT * FROM unknown_table"
  sqltree 'SELECT * FROM t WHERE a = 1'
  echo "SELECT 1" | sqltree
  sqltree --file query.sql
  sqltree --demo
  sqltree --catalog --describe test.t1

怎么传 SQL（重要）:
  SQL 里有特殊字符（反引号、$、\、单引号）时，**不要用反引号**。
  在 bash 里反引号是"命令替换"：shell 会先把它里面的内容当命令执行掉，
  再把执行结果塞回 SQL。sqltree 收到的是被改写过的 SQL——
  比如 SELECT @BT@echo pwned@BT@ FROM t 会变成 "SELECT pwned FROM t"，
  引号里的反引号标识符则会变成 "command not found"。

  **多行 SQL 的正解是"不作为命令行参数传"**，按省心程度选一种：

  1) 直接粘贴（最省心）：不带任何参数运行 sqltree，把 SQL 粘进去，
     然后按 Ctrl-D 结束。整条 SQL 原封不动地进来，一个字符都不改。

       sqltree
       <粘贴 SQL>
       <Ctrl-D>

  2) heredoc：定界符加单引号，shell 不做任何展开。

       sqltree <<'SQL'
       SELECT @BT@status@BT@, name
       FROM t
       WHERE a = 1
       SQL

  3) 剪贴板：从别人那里复制来的 SQL 用这个最省事，**多行完全没问题**。

       pbpaste | sqltree                          # macOS
       xclip -selection clipboard -o | sqltree    # Linux (X11)
       wl-paste | sqltree                         # Wayland

  4) 文件：复杂语句或脚本用这个，完全绕开 shell。

       sqltree --file query.sql

  5) 单引号包住整条 SQL：可以多行，但 SQL 里不能出现单引号。

       sqltree 'SELECT @BT@status@BT@ FROM t'

  为什么不能不加引号直接往命令行上粘：
    bash 会按空白把多行 SQL 拆成多个参数，**换行就丢了**——
    上一行剩下的内容会被当成下一条命令去执行（报 "FROM: command not found"），
    字符串里连续的空格也会被压成一个。
`

// usage 是最终显示的帮助文本。
var usage = strings.ReplaceAll(usageTemplate, "@BT@", "`")

func main() {
	args := os.Args[1:]
	opts := &demoOptions{}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			fmt.Print(usage)
			return
		case arg == "--demo":
			opts.demo = true
		case arg == "--catalog":
			opts.catalog = true
		case arg == "--describe":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "--describe 需要一个参数：<库>.<表>")
				os.Exit(2)
			}
			i++
			opts.describe = args[i]
		case arg == "--file":
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, "--file 需要一个参数：文件路径")
				os.Exit(2)
			}
			i++
			opts.file = args[i]
		case arg == "-":
			// 显式要求从标准输入读
			opts.stdin = true
		default:
			opts.sql = append(opts.sql, arg)
		}
	}

	// 默认无 catalog：解析器不需要知道任何表结构
	var ctx *sql.Context
	var engine *sqle.Engine

	if opts.catalog || opts.describe != "" {
		ctx, engine = newDemoEngine()
	}

	// --describe：表结构反查
	if opts.describe != "" {
		if engine == nil {
			fmt.Fprintln(os.Stderr, "--describe 需要 catalog（加 --catalog）")
			os.Exit(2)
		}
		dbName, tableName := splitQualifiedName(opts.describe)
		details, err := sqltree.DescribeTable(ctx, engine.Analyzer.Catalog, dbName, tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		printJSON(details)
		return
	}

	// --demo：跑一遍内置示例
	if opts.demo {
		failed := 0
		for _, tc := range demoCases {
			fmt.Println("=" + tc.name + ": " + tc.sql)
			if !parseAndPrint(ctx, engine, tc.sql) {
				failed++
			}
			fmt.Println()
		}
		if failed > 0 {
			fmt.Fprintf(os.Stderr, "%d/%d 条示例语句解析失败\n", failed, len(demoCases))
			os.Exit(1)
		}
		return
	}

	// --file：从文件读 SQL。复杂语句推荐用这个——完全绕开 shell 的引号展开。
	if opts.file != "" {
		raw, err := os.ReadFile(opts.file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读取文件失败: %v\n", err)
			os.Exit(1)
		}
		if strings.TrimSpace(string(raw)) == "" {
			fmt.Fprintf(os.Stderr, "文件 %s 是空的\n", opts.file)
			os.Exit(1)
		}
		if !parseAndPrint(ctx, engine, string(raw)) {
			os.Exit(1)
		}
		return
	}

	// 命令行参数里的 SQL
	if len(opts.sql) > 0 && !opts.stdin {
		if !parseAndPrint(ctx, engine, strings.Join(opts.sql, " ")) {
			os.Exit(1)
		}
		return
	}

	// 从标准输入读
	// 如果 stdin 是终端，说明用户是"直接粘贴"的用法——给个提示，
	// 否则用户会对着一个没反应的终端发愣。
	if isTerminal(os.Stdin) {
		fmt.Fprintln(os.Stderr, "把 SQL 粘贴进来，按 Ctrl-D 结束（多行没问题，不用加引号）：")
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取标准输入失败: %v\n", err)
		os.Exit(1)
	}
	if strings.TrimSpace(string(raw)) == "" {
		fmt.Print(usage)
		return
	}
	if !parseAndPrint(ctx, engine, string(raw)) {
		os.Exit(1)
	}
}

// demoOptions 是命令行选项。
type demoOptions struct {
	demo     bool
	catalog  bool
	describe string
	file     string
	stdin    bool
	sql      []string
}

// newDemoEngine 建一个内存 catalog（含 t1/t2 两张演示表）。
// 只有 --catalog / --describe 才需要它——普通解析完全用不到。
func newDemoEngine() (*sql.Context, *sqle.Engine) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	engine := sqle.NewDefault(pro)

	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("test")

	createTestTables(db)
	return ctx, engine
}

// createTestTables 建两张演示表（t1/t2），带主键。
//
// 注意：这张表**只服务于 --catalog / --describe 演示**，
// 解析器本身不依赖它们。
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

// isTerminal 判断 f 是不是终端（而不是管道或文件）。
//
// 用 Stat 的 ModeCharDevice 判断：终端是字符设备，管道/文件不是。
// 这样不用引第三方库（golang.org/x/term）就能区分"用户在终端里粘贴"
// 和"内容从管道进来"。
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// splitQualifiedName 把 "库.表" 拆成库名与表名。
func splitQualifiedName(name string) (string, string) {
	name = strings.TrimSpace(name)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[:idx], name[idx+1:]
	}
	return "test", name
}

// printJSON 缩进打印 JSON。
func printJSON(value any) {
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Printf("JSON error: %v\n", err)
		return
	}
	fmt.Println(string(out))
}

// parseAndPrint 解析并打印 SQL 的结构化描述。
//
// engine 传 nil 时是纯语法解析，不需要任何表结构；
// 传了 engine 时会额外用 plan 层把 SELECT * 展开成具体列。
//
// 返回 false 表示解析失败（调用方据此给非零退出码）。
func parseAndPrint(ctx *sql.Context, engine *sqle.Engine, sqlStr string) bool {
	stmt, err := sqltree.ParseSQL(ctx, engine, sqlStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return false
	}
	printJSON(stmt)
	return true
}

// demoCases 是内置的示例语句集，覆盖各种语句类型。
// 注意：这些语句里的表名大多是"不存在的表"——
// 恰恰说明解析不依赖表结构。
var demoCases = []struct {
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

	// DDL - 表结构
	{"CREATE TABLE", "CREATE TABLE t3 (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL DEFAULT 'x' COMMENT '名称', value DECIMAL(10,2) UNSIGNED, created_at DATETIME, KEY idx_name (name), UNIQUE KEY uk_val (value), CONSTRAINT fk_t3 FOREIGN KEY (id) REFERENCES t1 (id) ON DELETE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试表'"},
	{"CREATE TABLE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS t4 (id INT PRIMARY KEY)"},
	{"CREATE TABLE AS SELECT", "CREATE TABLE t5 AS SELECT id, name FROM t1"},
	{"CREATE TABLE LIKE", "CREATE TABLE t6 LIKE t1"},
	{"ALTER TABLE add column", "ALTER TABLE t1 ADD COLUMN extra INT NOT NULL DEFAULT 0"},
	{"ALTER TABLE drop column", "ALTER TABLE t1 DROP COLUMN extra"},
	{"ALTER TABLE modify column", "ALTER TABLE t1 MODIFY COLUMN name VARCHAR(128)"},
	{"ALTER TABLE rename column", "ALTER TABLE t1 RENAME COLUMN name TO nickname"},
	{"ALTER TABLE multi actions", "ALTER TABLE t1 ADD COLUMN a INT, DROP COLUMN b, MODIFY COLUMN c VARCHAR(10)"},
	{"ALTER TABLE add index", "ALTER TABLE t1 ADD INDEX idx_status (status)"},
	{"ALTER TABLE add foreign key", "ALTER TABLE t1 ADD CONSTRAINT fk_1 FOREIGN KEY (id) REFERENCES t2 (id)"},
	{"DROP TABLE", "DROP TABLE IF EXISTS t3, t4"},
	{"RENAME TABLE", "RENAME TABLE t5 TO t7, t6 TO t8"},
	{"TRUNCATE TABLE", "TRUNCATE TABLE t7"},
	{"CREATE INDEX", "CREATE UNIQUE INDEX idx_u ON t1 (id, status)"},
	{"DROP INDEX", "DROP INDEX idx_u ON t1"},

	// 视图
	{"CREATE VIEW", "CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE status = 1"},
	{"CREATE OR REPLACE VIEW", "CREATE OR REPLACE VIEW v1 AS SELECT id FROM t1"},
	{"CREATE VIEW with columns", "CREATE VIEW v2 (a, b) AS SELECT id, name FROM t1"},
	{"DROP VIEW", "DROP VIEW IF EXISTS v1, v2"},

	// 存储过程
	{"CREATE PROCEDURE", "CREATE PROCEDURE p1(IN a INT, OUT b VARCHAR(20), INOUT c INT) BEGIN SET b = 'x'; SELECT a INTO c FROM t1 LIMIT 1; END"},
	{"CREATE PROCEDURE simple", "CREATE PROCEDURE p2() SELECT 1"},
	{"CALL", "CALL p1(1, @out, @inout)"},
	{"DROP PROCEDURE", "DROP PROCEDURE IF EXISTS p1"},

	// 触发器
	{"CREATE TRIGGER", "CREATE TRIGGER trg1 BEFORE INSERT ON t1 FOR EACH ROW SET NEW.name = 'x'"},
	{"CREATE TRIGGER with body", "CREATE TRIGGER trg2 AFTER UPDATE ON t1 FOR EACH ROW BEGIN SET @a = 1; SET @b = 2; END"},
	{"DROP TRIGGER", "DROP TRIGGER IF EXISTS trg1"},

	// 函数（vitess 不支持语法，由自研解析器处理）
	{"CREATE FUNCTION", "CREATE FUNCTION f1(p INT) RETURNS INT DETERMINISTIC BEGIN RETURN p + 1; END"},
	{"CREATE FUNCTION simple", "CREATE FUNCTION f2(p VARCHAR(10)) RETURNS VARCHAR(20) NO SQL RETURN CONCAT(p, 'x')"},
	{"DROP FUNCTION", "DROP FUNCTION IF EXISTS f1"},

	// 其它
	{"SET", "SET @a = 1"},
	{"USE", "USE test"},
	{"SHOW TABLES", "SHOW TABLES"},
	{"SHOW CREATE TABLE", "SHOW CREATE TABLE t1"},

	// 完全不存在的表：证明解析不依赖表结构
	{"不存在的表", "SELECT a, b FROM some_unknown_table WHERE c = 1"},
}
