# sqltree —— 无需 catalog 的 MySQL SQL 结构化解析器

把一条 MySQL 语句解析成结构化的 JSON（表结构、列、索引、外键、CTE、集合运算、
存储过程参数与函数体……），**不需要知道表结构，也不需要任何数据库权限**。

## 为什么需要它

真实场景是：用户随手贴一条 SQL 过来，你既不知道他连的是哪张表，
也没有权限去查 catalog。这时候：

- go-mysql-server 的 `Engine.AnalyzeQuery`（语义层）**直接失败**——它要求表真实存在且有主键；
- 但它的**语法层**（底层是 `github.com/dolthub/vitess` 的 `sqlparser`）是纯语法解析，
  表在不在都照样能解析出结构。

sqltree 就是把语法层做成主力、把语义层降级成"可选增强"的产物。

```
用户给的 SQL ──▶ AST 层（vitess 语法层）──▶ 结构化 JSON   ← 主力，不需要 catalog
                     │
                     └──▶ plan 层（analyzer 语义层）    ← 可选增强：把 SELECT * 展开成具体列
```

**关键约定：输出格式与"表是否存在"无关。** 有 catalog 和没 catalog，
同一条语句输出的 JSON 结构完全一致（见 `parser_catalog_test.go`）。

## 快速开始

### 作为库调用（推荐）

```go
import "github.com/AIntelligenceGame/bus/sqltree"

stmt, err := sqltree.Parse("SELECT id, name FROM users WHERE status = 1 LIMIT 10")
if err != nil {
    return err
}

fmt.Println(stmt.Type)   // "Select"

if sel, ok := stmt.Details.(*sqltree.SelectDetails); ok {
    fmt.Println(sel.SelectedExprs)  // [id name]
    fmt.Println(sel.From)            // users
    fmt.Println(sel.Where)           // `status` = 1
    fmt.Println(sel.Limit)           // 10
}
```

`sqltree.Parse` 就是全部——**不需要 engine、不需要 ctx、不需要 catalog**。

完整可运行的例子（在 bus 模块根目录下执行）：

```bash
go run ./sqltree/example
```

### 作为命令行工具

```bash
# 编译（在 bus 模块根目录下执行）
go build -o sqltree ./sqltree/cmd/sqltree

# 直接给 SQL
./sqltree "SELECT id, name FROM users WHERE status = 1"

# 从管道读
echo "CREATE TABLE t (id INT PRIMARY KEY)" | ./sqltree

# 跑内置示例集
./sqltree --demo

# 建一个内存 catalog，演示 SELECT * 展开成具体列
./sqltree --catalog "SELECT * FROM t1"

# 表结构反查（需要真实 catalog）
./sqltree --catalog --describe test.t1
```

输出是缩进 JSON，直接可以给前端用。

### 怎么把 SQL 传给命令行（重要）

SQL 里常有特殊字符：`（反引号）` 标识符引用、`$`、`\`、单引号、多行……
**这些都不是 sqltree 的问题，是 shell 的引号规则。**

**核心结论：多行 SQL 的正解是「不作为命令行参数传」。** 按省心程度选一种：

```bash
# 1) 直接粘贴（最省心）：不带任何参数运行 sqltree，粘进去，Ctrl-D 结束。
#    整条 SQL 原封不动地进来，一个字符都不改。
sqltree
<粘贴 SQL>
<Ctrl-D>

# 2) heredoc：定界符加单引号，shell 不做任何展开
sqltree <<'SQL'
SELECT `status`, name
FROM users
WHERE status = 1
SQL

# 3) 剪贴板：从别人那里复制来的 SQL 用这个最省事，多行完全没问题
pbpaste | sqltree                          # macOS
xclip -selection clipboard -o | sqltree    # Linux (X11)
wl-paste | sqltree                         # Wayland

# 4) 文件：复杂 SQL 或脚本，完全绕开 shell
sqltree --file query.sql

# 5) 单引号包住整条 SQL：可以多行，但 SQL 里不能出现单引号
sqltree 'SELECT `status` FROM users'
```

### 哪些写法会踩坑

| 写法 | 结果 |
|------|------|
| `（反引号）…（反引号）` | **千万别用**：bash 的反引号是"命令替换"，里面的内容会被**执行**掉再替换回 SQL |
| `"SELECT \`status\` FROM t"` | **危险**：双引号里 bash 仍然做命令替换，`（反引号）status（反引号）` 会被 shell 当命令执行 |
| `"SELECT $col FROM t"` | **危险**：双引号里 `$col` 会被 shell 展开成环境变量 |
| 不加引号直接粘多行 | **危险**：bash 按空白把 SQL 拆成多个参数，**换行丢失**——上一行剩下的内容会被当成下一条命令执行（`FROM: command not found`），字符串里连续的空格也被压成一个 |
| `'SELECT （反引号）status（反引号） FROM t'` | 安全，但 SQL 里不能出现单引号 |

> 实际效果：把 `SELECT （反引号）echo pwned（反引号） FROM t` 交给 shell，
> sqltree 收到的是 `SELECT pwned FROM t`——SQL 已经被 shell 改写了。

作为库调用时（`sqltree.Parse(sql)`）根本没有这个问题——SQL 是从 Go 字符串里来的，
不经过 shell。

### 在你自己的项目里引用

sqltree 是 `github.com/AIntelligenceGame/bus` 模块下的一个普通包（没有独立的 go.mod），
所以 import 路径就是仓库地址 + 目录：

```go
import "github.com/AIntelligenceGame/bus/sqltree"
```

在消费方的 `go.mod` 里加：

```
require github.com/AIntelligenceGame/bus v0.0.0
```

然后 `go mod tidy` 即可。

> 本地还没发布到远端时，用 `replace` 指到本地路径：
>
> ```
> require github.com/AIntelligenceGame/bus v0.0.0
>
> replace github.com/AIntelligenceGame/bus => ../../bus
> ```

## 支持的语句类型

| 类别 | 语句 |
|------|------|
| DML | `SELECT` / `INSERT` / `REPLACE` / `UPDATE` / `DELETE`（含 JOIN、子查询、CTE、UNION/INTERSECT/EXCEPT、窗口函数） |
| DDL | `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` / `RENAME TABLE` / `TRUNCATE` / `CREATE`·`DROP INDEX` |
| 视图 | `CREATE [OR REPLACE] VIEW` / `DROP VIEW` |
| 存储过程 | `CREATE PROCEDURE`（参数方向、类型、函数体逐条语句）/ `CALL` / `DROP PROCEDURE` |
| 函数 | `CREATE [OR REPLACE] [AGGREGATE] FUNCTION` / `DROP FUNCTION` |
| 触发器 | `CREATE TRIGGER`（时机、事件、触发体）/ `DROP TRIGGER` |
| 事务 | `START TRANSACTION` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `RELEASE SAVEPOINT` |
| 库管理 | `CREATE`·`ALTER`·`DROP DATABASE` |
| 复制 | `START`·`STOP`·`RESET REPLICA/SLAVE` / `CHANGE MASTER`·`CHANGE REPLICATION SOURCE`·`CHANGE REPLICATION FILTER` / `PURGE BINARY LOGS` |
| 管理 | `ANALYZE` / `OPTIMIZE` / `CHECK` / `CHECKSUM` / `REPAIR TABLE` / `FLUSH` / `KILL` / `SHOW ENGINE INNODB STATUS` / `SHOW BINLOG EVENTS` |
| 权限 | `GRANT` / `REVOKE` / `CREATE`·`DROP USER` / `CREATE`·`DROP ROLE` / `SET PASSWORD` |
| 其它 | `SET` / `USE` / `SHOW` / `PREPARE` / `EXECUTE` / `DEALLOCATE` / `LOAD DATA` / `EXPLAIN` / `DO` / 多语句脚本 |

## 输出的 JSON 长什么样

### SELECT

```json
{
  "type": "Select",
  "details": {
    "selected_exprs": ["o.id", "SUM(o.amount)"],
    "from": "some_unknown_table as o",
    "where": "o.`status` = 1",
    "group_by": ["o.id"],
    "having": "SUM(o.amount) > 100",
    "order_by": ["o.id"],
    "limit": 10,
    "offset": 20
  }
}
```

### 集合运算（UNION / INTERSECT / EXCEPT）

各分支的表名和条件在 `set_op_branches` 里，顶层的 `selected_exprs` 是两侧投影的合并。

```json
{
  "type": "Select",
  "details": {
    "selected_exprs": ["a", "b"],
    "set_op": "union",
    "set_op_branches": [
      {"selected_exprs": ["a"], "from": "t1", "where": "x = 1"},
      {"selected_exprs": ["b"], "from": "t2", "where": "y = 2"}
    ]
  }
}
```

### CREATE TABLE

```json
{
  "type": "CreateTable",
  "details": {
    "table": "db.orders",
    "if_not_exists": true,
    "columns": [
      {"name": "id", "type": "BIGINT unsigned", "nullable": false, "auto_increment": true, "comment": "主键"},
      {"name": "amount", "type": "DECIMAL(20,4)", "nullable": false, "has_default": true, "default": "0.0000"},
      {"name": "gen", "type": "INT", "nullable": true, "generated": "(amount * 2)"}
    ],
    "primary_key": ["id"],
    "indexes": [
      {"name": "uk_user", "type": "UNIQUE KEY", "unique": true, "columns": ["user_id"]},
      {"name": "idx_err", "type": "KEY", "columns": ["error_msg"], "column_prefixes": {"error_msg": 30}},
      {"name": "idx_amount", "type": "KEY", "columns": [], "expression": "amount + status"}
    ],
    "foreign_keys": [
      {"name": "fk_user", "columns": ["user_id"], "referenced_table": "users", "referenced_columns": ["id"], "on_delete": "cascade"}
    ],
    "checks": [{"name": "ck_amt", "expression": "amt >= 0", "enforced": true}],
    "options": ["ENGINE=InnoDB", "CHARACTER SET=utf8mb4", "COMMENT=订单表"],
    "partition": {"type": "RANGE", "columns": ["dt"], "definitions": [...]}
  }
}
```

### CREATE PROCEDURE

```json
{
  "type": "CreateProcedure",
  "details": {
    "name": "p1",
    "params": [
      {"direction": "in", "name": "p_id", "type": "INT"},
      {"direction": "out", "name": "p_res", "type": "VARCHAR(64)"}
    ],
    "characteristics": ["DETERMINISTIC"],
    "body": "BEGIN ... END",
    "statements": ["declare v_cnt INT default 0", "select ... into v_cnt", "if ... end if"]
  }
}
```

## 注释怎么处理

开发人员写 SQL 时注释可能出现在**任何位置**，格式也因人而异。
sqltree 的做法是：**在入口把注释统一归一化掉**，之后 vitess 语法层和所有自研扫描器
看到的都是同一份"干净"的文本。

支持的注释写法（与 MySQL 一致）：

| 写法 | 处理 |
|------|------|
| `-- 注释` | 去掉（MySQL 要求 `--` 后跟空白才算注释） |
| `# 注释` | 去掉（MySQL 特有） |
| `/* 注释 */` | 去掉，替换成一个空格 |
| `/*!40101 SET NAMES utf8 */` | **MySQL 会执行里面的内容**，所以还原成真正的 SQL（去掉版本号） |
| `/*+ 优化器提示 */` | 当普通注释去掉 |
| `'字符串里的 -- 和 /* */'` | 原样保留——那不是注释 |

因此下面这些都能正常解析，且**结果里不会残留注释**：

```sql
SELECT a, /* 中间插一句 */ b FROM t
SELECT a FROM t WHERE /* 条件 */ id = 1
CREATE TABLE t ( /* 列说明 */ id INT, -- 主键
                  name VARCHAR(20) )
ALTER TABLE t ENGINE=InnoDB /* 换引擎 */ DEFAULT CHARSET=utf8mb4
SELECT 'a -- b' AS x          -- 字符串里的内容不是注释
```

只有 MySQL 本身也不接受的写法才会解析失败，例如**嵌套块注释**
（`/* a /* b */ c */`——MySQL 的块注释不嵌套，遇到第一个 `*/` 就结束），
以及版本注释后面直接跟另一条语句却不加分号。

实现见 `comment.go`（归一化）与 `literal_scan.go`（字面量/注释区域标记），
回归测试见 `parser_comment_test.go`。

## API 一览

| 函数 / 类型 | 用途 |
|-------------|------|
| `sqltree.Parse(sql string) (*SQLStatement, error)` | **最常用**。纯语法解析，不需要任何上下文 |
| `sqltree.ParseSQL(ctx, engine, sql string)` | 可选增强版：传 `engine` 则把 `SELECT *` 展开成具体列 |
| `sqltree.DescribeTable(ctx, catalog, db, table)` | 表结构反查：读真实 catalog 里的列/主键/索引/外键 |
| `sqltree.SQLStatement` | `{Type string, Details any}`。结构化语句的 `Details` 是**指针**；`SET`/`USE`/`SHOW`/`COMMIT` 等简单语句用 `map[string]string` |
| `sqltree.SelectDetails` / `InsertDetails` / `UpdateDetails` / `DeleteDetails` | DML 细节 |
| `sqltree.CreateTableDetails` / `AlterTableDetails` / `DropDetails` / `RenameTableDetails` | DDL 细节 |
| `sqltree.ViewDetails` / `ProcedureDetails` / `TriggerDetails` / `FunctionDetails` | 视图、存储过程、触发器、函数 |
| `sqltree.MultiStatementDetails` | 多语句脚本 |
| `sqltree.DatabaseDetails` | 库管理（CREATE/ALTER/DROP DATABASE） |
| `sqltree.AdminStatementDetails` | 复制与运维语句（口令类选项自动打码） |
| `sqltree.PartitionDetails` / `sqltree.IndexDetails` / `sqltree.ColumnInfo` | 分区、索引、列 |
| `sqltree.TableSchemaDetails` | 表结构反查的结果 |

结构化语句的 `Details` 一律是指针，类型断言统一写成 `stmt.Details.(*sqltree.SelectDetails)`。
`SET` / `USE` / `SHOW` / `COMMIT` / `ROLLBACK` / `UNLOCK TABLES` 这几类简单语句
没有固定结构，`Details` 是 `map[string]string`（键分别是 `set`/`database`/`show`/`table`）。

### vitess 不支持的语法怎么办

vitess 的语法层有几处缺口。这些语法在解析前会被**摘掉**让 vitess 能解析，
解析后再**补回结果**——所以输出里不会丢信息：

| 语法 | 输出字段 |
|------|---------|
| `INSERT LOW_PRIORITY` / `HIGH_PRIORITY` / `DELAYED` | `insert.priority` |
| `GROUP BY ... WITH ROLLUP` | `select.with_rollup` |
| `TRUNCATE TABLE t PARTITION p0` | `truncate.partition` |
| `DROP VIEW v CASCADE` | `drop.cascade` |
| `ALTER TABLE t ENGINE=x DEFAULT CHARSET=y`（空格分隔多选项） | 拆成多条 `option` 动作，保留原文字样 |
| 函数索引 `KEY idx ((expr))` | `indexes[].expression` |
| `PARTITION BY ...`（RANGE/LIST/KEY/HASH + SUBPARTITION） | `partition` |
| `ZEROFILL` | `columns[].zerofill` 与类型文本 |

## 目录结构

```
sqltree/
├── parser.go              核心：Parse / ParseSQL 分派，plan 层辅助
├── ast_parser.go          vitess AST → 结构化 Details（30+ 种语句类型）
├── ast_fixup.go           语法缺陷改写（函数索引等）+ 文本扫描工具
├── syntax_fixup.go        "剥离 + 记录"式语法补丁
├── comment.go             注释归一化（入口统一处理）
├── literal_scan.go        字面量/注释区域标记
├── partition.go           PARTITION 子句解析器（RANGE/RANGE COLUMNS/KEY/HASH + SUBPARTITION）
├── multi_statement.go     多语句拆分、DO 语句
├── func_parser.go         CREATE / DROP FUNCTION
├── admin_parser.go        管理类语句（FLUSH/ANALYZE/CHECK/KILL/权限…）
├── schema.go              表结构反查（需要真实 catalog）
├── cmd/sqltree/main.go    命令行入口
├── example/main.go        可运行的用法示例
└── *_test.go              回归测试（280+ 用例）
```

## 测试

在 bus 模块根目录下执行：

```bash
go test ./sqltree/...              # 全部跑一遍
go test ./sqltree/... -v           # 看每个用例
go test -run TestStress ./sqltree/... -v   # 只跑压测用例
```

压测文件：

| 文件 | 覆盖 |
|------|------|
| `parser_nocatalog_test.go` | 18 类语句，**engine 传 nil、完全无 catalog** |
| `parser_stress_test.go` | 26 条复杂语句（CTE、窗口函数、多表 JOIN、建表全家桶、存储过程…） |
| `parser_stress2_test.go` | 74 条冷门语句（分区、DEFINER、事务控制、LOCK、PREPARE、LOAD…） |
| `parser_stress3_test.go` | 70 条管理与运维语句（FLUSH、CHECKSUM、KILL、GRANT、JSON_TABLE…） |
| `parser_catalog_test.go` | 输出格式与表是否存在无关 |
| `parser_comment_test.go` | 注释出现在任何位置都能解析，且结果里不残留注释 |
| `parser_robust_test.go` | 畸形 SQL / 随机 SQL **不 panic**（没闭合的引号、没闭合的注释、截断的关键字） |
| `parser_api_test.go` | 公共 API 形状（Details 的类型约定、列类型不丢精度） |

## 已知边界

- **SELECT \*** 在没有 catalog 时保持 `"*"` 不展开——AST 是纯语法解析，不知道表里有哪些列。
- **列的完整 DDL 文本**（比如 `UNSIGNED`、`ZEROFILL`）会保留在 `type` 字段里；
  但 `ALTER TABLE` 的表选项（`ENGINE=`、`COMMENT=`）和 `ALTER COLUMN ... SET DEFAULT`
  vitess 的 AST 里**根本没有对应字段**，这些信息是从**原文**里回捞的。
- `ALTER TABLE` 的表选项：MySQL 允许一条 ALTER 里用空格分隔多个选项
  （`ALTER TABLE t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`），
  vitess 不认，会被改写成逗号分隔后再解析——**结果里保留用户写的原文字样**。
- 权限类语句（GRANT/REVOKE/用户管理）只保留原文与类别，不做细粒度结构化。
- 存储过程/触发器的函数体按分号切成语句列表，不做语句级语义分析。
- 注释会被归一化掉（见上文"注释怎么处理"），所以**解析结果里拿不到原始注释**——
  需要保留注释请自己留一份原文。

## 依赖

- `github.com/dolthub/go-mysql-server v0.20.0`（语义层，可选）
- `github.com/dolthub/vitess`（语法层，必需）
