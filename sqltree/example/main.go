package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/sqltree"
)

// 这个例子演示 sqltree 库怎么用。
//
// 运行：
//
//	go run ./example
//
// 核心只有一句话：**sqltree.Parse(sql)** —— 不需要 catalog、不需要表结构、
// 不需要任何数据库权限，用户给什么 SQL 就解析什么。
func main() {
	fmt.Println("========== 1. 最简用法：解析一条 SELECT ==========")
	stmt, err := sqltree.Parse("SELECT id, name FROM users WHERE status = 1 LIMIT 10")
	if err != nil {
		panic(err)
	}
	fmt.Printf("语句类型: %s\n", stmt.Type)

	// Details 是对应类型的结构体，可以直接类型断言拿到强类型字段
	if sel, ok := stmt.Details.(*sqltree.SelectDetails); ok {
		fmt.Printf("投影列  : %s\n", strings.Join(sel.SelectedExprs, ", "))
		fmt.Printf("FROM    : %s\n", sel.From)
		fmt.Printf("WHERE   : %s\n", sel.Where)
		fmt.Printf("LIMIT   : %d\n", sel.Limit)
	}

	fmt.Println("\n========== 2. 解析建表语句：拿到完整表结构 ==========")
	ddl := `CREATE TABLE IF NOT EXISTS ` + "`db`" + `.` + "`orders`" + ` (
  id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  user_id BIGINT NOT NULL,
  amount  DECIMAL(20,4) NOT NULL DEFAULT 0.0000,
  status  TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY uk_user (user_id),
  KEY idx_amount ((amount + status)),
  CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表'`

	stmt, err = sqltree.Parse(ddl)
	if err != nil {
		panic(err)
	}
	fmt.Printf("语句类型: %s\n", stmt.Type)
	if table, ok := stmt.Details.(*sqltree.CreateTableDetails); ok {
		fmt.Printf("表名    : %s\n", table.Table)
		fmt.Printf("列      :\n")
		for _, col := range table.Columns {
			fmt.Printf("  - %s %s (null=%v, default=%q)\n",
				col.Name, col.Type, col.Nullable, col.Default)
		}
		fmt.Printf("主键    : %s\n", strings.Join(table.PrimaryKey, ", "))
		fmt.Printf("索引    :\n")
		for _, idx := range table.Indexes {
			cols := strings.Join(idx.Columns, ", ")
			if idx.Expression != "" {
				cols = "(" + idx.Expression + ")" // 函数索引
			}
			fmt.Printf("  - %s [%s]%s\n", idx.Name, cols, map[bool]string{true: " UNIQUE"}[idx.Unique])
		}
		fmt.Printf("外键    :\n")
		for _, fk := range table.ForeignKeys {
			fmt.Printf("  - %s (%s) -> %s (%s)\n",
				fk.Name, strings.Join(fk.Columns, ", "),
				fk.ReferencedTable, strings.Join(fk.ReferencedColumns, ", "))
		}
		fmt.Printf("表选项  : %s\n", strings.Join(table.Options, ", "))
	}

	fmt.Println("\n========== 3. 解析存储过程 ==========")
	proc := `CREATE PROCEDURE p1(IN p_id INT, OUT p_res VARCHAR(64))
BEGIN
  DECLARE v_cnt INT DEFAULT 0;
  SELECT COUNT(*) INTO v_cnt FROM orders WHERE user_id = p_id;
  IF v_cnt > 0 THEN SET p_res = 'found'; ELSE SET p_res = 'empty'; END IF;
END`
	stmt, err = sqltree.Parse(proc)
	if err != nil {
		panic(err)
	}
	fmt.Printf("语句类型: %s\n", stmt.Type)
	if p, ok := stmt.Details.(*sqltree.ProcedureDetails); ok {
		fmt.Printf("过程名  : %s\n", p.Name)
		for _, param := range p.Params {
			fmt.Printf("  参数  : [%s] %s %s\n", param.Direction, param.Name, param.Type)
		}
		fmt.Printf("函数体  : %d 条语句\n", len(p.Statements))
		for _, s := range p.Statements {
			fmt.Printf("  - %s\n", s)
		}
	}

	fmt.Println("\n========== 4. 多语句脚本 ==========")
	stmt, err = sqltree.Parse("SELECT 1; INSERT INTO t (a) VALUES (2); UPDATE t SET a = 3")
	if err != nil {
		panic(err)
	}
	fmt.Printf("语句类型: %s\n", stmt.Type)
	if multi, ok := stmt.Details.(*sqltree.MultiStatementDetails); ok {
		for i, s := range multi.Statements {
			fmt.Printf("  [%d] %s\n", i+1, s.Type)
		}
	}

	fmt.Println("\n========== 5. 表结构完全未知也能解析 ==========")
	// 这张表在任何一个数据库里都不存在——但语句照样解析得出来
	stmt, err = sqltree.Parse("SELECT o.id, SUM(o.amount) FROM some_unknown_table o GROUP BY o.id")
	if err != nil {
		panic(err)
	}
	fmt.Printf("语句类型: %s\n", stmt.Type)
	out, _ := json.MarshalIndent(stmt, "", "  ")
	fmt.Println(string(out))

	fmt.Println("\n========== 6. 完整的 JSON 输出（直接给前端用） ==========")
	stmt, _ = sqltree.Parse("WITH cte AS (SELECT id FROM t) SELECT * FROM cte JOIN u ON u.id = cte.id")
	out, _ = json.MarshalIndent(stmt, "", "  ")
	fmt.Println(string(out))
}
