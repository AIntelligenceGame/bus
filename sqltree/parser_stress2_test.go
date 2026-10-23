package sqltree

import (
	"strings"
	"testing"
)

// TestStressExoticStatements 第二批压测：偏门但合法的 MySQL 语法。
//
// 第一批覆盖的是"复杂"，这一批覆盖的是"少见"——
// 分区表、DEFINER、临时表、事务控制、LOAD DATA、PREPARE 等等。
func TestStressExoticStatements(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantType     string
		wantContains []string
	}{
		{
			name:         "分区表 RANGE",
			wantType:     "CreateTable",
			wantContains: []string{"\"type\": \"RANGE\"", "\"p0\"", "\"pmax\""},
			sql: `CREATE TABLE t (
  id INT NOT NULL,
  dt DATE NOT NULL
) ENGINE=InnoDB
PARTITION BY RANGE (YEAR(dt)) (
  PARTITION p0 VALUES LESS THAN (2020),
  PARTITION p1 VALUES LESS THAN (2021),
  PARTITION pmax VALUES LESS THAN MAXVALUE
)`,
		},
		{
			name:         "分区表 HASH",
			wantType:     "CreateTable",
			wantContains: []string{"\"type\": \"HASH\""},
			sql:          `CREATE TABLE t (id INT NOT NULL) PARTITION BY HASH(id) PARTITIONS 4`,
		},
		{
			name:         "分区表 KEY + 子分区",
			wantType:     "CreateTable",
			wantContains: []string{"\"type\": \"KEY\"", "\"sub_partition\""},
			sql: `CREATE TABLE t (id INT NOT NULL, dt DATE NOT NULL)
PARTITION BY KEY (id)
SUBPARTITION BY HASH (TO_DAYS(dt))
SUBPARTITION p_s0, p_s1 (
  PARTITION p0 VALUES LESS THAN (2020),
  PARTITION p1 VALUES LESS THAN MAXVALUE
)`,
		},
		{
			name:         "临时表",
			wantType:     "CreateTable",
			wantContains: []string{"\"temporary\": true"},
			sql:          `CREATE TEMPORARY TABLE tmp_t (id INT PRIMARY KEY, nm VARCHAR(10)) ENGINE=MEMORY`,
		},
		{
			name:         "CREATE TABLE AS SELECT",
			wantType:     "CreateTable",
			wantContains: []string{"\"as_select\""},
			sql:          `CREATE TABLE t2 AS SELECT id, nm FROM t1 WHERE v > 0`,
		},
		{
			name:         "CREATE TABLE LIKE",
			wantType:     "CreateTable",
			wantContains: []string{"\"like\": \"t1\""},
			sql:          `CREATE TABLE t2 LIKE t1`,
		},
		{
			name:         "DEFINER 存储过程",
			wantType:     "CreateProcedure",
			wantContains: []string{"\"name\": \"p1\""},
			sql: `CREATE DEFINER=` + "`root`" + `@` + "`localhost`" + ` PROCEDURE p1(IN a INT)
SQL SECURITY INVOKER
COMMENT '过程注释'
BEGIN
  SELECT a;
END`,
		},
		{
			name:         "DEFINER 视图",
			wantType:     "CreateView",
			wantContains: []string{"\"view_name\": \"v\""},
			sql:          `CREATE DEFINER=` + "`root`" + `@` + "`%`" + ` SQL SECURITY INVOKER VIEW v AS SELECT id FROM t`,
		},
		{
			name:         "DEFINER 触发器",
			wantType:     "CreateTrigger",
			wantContains: []string{"\"trigger_name\": \"trg\""},
			sql:          `CREATE DEFINER=` + "`root`" + `@` + "`%`" + ` TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.c = 1`,
		},
		{
			name:     "ALTER TABLE 算法与锁",
			wantType: "Alter",
			sql:      `ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE`,
		},
		{
			name:     "ALTER TABLE 改字符集",
			wantType: "Alter",
			sql:      `ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`,
		},
		{
			name:         "ALTER TABLE 改引擎",
			wantType:     "Alter",
			wantContains: []string{"\"option\""},
			sql:          `ALTER TABLE t ENGINE=InnoDB`,
		},
		{
			name:         "ALTER TABLE 改注释",
			wantType:     "Alter",
			wantContains: []string{"COMMENT="},
			sql:          `ALTER TABLE t COMMENT='新的注释'`,
		},
		{
			name:     "ALTER TABLE 重命名表",
			wantType: "Alter",
			sql:      `ALTER TABLE t RENAME TO t_new`,
		},
		{
			name:         "ALTER TABLE 改列默认值",
			wantType:     "Alter",
			wantContains: []string{"\"alter_column\""},
			sql:          `ALTER TABLE t ALTER COLUMN c SET DEFAULT 100`,
		},
		{
			name:     "ALTER TABLE 改列类型",
			wantType: "Alter",
			sql:      `ALTER TABLE t MODIFY c BIGINT UNSIGNED NOT NULL`,
		},
		{
			name:     "ALTER TABLE 删除外键",
			wantType: "Alter",
			sql:      `ALTER TABLE t DROP FOREIGN KEY fk_c`,
		},
		{
			name:     "ALTER TABLE 添加主键",
			wantType: "Alter",
			sql:      `ALTER TABLE t ADD PRIMARY KEY (id)`,
		},
		{
			name:     "ALTER TABLE 添加 CHECK",
			wantType: "Alter",
			sql:      `ALTER TABLE t ADD CONSTRAINT ck CHECK (v > 0)`,
		},
		{
			name:         "CREATE INDEX 带 USING",
			wantType:     "CreateIndex",
			wantContains: []string{"\"action\": \"create\"", "\"idx_c\""},
			sql:          `CREATE INDEX idx_c ON t (c) USING BTREE`,
		},
		{
			name:         "CREATE UNIQUE INDEX",
			wantType:     "CreateIndex",
			wantContains: []string{"\"unique\": true"},
			sql:          `CREATE UNIQUE INDEX uk_c ON t (c)`,
		},
		{
			name:         "DROP INDEX ON 表",
			wantType:     "DropIndex",
			wantContains: []string{"\"action\": \"drop\""},
			sql:          `DROP INDEX idx_c ON t`,
		},
		{
			name:         "REPLACE INTO",
			wantType:     "Insert",
			wantContains: []string{"\"table\": \"t\""},
			sql:          `REPLACE INTO t (id, nm) VALUES (1, 'a')`,
		},
		{
			name:     "INSERT IGNORE",
			wantType: "Insert",
			sql:      `INSERT IGNORE INTO t (id, nm) VALUES (1, 'a')`,
		},
		{
			name:         "INSERT 低优先级",
			wantType:     "Insert",
			wantContains: []string{"\"table\": \"t\""},
			sql:          `INSERT LOW_PRIORITY INTO t (id, nm) VALUES (1, 'a')`,
		},
		{
			name:         "SELECT FOR UPDATE",
			wantType:     "Select",
			wantContains: []string{"\"from\": \"t\""},
			sql:          `SELECT id FROM t WHERE v > 0 FOR UPDATE`,
		},
		{
			name:     "SELECT FOR UPDATE SKIP LOCKED",
			wantType: "Select",
			sql:      `SELECT id FROM t WHERE v > 0 FOR UPDATE SKIP LOCKED`,
		},
		{
			name:     "SELECT INTO OUTFILE",
			wantType: "Select",
			sql:      `SELECT id FROM t INTO OUTFILE '/tmp/out.txt'`,
		},
		{
			name:         "SELECT GROUP BY ROLLUP",
			wantType:     "Select",
			wantContains: []string{"\"group_by\""},
			sql:          `SELECT a, b, SUM(v) FROM t GROUP BY a, b WITH ROLLUP`,
		},
		{
			name:     "SELECT SQL_CALC_FOUND_ROWS",
			wantType: "Select",
			sql:      `SELECT SQL_CALC_FOUND_ROWS id FROM t LIMIT 10`,
		},
		{
			name:     "SELECT STRAIGHT_JOIN",
			wantType: "Select",
			sql:      `SELECT STRAIGHT_JOIN t1.id FROM t1, t2 WHERE t1.id = t2.id`,
		},
		{
			name:     "事务控制",
			wantType: "Begin",
			sql:      `START TRANSACTION`,
		},
		{
			name:     "SET TRANSACTION",
			wantType: "Set",
			sql:      `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`,
		},
		{
			name:         "LOCK TABLES",
			wantType:     "LockTables",
			wantContains: []string{"\"lock\""},
			sql:          `LOCK TABLES t READ, t2 WRITE`,
		},
		{
			name:     "UNLOCK TABLES",
			wantType: "UnlockTables",
			sql:      `UNLOCK TABLES`,
		},
		{
			name:         "PREPARE",
			wantType:     "Prepare",
			wantContains: []string{"\"name\": \"stmt\""},
			sql:          `PREPARE stmt FROM 'SELECT * FROM t WHERE id = ?'`,
		},
		{
			name:         "EXECUTE",
			wantType:     "Execute",
			wantContains: []string{"\"name\": \"stmt\""},
			sql:          `EXECUTE stmt USING @a, @b`,
		},
		{
			name:         "DEALLOCATE",
			wantType:     "Deallocate",
			wantContains: []string{"\"name\": \"stmt\""},
			sql:          `DEALLOCATE PREPARE stmt`,
		},
		{
			name:         "LOAD DATA INFILE",
			wantType:     "Load",
			wantContains: []string{"\"infile\""},
			sql:          `LOAD DATA INFILE '/tmp/a.csv' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES (id, nm)`,
		},
		{
			name:         "DO",
			wantType:     "Do",
			wantContains: []string{"\"expression\": \"1\""},
			sql:          `DO 1`,
		},
		{
			name:         "EXPLAIN",
			wantType:     "Explain",
			wantContains: []string{"\"query\""},
			sql:          `EXPLAIN SELECT id FROM t`,
		},
		{
			name:         "TRUNCATE 分区",
			wantType:     "Truncate",
			wantContains: []string{"\"table\": \"t\""},
			sql:          `TRUNCATE TABLE t PARTITION p0`,
		},
		{
			name:     "DROP TABLE 多表带约束",
			wantType: "Drop",
			sql:      `DROP TABLE IF EXISTS a, b RESTRICT`,
		},
		{
			name:         "DROP VIEW",
			wantType:     "Drop",
			wantContains: []string{"\"kind\": \"view\""},
			sql:          `DROP VIEW IF EXISTS v1, v2 CASCADE`,
		},
		{
			name:         "DROP FUNCTION",
			wantType:     "Drop",
			wantContains: []string{"\"kind\": \"function\""},
			sql:          `DROP FUNCTION IF EXISTS f1`,
		},
		{
			name:     "DROP PROCEDURE",
			wantType: "Drop",
			sql:      `DROP PROCEDURE IF EXISTS p1`,
		},
		{
			name:     "DROP TRIGGER",
			wantType: "Drop",
			sql:      `DROP TRIGGER IF EXISTS trg`,
		},
		{
			name:         "SHOW 各种",
			wantType:     "Show",
			wantContains: []string{"\"table\": \"t\""},
			sql:          `SHOW FULL COLUMNS FROM t`,
		},
		{
			name:     "SHOW INDEX",
			wantType: "Show",
			sql:      `SHOW INDEX FROM t`,
		},
		{
			name:     "SHOW VARIABLES",
			wantType: "Show",
			sql:      `SHOW VARIABLES LIKE 'max_connections'`,
		},
		{
			name:     "SHOW STATUS",
			wantType: "Show",
			sql:      `SHOW GLOBAL STATUS`,
		},
		{
			name:     "SHOW DATABASES",
			wantType: "Show",
			sql:      `SHOW DATABASES`,
		},
		{
			name:     "SHOW ENGINES",
			wantType: "Show",
			sql:      `SHOW ENGINES`,
		},
		{
			name:     "SHOW PROCESSLIST",
			wantType: "Show",
			sql:      `SHOW PROCESSLIST`,
		},
		{
			name:     "USE 加限定",
			wantType: "Use",
			sql:      `USE db1`,
		},
		{
			name:         "带库名的 SELECT",
			wantType:     "Select",
			wantContains: []string{"db1.t1"},
			sql:          `SELECT db1.t1.id FROM db1.t1 WHERE db1.t1.v > 0`,
		},
		{
			name:     "带库名的 UPDATE",
			wantType: "Update",
			sql:      `UPDATE db1.t1 SET v = 1 WHERE id = 2`,
		},
		{
			name:     "带库名的 DELETE",
			wantType: "Delete",
			sql:      `DELETE FROM db1.t1 WHERE id = 2`,
		},
		{
			name:     "带库名的 INSERT",
			wantType: "Insert",
			sql:      `INSERT INTO db1.t1 (id) VALUES (1)`,
		},
		{
			name:     "递归 CTE 带多个",
			wantType: "Select",
			sql: `WITH RECURSIVE a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3)
SELECT * FROM a, b, c`,
		},
		{
			name:         "INTERSECT",
			wantType:     "Select",
			wantContains: []string{"\"set_op\""},
			sql:          `SELECT id FROM a INTERSECT SELECT id FROM b`,
		},
		{
			name:         "EXCEPT",
			wantType:     "Select",
			wantContains: []string{"\"set_op\""},
			sql:          `SELECT id FROM a EXCEPT SELECT id FROM b`,
		},
		{
			name:     "WITH RECURSIVE 引用多次",
			wantType: "Select",
			sql: `WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 100)
SELECT * FROM cte a JOIN cte b ON a.n = b.n`,
		},
		{
			name:         "多语句",
			wantType:     "Multi",
			wantContains: []string{"\"statements\""},
			sql:          `SELECT 1; SELECT 2`,
		},
		{
			name:     "JSON 函数",
			wantType: "Select",
			sql:      `SELECT JSON_EXTRACT(j, '$.a'), JSON_SET(j, '$.b', 1), JSON_ARRAY(1,2), JSON_OBJECT('k', 'v') FROM t`,
		},
		{
			name:     "子查询在 FROM 里带聚合",
			wantType: "Select",
			sql:      `SELECT s.* FROM (SELECT id, SUM(v) sv FROM t GROUP BY id HAVING SUM(v) > 100) s WHERE s.sv < 1000`,
		},
		{
			name:     "相关子查询在 SELECT 里",
			wantType: "Select",
			sql:      `SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.id = t.id AND t2.dt > t.dt) c FROM t`,
		},
		{
			name:     "EXISTS 与 NOT EXISTS 混用",
			wantType: "Select",
			sql:      `SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t2) AND NOT EXISTS (SELECT 1 FROM t3 WHERE t3.id = t.id)`,
		},
		{
			name:     "IN 与 NOT IN 混用",
			wantType: "Select",
			sql:      `SELECT id FROM t WHERE id IN (1,2,3) AND id NOT IN (SELECT id FROM t2)`,
		},
		{
			name:     "BETWEEN 与 CASE 混用",
			wantType: "Select",
			sql:      `SELECT CASE WHEN v BETWEEN 1 AND 10 THEN 'low' WHEN v BETWEEN 11 AND 20 THEN 'mid' ELSE 'high' END FROM t`,
		},
		{
			name:     "LIKE 各种通配",
			wantType: "Select",
			sql:      `SELECT id FROM t WHERE nm LIKE '%a%' OR nm LIKE 'b_' OR nm LIKE 'c\%' OR nm REGEXP '^x'`,
		},
		{
			name:     "UNION 带 ORDER BY LIMIT",
			wantType: "Select",
			sql:      `SELECT id FROM a UNION SELECT id FROM b ORDER BY id DESC LIMIT 5`,
		},
		{
			name:     "UNION 带列别名",
			wantType: "Select",
			sql:      `SELECT id AS x FROM a UNION SELECT id AS x FROM b`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseSQL(nil, nil, tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}
			if stmt == nil {
				t.Errorf("返回 nil")
				return
			}
			if stmt.Type != tc.wantType {
				t.Errorf("类型 = %q, 期望 %q", stmt.Type, tc.wantType)
			}
			rendered := renderDetails(stmt.Details)
			if rendered == "" {
				t.Errorf("details 渲染为空, type=%s", stmt.Type)
				return
			}
			for _, want := range tc.wantContains {
				if !strings.Contains(rendered, want) {
					t.Errorf("输出中缺少 %q\n实际输出: %s", want, rendered)
				}
			}
		})
	}
}
