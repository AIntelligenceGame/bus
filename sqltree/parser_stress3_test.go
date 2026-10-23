package sqltree

import (
	"testing"
)

// TestStressAdminStatements 压测管理与运维类语句。
//
// 这些语句在 vitess 里要么没有对应的展示节点，要么干脆没有语法，
// 解析器必须自己兜住，不能报"无法解析"。
func TestStressAdminStatements(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantType string
	}{
		{"递归 CTE", `WITH RECURSIVE tree AS (SELECT id, parent_id, 1 lvl FROM nodes WHERE parent_id IS NULL UNION ALL SELECT n.id, n.parent_id, t.lvl+1 FROM nodes n JOIN tree t ON n.parent_id = t.id WHERE t.lvl < 32) SELECT * FROM tree ORDER BY lvl`, "Select"},
		{"FOR UPDATE 与 hint", `SELECT /*+ MAX_EXECUTION_TIME(1000) */ a.id FROM a JOIN b USING (id) LEFT JOIN c ON c.id = a.id WHERE a.x IN (1,2,3) AND a.y BETWEEN 1 AND 10 ORDER BY a.id DESC LIMIT 10 OFFSET 5 FOR UPDATE`, "Select"},
		{"JSON_TABLE 表函数", `SELECT jt.a FROM JSON_TABLE('[{"a":1}]', '$[*]' COLUMNS (a INT PATH '$.a')) AS jt`, "Select"},
		{"GROUP BY WITH ROLLUP", `SELECT a, COUNT(*) c FROM t GROUP BY a WITH ROLLUP HAVING c > 1 ORDER BY c DESC`, "Select"},
		{"UNION INTERSECT EXCEPT", `SELECT * FROM t1 UNION SELECT * FROM t2 INTERSECT SELECT * FROM t3 EXCEPT SELECT * FROM t4`, "Select"},
		{"INSERT 行别名", `INSERT INTO t (a,b) VALUES (1,2) AS new ON DUPLICATE KEY UPDATE b = new.b + 1`, "Insert"},
		{"INSERT SET", `INSERT INTO t SET a = 1, b = DEFAULT`, "Insert"},
		{"INSERT LOW_PRIORITY", `INSERT LOW_PRIORITY IGNORE INTO t (a) VALUES (1)`, "Insert"},
		{"REPLACE", `REPLACE INTO t (a,b) VALUES (1,2)`, "Insert"},
		{"UPDATE ORDER BY LIMIT", `UPDATE t SET a = 1 ORDER BY id LIMIT 10`, "Update"},
		{"DELETE ORDER BY LIMIT", `DELETE FROM t WHERE a = 1 ORDER BY id LIMIT 5`, "Delete"},
		{"CREATE TABLE 函数索引", `CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, j JSON, g INT GENERATED ALWAYS AS (id*2) STORED, PRIMARY KEY(id), KEY k ((id+1)), CONSTRAINT c1 CHECK (id > 0)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, "CreateTable"},
		{"PARTITION BY RANGE", `CREATE TABLE t2 (id INT, dt DATE) PARTITION BY RANGE (TO_DAYS(dt)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2020-01-01')), PARTITION p1 VALUES LESS THAN MAXVALUE)`, "CreateTable"},
		{"PARTITION BY KEY", `CREATE TABLE t3 (id INT, name VARCHAR(10)) PARTITION BY KEY (id) PARTITIONS 4`, "CreateTable"},
		{"PARTITION BY HASH", `CREATE TABLE t4 (id INT) PARTITION BY HASH (id) PARTITIONS 8`, "CreateTable"},
		{"PARTITION BY RANGE COLUMNS", `CREATE TABLE t5 (id INT, dt DATE) PARTITION BY RANGE COLUMNS (dt) (PARTITION p0 VALUES LESS THAN ('2020-01-01'))`, "CreateTable"},
		{"ALTER 带 ALGORITHM", `ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE`, "Alter"},
		{"ALTER ADD INDEX", `ALTER TABLE t ADD INDEX (a, b) USING BTREE COMMENT 'xx'`, "Alter"},
		{"CREATE VIEW CASCADED", `CREATE VIEW v AS SELECT * FROM t WITH CASCADED CHECK OPTION`, "CreateView"},
		{"CREATE VIEW 属性", "CREATE ALGORITHM=MERGE DEFINER=`root`@`localhost` SQL SECURITY INVOKER VIEW v AS SELECT 1", "CreateView"},
		{"CREATE PROCEDURE", "CREATE DEFINER=`root`@`localhost` PROCEDURE p() BEGIN DECLARE c CURSOR FOR SELECT 1; OPEN c; CLOSE c; END", "CreateProcedure"},
		{"CREATE TRIGGER", `CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW BEGIN SET NEW.a = OLD.a + 1; END`, "CreateTrigger"},
		{"DROP 多表", `DROP TABLE IF EXISTS a, b, c`, "Drop"},
		{"DROP 多视图", `DROP VIEW v1, v2`, "Drop"},
		{"DROP INDEX", `DROP INDEX idx ON t`, "DropIndex"},
		{"CREATE INDEX", `CREATE INDEX idx ON t (a, b)`, "CreateIndex"},
		{"TRUNCATE", `TRUNCATE TABLE t`, "Truncate"},
		{"START TRANSACTION", `START TRANSACTION`, "Begin"},
		{"COMMIT", `COMMIT`, "Commit"},
		{"ROLLBACK", `ROLLBACK`, "Rollback"},
		{"SAVEPOINT", `SAVEPOINT sp1`, "Savepoint"},
		{"RELEASE SAVEPOINT", `RELEASE SAVEPOINT sp1`, "Savepoint"},
		{"SET TRANSACTION", `SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`, "Set"},
		{"SET PASSWORD", `SET PASSWORD FOR 'u'@'h' = PASSWORD('x')`, "Privileges"},
		{"SET 同名会话变量", `SET password = 'x'`, "Set"},
		{"SET 变量", `SET autocommit = 1`, "Set"},
		{"SHOW COLUMNS", `SHOW COLUMNS FROM t LIKE 'a%'`, "Show"},
		{"SHOW INDEX", `SHOW INDEX FROM t`, "Show"},
		{"SHOW VARIABLES", `SHOW VARIABLES LIKE 'max%'`, "Show"},
		{"SHOW STATUS", `SHOW STATUS`, "Show"},
		{"SHOW ENGINES", `SHOW ENGINES`, "Show"},
		{"SHOW PROCEDURE STATUS", `SHOW PROCEDURE STATUS WHERE Db = 'd'`, "Show"},
		{"FLUSH", `FLUSH TABLES`, "Flush"},
		{"ANALYZE TABLE", `ANALYZE TABLE t`, "TableMaintenance"},
		{"OPTIMIZE TABLE", `OPTIMIZE TABLE t`, "TableMaintenance"},
		{"CHECK TABLE", `CHECK TABLE t`, "TableMaintenance"},
		{"CHECKSUM TABLE", `CHECKSUM TABLE t`, "TableMaintenance"},
		{"KILL QUERY", `KILL QUERY 12345`, "Kill"},
		{"PREPARE", `PREPARE s FROM 'SELECT ?'`, "Prepare"},
		{"EXECUTE", `EXECUTE s USING @a, @b`, "Execute"},
		{"DEALLOCATE", `DEALLOCATE PREPARE s`, "Deallocate"},
		{"LOAD DATA", `LOAD DATA INFILE 'f.txt' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES STARTING BY 'xx' IGNORE 1 LINES`, "Load"},
		{"LOCK TABLES", `LOCK TABLES t READ, t2 WRITE`, "LockTables"},
		{"UNLOCK TABLES", `UNLOCK TABLES`, "UnlockTables"},
		{"EXPLAIN FORMAT", `EXPLAIN FORMAT=JSON SELECT * FROM t`, "Explain"},
		{"EXPLAIN ANALYZE", `EXPLAIN ANALYZE SELECT * FROM t`, "Explain"},
		{"DO", `DO 1`, "Do"},
		{"INTO OUTFILE", `SELECT * FROM t INTO OUTFILE '/tmp/x.txt'`, "Select"},
		{"CREATE TEMPORARY TABLE", `CREATE TEMPORARY TABLE tt (a INT)`, "CreateTable"},
		{"CREATE TABLE AS SELECT", `CREATE TABLE t AS SELECT a FROM t2`, "CreateTable"},
		{"CREATE TABLE LIKE", `CREATE TABLE t LIKE t2`, "CreateTable"},
		{"多语句", `SELECT 1; SELECT 2; SELECT 3`, "Multi"},
		{"标量子查询", `SELECT * FROM t WHERE a = (SELECT MAX(a) FROM t2) AND b IN (SELECT b FROM t3 WHERE c > 0)`, "Select"},
		{"类型转换函数", `SELECT CAST(a AS DECIMAL(10,2)), CONVERT(a USING utf8mb4), EXTRACT(YEAR FROM d) FROM t`, "Select"},
		{"GROUP BY 多列", `SELECT * FROM t GROUP BY a, b ORDER BY COUNT(*) DESC, a`, "Select"},
		{"日期函数", `SELECT DATE_ADD(NOW(), INTERVAL 1 DAY), TIMESTAMPDIFF(DAY, a, b) FROM t`, "Select"},
		{"NATURAL JOIN", `SELECT * FROM t1 NATURAL JOIN t2`, "Select"},
		{"STRAIGHT_JOIN", `SELECT STRAIGHT_JOIN a FROM t1, t2`, "Select"},
		{"多表 FROM", `SELECT a FROM t1 AS x, t2 AS y WHERE x.id = y.id`, "Select"},
		{"UPDATE 多表", `UPDATE t1, t2 SET t1.a = t2.b WHERE t1.id = t2.id`, "Update"},
		{"DELETE USING", `DELETE FROM t1, t2 USING t1 JOIN t2 ON t1.id = t2.id`, "Delete"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseSQL(nil, nil, tc.sql)
			if err != nil {
				t.Fatalf("解析失败: %v\nSQL: %s", err, tc.sql)
			}
			if stmt == nil {
				t.Fatalf("返回 nil\nSQL: %s", tc.sql)
			}
			if stmt.Type != tc.wantType {
				t.Errorf("类型 = %q, 期望 %q\nSQL: %s", stmt.Type, tc.wantType, tc.sql)
			}
			if renderDetails(stmt.Details) == "" {
				t.Errorf("details 渲染为空, type=%s", stmt.Type)
			}
		})
	}
}
