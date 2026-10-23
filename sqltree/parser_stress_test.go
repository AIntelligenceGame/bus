package sqltree

import (
	"strings"
	"testing"
)

// TestStressComplexStatements 用极其复杂的语句压测解析器。
//
// 这里刻意不检查字段内容，只检查"能不能解析出来"——
// 先看哪些语句会直接失败，再逐个补内容断言。
func TestStressComplexStatements(t *testing.T) {
	cases := []struct {
		name         string
		sql          string
		wantType     string
		wantContains []string
	}{
		{
			name:     "深层嵌套子查询 + CASE WHEN",
			sql:      `SELECT a.id, a.nm, CASE WHEN a.st = 1 THEN 'on' WHEN a.st = 2 THEN 'off' WHEN a.v > (SELECT AVG(b.v) FROM b WHERE b.aid = a.id AND b.st IN (SELECT st FROM c WHERE dt > '2020-01-01')) THEN 'high' ELSE 'unknown' END AS st_desc, (SELECT COUNT(*) FROM (SELECT x FROM y WHERE y.id = a.id) z) AS cnt FROM a WHERE a.id IN (SELECT id FROM (SELECT id FROM w WHERE v > 0) v2 WHERE v2.id > 100) AND EXISTS (SELECT 1 FROM q WHERE q.aid = a.id) AND a.nm LIKE CONCAT('%', (SELECT MAX(nm) FROM nn), '%')`,
			wantType: "Select",
			wantContains: []string{
				"case when",
				"exists",
				"CONCAT",
				"selected_exprs",
			},
		},
		{
			name:     "递归 CTE",
			sql:      `WITH RECURSIVE cte (id, parent_id, lvl) AS (SELECT id, parent_id, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.parent_id, c.lvl + 1 FROM tree t JOIN cte c ON t.parent_id = c.id WHERE c.lvl < 10) SELECT id, parent_id, lvl FROM cte ORDER BY lvl, id`,
			wantType: "Select",
			wantContains: []string{
				"cte",
				"union all",
				"lvl",
			},
		},
		{
			name:     "CTE + UNION + 窗口函数",
			sql:      `WITH a AS (SELECT id, SUM(v) s FROM t1 GROUP BY id), b AS (SELECT id, COUNT(*) c FROM t2 GROUP BY id) SELECT a.id, a.s, b.c, ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.s DESC) rn, SUM(a.s) OVER (ORDER BY a.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running FROM a JOIN b ON a.id = b.id UNION ALL SELECT id, s, 0, 0, 0 FROM a WHERE s > 1000 UNION DISTINCT SELECT id, s, 0, 0, 0 FROM a WHERE s < 0`,
			wantType: "Select",
			wantContains: []string{
				"ROW_NUMBER",
				"over",
				"union",
			},
		},
		{
			name:     "INSERT ... ON DUPLICATE KEY UPDATE",
			sql:      `INSERT INTO t (id, nm, v, dt) VALUES (1, 'a', 10, '2020-01-01'), (2, 'b', 20, '2020-01-02') ON DUPLICATE KEY UPDATE v = VALUES(v) + 1, nm = CONCAT(nm, '!'), dt = NOW()`,
			wantType: "Insert",
			wantContains: []string{
				"on_duplicate",
				"values(v)",
				"NOW()",
			},
		},
		{
			name:     "INSERT ... SELECT ... UNION",
			sql:      `INSERT INTO t2 (id, nm) SELECT id, nm FROM t1 WHERE v > 0 UNION ALL SELECT id, nm FROM t3`,
			wantType: "Insert",
			wantContains: []string{
				"select_data",
				"union all",
			},
		},
		{
			name:     "INSERT ... SET 语法",
			sql:      `INSERT INTO t SET id = 1, nm = 'x', v = 2`,
			wantType: "Insert",
			wantContains: []string{
				"\"id\"",
				"\"nm\"",
			},
		},
		{
			name:     "UPDATE 多表 JOIN",
			sql:      `UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.v = t2.v * 2, t1.nm = CONCAT(t1.nm, '-', t2.tag) WHERE t2.flag = 1 AND t1.v < 100`,
			wantType: "Update",
			wantContains: []string{
				"t1, t2",
				"from",
				"t2.tag",
			},
		},
		{
			name:     "UPDATE 子查询",
			sql:      `UPDATE t SET v = (SELECT AVG(v) FROM t2 WHERE t2.id = t.id) WHERE id IN (SELECT id FROM t3)`,
			wantType: "Update",
			wantContains: []string{
				"select AVG(v) from t2",
			},
		},
		{
			name:     "DELETE 多表",
			sql:      `DELETE t1, t2 FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.flag = 0`,
			wantType: "Delete",
			wantContains: []string{
				"t1, t2",
				"from",
			},
		},
		{
			name:     "DELETE 自引用子查询",
			sql:      `DELETE FROM t WHERE id IN (SELECT id FROM (SELECT id FROM t WHERE v < 0 ORDER BY v LIMIT 100) x)`,
			wantType: "Delete",
			wantContains: []string{
				"limit 100",
			},
		},
		{
			name:     "CREATE TABLE 全家桶",
			sql:      "CREATE TABLE IF NOT EXISTS `db`.`tbl` (\n  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n  nm VARCHAR(128) NOT NULL DEFAULT '' COMMENT '名称',\n  amt DECIMAL(20,4) UNSIGNED NOT NULL DEFAULT 0.0000,\n  ratio DOUBLE(10,4) NULL DEFAULT NULL,\n  flag TINYINT(1) NOT NULL DEFAULT 0,\n  blob_col BLOB,\n  txt_col TEXT,\n  json_col JSON,\n  dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n  dt2 TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n  gen INT GENERATED ALWAYS AS (amt * 2) STORED,\n  gen2 INT AS (flag + 1) VIRTUAL,\n  PRIMARY KEY (id),\n  UNIQUE KEY uk_nm (nm),\n  KEY idx_amt_dt (amt, dt),\n  KEY idx_expr ((amt + flag)),\n  FULLTEXT KEY ft_nm (nm),\n  CONSTRAINT ck_amt CHECK (amt >= 0),\n  CONSTRAINT fk_parent FOREIGN KEY (id) REFERENCES parent_t (pid) ON DELETE CASCADE ON UPDATE RESTRICT\n) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='业务表' ROW_FORMAT=DYNAMIC",
			wantType: "CreateTable",
			wantContains: []string{
				"db.tbl",
				"auto_increment",
				"primary_key",
				"uk_nm",
				"idx_amt_dt",
				"idx_expr",
				"expression",
				"ft_nm",
				"fk_parent",
				"ck_amt",
				"\"expression\": \"amt",
				"ROW_FORMAT=DYNAMIC",
				"generated",
			},
		},
		{
			name:     "ALTER TABLE 多动作",
			sql:      `ALTER TABLE t ADD COLUMN c1 INT NOT NULL DEFAULT 0 COMMENT 'c1', ADD COLUMN c2 VARCHAR(32) NULL AFTER c1, MODIFY COLUMN c3 BIGINT UNSIGNED NOT NULL DEFAULT 0, CHANGE COLUMN c4 c4_new INT, DROP COLUMN c5, RENAME COLUMN c6 TO c6_new, ADD INDEX idx_c1 (c1), ADD UNIQUE INDEX uk_c2 (c2), DROP INDEX idx_old, ADD CONSTRAINT fk_c FOREIGN KEY (c1) REFERENCES other_t (id) ON DELETE NO ACTION, ALTER COLUMN c1 SET DEFAULT 99, ENGINE=InnoDB, COMMENT='改过了'`,
			wantType: "Alter",
			wantContains: []string{
				"\"action\": \"add\"",
				"\"action\": \"drop\"",
				"\"action\": \"modify\"",
				"\"action\": \"change\"",
				"\"action\": \"rename\"",
				"\"action\": \"index\"",
				"alter_column",
				"option",
				"ENGINE=InnoDB",
				"AFTER c1",
				"c4 c4_new INT",
			},
		},
		{
			name:     "CREATE VIEW 复杂",
			sql:      `CREATE OR REPLACE VIEW v AS SELECT a.id, a.nm, (SELECT COUNT(*) FROM b WHERE b.aid = a.id) cnt FROM a WHERE a.v > 0 UNION ALL SELECT id, nm, 0 FROM a2 WHERE dt > '2020-01-01' WITH CHECK OPTION`,
			wantType: "CreateView",
			wantContains: []string{
				"view_name",
				"or_replace",
				"check_option",
				"union all",
			},
		},
		{
			name:     "CREATE VIEW 带列名",
			sql:      `CREATE VIEW v (c1, c2, c3) AS SELECT id, nm, v FROM t`,
			wantType: "CreateView",
			wantContains: []string{
				"c1",
				"c2",
				"c3",
			},
		},
		{
			name:     "CREATE PROCEDURE 复杂",
			sql:      `CREATE PROCEDURE p1(IN p_id INT, OUT p_res VARCHAR(64), INOUT p_cnt INT) BEGIN DECLARE v_cur INT DEFAULT 0; DECLARE v_done BOOL DEFAULT FALSE; DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE; DECLARE cur CURSOR FOR SELECT id FROM t WHERE v > p_id; SELECT COUNT(*) INTO v_cur FROM t WHERE nm LIKE CONCAT('%', p_id, '%'); OPEN cur; read_loop: LOOP FETCH cur INTO v_cur; IF v_done THEN LEAVE read_loop; END IF; SET p_cnt = p_cnt + 1; END LOOP; CLOSE cur; IF p_cnt > 0 THEN SET p_res = 'found'; ELSEIF p_cnt = 0 THEN SET p_res = 'empty'; ELSE SET p_res = 'err'; END IF; REPEAT SET p_cnt = p_cnt - 1; UNTIL p_cnt <= 0 END REPEAT; END`,
			wantType: "CreateProcedure",
			wantContains: []string{
				"\"direction\": \"in\"",
				"\"direction\": \"out\"",
				"\"direction\": \"inout\"",
				"VARCHAR(64)",
				"declare",
				"cursor",
				"handler",
			},
		},
		{
			name:     "CREATE FUNCTION 复杂",
			sql:      `CREATE FUNCTION calc_tax(p_amt DECIMAL(20,4), p_rate DECIMAL(10,4)) RETURNS DECIMAL(20,4) DETERMINISTIC NO SQL COMMENT '计算税额' BEGIN DECLARE v_tax DECIMAL(20,4) DEFAULT 0; SET v_tax = p_amt * p_rate; RETURN ROUND(v_tax, 4); END`,
			wantType: "CreateFunction",
			wantContains: []string{
				"calc_tax",
				"DECIMAL(20,4)",
				"DETERMINISTIC",
				"NO SQL",
				"RETURN ROUND",
			},
		},
		{
			name:     "CREATE TRIGGER 复杂",
			sql:      `CREATE TRIGGER trg_ai AFTER INSERT ON t FOR EACH ROW BEGIN IF NEW.v > 0 THEN INSERT INTO log_t (msg) VALUES (CONCAT('ins ', NEW.id)); ELSE SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'bad'; END IF; UPDATE cnt_t SET c = c + 1 WHERE id = NEW.id; END`,
			wantType: "CreateTrigger",
			wantContains: []string{
				"trg_ai",
				"\"time\": \"after\"",
				"\"event\": \"insert\"",
			},
		},
		{
			name:     "RENAME TABLE 多表",
			sql:      `RENAME TABLE a TO a1, b TO b1, c TO c1`,
			wantType: "RenameTable",
			wantContains: []string{
				"\"from\": \"a\"",
				"\"to\": \"a1\"",
			},
		},
		{
			name:     "SELECT DISTINCT + HAVING + 派生表",
			sql:      `SELECT DISTINCT t1.nm, COUNT(*) cnt FROM (SELECT * FROM t WHERE v > 0) t1 GROUP BY t1.nm HAVING COUNT(*) > 2 ORDER BY cnt DESC, t1.nm ASC LIMIT 100 OFFSET 20`,
			wantType: "Select",
			wantContains: []string{
				"distinct",
				"having",
				"offset",
				"limit",
			},
		},
		{
			name:     "SELECT 各种函数",
			sql:      `SELECT IFNULL(a, 0), COALESCE(b, c, 0), IF(x > 0, 'y', 'n'), CAST(d AS CHAR), CONVERT(e, DECIMAL(10,2)), DATE_FORMAT(f, '%Y-%m-%d'), JSON_EXTRACT(g, '$.k'), GROUP_CONCAT(h SEPARATOR ','), NOW(), CURDATE() FROM t`,
			wantType: "Select",
			wantContains: []string{
				"IFNULL",
				"COALESCE",
				"DATE_FORMAT",
				"JSON_EXTRACT",
			},
		},
		{
			name:     "SET 多变量",
			sql:      `SET @@global.max_connections = 1000, @@session.sql_mode = 'STRICT_TRANS_TABLES', @user_var = 1`,
			wantType: "Set",
			wantContains: []string{
				"max_connections",
			},
		},
		{
			name:     "SHOW 变体",
			sql:      `SHOW CREATE TABLE t`,
			wantType: "Show",
			wantContains: []string{
				"create table",
				"\"table\": \"t\"",
			},
		},
		{
			name:     "USE",
			sql:      `USE mydb`,
			wantType: "Use",
			wantContains: []string{
				"mydb",
			},
		},
		{
			name:     "SELECT 字符串含特殊字符",
			sql:      "SELECT 'it''s', \"quoted \\\"str\\\"\", `back` FROM t WHERE nm = 'a\\\\b'",
			wantType: "Select",
			wantContains: []string{
				"from",
			},
		},
		{
			name:     "SELECT 注释",
			sql:      "SELECT /* 块注释 */ id, -- 行注释\n       nm FROM t # MySQL 行注释\nWHERE id = 1",
			wantType: "Select",
			wantContains: []string{
				"\"id\"",
				"\"nm\"",
			},
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

// TestStressNoPanic 确认任何输入都不会 panic。
func TestStressNoPanic(t *testing.T) {
	inputs := []string{
		"",
		"   ",
		"SELECT",
		"SELECT * FROM",
		"CREATE TABLE",
		"BEGIN END",
		"'unterminated string",
		"`unterminated ident",
		"/* unterminated comment",
		"CREATE FUNCTION",
		"CREATE FUNCTION f(",
		"CREATE PROCEDURE p() BEGIN",
		"SELECT 1; SELECT 2; SELECT 3",
		"(((((((((1)))))))))",
		strings.Repeat("SELECT * FROM t WHERE a IN (", 20) + "1" + strings.Repeat(")", 20),
		"\x00\x01\x02",
		"SELECT * FROM t WHERE nm = '中文' AND nm2 = 'emoji😀'",
	}
	for _, in := range inputs {
		in := in
		t.Run(strings.ReplaceAll(in, "/", "_"), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v\n输入: %q", r, in)
				}
			}()
			_, _ = ParseSQL(nil, nil, in)
		})
	}
}
