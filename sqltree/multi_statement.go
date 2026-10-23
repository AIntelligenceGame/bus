package sqltree

import (
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// vitess 的 Parse 只接受单条语句，用户粘贴一段脚本时需要自己按分号拆开。
// 这里用 vitess 自带的 SplitStatementToPieces 来拆（它能正确处理字符串里的分号）。

// MultiStatementDetails 表示一段包含多条语句的脚本。
type MultiStatementDetails struct {
	Statements []*SQLStatement `json:"statements"`
}

// containsBeginBlock 判断文本里是否有 BEGIN...END 块。
//
// 存储过程/函数/触发器的函数体用 BEGIN...END 包裹，块内的分号不是语句分隔符。
//
// 只在**代码区**找 BEGIN——字面量里的 'begin'、注释里的 begin 都不算，
// 否则 `SELECT 'begin'; SELECT 2` 这种脚本会被误判成有块，整段不拆。
//
// 还要区分"BEGIN 块"和"事务起始语句 BEGIN"：
// 后者后面紧跟分号或就是语句结尾（`BEGIN; SELECT 1; COMMIT;`），
// 它是可以拆的；块里 BEGIN 后面是语句内容（`BEGIN SELECT 1; END`）。
func containsBeginBlock(text string) bool {
	regions := scanLiteralRegions(text)
	idx := regions.findKeyword("begin")
	if idx < 0 {
		return false
	}
	// BEGIN 后面：跳过空白看下一个字符
	i := idx + len("begin")
	for i < len(text) && isSpace(text[i]) {
		i++
	}
	if i >= len(text) {
		// 结尾就是 BEGIN —— 事务起始语句，不是块
		return false
	}
	if text[i] == ';' {
		// BEGIN; —— 事务起始语句，不是块
		return false
	}
	return true
}

// parseMultiStatement 判断 SQL 是否包含多条语句。
// 若是，逐条解析并返回汇总结果；若不是，返回 false 交给常规流程。
func parseMultiStatement(sqlStr string) (*SQLStatement, bool) {
	// 存储过程/函数/触发器的函数体里有分号，但它们不是多条语句。
	// vitess 的切分器不认识 BEGIN...END，会把函数体切碎，因此这里直接跳过。
	if containsBeginBlock(sqlStr) {
		return nil, false
	}

	pieces, err := sqlparser.SplitStatementToPieces(sqlStr)
	if err != nil {
		return nil, false
	}
	// 只有一条（或切不出东西）就不走多语句路径
	if len(pieces) < 2 {
		return nil, false
	}

	details := &MultiStatementDetails{Statements: []*SQLStatement{}}
	for _, piece := range pieces {
		trimmed := strings.TrimSpace(piece)
		if trimmed == "" {
			continue
		}
		stmt, err := parseSingleStatement(trimmed)
		if err != nil {
			// 某一条解析不了就把错误记下来，不影响其它条
			details.Statements = append(details.Statements, &SQLStatement{
				Type:    "Error",
				Details: map[string]string{"error": err.Error(), "sql": trimmed},
			})
			continue
		}
		details.Statements = append(details.Statements, stmt)
	}
	if len(details.Statements) == 0 {
		return nil, false
	}
	return &SQLStatement{Type: "Multi", Details: details}, true
}

// parseSingleStatement 解析单条语句（不做多语句拆分）。
func parseSingleStatement(sqlStr string) (*SQLStatement, error) {
	// 归一化：去掉注释（与 ParseSQL 保持一致）。
	// 单独调用时也走一遍，避免注释漏进解析结果。
	sqlStr = stripSQLComments(sqlStr)

	ast, ctx, astErr := parseAST(sqlStr)
	if astErr == nil {
		if stmt := extractASTStatement(ast, ctx); stmt != nil {
			return &SQLStatement{Type: stmt.Type, Details: stmt.Details}, nil
		}
	}
	// AST 层不支持的语句，走自研解析器。
	//
	// 这里必须和 ParseSQL 用**同一套**自研解析器——否则会出现
	// "同一条语句单独解析成功、放进脚本里却报错"的行为不一致。
	if stmt := parseHandWritten(sqlStr); stmt != nil {
		return stmt, nil
	}
	return nil, astErr
}

// parseHandWritten 尝试用自研解析器解析 vitess 语法层不支持的语句。
//
// 这些语句的共同点：vitess 的语法里没有，但 MySQL 合法，且信息量小，
// 直接从原文里取就够了。
//
// 返回 nil 表示"这条语句不是自研解析器负责的"。
func parseHandWritten(sqlStr string) *SQLStatement {
	if fn := parseCreateFunction(sqlStr); fn != nil {
		return &SQLStatement{Type: "CreateFunction", Details: fn}
	}
	if fn := parseDropFunction(sqlStr); fn != nil {
		return &SQLStatement{Type: "Drop", Details: fn}
	}
	if do := parseDoStatement(sqlStr); do != nil {
		return &SQLStatement{Type: "Do", Details: do}
	}
	// OPTIMIZE/CHECK/CHECKSUM/REPAIR TABLE 与 SET PASSWORD
	if tm := parseTableMaintenance(sqlStr); tm != nil {
		return &SQLStatement{Type: "TableMaintenance", Details: tm}
	}
	if sp := parseSetPassword(sqlStr); sp != nil {
		return &SQLStatement{Type: "Privileges", Details: sp}
	}
	// 复制/运维语句：vitess 只认 REPLICA 新语法，旧的 SLAVE 写法与
	// SHOW ENGINE INNODB STATUS 之类它根本不支持。
	if rep := parseReplicationStatement(sqlStr); rep != nil {
		return &SQLStatement{Type: "Admin", Details: rep}
	}
	return nil
}

// DoDetails 表示 DO 表达式语句。
type DoDetails struct {
	Expression string `json:"expression"`
}

// parseDoStatement 解析 DO expr 语句。
// vitess 语法层不支持 DO，这里直接取原文。
func parseDoStatement(sqlStr string) *DoDetails {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "do ") {
		return nil
	}
	expression := strings.TrimSpace(trimmed[len("do"):])
	if expression == "" {
		return nil
	}
	return &DoDetails{Expression: expression}
}
