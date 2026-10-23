package sqltree

import (
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// 这个文件补齐"管理类"语句的解析。
//
// vitess 的语法层能解析其中一部分（FLUSH / ANALYZE / KILL / SAVEPOINT），
// 但 AST 里没有对应的展示节点，这里补上；
// 另一部分 vitess 根本不支持语法（OPTIMIZE/CHECK/CHECKSUM/REPAIR TABLE、
// SET PASSWORD），这里自己扫文本解析。

// TableMaintenanceDetails 表示 ANALYZE / OPTIMIZE / CHECK / CHECKSUM / REPAIR TABLE。
type TableMaintenanceDetails struct {
	Operation string   `json:"operation"`
	Tables    []string `json:"tables"`
	Options   []string `json:"options,omitempty"`
}

// FlushDetails 表示 FLUSH 语句。
type FlushDetails struct {
	Type     string   `json:"type,omitempty"`
	Option   string   `json:"option,omitempty"`
	Tables   []string `json:"tables,omitempty"`
	ReadLock bool     `json:"read_lock,omitempty"`
}

// KillDetails 表示 KILL 语句。
type KillDetails struct {
	Connection bool   `json:"connection,omitempty"`
	Query      bool   `json:"query,omitempty"`
	Target     string `json:"target,omitempty"`
}

// SavepointDetails 表示 SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT。
type SavepointDetails struct {
	Action string `json:"action"`
	Name   string `json:"name"`
}

// PrivilegeDetails 表示权限与账号管理类语句（GRANT/REVOKE/用户与角色管理）。
//
// 这类语句 vitess 能解析但没有可展示的树结构，这里保留原始 SQL 与类别。
type PrivilegeDetails struct {
	Kind string `json:"kind"`
	User string `json:"user,omitempty"`
	SQL  string `json:"sql"`
}

// extractFlush 提取 FLUSH 语句的细节。
func extractFlush(node *sqlparser.Flush) *FlushDetails {
	details := FlushDetails{Type: node.Type}
	if node.Option == nil {
		return &details
	}
	details.Option = node.Option.Name
	details.ReadLock = node.Option.ReadLock
	for _, t := range node.Option.Tables {
		details.Tables = append(details.Tables, tableFullName(t))
	}
	return &details
}

// extractKill 提取 KILL 语句的细节。
func extractKill(node *sqlparser.Kill) *KillDetails {
	details := KillDetails{Connection: node.Connection}
	if !node.Connection {
		details.Query = true
	}
	if node.ConnID != nil {
		details.Target = sqlparser.String(node.ConnID)
	}
	return &details
}

// extractAnalyze 提取 ANALYZE TABLE 的细节。
func extractAnalyze(node *sqlparser.Analyze) *TableMaintenanceDetails {
	details := TableMaintenanceDetails{Operation: "ANALYZE"}
	for _, t := range node.Tables {
		details.Tables = append(details.Tables, tableFullName(t))
	}
	if node.Action != "" {
		details.Options = append(details.Options, node.Action)
	}
	for _, c := range node.Columns {
		details.Options = append(details.Options, c.String())
	}
	return &details
}

// extractSavepoint 提取 SAVEPOINT 家族的细节。
func extractSavepoint(action string, name string) *SavepointDetails {
	return &SavepointDetails{Action: action, Name: name}
}

// tableMaintenanceKeywords 是 vitess 不支持、但 MySQL 支持的表维护语句前缀。
var tableMaintenanceKeywords = []string{
	"optimize", "check", "checksum", "repair",
}

// parseTableMaintenance 解析 ANALYZE/OPTIMIZE/CHECK/CHECKSUM/REPAIR TABLE 语句。
// 若不是这类语句，返回 nil。
//
// vitess 只支持 ANALYZE TABLE，其余几种语法层直接报语法错误，只能自己扫文本。
func parseTableMaintenance(sqlStr string) *TableMaintenanceDetails {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)

	operation := ""
	for _, keyword := range tableMaintenanceKeywords {
		if strings.HasPrefix(lower, keyword+" ") {
			operation = strings.ToUpper(keyword)
			break
		}
	}
	if operation == "" {
		return nil
	}
	rest := strings.TrimSpace(trimmed[len(strings.ToLower(operation)):])
	// CHECKSUM TABLE 与 CHECK TABLE 的关键字相同，但 CHECKSUM 更长，先匹配 CHECKSUM
	if strings.HasPrefix(strings.ToLower(rest), "table") {
		rest = strings.TrimSpace(rest[len("table"):])
	} else {
		// 允许省略 TABLE 关键字：CHECK t
		rest = strings.TrimSpace(rest)
	}

	details := &TableMaintenanceDetails{Operation: operation, Tables: []string{}}
	for _, part := range splitTopLevel(rest, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// 表名之后的都是选项
		name, remainder, ok := scanIdentifier(part)
		if !ok {
			continue
		}
		details.Tables = append(details.Tables, name)
		remainder = strings.TrimSpace(remainder)
		if remainder != "" {
			details.Options = append(details.Options, remainder)
		}
	}
	if len(details.Tables) == 0 {
		return nil
	}
	return details
}

// parseSetPassword 解析 SET PASSWORD 语句。
// vitess 语法层不支持，这里自己扫文本。
func parseSetPassword(sqlStr string) *PrivilegeDetails {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "set ") {
		return nil
	}
	rest := strings.TrimSpace(trimmed[len("set"):])
	if !strings.HasPrefix(strings.ToLower(rest), "password") {
		return nil
	}
	rest = strings.TrimSpace(rest[len("password"):])
	// SET PASSWORD FOR user = 'xxx'
	// 必须是 FOR 形式：没有 FOR 的 `SET password = 'x'` 是设置同名会话变量，
	// 不能当成改密码语句。
	rest = strings.TrimSpace(rest)
	user := ""
	if strings.HasPrefix(strings.ToLower(rest), "for ") {
		rest = strings.TrimSpace(rest[len("for"):])
		user, rest, _ = scanUserName(rest)
		rest = strings.TrimSpace(rest)
	} else {
		return nil
	}
	if !strings.HasPrefix(rest, "=") {
		return nil
	}
	rest = strings.TrimSpace(rest[1:])
	if rest == "" {
		return nil
	}
	return &PrivilegeDetails{Kind: "set_password", User: user, SQL: trimmed}
}

// scanUserName 读取 'user'@'host' 形式的账号名。
func scanUserName(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", text, false
	}
	if text[0] != '\'' && text[0] != '"' && text[0] != '`' {
		// 没有引号，按空白截断
		if idx := strings.IndexAny(text, " \t\n"); idx >= 0 {
			return text[:idx], text[idx:], true
		}
		return text, "", true
	}
	quote := text[0]
	end := strings.IndexByte(text[1:], quote)
	if end < 0 {
		return "", text, false
	}
	user := text[1 : end+1]
	rest := text[end+2:]
	if strings.HasPrefix(rest, "@") {
		rest = rest[1:]
		if rest != "" && (rest[0] == '\'' || rest[0] == '"' || rest[0] == '`') {
			q := rest[0]
			e := strings.IndexByte(rest[1:], q)
			if e >= 0 {
				user += "@" + rest[1:e+1]
				rest = rest[e+2:]
			}
		}
	}
	return user, rest, true
}

// parseGrantLike 解析 GRANT/REVOKE/CREATE USER 等权限语句。
// 这类语句 vitess 能解析，但没有可展示的结构，这里保留原文。
func parseGrantLike(sqlStr string) *PrivilegeDetails {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	keywords := []string{
		"grant ", "revoke ", "create user ", "drop user ", "rename user ",
		"create role ", "drop role ", "show grants", "show privileges",
	}
	for _, keyword := range keywords {
		if strings.HasPrefix(lower, keyword) {
			kind := strings.TrimSpace(strings.TrimSuffix(keyword, " "))
			return &PrivilegeDetails{Kind: strings.ReplaceAll(kind, " ", "_"), SQL: trimmed}
		}
	}
	return nil
}

// connectionIDFromExpr 从 KILL 的目标表达式里取连接 ID。
func connectionIDFromExpr(expr sqlparser.Expr) int64 {
	literal, ok := expr.(*sqlparser.SQLVal)
	if !ok {
		return 0
	}
	value, err := strconv.ParseInt(string(literal.Val), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// parseReplicationStatement 解析 vitess 语法层**根本不支持**的复制/运维语句。
//
// vitess 只认 REPLICA 系列的新语法（START REPLICA、CHANGE REPLICATION SOURCE ...），
// MySQL 里更常见的旧写法（START SLAVE、CHANGE MASTER TO ...、RESET MASTER）
// 以及一些运维语句（SHOW ENGINE INNODB STATUS、SHOW BINLOG EVENTS）
// 它一律报语法错——这里自己扫文本解析。
//
// 返回 nil 表示"这条语句不是这里负责的"。
func parseReplicationStatement(sqlStr string) *AdminStatementDetails {
	trimmed := strings.TrimSpace(sqlStr)
	lower := strings.ToLower(trimmed)
	if trimmed == "" {
		return nil
	}

	// RESET MASTER / RESET SLAVE [ALL] / RESET REPLICA [ALL]
	// （RESET REPLICA 走 vitess，这里只兜 MASTER 与 SLAVE）
	if strings.HasPrefix(lower, "reset ") {
		rest := strings.TrimSpace(lower[len("reset "):])
		for _, target := range []string{"master", "slave", "replica"} {
			if !strings.HasPrefix(rest, target) {
				continue
			}
			tail := strings.TrimSpace(rest[len(target):])
			// 后面只能是空或 ALL
			if tail != "" && !strings.EqualFold(tail, "all") {
				return nil
			}
			return &AdminStatementDetails{
				Kind:   "replica",
				Action: "reset",
				All:    strings.EqualFold(tail, "all"),
				SQL:    trimmed,
			}
		}
		return nil
	}

	// START SLAVE / START REPLICA
	// STOP SLAVE / STOP REPLICA
	for _, verb := range []string{"start", "stop"} {
		if !strings.HasPrefix(lower, verb+" ") {
			continue
		}
		rest := strings.TrimSpace(lower[len(verb)+1:])
		for _, target := range []string{"slave", "replica"} {
			if !strings.HasPrefix(rest, target) {
				continue
			}
			tail := strings.TrimSpace(rest[len(target):])
			details := &AdminStatementDetails{
				Kind:   "replica",
				Action: verb,
				Target: target,
				SQL:    trimmed,
			}
			// START SLAVE 后可以跟 SQL_THREAD / IO_THREAD 等修饰
			if tail != "" {
				details.Options = []string{tail}
			}
			return details
		}
	}

	// CHANGE MASTER TO ... / CHANGE REPLICATION SOURCE TO ...
	// CHANGE REPLICATION FILTER ...
	if strings.HasPrefix(lower, "change ") {
		rest := strings.TrimSpace(lower[len("change "):])
		switch {
		case strings.HasPrefix(rest, "master to"),
			strings.HasPrefix(rest, "replication source to"):
			options := parseReplicationOptions(trimmed)
			return &AdminStatementDetails{
				Kind:    "replica",
				Action:  "change_source",
				Options: options,
				SQL:     trimmed,
			}
		case strings.HasPrefix(rest, "replication filter"):
			options := parseReplicationOptions(trimmed)
			return &AdminStatementDetails{
				Kind:    "replica",
				Action:  "change_filter",
				Options: options,
				SQL:     trimmed,
			}
		}
		return nil
	}

	// SHOW ENGINE INNODB STATUS / SHOW BINLOG EVENTS / SHOW OPEN TABLES
	if strings.HasPrefix(lower, "show ") {
		rest := strings.TrimSpace(lower[len("show "):])
		switch {
		case strings.HasPrefix(rest, "engine "):
			return &AdminStatementDetails{Kind: "other_read", SQL: trimmed}
		case strings.HasPrefix(rest, "binlog events"):
			return &AdminStatementDetails{Kind: "other_read", SQL: trimmed}
		case strings.HasPrefix(rest, "open tables"):
			return &AdminStatementDetails{Kind: "other_read", SQL: trimmed}
		}
		return nil
	}

	return nil
}

// parseReplicationOptions 解析复制语句的选项列表（`NAME=VALUE` 形式）。
//
// 口令类的选项值会被打码——解析结果经常要往外发，不能把口令带出去。
func parseReplicationOptions(sqlStr string) []string {
	// 找到 TO 之后的部分
	lower := strings.ToLower(sqlStr)
	idx := strings.Index(lower, " to ")
	if idx < 0 {
		return nil
	}
	rest := sqlStr[idx+len(" to "):]

	options := []string{}
	for _, part := range splitTopLevel(rest, ',') {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			options = append(options, part)
			continue
		}
		name := strings.TrimSpace(part[:eq])
		value := strings.TrimSpace(part[eq+1:])
		if isSecretOption(name) {
			value = "***"
		}
		options = append(options, name+"="+value)
	}
	return options
}
