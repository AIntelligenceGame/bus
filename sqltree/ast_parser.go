package sqltree

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// AST 层解析：纯语法解析，不依赖 catalog。
//
// 这是本解析器的主力层。plan 层（analyzer）虽然语义更丰富，但要求表真实存在
// 且可访问——现实中用户随手给一条 SQL，我们既不知道表结构也没有 catalog 权限，
// 因此凡是 AST 能解析的语句，一律走 AST 层。
//
// AST 层的另一个优势：存储过程/触发器能拿到完整的参数与 body 结构，
// 而 plan 层的 CreateProcedure 并不暴露这些信息。

// astContext 承载解析过程中需要在各处传递的上下文。
type astContext struct {
	// SQL 是原始语句文本。vitess 会丢掉一些信息（例如 ALTER TABLE 的表选项、
	// ALTER COLUMN ... SET DEFAULT），需要回到原文里补。
	SQL string
	// FnIndexes 是解析前从原文里剥离出来的函数索引，解析后用来还原。
	FnIndexes functionalIndexInfoList
	// Partition 是解析前剥离出来的 PARTITION 子句，解析后用来还原。
	Partition *PartitionDetails
	// Fixups 记录了为绕过 vitess 语法缺陷而做的改写。
	Fixups []syntaxFixup
	// ZerofillCols 是带 ZEROFILL 修饰的列名集合（vitess 不支持该关键字，
	// 解析前剥离，解析后补回）。
	ZerofillCols map[string]bool
}

// parseAST 使用 vitess 语法解析器解析 SQL，返回 AST。
//
// vitess 有几处语法缺口（函数索引、PARTITION 子句、INSERT 优先级等），
// 会直接报语法错误。这里先做一次改写把它们摘掉，解析后再还原。
func parseAST(sqlStr string) (sqlparser.Statement, *astContext, error) {
	ctx := &astContext{SQL: sqlStr}

	rewritten, fixups := applySyntaxFixups(sqlStr)
	ctx.Fixups = fixups

	// PARTITION 子句：vitess 只支持最简的 HASH 形式，其余自己解析
	if clause := findPartitionClause(rewritten); clause != nil {
		ctx.Partition = parsePartitionClause(clause.Text)
		rewritten = removePartitionClause(rewritten, clause)
		ctx.Fixups = append(ctx.Fixups, syntaxFixup{
			Kind:     fixupKindPartition,
			Original: clause.Text,
			Note:     "PARTITION 子句（vitess 不支持，已自行解析）",
		})
	}

	// ZEROFILL：vitess 不支持，解析前剥离，解析后补回
	for _, fixup := range ctx.Fixups {
		if fixup.Kind != fixupKindZerofill {
			continue
		}
		fields := strings.Fields(fixup.Original)
		if len(fields) == 0 {
			continue
		}
		if ctx.ZerofillCols == nil {
			ctx.ZerofillCols = map[string]bool{}
		}
		ctx.ZerofillCols[strings.TrimSuffix(fields[0], " ")] = true
	}

	// 函数索引：替换成占位列，解析后还原
	rewritten, fnIndexes := rewriteFunctionalIndexes(rewritten)
	ctx.FnIndexes = fnIndexes

	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return nil, nil, err
	}
	return stmt, ctx, nil
}

// ASTStatement 表示 AST 层解析出的语句类型。
type ASTStatement struct {
	Type    string
	Details any
}

// CreateTableDetails 表示 CREATE TABLE 语句的解析结果。
type CreateTableDetails struct {
	Table       string            `json:"table"`
	IfNotExists bool              `json:"if_not_exists,omitempty"`
	Temporary   bool              `json:"temporary,omitempty"`
	Columns     []ColumnInfo      `json:"columns,omitempty"`
	PrimaryKey  []string          `json:"primary_key,omitempty"`
	Indexes     []IndexInfo       `json:"indexes,omitempty"`
	ForeignKeys []ForeignKey      `json:"foreign_keys,omitempty"`
	Checks      []CheckConstraint `json:"checks,omitempty"`
	Options     []string          `json:"options,omitempty"`
	Like        string            `json:"like,omitempty"`
	AsSelect    string            `json:"as_select,omitempty"`
	SelectData  *SelectDetails    `json:"select_data,omitempty"`
	Partition   *PartitionDetails `json:"partition,omitempty"`
}

// CheckConstraint 表示一个 CHECK 约束。
//
// vitess 的 CheckConstraintDefinition 只带表达式，约束名在它外层的
// ConstraintDefinition 上，这里合并到一起。
type CheckConstraint struct {
	Name       string `json:"name,omitempty"`
	Expression string `json:"expression"`
	Enforced   bool   `json:"enforced"`
}

// ColumnInfo 表示一个列的定义。
type ColumnInfo struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Nullable      bool     `json:"nullable"`
	HasDefault    bool     `json:"has_default,omitempty"`
	Default       string   `json:"default,omitempty"`
	AutoIncrement bool     `json:"auto_increment,omitempty"`
	OnUpdate      string   `json:"on_update,omitempty"`
	Comment       string   `json:"comment,omitempty"`
	Unsigned      bool     `json:"unsigned,omitempty"`
	Zerofill      bool     `json:"zerofill,omitempty"`
	EnumValues    []string `json:"enum_values,omitempty"`
	Charset       string   `json:"charset,omitempty"`
	Collation     string   `json:"collation,omitempty"`
	PrimaryKey    bool     `json:"primary_key,omitempty"`
	Unique        bool     `json:"unique,omitempty"`
	Generated     string   `json:"generated,omitempty"`
}

// IndexInfo 表示一个索引定义。
type IndexInfo struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Unique  bool     `json:"unique,omitempty"`
	Primary bool     `json:"primary,omitempty"`
	Columns []string `json:"columns"`
	// ColumnPrefixes 是各索引列的前缀长度——`KEY idx (col(30))` 里的 30。
	// 键是列名，值是前缀长度；没有指定前缀长度的列不出现在这里。
	ColumnPrefixes map[string]int `json:"column_prefixes,omitempty"`
	// Expression 非空表示这是一个函数索引（MySQL 8.0.13+ 的 KEY idx ((expr))），
	// 此时 Columns 为空，表达式的文本放在这里。
	Expression string `json:"expression,omitempty"`
	FullText   bool   `json:"fulltext,omitempty"`
	Spatial    bool   `json:"spatial,omitempty"`
	Using      string `json:"using,omitempty"`
}

// ForeignKey 表示一个外键定义。
type ForeignKey struct {
	Name              string   `json:"name,omitempty"`
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns,omitempty"`
	OnDelete          string   `json:"on_delete,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
}

// AlterTableDetails 表示 ALTER TABLE 语句的解析结果。
type AlterTableDetails struct {
	Table   string        `json:"table"`
	Actions []AlterAction `json:"actions"`
}

// AlterAction 表示 ALTER TABLE 中的一条变更动作。
type AlterAction struct {
	Action string `json:"action"`
	Detail string `json:"detail,omitempty"`
}

// ViewDetails 表示 CREATE VIEW 语句的解析结果。
type ViewDetails struct {
	ViewName    string         `json:"view_name"`
	OrReplace   bool           `json:"or_replace,omitempty"`
	IfNotExists bool           `json:"if_not_exists,omitempty"`
	Columns     []string       `json:"columns,omitempty"`
	Algorithm   string         `json:"algorithm,omitempty"`
	Definer     string         `json:"definer,omitempty"`
	Security    string         `json:"security,omitempty"`
	CheckOption string         `json:"check_option,omitempty"`
	Definition  string         `json:"definition,omitempty"`
	SelectData  *SelectDetails `json:"select_data,omitempty"`
}

// ProcedureDetails 表示 CREATE PROCEDURE 语句的解析结果。
type ProcedureDetails struct {
	Name            string           `json:"name"`
	OrReplace       bool             `json:"or_replace,omitempty"`
	Definer         string           `json:"definer,omitempty"`
	Params          []ProcedureParam `json:"params,omitempty"`
	Characteristics []string         `json:"characteristics,omitempty"`
	Body            string           `json:"body,omitempty"`
	Statements      []string         `json:"statements,omitempty"`
}

// ProcedureParam 表示存储过程的一个参数。
type ProcedureParam struct {
	Direction string `json:"direction,omitempty"`
	Name      string `json:"name"`
	Type      string `json:"type"`
}

// TriggerDetails 表示 CREATE TRIGGER 语句的解析结果。
type TriggerDetails struct {
	TriggerName string `json:"trigger_name"`
	Table       string `json:"table"`
	Time        string `json:"time,omitempty"`
	Event       string `json:"event,omitempty"`
	Definer     string `json:"definer,omitempty"`
	Order       string `json:"order,omitempty"`
	Body        string `json:"body,omitempty"`
}

// CallDetails 表示 CALL 语句的解析结果。
type CallDetails struct {
	Procedure string   `json:"procedure"`
	Params    []string `json:"params,omitempty"`
}

// DropDetails 表示 DROP 语句的解析结果。
type DropDetails struct {
	Objects  []string `json:"objects"`
	Kind     string   `json:"kind,omitempty"`
	IfExists bool     `json:"if_exists,omitempty"`
	// Cascade 表示 DROP VIEW ... CASCADE（vitess 不支持，解析前剥离、解析后补回）。
	Cascade   bool `json:"cascade,omitempty"`
	Procedure bool `json:"procedure,omitempty"`
}

// RenameTableDetails 表示 RENAME TABLE 语句的解析结果。
type RenameTableDetails struct {
	Renames []RenamePair `json:"renames"`
}

// RenamePair 表示一次表重命名。
type RenamePair struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// TruncateDetails 表示 TRUNCATE 语句的解析结果。
type TruncateDetails struct {
	Table string `json:"table"`
	// Partition 是 TRUNCATE TABLE t PARTITION p0 里的分区名
	// （vitess 不支持，解析前剥离、解析后补回）。
	Partition string `json:"partition,omitempty"`
}

// CTEInfo 表示一个公共表表达式（WITH 子句）。
type CTEInfo struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns,omitempty"`
	Query   string   `json:"query,omitempty"`
}

// TransactionDetails 表示事务控制语句（START TRANSACTION / BEGIN）。
type TransactionDetails struct {
	Characteristic string `json:"characteristic,omitempty"`
}

// LockTableInfo 表示 LOCK TABLES 里的一张表及其锁类型。
type LockTableInfo struct {
	Table string `json:"table"`
	Lock  string `json:"lock,omitempty"`
}

// LockTablesDetails 表示 LOCK TABLES 语句。
type LockTablesDetails struct {
	Tables []LockTableInfo `json:"tables"`
}

// PrepareDetails 表示 PREPARE 语句。
type PrepareDetails struct {
	Name string `json:"name"`
	Expr string `json:"expression"`
}

// ExecuteDetails 表示 EXECUTE 语句。
type ExecuteDetails struct {
	Name    string   `json:"name"`
	VarList []string `json:"variables,omitempty"`
}

// DeallocateDetails 表示 DEALLOCATE PREPARE 语句。
type DeallocateDetails struct {
	Name string `json:"name"`
}

// LoadDetails 表示 LOAD DATA INFILE 语句。
type LoadDetails struct {
	Local     bool     `json:"local,omitempty"`
	Infile    string   `json:"infile"`
	Table     string   `json:"table"`
	Charset   string   `json:"charset,omitempty"`
	IgnoreNum int      `json:"ignore_lines,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	Fields    []string `json:"fields,omitempty"`
	Lines     []string `json:"lines,omitempty"`
	Mode      string   `json:"mode,omitempty"`
}

// ExplainDetails 表示 EXPLAIN 语句。
type ExplainDetails struct {
	Analyze bool   `json:"analyze,omitempty"`
	Format  string `json:"format,omitempty"`
	Query   string `json:"query"`
}

// IndexDetails 表示 CREATE / DROP INDEX 语句。
type IndexDetails struct {
	Action  string   `json:"action"`
	Name    string   `json:"name,omitempty"`
	Table   string   `json:"table,omitempty"`
	Type    string   `json:"type,omitempty"`
	Unique  bool     `json:"unique,omitempty"`
	Using   string   `json:"using,omitempty"`
	Columns []string `json:"columns,omitempty"`
	// ColumnPrefixes 是各索引列的前缀长度——`CREATE INDEX idx ON t (col(30))`
	// 里的 30。键是列名，值是前缀长度。
	ColumnPrefixes map[string]int `json:"column_prefixes,omitempty"`
}

// PartitionDetails 表示建表语句的分区定义。
type PartitionDetails struct {
	Type          string              `json:"type"`
	Linear        bool                `json:"linear,omitempty"`
	KeyAlgorithm  string              `json:"key_algorithm,omitempty"`
	Columns       []string            `json:"columns,omitempty"`
	Expr          string              `json:"expression,omitempty"`
	NumPartitions int                 `json:"num_partitions,omitempty"`
	SubPartition  *SubPartitionDetail `json:"sub_partition,omitempty"`
	Definitions   []PartitionDef      `json:"definitions,omitempty"`
}

// SubPartitionDetail 表示子分区定义。
type SubPartitionDetail struct {
	Type          string         `json:"type"`
	Columns       []string       `json:"columns,omitempty"`
	Expr          string         `json:"expression,omitempty"`
	NumPartitions int            `json:"num_partitions,omitempty"`
	Names         []string       `json:"names,omitempty"`
	Definitions   []PartitionDef `json:"definitions,omitempty"`
}

// PartitionDef 表示一个分区的取值范围。
type PartitionDef struct {
	Name string `json:"name"`
	// Limit 是 RANGE 分区的上界（`VALUES LESS THAN (100)` 里的 100）。
	Limit string `json:"limit,omitempty"`
	// Maxvalue 表示 `VALUES LESS THAN MAXVALUE`——MySQL 的标准写法**不带括号**。
	Maxvalue bool `json:"maxvalue,omitempty"`
	// Values 是 LIST 分区的取值列表（`VALUES IN (1,2,3)`）。
	Values []string `json:"values,omitempty"`
	// SubPartitions 是子分区名列表（`SUBPARTITION (s0, s1)`）。
	SubPartitions []string `json:"sub_partitions,omitempty"`
}

// extractASTStatement 将 AST 转换为结构化描述。
// 返回 nil 表示该语句类型未在 AST 层处理（可回退到 plan 层）。
//
// ctx 携带原始 SQL 与解析前剥离出来的函数索引。
func extractASTStatement(stmt sqlparser.Statement, ctx *astContext) *ASTStatement {
	result := extractASTStatementRaw(stmt, ctx)
	if result == nil {
		return nil
	}
	// 把解析前剥离掉的信息补回结果
	result.Details = applyFixupsToDetails(result.Details, ctx)
	return result
}

// applyFixupsToDetails 把解析前剥离掉的信息补回解析结果。
//
// vitess 不支持的几处语法（INSERT 优先级、GROUP BY ... WITH ROLLUP、
// TRUNCATE 的分区名、DROP VIEW 的 CASCADE）在解析前被摘掉了，
// 这里从 ctx.Fixups 里把原文捞回来补进 Details——
// 否则这些信息就彻底丢了，调用方看到的语句跟用户写的不是一回事。
func applyFixupsToDetails(details any, ctx *astContext) any {
	if ctx == nil {
		return details
	}
	for _, fixup := range ctx.Fixups {
		switch fixup.Kind {
		case fixupKindInsertPriority:
			if insert, ok := details.(*InsertDetails); ok {
				insert.Priority = strings.ToLower(fixup.Original)
			}
		case fixupKindWithRollup:
			if sel, ok := details.(*SelectDetails); ok {
				sel.WithRollup = true
			}
		case fixupKindTruncatePart:
			if trunc, ok := details.(*TruncateDetails); ok {
				trunc.Partition = strings.TrimSpace(strings.TrimPrefix(fixup.Original, "PARTITION "))
			}
		case fixupKindDropViewCasc:
			if drop, ok := details.(*DropDetails); ok {
				drop.Cascade = strings.EqualFold(fixup.Original, "CASCADE")
			}
		}
	}
	return details
}

// extractASTStatementRaw 是 extractASTStatement 的真正实现。
func extractASTStatementRaw(stmt sqlparser.Statement, ctx *astContext) *ASTStatement {
	switch node := stmt.(type) {
	case *sqlparser.DDL:
		return extractDDL(node, ctx)
	case *sqlparser.AlterTable:
		return extractAlterTable(node, ctx)
	case *sqlparser.Select:
		return &ASTStatement{Type: "Select", Details: extractSelectFromAST(node)}
	case *sqlparser.SetOp:
		return &ASTStatement{Type: "Select", Details: extractSetOpFromAST(node)}
	case *sqlparser.Insert:
		return &ASTStatement{Type: "Insert", Details: extractInsertFromAST(node)}
	case *sqlparser.Update:
		return &ASTStatement{Type: "Update", Details: extractUpdateFromAST(node)}
	case *sqlparser.Delete:
		return &ASTStatement{Type: "Delete", Details: extractDeleteFromAST(node)}
	case *sqlparser.Call:
		return &ASTStatement{
			Type: "Call",
			Details: &CallDetails{
				Procedure: node.ProcName.String(),
				Params:    exprsToStrings(node.Params),
			},
		}
	case *sqlparser.Set:
		return &ASTStatement{
			Type: "Set",
			Details: map[string]string{
				"set": sqlparser.String(node),
			},
		}
	case *sqlparser.Use:
		return &ASTStatement{
			Type: "Use",
			Details: map[string]string{
				"database": node.DBName.String(),
			},
		}
	case *sqlparser.Show:
		return extractShow(node)
	case *sqlparser.Begin:
		return &ASTStatement{
			Type:    "Begin",
			Details: &TransactionDetails{Characteristic: node.TransactionCharacteristic},
		}
	case *sqlparser.Commit:
		return &ASTStatement{Type: "Commit", Details: map[string]string{}}
	case *sqlparser.Rollback:
		return &ASTStatement{Type: "Rollback", Details: map[string]string{}}
	case *sqlparser.LockTables:
		return &ASTStatement{Type: "LockTables", Details: extractLockTables(node)}
	case *sqlparser.UnlockTables:
		return &ASTStatement{Type: "UnlockTables", Details: map[string]string{}}
	case *sqlparser.Prepare:
		return &ASTStatement{
			Type:    "Prepare",
			Details: &PrepareDetails{Name: node.Name, Expr: node.Expr},
		}
	case *sqlparser.Execute:
		return &ASTStatement{
			Type:    "Execute",
			Details: &ExecuteDetails{Name: node.Name, VarList: node.VarList},
		}
	case *sqlparser.Deallocate:
		return &ASTStatement{
			Type:    "Deallocate",
			Details: &DeallocateDetails{Name: node.Name},
		}
	case *sqlparser.Load:
		return &ASTStatement{Type: "Load", Details: extractLoad(node)}
	case *sqlparser.Explain:
		return &ASTStatement{Type: "Explain", Details: extractExplain(node, ctx)}
	case *sqlparser.ParenSelect:
		return &ASTStatement{Type: "Select", Details: extractSelectFromAny(node)}
	case *sqlparser.Flush:
		return &ASTStatement{Type: "Flush", Details: extractFlush(node)}
	case *sqlparser.Analyze:
		return &ASTStatement{Type: "TableMaintenance", Details: extractAnalyze(node)}
	case *sqlparser.Kill:
		return &ASTStatement{Type: "Kill", Details: extractKill(node)}
	case *sqlparser.Savepoint:
		return &ASTStatement{Type: "Savepoint", Details: extractSavepoint("savepoint", node.Identifier)}
	case *sqlparser.ReleaseSavepoint:
		return &ASTStatement{Type: "Savepoint", Details: extractSavepoint("release", node.Identifier)}
	case *sqlparser.RollbackSavepoint:
		return &ASTStatement{Type: "Savepoint", Details: extractSavepoint("rollback", node.Identifier)}
	case *sqlparser.ShowGrants:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "show_grants", SQL: sqlparser.String(node)}}
	case *sqlparser.ShowPrivileges:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "show_privileges", SQL: sqlparser.String(node)}}
	case *sqlparser.GrantPrivilege, *sqlparser.GrantRole, *sqlparser.GrantProxy:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "grant", SQL: sqlparser.String(node)}}
	case *sqlparser.RevokePrivilege, *sqlparser.RevokeAllPrivileges, *sqlparser.RevokeRole, *sqlparser.RevokeProxy:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "revoke", SQL: sqlparser.String(node)}}
	case *sqlparser.CreateUser:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "create_user", SQL: sqlparser.String(node)}}
	case *sqlparser.DropUser:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "drop_user", SQL: sqlparser.String(node)}}
	case *sqlparser.RenameUser:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "rename_user", SQL: sqlparser.String(node)}}
	case *sqlparser.CreateRole:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "create_role", SQL: sqlparser.String(node)}}
	case *sqlparser.DropRole:
		return &ASTStatement{Type: "Privileges", Details: &PrivilegeDetails{Kind: "drop_role", SQL: sqlparser.String(node)}}

	// 管理/复制类语句：vitess 能解析，但语义层的节点里什么都不剩，
	// 这里按类别归拢，原始语句一并带回去。
	case *sqlparser.OtherRead:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind: "other_read",
			SQL:  originalSQL(ctx),
		}}
	case *sqlparser.OtherAdmin:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind: "other_admin",
			SQL:  originalSQL(ctx),
		}}
	case *sqlparser.DBDDL:
		return &ASTStatement{Type: "Database", Details: &DatabaseDetails{
			Action:      strings.ToLower(node.Action),
			Name:        node.DBName,
			IfNotExists: node.IfNotExists,
			IfExists:    node.IfExists,
			SQL:         originalSQL(ctx),
		}}
	case *sqlparser.PurgeBinaryLogs:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind: "purge_binary_logs",
			SQL:  originalSQL(ctx),
		}}
	case *sqlparser.StartReplica:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind:   "replica",
			Action: "start",
			SQL:    originalSQL(ctx),
		}}
	case *sqlparser.StopReplica:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind:   "replica",
			Action: "stop",
			SQL:    originalSQL(ctx),
		}}
	case *sqlparser.ResetReplica:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind:   "replica",
			Action: "reset",
			All:    node.All,
			SQL:    originalSQL(ctx),
		}}
	case *sqlparser.ChangeReplicationSource:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind:    "replica",
			Action:  "change_source",
			Options: replicationOptionsText(node.Options),
			SQL:     originalSQL(ctx),
		}}
	case *sqlparser.ChangeReplicationFilter:
		return &ASTStatement{Type: "Admin", Details: &AdminStatementDetails{
			Kind:    "replica",
			Action:  "change_filter",
			Options: replicationOptionsText(node.Options),
			SQL:     originalSQL(ctx),
		}}
	}
	return nil
}

// DatabaseDetails 表示 CREATE/ALTER/DROP DATABASE 语句的细节。
type DatabaseDetails struct {
	// Action 是动作：create / alter / drop。
	Action string `json:"action"`
	// Name 是库名。
	Name string `json:"name"`
	// IfNotExists / IfExists 是存在性修饰。
	IfNotExists bool `json:"if_not_exists,omitempty"`
	IfExists    bool `json:"if_exists,omitempty"`
	// SQL 是原始语句文本。
	SQL string `json:"sql"`
}

// AdminStatementDetails 表示管理/复制类语句的细节。
//
// 这类语句（SHOW ENGINE INNODB STATUS、RESET MASTER、START REPLICA、
// CHANGE REPLICATION SOURCE ... 等）在 vitess 的 AST 层里没有可用的字段，
// 所以归成一类，把类别、动作、选项和原始语句带回去。
type AdminStatementDetails struct {
	// Kind 是语句类别：other_read / other_admin / replica。
	Kind string `json:"kind"`
	// Action 是具体动作：start / stop / reset / change_source / change_filter。
	Action string `json:"action,omitempty"`
	// All 表示 RESET REPLICA ALL 里的 ALL。
	All bool `json:"all,omitempty"`
	// Target 是操作对象：slave / replica。
	Target string `json:"target,omitempty"`
	// Options 是 CHANGE ... TO 后面的选项列表。
	Options []string `json:"options,omitempty"`
	// SQL 是原始语句文本。
	SQL string `json:"sql"`
}

// originalSQL 取回原始语句文本。
func originalSQL(ctx *astContext) string {
	if ctx == nil {
		return ""
	}
	return strings.TrimSpace(ctx.SQL)
}

// replicationOptionsText 渲染复制语句的选项列表。
//
// 密码类的选项值会被打码——解析结果经常要往外发，不能把口令带出去。
func replicationOptionsText(options []*sqlparser.ReplicationOption) []string {
	if len(options) == 0 {
		return nil
	}
	texts := make([]string, 0, len(options))
	for _, option := range options {
		if option == nil {
			continue
		}
		name := option.Name
		value := fmt.Sprintf("%v", option.Value)
		if isSecretOption(name) {
			value = "***"
		}
		texts = append(texts, name+"="+value)
	}
	return texts
}

// isSecretOption 判断选项名是不是口令类的。
func isSecretOption(name string) bool {
	upper := strings.ToUpper(name)
	return strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "SECRET")
}

// tableFullName 返回带库名的全限定表名。
//
// vitess 的 TableName 里库名放在 DbQualifier 上，直接取 Name 会把库名丢掉，
// 这里补回成 "库.表" 的形式（没有库名时就是表名本身）。
func tableFullName(name sqlparser.TableName) string {
	if name.DbQualifier.IsEmpty() {
		return name.Name.String()
	}
	return name.DbQualifier.String() + "." + name.Name.String()
}

// extractLockTables 提取 LOCK TABLES 的表与锁类型。
func extractLockTables(node *sqlparser.LockTables) *LockTablesDetails {
	details := LockTablesDetails{Tables: []LockTableInfo{}}
	for _, item := range node.Tables {
		if item == nil {
			continue
		}
		info := LockTableInfo{Lock: string(item.Lock)}
		if names := collectTableNames(sqlparser.TableExprs{item.Table}); len(names) > 0 {
			info.Table = names[0]
		}
		details.Tables = append(details.Tables, info)
	}
	return &details
}

// extractLoad 提取 LOAD DATA INFILE 的细节。
func extractLoad(node *sqlparser.Load) *LoadDetails {
	details := LoadDetails{
		Local:   bool(node.Local),
		Infile:  node.Infile,
		Table:   tableFullName(node.Table),
		Charset: node.Charset,
		Columns: []string{},
		Fields:  []string{},
		Lines:   []string{},
	}
	if node.IgnoreNum != nil {
		// Val 是整数字面量的字节串，取 [0] 会得到首字符的 ASCII 值
		//（"1" → 49），必须整体转成整数
		details.IgnoreNum = intVal(node.IgnoreNum)
	}
	for _, col := range node.Columns {
		details.Columns = append(details.Columns, col.String())
	}
	if node.Fields != nil {
		if node.Fields.TerminatedBy != nil {
			details.Fields = append(details.Fields, "terminated by "+string(node.Fields.TerminatedBy.Val))
		}
		if node.Fields.EnclosedBy != nil {
			enclosed := "enclosed by "
			if bool(node.Fields.EnclosedBy.Optionally) {
				enclosed = "optionally enclosed by "
			}
			if node.Fields.EnclosedBy.Delim != nil {
				details.Fields = append(details.Fields, enclosed+string(node.Fields.EnclosedBy.Delim.Val))
			}
		}
		if node.Fields.EscapedBy != nil {
			details.Fields = append(details.Fields, "escaped by "+string(node.Fields.EscapedBy.Val))
		}
	}
	if node.Lines != nil {
		if node.Lines.StartingBy != nil {
			details.Lines = append(details.Lines, "starting by "+string(node.Lines.StartingBy.Val))
		}
		if node.Lines.TerminatedBy != nil {
			details.Lines = append(details.Lines, "terminated by "+string(node.Lines.TerminatedBy.Val))
		}
	}
	details.Mode = node.IgnoreOrReplace
	return &details
}

// extractExplain 提取 EXPLAIN 的细节。
func extractExplain(node *sqlparser.Explain, ctx *astContext) *ExplainDetails {
	details := ExplainDetails{
		Analyze: node.Analyze,
		Format:  node.ExplainFormat,
	}
	// FORMAT 的值在解析前被换成了占位标识符，这里换回原值
	if ctx != nil {
		for _, fixup := range ctx.Fixups {
			if fixup.Kind == fixupKindExplainFormat {
				details.Format = fixup.Original
			}
		}
	}
	if node.Statement != nil {
		details.Query = sqlparser.String(node.Statement)
	}
	return &details
}

// extractDDL 处理 CREATE/ALTER/DROP/RENAME/TRUNCATE 等 DDL 语句。
func extractDDL(node *sqlparser.DDL, ctx *astContext) *ASTStatement {
	switch node.Action {
	case sqlparser.CreateStr:
		switch {
		case node.ViewSpec != nil:
			return &ASTStatement{Type: "CreateView", Details: extractCreateView(node)}
		case node.TriggerSpec != nil:
			return &ASTStatement{Type: "CreateTrigger", Details: extractCreateTrigger(node)}
		case node.ProcedureSpec != nil:
			return &ASTStatement{Type: "CreateProcedure", Details: extractCreateProcedure(node)}
		default:
			return &ASTStatement{Type: "CreateTable", Details: extractCreateTable(node, ctx)}
		}
	case sqlparser.DropStr:
		return extractDrop(node)
	case sqlparser.RenameStr:
		return extractRename(node)
	case sqlparser.TruncateStr:
		return &ASTStatement{
			Type:    "Truncate",
			Details: &TruncateDetails{Table: tableFullName(node.Table)},
		}
	case sqlparser.AlterStr:
		return &ASTStatement{Type: "Alter", Details: extractAlterActions(node, ctx)}
	}
	return nil
}

// extractCreateTable 解析 CREATE TABLE。
func extractCreateTable(node *sqlparser.DDL, ctx *astContext) *CreateTableDetails {
	details := CreateTableDetails{
		Table:       tableFullName(node.Table),
		IfNotExists: node.IfNotExists,
		Temporary:   node.Temporary,
		Columns:     []ColumnInfo{},
		Indexes:     []IndexInfo{},
		ForeignKeys: []ForeignKey{},
		Checks:      []CheckConstraint{},
		Options:     []string{},
	}

	if node.OptLike != nil && len(node.OptLike.LikeTables) > 0 {
		details.Like = node.OptLike.LikeTables[0].Name.String()
	}
	if node.OptSelect != nil && node.OptSelect.Select != nil {
		details.AsSelect = sqlparser.String(node.OptSelect.Select)
		details.SelectData = extractSelectFromAny(node.OptSelect.Select)
	}

	spec := node.TableSpec
	if spec == nil {
		return &details
	}

	for _, col := range spec.Columns {
		details.Columns = append(details.Columns, *extractColumn(col, ctx))
	}

	for _, idx := range spec.Indexes {
		info := IndexInfo{Columns: []string{}}
		if idx.Info != nil {
			info.Name = idx.Info.Name.String()
			info.Type = idx.Info.Type
			info.Unique = idx.Info.Unique
			info.Primary = idx.Info.Primary
		}
		for _, col := range idx.Columns {
			info.Columns = append(info.Columns, col.Column.String())
			// `KEY idx (col(30))` 的 30 是前缀长度，vitess 存在 Length 里
			if col.Length != nil {
				if info.ColumnPrefixes == nil {
					info.ColumnPrefixes = map[string]int{}
				}
				info.ColumnPrefixes[col.Column.String()] = intVal(col.Length)
			}
		}
		// 函数索引在解析前被替换成了占位列，这里还原成真正的表达式索引
		if len(info.Columns) == 1 && ctx != nil {
			if fn, ok := ctx.FnIndexes.lookup(info.Columns[0]); ok {
				info.Name = fn.Name
				info.Expression = fn.Expr
				info.Unique = fn.Unique
				info.FullText = fn.FullText
				info.Spatial = fn.Spatial
				info.Using = fn.Using
				info.Columns = []string{}
			}
		}
		if info.Primary {
			details.PrimaryKey = info.Columns
		}
		details.Indexes = append(details.Indexes, info)
	}

	// 列级 PRIMARY KEY 定义也要计入主键
	for _, col := range details.Columns {
		if col.PrimaryKey {
			details.PrimaryKey = append(details.PrimaryKey, col.Name)
		}
	}

	for _, constraint := range spec.Constraints {
		switch det := constraint.Details.(type) {
		case *sqlparser.ForeignKeyDefinition:
			details.ForeignKeys = append(details.ForeignKeys, ForeignKey{
				Name:              constraint.Name,
				Columns:           colsToStrings(det.Source),
				ReferencedTable:   tableFullName(det.ReferencedTable),
				ReferencedColumns: colsToStrings(det.ReferencedColumns),
				OnDelete:          referenceActionName(det.OnDelete),
				OnUpdate:          referenceActionName(det.OnUpdate),
			})
		case *sqlparser.CheckConstraintDefinition:
			details.Checks = append(details.Checks, CheckConstraint{
				Name:       constraint.Name,
				Expression: sqlparser.String(det.Expr),
				Enforced:   det.Enforced,
			})
		}
	}

	for _, opt := range spec.TableOpts {
		details.Options = append(details.Options, fmt.Sprintf("%s=%s", opt.Name, opt.Value))
	}

	if spec.PartitionOpt != nil {
		details.Partition = extractPartition(spec.PartitionOpt)
	} else if ctx != nil && ctx.Partition != nil {
		// vitess 只认最简的 HASH 形式，其余 PARTITION 子句是我们自己解析的
		details.Partition = ctx.Partition
	}

	return &details
}

// extractPartition 提取建表语句的分区定义。
func extractPartition(opt *sqlparser.PartitionOption) *PartitionDetails {
	details := &PartitionDetails{
		Type:         opt.PartitionType,
		Linear:       opt.IsLinear,
		KeyAlgorithm: opt.KeyAlgorithm,
		Columns:      []string{},
	}
	for _, col := range opt.ColList {
		details.Columns = append(details.Columns, col.String())
	}
	if opt.Expr != nil {
		details.Expr = sqlparser.String(opt.Expr)
	}
	if opt.Partitions != nil {
		if value, err := strconv.Atoi(string(opt.Partitions.Val)); err == nil {
			details.NumPartitions = value
		}
	}
	for _, def := range opt.Definitions {
		details.Definitions = append(details.Definitions, PartitionDef{
			Name:     def.Name.String(),
			Maxvalue: def.Maxvalue,
		})
		if def.Limit != nil {
			details.Definitions[len(details.Definitions)-1].Limit = sqlparser.String(def.Limit)
		}
	}
	if opt.SubPartition != nil {
		sub := &SubPartitionDetail{
			Type:    opt.SubPartition.PartitionType,
			Columns: []string{},
		}
		for _, col := range opt.SubPartition.ColList {
			sub.Columns = append(sub.Columns, col.String())
		}
		if opt.SubPartition.Expr != nil {
			sub.Expr = sqlparser.String(opt.SubPartition.Expr)
		}
		if opt.SubPartition.SubPartitions != nil {
			if value, err := strconv.Atoi(string(opt.SubPartition.SubPartitions.Val)); err == nil {
				sub.NumPartitions = value
			}
		}
		details.SubPartition = sub
	}
	return details
}

// extractColumn 解析单个列定义。
func extractColumn(col *sqlparser.ColumnDefinition, ctx *astContext) *ColumnInfo {
	ct := col.Type
	info := ColumnInfo{
		Name:          col.Name.String(),
		Type:          columnTypeText(ct, bool(ct.Zerofill) || (ctx != nil && ctx.ZerofillCols[col.Name.String()])),
		Nullable:      !bool(ct.NotNull),
		AutoIncrement: bool(ct.Autoincrement),
		Unsigned:      bool(ct.Unsigned),
		Zerofill:      bool(ct.Zerofill) || (ctx != nil && ctx.ZerofillCols[col.Name.String()]),
		Charset:       ct.Charset,
		Collation:     ct.Collate,
	}
	if len(ct.EnumValues) > 0 {
		info.EnumValues = ct.EnumValues
	}

	if ct.Default != nil {
		info.HasDefault = true
		info.Default = sqlparser.String(ct.Default)
	}
	if ct.OnUpdate != nil {
		info.OnUpdate = sqlparser.String(ct.OnUpdate)
	}
	if ct.Comment != nil {
		info.Comment = string(ct.Comment.Val)
	}
	if ct.GeneratedExpr != nil {
		info.Generated = sqlparser.String(ct.GeneratedExpr)
	}
	// 主键/唯一键：vitess 有专门的 KeyOpt 字段，比字符串匹配可靠——
	// 字符串匹配会把 `COMMENT 'unique'` 里的 unique 当成唯一键。
	info.PrimaryKey = ct.KeyOpt == vitessKeyPrimary
	info.Unique = ct.KeyOpt == vitessKeyUnique || ct.KeyOpt == vitessKeyUniqueKey
	if info.PrimaryKey {
		// MySQL 里主键列隐含 NOT NULL（显式写 NULL 也会被 MySQL 改成 NOT NULL），
		// 但 vitess 的 AST 只看有没有写 NOT NULL 关键字，这里补上 MySQL 的语义。
		info.Nullable = false
	}
	return &info
}

// vitess 的 ColumnKeyOption 常量没有导出，只能按序号取值。
//
// 顺序见 vitess 的 ast.go：
//
//	colKeyNone=0 colKeyPrimary=1 colKeySpatialKey=2
//	colKeyUnique=3 colKeyUniqueKey=4 colKey=5 colKeyFulltextKey=6
//
// 万一 vitess 改了顺序，TestColumnKeyOptionMapping 会立刻报出来，
// 不会等到线上才发现主键认错了。
const (
	vitessKeyNone      = sqlparser.ColumnKeyOption(0)
	vitessKeyPrimary   = sqlparser.ColumnKeyOption(1)
	vitessKeyUnique    = sqlparser.ColumnKeyOption(3)
	vitessKeyUniqueKey = sqlparser.ColumnKeyOption(4)
)

// columnTypeText 拼出完整的列类型文本。
//
// vitess 的 ColumnType.Type 只有基础类型名（"decimal"），
// 精度/长度（"(20,4)"）与 UNSIGNED/ZEROFILL 在别的字段上，
// 这里拼成 "decimal(20,4) unsigned" 这样的完整形式。
func columnTypeText(ct sqlparser.ColumnType, zerofill bool) string {
	text := ct.Type
	if ct.Length != nil {
		text += "(" + string(ct.Length.Val)
		if ct.Scale != nil {
			text += "," + string(ct.Scale.Val)
		}
		text += ")"
	}
	if bool(ct.Unsigned) {
		text += " unsigned"
	}
	if zerofill {
		text += " zerofill"
	}
	return text
}

// referenceActionName 把外键引用动作转为可读名称。
func referenceActionName(action sqlparser.ReferenceAction) string {
	switch action {
	case sqlparser.Restrict:
		return "restrict"
	case sqlparser.Cascade:
		return "cascade"
	case sqlparser.NoAction:
		return "no action"
	case sqlparser.SetNull:
		return "set null"
	case sqlparser.SetDefault:
		return "set default"
	}
	return ""
}

// extractAlterTable 解析 ALTER TABLE（多动作形式）。
func extractAlterTable(node *sqlparser.AlterTable, ctx *astContext) *ASTStatement {
	// vitess 把独立的 CREATE INDEX / DROP INDEX 也表达成 AlterTable，
	// 这里按原始语句区分开。
	if ctx != nil {
		if isIndexStatement(ctx.SQL, "create") {
			return &ASTStatement{Type: "CreateIndex", Details: extractIndexDetails(node)}
		}
		if isIndexStatement(ctx.SQL, "drop") {
			return &ASTStatement{Type: "DropIndex", Details: extractIndexDetails(node)}
		}
	}

	actions := make([]AlterAction, 0, len(node.Statements))
	for _, ddl := range node.Statements {
		actions = append(actions, extractAlterAction(ddl))
	}
	if ctx != nil {
		actions = enrichAlterActions(actions, ctx.SQL)
	}
	return &ASTStatement{
		Type: "Alter",
		Details: &AlterTableDetails{
			Table:   tableFullName(node.Table),
			Actions: actions,
		},
	}
}

// isIndexStatement 判断原始语句是否是独立的 CREATE/DROP INDEX 语句。
//
// vitess 把它们表达成 AlterTable，靠原文关键字与 ALTER TABLE 区分。
func isIndexStatement(sqlStr, action string) bool {
	lower := strings.ToLower(strings.TrimSpace(sqlStr))
	if !strings.HasPrefix(lower, action+" ") {
		return false
	}
	rest := strings.TrimSpace(lower[len(action):])
	// CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX ...
	for _, keyword := range []string{"unique ", "fulltext ", "spatial "} {
		if strings.HasPrefix(rest, keyword) {
			rest = strings.TrimSpace(rest[len(keyword):])
			break
		}
	}
	return strings.HasPrefix(rest, "index ")
}

// extractIndexDetails 提取 CREATE/DROP INDEX 的细节。
func extractIndexDetails(node *sqlparser.AlterTable) *IndexDetails {
	details := &IndexDetails{Columns: []string{}}
	for _, ddl := range node.Statements {
		if ddl.IndexSpec == nil {
			continue
		}
		spec := ddl.IndexSpec
		details.Action = spec.Action
		details.Table = tableFullName(ddl.Table)
		details.Using = spec.Using.String()
		details.Type = spec.Type
		// 索引名在 ToName（新建）或 FromName（改名）里
		if !spec.ToName.IsEmpty() {
			details.Name = spec.ToName.String()
		} else if !spec.FromName.IsEmpty() {
			details.Name = spec.FromName.String()
		}
		details.Unique = strings.EqualFold(spec.Type, "unique")
		for _, col := range spec.Columns {
			details.Columns = append(details.Columns, col.Column.String())
			// `CREATE INDEX idx ON t (col(30))` 的 30 是前缀长度
			if col.Length != nil {
				if details.ColumnPrefixes == nil {
					details.ColumnPrefixes = map[string]int{}
				}
				details.ColumnPrefixes[col.Column.String()] = intVal(col.Length)
			}
		}
	}
	return details
}

// extractAlterActions 解析单个 ALTER 动作。
func extractAlterActions(node *sqlparser.DDL, ctx *astContext) any {
	actions := []AlterAction{extractAlterAction(node)}
	if ctx != nil {
		actions = enrichAlterActions(actions, ctx.SQL)
	}
	return AlterTableDetails{
		Table:   tableFullName(node.Table),
		Actions: actions,
	}
}

// extractAlterAction 解析一条 ALTER 动作。
func extractAlterAction(ddl *sqlparser.DDL) AlterAction {
	switch {
	case ddl.ColumnAction != "":
		return AlterAction{Action: ddl.ColumnAction, Detail: alterColumnDetail(ddl)}
	case ddl.ConstraintAction != "":
		detail := ""
		if ddl.TableSpec != nil {
			detail = columnSpecDetail(ddl.TableSpec)
		}
		return AlterAction{Action: ddl.ConstraintAction, Detail: detail}
	case ddl.IndexSpec != nil:
		return AlterAction{Action: "index", Detail: sqlparser.String(ddl.IndexSpec)}
	}
	return AlterAction{Action: ddl.Action, Detail: ddl.Action}
}

// alterColumnDetail 渲染列级 ALTER 动作的细节。
//
// vitess 把列定义放在 TableSpec 里（且会带上列名），
// 这里重组成 "列名 类型" 的形式，避免列名出现两次。
func alterColumnDetail(ddl *sqlparser.DDL) string {
	if !ddl.ToColumn.IsEmpty() {
		return fmt.Sprintf("%s TO %s", ddl.Column.String(), ddl.ToColumn.String())
	}
	detail := ddl.Column.String()
	if ddl.TableSpec != nil && len(ddl.TableSpec.Columns) > 0 {
		col := ddl.TableSpec.Columns[0]
		// CHANGE 动作里 ddl.Column 是旧列名，spec 里是新列名
		if col.Name.String() != ddl.Column.String() {
			detail = fmt.Sprintf("%s %s", detail, col.Name.String())
		}
		detail = fmt.Sprintf("%s %s", detail, col.Type.String())
	}
	if ddl.ColumnOrder != nil {
		if ddl.ColumnOrder.First {
			detail += " FIRST"
		} else if !ddl.ColumnOrder.AfterColumn.IsEmpty() {
			detail += " AFTER " + ddl.ColumnOrder.AfterColumn.String()
		}
	}
	return detail
}

// columnSpecDetail 渲染 TableSpec 里的约束定义。
func columnSpecDetail(spec *sqlparser.TableSpec) string {
	parts := []string{}
	for _, constraint := range spec.Constraints {
		parts = append(parts, sqlparser.String(constraint))
	}
	return strings.Join(parts, ", ")
}

// extractCreateView 解析 CREATE VIEW。
func extractCreateView(node *sqlparser.DDL) *ViewDetails {
	spec := node.ViewSpec
	details := ViewDetails{
		ViewName:    spec.ViewName.Name.String(),
		OrReplace:   node.OrReplace,
		IfNotExists: node.IfNotExists,
		Algorithm:   spec.Algorithm,
		Definer:     spec.Definer,
		Security:    spec.Security,
	}
	if spec.CheckOption != sqlparser.ViewCheckOptionUnspecified {
		details.CheckOption = string(spec.CheckOption)
	}
	for _, col := range spec.Columns {
		details.Columns = append(details.Columns, col.String())
	}
	if spec.ViewExpr != nil {
		details.Definition = sqlparser.String(spec.ViewExpr)
		details.SelectData = extractSelectFromAny(spec.ViewExpr)
	}
	return &details
}

// extractCreateProcedure 解析 CREATE PROCEDURE。
func extractCreateProcedure(node *sqlparser.DDL) *ProcedureDetails {
	spec := node.ProcedureSpec
	details := ProcedureDetails{
		Name:      spec.ProcName.String(),
		OrReplace: node.OrReplace,
		Definer:   spec.Definer,
		Params:    []ProcedureParam{},
	}
	for _, param := range spec.Params {
		details.Params = append(details.Params, ProcedureParam{
			Direction: string(param.Direction),
			Name:      param.Name,
			Type:      param.Type.String(),
		})
	}
	for _, characteristic := range spec.Characteristics {
		details.Characteristics = append(details.Characteristics, characteristic.String())
	}
	if spec.Body != nil {
		details.Body = sqlparser.String(spec.Body)
		details.Statements = extractBodyStatements(spec.Body)
	}
	return &details
}

// extractBodyStatements 提取存储过程/触发器 body 中的语句列表。
func extractBodyStatements(body sqlparser.Statement) []string {
	stmts := []string{}
	switch b := body.(type) {
	case *sqlparser.BeginEndBlock:
		for _, s := range b.Statements {
			stmts = append(stmts, sqlparser.String(s))
		}
	default:
		stmts = append(stmts, sqlparser.String(body))
	}
	return stmts
}

// extractCreateTrigger 解析 CREATE TRIGGER。
func extractCreateTrigger(node *sqlparser.DDL) *TriggerDetails {
	spec := node.TriggerSpec
	details := TriggerDetails{
		TriggerName: spec.TrigName.Name.String(),
		Table:       tableFullName(node.Table),
		Time:        spec.Time,
		Event:       spec.Event,
		Definer:     spec.Definer,
	}
	if spec.Order != nil {
		details.Order = fmt.Sprintf("%s %s", spec.Order.PrecedesOrFollows, spec.Order.OtherTriggerName)
	}
	if spec.Body != nil {
		details.Body = sqlparser.String(spec.Body)
	}
	return &details
}

// extractDrop 解析 DROP 语句。
func extractDrop(node *sqlparser.DDL) *ASTStatement {
	details := DropDetails{
		Objects:  []string{},
		IfExists: node.IfExists,
	}

	switch {
	case node.ProcedureSpec != nil:
		details.Procedure = true
		details.Kind = "procedure"
		details.Objects = append(details.Objects, node.ProcedureSpec.ProcName.String())
	case node.TriggerSpec != nil:
		details.Kind = "trigger"
		details.Objects = append(details.Objects, node.TriggerSpec.TrigName.Name.String())
	case len(node.FromViews) > 0:
		details.Kind = "view"
		for _, v := range node.FromViews {
			details.Objects = append(details.Objects, v.Name.String())
		}
	case len(node.FromTables) > 0:
		details.Kind = "table"
		for _, t := range node.FromTables {
			details.Objects = append(details.Objects, t.Name.String())
		}
	default:
		details.Kind = node.Action
		if tableFullName(node.Table) != "" {
			details.Objects = append(details.Objects, tableFullName(node.Table))
		}
	}

	return &ASTStatement{Type: "Drop", Details: &details}
}

// extractRename 解析 RENAME TABLE。
func extractRename(node *sqlparser.DDL) *ASTStatement {
	details := RenameTableDetails{Renames: []RenamePair{}}
	for i, from := range node.FromTables {
		pair := RenamePair{From: from.Name.String()}
		if i < len(node.ToTables) {
			pair.To = node.ToTables[i].Name.String()
		}
		details.Renames = append(details.Renames, pair)
	}
	return &ASTStatement{Type: "RenameTable", Details: &details}
}

// extractShow 解析 SHOW 语句。
func extractShow(node *sqlparser.Show) *ASTStatement {
	details := map[string]string{
		"show": node.Type,
	}
	if tableFullName(node.Table) != "" {
		details["table"] = tableFullName(node.Table)
	}
	if node.Database != "" {
		details["database"] = node.Database
	}
	return &ASTStatement{Type: "Show", Details: details}
}

// extractInsertFromAST 从 AST 提取 INSERT 细节。
func extractInsertFromAST(node *sqlparser.Insert) *InsertDetails {
	details := &InsertDetails{
		Table:       tableFullName(node.Table),
		Columns:     []string{},
		Values:      []string{},
		OnDuplicate: []string{},
	}
	for _, col := range node.Columns {
		details.Columns = append(details.Columns, col.String())
	}
	switch rows := node.Rows.(type) {
	case *sqlparser.AliasedValues:
		details.Values = extractValueTuples(rows.Values)
	case sqlparser.Values:
		details.Values = extractValueTuples(rows)
	case *sqlparser.Select:
		details.SelectData = extractSelectFromAST(rows)
	case *sqlparser.SetOp:
		details.SelectData = extractSetOpFromAST(rows)
	case *sqlparser.ParenSelect:
		// INSERT INTO t (SELECT ...) —— 括号包起来的子查询
		if sel, ok := rows.Select.(*sqlparser.Select); ok {
			details.SelectData = extractSelectFromAST(sel)
		}
	}
	// ON DUPLICATE KEY UPDATE
	for _, assignment := range node.OnDup {
		details.OnDuplicate = append(details.OnDuplicate,
			fmt.Sprintf("%s = %s", assignment.Name.String(), sqlparser.String(assignment.Expr)))
	}
	return details
}

// extractValueTuples 把 VALUES 元组列表转为字符串列表。
func extractValueTuples(rows sqlparser.Values) []string {
	values := []string{}
	for _, tuple := range rows {
		items := []string{}
		for _, expr := range tuple {
			items = append(items, sqlparser.String(expr))
		}
		values = append(values, "("+strings.Join(items, ", ")+")")
	}
	return values
}

// extractUpdateFromAST 从 AST 提取 UPDATE 细节。
func extractUpdateFromAST(node *sqlparser.Update) *UpdateDetails {
	details := &UpdateDetails{
		Table:      updateTableName(node.TableExprs),
		From:       tableExprsToString(node.TableExprs),
		SetClauses: []string{},
		OrderBy:    []string{},
	}
	for _, assignment := range node.Exprs {
		details.SetClauses = append(details.SetClauses,
			fmt.Sprintf("%s = %s", assignment.Name.String(), sqlparser.String(assignment.Expr)))
	}
	if node.Where != nil {
		details.Where = sqlparser.String(node.Where.Expr)
	}
	for _, order := range node.OrderBy {
		details.OrderBy = append(details.OrderBy, orderString(order))
	}
	if node.Limit != nil {
		details.Limit = limitRowCount(node.Limit)
		details.Offset = limitOffset(node.Limit)
	}
	return details
}

// extractDeleteFromAST 从 AST 提取 DELETE 细节。
func extractDeleteFromAST(node *sqlparser.Delete) *DeleteDetails {
	details := &DeleteDetails{
		Table:   deleteTableName(node.Targets, node.TableExprs),
		From:    tableExprsToString(node.TableExprs),
		OrderBy: []string{},
	}
	if node.Where != nil {
		details.Where = sqlparser.String(node.Where.Expr)
	}
	for _, order := range node.OrderBy {
		details.OrderBy = append(details.OrderBy, orderString(order))
	}
	if node.Limit != nil {
		details.Limit = limitRowCount(node.Limit)
		details.Offset = limitOffset(node.Limit)
	}
	return details
}

// updateTableName 从 UPDATE 的表表达式中取出表名（多表 JOIN 时全部列出）。
func updateTableName(exprs sqlparser.TableExprs) string {
	names := collectTableNames(exprs)
	return strings.Join(names, ", ")
}

// deleteTableName 从 DELETE 的目标表中取出表名。
func deleteTableName(targets sqlparser.TableNames, exprs sqlparser.TableExprs) string {
	if len(targets) > 0 {
		names := []string{}
		for _, target := range targets {
			names = append(names, target.Name.String())
		}
		return strings.Join(names, ", ")
	}
	return strings.Join(collectTableNames(exprs), ", ")
}

// extractSelectFromAST 从 AST 提取 SELECT 细节。
func extractSelectFromAST(sel *sqlparser.Select) *SelectDetails {
	details := &SelectDetails{}
	if sel.With != nil {
		details.CTEs = extractCTEs(sel.With)
	}
	details.Distinct = sel.QueryOpts.Distinct
	for _, selectExpr := range sel.SelectExprs {
		details.SelectedExprs = append(details.SelectedExprs, sqlparser.String(selectExpr))
	}
	details.From = tableExprsToString(sel.From)
	if sel.Where != nil {
		details.Where = sqlparser.String(sel.Where.Expr)
	}
	for _, groupBy := range sel.GroupBy {
		details.GroupBy = append(details.GroupBy, sqlparser.String(groupBy))
	}
	if sel.Having != nil {
		details.Having = sqlparser.String(sel.Having.Expr)
	}
	for _, order := range sel.OrderBy {
		details.OrderBy = append(details.OrderBy, orderString(order))
	}
	if sel.Limit != nil {
		details.Limit = limitRowCount(sel.Limit)
		details.Offset = limitOffset(sel.Limit)
	}
	return details
}

// extractSetOpFromAST 从 AST 提取集合运算（UNION/INTERSECT/EXCEPT）细节。
func extractSetOpFromAST(node *sqlparser.SetOp) *SelectDetails {
	details := &SelectDetails{}
	if node.With != nil {
		details.CTEs = extractCTEs(node.With)
	}
	// 左右分支的投影都收进来，方便看清整个结果集；
	// 同时把两侧的完整细节（FROM/WHERE/GROUP BY…）留在 SetOpBranches 里，
	// 否则集合运算各分支的表与条件就丢了。
	if left := extractSelectFromAny(node.Left); left != nil {
		details.SelectedExprs = append(details.SelectedExprs, left.SelectedExprs...)
		details.SetOpBranches = append(details.SetOpBranches, *left)
	}
	if right := extractSelectFromAny(node.Right); right != nil {
		details.SelectedExprs = append(details.SelectedExprs, right.SelectedExprs...)
		details.SetOpBranches = append(details.SetOpBranches, *right)
	}
	details.SetOp = node.Type
	for _, order := range node.OrderBy {
		details.OrderBy = append(details.OrderBy, orderString(order))
	}
	if node.Limit != nil {
		details.Limit = limitRowCount(node.Limit)
	}
	return details
}

// extractSelectFromAny 从任意 SelectStatement 提取 SELECT 细节。
func extractSelectFromAny(stmt sqlparser.SelectStatement) *SelectDetails {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		return extractSelectFromAST(node)
	case *sqlparser.SetOp:
		return extractSetOpFromAST(node)
	}
	return nil
}

// extractCTEs 提取 WITH 子句中的公共表表达式。
func extractCTEs(with *sqlparser.With) []CTEInfo {
	ctes := []CTEInfo{}
	for _, cte := range with.Ctes {
		info := CTEInfo{
			Name:    cte.As.String(),
			Columns: colsToStrings(cte.Columns),
		}
		if cte.Expr != nil {
			if tableName, ok := cte.Expr.(sqlparser.TableName); ok {
				info.Query = tableName.Name.String()
			} else {
				info.Query = sqlparser.String(cte.Expr)
			}
		}
		ctes = append(ctes, info)
	}
	return ctes
}

// limitRowCount 提取 LIMIT 的行数。
// AST 中 Rowcount 是表达式，字面量时可直接取整数值。
func limitRowCount(limit *sqlparser.Limit) int {
	if limit == nil || limit.Rowcount == nil {
		return 0
	}
	return intVal(limit.Rowcount)
}

// intVal 把一个整数字面量转成 int；不是整数字面量时返回 0。
func intVal(expr sqlparser.Expr) int {
	literal, ok := expr.(*sqlparser.SQLVal)
	if !ok || literal.Type != sqlparser.IntVal {
		return 0
	}
	n, err := strconv.Atoi(string(literal.Val))
	if err != nil {
		return 0
	}
	return n
}

// orderString 渲染一条 ORDER BY 项，带上排序方向。
//
// vitess 的 Order 结构是 {Expr, Direction}，只取 Expr 会把 DESC 丢掉——
// `ORDER BY a DESC` 与 `ORDER BY a` 渲染出来一模一样，方向信息全丢。
// 方向是 asc 时不重复输出（ASC 本来就是默认值），desc 时补上。
func orderString(order *sqlparser.Order) string {
	if order == nil {
		return ""
	}
	text := sqlparser.String(order.Expr)
	if strings.EqualFold(order.Direction, sqlparser.DescScr) {
		text += " desc"
	}
	return text
}

// tableExprsToString 把 FROM 子句的表表达式列表拼成字符串。
func tableExprsToString(exprs sqlparser.TableExprs) string {
	if len(exprs) == 0 {
		return ""
	}
	parts := []string{}
	for _, expr := range exprs {
		parts = append(parts, sqlparser.String(expr))
	}
	return strings.Join(parts, ", ")
}

// exprsToStrings 把表达式列表转为字符串列表。
func exprsToStrings(exprs []sqlparser.Expr) []string {
	result := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		result = append(result, sqlparser.String(expr))
	}
	return result
}

// colsToStrings 把列标识列表转为字符串列表。
func colsToStrings(cols sqlparser.Columns) []string {
	result := make([]string, 0, len(cols))
	for _, col := range cols {
		result = append(result, col.String())
	}
	return result
}
