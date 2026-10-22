package main

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

// parseAST 使用 vitess 语法解析器解析 SQL，返回 AST。
func parseAST(sqlStr string) (sqlparser.Statement, error) {
	return sqlparser.Parse(sqlStr)
}

// ASTStatement 表示 AST 层解析出的语句类型。
type ASTStatement struct {
	Type    string
	Details any
}

// CreateTableDetails 表示 CREATE TABLE 语句的解析结果。
type CreateTableDetails struct {
	Table       string         `json:"table"`
	IfNotExists bool           `json:"if_not_exists,omitempty"`
	Temporary   bool           `json:"temporary,omitempty"`
	Columns     []ColumnInfo   `json:"columns,omitempty"`
	PrimaryKey  []string       `json:"primary_key,omitempty"`
	Indexes     []IndexInfo    `json:"indexes,omitempty"`
	ForeignKeys []ForeignKey   `json:"foreign_keys,omitempty"`
	Checks      []string       `json:"checks,omitempty"`
	Options     []string       `json:"options,omitempty"`
	Like        string         `json:"like,omitempty"`
	AsSelect    string         `json:"as_select,omitempty"`
	SelectData  *SelectDetails `json:"select_data,omitempty"`
}

// ColumnInfo 表示一个列的定义。
type ColumnInfo struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	HasDefault    bool   `json:"has_default,omitempty"`
	Default       string `json:"default,omitempty"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	OnUpdate      string `json:"on_update,omitempty"`
	Comment       string `json:"comment,omitempty"`
	Charset       string `json:"charset,omitempty"`
	Collation     string `json:"collation,omitempty"`
	PrimaryKey    bool   `json:"primary_key,omitempty"`
	Unique        bool   `json:"unique,omitempty"`
	Generated     string `json:"generated,omitempty"`
}

// IndexInfo 表示一个索引定义。
type IndexInfo struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Unique  bool     `json:"unique,omitempty"`
	Primary bool     `json:"primary,omitempty"`
	Columns []string `json:"columns"`
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
	Objects   []string `json:"objects"`
	Kind      string   `json:"kind,omitempty"`
	IfExists  bool     `json:"if_exists,omitempty"`
	Procedure bool     `json:"procedure,omitempty"`
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
}

// CTEInfo 表示一个公共表表达式（WITH 子句）。
type CTEInfo struct {
	Name    string `json:"name"`
	Columns []string `json:"columns,omitempty"`
	Query   string `json:"query,omitempty"`
}

// extractASTStatement 将 AST 转换为结构化描述。
// 返回 nil 表示该语句类型未在 AST 层处理（可回退到 plan 层）。
func extractASTStatement(stmt sqlparser.Statement) *ASTStatement {
	switch node := stmt.(type) {
	case *sqlparser.DDL:
		return extractDDL(node)
	case *sqlparser.AlterTable:
		return extractAlterTable(node)
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
			Details: CallDetails{
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
	}
	return nil
}

// extractDDL 处理 CREATE/ALTER/DROP/RENAME/TRUNCATE 等 DDL 语句。
func extractDDL(node *sqlparser.DDL) *ASTStatement {
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
			return &ASTStatement{Type: "CreateTable", Details: extractCreateTable(node)}
		}
	case sqlparser.DropStr:
		return extractDrop(node)
	case sqlparser.RenameStr:
		return extractRename(node)
	case sqlparser.TruncateStr:
		return &ASTStatement{
			Type:    "Truncate",
			Details: TruncateDetails{Table: node.Table.Name.String()},
		}
	case sqlparser.AlterStr:
		return &ASTStatement{Type: "Alter", Details: extractAlterActions(node)}
	}
	return nil
}

// extractCreateTable 解析 CREATE TABLE。
func extractCreateTable(node *sqlparser.DDL) CreateTableDetails {
	details := CreateTableDetails{
		Table:       node.Table.Name.String(),
		IfNotExists: node.IfNotExists,
		Temporary:   node.Temporary,
		Columns:     []ColumnInfo{},
		Indexes:     []IndexInfo{},
		ForeignKeys: []ForeignKey{},
		Checks:      []string{},
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
		return details
	}

	for _, col := range spec.Columns {
		details.Columns = append(details.Columns, extractColumn(col))
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
				ReferencedTable:   det.ReferencedTable.Name.String(),
				ReferencedColumns: colsToStrings(det.ReferencedColumns),
				OnDelete:          referenceActionName(det.OnDelete),
				OnUpdate:          referenceActionName(det.OnUpdate),
			})
		case *sqlparser.CheckConstraintDefinition:
			details.Checks = append(details.Checks, sqlparser.String(det.Expr))
		}
	}

	for _, opt := range spec.TableOpts {
		details.Options = append(details.Options, fmt.Sprintf("%s=%s", opt.Name, opt.Value))
	}

	return details
}

// extractColumn 解析单个列定义。
func extractColumn(col *sqlparser.ColumnDefinition) ColumnInfo {
	ct := col.Type
	info := ColumnInfo{
		Name:          col.Name.String(),
		Type:          ct.Type,
		Nullable:      !bool(ct.NotNull),
		AutoIncrement: bool(ct.Autoincrement),
		Charset:       ct.Charset,
		Collation:     ct.Collate,
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
	// vitess 未导出 colKey* 常量，用渲染结果的特征来判断主键/唯一键
	typeStr := strings.ToLower(ct.String())
	info.PrimaryKey = strings.Contains(typeStr, "primary key")
	info.Unique = strings.Contains(typeStr, "unique")
	return info
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
func extractAlterTable(node *sqlparser.AlterTable) *ASTStatement {
	actions := make([]AlterAction, 0, len(node.Statements))
	for _, ddl := range node.Statements {
		actions = append(actions, extractAlterAction(ddl))
	}
	return &ASTStatement{
		Type: "Alter",
		Details: AlterTableDetails{
			Table:   node.Table.Name.String(),
			Actions: actions,
		},
	}
}

// extractAlterActions 解析单个 ALTER 动作。
func extractAlterActions(node *sqlparser.DDL) any {
	return AlterTableDetails{
		Table:   node.Table.Name.String(),
		Actions: []AlterAction{extractAlterAction(node)},
	}
}

// extractAlterAction 解析一条 ALTER 动作。
func extractAlterAction(ddl *sqlparser.DDL) AlterAction {
	switch {
	case ddl.ColumnAction != "":
		detail := ddl.Column.String()
		switch {
		case !ddl.ToColumn.IsEmpty():
			detail = fmt.Sprintf("%s TO %s", ddl.Column.String(), ddl.ToColumn.String())
		case ddl.TableSpec != nil:
			detail = fmt.Sprintf("%s %s", ddl.Column.String(), sqlparser.String(ddl.TableSpec))
		}
		return AlterAction{Action: ddl.ColumnAction, Detail: detail}
	case ddl.ConstraintAction != "":
		detail := ""
		if ddl.TableSpec != nil {
			detail = sqlparser.String(ddl.TableSpec)
		}
		return AlterAction{Action: ddl.ConstraintAction, Detail: detail}
	case ddl.IndexSpec != nil:
		return AlterAction{Action: "index", Detail: sqlparser.String(ddl.IndexSpec)}
	}
	return AlterAction{Action: ddl.Action, Detail: sqlparser.String(ddl)}
}

// extractCreateView 解析 CREATE VIEW。
func extractCreateView(node *sqlparser.DDL) ViewDetails {
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
	return details
}

// extractCreateProcedure 解析 CREATE PROCEDURE。
func extractCreateProcedure(node *sqlparser.DDL) ProcedureDetails {
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
	return details
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
func extractCreateTrigger(node *sqlparser.DDL) TriggerDetails {
	spec := node.TriggerSpec
	details := TriggerDetails{
		TriggerName: spec.TrigName.Name.String(),
		Table:       node.Table.Name.String(),
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
	return details
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
		if node.Table.Name.String() != "" {
			details.Objects = append(details.Objects, node.Table.Name.String())
		}
	}

	return &ASTStatement{Type: "Drop", Details: details}
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
	return &ASTStatement{Type: "RenameTable", Details: details}
}

// extractShow 解析 SHOW 语句。
func extractShow(node *sqlparser.Show) *ASTStatement {
	details := map[string]string{
		"show": node.Type,
	}
	if node.Table.Name.String() != "" {
		details["table"] = node.Table.Name.String()
	}
	if node.Database != "" {
		details["database"] = node.Database
	}
	return &ASTStatement{Type: "Show", Details: details}
}

// extractInsertFromAST 从 AST 提取 INSERT 细节。
func extractInsertFromAST(node *sqlparser.Insert) *InsertDetails {
	details := &InsertDetails{
		Table:   node.Table.Name.String(),
		Columns: []string{},
		Values:  []string{},
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
		details.OrderBy = append(details.OrderBy, sqlparser.String(order.Expr))
	}
	if node.Limit != nil {
		details.Limit = limitRowCount(node.Limit)
	}
	return details
}

// extractDeleteFromAST 从 AST 提取 DELETE 细节。
func extractDeleteFromAST(node *sqlparser.Delete) *DeleteDetails {
	details := &DeleteDetails{
		Table:   deleteTableName(node.Targets, node.TableExprs),
		OrderBy: []string{},
	}
	if node.Where != nil {
		details.Where = sqlparser.String(node.Where.Expr)
	}
	for _, order := range node.OrderBy {
		details.OrderBy = append(details.OrderBy, sqlparser.String(order.Expr))
	}
	if node.Limit != nil {
		details.Limit = limitRowCount(node.Limit)
	}
	return details
}

// updateTableName 从 UPDATE 的表表达式中取出表名。
func updateTableName(exprs sqlparser.TableExprs) string {
	for _, expr := range exprs {
		if aliased, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				return tableName.Name.String()
			}
		}
	}
	return ""
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
	for _, expr := range exprs {
		if aliased, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				return tableName.Name.String()
			}
		}
	}
	return ""
}

// extractSelectFromAST 从 AST 提取 SELECT 细节。
func extractSelectFromAST(sel *sqlparser.Select) *SelectDetails {
	details := &SelectDetails{}
	if sel.With != nil {
		details.CTEs = extractCTEs(sel.With)
	}
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
		details.OrderBy = append(details.OrderBy, sqlparser.String(order.Expr))
	}
	if sel.Limit != nil {
		details.Limit = limitRowCount(sel.Limit)
	}
	return details
}

// extractSetOpFromAST 从 AST 提取集合运算（UNION/INTERSECT/EXCEPT）细节。
func extractSetOpFromAST(node *sqlparser.SetOp) *SelectDetails {
	details := &SelectDetails{}
	if node.With != nil {
		details.CTEs = extractCTEs(node.With)
	}
	// 左右分支的投影都收进来，方便看清整个结果集
	if left := extractSelectFromAny(node.Left); left != nil {
		details.SelectedExprs = append(details.SelectedExprs, left.SelectedExprs...)
	}
	if right := extractSelectFromAny(node.Right); right != nil {
		details.SelectedExprs = append(details.SelectedExprs, right.SelectedExprs...)
	}
	details.SetOp = node.Type
	for _, order := range node.OrderBy {
		details.OrderBy = append(details.OrderBy, sqlparser.String(order.Expr))
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
	if literal, ok := limit.Rowcount.(*sqlparser.SQLVal); ok {
		if literal.Type == sqlparser.IntVal {
			if n, err := strconv.Atoi(string(literal.Val)); err == nil {
				return n
			}
		}
	}
	return 0
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
