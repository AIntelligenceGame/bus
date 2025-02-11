package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// SQLStatement represents a parsed SQL statement with its type and details.
type SQLStatement struct {
	Type    string      `json:"type"`
	Details interface{} `json:"details"`
}

// SelectDetails represents the details of a SELECT statement.
type SelectDetails struct {
	SelectedExprs []string `json:"selected_exprs"`
	From          string   `json:"from"`
	Where         string   `json:"where"`
	GroupBy       []string `json:"group_by"`
	Having        string   `json:"having"`
	OrderBy       []string `json:"order_by"`
	Limit         int      `json:"limit"`
}

func main() {
	// 定义要解析的SQL语句
	sqlStr := "CREATE TABLE tablename (id INT, name VARCHAR(20));"

	// 创建解析上下文
	ctx := sql.NewContext(nil)

	// 解析SQL语句
	stmt, err := parse.Parse(ctx, sqlStr)
	if err != nil {
		fmt.Println("parse error:", err)
		return
	}

	// 创建SQLStatement结构体实例
	var sqlStmt SQLStatement

	// 处理解析后的结构体
	switch node := stmt.(type) {
	case *plan.CreateTable:
		sqlStmt = SQLStatement{
			Type:    "CreateTable",
			Details: node,
		}
	case *plan.InsertInto:
		sqlStmt = SQLStatement{
			Type:    "InsertInto",
			Details: node,
		}
	case *plan.Update:
		sqlStmt = SQLStatement{
			Type:    "Update",
			Details: node,
		}
	case *plan.DeleteFrom:
		sqlStmt = SQLStatement{
			Type:    "DeleteFrom",
			Details: node,
		}
	case *plan.Project:
		selectDetails := SelectDetails{
			SelectedExprs: extractSelectedExprs(node),
			From:          extractFrom(node),
			Where:         extractWhere(node),
			GroupBy:       extractGroupBy(node),
			Having:        extractHaving(node),
			OrderBy:       extractOrderBy(node),
			Limit:         extractLimit(node),
		}
		sqlStmt = SQLStatement{
			Type:    "Select",
			Details: selectDetails,
		}
	case *plan.Filter:
		selectDetails := SelectDetails{
			Where: extractWhere(node),
		}
		sqlStmt = SQLStatement{
			Type:    "Filter",
			Details: selectDetails,
		}
	case *plan.GroupBy:
		selectDetails := SelectDetails{
			GroupBy: extractGroupBy(node),
		}
		sqlStmt = SQLStatement{
			Type:    "GroupBy",
			Details: selectDetails,
		}
	case *plan.Sort:
		selectDetails := SelectDetails{
			OrderBy: extractOrderBy(node),
		}
		sqlStmt = SQLStatement{
			Type:    "Sort",
			Details: selectDetails,
		}
	case *plan.Limit:
		// 获取子节点的详细信息
		childDetails := getChildDetails(node.Child)
		selectDetails := SelectDetails{
			SelectedExprs: childDetails.SelectedExprs,
			From:          childDetails.From,
			Where:         childDetails.Where,
			GroupBy:       childDetails.GroupBy,
			Having:        childDetails.Having,
			OrderBy:       childDetails.OrderBy,
			Limit:         extractLimit(node),
		}
		sqlStmt = SQLStatement{
			Type:    "Select",
			Details: selectDetails,
		}
	default:
		sqlStmt = SQLStatement{
			Type:    "Other",
			Details: stmt,
		}
	}

	// 将SQLStatement结构体实例转换为JSON格式并输出
	jsonOutput, err := json.MarshalIndent(sqlStmt, "", "  ")
	if err != nil {
		fmt.Println("json marshal error:", err)
		return
	}
	fmt.Println(string(jsonOutput))
}

// getChildDetails extracts details from the child node of a plan.Limit node.
func getChildDetails(node sql.Node) SelectDetails {
	var selectDetails SelectDetails
	switch child := node.(type) {
	case *plan.Project:
		selectDetails.SelectedExprs = extractSelectedExprs(child)
		selectDetails.From = extractFrom(child)
	case *plan.Filter:
		selectDetails.Where = extractWhere(child)
	case *plan.GroupBy:
		selectDetails.GroupBy = extractGroupBy(child)
	case *plan.Sort:
		selectDetails.OrderBy = extractOrderBy(child)
	}
	return selectDetails
}

// extractSelectedExprs extracts the selected expressions from a plan.Project node.
func extractSelectedExprs(node *plan.Project) []string {
	var exprs []string
	for _, expr := range node.Projections {
		exprs = append(exprs, expr.String())
	}
	return exprs
}

// extractFrom extracts the FROM clause from a plan.Project node.
func extractFrom(node *plan.Project) string {
	// Assuming the child node is a plan.ResolvedTable
	if child, ok := node.Child.(*plan.ResolvedTable); ok {
		return child.Name()
	}
	return ""
}

// extractWhere extracts the WHERE clause from a plan.Filter node.
func extractWhere(node sql.Node) string {
	if filter, ok := node.(*plan.Filter); ok {
		return filter.Expression.String()
	}
	return ""
}

// extractGroupBy extracts the GROUP BY clause from a plan.GroupBy node.
func extractGroupBy(node sql.Node) []string {
	if groupBy, ok := node.(*plan.GroupBy); ok {
		var groupByExprs []string
		for _, expr := range groupBy.GroupByExprs {
			groupByExprs = append(groupByExprs, expr.String())
		}
		return groupByExprs
	}
	return nil
}

// extractHaving extracts the HAVING clause from a plan.Filter node.
func extractHaving(node sql.Node) string {
	if filter, ok := node.(*plan.Filter); ok {
		return filter.Expression.String()
	}
	return ""
}

// extractOrderBy extracts the ORDER BY clause from a plan.Sort node.
func extractOrderBy(node sql.Node) []string {
	if sort, ok := node.(*plan.Sort); ok {
		var orderByExprs []string
		for _, field := range sort.SortFields {
			orderByExprs = append(orderByExprs, field.Column.String())
		}
		return orderByExprs
	}
	return nil
}

// extractLimit extracts the LIMIT clause from a plan.Limit node.
func extractLimit(node sql.Node) int {
	if limit, ok := node.(*plan.Limit); ok {
		// Assuming limit.Limit is a sql.Expression
		if limitExpr, ok := limit.Limit.(*expression.Literal); ok {
			rowCount, err := strconv.Atoi(fmt.Sprintf("%v", limitExpr.Value()))
			if err != nil {
				return 0
			}
			return rowCount
		}
	}
	return 0
}
