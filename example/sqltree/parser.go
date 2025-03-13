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
	sqlStr := "select c,count(*) cnt from tablename where a = 1 and b = 2 group by c having count(*) > 1 order by c desc limit 10"

	// 创建解析上下文
	ctx := sql.NewContext(nil)

	// 解析SQL语句
	stmt, err := parse.Parse(ctx, sqlStr)
	if err != nil {
		fmt.Println("parse error:", err)
		return
	}
	fmt.Println("parse top:", stmt.String())

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
		selectDetails := getChildDetails(node)
		sqlStmt = SQLStatement{
			Type:    "Select",
			Details: selectDetails,
		}
	case *plan.Filter:
		selectDetails := getChildDetails(node)
		sqlStmt = SQLStatement{
			Type:    "Filter",
			Details: selectDetails,
		}
	case *plan.GroupBy:
		selectDetails := getChildDetails(node)
		sqlStmt = SQLStatement{
			Type:    "GroupBy",
			Details: selectDetails,
		}
	case *plan.Sort:
		selectDetails := getChildDetails(node)
		sqlStmt = SQLStatement{
			Type:    "Sort",
			Details: selectDetails,
		}
	case *plan.Limit:
		selectDetails := getChildDetails(node)
		selectDetails.Limit = extractLimit(node)
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

// getChildDetails extracts details from the child node of a plan node.
func getChildDetails(node sql.Node) SelectDetails {
	var selectDetails SelectDetails
	switch child := node.(type) {
	case *plan.Project:
		selectDetails.SelectedExprs = extractSelectedExprs(child)
		childDetails := getChildDetails(child.Child)
		selectDetails.From = childDetails.From
		selectDetails.Where = childDetails.Where
		selectDetails.GroupBy = childDetails.GroupBy
		selectDetails.Having = childDetails.Having
		selectDetails.OrderBy = childDetails.OrderBy
	case *plan.Filter:
		selectDetails.Where = extractWhere(child)
		childDetails := getChildDetails(child.Child)
		selectDetails.SelectedExprs = childDetails.SelectedExprs
		selectDetails.From = childDetails.From
		selectDetails.GroupBy = childDetails.GroupBy
		selectDetails.Having = childDetails.Having
		selectDetails.OrderBy = childDetails.OrderBy
	case *plan.GroupBy:
		selectDetails.GroupBy = extractGroupBy(child)
		selectDetails.Having = extractHaving(child)
		childDetails := getChildDetails(child.Child)
		selectDetails.SelectedExprs = childDetails.SelectedExprs
		selectDetails.From = childDetails.From
		selectDetails.Where = childDetails.Where
		selectDetails.OrderBy = childDetails.OrderBy
	case *plan.Sort:
		selectDetails.OrderBy = extractOrderBy(child)
		childDetails := getChildDetails(child.Child)
		selectDetails.SelectedExprs = childDetails.SelectedExprs
		selectDetails.From = childDetails.From
		selectDetails.Where = childDetails.Where
		selectDetails.GroupBy = childDetails.GroupBy
		selectDetails.Having = childDetails.Having
	case *plan.Limit:
		selectDetails.Limit = extractLimit(child)
		childDetails := getChildDetails(child.Child)
		selectDetails.SelectedExprs = childDetails.SelectedExprs
		selectDetails.From = childDetails.From
		selectDetails.Where = childDetails.Where
		selectDetails.GroupBy = childDetails.GroupBy
		selectDetails.Having = childDetails.Having
		selectDetails.OrderBy = childDetails.OrderBy
	case *plan.ResolvedTable:
		selectDetails.From = child.Name()
	case *plan.TableAlias:
		selectDetails.From = child.Name()
	}
	return selectDetails
}

// extractSelectedExprs extracts the selected expressions from a plan.Project node.
func extractSelectedExprs(node sql.Node) []string {
	var exprs []string
	if project, ok := node.(*plan.Project); ok {
		for _, expr := range project.Projections {
			exprs = append(exprs, expr.String())
		}
	}
	return exprs
}

// extractFrom extracts the FROM clause from a plan.Project node.
func extractFrom(node sql.Node) string {
	switch n := node.(type) {
	case *plan.Project:
		return extractFrom(n.Child)
	case *plan.Filter:
		return extractFrom(n.Child)
	case *plan.GroupBy:
		return extractFrom(n.Child)
	case *plan.Sort:
		return extractFrom(n.Child)
	case *plan.Limit:
		return extractFrom(n.Child)
	case *plan.ResolvedTable:
		return n.Name()
	case *plan.TableAlias:
		return n.Name()
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

// extractHaving extracts the HAVING clause from a plan.GroupBy node.
func extractHaving(node sql.Node) string {
	if groupBy, ok := node.(*plan.GroupBy); ok {
		if filter, ok := groupBy.Child.(*plan.Filter); ok {
			return filter.Expression.String()
		}
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
