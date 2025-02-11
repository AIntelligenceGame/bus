package main

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// 代码实测没问题，但是依赖1.19
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

	// 输出语法树和语义信息
	fmt.Println("AST:", stmt.String())

	// 处理解析后的结构体
	switch node := stmt.(type) {
	case *plan.CreateTable:
		fmt.Println("Parsed DDL statement: CreateTable")
		// 输出CreateTable结构体信息
		fmt.Printf("CreateTable: %+v\n", node)
	case *plan.InsertInto:
		fmt.Println("Parsed DML statement: InsertInto")
		// 输出InsertInto结构体信息
		fmt.Printf("InsertInto: %+v\n", node)
	case *plan.Update:
		fmt.Println("Parsed DML statement: Update")
		// 输出Update结构体信息
		fmt.Printf("Update: %+v\n", node)
	case *plan.DeleteFrom:
		fmt.Println("Parsed DML statement: DeleteFrom")
		// 输出DeleteFrom结构体信息
		fmt.Printf("DeleteFrom: %+v\n", node)
	default:
		fmt.Println("Parsed other type of statement")
	}
}
