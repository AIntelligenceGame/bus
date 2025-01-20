package main

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
)

// 代码实测没问题，但是依赖1.19
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

	// 输出语法树和语义信息
	fmt.Println("AST:", stmt.String())
}
