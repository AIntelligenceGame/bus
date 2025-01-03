package main

import (
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/logger"
	"github.com/AIntelligenceGame/bus/parser"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"go.uber.org/zap"
)

type Ml struct {
	*parser.BaseMySqlParserListener
	tableNames map[string]struct{}
}

func (m *Ml) EnterTableName(ctx *parser.TableNameContext) {
	tableName := strings.ToLower(ctx.GetText())
	m.tableNames[tableName] = struct{}{}
}

func (m *Ml) GetTableNames() []string {
	arr := make([]string, 0)
	if m.tableNames != nil {
		for k := range m.tableNames {
			arr = append(arr, k)
		}
	}
	return arr
}

func ParseSQL(sql string) []string {
	input := antlr.NewInputStream(sql)
	lexer := parser.NewMySqlLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewMySqlParser(stream)
	ml := Ml{tableNames: make(map[string]struct{})}
	antlr.ParseTreeWalkerDefault.Walk(&ml, p.Root())
	return ml.GetTableNames()
}

// github.com/akito0107/xsqlparser 支持with 语法
func main() {
	_ = logger.InitLogger(logger.LoggerConfig{})
	sql1 := "SELECT * FROM table1;"
	fmt.Println(ParseSQL(sql1))

	sql2 := "INSERT INTO table2 VALUES (1, 'test');"
	fmt.Println(ParseSQL(sql2))

	sql3 := "UPDATE table3 SET name = 'test' WHERE id = 1;"
	fmt.Println(ParseSQL(sql3))

	sql4 := "DELETE FROM table4 WHERE id = 1;"
	fmt.Println(ParseSQL(sql4))

	sql5 := "TRUNCATE TABLE table5;"
	fmt.Println(ParseSQL(sql5))

	sql6 := "DROP TABLE table6;"
	fmt.Println(ParseSQL(sql6))

	sql7 := "SELECT inv.`customer_code` AS customerCode, inv.`owner_code` AS ownerCode, inv.`bin_code` AS binCode, inv.`sku_code` AS skuCode, inv.`inv_status_code` AS invStatusCode , inv.`location_code` AS locationCode, inv.quota_interval AS quotaInterval, inv.`qty` AS qty , CASE WHEN SUM(occ.`qty`) != '' THEN SUM(occ.`qty`) ELSE 0 END AS occupyQty , CASE WHEN SUM(occ.`qty`) != '' THEN inv.`qty` + SUM(occ.`qty`) ELSE inv.`qty` END AS availableQty FROM ( SELECT t.id AS id, t.`customer_code` AS customer_code, t.`owner_code` AS owner_code, t.`bin_code` AS bin_code, t.`sku_code` AS sku_code , t.`inv_status_code` AS inv_status_code, t.`location_code` AS location_code, t.quota_interval AS quota_interval, t.`qty` AS qty FROM inv_customer_inventory_63 t WHERE t.customer_code = 'PUMAKG' AND t.owner_code IN ('PUMA官方店', 'PUMA_YY0I01') AND t.bin_code IN ('SHWH430', 'SHWH512', 'PUMA_YY_0001', 'xxx') AND t.inv_status_code = '10' AND t.qty >= 1 AND t.saas_tenant_code = 'baozun' ORDER BY t.id ASC LIMIT 0, 20 ) inv LEFT JOIN inv_occupy_63 occ ON occ.`bin_code` = inv.`bin_code` AND occ.`sku_code` = inv.`sku_code` AND occ.`inv_status_code` = inv.`inv_status_code` AND occ.`location_code` = inv.`location_code` AND occ.`customer_code` = inv.`customer_code` AND occ.`owner_code` = inv.`owner_code` AND occ.`quota_interval` = inv.`quota_interval` AND occ.`qty` < 0 AND occ.saas_tenant_code = 'baozun' GROUP BY inv.`sku_code`, inv.`owner_code`, inv.`customer_code`, inv.`bin_code`, inv.`location_code`, inv.`inv_status_code`, inv.quota_interval"
	fmt.Println(ParseSQL(sql7))
	zap.L().Info("aaaaa....")
}
