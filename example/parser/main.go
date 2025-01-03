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
	sql1 := "SELECT inv.id AS id, inv.version AS version, inv.im_organization AS imOrganization, inv.sku_code AS skuCode, inv.inv_status_code AS invStatusCode , inv.location_code AS locationCode, inv.cw_code AS cwCode, inv.quota_interval AS quotaInterval, inv.create_time AS createTime, inv.last_modify_time AS lastModifyTime , inv.`qty` AS qty, SUM(CASE WHEN occ.current_occupy_qty IS NULL THEN 0 ELSE occ.current_occupy_qty END) AS occupyQty , inv.qty + SUM(CASE WHEN occ.current_occupy_qty IS NULL THEN 0 ELSE occ.current_occupy_qty END) AS availableQty, inv.saas_tenant_code AS saasTenantCode FROM ( SELECT * FROM t_cloud_location_inventory_6 inv1 WHERE inv1.im_organization = 'JackWolfskinGG' AND inv1.saas_tenant_code = 'JACKWOLFSKIN' ORDER BY inv1.id ASC LIMIT 0, 50000 ) inv LEFT JOIN t_cloud_inventory_occupy_6 occ ON inv.im_organization = occ.im_organization AND inv.sku_code = occ.sku_code AND inv.inv_status_code = occ.inv_status_code AND inv.cw_code = occ.cw_code AND inv.location_code = occ.location_code AND inv.quota_interval = occ.quota_interval AND occ.saas_tenant_code = 'JACKWOLFSKIN' GROUP BY inv.im_organization, inv.sku_code, inv.inv_status_code, inv.cw_code, inv.location_code, inv.quota_interval ORDER BY inv.id ASC"
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

	sql7 := "ALTER TABLE table7 ADD COLUMN column1 varchar(255);"
	fmt.Println(ParseSQL(sql7))
	zap.L().Info("aaaaa....")
}
