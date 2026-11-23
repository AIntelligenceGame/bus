package platform

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// 订单表主键迁移。
//
// 旧版 pay_orders 的主键是**全局唯一的 out_trade_no**,有两个问题:
//
//  1. 多租户隔离只能靠代码层保证——任何一处忘了带 user_id 过滤,就能改到别人的订单;
//  2. 商户单号变成全局资源,恶意商户可以预先占用他人单号造成拒绝服务。
//
// 新主键是 (user_id, out_trade_no):商户单号只在**本商户内**唯一,
// 查/改订单一律带 user_id,越权在存储层就被挡住。
//
// GORM 的 AutoMigrate 不会改主键,所以这一步要手写。
// 迁移是幂等的:检测到新主键直接跳过;检测不出来就保持旧主键(功能仍正确),
// 不冒险重建表。

// ordersTable 订单表名。
const ordersTable = "pay_orders"

// migrateAccessTokens 把老库里**明文保存**的访问令牌迁移成带密钥散列。
//
// 迁移完成后清空明文列:数据库被拖走时,攻击者拿不到可直接使用的令牌。
// 迁移是幂等的:已经迁过的行(access_token_hash 非空)直接跳过。
func (s *Store) migrateAccessTokens() error {
	var rows []UserModel
	err := s.db.Where("access_token <> '' AND (access_token_hash IS NULL OR access_token_hash = '')").
		Find(&rows).Error
	if err != nil {
		return fmt.Errorf("platform: 迁移访问令牌失败: %w", err)
	}
	for i := range rows {
		hash := s.hasher.sum(rows[i].AccessToken)
		res := s.db.Model(&UserModel{}).Where("user_id = ?", rows[i].UserID).
			Updates(map[string]any{
				"access_token_hash": hash,
				"access_token":      "",
				"updated_at":        formatTime(time.Now()),
			})
		if res.Error != nil {
			return fmt.Errorf("platform: 迁移访问令牌失败: %w", res.Error)
		}
	}
	return nil
}

// ordersColumns 订单表列顺序(建表与拷贝共用,改一处即可)。
var ordersColumns = []string{
	"out_trade_no", "user_id", "channel", "trade_type", "subject",
	"amount", "amount_yuan", "order_status", "transaction_id",
	"qrcode", "notify_raw", "created_at", "updated_at",
}

// ordersExtraColumns 重建订单表时要**额外**带过去的列。
//
// app_id 是后来加的(多产品维度)。重建表时如果按固定列清单来,
// 源表里已有的 app_id 就会被丢掉 —— 那批订单的应用归属直接归零,
// 商户建好的产品与订单的对应关系全没了。
//
// channel_created 同理:它是「这笔单在渠道侧到底建没建」的标记。
// 重建时丢了这一列,所有订单的标记都会归零(新表里这一列不存在,
// AutoMigrate 之后再加回来时全是 NULL)—— 而 migrateChannelCreatedFlag
// 认的是 NULL,会把**所有**订单都当成「老订单」标成渠道侧已建单,
// 那些本来就只落了库、渠道侧没建单的半成品订单会被误标。
// 后果:商户拿同一个商户单号重试会被判成「单号已存在」(409),
// 关单也会去渠道关一个根本不存在的单。
//
// 所以列清单要**按源表实际的列**来定:源表有哪几列就带哪几列。
var ordersExtraColumns = []string{
	"app_id",
	"channel_created",
}

// ordersExtraColumnDDL 上面那些「额外列」在建表语句里的定义。
//
// app_id 刻意**不加 NOT NULL**:老库里这一列可能是 NULL
// (AutoMigrate 加列不会给存量行填默认值),往 NOT NULL 列里插 NULL 会直接报错,
// 迁移整个卡住。加了 AutoMigrate 建出来的列同样是可空的,定义保持一致。
//
// channel_created 同理:老库里这一列可能是 NULL(而且**必须**保持可空 ——
// migrateChannelCreatedFlag 就是靠 NULL 来认出「老库的存量行」的,
// 一旦建成 NOT NULL,老行会被填成 0,那一列就再也认不出来了)。
var ordersExtraColumnDDL = map[string]string{
	"app_id":          "app_id VARCHAR(64)",
	"channel_created": "channel_created INTEGER",
}

// rebuildOrderColumns 返回重建订单表时要拷贝的列。
//
// 固定列(ordersColumns)总是带上;额外列按源表**实际有没有**来定 ——
// 老库还没有 app_id 时就不带(留空即可,老订单本来就是「未分类」),
// 已经有 app_id 时必须带上,否则数据会被丢掉。
func rebuildOrderColumns(db *gorm.DB) []string {
	cols := append([]string{}, ordersColumns...)
	for _, col := range ordersExtraColumns {
		if db.Migrator().HasColumn(ordersTable, col) {
			cols = append(cols, col)
		}
	}
	return cols
}

// migrateRefundDuplicates 清理同一个 (user_id, out_refund_no) 的重复退款流水。
//
// 为什么需要:商户退款单号是**幂等键**,同一个退款单号只应该对应一笔退款。
// 老版本没有唯一约束,重复提交同一个 out_refund_no 就可能落了两行 ——
// 后果是「已退金额」被算多(白白卡住商户继续退款),
// 而且唯一索引建不上(唯一索引要求这组列不重复)。
//
// 保留 created_at 最早的那条(那才是原始那一笔),其余删除。
// 迁移是幂等的:没有重复行时什么都不做。
func (s *Store) migrateRefundDuplicates() error {
	if !s.db.Migrator().HasTable(&RefundModel{}) {
		return nil // 表还没建,交给 AutoMigrate
	}
	type dupGroup struct {
		UserID      string
		OutRefundNo string
	}
	var groups []dupGroup
	err := s.db.Model(&RefundModel{}).
		Select("user_id, out_refund_no").
		Group("user_id, out_refund_no").
		Having("COUNT(*) > 1").
		Scan(&groups).Error
	if err != nil {
		return fmt.Errorf("platform: 检测重复退款流水失败: %w", err)
	}
	for _, g := range groups {
		var rows []RefundModel
		err := s.db.Where("user_id = ? AND out_refund_no = ?", g.UserID, g.OutRefundNo).
			Order("created_at ASC, refund_id ASC").Find(&rows).Error
		if err != nil {
			return fmt.Errorf("platform: 查询重复退款流水失败: %w", err)
		}
		for _, dup := range rows[1:] {
			if err := s.db.Delete(&RefundModel{}, "refund_id = ?", dup.RefundID).Error; err != nil {
				return fmt.Errorf("platform: 删除重复退款流水失败: %w", err)
			}
		}
	}
	return nil
}

// migrateTimeColumns 把老库里「本地时区 + 变长」的 RFC3339Nano 时间串
// 改写成「固定宽度 UTC」(见 util.go 里的 timeLayout)。
//
// 为什么必须改:时间列是字符串,查询靠**逐字符比较**。
// 老数据长这样:"2026-11-20T09:12:00.123+08:00"(带偏移、宽度还不固定),
// 新写入的长这样:"2026-11-20T01:12:00.123000000Z"。
// 两种混在一张表里,字符串比较比的是「墙上时间」而不是「时刻」,
// 查出来的时间范围能差 8 小时 —— 而且不报错,只是数字不对。
//
// 迁移是幂等的:已经是新格式的行不动。
func (s *Store) migrateTimeColumns() error {
	tables := []struct {
		table  string
		column string
	}{
		{"pay_orders", "created_at"},
		{"pay_orders", "updated_at"},
		{"pay_refunds", "created_at"},
		{"pay_refunds", "updated_at"},
		{"pay_users", "created_at"},
		{"pay_users", "updated_at"},
		{"pay_users", "token_expires_at"},
		{"pay_users", "locked_until"},
		{"pay_api_keys", "created_at"},
		{"pay_api_keys", "expires_at"},
		{"pay_api_keys", "last_used_at"},
		{"pay_mfa_codes", "created_at"},
		{"pay_mfa_codes", "expires_at"},
		{"pay_audit_logs", "created_at"},
		{"pay_verify_tokens", "created_at"},
		{"pay_verify_tokens", "expires_at"},
		{"pay_verify_tokens", "used_at"},
		{"pay_user_channels", "updated_at"},
		{"pay_mfa_channels", "updated_at"},
	}
	for _, item := range tables {
		if !s.db.Migrator().HasTable(item.table) {
			continue
		}
		if !s.db.Migrator().HasColumn(item.table, item.column) {
			continue
		}
		if err := s.migrateTimeColumn(item.table, item.column); err != nil {
			return err
		}
	}
	return nil
}

// migrateTimeColumn 改写一张表一个时间列里的旧格式值。
//
// 做法:把这一列的**不同取值**取出来(时间戳大多互不相同,但仍去重),
// 逐个转换成新格式,再按「旧值 = 新值」做一次批量 UPDATE。
// 这样不需要知道表的主键是什么(各表主键不一样)。
func (s *Store) migrateTimeColumn(table, column string) error {
	var values []string
	err := s.db.Table(table).
		Distinct(column).
		Where(column+" <> ''").
		Pluck(column, &values).Error
	if err != nil {
		return fmt.Errorf("platform: 迁移 %s.%s 时间格式失败: %w", table, column, err)
	}
	for _, raw := range values {
		if _, perr := time.Parse(timeLayout, raw); perr == nil {
			continue // 已经是新格式
		}
		t, perr := parseTime(raw)
		if perr != nil {
			continue // 认不出来的原样留着(不影响其它行)
		}
		newValue := formatTime(t)
		if newValue == raw {
			continue
		}
		if err := s.db.Table(table).
			Where(column+" = ?", raw).
			Update(column, newValue).Error; err != nil {
			return fmt.Errorf("platform: 迁移 %s.%s 时间格式失败: %w", table, column, err)
		}
	}
	return nil
}

// migrateOrderPrimaryKey 把订单表主键从 (out_trade_no) 迁到 (user_id, out_trade_no)。
func (s *Store) migrateOrderPrimaryKey() error {
	pk, err := s.orderPrimaryKeyColumns()
	if err != nil {
		return fmt.Errorf("platform: 检测订单表主键失败: %w", err)
	}
	if len(pk) == 0 {
		return nil // 表还不存在,交给 AutoMigrate 按新主键建表
	}
	if len(pk) == 2 && containsColumn(pk, "user_id") && containsColumn(pk, "out_trade_no") {
		return nil // 已经是新主键
	}
	if s.dialect() == DialectMySQL {
		return s.rebuildOrdersMySQL()
	}
	return s.rebuildOrdersSQLite()
}

// orderPrimaryKeyColumns 返回订单表主键列。
func (s *Store) orderPrimaryKeyColumns() ([]string, error) {
	if s.dialect() == DialectMySQL {
		var rows []struct {
			ColumnName string `gorm:"column:COLUMN_NAME"`
		}
		err := s.db.Raw(`SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY ORDINAL_POSITION`, ordersTable).Scan(&rows).Error
		if err != nil {
			return nil, err
		}
		cols := make([]string, 0, len(rows))
		for _, row := range rows {
			cols = append(cols, row.ColumnName)
		}
		return cols, nil
	}

	// SQLite:PRAGMA table_info 返回 (cid, name, type, notnull, dflt_value, pk),
	// pk 字段是主键中的序号(1..n),非主键列为 0。
	var rows []struct {
		CID      int            `gorm:"column:cid"`
		Name     string         `gorm:"column:name"`
		NotNull  int            `gorm:"column:notnull"`
		DefaultV sql.NullString `gorm:"column:dflt_value"`
		PK       int            `gorm:"column:pk"`
	}
	err := s.db.Raw(`PRAGMA table_info(` + ordersTable + `)`).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	var cols []string
	for _, row := range rows {
		if row.PK > 0 {
			cols = append(cols, row.Name)
		}
	}
	return cols, nil
}

// rebuildOrdersSQLite 在一个事务里重建订单表(SQLite 支持事务化 DDL)。
func (s *Store) rebuildOrdersSQLite() error {
	err := s.db.Transaction(func(tx *gorm.DB) error {
		return rebuildOrders(tx)
	})
	if err != nil {
		return err
	}
	return nil
}

// rebuildOrdersMySQL 逐步重建订单表(MySQL 的 DDL 不能放进事务)。
//
// 顺序:建新表 → 拷数据 → 核对行数 → 删旧表 → 改名。
// 核对行数放在删表之前,数量对不上就直接放弃,不动旧表。
func (s *Store) rebuildOrdersMySQL() error {
	before, err := countOrders(s.db, ordersTable)
	if err != nil {
		return err
	}
	if err := s.db.Exec(`DROP TABLE IF EXISTS pay_orders_new`).Error; err != nil {
		return fmt.Errorf("platform: 清理迁移临时表失败: %w", err)
	}
	cols := rebuildOrderColumns(s.db)
	if err := s.db.Exec(createOrdersTableSQL("pay_orders_new", cols)).Error; err != nil {
		return fmt.Errorf("platform: 创建迁移临时表失败: %w", err)
	}
	if err := s.db.Exec(copyOrdersSQL("pay_orders_new", cols)).Error; err != nil {
		return fmt.Errorf("platform: 拷贝订单数据失败: %w", err)
	}
	after, err := countOrders(s.db, "pay_orders_new")
	if err != nil {
		return err
	}
	if after != before {
		return fmt.Errorf("platform: 迁移中止:订单数不一致(原 %d,新 %d),旧表未改动", before, after)
	}
	if err := s.db.Exec(`DROP TABLE ` + ordersTable).Error; err != nil {
		return fmt.Errorf("platform: 删除旧订单表失败: %w", err)
	}
	if err := s.db.Exec(`ALTER TABLE pay_orders_new RENAME TO ` + ordersTable).Error; err != nil {
		return fmt.Errorf("platform: 重命名订单表失败: %w", err)
	}
	return nil
}

// rebuildOrders 在给定连接上完成重建(供 SQLite 事务复用)。
func rebuildOrders(tx *gorm.DB) error {
	if err := tx.Exec(`DROP TABLE IF EXISTS pay_orders_new`).Error; err != nil {
		return fmt.Errorf("platform: 清理迁移临时表失败: %w", err)
	}
	cols := rebuildOrderColumns(tx)
	if err := tx.Exec(createOrdersTableSQL("pay_orders_new", cols)).Error; err != nil {
		return fmt.Errorf("platform: 创建迁移临时表失败: %w", err)
	}
	if err := tx.Exec(copyOrdersSQL("pay_orders_new", cols)).Error; err != nil {
		return fmt.Errorf("platform: 拷贝订单数据失败: %w", err)
	}
	before, err := countOrders(tx, ordersTable)
	if err != nil {
		return err
	}
	after, err := countOrders(tx, "pay_orders_new")
	if err != nil {
		return err
	}
	if after != before {
		return fmt.Errorf("platform: 迁移中止:订单数不一致(原 %d,新 %d),旧表未改动", before, after)
	}
	if err := tx.Exec(`DROP TABLE ` + ordersTable).Error; err != nil {
		return fmt.Errorf("platform: 删除旧订单表失败: %w", err)
	}
	if err := tx.Exec(`ALTER TABLE pay_orders_new RENAME TO ` + ordersTable).Error; err != nil {
		return fmt.Errorf("platform: 重命名订单表失败: %w", err)
	}
	return nil
}

// countOrders 统计订单行数。
func countOrders(db *gorm.DB, table string) (int64, error) {
	var n int64
	row := db.Raw(`SELECT COUNT(*) FROM ` + table).Row()
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("platform: 统计 %s 行数失败: %w", table, err)
	}
	return n, nil
}

// createOrdersTableSQL 生成指定表名的建表语句(主键为 user_id + out_trade_no)。
//
// cols 是要拷贝的列清单(见 rebuildOrderColumns)。
func createOrdersTableSQL(name string, cols []string) string {
	defs := []string{
		"out_trade_no   VARCHAR(64) NOT NULL",
		"user_id       VARCHAR(64) NOT NULL",
		"channel       VARCHAR(32) NOT NULL",
		"trade_type    VARCHAR(32) NOT NULL",
		"subject       VARCHAR(255) NOT NULL",
		"amount        BIGINT NOT NULL",
		"amount_yuan   VARCHAR(32) NOT NULL",
		"order_status  VARCHAR(32) NOT NULL",
		"transaction_id VARCHAR(128) NOT NULL",
		"qrcode        TEXT NOT NULL",
		"notify_raw    TEXT NOT NULL",
		"created_at    VARCHAR(40) NOT NULL",
		"updated_at    VARCHAR(40) NOT NULL",
	}
	// 额外列(app_id 等)按列清单补上定义。
	for _, col := range cols {
		if ddl, ok := ordersExtraColumnDDL[col]; ok {
			defs = append(defs, ddl)
		}
	}
	return `CREATE TABLE ` + name + ` (` + strings.Join(defs, ",\n\t\t") + `,
		PRIMARY KEY (user_id, out_trade_no)
	)`
}

// copyOrdersSQL 生成把旧表数据拷进新表的语句。
func copyOrdersSQL(target string, cols []string) string {
	list := strings.Join(cols, ", ")
	return `INSERT INTO ` + target + ` (` + list + `) SELECT ` + list + ` FROM ` + ordersTable
}

// containsColumn 判断列名列表里是否含有指定列(忽略大小写)。
func containsColumn(list []string, want string) bool {
	for _, item := range list {
		if strings.EqualFold(item, want) {
			return true
		}
	}
	return false
}
