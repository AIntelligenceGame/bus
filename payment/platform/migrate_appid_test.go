package platform

import (
	"path/filepath"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/glebarez/sqlite"
)

// TestRebuildOrdersKeepsAppID 重建订单表不能把 app_id 弄丢。
//
// 场景:老库的订单表主键还是旧的(只有 out_trade_no),但 app_id 列
// 已经加上了(多产品维度上线过一次)。这时启动迁移会重建订单表 ——
// 重建要按**列清单**来,列清单必须跟着源表走。
//
// 之前是写死的固定列清单,重建完 app_id 直接消失:
// 商户建好的产品与订单的对应关系全归零,而报表还不报错。
func TestRebuildOrdersKeepsAppID(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	// 老表:主键只有 out_trade_no(全局唯一),但已经有 app_id 列。
	if err := db.Exec(`CREATE TABLE pay_orders (
		out_trade_no   VARCHAR(64) NOT NULL,
		user_id       VARCHAR(64) NOT NULL,
		channel       VARCHAR(32) NOT NULL,
		trade_type    VARCHAR(32) NOT NULL,
		subject       VARCHAR(255) NOT NULL,
		amount        BIGINT NOT NULL,
		amount_yuan   VARCHAR(32) NOT NULL,
		order_status  VARCHAR(32) NOT NULL,
		transaction_id VARCHAR(128) NOT NULL,
		qrcode        TEXT NOT NULL,
		notify_raw    TEXT NOT NULL,
		app_id        VARCHAR(64),
		created_at    VARCHAR(40) NOT NULL,
		updated_at    VARCHAR(40) NOT NULL,
		PRIMARY KEY (out_trade_no)
	)`).Error; err != nil {
		t.Fatalf("建老表失败: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO pay_orders (out_trade_no,user_id,channel,trade_type,subject,amount,amount_yuan,"+
			"order_status,transaction_id,qrcode,notify_raw,app_id,created_at,updated_at) "+
			"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		"MIG-1", "u1", "wechat", "native", "老订单", 100, "1.00",
		string(payment.TradeStatusSuccess), "t1", "", "", "app_keep",
		"2026-11-01T00:00:00.000000000Z", "2026-11-01T00:00:00.000000000Z").Error; err != nil {
		t.Fatalf("插老行失败: %v", err)
	}

	store := &Store{db: db}
	if err := store.migrateOrderPrimaryKey(); err != nil {
		t.Fatalf("迁移主键失败: %v", err)
	}

	// 主键应该已经变成 (user_id, out_trade_no)。
	pk, err := store.orderPrimaryKeyColumns()
	if err != nil {
		t.Fatalf("检测主键失败: %v", err)
	}
	if len(pk) != 2 || !containsColumn(pk, "user_id") || !containsColumn(pk, "out_trade_no") {
		t.Fatalf("主键没迁过来: %v", pk)
	}

	// app_id 必须还在,而且值不能变。
	if !db.Migrator().HasColumn(ordersTable, "app_id") {
		t.Fatal("重建后 app_id 列没了")
	}
	var rows []struct {
		OutTradeNo string
		AppID      *string
	}
	if err := db.Raw("SELECT out_trade_no, app_id FROM pay_orders").Scan(&rows).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("订单数不对: %d", len(rows))
	}
	if rows[0].AppID == nil || *rows[0].AppID != "app_keep" {
		t.Fatalf("app_id 丢了: %+v", rows[0])
	}
}

// TestRebuildOrdersWithoutAppIDColumn 老库没有 app_id 列时重建也不能出错。
//
// 这是绝大多数老库的样子(AutoMigrate 还没跑到加列那一步)。
// 重建出来的表没有 app_id 列,后面 AutoMigrate 会补上 ——
// 补出来的列是 NULL,老订单就是「未分类」。
func TestRebuildOrdersWithoutAppIDColumn(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	if err := db.Exec(`CREATE TABLE pay_orders (
		out_trade_no   VARCHAR(64) NOT NULL,
		user_id       VARCHAR(64) NOT NULL,
		channel       VARCHAR(32) NOT NULL,
		trade_type    VARCHAR(32) NOT NULL,
		subject       VARCHAR(255) NOT NULL,
		amount        BIGINT NOT NULL,
		amount_yuan   VARCHAR(32) NOT NULL,
		order_status  VARCHAR(32) NOT NULL,
		transaction_id VARCHAR(128) NOT NULL,
		qrcode        TEXT NOT NULL,
		notify_raw    TEXT NOT NULL,
		created_at    VARCHAR(40) NOT NULL,
		updated_at    VARCHAR(40) NOT NULL,
		PRIMARY KEY (out_trade_no)
	)`).Error; err != nil {
		t.Fatalf("建老表失败: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO pay_orders (out_trade_no,user_id,channel,trade_type,subject,amount,amount_yuan,"+
			"order_status,transaction_id,qrcode,notify_raw,created_at,updated_at) "+
			"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
		"MIG-2", "u1", "wechat", "native", "老订单", 100, "1.00",
		string(payment.TradeStatusSuccess), "t1", "", "",
		"2026-11-01T00:00:00.000000000Z", "2026-11-01T00:00:00.000000000Z").Error; err != nil {
		t.Fatalf("插老行失败: %v", err)
	}

	store := &Store{db: db}
	if err := store.migrateOrderPrimaryKey(); err != nil {
		t.Fatalf("迁移主键失败: %v", err)
	}
	var n int64
	if err := db.Model(&OrderModel{}).Count(&n).Error; err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if n != 1 {
		t.Fatalf("订单数不对: %d", n)
	}
}
