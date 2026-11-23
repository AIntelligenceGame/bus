package platform

import (
	"path/filepath"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// legacyOrdersTable 老版 pay_orders 的建表语句(没有 channel_created 列)。
//
// 用来模拟「从老版本升级上来」的库:AutoMigrate 会给它加上 channel_created 列,
// 但**不会给存量行填默认值** —— 那些行的这一列是 NULL。
const legacyOrdersTable = `CREATE TABLE pay_orders (
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
)`

// newLegacyDB 造一个「老库」:只有旧结构的 pay_orders,没有其它表。
//
// 只建表,**不**调 NewStore —— 存量行必须在 AutoMigrate 加列**之前**写进去,
// 才模拟得出「升级到老版本时库里已有的那些行」。
func newLegacyDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	if err := db.Exec(legacyOrdersTable).Error; err != nil {
		t.Fatalf("建老表失败: %v", err)
	}
	return db
}

// insertLegacyOrder 往老库里写一行存量订单。
func insertLegacyOrder(t *testing.T, db *gorm.DB, tradeNo, userID string) {
	t.Helper()
	if err := db.Exec(`INSERT INTO pay_orders (
		out_trade_no, user_id, channel, trade_type, subject, amount, amount_yuan,
		order_status, transaction_id, qrcode, notify_raw, created_at, updated_at
	) VALUES (
		?, ?, 'wechat', 'native', '老订单', 100, '1.00',
		'SUCCESS', '', '', '',
		'2026-01-01T00:00:00.000000000Z', '2026-01-01T00:00:00.000000000Z')`,
		tradeNo, userID).Error; err != nil {
		t.Fatalf("写老订单失败: %v", err)
	}
}

// upgradeToNewStore 在老库上跑一遍升级(AutoMigrate + 迁移),返回新的 Store。
func upgradeToNewStore(t *testing.T, db *gorm.DB) *Store {
	t.Helper()
	box, err := NewSecretBox("test-master-key")
	if err != nil {
		t.Fatalf("NewSecretBox: %v", err)
	}
	store, err := NewStore(db, box)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store
}

// TestMigrateChannelCreatedMarksLegacyRows 老库里的存量订单必须被标成
// 「渠道侧已创建」。
//
// 老流程是「先调渠道、成功后落库」:库里有一行,渠道那边就一定有单。
// 不标的话关单时不会去渠道关单,渠道那边的单一直挂着,用户还能继续付钱。
//
// 坑在判据上:AutoMigrate 加列**不会给存量行填默认值**,老行拿到的是
// NULL 而不是 0 —— 用 `channel_created = 0` 去匹配,一行都匹配不到。
func TestMigrateChannelCreatedMarksLegacyRows(t *testing.T) {
	db := newLegacyDB(t)
	// 存量行必须在升级**之前**写进去:这才是「老库里本来就有的订单」。
	insertLegacyOrder(t, db, "T1", "U1")

	// 升级(AutoMigrate 加列 + 迁移)。
	store := upgradeToNewStore(t, db)

	rec, err := store.OrderByTradeNo("U1", "T1")
	if err != nil {
		t.Fatalf("读订单失败: %v", err)
	}
	if rec == nil {
		t.Fatal("订单读不到")
	}
	if !rec.ChannelCreated {
		t.Fatal("老订单没被标记成 channel_created=1:" +
			"关单时不会去渠道关单,渠道那边的单会一直挂着")
	}
}

// TestMigrateChannelCreatedKeepsSemiFinished 半成品订单(渠道侧还没建单)
// 在平台重启后**不能**被误标成「渠道侧已创建」。
//
// 这是上一条的反面:判据如果用 `channel_created = 0`,就会把新流程的半成品
// 订单一起翻掉 —— 商户再拿同一个商户单号重试会被判成「单号已存在」(409),
// 关单也会去渠道关一个根本不存在的单。
//
// 半成品的这一列是**显式**写的 0(见 AddOrder),不是 NULL ——
// 所以判据必须是 IS NULL。
func TestMigrateChannelCreatedKeepsSemiFinished(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")

	// 半成品:平台先落了单,调渠道失败(或进程正好崩在中间)。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "HALF-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "半成品",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     "PENDING",
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}

	// 模拟平台重启:同一个库再开一次 Store(启动时会跑一遍迁移)。
	store2 := upgradeToNewStore(t, store.db)

	rec, err := store2.OrderByTradeNo(user.UserID, "HALF-1")
	if err != nil {
		t.Fatalf("读订单失败: %v", err)
	}
	if rec == nil {
		t.Fatal("订单读不到")
	}
	if rec.ChannelCreated {
		t.Fatal("半成品订单在重启后被误标成 channel_created=1:" +
			"商户用同一个商户单号重试会被判成「已存在」,关单也会去渠道关一个不存在的单")
	}
}

// TestMigrateChannelCreatedIdempotent 迁移可以反复跑,结果必须一样。
//
// 判据用 NULL 天然幂等:跑完之后没有 NULL 行了,再跑就是空操作。
func TestMigrateChannelCreatedIdempotent(t *testing.T) {
	db := newLegacyDB(t)
	insertLegacyOrder(t, db, "T1", "U1")
	store := upgradeToNewStore(t, db)

	for i := 0; i < 3; i++ {
		if err := store.migrateChannelCreatedFlag(); err != nil {
			t.Fatalf("第 %d 次迁移失败: %v", i+1, err)
		}
		rec, err := store.OrderByTradeNo("U1", "T1")
		if err != nil {
			t.Fatalf("读订单失败: %v", err)
		}
		if rec == nil || !rec.ChannelCreated {
			t.Fatalf("第 %d 次迁移后老订单的标记不对: %+v", i+1, rec)
		}
	}
}

// TestRebuildOrdersKeepsChannelCreated 重建订单表不能把 channel_created 弄丢。
//
// 场景:老库的订单表主键还是旧的(只有 out_trade_no),但 channel_created 列
// 已经加上了(新流程上线过一次)。这时启动迁移会重建订单表 ——
// 重建要按**列清单**来,列清单必须跟着源表走。
//
// 之前列清单是写死的固定清单,重建完这一列直接消失:
// AutoMigrate 之后把它加回来时全是 NULL,而 migrateChannelCreatedFlag
// 认的是 NULL —— 于是**所有**订单都被当成「老库的存量行」标成渠道侧已建单,
// 那些本来就只落了库、渠道侧没建单的半成品订单被误标:
// 商户拿同一个商户单号重试会被判成「单号已存在」(409),
// 关单也会去渠道关一个根本不存在的单。
func TestRebuildOrdersKeepsChannelCreated(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	// 老表:主键只有 out_trade_no(全局唯一),但已经有 channel_created 列。
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
		channel_created INTEGER,
		created_at    VARCHAR(40) NOT NULL,
		updated_at    VARCHAR(40) NOT NULL,
		PRIMARY KEY (out_trade_no)
	)`).Error; err != nil {
		t.Fatalf("建老表失败: %v", err)
	}
	// 半成品订单:平台落了库,渠道侧没建单 → channel_created = 0。
	if err := db.Exec(
		"INSERT INTO pay_orders (out_trade_no,user_id,channel,trade_type,subject,amount,amount_yuan,"+
			"order_status,transaction_id,qrcode,notify_raw,channel_created,created_at,updated_at) "+
			"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		"MIG-1", "u1", "wechat", "native", "半成品订单", 100, "1.00",
		string(payment.TradeStatusPending), "", "", "", 0,
		"2026-11-01T00:00:00.000000000Z", "2026-11-01T00:00:00.000000000Z").Error; err != nil {
		t.Fatalf("插老行失败: %v", err)
	}

	store := &Store{db: db}
	if err := store.migrateOrderPrimaryKey(); err != nil {
		t.Fatalf("迁移主键失败: %v", err)
	}

	if !db.Migrator().HasColumn(ordersTable, "channel_created") {
		t.Fatal("重建后 channel_created 列没了")
	}
	var rows []struct {
		OutTradeNo     string
		ChannelCreated *int32 `gorm:"column:channel_created"`
	}
	if err := db.Raw("SELECT out_trade_no, channel_created FROM pay_orders").Scan(&rows).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("订单数不对: %d", len(rows))
	}
	// 值必须原样保住(还是 0),不能变成 NULL ——
	// 变成 NULL 的话迁移会把它当成老订单,误标成「渠道侧已建单」。
	if rows[0].ChannelCreated == nil {
		t.Fatal("重建后 channel_created 的值丢了(变成 NULL):" +
			"半成品订单会被 migrateChannelCreatedFlag 误标成渠道侧已建单")
	}
	if *rows[0].ChannelCreated != 0 {
		t.Fatalf("channel_created 的值不对: %d", *rows[0].ChannelCreated)
	}
}
