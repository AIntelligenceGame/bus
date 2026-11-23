package platform

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/AIntelligenceGame/bus/payment"
)

// newTestStore 建一个临时 SQLite 数据库的 Store(纯 Go 驱动,不需要 cgo)。
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
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

// newTestUser 建一个测试用户。
//
// 新账户默认是「订单必须绑定应用」(见 CreateUser)。这里**显式关掉**,
// 好让各个测试专心测自己关心的事(下单、查单、退款……),
// 而不是每写一个用例都得先建个应用。
// 「强制绑定应用」本身的行为由 require_app_binding_test.go 专门覆盖。
func newTestUser(t *testing.T, store *Store, name string) *User {
	t.Helper()
	user, err := store.CreateUser(name, "hash")
	if err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	if err := store.SetRequireAppBinding(user.UserID, false); err != nil {
		t.Fatalf("关闭强制绑定失败: %v", err)
	}
	// 重新读一遍:CreateUser 返回的结构体还是「强制」那一份,
	// 直接返回它的话,调用方拿到的状态和库里对不上。
	fresh, err := store.UserByID(user.UserID)
	if err != nil {
		t.Fatalf("读用户失败: %v", err)
	}
	if fresh == nil {
		t.Fatal("用户读不到")
	}
	// 令牌明文只在创建时返回,重新读的话要补回去。
	fresh.AccessToken = user.AccessToken
	return fresh
}

// TestOrdersAreIsolatedPerMerchant 核心越权回归:商户之间互相看不到、也改不了对方的订单。
func TestOrdersAreIsolatedPerMerchant(t *testing.T) {
	store := newTestStore(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")

	// 商户 A 的订单。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "ALICE-ORDER",
		UserID:     alice.UserID,
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "测试订单",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     "pending",
	}); err != nil {
		t.Fatalf("商户 A 下单失败: %v", err)
	}
	// 商户 B 用**同一个商户单号**下单:新主键下允许(单号只在商户内唯一)。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "ALICE-ORDER",
		UserID:     bob.UserID,
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "测试订单",
		Amount:     29900,
		AmountYuan: "299.00",
		Status:     "pending",
	}); err != nil {
		t.Fatalf("商户 B 下单失败: %v", err)
	}

	// 各自只能看到自己那一条。
	for _, u := range []*User{alice, bob} {
		orders, err := store.Orders(u.UserID)
		if err != nil {
			t.Fatalf("查订单失败: %v", err)
		}
		if len(orders) != 1 {
			t.Fatalf("商户 %s 应只看到 1 条订单,实际 %d 条", u.UserName, len(orders))
		}
		if orders[0].UserID != u.UserID {
			t.Fatalf("商户 %s 看到了别人的订单: %+v", u.UserName, orders[0])
		}
	}

	// 商户 B 改商户 A 的单号(A 没有 B 的单号 → 改不到)。
	ok, err := store.UpdateOrder(bob.UserID, "ALICE-ONLY", func(record *OrderRecord) {
		record.Status = "hacked"
	})
	if err != nil {
		t.Fatalf("UpdateOrder: %v", err)
	}
	if ok {
		t.Fatalf("商户 B 竟然改到了不属于自己的订单")
	}

	// 商户 B 改自己那条同名订单 → 只能改自己的,金额/状态仍是 B 的。
	ok, err = store.UpdateOrder(bob.UserID, "ALICE-ORDER", func(record *OrderRecord) {
		record.Status = "paid"
	})
	if err != nil {
		t.Fatalf("UpdateOrder: %v", err)
	}
	if !ok {
		t.Fatalf("商户 B 改自己的订单竟然失败了")
	}

	// 商户 A 的订单状态没被动过。
	record, err := store.OrderByTradeNo(alice.UserID, "ALICE-ORDER")
	if err != nil {
		t.Fatalf("OrderByTradeNo: %v", err)
	}
	if record == nil || record.Status != "pending" {
		t.Fatalf("商户 A 的订单被改了: %+v", record)
	}

	// 商户 B 查订单时按商户过滤:单号虽相同,金额是 B 自己的。
	own, err := store.OrderByTradeNo(bob.UserID, "ALICE-ORDER")
	if err != nil {
		t.Fatalf("OrderByTradeNo: %v", err)
	}
	if own == nil || own.Amount != 29900 {
		t.Fatalf("商户 B 查到的不是自己的订单: %+v", own)
	}
}

// TestDuplicateOrderNoSameMerchantRejected 同一商户内单号不能重复。
func TestDuplicateOrderNoSameMerchantRejected(t *testing.T) {
	store := newTestStore(t)
	alice := newTestUser(t, store, "商户A")
	record := OrderRecord{
		OutTradeNo: "DUP-1",
		UserID:     alice.UserID,
		Channel:    "wechat",
		Amount:     100,
		AmountYuan: "1.00",
		Status:     "pending",
	}
	if err := store.AddOrder(record); err != nil {
		t.Fatalf("首次下单失败: %v", err)
	}
	if err := store.AddOrder(record); err == nil {
		t.Fatalf("同一商户重复单号竟然被接受了")
	}
}

// refundRecorder 记录退款调用的假渠道。
type refundRecorder struct {
	refunded []string
}

func (f *refundRecorder) Channel() payment.Channel { return payment.ChannelWechat }

func (f *refundRecorder) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeNative}
}

func (f *refundRecorder) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	return &payment.PaymentSession{Channel: payment.ChannelWechat}, nil
}

func (f *refundRecorder) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	return &payment.TradeResult{Channel: payment.ChannelWechat}, nil
}

func (f *refundRecorder) Close(ctx context.Context, outTradeNo string) error { return nil }

func (f *refundRecorder) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	f.refunded = append(f.refunded, req.OutTradeNo)
	return &payment.RefundResult{OutTradeNo: req.OutTradeNo}, nil
}

func (f *refundRecorder) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	return &payment.Notify{Channel: payment.ChannelWechat}, nil
}

// TestRefundCannotTouchOtherMerchantOrder 接口层回归:拿别人的单号退款必须被拒。
func TestRefundCannotTouchOtherMerchantOrder(t *testing.T) {
	store := newTestStore(t)
	alice := newTestUser(t, store, "商户A")
	bob := newTestUser(t, store, "商户B")

	// 商户 A 的订单。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "ALICE-ORDER",
		UserID:     alice.UserID,
		Channel:    "wechat",
		Amount:     19900,
		AmountYuan: "199.00",
		Status:     "pending",
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}

	recorder := &refundRecorder{}
	p := &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
		},
		store: store,
		providers: map[string]map[payment.Channel]payment.Provider{
			bob.UserID: {payment.ChannelWechat: recorder},
		},
	}

	// 商户 B 拿到自己的敏感操作票据(二次验证)。
	verifyToken, err := store.CreateVerifyToken(bob.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	// 拿商户 A 的单号去退款 → 必须 404,且渠道一次都不能被调用。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/refund",
		strings.NewReader(`{"out_trade_no":"ALICE-ORDER","refund_amount":19900}`))
	req.Header.Set("X-User-Token", bob.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleRefund(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("越权退款应返回 404,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(recorder.refunded) != 0 {
		t.Fatalf("越权请求竟然调用了渠道退款: %v", recorder.refunded)
	}

	// 商户 A 的订单状态没变。
	record, err := store.OrderByTradeNo(alice.UserID, "ALICE-ORDER")
	if err != nil {
		t.Fatalf("OrderByTradeNo: %v", err)
	}
	if record == nil || record.Status != "pending" {
		t.Fatalf("商户 A 的订单状态被改了: %+v", record)
	}
}

// TestOrderPrimaryKeyMigration 老库(主键是 out_trade_no)迁移后数据不丢、隔离生效。
func TestOrderPrimaryKeyMigration(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "old.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	// 手工建一张「旧版」订单表:主键是全局唯一的 out_trade_no。
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
		t.Fatalf("建旧表失败: %v", err)
	}
	if err := db.Exec(`INSERT INTO pay_orders
		(out_trade_no, user_id, channel, trade_type, subject, amount, amount_yuan, order_status,
		 transaction_id, qrcode, notify_raw, created_at, updated_at)
		VALUES ('OLD-1','u1','wechat','native','旧订单',100,'1.00','paid','','','','','')`).Error; err != nil {
		t.Fatalf("写旧数据失败: %v", err)
	}

	box, err := NewSecretBox("test-master-key")
	if err != nil {
		t.Fatalf("NewSecretBox: %v", err)
	}
	store, err := NewStore(db, box)
	if err != nil {
		t.Fatalf("NewStore(应触发主键迁移): %v", err)
	}

	// 老数据还在。
	orders, err := store.Orders("u1")
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if len(orders) != 1 || orders[0].OutTradeNo != "OLD-1" {
		t.Fatalf("迁移后老数据丢了: %+v", orders)
	}
	// 别的商户看不到它。
	if other, err := store.OrderByTradeNo("u2", "OLD-1"); err != nil {
		t.Fatalf("OrderByTradeNo: %v", err)
	} else if other != nil {
		t.Fatalf("迁移后仍然能跨商户读到订单: %+v", other)
	}
	// 迁移是幂等的:再初始化一次不报错、数据不变。
	if _, err := NewStore(db, box); err != nil {
		t.Fatalf("重复初始化失败: %v", err)
	}
	orders, err = store.Orders("u1")
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if len(orders) != 1 {
		t.Fatalf("重复初始化后订单数变了: %d", len(orders))
	}
}
