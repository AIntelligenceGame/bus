package googlepay

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
)

// fakeAcquirer 假收单网关:记录收到的请求,按需返回结果。
type fakeAcquirer struct {
	sessions      map[string]string
	amounts       map[string]int64
	queryErr      error
	queryCurrency string

	chargeReq  gateway.ChargeRequest
	sessionReq gateway.SessionRequest
}

func (f *fakeAcquirer) Name() string { return "fake" }

func (f *fakeAcquirer) CreateSession(_ context.Context, req gateway.SessionRequest) (*gateway.Session, error) {
	if f.sessions == nil {
		f.sessions = map[string]string{}
		f.amounts = map[string]int64{}
	}
	f.sessions[req.OutTradeNo] = req.Currency
	f.amounts[req.OutTradeNo] = req.Amount
	f.sessionReq = req
	return &gateway.Session{
		Token:          "cs_test_123",
		Gateway:        "fake",
		Reference:      "pi_test_123",
		PublishableKey: "pk_test_123",
	}, nil
}

func (f *fakeAcquirer) Charge(_ context.Context, req gateway.ChargeRequest) (*gateway.ChargeResult, error) {
	f.chargeReq = req
	return &gateway.ChargeResult{
		Reference:  "pi_charged_1",
		OutTradeNo: req.OutTradeNo,
		Status:     "succeeded",
		Amount:     req.Amount,
		Currency:   req.Currency,
		Paid:       true,
	}, nil
}

func (f *fakeAcquirer) Query(_ context.Context, reference string) (*gateway.ChargeResult, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	currency := f.queryCurrency
	if currency == "" {
		currency = f.sessions[reference]
	}
	if currency == "" {
		return nil, fmt.Errorf("%w: %s", payment.ErrOrderNotFound, reference)
	}
	return &gateway.ChargeResult{
		Reference:  "pi_test_123",
		OutTradeNo: reference,
		Status:     "requires_payment_method",
		Amount:     f.amounts[reference],
		Currency:   currency,
	}, nil
}

func (f *fakeAcquirer) Refund(_ context.Context, req gateway.RefundRequest) (*gateway.RefundResult, error) {
	return &gateway.RefundResult{
		RefundID:  req.RefundID,
		Reference: "re_test_1",
		Status:    "succeeded",
		Amount:    req.Amount,
	}, nil
}

func (f *fakeAcquirer) ParseWebhook(_ context.Context, _ *payment.NotifyRequest) (*payment.Notify, error) {
	return &payment.Notify{OutTradeNo: "OT20260001"}, nil
}

func newProvider(t *testing.T, gw gateway.Acquirer) *Provider {
	t.Helper()
	p, err := New(&Config{
		MerchantID:   "merchant.com.example",
		MerchantName: "示例商户",
		Currency:     "CNY",
		Gateway:      gw,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// TestChargeWalletUsesSessionCurrency 扣款必须用**建会话时**的币种。
//
// 钱包令牌是在那个币种的收银台里产生的,币种对不上,收单机构要么拒,
// 要么扣成另一种币种的钱。之前扣款固定用配置里的默认币种:
// 订单币种是 USD、配置是 CNY 时,收银台显示美元、实际扣人民币。
func TestChargeWalletUsesSessionCurrency(t *testing.T) {
	gw := &fakeAcquirer{}
	p := newProvider(t, gw)
	if _, err := p.Create(context.Background(), &payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "Pixel",
		Amount:     19900,
		Currency:   "USD",
	}, payment.TradeTypeWallet); err != nil {
		t.Fatalf("Create: %v", err)
	}
	res, err := p.ChargeWallet(context.Background(), "OT20260001", "pm_test_1", 19900)
	if err != nil {
		t.Fatalf("ChargeWallet: %v", err)
	}
	if gw.chargeReq.Currency != "USD" {
		t.Fatalf("扣款币种不对: %q(会话用的是 USD)", gw.chargeReq.Currency)
	}
	if res.Currency != "USD" {
		t.Fatalf("结果里的币种不对: %q", res.Currency)
	}
}

// TestChargeWalletFallsBackToConfigCurrency 网关里查不到这笔单时用默认币种。
func TestChargeWalletFallsBackToConfigCurrency(t *testing.T) {
	gw := &fakeAcquirer{}
	p := newProvider(t, gw)
	if _, err := p.ChargeWallet(context.Background(), "OT20260001", "pm_test_1", 19900); err != nil {
		t.Fatalf("ChargeWallet: %v", err)
	}
	if gw.chargeReq.Currency != "CNY" {
		t.Fatalf("兜底币种不对: %q", gw.chargeReq.Currency)
	}
}

// TestChargeWalletQueryErrorAborts 查单出了别的问题时不能盲扣。
func TestChargeWalletQueryErrorAborts(t *testing.T) {
	gw := &fakeAcquirer{queryErr: errors.New("network down")}
	p := newProvider(t, gw)
	if _, err := p.ChargeWallet(context.Background(), "OT20260001", "pm_test_1", 19900); err == nil {
		t.Fatal("查单失败时应该报错,不能盲扣")
	}
	if gw.chargeReq.OutTradeNo != "" {
		t.Fatal("查单失败时不该发出扣款请求")
	}
}

// TestChargeWalletNotFoundUsesFallback 明确「没这笔单」时退回默认币种。
func TestChargeWalletNotFoundUsesFallback(t *testing.T) {
	gw := &fakeAcquirer{queryErr: fmt.Errorf("%w: OT20260001", payment.ErrOrderNotFound)}
	p := newProvider(t, gw)
	if _, err := p.ChargeWallet(context.Background(), "OT20260001", "pm_test_1", 19900); err != nil {
		t.Fatalf("查不到订单不该报错: %v", err)
	}
	if gw.chargeReq.Currency != "CNY" {
		t.Fatalf("兜底币种不对: %q", gw.chargeReq.Currency)
	}
}

// TestCreateUsesOrderCurrency 建会话用的币种是订单上的币种。
func TestCreateUsesOrderCurrency(t *testing.T) {
	gw := &fakeAcquirer{}
	p := newProvider(t, gw)
	session, err := p.Create(context.Background(), &payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "Pixel",
		Amount:     19900,
		Currency:   "USD",
	}, payment.TradeTypeWallet)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if gw.sessionReq.Currency != "USD" {
		t.Fatalf("建会话的币种不对: %q", gw.sessionReq.Currency)
	}
	if session.ClientParams.GooglePay.Currency != "USD" {
		t.Fatalf("给客户端的币种不对: %q", session.ClientParams.GooglePay.Currency)
	}
}

// TestNewValidatesConfig 配置缺项要在启动时就报错。
func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil 配置应该报错")
	}
	if _, err := New(&Config{Gateway: &fakeAcquirer{}}); err == nil {
		t.Fatal("缺 MerchantID 应该报错")
	}
	if _, err := New(&Config{MerchantID: "merchant.com.example"}); err == nil {
		t.Fatal("缺 Gateway 应该报错")
	}
}

// TestCloseNotSupported 钱包渠道不支持主动关单。
func TestCloseNotSupported(t *testing.T) {
	p := newProvider(t, &fakeAcquirer{})
	if err := p.Close(context.Background(), "OT20260001"); !errors.Is(err, payment.ErrNotImplemented) {
		t.Fatalf("错误类型不对: %v", err)
	}
}
