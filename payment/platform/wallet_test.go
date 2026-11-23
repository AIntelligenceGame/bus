package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// fakeWalletProvider 是一个假的钱包渠道 Provider,记录扣款调用。
type fakeWalletProvider struct {
	chargedOutTradeNo string
	chargedToken      string
	chargedAmount     int64
}

func (f *fakeWalletProvider) Channel() payment.Channel { return payment.ChannelApplePay }

func (f *fakeWalletProvider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeWallet}
}

func (f *fakeWalletProvider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	return &payment.PaymentSession{Channel: payment.ChannelApplePay, TradeType: tt, OutTradeNo: order.OutTradeNo}, nil
}

func (f *fakeWalletProvider) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	return &payment.TradeResult{Channel: payment.ChannelApplePay, OutTradeNo: outTradeNo}, nil
}

func (f *fakeWalletProvider) Close(ctx context.Context, outTradeNo string) error { return nil }

func (f *fakeWalletProvider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	return &payment.RefundResult{Channel: payment.ChannelApplePay, OutTradeNo: req.OutTradeNo}, nil
}

func (f *fakeWalletProvider) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	return &payment.Notify{Channel: payment.ChannelApplePay}, nil
}

// ChargeWallet 实现 payment.WalletProvider。
func (f *fakeWalletProvider) ChargeWallet(ctx context.Context, outTradeNo, paymentToken string, amount int64) (*payment.WalletChargeResult, error) {
	f.chargedOutTradeNo = outTradeNo
	f.chargedToken = paymentToken
	f.chargedAmount = amount
	return &payment.WalletChargeResult{
		OutTradeNo:    outTradeNo,
		TransactionID: "TXN-" + outTradeNo,
		Amount:        amount,
		Currency:      "CNY",
		Status:        payment.TradeStatusSuccess,
		Paid:          true,
	}, nil
}

// TestWalletProviderInterface 校验钱包 Provider 满足 payment.WalletProvider。
func TestWalletProviderInterface(t *testing.T) {
	var p payment.Provider = &fakeWalletProvider{}
	if _, ok := p.(payment.WalletProvider); !ok {
		t.Fatalf("钱包渠道 Provider 应实现 payment.WalletProvider")
	}
}

// TestWalletChargeHandler 校验钱包扣款接口:参数校验与扣款调用。
//
// 必须打真实的 handler:之前这个测试里的 Platform 连 store 都没有,
// 请求里也没有令牌,第一步 currentUser 就 401 了 ——
// 「缺少 payment_token 时应拒绝」那个断言被这个 401 满足了,
// 根本没走到 payment_token 的校验(那在 core.go 里)。
// 后面又绕开 handler 直接调 fake.ChargeWallet,验的是 fake 自己。
func TestWalletChargeHandler(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户戊")
	fake := &fakeWalletProvider{}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelApplePay: fake,
	}
	// 一笔走钱包渠道的订单。
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "WALLET-1",
		UserID:     user.UserID,
		Channel:    string(payment.ChannelApplePay),
		TradeType:  string(payment.TradeTypeWallet),
		Amount:     19900,
		AmountYuan: payment.CentsToYuan(19900),
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("造订单失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	call := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/wallet/charge", strings.NewReader(body))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("X-Verify-Token", verifyToken)
		req.Header.Set("Content-Type", "application/json")
		p.handleWalletCharge(rec, req)
		return rec
	}

	// 缺少 payment_token → 必须被拒,而且理由是 payment_token 缺失。
	rec := call(`{"out_trade_no":"WALLET-1","channel":"applepay","payment_token":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("缺少支付令牌应返回 400,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "payment_token") {
		t.Fatalf("错误信息里应点名 payment_token: %s", rec.Body.String())
	}
	if fake.chargedOutTradeNo != "" {
		t.Fatalf("参数不合法时不该调渠道: %+v", fake)
	}

	// 金额与订单不一致 → 拒绝(防篡改)。
	rec = call(`{"out_trade_no":"WALLET-1","channel":"applepay","payment_token":"tok-1","amount":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("金额不一致应返回 400,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.chargedOutTradeNo != "" {
		t.Fatalf("金额被篡改时不该调渠道: %+v", fake)
	}

	// 正常扣款。
	rec = call(`{"out_trade_no":"WALLET-1","channel":"applepay","payment_token":"tok-1","amount":19900}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("正常扣款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.chargedOutTradeNo != "WALLET-1" || fake.chargedToken != "tok-1" || fake.chargedAmount != 19900 {
		t.Fatalf("扣款参数不对: %+v", fake)
	}
	// 响应里要有 transaction_id。
	if !strings.Contains(rec.Body.String(), "transaction_id") {
		t.Fatalf("扣款结果应包含 transaction_id: %s", rec.Body.String())
	}
	// 订单状态应被推进成成功。
	order, err := store.OrderByTradeNo(user.UserID, "WALLET-1")
	if err != nil || order == nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusSuccess) {
		t.Fatalf("扣款成功后订单状态应为 SUCCESS,实际 %s", order.Status)
	}
}
