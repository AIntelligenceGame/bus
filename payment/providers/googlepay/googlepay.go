// Package googlepay 实现 Google Pay 支付。
//
// Google Pay 与商户之间同样通过收单机构结算(Stripe、Adyen 等),流程与 Apple Pay 一致:
//
//  1. 服务端调用 Acquirer.CreateSession 拿到客户端令牌;
//  2. 客户端用 Google Pay 按钮拉起收银台,拿到支付令牌;
//  3. 服务端调用 Acquirer.Charge 完成扣款;
//  4. 通过 Acquirer.ParseWebhook 校验回调并入账。
package googlepay

import (
	"context"
	"fmt"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
)

// Config Google Pay 配置(基于收单网关)。
type Config struct {
	// MerchantID Google Pay 商户号。
	MerchantID string
	// MerchantName 收银台展示名称。
	MerchantName string
	// Currency 默认币种,默认 CNY。
	Currency string
	// Gateway 收单网关,必填。
	Gateway gateway.Acquirer
}

// Provider Google Pay 渠道实现。
type Provider struct {
	cfg *Config
	gw  gateway.Acquirer
}

// New 创建 Google Pay Provider。
func New(cfg *Config) (*Provider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil googlepay config", payment.ErrInvalidConfig)
	}
	if cfg.MerchantID == "" {
		return nil, fmt.Errorf("%w: googlepay MerchantID is required", payment.ErrInvalidConfig)
	}
	if cfg.Gateway == nil {
		return nil, fmt.Errorf("%w: googlepay Gateway is required", payment.ErrInvalidConfig)
	}
	if cfg.Currency == "" {
		cfg.Currency = "CNY"
	}
	return &Provider{cfg: cfg, gw: cfg.Gateway}, nil
}

// Channel 渠道标识。
func (p *Provider) Channel() payment.Channel { return payment.ChannelGooglePay }

// SupportedTradeTypes 支持的交易类型。
func (p *Provider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeWallet}
}

// Create 创建钱包支付会话。
func (p *Provider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	if err := order.Validate(); err != nil {
		return nil, err
	}
	if tt != payment.TradeTypeWallet {
		return nil, fmt.Errorf("%w: googlepay does not support %s", payment.ErrUnsupportedTradeType, tt)
	}
	currency := order.Currency
	if currency == "" {
		currency = p.cfg.Currency
	}
	session, err := p.gw.CreateSession(ctx, gateway.SessionRequest{
		OutTradeNo:         order.OutTradeNo,
		Amount:             order.Amount,
		Currency:           currency,
		Description:        order.Subject,
		NotifyURL:          order.NotifyURL,
		ReturnURL:          order.ReturnURL,
		PaymentMethodTypes: []string{"card", "google_pay"},
		Extra:              order.Extra,
	})
	if err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:    payment.ChannelGooglePay,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		Token:      session.Token,
		AppParams: map[string]any{
			"merchant_id":          p.cfg.MerchantID,
			"merchant_name":        p.cfg.MerchantName,
			"currency":             currency,
			"gateway":              session.Gateway,
			"publishable_key":      session.PublishableKey,
			"payment_method_types": []string{"card", "google_pay"},
			"client_secret":        session.Token,
		},
		ClientParams: &payment.ClientParams{
			GooglePay: &payment.GooglePayClientParams{
				Gateway:        session.Gateway,
				PublishableKey: session.PublishableKey,
				MerchantName:   p.cfg.MerchantName,
				MerchantID:     p.cfg.MerchantID,
				Currency:       currency,
				Amount:         order.Amount,
				OutTradeNo:     order.OutTradeNo,
				Environment:    "PRODUCTION",
				ClientSecret:   session.Token,
			},
		},
		Extra: map[string]any{
			"reference": session.Reference,
		},
	}, nil
}

// Charge 用钱包令牌完成扣款。
func (p *Provider) Charge(ctx context.Context, outTradeNo, paymentToken string, amount int64) (*gateway.ChargeResult, error) {
	// 币种必须和**建会话时**用的一致(见 gateway.ResolveCurrency 的说明)。
	currency, err := gateway.ResolveCurrency(ctx, p.gw, outTradeNo, p.cfg.Currency)
	if err != nil {
		return nil, err
	}
	return p.gw.Charge(ctx, gateway.ChargeRequest{
		OutTradeNo:    outTradeNo,
		Amount:        amount,
		Currency:      currency,
		PaymentToken:  paymentToken,
		PaymentMethod: "google_pay",
	})
}

// ChargeWallet 用客户端回传的支付令牌完成扣款(统一结果模型)。
//
// 这是 Google Pay 移动端闭环的最后一步:客户端用 PaymentsClient 拿到
// PaymentData 后回传服务端,由这里完成实际扣款。
func (p *Provider) ChargeWallet(ctx context.Context, outTradeNo, paymentToken string, amount int64) (*payment.WalletChargeResult, error) {
	res, err := p.Charge(ctx, outTradeNo, paymentToken, amount)
	if err != nil {
		return nil, err
	}
	return &payment.WalletChargeResult{
		OutTradeNo:    firstNonEmpty(res.OutTradeNo, outTradeNo),
		TransactionID: res.Reference,
		Amount:        res.Amount,
		Currency:      res.Currency,
		Status:        mapStatus(res),
		Paid:          res.Paid,
		Raw:           res.Raw,
	}, nil
}

// Query 查询支付结果。
func (p *Provider) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	result, err := p.gw.Query(ctx, outTradeNo)
	if err != nil {
		return nil, err
	}
	return &payment.TradeResult{
		Channel:       payment.ChannelGooglePay,
		OutTradeNo:    result.OutTradeNo,
		TransactionID: result.Reference,
		Amount:        result.Amount,
		Currency:      result.Currency,
		Status:        mapStatus(result),
		Raw:           result.Raw,
	}, nil
}

// Close 钱包场景不支持主动关闭订单。
func (p *Provider) Close(ctx context.Context, outTradeNo string) error {
	return fmt.Errorf("%w: googlepay does not support close", payment.ErrNotImplemented)
}

// Refund 退款。
func (p *Provider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	if req == nil || req.OutTradeNo == "" {
		return nil, fmt.Errorf("googlepay: out_trade_no is required")
	}
	amount := req.RefundAmount
	if amount <= 0 {
		q, err := p.Query(ctx, req.OutTradeNo)
		if err != nil {
			return nil, err
		}
		amount = q.Amount
	}
	res, err := p.gw.Refund(ctx, gateway.RefundRequest{
		OutTradeNo: req.OutTradeNo,
		RefundID:   req.OutRefundNo,
		Amount:     amount,
		Reason:     req.Reason,
	})
	if err != nil {
		return nil, err
	}
	return &payment.RefundResult{
		Channel:      payment.ChannelGooglePay,
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  req.OutRefundNo,
		RefundID:     res.Reference,
		Status:       res.Status,
		RefundAmount: res.Amount,
	}, nil
}

// ParseNotify 解析收单网关回调。
func (p *Provider) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	notify, err := p.gw.ParseWebhook(ctx, req)
	if err != nil {
		return nil, err
	}
	notify.Channel = payment.ChannelGooglePay
	return notify, nil
}

// firstNonEmpty 返回第一个非空字符串。
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// mapStatus 网关状态映射为统一交易状态。
func mapStatus(result *gateway.ChargeResult) payment.TradeStatus {
	if result.Paid {
		return payment.TradeStatusSuccess
	}
	switch result.Status {
	case "canceled", "cancelled":
		return payment.TradeStatusClosed
	case "failed":
		return payment.TradeStatusFailed
	default:
		return payment.TradeStatusPending
	}
}
