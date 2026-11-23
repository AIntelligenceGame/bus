// Package applepay 实现 Apple Pay 支付。
//
// Apple Pay 与商户之间不直接结算,必须通过收单机构(Stripe、Adyen、PayPal 等)
// 完成授权与扣款,因此本包以 gateway.Acquirer 作为底层能力:
//
//  1. 服务端调用 Acquirer.CreateSession 拿到 client_secret / session id;
//  2. 客户端用 Apple Pay 组件(PKPaymentButton / PKPaymentAuthorizationController)
//     拉起收银台,拿到 payment token;
//  3. 客户端把 token 回传服务端,服务端调用 Acquirer.Charge 完成扣款;
//  4. 支付结果通过 Acquirer.ParseWebhook 校验并入账。
//
// 本包同时提供商户证书相关的辅助函数(生成 CSR、解析 EC 私钥),
// 便于向 Apple 申请 Merchant ID 与商户身份证书。
package applepay

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
)

// Config Apple Pay 配置(基于收单网关)。
type Config struct {
	// MerchantID Apple 分配的商户标识(形如 merchant.com.example)。
	MerchantID string
	// DisplayName 收银台展示名称。
	DisplayName string
	// DomainName 商户域名,用于校验支付来源。
	DomainName string
	// Currency 默认币种,默认 CNY。
	Currency string
	// Gateway 收单网关,必填。
	Gateway gateway.Acquirer
}

// Provider Apple Pay 渠道实现。
type Provider struct {
	cfg *Config
	gw  gateway.Acquirer
}

// New 创建 Apple Pay Provider。
func New(cfg *Config) (*Provider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil applepay config", payment.ErrInvalidConfig)
	}
	if cfg.MerchantID == "" {
		return nil, fmt.Errorf("%w: applepay MerchantID is required", payment.ErrInvalidConfig)
	}
	if cfg.Gateway == nil {
		return nil, fmt.Errorf("%w: applepay Gateway is required", payment.ErrInvalidConfig)
	}
	if cfg.Currency == "" {
		cfg.Currency = "CNY"
	}
	return &Provider{cfg: cfg, gw: cfg.Gateway}, nil
}

// Channel 渠道标识。
func (p *Provider) Channel() payment.Channel { return payment.ChannelApplePay }

// SupportedTradeTypes 支持的交易类型。
func (p *Provider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeWallet}
}

// Create 创建钱包支付会话。
//
// 返回的 Session.Token 为收单网关的客户端令牌,AppParams["merchant_id"] 为 Apple 商户号,
// AppParams["payment_method_types"] 供客户端钱包 SDK 使用。
func (p *Provider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	if err := order.Validate(); err != nil {
		return nil, err
	}
	if tt != payment.TradeTypeWallet {
		return nil, fmt.Errorf("%w: applepay does not support %s", payment.ErrUnsupportedTradeType, tt)
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
		PaymentMethodTypes: []string{"card", "apple_pay"},
		Extra:              order.Extra,
	})
	if err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:    payment.ChannelApplePay,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		Token:      session.Token,
		AppParams: map[string]any{
			"merchant_id":          p.cfg.MerchantID,
			"display_name":         p.cfg.DisplayName,
			"domain_name":          p.cfg.DomainName,
			"currency":             currency,
			"gateway":              session.Gateway,
			"publishable_key":      session.PublishableKey,
			"payment_method_types": []string{"card", "apple_pay"},
			"client_secret":        session.Token,
		},
		ClientParams: &payment.ClientParams{
			ApplePay: &payment.ApplePayClientParams{
				MerchantID:     p.cfg.MerchantID,
				DisplayName:    p.cfg.DisplayName,
				DomainName:     p.cfg.DomainName,
				CountryCode:    "CN",
				Currency:       currency,
				Amount:         order.Amount,
				OutTradeNo:     order.OutTradeNo,
				Gateway:        session.Gateway,
				PublishableKey: session.PublishableKey,
				ClientSecret:   session.Token,
			},
		},
		Extra: map[string]any{
			"reference": session.Reference,
		},
	}, nil
}

// Charge 用钱包令牌完成扣款。
//
// 客户端拿到 Apple Pay 的 payment token 后调用本方法。
func (p *Provider) Charge(ctx context.Context, outTradeNo, paymentToken string, amount int64) (*gateway.ChargeResult, error) {
	// 币种必须和**建会话时**用的一致(见 gateway.ResolveCurrency 的说明):
	// 钱包令牌是在那个币种的收银台里产生的,币种对不上收单机构会拒,
	// 甚至扣成另一种币种的钱。
	currency, err := gateway.ResolveCurrency(ctx, p.gw, outTradeNo, p.cfg.Currency)
	if err != nil {
		return nil, err
	}
	return p.gw.Charge(ctx, gateway.ChargeRequest{
		OutTradeNo:    outTradeNo,
		Amount:        amount,
		Currency:      currency,
		PaymentToken:  paymentToken,
		PaymentMethod: "apple_pay",
	})
}

// ChargeWallet 用客户端回传的支付令牌完成扣款(统一结果模型)。
//
// 这是 Apple Pay 移动端闭环的最后一步:客户端用 PassKit 拿到 paymentMethod
// (收单网关形态下即 payment token)后回传服务端,由这里完成实际扣款。
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
		Channel:       payment.ChannelApplePay,
		OutTradeNo:    firstNonEmpty(result.OutTradeNo, outTradeNo),
		TransactionID: result.Reference,
		Amount:        result.Amount,
		Currency:      result.Currency,
		Status:        mapStatus(result),
		Raw:           result.Raw,
	}, nil
}

// Close 关闭订单:钱包场景下不做主动关闭,交由收单机构处理。
func (p *Provider) Close(ctx context.Context, outTradeNo string) error {
	return fmt.Errorf("%w: applepay does not support close", payment.ErrNotImplemented)
}

// Refund 退款。
func (p *Provider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	if req == nil || req.OutTradeNo == "" {
		return nil, fmt.Errorf("applepay: out_trade_no is required")
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
		Channel:      payment.ChannelApplePay,
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
	notify.Channel = payment.ChannelApplePay
	return notify, nil
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

// firstNonEmpty 返回第一个非空字符串。
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// GenerateCSR 生成 Apple 支付所需的商户身份证书签名请求(CSR)。
//
// 生成 EC P-256 私钥并返回 PEM 私钥与 CSR,提交给 Apple 换取商户身份证书。
func GenerateCSR(commonName, organization string, domains []string) (privateKeyPEM []byte, csrPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("applepay: generate key: %w", err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("applepay: marshal key: %w", err)
	}
	privateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{organization},
		},
		DNSNames: domains,
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, template, key)
	if err != nil {
		return nil, nil, fmt.Errorf("applepay: create csr: %w", err)
	}
	csrPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
	return privateKeyPEM, csrPEM, nil
}

// ParseECPrivateKey 解析 Apple 商户身份的 EC 私钥(PEM)。
func ParseECPrivateKey(pemData []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("%w: applepay private key is not valid pem", payment.ErrInvalidConfig)
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse applepay private key: %v", payment.ErrInvalidConfig, err)
	}
	return key, nil
}
