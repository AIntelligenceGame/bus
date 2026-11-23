// Package gateway 定义收单网关抽象。
//
// Apple Pay 与 Google Pay 本身不直接与商户结算,必须通过收单机构
// (Stripe、Adyen、PayPal、Braintree 等)完成授权与扣款。
// 本包把这类收单机构统一成 Acquirer 接口,Apple/Google 渠道再基于它实现。
package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/AIntelligenceGame/bus/payment"
)

// Acquirer 收单网关接口(Stripe / Adyen / PayPal 等)。
type Acquirer interface {
	// Name 网关名称,例如 "stripe"。
	Name() string
	// CreateSession 创建支付会话,返回客户端钱包 SDK 使用的令牌。
	CreateSession(ctx context.Context, req SessionRequest) (*Session, error)
	// Charge 用钱包令牌直接发起扣款(服务端拿到 token 后调用)。
	Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error)
	// Query 查询扣款结果,reference 可以是网关流水号或商户订单号。
	Query(ctx context.Context, reference string) (*ChargeResult, error)
	// Refund 退款。
	Refund(ctx context.Context, req RefundRequest) (*RefundResult, error)
	// ParseWebhook 验签并解析网关回调,返回统一模型。
	ParseWebhook(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error)
}

// SessionRequest 创建支付会话的请求。
type SessionRequest struct {
	// OutTradeNo 商户订单号。
	OutTradeNo string
	// Amount 金额,单位:分。
	Amount int64
	// Currency 币种,默认 CNY。
	Currency string
	// Description 商品描述。
	Description string
	// NotifyURL 异步回调地址。
	NotifyURL string
	// ReturnURL 同步跳转地址。
	ReturnURL string
	// PaymentMethodTypes 允许的支付方式,例如 ["card"] 或 ["apple_pay", "google_pay"]。
	PaymentMethodTypes []string
	// CustomerID 客户标识(可选)。
	CustomerID string
	// Extra 附加参数。
	Extra map[string]string
}

// Session 支付会话。
type Session struct {
	// Token 客户端令牌,例如 Stripe 的 client_secret。
	Token string
	// Gateway 网关名称。
	Gateway string
	// Reference 网关侧流水号。
	Reference string
	// PublishableKey 客户端可发布密钥。
	PublishableKey string
	// Extra 透传给客户端的参数。
	Extra map[string]any
}

// ChargeRequest 直接扣款的请求。
type ChargeRequest struct {
	// OutTradeNo 商户订单号。
	OutTradeNo string
	// Amount 金额,单位:分。
	Amount int64
	// Currency 币种。
	Currency string
	// Description 商品描述。
	Description string
	// PaymentToken 钱包返回的支付令牌(如 Apple Pay payment token)。
	PaymentToken string
	// PaymentMethod 支付方式,例如 "apple_pay" / "google_pay"。
	PaymentMethod string
	// NotifyURL 异步回调地址。
	NotifyURL string
	// Extra 附加参数。
	Extra map[string]string
}

// ChargeResult 扣款结果。
type ChargeResult struct {
	// Reference 网关流水号。
	Reference string
	// OutTradeNo 商户订单号。
	OutTradeNo string
	// Status 网关状态原文,例如 succeeded / requires_payment_method。
	Status string
	// Amount 金额,单位:分。
	Amount int64
	// Currency 币种。
	Currency string
	// Paid 是否已支付成功。
	Paid bool
	// Raw 网关原始数据。
	Raw map[string]string
}

// RefundRequest 退款请求。
type RefundRequest struct {
	// OutTradeNo 商户订单号。
	OutTradeNo string
	// RefundID 商户退款单号(幂等键)。
	RefundID string
	// Amount 退款金额,单位:分;为 0 表示全额退款。
	Amount int64
	// Reason 退款原因。
	Reason string
}

// RefundResult 退款结果。
type RefundResult struct {
	// RefundID 退款单号。
	RefundID string
	// Reference 网关退款流水号。
	Reference string
	// Status 状态。
	Status string
	// Amount 退款金额,单位:分。
	Amount int64
}

// ErrUnsupported 网关不支持该操作。
var ErrUnsupported = fmt.Errorf("gateway: operation not supported")

// ResolveCurrency 查回「建会话时用的那个币种」。
//
// 钱包支付的关键不变式:**扣款的币种必须和建会话的币种一致**。
// 钱包令牌是在那个币种的收银台里产生的(Apple Pay 的令牌里带金额与币种),
// 币种对不上,收单机构要么直接拒,要么扣成另一种币种的钱 ——
// 后者是真金白银的事故,所以这条不能靠"调用方记得传对"。
//
// 为什么要查:扣款的入参里**没有币种**(钱包渠道的 ChargeWallet 只收
// 订单号、令牌与金额),币种是建会话时定的,扣款时必须把它找回来。
//
// 查不到(网关里还没这笔单 —— 「没建会话就直接扣」的兜底路径)就用 fallback;
// 查询过程中出了别的问题就把错误返回:宁可扣不成,也不能用错的币种扣成。
func ResolveCurrency(ctx context.Context, acq Acquirer, outTradeNo, fallback string) (string, error) {
	if acq == nil || outTradeNo == "" {
		return fallback, nil
	}
	res, err := acq.Query(ctx, outTradeNo)
	if err == nil {
		if res != nil && res.Currency != "" {
			return res.Currency, nil
		}
		return fallback, nil
	}
	if errors.Is(err, payment.ErrOrderNotFound) {
		return fallback, nil
	}
	return "", err
}
