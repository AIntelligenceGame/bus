package payment

import (
	"context"
	"fmt"
)

// 钱包类渠道(Apple Pay / Google Pay)的扣款能力。
//
// 钱包支付的完整闭环与微信/支付宝不同:
//
//  1. 服务端 Create 交易类型=wallet,拿到 session.client_params(含商户号、可发布密钥等);
//  2. 客户端用原生 SDK(PassKit / Google Pay PaymentsClient)拉起收银台,用户在钱包里输密码;
//  3. 客户端拿到「支付令牌」(Apple 的 paymentMethod / Google Pay 的 paymentData)回传服务端;
//  4. 服务端用 ChargeWallet 拿令牌换一次真实扣款(收单网关侧完成扣款);
//  5. 结果以异步回调(Acquire.ParseWebhook)为准,客户端只是拿了个「已授权」信号。
//
// 因此钱包渠道除了 Provider 的通用能力外,还必须实现 WalletProvider。
//
// WalletProvider 钱包渠道的扣款能力。
type WalletProvider interface {
	Provider
	// ChargeWallet 用客户端回传的支付令牌完成扣款。
	//
	// outTradeNo 为商户订单号,paymentToken 为客户端 SDK 返回的支付令牌,
	// amount 为订单金额(分)。
	ChargeWallet(ctx context.Context, outTradeNo, paymentToken string, amount int64) (*WalletChargeResult, error)
}

// WalletChargeResult 钱包渠道扣款结果。
type WalletChargeResult struct {
	// OutTradeNo 商户订单号。
	OutTradeNo string `json:"out_trade_no"`
	// TransactionID 收单网关流水号。
	TransactionID string `json:"transaction_id,omitempty"`
	// Amount 扣款金额(分)。
	Amount int64 `json:"amount"`
	// Currency 币种。
	Currency string `json:"currency,omitempty"`
	// Status 统一交易状态。
	Status TradeStatus `json:"status"`
	// Paid 是否已扣款成功(以回调为准时可能先为 false)。
	Paid bool `json:"paid"`
	// Raw 渠道原始数据,便于对账。
	Raw map[string]string `json:"raw,omitempty"`
}

// ChargeWallet 用客户端回传的支付令牌完成扣款(Apple Pay / Google Pay)。
//
// 非钱包渠道会返回 ErrUnsupportedTradeType。
func (c *Client) ChargeWallet(ctx context.Context, ch Channel, outTradeNo, paymentToken string, amount int64) (*WalletChargeResult, error) {
	p, ok := c.Provider(ch)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not registered", ErrInvalidConfig, ch)
	}
	wp, ok := p.(WalletProvider)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s does not support wallet charge", ErrUnsupportedTradeType, ch)
	}
	return wp.ChargeWallet(ctx, outTradeNo, paymentToken, amount)
}
