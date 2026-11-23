package payment

import (
	"context"
	"errors"
)

// 通用错误。
var (
	// ErrOrderNotFound 订单不存在。
	ErrOrderNotFound = errors.New("payment: order not found")
	// ErrOrderClosed 订单已关闭。
	ErrOrderClosed = errors.New("payment: order closed")
	// ErrUnsupportedTradeType 渠道不支持该交易类型。
	ErrUnsupportedTradeType = errors.New("payment: unsupported trade type")
	// ErrInvalidSignature 签名校验失败。
	ErrInvalidSignature = errors.New("payment: invalid signature")
	// ErrInvalidConfig 配置错误。
	ErrInvalidConfig = errors.New("payment: invalid config")
	// ErrNotImplemented 渠道未实现该能力。
	ErrNotImplemented = errors.New("payment: not implemented")
)

// NotifyRequest 渠道回调原始请求的通用封装。
//
// Headers 建议直接放 http.Header 的副本;Header() 方法大小写不敏感。
type NotifyRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
	Query   map[string]string
}

// Header 大小写不敏感地取请求头。
func (r *NotifyRequest) Header(name string) string {
	if r == nil || r.Headers == nil {
		return ""
	}
	if v, ok := r.Headers[name]; ok {
		return v
	}
	lower := lowerASCII(name)
	for k, v := range r.Headers {
		if lowerASCII(k) == lower {
			return v
		}
	}
	return ""
}

// QueryValue 取查询参数,不存在返回空串。
func (r *NotifyRequest) QueryValue(name string) string {
	if r == nil || r.Query == nil {
		return ""
	}
	return r.Query[name]
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// Provider 支付渠道统一接口(各渠道包实现)。
//
// 每个渠道包(providers/wechat、providers/alipay、providers/applepay、providers/googlepay)
// 都实现该接口,注册到 Client 后即可用统一入口调用。
type Provider interface {
	// Channel 渠道标识。
	Channel() Channel
	// SupportedTradeTypes 该渠道支持的交易类型。
	SupportedTradeTypes() []TradeType
	// Create 发起支付。
	Create(ctx context.Context, order *Order, tt TradeType) (*PaymentSession, error)
	// Query 查询交易状态。
	Query(ctx context.Context, outTradeNo string) (*TradeResult, error)
	// Close 关闭未支付订单。
	Close(ctx context.Context, outTradeNo string) error
	// Refund 发起退款。
	Refund(ctx context.Context, req *RefundRequest) (*RefundResult, error)
	// ParseNotify 验签并解析异步回调。
	ParseNotify(ctx context.Context, req *NotifyRequest) (*Notify, error)
}
