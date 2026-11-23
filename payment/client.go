package payment

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Client 支付客户端,聚合多个渠道 Provider,提供统一的调用入口(核心层)。
//
//	c := payment.NewClient()
//	c.Register(wechatProvider)
//	session, err := c.Create(ctx, payment.ChannelWechat, payment.TradeTypeNative, &order)
type Client struct {
	mu        sync.RWMutex
	providers map[Channel]Provider
}

// NewClient 创建客户端,可一次注册多个渠道。
func NewClient(providers ...Provider) (*Client, error) {
	c := &Client{providers: make(map[Channel]Provider, len(providers))}
	for _, p := range providers {
		if err := c.Register(p); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register 注册(或覆盖)一个渠道 Provider。
func (c *Client) Register(p Provider) error {
	if p == nil {
		return fmt.Errorf("%w: nil provider", ErrInvalidConfig)
	}
	ch := p.Channel()
	if ch == "" {
		return fmt.Errorf("%w: provider has empty channel", ErrInvalidConfig)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.providers[ch] = p
	return nil
}

// Channels 返回已注册的渠道列表。
func (c *Client) Channels() []Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Channel, 0, len(c.providers))
	for ch := range c.providers {
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Provider 获取指定渠道的 Provider。
func (c *Client) Provider(ch Channel) (Provider, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.providers[ch]
	return p, ok
}

// MustProvider 获取指定渠道的 Provider,不存在时返回错误。
func (c *Client) MustProvider(ch Channel) (Provider, error) {
	p, ok := c.Provider(ch)
	if !ok {
		return nil, fmt.Errorf("payment: channel %q not registered", ch)
	}
	return p, nil
}

// Create 发起支付。
func (c *Client) Create(ctx context.Context, ch Channel, tt TradeType, order *Order) (*PaymentSession, error) {
	p, err := c.MustProvider(ch)
	if err != nil {
		return nil, err
	}
	if err := order.Validate(); err != nil {
		return nil, err
	}
	if !supports(p, tt) {
		return nil, fmt.Errorf("%w: channel %s does not support %s", ErrUnsupportedTradeType, ch, tt)
	}
	return p.Create(ctx, order, tt)
}

// CreateAuto 自动选择交易类型:有 payer_id 走 jsapi,否则走扫码。
func (c *Client) CreateAuto(ctx context.Context, ch Channel, order *Order) (*PaymentSession, error) {
	return c.Create(ctx, ch, AutoTradeType(ch, order), order)
}

// Query 查询订单状态。
func (c *Client) Query(ctx context.Context, ch Channel, outTradeNo string) (*TradeResult, error) {
	p, err := c.MustProvider(ch)
	if err != nil {
		return nil, err
	}
	return p.Query(ctx, outTradeNo)
}

// Close 关闭订单。
func (c *Client) Close(ctx context.Context, ch Channel, outTradeNo string) error {
	p, err := c.MustProvider(ch)
	if err != nil {
		return err
	}
	return p.Close(ctx, outTradeNo)
}

// Refund 发起退款。
func (c *Client) Refund(ctx context.Context, ch Channel, req *RefundRequest) (*RefundResult, error) {
	if req == nil || req.OutTradeNo == "" {
		return nil, fmt.Errorf("payment: out_trade_no is required")
	}
	p, err := c.MustProvider(ch)
	if err != nil {
		return nil, err
	}
	return p.Refund(ctx, req)
}

// ParseNotify 验签并解析异步回调。
func (c *Client) ParseNotify(ctx context.Context, ch Channel, req *NotifyRequest) (*Notify, error) {
	p, err := c.MustProvider(ch)
	if err != nil {
		return nil, err
	}
	return p.ParseNotify(ctx, req)
}

// AutoTradeType 根据渠道与订单内容推断交易类型。
func AutoTradeType(ch Channel, order *Order) TradeType {
	switch ch {
	case ChannelWechat:
		if order != nil && order.PayerID != "" {
			return TradeTypeJSAPI
		}
		return TradeTypeNative
	case ChannelAlipay:
		if order != nil && order.PayerID != "" {
			return TradeTypeWap
		}
		return TradeTypeNative
	case ChannelApplePay, ChannelGooglePay:
		return TradeTypeWallet
	default:
		return TradeTypeNative
	}
}

func supports(p Provider, tt TradeType) bool {
	for _, s := range p.SupportedTradeTypes() {
		if s == tt {
			return true
		}
	}
	return false
}
