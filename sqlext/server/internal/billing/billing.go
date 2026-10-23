// Package billing 定义充值渠道抽象。
//
// 为什么抽象：支付落地方式变化极快（手工转账 → 聚合支付 → 微信/支付宝直连 →
// Stripe），而"下单 / 查单 / 到账"这三件事的形状是稳定的。把渠道隔离开，
// 换支付方式时不用动订单、余额和接口层。
package billing

import (
	"context"
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/sqlext/server/internal/store"
)

// Checkout 是一次支付会话的描述。
type Checkout struct {
	// Channel 渠道名，如 manual、wechat、alipay、stripe。
	Channel string `json:"channel"`
	// Method 展示方式：qrcode（二维码）、redirect（跳转）、manual（人工确认）。
	Method string `json:"method"`
	// QRCodeURL 二维码图片地址（method=qrcode 时有值）。
	QRCodeURL string `json:"qrcode_url,omitempty"`
	// RedirectURL 支付完成后跳转的地址（method=redirect 时有值）。
	RedirectURL string `json:"redirect_url,omitempty"`
	// Instructions 给用户看的说明（人工转账时用）。
	Instructions string `json:"instructions,omitempty"`
	// Payee 收款方标识（人工转账时用）。
	Payee string `json:"payee,omitempty"`
}

// Channel 是一个支付渠道。
type Channel interface {
	// Name 渠道名。
	Name() string
	// CreateCheckout 为订单生成一次支付会话。
	CreateCheckout(ctx context.Context, order *store.Order, baseURL string) (*Checkout, error)
}

// ManualChannel 是"人工转账"渠道：生成订单，用户转账，管理员（或用户自证
// 流水号后由管理员核对）确认到账。
//
// 这是**零资质、今天就能上线**的方案：不需要商户号、不需要 ICP、不需要
// 第三方支付账号。代价是要人工对账，掉单得靠人工补。
type ManualChannel struct {
	// Payee 收款方（微信/支付宝收款码上的名字，或账号）。
	Payee string
	// Notice 额外的说明文字。
	Notice string
}

// Name 实现 Channel。
func (m ManualChannel) Name() string { return "manual" }

// CreateCheckout 实现 Channel。
func (m ManualChannel) CreateCheckout(ctx context.Context, order *store.Order, baseURL string) (*Checkout, error) {
	notice := m.Notice
	if notice == "" {
		notice = "请向收款方转账并在备注里填写订单号，转账后等待到账确认。"
	}
	notice = fmt.Sprintf("%s 订单号：%s，需到账 %d 积分。",
		notice, order.ID, order.Amount)
	if m.Payee != "" {
		notice += " 收款方：" + m.Payee
	}
	return &Checkout{
		Channel:      m.Name(),
		Method:       "manual",
		Instructions: notice,
		Payee:        m.Payee,
	}, nil
}

// Registry 维护可用渠道。
type Registry struct {
	channels map[string]Channel
}

// NewRegistry 创建渠道注册表。
func NewRegistry(channels ...Channel) *Registry {
	r := &Registry{channels: map[string]Channel{}}
	for _, c := range channels {
		r.channels[c.Name()] = c
	}
	return r
}

// Register 注册（或替换）一个渠道。
func (r *Registry) Register(c Channel) { r.channels[c.Name()] = c }

// Get 按名字取渠道。
func (r *Registry) Get(name string) (Channel, bool) {
	c, ok := r.channels[strings.TrimSpace(name)]
	return c, ok
}

// Default 返回默认渠道，没有配置时退化成 manual。
func (r *Registry) Default() Channel {
	if c, ok := r.channels["manual"]; ok {
		return c
	}
	return ManualChannel{}
}
