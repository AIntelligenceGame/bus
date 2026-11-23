// Package pay 是支付库的「易用门面」。
//
// 设计目标:调用者用最少的代码就能发起支付、查单、退款、处理回调。
//
//	client, err := pay.New(pay.Config{
//	    Wechat: &wechat.Config{ /* ... */ },
//	    Alipay: &alipay.Config{ /* ... */ },
//	})
//	if err != nil {
//	    return err
//	}
//	session, err := client.Pay(ctx, pay.Wechat, pay.Order{
//	    OutTradeNo: "ORDER202601010001",
//	    Subject:    "iPhone 17 Pro",
//	    Amount:     19900, // 单位:分
//	})
//
// 其中 Pay 会根据渠道与订单内容自动选择交易类型:
//   - 微信: 传了 PayerID(openid) 走 JSAPI,否则走扫码(二维码);
//   - 支付宝: 传了 PayerID(buyer_id) 走手机网站支付,否则走当面付(二维码);
//   - Apple Pay / Google Pay: 走钱包支付。
//
// 也可以显式指定场景:client.PayQRCode / client.PayApp / client.PayJSAPI / client.PayWallet。
package pay

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/applepay"
	"github.com/AIntelligenceGame/bus/payment/providers/googlepay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
	"github.com/AIntelligenceGame/bus/payment/qrcode"
)

// 类型别名,方便调用者少写包名。
type (
	// Order 订单。
	Order = payment.Order
	// Session 支付会话。
	Session = payment.PaymentSession
	// Result 交易结果。
	Result = payment.TradeResult
	// RefundRequest 退款请求。
	RefundRequest = payment.RefundRequest
	// RefundResult 退款结果。
	RefundResult = payment.RefundResult
	// Notify 统一的回调解析结果。
	Notify = payment.Notify
	// Channel 支付渠道。
	Channel = payment.Channel
	// TradeType 交易类型。
	TradeType = payment.TradeType
)

// 渠道常量别名。
const (
	Wechat    = payment.ChannelWechat
	Alipay    = payment.ChannelAlipay
	ApplePay  = payment.ChannelApplePay
	GooglePay = payment.ChannelGooglePay
)

// 交易类型常量别名。
const (
	Native = payment.TradeTypeNative
	App    = payment.TradeTypeApp
	JSAPI  = payment.TradeTypeJSAPI
	H5     = payment.TradeTypeH5
	Wap    = payment.TradeTypeWap
	Web    = payment.TradeTypeWeb
	Wallet = payment.TradeTypeWallet
)

// Client 支付客户端(聚合多个渠道)。
type Client struct {
	inner *payment.Client
}

// New 根据配置创建客户端,只创建配置了的渠道(可配合 LoadConfigFile 使用)。
func New(cfg Config) (*Client, error) {
	inner, err := payment.NewClient()
	if err != nil {
		return nil, err
	}
	if cfg.Wechat != nil {
		p, err := wechat.New(cfg.Wechat)
		if err != nil {
			return nil, err
		}
		if err := inner.Register(p); err != nil {
			return nil, err
		}
	}
	if cfg.Alipay != nil {
		p, err := alipay.New(cfg.Alipay)
		if err != nil {
			return nil, err
		}
		if err := inner.Register(p); err != nil {
			return nil, err
		}
	}
	if cfg.ApplePay != nil {
		p, err := applepay.New(cfg.ApplePay)
		if err != nil {
			return nil, err
		}
		if err := inner.Register(p); err != nil {
			return nil, err
		}
	}
	if cfg.GooglePay != nil {
		p, err := googlepay.New(cfg.GooglePay)
		if err != nil {
			return nil, err
		}
		if err := inner.Register(p); err != nil {
			return nil, err
		}
	}
	return &Client{inner: inner}, nil
}

// Channels 返回已启用渠道。
func (c *Client) Channels() []payment.Channel { return c.inner.Channels() }

// Create 按指定交易类型发起支付。
//
// 需要精确控制交易类型时用本方法(如服务端要按 trade_type 下发不同场景)。
func (c *Client) Create(ctx context.Context, ch payment.Channel, tt payment.TradeType, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.Create(ctx, ch, tt, &o)
}

// Pay 发起支付并自动选择交易类型。
func (c *Client) Pay(ctx context.Context, ch payment.Channel, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.CreateAuto(ctx, ch, &o)
}

// PayQRCode 发起扫码支付,返回的 Session.QRCode 可渲染成二维码给手机扫。
func (c *Client) PayQRCode(ctx context.Context, ch payment.Channel, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.Create(ctx, ch, payment.TradeTypeNative, &o)
}

// PayApp 发起移动端拉起支付(微信 APP 支付 / 支付宝 APP 支付)。
func (c *Client) PayApp(ctx context.Context, ch payment.Channel, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.Create(ctx, ch, payment.TradeTypeApp, &o)
}

// PayJSAPI 发起微信内网页支付。
func (c *Client) PayJSAPI(ctx context.Context, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.Create(ctx, payment.ChannelWechat, payment.TradeTypeJSAPI, &o)
}

// PayWallet 发起钱包支付(Apple Pay / Google Pay)。
func (c *Client) PayWallet(ctx context.Context, ch payment.Channel, order payment.Order) (*payment.PaymentSession, error) {
	o := order
	return c.inner.Create(ctx, ch, payment.TradeTypeWallet, &o)
}

// ChargeWallet 用客户端回传的支付令牌完成扣款(Apple Pay / Google Pay)。
//
// 移动端闭环的最后一步:客户端 SDK 拉起收银台、用户在钱包里输密码后,
// 客户端把支付令牌回传服务端,由本方法完成实际扣款。
func (c *Client) ChargeWallet(ctx context.Context, ch payment.Channel, outTradeNo, paymentToken string, amount int64) (*payment.WalletChargeResult, error) {
	return c.inner.ChargeWallet(ctx, ch, outTradeNo, paymentToken, amount)
}

// Query 查询订单状态。
func (c *Client) Query(ctx context.Context, ch payment.Channel, outTradeNo string) (*payment.TradeResult, error) {
	return c.inner.Query(ctx, ch, outTradeNo)
}

// Close 关闭未支付订单。
func (c *Client) Close(ctx context.Context, ch payment.Channel, outTradeNo string) error {
	return c.inner.Close(ctx, ch, outTradeNo)
}

// Refund 发起退款。
func (c *Client) Refund(ctx context.Context, ch payment.Channel, req payment.RefundRequest) (*payment.RefundResult, error) {
	return c.inner.Refund(ctx, ch, &req)
}

// HandleNotify 验签并解析异步回调,返回统一模型。
//
// 在 HTTP 回调处理器里直接调用即可;校验失败会返回 payment.ErrInvalidSignature。
func (c *Client) HandleNotify(ctx context.Context, ch payment.Channel, r *http.Request) (*payment.Notify, error) {
	req, err := FromHTTPRequest(r)
	if err != nil {
		return nil, err
	}
	return c.inner.ParseNotify(ctx, ch, req)
}

// QRCodePNG 把 Session.QRCode 渲染成 PNG 图片,可直接返回给前端展示。
func QRCodePNG(session *payment.PaymentSession, scale int) ([]byte, error) {
	if session == nil || session.QRCode == "" {
		return nil, fmt.Errorf("pay: session has no qrcode")
	}
	return qrcode.PNG(session.QRCode, scale)
}

// QRCodeSVG 把 Session.QRCode 渲染成 SVG 矢量图。
func QRCodeSVG(session *payment.PaymentSession, scale int) ([]byte, error) {
	if session == nil || session.QRCode == "" {
		return nil, fmt.Errorf("pay: session has no qrcode")
	}
	return qrcode.SVG(session.QRCode, scale)
}

// FromHTTPRequest 把 *http.Request 转换成统一的回调请求结构。
func FromHTTPRequest(r *http.Request) (*payment.NotifyRequest, error) {
	if r == nil {
		return nil, fmt.Errorf("pay: nil request")
	}
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("pay: read request body: %w", err)
		}
		body = b
	}
	headers := make(map[string]string, len(r.Header))
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}
	query := make(map[string]string, len(r.URL.Query()))
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			query[k] = v[0]
		}
	}
	return &payment.NotifyRequest{
		Method:  r.Method,
		URL:     r.URL.String(),
		Headers: headers,
		Body:    body,
		Query:   query,
	}, nil
}
