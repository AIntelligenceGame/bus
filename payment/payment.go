// Package payment 提供一套通用的支付抽象层。
//
// 目录结构:
//
//	payment/                 核心抽象层(本文件所在目录)
//	├── payment.go           数据模型:订单、会话、交易结果、退款、回调
//	├── provider.go          Provider 接口与通用错误
//	├── client.go            统一客户端(多渠道注册与调度)
//	├── money.go             金额换算(内部统一用「分」)
//	├── crypto.go            RSA/ECDSA/AES 加解密与签名
//	├── hash.go              摘要与 HMAC 工具
//	├── pay/                 易用门面:一行配置即可发起支付
//	├── providers/           各支付渠道实现
//	│   ├── wechat/          微信支付 APIv3
//	│   ├── alipay/          支付宝开放平台
//	│   ├── applepay/        Apple Pay
//	│   ├── googlepay/       Google Pay
//	│   └── gateway/         收单网关抽象(Apple/Google Pay 走网关)
//	├── qrcode/              二维码编码与渲染(PNG/SVG)
//	└── example/             使用示例
//
// 设计目标:
//   - 用同一套 Order / PaymentSession / TradeResult 模型对接 微信、支付宝、Apple Pay、Google Pay;
//   - 覆盖两类场景:移动端拉起支付(APP 调起 / JSAPI / 钱包)、展示二维码给手机扫码支付;
//   - 回调通知统一验签、统一模型,便于幂等入账。
//
// 金额单位统一为「分」(int64),各渠道需要的「元」由渠道包自行换算,避免浮点误差。
package payment

import (
	"errors"
	"time"
)

// Channel 支付渠道。
type Channel string

const (
	ChannelWechat    Channel = "wechat"
	ChannelAlipay    Channel = "alipay"
	ChannelApplePay  Channel = "applepay"
	ChannelGooglePay Channel = "googlepay"
)

func (c Channel) String() string { return string(c) }

// TradeType 支付场景/交易类型。
type TradeType string

const (
	// TradeTypeNative 扫码支付,返回二维码内容,用户用手机 App 扫一扫。
	TradeTypeNative TradeType = "native"
	// TradeTypeApp 移动端 App 拉起支付(微信 APP 支付、支付宝 APP 支付)。
	TradeTypeApp TradeType = "app"
	// TradeTypeJSAPI 微信内网页支付(公众号/小程序)。
	TradeTypeJSAPI TradeType = "jsapi"
	// TradeTypeH5 微信 H5 支付(非微信内浏览器)。
	TradeTypeH5 TradeType = "h5"
	// TradeTypeWap 支付宝手机网站支付,返回跳转 URL。
	TradeTypeWap TradeType = "wap"
	// TradeTypeWeb 支付宝电脑网站支付,返回跳转 URL。
	TradeTypeWeb TradeType = "web"
	// TradeTypeWallet 移动端钱包支付(Apple Pay / Google Pay)。
	TradeTypeWallet TradeType = "wallet"
)

// Order 一笔待支付订单。渠道私有参数放在 Extra 中(金额单位:分)。
type Order struct {
	// OutTradeNo 商户订单号,全局唯一,同时作为幂等键。
	OutTradeNo string `json:"out_trade_no"`
	// Subject 订单标题(展示给用户)。
	Subject string `json:"subject"`
	// Body 订单描述,可选。
	Body string `json:"body"`
	// Amount 支付金额,单位:分。
	Amount int64 `json:"amount"`
	// Currency 币种,默认 CNY。
	Currency string `json:"currency,omitempty"`
	// ClientIP 用户 IP,H5 / 当面付场景建议填写。
	ClientIP string `json:"client_ip,omitempty"`
	// NotifyURL 异步回调地址,覆盖渠道默认配置。
	NotifyURL string `json:"notify_url,omitempty"`
	// ReturnURL 同步跳转地址(网页支付场景)。
	ReturnURL string `json:"return_url,omitempty"`
	// Attach 业务附加数据,渠道回调时原样回传。
	//
	// 各渠道落在哪个字段上不一样:
	//   - 微信:attach(最多 128 字符);
	//   - 支付宝:passback_params(最多 512 字符,平台会自动 UrlEncoded;
	//     官方要求必须编码,且不能是 JSON 格式)。
	Attach string `json:"attach,omitempty"`
	// PayerID 用户标识:微信 JSAPI 填 openid,支付宝 WAP 填 buyer_id。
	PayerID string `json:"payer_id,omitempty"`
	// Expire 订单有效期,为 0 时使用渠道默认值。
	Expire time.Duration `json:"expire,omitempty"`
	// Extra 渠道私有参数,例如微信 scene_info、支付宝 extend_params。
	Extra map[string]string `json:"extra,omitempty"`
}

// Validate 校验订单基础字段。
func (o *Order) Validate() error {
	if o == nil {
		return errors.New("payment: order is nil")
	}
	if o.OutTradeNo == "" {
		return errors.New("payment: out_trade_no is required")
	}
	if o.Subject == "" {
		return errors.New("payment: subject is required")
	}
	if o.Amount <= 0 {
		return errors.New("payment: amount must be greater than 0")
	}
	return nil
}

// PaymentSession 一次支付会话的返回结果。
//
// 不同渠道通过不同字段承载信息:
//
//	微信 APP 支付   -> ClientParams.Wechat: Swift WXApi / Kotlin IWXAPI 调起参数
//	支付宝 APP 支付 -> ClientParams.Alipay: 已签名的 orderString
//	微信 Native / 支付宝当面付 -> QRCode: 二维码原始内容
//	网页支付        -> RedirectURL: 跳转地址
//	Apple Pay       -> ClientParams.ApplePay: PassKit 所需参数
//	Google Pay      -> ClientParams.GooglePay: PaymentsClient / JS API 所需参数
//
// AppParams 保留为底层的通用参数集合(按渠道原始字段名),
// ClientParams 是面向客户端 SDK 的结构化版本,新代码优先用后者。
type PaymentSession struct {
	Channel    Channel   `json:"channel"`
	TradeType  TradeType `json:"trade_type"`
	OutTradeNo string    `json:"out_trade_no"`
	// PrepayID 微信预支付交易会话标识。
	PrepayID string `json:"prepay_id,omitempty"`
	// QRCode 二维码原始内容,前端可用 qrcode 包渲染成图片。
	QRCode string `json:"qrcode,omitempty"`
	// RedirectURL 需要浏览器跳转的地址。
	RedirectURL string `json:"redirect_url,omitempty"`
	// Token 钱包(Apple Pay / Google Pay)会话令牌。
	Token string `json:"token,omitempty"`
	// ClientParams 面向客户端 SDK 的结构化调起参数。
	ClientParams *ClientParams `json:"client_params,omitempty"`
	// AppParams 移动端拉起支付所需的参数集合(通用扁平形式)。
	AppParams map[string]any `json:"app_params,omitempty"`
	// Extra 其他透传信息。
	Extra map[string]any `json:"extra,omitempty"`
}

// TradeStatus 交易状态。
type TradeStatus string

const (
	TradeStatusSuccess  TradeStatus = "SUCCESS"
	TradeStatusPending  TradeStatus = "PENDING"
	TradeStatusFailed   TradeStatus = "FAILED"
	TradeStatusClosed   TradeStatus = "CLOSED"
	TradeStatusRefunded TradeStatus = "REFUNDED"
	// TradeStatusPartiallyRefunded 部分退款(还没退完)。
	TradeStatusPartiallyRefunded TradeStatus = "PARTIALLY_REFUNDED"
)

// TradeResult 交易查询结果。
type TradeResult struct {
	Channel       Channel     `json:"channel"`
	OutTradeNo    string      `json:"out_trade_no"`
	TransactionID string      `json:"transaction_id,omitempty"`
	Amount        int64       `json:"amount"`
	Currency      string      `json:"currency,omitempty"`
	Status        TradeStatus `json:"status"`
	PaidAt        time.Time   `json:"paid_at,omitempty"`
	// Raw 渠道原始数据,便于对账与排查。
	Raw map[string]string `json:"raw,omitempty"`
}

// RefundRequest 退款请求。RefundAmount 为 0 表示全额退款。
type RefundRequest struct {
	OutTradeNo   string `json:"out_trade_no"`
	OutRefundNo  string `json:"out_refund_no"`
	RefundAmount int64  `json:"refund_amount,omitempty"`
	Reason       string `json:"reason,omitempty"`
	NotifyURL    string `json:"notify_url,omitempty"`
	// TotalAmount 原订单金额(分)。为 0 时渠道会先查询订单补齐。
	TotalAmount int64 `json:"total_amount,omitempty"`
}

// RefundResult 退款结果。
type RefundResult struct {
	Channel      Channel `json:"channel"`
	OutTradeNo   string  `json:"out_trade_no"`
	OutRefundNo  string  `json:"out_refund_no"`
	RefundID     string  `json:"refund_id,omitempty"`
	Status       string  `json:"status,omitempty"`
	RefundAmount int64   `json:"refund_amount,omitempty"`
}

// Notify 统一的异步回调解析结果。
type Notify struct {
	Channel       Channel     `json:"channel"`
	OutTradeNo    string      `json:"out_trade_no"`
	TransactionID string      `json:"transaction_id,omitempty"`
	Amount        int64       `json:"amount"`
	Currency      string      `json:"currency,omitempty"`
	Status        TradeStatus `json:"status"`
	EventTime     time.Time   `json:"event_time,omitempty"`
	// Raw 渠道原始字段。
	Raw map[string]string `json:"raw,omitempty"`
}
