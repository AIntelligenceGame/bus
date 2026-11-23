package platform

import (
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

// 对外的数据结构(与数据库模型分离:数据库模型见 gormmodels.go)。

// WalletSettings 钱包渠道(Apple Pay / Google Pay)配置。
type WalletSettings struct {
	// MerchantID 钱包商户号。
	MerchantID string `json:"merchant_id"`
	// DisplayName 收银台展示名称(Google Pay 用作 MerchantName)。
	DisplayName string `json:"display_name"`
	// DomainName 商户域名。
	DomainName string `json:"domain_name"`
	// Currency 默认币种。
	Currency string `json:"currency"`
	// StripeSecretKey 收单机构私钥。
	StripeSecretKey string `json:"stripe_secret_key"`
	// StripePublishableKey 收单机构可发布密钥。
	StripePublishableKey string `json:"stripe_publishable_key"`
	// StripeWebhookSecret 收单机构 Webhook 签名密钥。
	StripeWebhookSecret string `json:"stripe_webhook_secret"`
}

// ChannelConfig 某个用户在某个渠道上的支付配置。
type ChannelConfig struct {
	// Channel 渠道标识:wechat / alipay / applepay / googlepay。
	Channel string `json:"channel"`
	// Enabled 是否启用。
	Enabled bool `json:"enabled"`
	// IsDefault 是否为该用户的默认渠道。
	IsDefault bool `json:"is_default"`
	// Wechat 微信支付配置(仅 channel=wechat 时有效)。
	Wechat *wechat.Config `json:"wechat,omitempty"`
	// Alipay 支付宝配置(仅 channel=alipay 时有效)。
	Alipay *alipay.Config `json:"alipay,omitempty"`
	// Wallet 钱包渠道配置(仅 channel=applepay/googlepay 时有效)。
	Wallet *WalletSettings `json:"wallet,omitempty"`
}

// User 平台用户(商户)。
type User struct {
	// UserID 用户标识。
	UserID string `json:"user_id"`
	// UserName 展示名称。
	UserName string `json:"user_name"`
	// AccessToken 访问令牌明文。
	//
	// 库里只存它的带密钥散列,明文只在「刚签发」的那一刻存在,
	// 调用接口时放在 X-User-Token 头(或 pay_token Cookie)。
	AccessToken string `json:"-"`
	// PasswordHash 口令散列(永不返回给前端)。
	PasswordHash string `json:"-"`
	// NotifyBaseURL 商户自己配置的对外访问地址(回调地址前缀)。
	//
	// 空表示用平台默认地址(启动参数 -base)。有了它,商户就能在控制台里
	// 换掉回调域名,不用改命令行、不用重启平台。
	NotifyBaseURL string `json:"notify_base_url"`
	// MFAType 二次验证方式:email / sms,空表示未绑定。
	MFAType string `json:"mfa_type"`
	// MFATarget 二次验证目标(邮箱或手机号)。
	MFATarget string `json:"mfa_target"`
	// MFABackupType 备用二次验证方式(email/sms),空表示没绑。
	//
	// 「验证器 App 丢了」时的第二条腿:登录可以改用它。
	MFABackupType string `json:"mfa_backup_type"`
	// MFABackupTarget 备用二次验证的接收地址。
	MFABackupTarget string `json:"mfa_backup_target"`
	// MFAEnabled 是否已开启二次验证。
	MFAEnabled bool `json:"mfa_enabled"`
	// RequireAppBinding 是否强制订单绑定应用。
	//
	// 开启后这个商户的每一笔支付都必须归属某个应用:没带 app_id 的下单
	// 会被拒绝,没绑应用的密钥也不能下单。不开的话订单会落进「未分类」。
	RequireAppBinding bool `json:"require_app_binding"`
	// FailedLogins 连续登录失败次数(内部使用,不下发前端)。
	FailedLogins int32 `json:"-"`
	// LockedUntil 账户锁定截止时间(RFC3339Nano),空表示未锁定。
	LockedUntil string `json:"-"`
	// Disabled 是否被平台禁用。被禁用后不能登录,已有令牌立即失效。
	Disabled bool `json:"disabled"`
	// CreatedAt 创建时间。
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt 更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

// App 商户的一个应用 / 产品。
//
// 商户用它把订单分成「产品维度」:每个产品可以有自己的一套渠道配置
// (两个微信 App 各配各的 appid/mchid),订单、退款、统计、报表都能按它分开。
type App struct {
	// AppID 应用标识(平台生成)。
	AppID string `json:"app_id"`
	// UserID 所属商户。
	UserID string `json:"user_id"`
	// Name 应用名称(同一商户下唯一)。
	Name string `json:"name"`
	// Remark 备注。
	Remark string `json:"remark,omitempty"`
	// OrderCount 该应用下的订单数(列表接口回填)。
	OrderCount int64 `json:"order_count,omitempty"`
	// Amount 该应用下的成功金额合计(分,列表接口回填)。
	Amount int64 `json:"amount,omitempty"`
	// CreatedAt 创建时间。
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt 更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

// OrderRecord 一条订单记录。
type OrderRecord struct {
	OutTradeNo string `json:"out_trade_no"`
	UserID     string `json:"user_id"`
	// AppID 订单归属的应用(空 = 未分类)。
	AppID      string `json:"app_id"`
	Channel    string `json:"channel"`
	TradeType  string `json:"trade_type"`
	Subject    string `json:"subject"`
	Amount     int64  `json:"amount"`
	AmountYuan string `json:"amount_yuan"`
	// RefundedAmount 这张订单**已经退掉**的金额(分)。
	//
	// 前端要靠它算「还能退多少」:可退余额 = amount - refunded_amount。
	// 没有这个字段的话,前端只能拿订单金额当上限,
	// 于是「全额退款」在部分退款过的订单上会被后端拒掉
	// (后端按「已退 + 本次 ≤ 订单金额」校验,而前端填的是订单全额)。
	RefundedAmount int64 `json:"refunded_amount"`
	// RefundableAmount 这张订单**还能退**的金额(分)。
	RefundableAmount int64           `json:"refundable_amount"`
	Status           string          `json:"status"`
	TransactionID    string          `json:"transaction_id,omitempty"`
	QRCode           string          `json:"qrcode,omitempty"`
	Notify           *payment.Notify `json:"notify,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	// ChannelCreated 渠道侧是否真的有这笔单。
	//
	// false = 平台已落库,但调渠道没成功(或者进程正好崩在那一步):
	// 渠道那边没有这笔单,关单时不必去调渠道,商户也可以用同一个商户单号重试。
	ChannelCreated bool `json:"channel_created"`
}

// AuditLog 一条审计日志。
type AuditLog struct {
	LogID     string    `json:"log_id"`
	UserID    string    `json:"user_id"`
	Action    string    `json:"action"`
	Target    string    `json:"target"`
	Detail    string    `json:"detail"`
	ClientIP  string    `json:"client_ip"`
	CreatedAt time.Time `json:"created_at"`
}

// RefundRecord 一条退款记录。
//
// 平台必须自己记退款流水:否则「累计退了多少」无从判断,
// 就会出现「退 1 元、再退 1 元」直到超过订单金额的情况。
type RefundRecord struct {
	// RefundID 平台侧退款流水号。
	RefundID string `json:"refund_id"`
	// UserID 所属商户。
	UserID string `json:"user_id"`
	// OutTradeNo 商户订单号。
	OutTradeNo string `json:"out_trade_no"`
	// OutRefundNo 商户退款单号(幂等键)。
	OutRefundNo string `json:"out_refund_no"`
	// Channel 渠道。
	Channel string `json:"channel"`
	// AppID 退款归属的应用(继承自订单)。
	AppID string `json:"app_id"`
	// Amount 退款金额(分)。
	Amount int64 `json:"amount"`
	// AmountYuan 退款金额(元)。
	AmountYuan string `json:"amount_yuan"`
	// Status 退款状态(渠道返回)。
	Status string `json:"status"`
	// Reason 退款原因。
	Reason string `json:"reason,omitempty"`
	// CreatedAt 退款时间。
	CreatedAt time.Time `json:"created_at"`
}

// OrderFilter 订单列表的筛选与分页条件。
type OrderFilter struct {
	// Page 页码(从 1 开始)。
	Page int
	// Size 每页条数。
	Size int
	// Status 订单状态(精确匹配)。
	Status string
	// Channel 渠道(精确匹配)。
	Channel string
	// AppID 应用(精确匹配)。空 = 不限。
	AppID string
	// Keyword 关键字(订单号 / 标题 / 渠道流水号)。
	Keyword string
	// From 起始时间(含),RFC3339。
	From string
	// To 截止时间(含),RFC3339。
	To string
}

// PageInfo 分页信息。
type PageInfo struct {
	// Page 当前页码。
	Page int `json:"page"`
	// Size 每页条数。
	Size int `json:"size"`
	// Total 总条数。
	Total int64 `json:"total"`
	// Pages 总页数。
	Pages int `json:"pages"`
}

// Stats 商户维度的交易统计。
type Stats struct {
	// OrderCount 订单总数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// FailedCount 失败/关闭订单数。
	FailedCount int64 `json:"failed_count"`
	// RefundCount 退款笔数。
	RefundCount int64 `json:"refund_count"`
	// TotalAmount 支付成功金额合计(分)。
	TotalAmount int64 `json:"total_amount"`
	// RefundAmount 退款金额合计(分)。
	RefundAmount int64 `json:"refund_amount"`
	// NetAmount 净收入(成功金额 - 退款金额,分)。
	NetAmount int64 `json:"net_amount"`
	// TodayCount 今日订单数。
	TodayCount int64 `json:"today_count"`
	// TodayAmount 今日成功金额(分)。
	TodayAmount int64 `json:"today_amount"`
	// SuccessRate 成功率(0~1)。
	SuccessRate float64 `json:"success_rate"`
}

// StatGroup 某个分组维度上的统计(按应用 / 按渠道 / 按天)。
type StatGroup struct {
	// Key 分组键:应用 ID / 渠道 / 日期(YYYY-MM-DD)。
	Key string `json:"key"`
	// Label 分组显示名(应用名 / 渠道名 / 日期)。
	Label string `json:"label"`
	// OrderCount 订单总数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// RefundCount 退款笔数。
	RefundCount int64 `json:"refund_count"`
	// TotalAmount 支付成功金额合计(分)。
	TotalAmount int64 `json:"total_amount"`
	// RefundAmount 退款金额合计(分)。
	RefundAmount int64 `json:"refund_amount"`
	// NetAmount 净收入(分)。
	NetAmount int64 `json:"net_amount"`
}
