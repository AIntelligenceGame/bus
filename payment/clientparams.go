package payment

// 客户端(移动端)拉起支付所需的参数。
//
// 设计背景:支付库是服务端的(Golang),但真正调起支付界面的是客户端原生 SDK:
//
//	iOS     -> Swift + WXApi / ALIPAYClient / PassKit(PKPaymentRequest)
//	Android -> Kotlin + IWXAPI / AlipayClient / Google Pay PaymentsClient
//
// 各家 SDK 期望的字段名与类型并不一致(例如微信 iOS 的 PayReq 用 prepayId,
// Android 的 requestPay 是位置参数),如果服务端只返回一堆扁平的 map,
// 客户端就得自己猜字段名,极易出错。
//
// 因此这里按「客户端 SDK」把参数显式建模,服务端按 SDK 的字段名填好,
// 客户端只需要做一次字段映射即可。

// ClientParams 一次支付会话下发给客户端的参数集合。
//
// 不同渠道只填对应的字段,其余为 nil。
type ClientParams struct {
	// Wechat 微信支付参数(Swift 的 WXApi 与 Kotlin 的 IWXAPI 通用)。
	Wechat *WechatClientParams `json:"wechat,omitempty"`
	// Alipay 支付宝参数(Swift 的 ALIPAYClient 与 Kotlin 的 AlipayClient 通用)。
	Alipay *AlipayClientParams `json:"alipay,omitempty"`
	// ApplePay Apple Pay 参数(仅 iOS,Swift + PassKit)。
	ApplePay *ApplePayClientParams `json:"apple_pay,omitempty"`
	// GooglePay Google Pay 参数(Android 用 Kotlin PaymentsClient,Web 用 JS API)。
	GooglePay *GooglePayClientParams `json:"google_pay,omitempty"`
}

// WechatClientParams 微信 APP 支付参数。
//
// Swift(SwiftUI/UIKit):
//
//	req := WXApi.PayReq()
//	req.partnerId = p.partnerID
//	req.prepayId  = p.prepayID
//	req.nonceStr  = p.nonceStr
//	req.timeStamp = UInt32(p.timeStamp)!
//	req.packageValue = p.packageValue
//	req.sign      = p.sign
//	WXApi.requestOrder(req, completion:)
//
// Kotlin:
//
//	api.requestOrder(
//	    appId, partnerId, prepayId, nonceStr, timeStamp, packageValue, sign,
//	    callback
//	)
type WechatClientParams struct {
	// AppID 微信开放平台 AppID(Android 的 requestPay 需要,iOS 由注册时确定)。
	AppID string `json:"app_id"`
	// PartnerID 商户号(MchID)。
	PartnerID string `json:"partner_id"`
	// PrepayID 预支付交易会话标识。
	PrepayID string `json:"prepay_id"`
	// NonceStr 随机字符串。
	NonceStr string `json:"nonce_str"`
	// TimeStamp 时间戳(秒)。
	TimeStamp string `json:"time_stamp"`
	// PackageValue 扩展字段,APP 支付固定为 "Sign=WXPay"。
	PackageValue string `json:"package_value"`
	// Sign 客户端签名。
	Sign string `json:"sign"`
	// OutTradeNo 商户订单号,用于客户端对账。
	OutTradeNo string `json:"out_trade_no"`
	// Amount 订单金额(分)。
	Amount int64 `json:"amount"`
}

// AlipayClientParams 支付宝 APP 支付参数。
//
// Swift:
//
//	ALIPAYClient().payOrder(orderString, scheme: "yourapp", options: nil)
//
// Kotlin:
//
//	AlipayClient(activity).payV2(orderString, callback)
type AlipayClientParams struct {
	// AppID 支付宝应用 ID。
	AppID string `json:"app_id"`
	// OrderString 已签名的请求串,直接交给 SDK 调起。
	OrderString string `json:"order_string"`
	// OutTradeNo 商户订单号。
	OutTradeNo string `json:"out_trade_no"`
	// Amount 订单金额(分)。
	Amount int64 `json:"amount"`
}

// ApplePayClientParams Apple Pay 参数(仅 iOS)。
//
// Swift(PassKit):
//
//	let request = PKPaymentRequest()
//	request.merchantIdentifier = p.merchantID
//	request.countryCode = "CN"          // Apple Pay 结算国家
//	request.currencyCode = p.currency
//	request.merchantCapabilities = [.capability3DS]
//	request.supportedNetworks = [.visa, .masterCard, .amex]
//	request.paymentSummaryItems = [
//	    PKPaymentSummaryItem(label: p.displayName, amount: NSDecimalNumber(value: Double(p.amount) / 100.0))
//	]
//	let controller = PKPaymentAuthorizationController(paymentRequest: request)
//	controller.present { presented in ... }
//
// 拿到 Stripe 的 paymentMethod 后,回到服务端调用 Charge 完成扣款。
type ApplePayClientParams struct {
	// MerchantID Apple Pay 商户标识(merchant.com.example 形式)。
	MerchantID string `json:"merchant_id"`
	// DisplayName 展示给用户的商户名。
	DisplayName string `json:"display_name"`
	// DomainName 商户域名(用于校验支付来源)。
	DomainName string `json:"domain_name"`
	// CountryCode 结算国家码,CNY 一般为 "CN"。
	CountryCode string `json:"country_code"`
	// Currency 币种,默认 CNY。
	Currency string `json:"currency"`
	// Amount 订单金额(分)。
	Amount int64 `json:"amount"`
	// OutTradeNo 商户订单号。
	OutTradeNo string `json:"out_trade_no"`
	// Gateway 收单网关标识(如 stripe)。
	Gateway string `json:"gateway"`
	// PublishableKey 收单网关的可发布密钥。
	PublishableKey string `json:"publishable_key"`
	// ClientSecret 收单网关的客户端令牌,用于服务端确认支付。
	ClientSecret string `json:"client_secret"`
}

// GooglePayClientParams Google Pay 参数。
//
// Android(Kotlin,PaymentsClient):
//
//	val request = PaymentRequest().apply {
//	    gateway = GooglePay(p.gateway, p.publishableKey, p.merchantName)
//	    api = PaymentsApi(baseEncodedPaymentMethod)
//
//	    transactionInfo = TransactionInfo(
//	        currencyCode = p.currency,
//	        totalPrice = PriceDetails(totalPrice = "1.00", currencyCode = p.currency)
//	    )
//	    merchantName = p.merchantName
//	}
//
// Web(Google Pay for Web,JS API):
//
//	const paymentMethod = {
//	    gateway: 'stripe',
//	    'stripe.publishableKey': p.publishableKey,
//	    'stripe.version': '20220801'
//	}
//	const paymentData = {
//	    apiVersion: 2,
//	    apiVersionMinor: 0,
//	    paymentMethodData: { ... },
//	    transactionInfo: { currencyCode, totalPriceStatus: 'FINAL', totalPrice }
//	}
type GooglePayClientParams struct {
	// Gateway 收单网关标识(如 stripe)。
	Gateway string `json:"gateway"`
	// PublishableKey 收单网关的可发布密钥。
	PublishableKey string `json:"publishable_key"`
	// MerchantName 展示给用户的商户名。
	MerchantName string `json:"merchant_name"`
	// MerchantID 钱包商户号。
	MerchantID string `json:"merchant_id"`
	// Currency 币种,默认 CNY。
	Currency string `json:"currency"`
	// Amount 订单金额(分)。
	Amount int64 `json:"amount"`
	// OutTradeNo 商户订单号。
	OutTradeNo string `json:"out_trade_no"`
	// Environment 运行环境,PRODUCTION 或 TEST。
	Environment string `json:"environment"`
	// ClientSecret 收单网关的客户端令牌,用于服务端确认支付。
	ClientSecret string `json:"client_secret"`
}
