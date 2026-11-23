import Foundation

// 支付会话(对应服务端返回的 session)。
//
// 服务端 bus/payment 下发的 JSON 结构:
//
//	{
//	  "channel": "wechat",
//	  "trade_type": "app",
//	  "out_trade_no": "BIZ20260101000001",
//	  "client_params": { "wechat": { ... }, "alipay": null, "apple_pay": null, "google_pay": null },
//	  "app_params": { ... }        // 旧版扁平参数,新代码请用 client_params
//	}
struct PaySession: Decodable {
    let channel: String
    let tradeType: String
    let outTradeNo: String
    let prepayID: String?
    let qrcode: String?
    let redirectURL: String?
    let token: String?
    let clientParams: ClientParams?

    enum CodingKeys: String, CodingKey {
        case channel
        case tradeType = "trade_type"
        case outTradeNo = "out_trade_no"
        case prepayID = "prepay_id"
        case qrcode
        case redirectURL = "redirect_url"
        case token
        case clientParams = "client_params"
    }
}

// ClientParams 面向客户端 SDK 的结构化调起参数。
//
// 服务端已经按各 SDK 的字段名填好了,客户端只做「取字段 → 塞进 SDK」这一步,
// 不用自己猜字段名。
struct ClientParams: Decodable {
    let wechat: WechatParams?
    let alipay: AlipayParams?
    let applePay: ApplePayParams?
    let googlePay: GooglePayParams?

    enum CodingKeys: String, CodingKey {
        case wechat
        case alipay
        case applePay = "apple_pay"
        case googlePay = "google_pay"
    }
}

// WechatParams 微信 APP 支付参数。
struct WechatParams: Decodable {
    let appID: String
    let partnerID: String
    let prepayID: String
    let nonceStr: String
    let timeStamp: String
    let packageValue: String
    let sign: String
    let outTradeNo: String
    let amount: Int64

    enum CodingKeys: String, CodingKey {
        case appID = "app_id"
        case partnerID = "partner_id"
        case prepayID = "prepay_id"
        case nonceStr = "nonce_str"
        case timeStamp = "time_stamp"
        case packageValue = "package_value"
        case sign
        case outTradeNo = "out_trade_no"
        case amount
    }
}

// AlipayParams 支付宝 APP 支付参数。
struct AlipayParams: Decodable {
    let appID: String
    let orderString: String
    let outTradeNo: String
    let amount: Int64

    enum CodingKeys: String, CodingKey {
        case appID = "app_id"
        case orderString = "order_string"
        case outTradeNo = "out_trade_no"
        case amount
    }
}

// ApplePayParams Apple Pay 参数(仅 iOS)。
struct ApplePayParams: Decodable {
    let merchantID: String
    let displayName: String
    let domainName: String
    let countryCode: String
    let currency: String
    let amount: Int64
    let outTradeNo: String
    let gateway: String
    let publishableKey: String
    let clientSecret: String

    enum CodingKeys: String, CodingKey {
        case merchantID = "merchant_id"
        case displayName = "display_name"
        case domainName = "domain_name"
        case countryCode = "country_code"
        case currency
        case amount
        case outTradeNo = "out_trade_no"
        case gateway
        case publishableKey = "publishable_key"
        case clientSecret = "client_secret"
    }
}

// GooglePayParams Google Pay 参数。
//
// iOS 上 Google Pay 一般走 Web(PKPaymentButton 不支持 Google Pay),
// 参数用于内嵌 WKWebView 里的 Google Pay JS API。
struct GooglePayParams: Decodable {
    let gateway: String
    let publishableKey: String
    let merchantName: String
    let merchantID: String
    let currency: String
    let amount: Int64
    let outTradeNo: String
    let environment: String
    let clientSecret: String

    enum CodingKeys: String, CodingKey {
        case gateway
        case publishableKey = "publishable_key"
        case merchantName = "merchant_name"
        case merchantID = "merchant_id"
        case currency
        case amount
        case outTradeNo = "out_trade_no"
        case environment
        case clientSecret = "client_secret"
    }
}

// 下单请求。
struct PayRequest: Encodable {
    let channel: String
    let tradeType: String?
    let outTradeNo: String?
    let subject: String?
    /// 金额,单位:分。
    let amount: Int64
    /// 微信 openid / 支付宝 buyer_id(JSAPI 场景需要)。
    let payerID: String?

    enum CodingKeys: String, CodingKey {
        case channel
        case tradeType = "trade_type"
        case outTradeNo = "out_trade_no"
        case subject
        case amount
        case payerID = "payer_id"
    }
}

// 下单响应。
struct PayResponse: Decodable {
    let orderNo: String
    let amount: Int64
    let yuan: String
    let session: PaySession

    enum CodingKeys: String, CodingKey {
        case orderNo = "order_no"
        case amount
        case yuan
        case session
    }
}

// 钱包扣款请求(Apple Pay / Google Pay)。
struct ChargeRequest: Encodable {
    let channel: String
    let outTradeNo: String
    let paymentToken: String
    let amount: Int64

    enum CodingKeys: String, CodingKey {
        case channel
        case outTradeNo = "out_trade_no"
        case paymentToken = "payment_token"
        case amount
    }
}

// 扣款响应。
struct ChargeResponse: Decodable {
    let outTradeNo: String
    let transactionID: String?
    let amount: Int64
    let currency: String?
    let status: String
    let paid: Bool

    enum CodingKeys: String, CodingKey {
        case outTradeNo = "out_trade_no"
        case transactionID = "transaction_id"
        case amount
        case currency
        case status
        case paid
    }
}

// 查单响应。
//
// 注意:查单返回的是服务端统一交易结果(payment.TradeResult),**没有** `paid` 字段
// —— `paid` 只有「钱包扣款」接口才有。所以查单必须用这个模型,
// 拿 ChargeResponse 去解会直接抛 Decodable 错误。
//
// 是否已支付看 status == "SUCCESS"。
struct QueryResponse: Decodable {
    let channel: String
    let outTradeNo: String
    let transactionID: String?
    let amount: Int64
    let currency: String?
    let status: String

    enum CodingKeys: String, CodingKey {
        case channel
        case outTradeNo = "out_trade_no"
        case transactionID = "transaction_id"
        case amount
        case currency
        case status
    }

    /// 是否已支付(服务端只向前走状态,SUCCESS 即已支付)。
    var isPaid: Bool { status == "SUCCESS" }
}
