package com.example.payment

import com.google.gson.annotations.SerializedName

// 支付会话(对应服务端返回的 session)。
//
// 服务端 bus/payment 下发的 JSON 结构:
//
//	{
//	  "channel": "wechat",
//	  "trade_type": "app",
//	  "out_trade_no": "BIZ20260101000001",
//	  "client_params": { "wechat": { ... }, "alipay": null, "apple_pay": null, "google_pay": null }
//	}
data class PaySession(
    val channel: String,
    @SerializedName("trade_type") val tradeType: String,
    @SerializedName("out_trade_no") val outTradeNo: String,
    @SerializedName("prepay_id") val prepayID: String? = null,
    val qrcode: String? = null,
    @SerializedName("redirect_url") val redirectURL: String? = null,
    val token: String? = null,
    @SerializedName("client_params") val clientParams: ClientParams? = null
)

// ClientParams 面向客户端 SDK 的结构化调起参数。
//
// 服务端已按各 SDK 的字段名填好,客户端只做「取字段 → 塞进 SDK」。
data class ClientParams(
    val wechat: WechatParams? = null,
    val alipay: AlipayParams? = null,
    @SerializedName("apple_pay") val applePay: ApplePayParams? = null,
    @SerializedName("google_pay") val googlePay: GooglePayParams? = null
)

// WechatParams 微信 APP 支付参数。
data class WechatParams(
    @SerializedName("app_id") val appID: String,
    @SerializedName("partner_id") val partnerID: String,
    @SerializedName("prepay_id") val prepayID: String,
    @SerializedName("nonce_str") val nonceStr: String,
    @SerializedName("time_stamp") val timeStamp: String,
    @SerializedName("package_value") val packageValue: String,
    val sign: String,
    @SerializedName("out_trade_no") val outTradeNo: String,
    val amount: Long
)

// AlipayParams 支付宝 APP 支付参数。
data class AlipayParams(
    @SerializedName("app_id") val appID: String,
    @SerializedName("order_string") val orderString: String,
    @SerializedName("out_trade_no") val outTradeNo: String,
    val amount: Long
)

// ApplePayParams Apple Pay 参数(仅 iOS,Android 不用)。
data class ApplePayParams(
    @SerializedName("merchant_id") val merchantID: String,
    @SerializedName("display_name") val displayName: String,
    @SerializedName("domain_name") val domainName: String,
    @SerializedName("country_code") val countryCode: String,
    val currency: String,
    val amount: Long,
    @SerializedName("out_trade_no") val outTradeNo: String,
    val gateway: String,
    @SerializedName("publishable_key") val publishableKey: String,
    @SerializedName("client_secret") val clientSecret: String
)

// GooglePayParams Google Pay 参数。
data class GooglePayParams(
    val gateway: String,
    @SerializedName("publishable_key") val publishableKey: String,
    @SerializedName("merchant_name") val merchantName: String,
    @SerializedName("merchant_id") val merchantID: String,
    val currency: String,
    val amount: Long,
    @SerializedName("out_trade_no") val outTradeNo: String,
    val environment: String,
    @SerializedName("client_secret") val clientSecret: String
)

// 下单请求。
data class PayRequest(
    val channel: String,
    @SerializedName("trade_type") val tradeType: String? = null,
    @SerializedName("out_trade_no") val outTradeNo: String? = null,
    val subject: String? = null,
    /** 金额,单位:分。 */
    val amount: Long,
    @SerializedName("payer_id") val payerID: String? = null
)

// 下单响应。
data class PayResponse(
    @SerializedName("order_no") val orderNo: String,
    val amount: Long,
    val yuan: String,
    val session: PaySession
)

// 钱包扣款请求(Google Pay)。
data class ChargeRequest(
    val channel: String,
    @SerializedName("out_trade_no") val outTradeNo: String,
    @SerializedName("payment_token") val paymentToken: String,
    val amount: Long
)

// 扣款响应。
data class ChargeResponse(
    @SerializedName("out_trade_no") val outTradeNo: String,
    @SerializedName("transaction_id") val transactionID: String? = null,
    val amount: Long,
    val currency: String? = null,
    val status: String,
    val paid: Boolean
)

/**
 * 查单响应。
 *
 * 注意:查单返回的是服务端统一交易结果(payment.TradeResult),**没有** `paid` 字段
 * —— `paid` 只有「钱包扣款」接口才有。所以查单必须用这个模型。
 * 用 Gson 解 ChargeResponse 不会报错,但 `paid` 会永远是 false(缺字段取默认值),
 * 很容易被误当成「没支付成功」。判断是否已支付看 status == "SUCCESS"。
 */
data class QueryResponse(
    val channel: String,
    @SerializedName("out_trade_no") val outTradeNo: String,
    @SerializedName("transaction_id") val transactionID: String? = null,
    val amount: Long,
    val currency: String? = null,
    val status: String
) {
    /** 是否已支付。 */
    val isPaid: Boolean get() = status == "SUCCESS"
}
