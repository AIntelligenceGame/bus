package com.example.payment

import android.app.Activity
import com.google.android.gms.wallet.PaymentRequest
import com.google.android.gms.wallet.PaymentsApi
import com.google.android.gms.wallet.PaymentsClient
import com.google.android.gms.wallet.Wallet
import com.google.android.gms.wallet.api.GooglePay as GooglePayApi
import android.util.Base64
import java.math.BigDecimal

// Google Pay 支付(Kotlin + Google Play Services Wallet)。
//
// 与微信/支付宝不同:Google Pay 不在 App 内输密码,而是弹出系统钱包面板,
// 用户用设备密码 / 生物识别授权后,App 拿到一段 paymentData JSON,
// 必须把它回传服务端完成扣款。
//
// 依赖:
//
//	implementation("com.google.android.gms:play-services-wallet:19.2.1")
//
// AndroidManifest 需要声明允许 Google Pay 支付:
//
//	<meta-data android:name="com.google.android.gms.wallet.api.enabled" android:value="true" />
object GooglePay {
    /**
     * 拉起 Google Pay 收银台。
     *
     * @param params session.client_params.google_pay
     * @param onResult 回调,paymentData 非 null 表示用户已授权,
     *                 把它作为 payment_token 回传服务端完成扣款
     */
    fun pay(activity: Activity, params: GooglePayParams, onResult: (String?, Throwable?) -> Unit) {
        val walletOptions = Wallet.getWalletOptions(activity)
        val client = PaymentsClient.create(activity, walletOptions)

        val request = PaymentRequest().apply {
            // Google Pay 的接口版本必须显式声明,漏了运行时会直接抛 IllegalArgumentException。
            apiVersion = 2
            apiVersionMinor = 0
            // 收单网关 + 可发布密钥由服务端下发。
            gateway = GooglePayApi(params.gateway, params.publishableKey, params.merchantName)
            // baseEncodedPaymentMethod:base64url 编码后的**支付方式 JSON**。
            //
            // 注意:不是服务端下发的 client_secret —— client_secret 是
            // 「服务端创建的支付会话令牌」,只能给收单机构用,填到这里 Google 会拒绝。
            // 直连收单机构时这里放卡信息;走 Stripe 之类网关时要用网关 SDK 的产物。
            api = PaymentsApi(baseEncodedPaymentMethod = encodePaymentMethod(sampleCardJSON()))
            transactionInfo = com.google.android.gms.wallet.TransactionInfo(
                currencyCode = params.currency.ifEmpty { "CNY" },
                totalPrice = com.google.android.gms.wallet.PriceDetails(
                    totalPrice = formatAmount(params.amount),
                    currencyCode = params.currency.ifEmpty { "CNY" }
                )
            )
            merchantName = params.merchantName
        }

        client.loadPaymentData(request).addOnCompleteListener { task ->
            val data = task.result
            if (task.isSuccessful && data != null) {
                // 拿到 paymentData,回传服务端扣款。
                //
                // 直连收单机构:整段 paymentData JSON 就是令牌(里面含卡 token)。
                // 走 Stripe 之类网关:要用网关 SDK 换成 PaymentMethod id(pm_xxx)再回传。
                onResult(data.toJson(), null)
            } else {
                onResult(null, task.exception ?: RuntimeException("Google Pay 未完成"))
            }
        }
    }

    /**
     * 把「分」转成 Google Pay 要的小数字符串(单位:元)。
     *
     * 19900 -> "199.00"
     */
    fun formatAmount(cents: Long): String =
        BigDecimal.valueOf(cents, 2).toPlainString()

    /**
     * base64url 编码支付方式 JSON(Google Pay 要 base64url,不要 padding)。
     */
    fun encodePaymentMethod(json: String): String =
        Base64.encodeToString(
            json.toByteArray(),
            Base64.URL_SAFE or Base64.NO_PADDING or Base64.NO_WRAP
        )

    /**
     * 示例用的卡支付方式 JSON。
     *
     * 真机上应该由收银台收集卡信息后生成(或直接用 Google Pay 弹窗返回的
     * paymentMethodData),这里给一个占位值,方便看字段长什么样。
     */
    private fun sampleCardJSON(): String =
        """{"type":"card","name":"Test Card","last4":"1111"}"""
}

// 用法:
//
//	val resp = PaymentService().createOrder(channel = "google_pay", tradeType = "wallet", amount = 19900)
//	val params = resp.session.clientParams?.googlePay ?: return
//	GooglePay.pay(this, params) { paymentData, error ->
//	    if (paymentData != null) {
//	        // 回传服务端完成扣款
//	        PaymentService().chargeWallet("google_pay", resp.orderNo, paymentData, 19900)
//	    } else {
//	        // 用户取消或失败
//	    }
//	}
//
// PaymentData 的 toJson() 来自 com.google.android.gms.wallet.PaymentData,
// 上面的 loadPaymentData 回调里拿到的就是它。
