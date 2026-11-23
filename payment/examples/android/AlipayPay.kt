package com.example.payment

import android.app.Activity
import com.alipay.sdk.pay.AlipayClient
import com.alipay.sdk.pay.AlipayResult

// 支付宝 APP 支付(Kotlin + Alipay SDK)。
//
// 支付宝客户端支付不需要在本 App 里处理密码,用户是在支付宝 App / WebView 里完成,
// 本 App 只需要:
//  1. 把服务端下发并签好名的 order_string 交给支付宝 SDK;
//  2. 在回调里拿到结果,再查服务端状态。
object AlipayPay {
    /**
     * 调起支付宝支付。
     *
     * @param params session.client_params.alipay
     * @param onResult 回调,result 非 null 且 resultCode == "9000" 表示用户已授权
     */
    fun pay(activity: Activity, params: AlipayParams, onResult: (AlipayResult?) -> Unit) {
        val client = AlipayClient(activity)
        client.payV2(params.orderString, object : AlipayClient.Callback {
            override fun onResult(result: AlipayResult?) {
                activity.runOnUiThread { onResult(result) }
            }
        })
    }
}

// 用法:
//
//	val resp = PaymentService().createOrder(channel = "alipay", tradeType = "app", amount = 19900)
//	val params = resp.session.clientParams?.alipay ?: return
//	AlipayPay.pay(this, params) { result ->
//	    if (result?.resultCode == "9000") {
//	        // 已授权,最终结果以服务端回调为准
//	        PaymentService().query("alipay", resp.orderNo)
//	    } else {
//	        // result?.subMsg 说明失败原因
//	    }
//	}
//
// AndroidManifest 需要配置支付宝返回的 Activity(与开放平台里的 scheme 一致):
//
//	<activity android:name="com.alipay.sdk.app.PayActivity" android:exported="true" />
