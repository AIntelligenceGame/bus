package com.example.payment

import android.content.Context
import com.tencent.mm.opensdk.openapi.IWXAPI
import com.tencent.mm.opensdk.openapi.WXAPIFactory
import com.tencent.mm.opensdk.modelbase.BaseReq
import com.tencent.mm.opensdk.modelpay.PayReq
import com.tencent.mm.opensdk.openapi.WXApiEventHandler

// 微信 APP 支付(Kotlin + WeChat SDK)。
//
// 完整链路:
//
//	1. 服务端下单拿到 client_params.wechat;
//	2. 组装 PayReq,交给 IWXAPI 调起微信;
//	3. 用户在微信里输密码;
//	4. 收到回调后查服务端状态。
//
// 注意:微信 APP 支付的结果最终以服务端回调为准,客户端回调只用于给用户即时反馈。
object WechatPay {
    /** 微信开放平台 AppID。 */
    const val APP_ID = "wx1234567890abcdef"

    /** 懒加载 IWXAPI(注册在 Application 里调用一次)。 */
    private var api: IWXAPI? = null

    /** 在 Application.onCreate 里调用,完成注册。 */
    fun register(context: Context): IWXAPI {
        val instance = WXAPIFactory.createWXAPI(context.applicationContext, APP_ID, true)
        instance.registerApp(APP_ID)
        api = instance
        return instance
    }

    /** 微信是否已安装。 */
    fun isInstalled(context: Context): Boolean {
        val instance = api ?: register(context)
        return instance.isWXAppInstalled
    }

    /**
     * 用服务端下发的参数调起微信支付。
     *
     * @param params session.client_params.wechat
     * @param onResult 回调,error 为 null 表示用户已授权(最终结果以服务端回调为准)
     */
    fun pay(activity: android.app.Activity, params: WechatParams, onResult: (Throwable?) -> Unit) {
        val instance = api ?: register(activity)
        val req = PayReq()
        req.appId = params.appID
        req.partnerId = params.partnerID
        req.prepayId = params.prepayID
        req.nonceStr = params.nonceStr
        req.timeStamp = params.timeStamp
        req.packageValue = params.packageValue
        req.sign = params.sign

        // 新版 SDK 也可以用位置参数版本:
//	instance.requestOrder(params.appID, params.partnerID, params.prepayID, params.nonceStr,
//	    params.timeStamp, params.packageValue, params.sign) { err -> onResult(err) }

        instance.sendReq(req)
    }

    /**
     * 处理微信回跳(在 WXEntryActivity 的 onResp 里调用)。
     *
     * @return true 表示已处理
     */
    fun handleIntent(intent: android.content.Intent, onResult: (Throwable?) -> Unit): Boolean {
        val api = api ?: return false
        api.handleIntent(intent, object : WXApiEventHandler() {
            override fun onReq(req: BaseReq) {
                // 这里是微信跳回本 App 的请求,不是支付结果
            }

            override fun onResp(resp: com.tencent.mm.opensdk.modelbase.BaseResp) {
                if (resp.type == com.tencent.mm.opensdk.modelpay.PayReq.TYPE) {
                    onResult(if (resp.errCode == 0) null else RuntimeException(resp.errMsg))
                }
            }
        })
        return true
    }
}

// 用法(在 Activity 里):
//
//	WechatPay.register(this)
//	// 下单
//	val resp = PaymentService().createOrder(channel = "wechat", tradeType = "app", amount = 19900)
//	val params = resp.session.clientParams?.wechat ?: return
//	WechatPay.pay(this, params) { err ->
//	    if (err != null) {
//	        // 用户取消或失败
//	    } else {
//	        // 已授权,再查一次服务端状态
//	        PaymentService().query("wechat", resp.orderNo)
//	    }
//	}
//
// AndroidManifest 需要注册 WXEntryActivity:
//
//	<activity android:name=".wxapi.WXEntryActivity" android:exported="true" />
