package com.example.payment

import android.os.Bundle
import android.widget.Button
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import org.json.JSONObject

// 把三个渠道串起来的示例页面。
//
// 点击按钮 -> 下单 -> 调起对应 SDK -> 回传令牌(钱包类)-> 查服务端状态。
class PaymentActivity : AppCompatActivity() {
    private val service = PaymentService()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        WechatPay.register(this)

        val root = android.widget.LinearLayout(this).apply {
            orientation = android.widget.LinearLayout.VERTICAL
            setPadding(48, 48, 48, 48)
        }
        root.addView(Button(this).apply {
            text = "微信支付 199.00"
            setOnClickListener { payWechat() }
        })
        root.addView(Button(this).apply {
            text = "支付宝支付 199.00"
            setOnClickListener { payAlipay() }
        })
        root.addView(Button(this).apply {
            text = "Google Pay 199.00"
            setOnClickListener { payGooglePay() }
        })
        setContentView(root)
    }

    /** 微信支付:下单 -> 调起微信 -> 查状态。 */
    private fun payWechat() {
        thread {
            val resp = service.createOrder(channel = "wechat", tradeType = "app", amount = 19900)
            val params = resp.session.clientParams?.wechat ?: return@thread
            runOnUiThread {
                WechatPay.pay(this, params) { error ->
                    runOnUiThread {
                        if (error != null) {
                            toast("微信支付未完成: ${error.message}")
                        } else {
                            // 已授权,最终结果以服务端回调为准,这里查一次给用户即时反馈
                            queryResult(resp.orderNo, "wechat")
                        }
                    }
                }
            }
        }
    }

    /** 支付宝支付:下单 -> 调起支付宝 -> 查状态。 */
    private fun payAlipay() {
        thread {
            val resp = service.createOrder(channel = "alipay", tradeType = "app", amount = 19900)
            val params = resp.session.clientParams?.alipay ?: return@thread
            runOnUiThread {
                AlipayPay.pay(this, params) { result ->
                    if (result?.resultCode == "9000") {
                        queryResult(resp.orderNo, "alipay")
                    } else {
                        toast("支付宝支付未完成: ${result?.subMsg ?: "未知原因"}")
                    }
                }
            }
        }
    }

    /** Google Pay:下单 -> 拉起收银台 -> 回传令牌扣款。 */
    private fun payGooglePay() {
        thread {
            val resp = service.createOrder(channel = "google_pay", tradeType = "wallet", amount = 19900)
            val params = resp.session.clientParams?.googlePay ?: return@thread
            runOnUiThread {
                GooglePay.pay(this, params) { paymentData, error ->
                    if (paymentData == null) {
                        toast("Google Pay 未完成: ${error?.message}")
                        return@pay
                    }
                    // 钱包类渠道:令牌回传服务端,由服务端完成真实扣款
                    thread {
                        try {
                            val result = service.chargeWallet("google_pay", resp.orderNo, paymentData, 19900)
                            runOnUiThread { toast("扣款结果: ${result.status}") }
                        } catch (e: Throwable) {
                            runOnUiThread { toast("扣款失败: ${e.message}") }
                        }
                    }
                }
            }
        }
    }

    /** 查单,给用户一个即时状态。 */
    private fun queryResult(orderNo: String, channel: String) {
        thread {
            try {
                val result = service.query(channel, orderNo)
                runOnUiThread { toast("订单状态: ${result.status}") }
            } catch (e: Throwable) {
                runOnUiThread { toast("查单失败: ${e.message}") }
            }
        }
    }

    private fun toast(msg: String) {
        Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
    }
}

/** 简单起见用线程代替协程;生产项目请用协程 + lifecycleScope。 */
private fun thread(block: () -> Unit) {
    Thread(block).start()
}

/** 参考:paymentData 是 JSON 字符串,回传服务端时原样传即可。 */
@Suppress("unused")
private fun paymentDataPreview(json: String): String = JSONObject(json).toString(2)
