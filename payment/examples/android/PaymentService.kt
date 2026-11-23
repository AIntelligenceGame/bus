package com.example.payment

import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.concurrent.TimeUnit

// 支付服务端接口封装。
//
// 对应 examples/server 提供的三个接口:
//
//	POST /api/pay            下单
//	POST /api/wallet/charge  钱包扣款
//	GET  /api/query          查单
class PaymentService(
    // 默认值对应 `go run ./payment/examples/server`(监听 8088)。
    // 真机调试要换成电脑在局域网里的地址,比如 "http://192.168.1.10:8088"。
    private val baseURL: String = "http://127.0.0.1:8088",
    private val authToken: String = ""
) {
    private val gson = Gson()
    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    /** 下单,返回含 client_params 的支付会话。 */
    fun createOrder(
        channel: String,
        tradeType: String? = null,
        outTradeNo: String? = null,
        subject: String? = null,
        amount: Long,
        payerID: String? = null
    ): PayResponse {
        val body = PayRequest(channel, tradeType, outTradeNo, subject, amount, payerID)
        val json = post("/api/pay", gson.toJson(body))
        return parse(json)
    }

    /** 用客户端 SDK 回传的支付令牌完成扣款(Google Pay)。 */
    fun chargeWallet(channel: String, outTradeNo: String, paymentToken: String, amount: Long): ChargeResponse {
        val body = ChargeRequest(channel, outTradeNo, paymentToken, amount)
        val json = post("/api/wallet/charge", gson.toJson(body))
        return parse(json)
    }

    /**
     * 查单。
     *
     * 客户端显示的「支付成功」只是授权信号,最终状态以服务端回调为准,
     * 这里查到的结果用于给用户一个即时反馈。
     */
    fun query(channel: String, outTradeNo: String): QueryResponse {
        val url = "$baseURL/api/query?channel=$channel&out_trade_no=$outTradeNo"
        val request = Request.Builder().url(url).get().apply {
            if (authToken.isNotEmpty()) header("Authorization", authToken)
        }.build()
        return parse(execute(request))
    }

    private fun post(path: String, json: String): String {
        val request = Request.Builder()
            .url("$baseURL$path")
            .post(json.toRequestBody("application/json".toMediaType()))
            .apply { if (authToken.isNotEmpty()) header("Authorization", authToken) }
            .build()
        return execute(request)
    }

    private fun execute(request: Request): String {
        client.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            if (!response.isSuccessful) {
                throw IOException("接口返回 ${response.code}: $text")
            }
            return text
        }
    }

    private inline fun <reified T> parse(json: String): T =
        try {
            gson.fromJson(json, T::class.java)
        } catch (e: JsonSyntaxException) {
            throw IOException("响应解析失败: $json", e)
        }
}
