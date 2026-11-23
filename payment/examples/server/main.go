// Package main 是移动端支付接入的服务端示例。
//
// 这个示例把 bus/payment 包装成三个 HTTP 接口,移动端(Swift / Kotlin)按固定流程调用:
//
//	POST /api/pay            下单,返回 session(含 client_params,客户端 SDK 直接用)
//	POST /api/wallet/charge  钱包类渠道(Apple Pay / Google Pay)用客户端回传的令牌扣款
//	GET  /api/query          查单
//	POST /api/notify/{渠道}   渠道异步回调(验签 -> 入账)
//
// 运行:
//
//	go run ./payment/examples/server
//
// 环境变量(未配置的渠道会被跳过):
//
//	WECHAT_APP_ID / WECHAT_MCH_ID / WECHAT_SERIAL_NO / WECHAT_PRIVATE_KEY / WECHAT_API_V3_KEY
//	ALIPAY_APP_ID / ALIPAY_PRIVATE_KEY / ALIPAY_PUBLIC_KEY
//	APPLE_MERCHANT_ID / GOOGLE_MERCHANT_ID
//	STRIPE_SECRET_KEY / STRIPE_PUBLISHABLE_KEY / STRIPE_WEBHOOK_SECRET
//	  —— 两个钱包共用一个 Stripe 账户时配这三个就够;
//	     要 Apple Pay / Google Pay 各用各的 Stripe 账户,改配
//	     APPLE_STRIPE_SECRET_KEY / GOOGLE_STRIPE_SECRET_KEY(同名变量优先)
//
//	NOTIFY_BASE 渠道回调地址的域名部分,例如 https://pay.example.com。
//	不配就用示例值 pay.example.com —— 那只是个占位,渠道会真的往那儿发通知,
//	真机联调时务必改成自己能在公网访问到的地址,否则永远收不到回调。
//
// 安全提示:环境变量只是示例的取密钥方式。生产环境请用平台
// (bus/payment/platform)在网页上配置,密钥加密入库、页面不回显、敏感操作二次验证;
// 或者用 KMS / K8s Secret 以文件形式挂载。
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/pay"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/applepay"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
	"github.com/AIntelligenceGame/bus/payment/providers/googlepay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

func main() {
	client, err := pay.New(pay.Config{
		Wechat:    wechatConfig(),
		Alipay:    alipayConfig(),
		ApplePay:  applePayConfig(),
		GooglePay: googlePayConfig(),
	})
	if err != nil {
		log.Fatalf("创建支付客户端失败: %v", err)
	}
	log.Printf("已启用渠道: %v", client.Channels())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		handleIndex(client, w, r)
	})
	mux.HandleFunc("GET /api/channels", func(w http.ResponseWriter, r *http.Request) {
		handleChannels(client, w, r)
	})
	mux.HandleFunc("POST /api/pay", func(w http.ResponseWriter, r *http.Request) {
		handlePay(client, w, r)
	})
	mux.HandleFunc("POST /api/wallet/charge", func(w http.ResponseWriter, r *http.Request) {
		handleCharge(client, w, r)
	})
	mux.HandleFunc("GET /api/query", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(client, w, r)
	})
	mux.HandleFunc("POST /api/notify/{channel}", func(w http.ResponseWriter, r *http.Request) {
		handleNotify(client, w, r)
	})
	// 其它路径给出友好的提示,而不是 Go 默认的 "404 page not found"。
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "接口不存在: " + r.Method + " " + r.URL.Path,
			"hint":  "打开根路径可以看到接口清单",
			"endpoints": []string{
				"GET  /api/channels",
				"POST /api/pay",
				"POST /api/wallet/charge",
				"GET  /api/query?channel=&out_trade_no=",
				"POST /api/notify/{channel}",
			},
		})
	})

	addr := ":8088"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Printf("示例服务已启动: http://127.0.0.1%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

// handleChannels 返回已启用的渠道列表。
func handleChannels(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	channels := client.Channels()
	names := make([]string, 0, len(channels))
	for _, ch := range channels {
		names = append(names, string(ch))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels": names,
		"hint": "没有配置任何密钥渠道时这里是空数组,下单会失败;" +
			"请按 examples/server/main.go 顶部注释设置环境变量后重启",
	})
}

// handleIndex 首页:列出接口,并提供一个小表单直接试下单。
//
// 用浏览器打开 http://127.0.0.1:8088 就能看到。
func handleIndex(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	channels := client.Channels()
	options := ""
	for _, ch := range channels {
		options += `<option value="` + string(ch) + `">` + string(ch) + `</option>`
	}
	warn := ""
	if len(channels) == 0 {
		warn = `<p class="warn">当前没有启用任何渠道(日志里的「已启用渠道: []」)。
		下单会直接失败。按 examples/server/main.go 顶部注释设置环境变量后重启即可。</p>`
	}
	page := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>bus/payment 移动端接入示例</title>
<style>
  body { font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
         max-width: 900px; margin: 40px auto; padding: 0 16px; color: #222; line-height: 1.6; }
  h1 { font-size: 20px; }
  h2 { font-size: 16px; margin-top: 28px; }
  code { background: #f5f5f5; padding: 1px 5px; border-radius: 3px; }
  table { border-collapse: collapse; width: 100%; margin-top: 8px; }
  th, td { border: 1px solid #e0e0e0; padding: 6px 10px; text-align: left; font-size: 14px; }
  th { background: #fafafa; }
  form { margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  input, select, button { padding: 6px 10px; font-size: 14px; }
  pre { background: #f7f7f7; padding: 12px; border-radius: 4px; overflow: auto;
        font-size: 13px; max-height: 420px; }
  .warn { background: #fff7e6; border-left: 3px solid #faad14; padding: 10px 12px; }
</style>
</head>
<body>
<h1>bus/payment 移动端接入示例服务端</h1>
<p class="warn">这不是「支付配置选择平台」的管理控制台,这里只演示给 Swift / Kotlin 客户端调用的最小接口。
   要看管理控制台(渠道配置 / 支付体验 / 订单 / 账户安全),请另起一个进程:<br>
   <code>cd bus &amp;&amp; go run ./payment/examples/platform</code><br>
   然后打开 <code>http://127.0.0.1:8089/</code>。</p>
<p>当前已启用渠道:
   <code>` + strings.Join(channelNames(channels), ", ") + `</code></p>
` + warn + `
<h2>接口</h2>
<table>
  <tr><th>方法</th><th>路径</th><th>说明</th></tr>
  <tr><td>GET</td><td>/api/channels</td><td>已启用渠道</td></tr>
  <tr><td>POST</td><td>/api/pay</td><td>下单,返回 session(含 client_params)</td></tr>
  <tr><td>POST</td><td>/api/wallet/charge</td><td>钱包扣款(Apple Pay / Google Pay)</td></tr>
  <tr><td>GET</td><td>/api/query?channel=&amp;out_trade_no=</td><td>查单</td></tr>
  <tr><td>POST</td><td>/api/notify/{channel}</td><td>渠道异步回调(验签 → 入账)</td></tr>
</table>

<h2>试一下下单</h2>
<form id="f">
  <select name="channel">` + options + `</select>
  <select name="trade_type">
    <option value="app">app(APP 拉起)</option>
    <option value="wallet">wallet(钱包)</option>
    <option value="native">native(扫码)</option>
  </select>
  <input name="amount" value="19900" size="10" title="金额,单位:分">
  <input name="subject" value="示例商品" size="14" title="订单标题">
  <button type="submit">下单</button>
</form>
<pre id="out">响应会显示在这里</pre>

<script>
document.getElementById('f').addEventListener('submit', async function (e) {
  e.preventDefault();
  var out = document.getElementById('out');
  var fd = new FormData(e.target);
  var body = {
    channel: fd.get('channel'),
    trade_type: fd.get('trade_type'),
    amount: Number(fd.get('amount')),
    subject: fd.get('subject')
  };
  out.textContent = '请求中...';
  try {
    var resp = await fetch('/api/pay', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    var text = await resp.text();
    try { out.textContent = JSON.stringify(JSON.parse(text), null, 2); }
    catch (err) { out.textContent = text; }
  } catch (err) {
    out.textContent = '请求失败: ' + err;
  }
});
</script>
</body>
</html>`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(page))
}

// channelNames 把渠道列表转成字符串切片。
func channelNames(channels []payment.Channel) []string {
	names := make([]string, 0, len(channels))
	for _, ch := range channels {
		names = append(names, string(ch))
	}
	return names
}

// payRequest 下单请求。
type payRequest struct {
	// Channel 渠道:wechat / alipay / applepay / googlepay。
	Channel string `json:"channel"`
	// TradeType 交易类型:app(APP 拉起)、wallet(钱包)、native(扫码)。
	// 为空时按渠道自动推断。
	TradeType string `json:"trade_type"`
	// OutTradeNo 商户订单号,留空则自动生成。
	OutTradeNo string `json:"out_trade_no"`
	// Subject 订单标题。
	Subject string `json:"subject"`
	// Amount 金额,单位:分。
	Amount int64 `json:"amount"`
	// PayerID 微信 openid / 支付宝 buyer_id,JSAPI 场景需要。
	PayerID string `json:"payer_id"`
}

// handlePay 下单。
//
// 响应体里的 session.client_params 就是「给客户端 SDK 用的字段」:
// 微信是 wechat、支付宝是 alipay、Apple Pay 是 apple_pay、Google Pay 是 google_pay,
// 其余字段为 null。客户端按自己用的渠道取对应对象即可。
func handlePay(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	var req payRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体解析失败: " + err.Error()})
		return
	}
	if req.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "金额必须大于 0(单位:分)"})
		return
	}
	channel := payment.Channel(req.Channel)
	if channel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "channel 不能为空"})
		return
	}
	if req.OutTradeNo == "" {
		req.OutTradeNo = fmt.Sprintf("BIZ%s%06d", time.Now().Format("20060102"), time.Now().UnixNano()%1000000)
	}
	if req.Subject == "" {
		req.Subject = "商品订单 " + req.OutTradeNo
	}

	order := pay.Order{
		OutTradeNo: req.OutTradeNo,
		Subject:    req.Subject,
		Amount:     req.Amount,
		PayerID:    req.PayerID,
		NotifyURL:  notifyBase() + "/api/notify/" + string(channel),
	}

	tradeType := payment.TradeType(req.TradeType)
	if tradeType == "" {
		tradeType = payment.AutoTradeType(channel, &order)
	}
	session, err := client.Create(r.Context(), channel, tradeType, order)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "发起支付失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_no": order.OutTradeNo,
		"amount":   order.Amount,
		"yuan":     payment.CentsToYuan(order.Amount),
		"session":  session,
	})
}

// chargeRequest 钱包扣款请求。
type chargeRequest struct {
	// Channel 渠道:applepay / googlepay。
	Channel string `json:"channel"`
	// OutTradeNo 商户订单号(与下单时一致)。
	OutTradeNo string `json:"out_trade_no"`
	// PaymentToken 客户端 SDK 返回的支付令牌。
	PaymentToken string `json:"payment_token"`
	// Amount 金额,单位:分。
	Amount int64 `json:"amount"`
}

// handleCharge 钱包扣款:Apple Pay / Google Pay 的最后一步。
//
// 客户端在收银台里输完密码后,拿到的 paymentMethod / PaymentData 就是 payment_token,
// 回传服务端完成真实扣款。
func handleCharge(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	var req chargeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体解析失败: " + err.Error()})
		return
	}
	if req.OutTradeNo == "" || req.PaymentToken == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "out_trade_no 与 payment_token 必填"})
		return
	}
	result, err := client.ChargeWallet(r.Context(), payment.Channel(req.Channel), req.OutTradeNo, req.PaymentToken, req.Amount)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "扣款失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleQuery 查单。
func handleQuery(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	channel := payment.Channel(r.URL.Query().Get("channel"))
	outTradeNo := r.URL.Query().Get("out_trade_no")
	if channel == "" || outTradeNo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "channel 与 out_trade_no 必填"})
		return
	}
	result, err := client.Query(r.Context(), channel, outTradeNo)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "查单失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleNotify 渠道异步回调:验签 -> 入账。
//
// 客户端拿到的「支付成功」只是用户授权信号,最终状态以这里的回调为准,
// 入账务必用 notify.OutTradeNo 做幂等键。
func handleNotify(client *pay.Client, w http.ResponseWriter, r *http.Request) {
	channel := payment.Channel(r.PathValue("channel"))
	notify, err := client.HandleNotify(r.Context(), channel, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "回调验签失败: " + err.Error()})
		return
	}
	log.Printf("收到回调: 渠道=%s 订单=%s 状态=%s 金额=%s",
		notify.Channel, notify.OutTradeNo, notify.Status, payment.FormatCents(notify.Amount))
	if notify.Status == payment.TradeStatusSuccess {
		// TODO: 幂等入账 —— 以 notify.OutTradeNo 为幂等键,重复回调不能重复发货。
		log.Printf("入账成功: %s", notify.OutTradeNo)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// writeJSON 写 JSON 响应。
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	// 不把 & < > 转成 \u00xx,方便人直接看接口返回。
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// notifyBase 渠道回调地址的域名部分。
//
// 渠道(如微信)会真的按这个地址发通知,所以它必须是**公网能访问到**的地址。
// 这里不写死成某个域名:用 NOTIFY_BASE 环境变量配,没配才退回到示例值。
func notifyBase() string {
	if v := strings.TrimRight(os.Getenv("NOTIFY_BASE"), "/"); v != "" {
		return v
	}
	return "https://pay.example.com"
}

// envFirst 依次取第一个非空的环境变量。
//
// 用途:钱包类渠道可以各自一套收单密钥(APPLE_STRIPE_* / GOOGLE_STRIPE_*),
// 也可以两个钱包共用一个 Stripe 账户(只配 STRIPE_*)——
// 前者优先,没配才回退到后面那个。
func envFirst(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// wechatConfig 从环境变量读取微信支付配置,未配置返回 nil。
func wechatConfig() *wechat.Config {
	if os.Getenv("WECHAT_APP_ID") == "" {
		return nil
	}
	return &wechat.Config{
		AppID:          os.Getenv("WECHAT_APP_ID"),
		MchID:          os.Getenv("WECHAT_MCH_ID"),
		SerialNo:       os.Getenv("WECHAT_SERIAL_NO"),
		PrivateKeyPEM:  os.Getenv("WECHAT_PRIVATE_KEY"),
		APIv3Key:       os.Getenv("WECHAT_API_V3_KEY"),
		NotifyURL:      notifyBase() + "/api/notify/wechat",
		AutoFetchCerts: true,
	}
}

// alipayConfig 从环境变量读取支付宝配置,未配置返回 nil。
func alipayConfig() *alipay.Config {
	if os.Getenv("ALIPAY_APP_ID") == "" {
		return nil
	}
	return &alipay.Config{
		AppID:              os.Getenv("ALIPAY_APP_ID"),
		PrivateKeyPEM:      os.Getenv("ALIPAY_PRIVATE_KEY"),
		AlipayPublicKeyPEM: os.Getenv("ALIPAY_PUBLIC_KEY"),
		NotifyURL:          notifyBase() + "/api/notify/alipay",
	}
}

// applePayConfig 从环境变量读取 Apple Pay 配置,未配置返回 nil。
func applePayConfig() *applepay.Config {
	secret := envFirst("APPLE_STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY")
	if secret == "" {
		return nil
	}
	stripe, err := gateway.NewStripe(&gateway.StripeConfig{
		SecretKey:      secret,
		PublishableKey: envFirst("APPLE_STRIPE_PUBLISHABLE_KEY", "STRIPE_PUBLISHABLE_KEY"),
		WebhookSecret:  envFirst("APPLE_STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"),
	})
	if err != nil {
		return nil
	}
	return &applepay.Config{
		MerchantID:  os.Getenv("APPLE_MERCHANT_ID"),
		DisplayName: "示例商城",
		DomainName:  "pay.example.com",
		Gateway:     stripe,
	}
}

// googlePayConfig 从环境变量读取 Google Pay 配置,未配置返回 nil。
func googlePayConfig() *googlepay.Config {
	secret := envFirst("GOOGLE_STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY")
	if secret == "" {
		return nil
	}
	stripe, err := gateway.NewStripe(&gateway.StripeConfig{
		SecretKey:      secret,
		PublishableKey: envFirst("GOOGLE_STRIPE_PUBLISHABLE_KEY", "STRIPE_PUBLISHABLE_KEY"),
		WebhookSecret:  envFirst("GOOGLE_STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"),
	})
	if err != nil {
		return nil
	}
	return &googlepay.Config{
		MerchantID:   os.Getenv("GOOGLE_MERCHANT_ID"),
		MerchantName: "示例商城",
		Gateway:      stripe,
	}
}
