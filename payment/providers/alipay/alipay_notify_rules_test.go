package alipay

import (
	"context"
	"crypto/rsa"
	"net/http"
	"net/url"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// signNotify 按**官方验签规则**(剔除 sign 与 sign_type)给通知参数签名。
//
// 支付宝发来的异步通知里的 sign,就是用这条规则算出来的
// (官方 SDK 的 getSignCheckContentV1 / 支付宝 verifyV1)。
func signNotify(t *testing.T, priv *rsa.PrivateKey, params map[string]string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	sig, err := payment.RSA2Sign(priv, verifyContent(out))
	if err != nil {
		t.Fatalf("通知签名失败: %v", err)
	}
	out["sign"] = sig
	return out
}

// TestNotifyWithSignType 通知里带 sign_type=RSA2 时必须能验过。
//
// 官方验签规则:sign 与 sign_type 都不参与验签。
// 如果这里把 sign_type 算进去,支付宝发来的通知就一律验不过,
// 订单会永远停在「待支付」—— 商户的钱收了,平台却不知道。
func TestNotifyWithSignType(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"app_id":       "2021000000000001",
		"out_trade_no": "ORDER-20260101002",
		"trade_no":     "2026010122001499880002",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "199.00",
		"sign_type":    "RSA2",
	}
	signed := signNotify(t, priv, params)
	form := url.Values{}
	for k, v := range signed {
		form.Set(k, v)
	}

	notify, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost,
		URL:    "/notify/alipay/u1",
		Body:   []byte(form.Encode()),
	})
	if err != nil {
		t.Fatalf("带 sign_type 的通知竟然验签失败: %v", err)
	}
	if notify.OutTradeNo != "ORDER-20260101002" {
		t.Fatalf("商户单号不对: %q", notify.OutTradeNo)
	}
	if notify.Amount != 19900 {
		t.Fatalf("金额不对: %d", notify.Amount)
	}
}

// TestNotifyForeignAppID 通知里的 app_id 不是自己的应用时必须拒绝。
//
// 官方异步通知处理步骤:验签之后要校验 app_id 是否为商户自身。
func TestNotifyForeignAppID(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub, cfg: &Config{AppID: "2021000000000001"}}
	params := map[string]string{
		"app_id":       "2021999999999999",
		"out_trade_no": "ORDER-20260101003",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "199.00",
	}
	signed := signNotify(t, priv, params)
	form := url.Values{}
	for k, v := range signed {
		form.Set(k, v)
	}
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost,
		Body:   []byte(form.Encode()),
	}); err == nil {
		t.Fatalf("别人应用的交易通知竟然被接收了")
	}
}

// TestRequestSignIncludesSignType 请求签名必须把 sign_type 算进去。
//
// 官方《签名》:剔除 sign 字段、剔除值为空的参数 —— sign_type 是公共参数,参与签名。
// 官方 SDK 的请求签名链路(AbstractAlipayClient → getSignatureContent → getSignContent)
// 走的正是这条路,而 getSignContent 只剔除空值。
//
// 少了 sign_type,支付宝网关算出的待签名串与我们算的不一样,
// 每次请求都回 40002 / isv.invalid-signature —— 支付宝渠道彻底不可用。
func TestRequestSignIncludesSignType(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"app_id":      "2021000000000001",
		"method":      "alipay.trade.app.pay",
		"format":      "JSON",
		"charset":     "utf-8",
		"sign_type":   "RSA2",
		"timestamp":   "2026-01-01 10:00:00",
		"version":     "1.0",
		"biz_content": `{"out_trade_no":"ORDER-1","total_amount":"199.00"}`,
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	// 用官方的请求规则验签:sign_type 参与。
	if err := payment.RSA2Verify(pub, signContent(params), signed["sign"]); err != nil {
		t.Fatalf("按官方请求规则验签失败: %v", err)
	}
	// 若把 sign_type 排除掉,验签必须不通过 —— 说明这条规则是有约束力的。
	if err := payment.RSA2Verify(pub, verifyContent(params), signed["sign"]); err == nil {
		t.Fatalf("剔除 sign_type 的验签串竟然也验过了,说明规则没起到作用")
	}
}
