package alipay

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/url"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// testKeyPair 生成测试用 RSA 密钥对。
func testKeyPair(t *testing.T) (*rsa.PrivateKey, *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成测试私钥失败: %v", err)
	}
	return key, &key.PublicKey
}

// privateKeyPEM 把私钥转成 PKCS#1 PEM。
func privateKeyPEM(key *rsa.PrivateKey) string {
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}))
}

// publicKeyPEM 把公钥转成 PKIX PEM。
func publicKeyPEM(pub *rsa.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return ""
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

// TestSignContent 请求签名串:参数按 key 升序,sign 与空值不参与,**sign_type 参与**。
//
// 依据官方《签名》(opendocs.alipay.com/apis/02khjm)与官方 SDK 的请求签名链路
// (AbstractAlipayClient → getSignatureContent → getSignContent):剔除 sign 与空值,
// sign_type 作为公共参数参与签名。
func TestSignContent(t *testing.T) {
	params := map[string]string{
		"app_id":      "2021000000000001",
		"method":      "alipay.trade.create",
		"sign":        "should-be-ignored",
		"sign_type":   "RSA2",
		"timestamp":   "2026-01-01 10:00:00",
		"empty_param": "",
		"charset":     "utf-8",
		"version":     "1.0",
		"notify_url":  "https://pay.example.com/notify/alipay",
		"biz_content": `{"out_trade_no":"ORDER-1"}`,
	}
	got := signContent(params)
	want := "app_id=2021000000000001&biz_content={\"out_trade_no\":\"ORDER-1\"}&" +
		"charset=utf-8&method=alipay.trade.create&notify_url=https://pay.example.com/notify/alipay&" +
		"sign_type=RSA2&timestamp=2026-01-01 10:00:00&version=1.0"
	if got != want {
		t.Fatalf("签名串不对:\n got: %s\nwant: %s", got, want)
	}
}

// TestVerifyContent 验签串:sign 与 sign_type 都不参与。
//
// 官方 SDK 的 getSignCheckContentV1(支付宝 verifyV1 用它)就是剔除这两个。
func TestVerifyContent(t *testing.T) {
	params := map[string]string{
		"app_id":      "2021000000000001",
		"method":      "alipay.trade.create",
		"sign":        "should-be-ignored",
		"sign_type":   "RSA2",
		"timestamp":   "2026-01-01 10:00:00",
		"empty_param": "",
		"charset":     "utf-8",
		"version":     "1.0",
		"notify_url":  "https://pay.example.com/notify/alipay",
		"biz_content": `{"out_trade_no":"ORDER-1"}`,
	}
	got := verifyContent(params)
	want := "app_id=2021000000000001&biz_content={\"out_trade_no\":\"ORDER-1\"}&" +
		"charset=utf-8&method=alipay.trade.create&notify_url=https://pay.example.com/notify/alipay&" +
		"timestamp=2026-01-01 10:00:00&version=1.0"
	if got != want {
		t.Fatalf("验签串不对:\n got: %s\nwant: %s", got, want)
	}
}

// TestSignParamsAndVerify 签出来的签名必须能用支付宝公钥验过。
func TestSignParamsAndVerify(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"app_id":      "2021000000000001",
		"method":      "alipay.trade.create",
		"timestamp":   "2026-01-01 10:00:00",
		"charset":     "utf-8",
		"version":     "1.0",
		"sign_type":   "RSA2",
		"biz_content": `{"out_trade_no":"ORDER-1","total_amount":"199.00"}`,
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	if signed["sign"] == "" {
		t.Fatalf("没有生成 sign")
	}
	if err := payment.RSA2Verify(pub, signContent(params), signed["sign"]); err != nil {
		t.Fatalf("验签失败: %v", err)
	}
	// 改一个业务参数 → 验签必须失败。
	tampered := make(map[string]string, len(params))
	for k, v := range params {
		tampered[k] = v
	}
	tampered["biz_content"] = `{"out_trade_no":"ORDER-2","total_amount":"1.00"}`
	if err := payment.RSA2Verify(pub, signContent(tampered), signed["sign"]); err == nil {
		t.Fatalf("被篡改的参数竟然验签通过")
	}
}

// TestVerifyParams 支付宝公钥验签(含缺 sign 的情况)。
func TestVerifyParams(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"out_trade_no": "ORDER-1",
		"trade_no":     "2026010122001",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "199.00",
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	if err := p.verifyParams(signed); err != nil {
		t.Fatalf("合法回调验签失败: %v", err)
	}
	// 缺 sign。
	if err := p.verifyParams(map[string]string{"out_trade_no": "ORDER-1"}); err == nil {
		t.Fatalf("缺 sign 的回调竟然验签通过")
	}
	// 篡改金额。
	bad := map[string]string{}
	for k, v := range signed {
		bad[k] = v
	}
	bad["total_amount"] = "1.00"
	if err := p.verifyParams(bad); err == nil {
		t.Fatalf("金额被改的回调竟然验签通过")
	}
}

// TestParseNotify 异步通知:表单解析 + 验签 + 字段映射。
func TestParseNotify(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"app_id":       "2021000000000001",
		"out_trade_no": "ORDER-20260101001",
		"trade_no":     "2026010122001499880001",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "199.00",
		"gmt_payment":  "2026-01-01 10:00:00",
		"subject":      "iPhone 17 Pro",
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	form := url.Values{}
	for k, v := range signed {
		form.Set(k, v)
	}
	body := form.Encode()

	notify, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost,
		URL:    "/notify/alipay/u1",
		Body:   []byte(body),
	})
	if err != nil {
		t.Fatalf("解析回调失败: %v", err)
	}
	if notify.OutTradeNo != "ORDER-20260101001" {
		t.Fatalf("商户单号不对: %q", notify.OutTradeNo)
	}
	if notify.TransactionID != "2026010122001499880001" {
		t.Fatalf("支付宝交易号不对: %q", notify.TransactionID)
	}
	if notify.Amount != 19900 {
		t.Fatalf("金额不对(应为 19900 分): %d", notify.Amount)
	}
	if notify.Status != payment.TradeStatusSuccess {
		t.Fatalf("状态不对: %v", notify.Status)
	}
	if notify.Channel != payment.ChannelAlipay {
		t.Fatalf("渠道不对: %v", notify.Channel)
	}

	// 篡改表单里的金额 → 必须验签失败。
	badForm := url.Values{}
	for k, v := range signed {
		badForm.Set(k, v)
	}
	badForm.Set("total_amount", "1.00")
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost, Body: []byte(badForm.Encode()),
	}); err == nil {
		t.Fatalf("金额被改的回调竟然解析成功")
	}

	// 缺 sign → 拒绝。
	noSign := url.Values{}
	for k, v := range params {
		noSign.Set(k, v)
	}
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost, Body: []byte(noSign.Encode()),
	}); err == nil {
		t.Fatalf("缺 sign 的回调竟然解析成功")
	}
}

// TestVerifyNotify 同步回单单独验签。
func TestVerifyNotify(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"out_trade_no": "ORDER-1",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "199.00",
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	ok, err := p.VerifyNotify(&payment.NotifyRequest{
		Method: http.MethodGet,
		Query:  signed,
	})
	if err != nil || !ok {
		t.Fatalf("合法回单验签失败: ok=%v err=%v", ok, err)
	}
	ok, err = p.VerifyNotify(&payment.NotifyRequest{
		Method: http.MethodGet,
		Query:  map[string]string{"out_trade_no": "ORDER-1"},
	})
	if err == nil || ok {
		t.Fatalf("缺 sign 的回单竟然验签通过")
	}
}

// TestNewProvider 配置与公钥加载。
func TestNewProvider(t *testing.T) {
	priv, pub := testKeyPair(t)
	cfg := &Config{
		AppID:              "2021000000000001",
		PrivateKeyPEM:      privateKeyPEM(priv),
		AlipayPublicKeyPEM: publicKeyPEM(pub),
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	if p.Channel() != payment.ChannelAlipay {
		t.Fatalf("渠道不对: %v", p.Channel())
	}
	if cfg.Gateway != DefaultGateway {
		t.Fatalf("默认网关不对: %s", cfg.Gateway)
	}
	if cfg.SignType != "RSA2" {
		t.Fatalf("默认签名算法不对: %s", cfg.SignType)
	}
	// 缺公钥必须报错。
	bad := *cfg
	bad.AlipayPublicKeyPEM = ""
	if _, err := New(&bad); err == nil {
		t.Fatalf("缺支付宝公钥竟然创建成功")
	}
	// 非 RSA2 签名算法必须报错。
	bad2 := *cfg
	bad2.SignType = "MD5"
	if _, err := New(&bad2); err == nil {
		t.Fatalf("非 RSA2 签名算法竟然创建成功")
	}
}

// TestParseNotifyBodyFormAndJSON 同时兼容表单与 JSON 两种通知体。
func TestParseNotifyBodyFormAndJSON(t *testing.T) {
	priv, pub := testKeyPair(t)
	p := &Provider{privKey: priv, alipayPub: pub}
	params := map[string]string{
		"out_trade_no": "ORDER-JSON",
		"trade_status": "TRADE_SUCCESS",
		"total_amount": "1.00",
	}
	signed, err := p.signParams(params)
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	notify, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost,
		Body:   []byte(`{"out_trade_no":"ORDER-JSON","trade_status":"TRADE_SUCCESS","total_amount":"1.00","sign":"` + signed["sign"] + `"}`),
	})
	if err != nil {
		t.Fatalf("JSON 通知体解析失败: %v", err)
	}
	if notify.OutTradeNo != "ORDER-JSON" {
		t.Fatalf("商户单号不对: %q", notify.OutTradeNo)
	}
}
