package wechat

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// testPrivateKey 生成一把测试用 RSA 私钥(2048 位)。
func testPrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成测试私钥失败: %v", err)
	}
	return key
}

// testPrivateKeyPEM 生成测试用私钥的 PKCS#1 PEM。
func testPrivateKeyPEM(t *testing.T) (string, *rsa.PrivateKey) {
	t.Helper()
	key := testPrivateKey(t)
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})), key
}

// testPlatformCert 生成一张自签「平台证书」(回调验签用)。
func testPlatformCert(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key := testPrivateKey(t)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(20260101),
		Subject:      pkix.Name{CommonName: "wechatpay platform test cert"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("生成测试证书失败: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("解析测试证书失败: %v", err)
	}
	return cert, key
}

// parseAuthHeader 解析 WECHATPAY2-SHA256-RSA2048 请求头。
func parseAuthHeader(t *testing.T, header string) map[string]string {
	t.Helper()
	const prefix = "WECHATPAY2-SHA256-RSA2048 "
	if !strings.HasPrefix(header, prefix) {
		t.Fatalf("Authorization 头格式不对: %q", header)
	}
	out := map[string]string{}
	for _, part := range strings.Split(strings.TrimPrefix(header, prefix), ",") {
		part = strings.TrimSpace(part)
		idx := strings.Index(part, "=")
		if idx < 0 {
			continue
		}
		key := part[:idx]
		value := strings.Trim(part[idx+1:], `"`)
		out[key] = value
	}
	return out
}

// TestAuthorizationHeaderVerifiable 请求头里的签名必须能被公钥验证。
func TestAuthorizationHeaderVerifiable(t *testing.T) {
	_, key := testPrivateKeyPEM(t)
	p := &Provider{
		cfg:     &Config{MchID: "1900000109", SerialNo: "SERIAL1"},
		privKey: key,
	}
	body := []byte(`{"appid":"wx123","description":"测试"}`)
	requestURL := "/v3/pay/transactions/native?foo=bar"
	auth, err := p.authorization(http.MethodPost, requestURL, body)
	if err != nil {
		t.Fatalf("生成 Authorization 失败: %v", err)
	}
	fields := parseAuthHeader(t, auth)
	if fields["mchid"] != "1900000109" {
		t.Fatalf("mchid 不对: %q", fields["mchid"])
	}
	if fields["serial_no"] != "SERIAL1" {
		t.Fatalf("serial_no 不对: %q", fields["serial_no"])
	}
	if fields["timestamp"] == "" || fields["nonce_str"] == "" {
		t.Fatalf("timestamp / nonce_str 为空: %+v", fields)
	}
	sig, err := base64.StdEncoding.DecodeString(fields["signature"])
	if err != nil {
		t.Fatalf("签名不是合法 base64: %v", err)
	}
	msg := buildAuthMessage(http.MethodPost, requestURL, fields["timestamp"], fields["nonce_str"], string(body))
	if err := payment.RSAVerifySHA256(&key.PublicKey, []byte(msg), sig); err != nil {
		t.Fatalf("签名验不过: %v", err)
	}
	// 改一个字节就必须验不过。
	tampered := buildAuthMessage(http.MethodPost, requestURL, fields["timestamp"], fields["nonce_str"], string(body)+" ")
	if err := payment.RSAVerifySHA256(&key.PublicKey, []byte(tampered), sig); err == nil {
		t.Fatalf("被篡改的请求体竟然验签通过")
	}
}

// TestBuildAuthMessageFormat 待签名字符串的格式。
func TestBuildAuthMessageFormat(t *testing.T) {
	got := buildAuthMessage("POST", "/v3/pay/transactions/native", "1700000000", "abc123", `{"a":1}`)
	want := "POST\n/v3/pay/transactions/native\n1700000000\nabc123\n{\"a\":1}\n"
	if got != want {
		t.Fatalf("待签名字符串不对:\n got: %q\nwant: %q", got, want)
	}
}

// TestDoSendsVerifiableAuthorization 真正发出的请求带的是可验签的 Authorization。
func TestDoSendsVerifiableAuthorization(t *testing.T) {
	_, key := testPrivateKeyPEM(t)
	var gotAuth string
	var gotURI string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotURI = r.URL.RequestURI()
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code_url":"weixin://wxpay/bizpayurl?pr=test"}`))
	}))
	defer srv.Close()

	p := &Provider{
		cfg: &Config{
			MchID:    "1900000109",
			SerialNo: "SERIAL1",
			BaseURL:  srv.URL,
		},
		privKey: key,
		client:  srv.Client(),
	}
	var out struct {
		CodeURL string `json:"code_url"`
	}
	if err := p.do(context.Background(), http.MethodPost, "/v3/pay/transactions/native", nil,
		map[string]any{"appid": "wx123"}, &out); err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	if out.CodeURL == "" {
		t.Fatalf("没有解析出 code_url")
	}
	fields := parseAuthHeader(t, gotAuth)
	sig, err := base64.StdEncoding.DecodeString(fields["signature"])
	if err != nil {
		t.Fatalf("签名不是合法 base64: %v", err)
	}
	// 服务端按同样的规则重算签名串,必须一致。
	msg := buildAuthMessage(http.MethodPost, gotURI, fields["timestamp"], fields["nonce_str"], string(gotBody))
	if err := payment.RSAVerifySHA256(&key.PublicKey, []byte(msg), sig); err != nil {
		t.Fatalf("服务端验签失败: %v", err)
	}
}

// TestParseNotify 回调验签 + 解密。
func TestParseNotify(t *testing.T) {
	cert, certKey := testPlatformCert(t)
	apiV3Key := "0123456789abcdef0123456789abcdef"
	p := &Provider{
		cfg: &Config{
			AppID:         "wx123",
			MchID:         "1900000109",
			SerialNo:      "SERIAL1",
			APIv3Key:      apiV3Key,
			PlatformCerts: map[string]*x509.Certificate{"PLATFORM1": cert},
		},
		platformCerts: map[string]*x509.Certificate{"PLATFORM1": cert},
	}

	resource := map[string]any{
		"appid":          "wx123",
		"mchid":          "1900000109",
		"out_trade_no":   "ORDER-001",
		"transaction_id": "4200001234567890",
		"trade_type":     "NATIVE",
		"trade_state":    "SUCCESS",
		"success_time":   "2026-01-01T10:00:00+08:00",
		"amount": map[string]any{
			"total":          19900,
			"currency":       "CNY",
			"payer_total":    19900,
			"payer_currency": "CNY",
		},
	}
	plain, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("序列化明文失败: %v", err)
	}
	nonce := "0123456789ab"
	ciphertext, err := payment.AESGCMEncrypt([]byte(apiV3Key), []byte(nonce), plain, []byte("transaction"))
	if err != nil {
		t.Fatalf("加密失败: %v", err)
	}
	body, err := json.Marshal(map[string]any{
		"id":         "EV-TEST-1",
		"event_type": "TRANSACTION.SUCCESS",
		"resource": map[string]any{
			"algorithm":       "AEAD_AES_256_GCM",
			"ciphertext":      base64.StdEncoding.EncodeToString(ciphertext),
			"associated_data": "transaction",
			"nonce":           nonce,
			"original_type":   "transaction",
		},
	})
	if err != nil {
		t.Fatalf("序列化回调体失败: %v", err)
	}

	timestamp := "1700000000"
	// 验签串用的是请求头里的 Wechatpay-Nonce,不是资源里的 AES nonce。
	signMsg := timestamp + "\n" + "notify-nonce" + "\n" + string(body) + "\n"
	sigBytes, err := payment.RSASignSHA256(certKey, []byte(signMsg))
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	headers := map[string]string{
		"Wechatpay-Timestamp": timestamp,
		"Wechatpay-Nonce":     "notify-nonce",
		"Wechatpay-Signature": base64.StdEncoding.EncodeToString(sigBytes),
		"Wechatpay-Serial":    "PLATFORM1",
	}
	notify, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method:  http.MethodPost,
		URL:     "/notify/wechat/u1",
		Headers: headers,
		Body:    body,
	})
	if err != nil {
		t.Fatalf("解析回调失败: %v", err)
	}
	if notify.OutTradeNo != "ORDER-001" {
		t.Fatalf("商户单号不对: %q", notify.OutTradeNo)
	}
	if notify.TransactionID != "4200001234567890" {
		t.Fatalf("微信交易号不对: %q", notify.TransactionID)
	}
	if notify.Amount != 19900 {
		t.Fatalf("金额不对: %d", notify.Amount)
	}
	if notify.Status != payment.TradeStatusSuccess {
		t.Fatalf("状态不对: %v", notify.Status)
	}
	if notify.Channel != payment.ChannelWechat {
		t.Fatalf("渠道不对: %v", notify.Channel)
	}

	// 篡改回调体 → 验签必须失败。
	badBody := []byte(strings.Replace(string(body), "EV-TEST-1", "EV-TEST-2", 1))
	badReq := &payment.NotifyRequest{Method: http.MethodPost, Headers: headers, Body: badBody}
	if _, err := p.ParseNotify(context.Background(), badReq); err == nil {
		t.Fatalf("被篡改的回调体竟然验签通过")
	}

	// 签名正确但密文被改 → 解密必须失败(GCM 认证)。
	brokenCipher := []byte(strings.Replace(string(body),
		base64.StdEncoding.EncodeToString(ciphertext), "AAAA", 1))
	brokenSig, err := payment.RSASignSHA256(certKey,
		[]byte(timestamp+"\n"+"notify-nonce"+"\n"+string(brokenCipher)+"\n"))
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	brokenHeaders := map[string]string{}
	for k, v := range headers {
		brokenHeaders[k] = v
	}
	brokenHeaders["Wechatpay-Signature"] = base64.StdEncoding.EncodeToString(brokenSig)
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost, Headers: brokenHeaders, Body: brokenCipher,
	}); err == nil {
		t.Fatalf("密文被篡改的回调竟然解析成功")
	}

	// 未知平台证书序列号 → 拒绝。
	unknown := map[string]string{}
	for k, v := range headers {
		unknown[k] = v
	}
	unknown["Wechatpay-Serial"] = "UNKNOWN"
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost, Headers: unknown, Body: body,
	}); err == nil {
		t.Fatalf("未知平台证书竟然验签通过")
	}

	// 缺少签名头 → 拒绝。
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Method: http.MethodPost, Headers: map[string]string{}, Body: body,
	}); err == nil {
		t.Fatalf("缺少签名头竟然解析成功")
	}
}

// TestConfigValidate 配置校验。
func TestConfigValidate(t *testing.T) {
	privPEM, _ := testPrivateKeyPEM(t)
	cfg := &Config{
		AppID:         "wx123",
		MchID:         "1900000109",
		SerialNo:      "SERIAL1",
		APIv3Key:      "0123456789abcdef0123456789abcdef",
		PrivateKeyPEM: privPEM,
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("合法配置被拒: %v", err)
	}
	// nil 配置。
	var nilCfg *Config
	if err := nilCfg.validate(); err == nil {
		t.Fatalf("nil 配置竟然通过了校验")
	}
	// APIv3 密钥必须是 32 字节。
	bad := *cfg
	bad.APIv3Key = "too-short"
	if err := bad.validate(); err == nil {
		t.Fatalf("过短的 APIv3 密钥竟然通过了校验")
	}
	// 缺必填项 —— 每一项都要单独测一遍:
	// 只测 AppID 的话,其它三项漏校验了测试照样绿。
	missing := *cfg
	missing.AppID = ""
	if err := missing.validate(); err == nil {
		t.Fatalf("缺 AppID 竟然通过了校验")
	}
	missing = *cfg
	missing.MchID = ""
	if err := missing.validate(); err == nil {
		t.Fatalf("缺 MchID 竟然通过了校验")
	}
	missing = *cfg
	missing.SerialNo = ""
	if err := missing.validate(); err == nil {
		t.Fatalf("缺 SerialNo 竟然通过了校验")
	}
	missing = *cfg
	missing.PrivateKeyPEM = ""
	if err := missing.validate(); err == nil {
		t.Fatalf("缺私钥竟然通过了校验")
	}
}
