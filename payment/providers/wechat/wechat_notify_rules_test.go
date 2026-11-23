package wechat

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strconv"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// buildNotifyBody 构造一个 APIv3 回调报文(明文 → AEAD 加密 → JSON)。
func buildNotifyBody(t *testing.T, apiV3Key string, resource map[string]any) []byte {
	t.Helper()
	plain, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("序列化明文失败: %v", err)
	}
	nonce := "0123456789ab"
	ct, err := payment.AESGCMEncrypt([]byte(apiV3Key), []byte(nonce), plain, []byte("transaction"))
	if err != nil {
		t.Fatalf("加密失败: %v", err)
	}
	body, err := json.Marshal(map[string]any{
		"id":         "EV-RULES-1",
		"event_type": "TRANSACTION.SUCCESS",
		"resource": map[string]any{
			"algorithm":       "AEAD_AES_256_GCM",
			"ciphertext":      base64.StdEncoding.EncodeToString(ct),
			"associated_data": "transaction",
			"nonce":           nonce,
			"original_type":   "transaction",
		},
	})
	if err != nil {
		t.Fatalf("序列化回调体失败: %v", err)
	}
	return body
}

// signedNotify 给回调报文签名并返回请求头。
func signedNotify(t *testing.T, certKey *rsa.PrivateKey, body []byte, timestamp string) map[string]string {
	t.Helper()
	sig, err := payment.RSASignSHA256(certKey, []byte(timestamp+"\nnotify-nonce\n"+string(body)+"\n"))
	if err != nil {
		t.Fatalf("签名失败: %v", err)
	}
	return map[string]string{
		"Wechatpay-Timestamp": timestamp,
		"Wechatpay-Nonce":     "notify-nonce",
		"Wechatpay-Signature": base64.StdEncoding.EncodeToString(sig),
		"Wechatpay-Serial":    "PLATFORM1",
	}
}

// TestWechatNotifyRejectsForeignAppID 通知里的 appid 不是自己的必须拒绝。
//
// 验签只能证明报文没被篡改,不能证明「这笔钱是付给这个商户的」。
func TestWechatNotifyRejectsForeignAppID(t *testing.T) {
	cert, certKey := testPlatformCert(t)
	apiV3Key := "0123456789abcdef0123456789abcdef"
	p := &Provider{
		cfg:           &Config{AppID: "wx_ME", MchID: "1900000109", APIv3Key: apiV3Key},
		platformCerts: map[string]*x509.Certificate{"PLATFORM1": cert},
	}
	body := buildNotifyBody(t, apiV3Key, map[string]any{
		"appid":        "wx_OTHER",
		"mchid":        "1900000109",
		"out_trade_no": "ORDER-1",
		"trade_state":  "SUCCESS",
		"amount":       map[string]any{"total": 19900, "currency": "CNY"},
	})
	headers := signedNotify(t, certKey, body, "1700000000")
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Headers: headers, Body: body,
	}); err == nil {
		t.Fatal("别人 appid 的通知竟然被接收了")
	}
}

// TestWechatNotifyRejectsForeignMchID 通知里的 mchid 不是自己的必须拒绝。
func TestWechatNotifyRejectsForeignMchID(t *testing.T) {
	cert, certKey := testPlatformCert(t)
	apiV3Key := "0123456789abcdef0123456789abcdef"
	p := &Provider{
		cfg:           &Config{AppID: "wx123", MchID: "1900000109", APIv3Key: apiV3Key},
		platformCerts: map[string]*x509.Certificate{"PLATFORM1": cert},
	}
	body := buildNotifyBody(t, apiV3Key, map[string]any{
		"appid":        "wx123",
		"mchid":        "1900000999",
		"out_trade_no": "ORDER-1",
		"trade_state":  "SUCCESS",
		"amount":       map[string]any{"total": 19900, "currency": "CNY"},
	})
	headers := signedNotify(t, certKey, body, "1700000000")
	if _, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Headers: headers, Body: body,
	}); err == nil {
		t.Fatal("别人 mchid 的通知竟然被接收了")
	}
}

// TestWechatNotifyAcceptsOldTimestamp 24 小时前的回调仍然接收 —— 这是刻意的。
//
// 微信的回调重试策略最长拖到 24 小时之后(15s/15s/30m/3h/6h/12h/24h),
// 支付宝更久(立即 3 次 + 4m/10m/10m/1h/2h/6h/15h,跨 24 小时 22 分)。
// 卡死一个时间窗口,渠道的**正常**重投会被拒,订单永远停在「待支付」。
// 防重放靠的是幂等 + 状态机只向前走。
func TestWechatNotifyAcceptsOldTimestamp(t *testing.T) {
	cert, certKey := testPlatformCert(t)
	apiV3Key := "0123456789abcdef0123456789abcdef"
	p := &Provider{
		cfg:           &Config{AppID: "wx123", MchID: "1900000109", APIv3Key: apiV3Key},
		platformCerts: map[string]*x509.Certificate{"PLATFORM1": cert},
	}
	body := buildNotifyBody(t, apiV3Key, map[string]any{
		"appid":        "wx123",
		"mchid":        "1900000109",
		"out_trade_no": "ORDER-1",
		"trade_state":  "SUCCESS",
		"amount":       map[string]any{"total": 19900, "currency": "CNY"},
	})
	old := strconv.FormatInt(time.Now().Add(-24*time.Hour).Unix(), 10)
	headers := signedNotify(t, certKey, body, old)
	n, err := p.ParseNotify(context.Background(), &payment.NotifyRequest{
		Headers: headers, Body: body,
	})
	if err != nil {
		t.Fatalf("24 小时前的(渠道重投的)通知被拒了: %v", err)
	}
	if n.OutTradeNo != "ORDER-1" {
		t.Fatalf("订单号不对: %q", n.OutTradeNo)
	}
}

// TestWechatCertDirIsCreatedOnDemand 证书目录不存在时要自动建,不能因此建不起 Provider。
func TestWechatCertDirIsCreatedOnDemand(t *testing.T) {
	dir := t.TempDir() + "/not-exist-yet"
	p := &Provider{
		cfg:           &Config{CertDir: dir},
		platformCerts: map[string]*x509.Certificate{},
	}
	cert, _ := testPlatformCert(t)
	p.platformCerts["PLATFORM1"] = cert
	// 触发写盘路径:直接调写证书那段逻辑(不联网)。
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := saveCertFile(dir, "PLATFORM1", pemBytes); err != nil {
		t.Fatalf("证书目录不存在时应当自动创建: %v", err)
	}
}
