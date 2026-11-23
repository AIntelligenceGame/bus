package alipay

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// writeCertFile 把证书写成 PEM 文件。
func writeCertFile(t *testing.T, dir, name string, cert *x509.Certificate) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600); err != nil {
		t.Fatalf("写证书失败: %v", err)
	}
	return path
}

// certModeFiles 造出证书模式需要的三份文件:应用公钥证书 / 支付宝公钥证书 / 支付宝根证书。
func certModeFiles(t *testing.T) (appPath, aliPath, rootPath string) {
	t.Helper()
	dir := t.TempDir()

	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	rootKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	rootTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(0x1234567890),
		Subject:               rootName,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTmpl, rootTmpl, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("签发根证书失败: %v", err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("解析根证书失败: %v", err)
	}

	leaf := func(cn string, serial int64) *x509.Certificate {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("生成私钥失败: %v", err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject: pkix.Name{
				Country:      []string{"CN"},
				Organization: []string{"Test Org"},
				CommonName:   cn,
			},
			Issuer:    rootName,
			NotBefore: time.Now().Add(-time.Hour),
			NotAfter:  time.Now().Add(24 * time.Hour),
			KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, root, &key.PublicKey, rootKey)
		if err != nil {
			t.Fatalf("签发证书失败: %v", err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("解析证书失败: %v", err)
		}
		return cert
	}

	app := leaf("Test App", 0x1234567890)
	ali := leaf("Alipay Public Cert", 0x2222)

	return writeCertFile(t, dir, "appCertPublicKey.crt", app),
		writeCertFile(t, dir, "alipayCertPublicKey.crt", ali),
		writeCertFile(t, dir, "alipayRootCert.crt", root)
}

// TestCertModeRejectsExpiredCert 证书过期要在启动时就报错。
//
// 过期证书的症状很迷惑:请求照发不误,网关一律回 40002 / isv.invalid-signature,
// 异步通知也验签失败 —— 看着像签名算法错了,其实只是证书该换了。
func TestCertModeRejectsExpiredCert(t *testing.T) {
	_, aliPath, rootPath := certModeFiles(t)

	// 造一张**已经过期**的应用公钥证书。
	rootKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	rootTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(0x9999),
		Subject:               pkix.Name{CommonName: "Test Root"},
		NotBefore:             time.Now().Add(-48 * time.Hour),
		NotAfter:              time.Now().Add(-24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTmpl, rootTmpl, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("签发根证书失败: %v", err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("解析根证书失败: %v", err)
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(0x8888),
		Subject:      pkix.Name{CommonName: "Expired App"},
		NotBefore:    time.Now().Add(-48 * time.Hour),
		NotAfter:     time.Now().Add(-24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, root, &leafKey.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("签发证书失败: %v", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("解析证书失败: %v", err)
	}
	expiredPath := writeCertFile(t, t.TempDir(), "appCertPublicKey.crt", leaf)

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})
	_, err = New(&Config{
		AppID:                "2021000000000000",
		PrivateKeyPEM:        string(privPEM),
		AlipayPublicCertPath: aliPath,
		AppCertPath:          expiredPath,
		RootCertPath:         rootPath,
	})
	if err == nil {
		t.Fatal("应用公钥证书已过期,应该直接报错")
	}
	if !errors.Is(err, payment.ErrInvalidConfig) {
		t.Fatalf("报错类型不对: %v", err)
	}
	if !strings.Contains(err.Error(), "过期") {
		t.Fatalf("报错没说清楚是过期: %v", err)
	}
}

// TestCertModeSendsBothCertSNs 证书模式的请求必须带上 app_cert_sn 和 alipay_root_cert_sn,
// 而且这两个都要参与签名(官方《签名》)。
func TestCertModeSendsBothCertSNs(t *testing.T) {
	appPath, aliPath, rootPath := certModeFiles(t)

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})

	p, err := New(&Config{
		AppID:                "2021000000000000",
		PrivateKeyPEM:        string(privPEM),
		AlipayPublicCertPath: aliPath,
		AppCertPath:          appPath,
		RootCertPath:         rootPath,
	})
	if err != nil {
		t.Fatalf("证书模式创建 Provider 失败: %v", err)
	}

	params, err := p.buildParams("alipay.trade.precreate", map[string]any{"out_trade_no": "T1"})
	if err != nil {
		t.Fatalf("组装参数失败: %v", err)
	}
	if params["app_cert_sn"] == "" {
		t.Fatal("证书模式请求里没有 app_cert_sn")
	}
	if params["alipay_root_cert_sn"] == "" {
		t.Fatal("证书模式请求里没有 alipay_root_cert_sn")
	}
	content := signContent(params)
	if !strings.Contains(content, "app_cert_sn="+params["app_cert_sn"]) {
		t.Fatalf("app_cert_sn 没有参与签名:\n%s", content)
	}
	if !strings.Contains(content, "alipay_root_cert_sn="+params["alipay_root_cert_sn"]) {
		t.Fatalf("alipay_root_cert_sn 没有参与签名:\n%s", content)
	}
}

// TestCertModeRequiresAppAndRootCert 证书模式少配证书要立刻报错,
// 而不是等到运行时每个请求都被网关判 40002。
func TestCertModeRequiresAppAndRootCert(t *testing.T) {
	_, aliPath, rootPath := certModeFiles(t)

	base := func() *Config {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("生成私钥失败: %v", err)
		}
		privPEM := pem.EncodeToMemory(&pem.Block{
			Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv),
		})
		return &Config{
			AppID:                "2021000000000000",
			PrivateKeyPEM:        string(privPEM),
			AlipayPublicCertPath: aliPath,
		}
	}

	if _, err := New(base()); err == nil {
		t.Fatal("证书模式只配支付宝公钥证书,应该报错")
	}
	cfg := base()
	cfg.AppCertPath = rootPath
	if _, err := New(cfg); err == nil {
		t.Fatal("证书模式没配支付宝根证书,应该报错")
	}
}

// TestPublicKeyModeHasNoCertSN 公钥模式不带证书序列号。
func TestPublicKeyModeHasNoCertSN(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv),
	})
	alipayKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&alipayKey.PublicKey)
	if err != nil {
		t.Fatalf("序列化公钥失败: %v", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	p, err := New(&Config{
		AppID:              "2021000000000000",
		PrivateKeyPEM:      string(privPEM),
		AlipayPublicKeyPEM: string(pubPEM),
	})
	if err != nil {
		t.Fatalf("创建 Provider 失败: %v", err)
	}
	params, err := p.buildParams("alipay.trade.precreate", map[string]any{"out_trade_no": "T1"})
	if err != nil {
		t.Fatalf("组装参数失败: %v", err)
	}
	if params["app_cert_sn"] != "" || params["alipay_root_cert_sn"] != "" {
		t.Fatalf("公钥模式不该带证书序列号: %v", params)
	}
}
