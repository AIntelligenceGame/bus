package alipay

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 本文件的期望值来自**官方算法**(不是我们自己实现的算法)跑出来的结果:
//
//	证书序列号算法 = AntCertificationUtil.getCertSN / getRootCertSN
//	  (支付宝官方 Java SDK,见 https://opendocs.alipay.com/support/01raux 与 .../01rauy)
//	  MD5(issuerDN + serialNumber) 的小写十六进制,不足 32 位补 0
//	  rootCertSN = 根证书文件里每张 RSA 签名的证书各算一个,用 "_" 连接
//
// 期望值由 JDK(X500Principal.getName() + MessageDigest)对着同样的
// issuer/subject/serial 证书算出来 —— 即"官方口径"的已知答案向量。
//
// 之所以要钉死这个向量:Go 的 x509.Certificate.Issuer.String() 输出的是
// "C=CN,O=...,CN=..."(根在前),而 Java 的 X500Principal.getName() 输出的是
// "CN=...,O=...,C=CN"(叶子在前)。顺序差一个,MD5 就完全对不上,
// 结果是每次请求都被网关判 40002 / isv.invalid-signature。

// testCertDN 官方 Java SDK 那套算法要求 issuer DN 与 serial 完全一致。
var testCertDN = pkix.Name{
	Country:      []string{"CN"},
	Organization: []string{"Test Org"},
}

// testCert 造一张证书:issuer 由 issuerName/issuerKey 决定,序列号固定。
func testCert(t *testing.T, subject pkix.Name, serial int64, issuerName pkix.Name, issuerKey *rsa.PrivateKey, issuerCert *x509.Certificate) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               subject,
		Issuer:                issuerName,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	parent := tmpl
	signer := issuerKey
	if issuerCert != nil {
		parent = issuerCert
	}
	if signer == nil {
		// 自签发:用自己刚生成的那把私钥。
		signer = key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatalf("签发证书失败: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("解析证书失败: %v", err)
	}
	return cert, key
}

// TestCertSNMatchesOfficialAlgorithm 序列号必须与官方算法一致。
func TestCertSNMatchesOfficialAlgorithm(t *testing.T) {
	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	root, rootKey := testCert(t, rootName, 0x1234567890, rootName, nil, nil)

	appName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test App",
	}
	app, _ := testCert(t, appName, 0x1234567890, rootName, rootKey, root)

	// 期望值:官方 Java SDK 对同样的 issuer DN / serial 算出来的结果。
	const want = "fdc1bebbcb5a12b933d9dfbd8df5dac2"
	got, err := certSN(app)
	if err != nil {
		t.Fatalf("计算序列号失败: %v", err)
	}
	if got != want {
		t.Fatalf("应用证书序列号不对:\n 实际 %s\n 期望 %s", got, want)
	}
	gotRoot, err := certSN(root)
	if err != nil {
		t.Fatalf("计算序列号失败: %v", err)
	}
	if gotRoot != want {
		t.Fatalf("根证书序列号不对:\n 实际 %s\n 期望 %s", gotRoot, want)
	}
}

// TestCertSNDifferentSerial 序列号不同,结果必须不同(别把 serial 漏掉)。
func TestCertSNDifferentSerial(t *testing.T) {
	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	root, rootKey := testCert(t, rootName, 0x1234567890, rootName, nil, nil)
	appName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test App",
	}
	app, _ := testCert(t, appName, 0x2222, rootName, rootKey, root)

	got, err := certSN(app)
	if err != nil {
		t.Fatalf("计算序列号失败: %v", err)
	}
	const want = "9ed9322fcdce356b57907415bb99a55f"
	if got != want {
		t.Fatalf("序列号不对:\n 实际 %s\n 期望 %s", got, want)
	}
}

// TestRootCertSNJoinsWithUnderscore 根证书序列号要按文件顺序用 "_" 连接。
func TestRootCertSNJoinsWithUnderscore(t *testing.T) {
	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	root, rootKey := testCert(t, rootName, 0x1234567890, rootName, nil, nil)
	appName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test App",
	}
	app, _ := testCert(t, appName, 0x2222, rootName, rootKey, root)

	pemBytes := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: app.Raw})...,
	)
	got, err := rootCertSN(pemBytes)
	if err != nil {
		t.Fatalf("计算根证书序列号失败: %v", err)
	}
	const want = "fdc1bebbcb5a12b933d9dfbd8df5dac2_9ed9322fcdce356b57907415bb99a55f"
	if got != want {
		t.Fatalf("根证书序列号不对:\n 实际 %s\n 期望 %s", got, want)
	}
}

// TestJavaX500NameReversesRDNOrder Java 的 DN 是叶子在前,别写成根在前。
func TestJavaX500NameReversesRDNOrder(t *testing.T) {
	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	root, _ := testCert(t, rootName, 0x1234567890, rootName, nil, nil)
	got, err := javaX500Name(root.RawSubject)
	if err != nil {
		t.Fatalf("解析 DN 失败: %v", err)
	}
	const want = "CN=Test Root,O=Test Org,C=CN"
	if got != want {
		t.Fatalf("DN 字符串不对:\n 实际 %s\n 期望 %s", got, want)
	}
}

// TestLoadRootCertSNFromFile 从文件读根证书并算序列号。
func TestLoadRootCertSNFromFile(t *testing.T) {
	rootName := pkix.Name{
		Country:      []string{"CN"},
		Organization: []string{"Test Org"},
		CommonName:   "Test Root",
	}
	root, _ := testCert(t, rootName, 0x1234567890, rootName, nil, nil)
	dir := t.TempDir()
	path := filepath.Join(dir, "alipayRootCert.crt")
	if err := os.WriteFile(path,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}), 0o600); err != nil {
		t.Fatalf("写根证书失败: %v", err)
	}
	got, err := loadRootCertSNFromFile(path)
	if err != nil {
		t.Fatalf("计算根证书序列号失败: %v", err)
	}
	const want = "fdc1bebbcb5a12b933d9dfbd8df5dac2"
	if got != want {
		t.Fatalf("根证书序列号不对:\n 实际 %s\n 期望 %s", got, want)
	}
}
