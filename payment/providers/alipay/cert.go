package alipay

import (
	"crypto/md5" // #nosec G501 -- 官方证书序列号算法规定用 MD5,不是用于安全用途
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 证书序列号的算法严格照抄官方实现,不要"优化":
//
//   - app_cert_sn         官方文档 https://opendocs.alipay.com/support/01raux
//     官方 SDK   AlipaySignature.getCertSN → AntCertificationUtil.getCertSN
//     算法       MD5(issuerDN + serialNumber) 的小写十六进制,不足 32 位前面补 0
//   - alipay_root_cert_sn 官方文档 https://opendocs.alipay.com/support/01rauy
//     官方 SDK   AntCertificationUtil.getRootCertSN
//     算法       根证书文件里**每一张 RSA 签名的证书**各算一个 SN,按文件顺序用 "_" 连接
//
// 这里的 issuerDN 必须与 Java 的 X500Principal.getName() **逐字节一致**,
// 否则算出来的 MD5 跟支付宝网关算的不一样,请求会返回 40002 / isv.invalid-signature。
//
// Java 那个方法的特点(踩坑点):
//
//	1. RDN 顺序是**倒着**输出的:DER 里是根在前(C,O,CN),Java 输出的是叶子在前(CN,O,C);
//	2. 分隔符是 ","(后面没有空格)、同一个 RDN 内多个属性用 "+";
//	3. 属性名用短名(CN/O/OU/C/ST/L/DC/UID/emailAddress),未知 OID 用点分形式。
//
// Go 的 x509.Certificate.Issuer.String() 输出的是"C=CN,O=...,CN=..."(根在前),
// 跟 Java 差一个顺序,直接拿它算 MD5 一定对不上 —— 所以这里自己解析 RawIssuer。

// javaDNNames 属性 OID → Java 的短名。
var javaDNNames = map[string]string{
	"2.5.4.3":                    "CN",
	"2.5.4.4":                    "SN",
	"2.5.4.5":                    "serialNumber",
	"2.5.4.6":                    "C",
	"2.5.4.7":                    "L",
	"2.5.4.8":                    "ST",
	"2.5.4.9":                    "STREET",
	"2.5.4.10":                   "O",
	"2.5.4.11":                   "OU",
	"2.5.4.42":                   "GN",
	"0.9.2342.19200300.100.1.1":  "UID",
	"0.9.2342.19200300.100.1.25": "DC",
	"1.2.840.113549.1.9.1":       "emailAddress",
}

// javaX500Name 复刻 Java X500Principal.getName() 的输出(issuer 用的是证书里的 RawIssuer)。
func javaX500Name(raw []byte) (string, error) {
	var rdns pkix.RDNSequence
	if _, err := asn1.Unmarshal(raw, &rdns); err != nil {
		return "", fmt.Errorf("alipay: parse distinguished name: %w", err)
	}
	parts := make([]string, 0, len(rdns))
	// Java 内部把 RDN 顺序反过来存,输出时按叶子在前打印。
	for i := len(rdns) - 1; i >= 0; i-- {
		atvs := rdns[i]
		sub := make([]string, 0, len(atvs))
		for _, atv := range atvs {
			sub = append(sub, javaAttrTypeAndValue(atv))
		}
		parts = append(parts, strings.Join(sub, "+"))
	}
	return strings.Join(parts, ","), nil
}

// javaAttrTypeAndValue 格式化一个属性:type=value。
func javaAttrTypeAndValue(atv pkix.AttributeTypeAndValue) string {
	name := atv.Type.String()
	if short, ok := javaDNNames[name]; ok {
		name = short
	}
	return name + "=" + javaEscapeValue(atv.Value)
}

// javaEscapeValue 按 RFC2253 转义属性值里的特殊字符。
func javaEscapeValue(v any) string {
	s, ok := v.(string)
	if !ok {
		if b, isBytes := v.([]byte); isBytes {
			// 非字符串类型用 RFC2253 的二进制写法:# + 大写十六进制。
			return "#" + strings.ToUpper(fmt.Sprintf("%x", b))
		}
		s = fmt.Sprintf("%v", v)
	}
	var b strings.Builder
	for i, r := range s {
		switch r {
		case ',', '+', '"', '\\', '<', '>', ';':
			b.WriteByte('\\')
		case ' ', '#':
			// 只在开头/结尾的空��,以及开头的 # 需要转义。
			if (i == 0 || i == len(s)-1) && r == ' ' {
				b.WriteByte('\\')
			} else if i == 0 && r == '#' {
				b.WriteByte('\\')
			}
		}
		b.WriteRune(r)
	}
	return b.String()
}

// certSN 计算证书序列号(官方 AlipaySignature.getCertSN 的等价实现)。
//
// 返回值形如 "fdc1bebbcb5a12b933d9dfbd8df5dac2"(32 位小写十六进制)。
func certSN(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", fmt.Errorf("%w: nil certificate", payment.ErrInvalidConfig)
	}
	dn, err := javaX500Name(cert.RawIssuer)
	if err != nil {
		return "", err
	}
	sum := md5.Sum([]byte(dn + cert.SerialNumber.String())) // #nosec G401 -- 官方算法规定
	return fmt.Sprintf("%032x", sum), nil
}

// isRSASigned 判断证书是不是 RSA 签名(官方用签名算法 OID 前缀 "1.2.840.113549.1.1" 判断)。
//
// Java 的判断是 OID 前缀,会包含 RSASSA-PSS(…1.1.10);Go 这边没有直接给 OID,
// 用算法枚举逐个对上,保证跟 Java 的筛选结果一致。
func isRSASigned(cert *x509.Certificate) bool {
	switch cert.SignatureAlgorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.SHA256WithRSA,
		x509.SHA384WithRSA, x509.SHA512WithRSA,
		x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS:
		return true
	default:
		return false
	}
}

// rootCertSN 计算支付宝根证书序列号(官方 AntCertificationUtil.getRootCertSN 的等价实现)。
//
// 根证书文件里通常有多张证书(根证书 + 中间证书),每张 RSA 签名的都算一个 SN,
// 按文件里的先后顺序用 "_" 连接。
func rootCertSN(pemBytes []byte) (string, error) {
	certs, err := parseCertChain(pemBytes)
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(certs))
	for _, cert := range certs {
		if !isRSASigned(cert) {
			continue
		}
		sn, err := certSN(cert)
		if err != nil {
			return "", err
		}
		parts = append(parts, sn)
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("%w: alipay root cert has no rsa certificate", payment.ErrInvalidConfig)
	}
	return strings.Join(parts, "_"), nil
}

// parseCertChain 解析 PEM 里可能存在的多张证书。
func parseCertChain(pemBytes []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("alipay: parse certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("%w: no certificate found in pem", payment.ErrInvalidConfig)
	}
	return certs, nil
}

// loadCertFromFile 从 PEM 文件里读出第一张证书。
func loadCertFromFile(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("alipay: read cert file: %w", err)
	}
	certs, err := parseCertChain(data)
	if err != nil {
		return nil, err
	}
	return certs[0], nil
}

// checkCertNotExpired 检查证书是否还在有效期内。
//
// 证书过期是个**部署**问题,不是代码问题,但症状很迷惑:请求照发不误,
// 网关一律回 40002 / isv.invalid-signature,异步通知也验签失败 ——
// 看起来像签名算法错了,其实只是证书该换了。启动时直接说清楚,
// 比让商户对着 40002 猜要省事得多。
func checkCertNotExpired(cert *x509.Certificate, what string) error {
	if cert == nil {
		return nil
	}
	now := time.Now()
	if now.After(cert.NotAfter) {
		return fmt.Errorf("%w: alipay %s 已于 %s 过期,请到开放平台重新下载证书后替换",
			payment.ErrInvalidConfig, what, cert.NotAfter.Format("2006-01-02"))
	}
	if now.Before(cert.NotBefore) {
		return fmt.Errorf("%w: alipay %s 还没生效(有效期从 %s 开始)",
			payment.ErrInvalidConfig, what, cert.NotBefore.Format("2006-01-02"))
	}
	return nil
}

// loadRootCertSNFromFile 读支付宝根证书文件并算出 alipay_root_cert_sn。
func loadRootCertSNFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("alipay: read root cert file: %w", err)
	}
	return rootCertSN(data)
}

// certSNHexUpper 仅用于调试输出,不做任何安全用途。
func certSNHexUpper(sn string) string { return strconv.Quote(sn) }
