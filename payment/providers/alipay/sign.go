package alipay

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/AIntelligenceGame/bus/payment"
)

// signContent 拼接待签名串(**请求**用):参数按 key 升序,拼接为 k1=v1&k2=v2(值不编码)。
//
// 官方规则(《签名》opendocs.alipay.com/apis/02khjm):
// 「剔除 sign 字段,剔除值为空的参数」,其余按 key 的 ASCII 升序拼成 k=v&k=v。
//
// sign_type 是**参与**请求签名的 —— 它是公共参数,官方 SDK 的请求签名链路
// (AbstractAlipayClient → AlipaySignature.getSignatureContent → getSignContent)
// 走的正是这条路,而 getSignContent 只剔除空值、不剔除 sign_type。
//
// 这里曾经把 sign_type 一起剔掉了,等于跟支付宝网关算出了两份不同的待签名串,
// 结果是每次请求都回 40002 / isv.invalid-signature。
func signContent(params map[string]string) string {
	return buildSignContent(params, false)
}

// verifyContent 拼接待验签串(异步通知 / 同步返回用)。
//
// 官方规则与请求签名**不同**:验签时 sign 与 sign_type 都不参与
// (官方 SDK 的 getSignCheckContentV1 就是这么做的,支付宝的 verifyV1 也用它)。
func verifyContent(params map[string]string) string {
	return buildSignContent(params, true)
}

// buildSignContent 按官方规则拼接待签名/验签串。
//
// dropSignType 为 true 时额外剔除 sign_type(验签场景)。
func buildSignContent(params map[string]string, dropSignType bool) string {
	keys := make([]string, 0, len(params))
	for k, v := range params {
		if k == "sign" || v == "" {
			continue
		}
		if dropSignType && k == "sign_type" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	return strings.Join(parts, "&")
}

// signParams 用应用私钥对公共参数+业务参数签名,并把 sign 写回参数表。
func (p *Provider) signParams(params map[string]string) (map[string]string, error) {
	content := signContent(params)
	sign, err := payment.RSA2Sign(p.privKey, content)
	if err != nil {
		return nil, fmt.Errorf("alipay: sign request: %w", err)
	}
	out := make(map[string]string, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out["sign"] = sign
	return out, nil
}

// verifyParams 用支付宝公钥验证签名(异步通知用)。
func (p *Provider) verifyParams(params map[string]string) error {
	sign := params["sign"]
	if sign == "" {
		return fmt.Errorf("%w: alipay notify missing sign", payment.ErrInvalidSignature)
	}
	if err := payment.RSA2Verify(p.alipayPub, verifyContent(params), sign); err != nil {
		return fmt.Errorf("%w: alipay notify verify failed", payment.ErrInvalidSignature)
	}
	return nil
}

// loadPublicKeyFromCert 从 PEM 证书文件中提取公钥。
func loadPublicKeyFromCert(path string) (*rsa.PublicKey, *x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("alipay: read public cert: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, nil, fmt.Errorf("%w: alipay public cert is not valid pem", payment.ErrInvalidConfig)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("alipay: parse public cert: %w", err)
	}
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, nil, fmt.Errorf("%w: alipay public cert is not rsa", payment.ErrInvalidConfig)
	}
	return pub, cert, nil
}
