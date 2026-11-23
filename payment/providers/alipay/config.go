// Package alipay 实现支付宝开放平台的支付能力。
//
// 支持场景:
//
//	TradeTypeNative 当面付(扫码),返回 qr_code,渲染成二维码给手机扫
//	TradeTypeApp    APP 支付,返回 orderString,交给支付宝 SDK 调起
//	TradeTypeWap    手机网站支付,返回跳转 URL
//	TradeTypeWeb    电脑网站支付,返回跳转 URL
//
// 密钥准备(两种模式,照官方 SDK 的语义):
//
//	公钥模式:应用私钥(app_private_key.pem)+ 支付宝公钥(alipay_public_key.pem)。
//	证书模式:再配 支付宝公钥证书(alipayCertPublicKey.crt)+ 应用公钥证书(appCertPublicKey.crt)
//	  + 支付宝根证书(alipayRootCert.crt)。库会自动算出 app_cert_sn 与 alipay_root_cert_sn,
//	  并按官方《签名》要求把这两个 SN 一起放进待签名字符串。
//
// 证书模式缺任何一个证书都会在 New() 阶段直接报错(官方 SDK 也是这么拦的)。
package alipay

import (
	"fmt"
	"net/http"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 网关地址。
const (
	// DefaultGateway 生产环境网关。
	DefaultGateway = "https://openapi.alipay.com/gateway.do"
	// SandboxGateway 沙箱网关。
	SandboxGateway = "https://openapi.alipaydev.com/gateway.do"
	// defaultTimeout 默认 HTTP 超时。
	defaultTimeout = 10 * time.Second
)

// Config 支付宝配置(开放平台)。
type Config struct {
	// AppID 支付宝开放平台应用 APPID。
	AppID string `mapstructure:"app_id" json:"app_id"`
	// PrivateKeyPEM 应用私钥 PEM 内容(PKCS#1 或 PKCS#8)。
	PrivateKeyPEM string `mapstructure:"private_key_pem" json:"private_key_pem"`
	// AlipayPublicKeyPEM 支付宝公钥 PEM 内容,用于验签异步通知。
	AlipayPublicKeyPEM string `mapstructure:"alipay_public_key_pem" json:"alipay_public_key_pem"`
	// AlipayPublicCertPath 支付宝公钥证书文件路径(.crt),与 AlipayPublicKeyPEM 二选一。
	//
	// 一旦填了这个,就是**证书模式**:按官方 SDK 的语义,证书模式必须同时提供
	// 应用公钥证书(AppCertPath)和支付宝根证书(RootCertPath),缺一个都起不来。
	AlipayPublicCertPath string `mapstructure:"alipay_public_cert_path" json:"alipay_public_cert_path"`
	// AppCertPath 应用公钥证书文件路径(.crt),证书模式必填。
	//
	// 它的序列号就是请求里的 app_cert_sn(官方:opendocs.alipay.com/support/01raux)。
	AppCertPath string `mapstructure:"app_cert_path" json:"app_cert_path"`
	// RootCertPath 支付宝根证书文件路径(.crt),证书模式必填。
	//
	// 它的序列号就是请求里的 alipay_root_cert_sn(官方:opendocs.alipay.com/support/01rauy)。
	RootCertPath string `mapstructure:"root_cert_path" json:"root_cert_path"`
	// SignType 签名算法,默认 RSA2。
	SignType string `mapstructure:"sign_type" json:"sign_type"`
	// NotifyURL 默认异步通知地址。
	NotifyURL string `mapstructure:"notify_url" json:"notify_url"`
	// ReturnURL 默认同步跳转地址。
	ReturnURL string `mapstructure:"return_url" json:"return_url"`
	// Gateway 网关地址,默认生产环境。
	Gateway string `mapstructure:"gateway" json:"gateway"`
	// HTTPClient 自定义 HTTP 客户端(不参与配置解析)。
	HTTPClient *http.Client `mapstructure:"-"`
	// Timeout HTTP 超时,默认 10s。
	Timeout time.Duration `mapstructure:"timeout"`
}

// validate 校验配置。
func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil alipay config", payment.ErrInvalidConfig)
	}
	if c.AppID == "" {
		return fmt.Errorf("%w: alipay AppID is required", payment.ErrInvalidConfig)
	}
	if c.PrivateKeyPEM == "" {
		return fmt.Errorf("%w: alipay PrivateKeyPEM is required", payment.ErrInvalidConfig)
	}
	if c.AlipayPublicKeyPEM == "" && c.AlipayPublicCertPath == "" {
		return fmt.Errorf("%w: alipay AlipayPublicKeyPEM or AlipayPublicCertPath is required", payment.ErrInvalidConfig)
	}
	// 证书模式的完整性校验(照官方 SDK 的判定顺序):
	// 官方 AbstractAlipayClient 只要看到支付宝公钥证书,就直接要求根证书和应用证书,
	// 少一个就抛 ROOT_CERT_EMPTY_ERROR / APP_CERT_EMPTY_ERROR。
	// 这里提前拦下来,免得配置保存成功、跑起来才发现每个请求都 40002。
	if c.AlipayPublicCertPath != "" {
		if c.AppCertPath == "" {
			return fmt.Errorf("%w: alipay 证书模式必须配置应用公钥证书(app_cert_path),它决定请求里的 app_cert_sn",
				payment.ErrInvalidConfig)
		}
		if c.RootCertPath == "" {
			return fmt.Errorf("%w: alipay 证书模式必须配置支付宝根证书(root_cert_path),它决定请求里的 alipay_root_cert_sn",
				payment.ErrInvalidConfig)
		}
	}
	if c.SignType == "" {
		c.SignType = "RSA2"
	}
	if c.SignType != "RSA2" {
		return fmt.Errorf("%w: alipay only supports RSA2, got %s", payment.ErrInvalidConfig, c.SignType)
	}
	if c.Gateway == "" {
		c.Gateway = DefaultGateway
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	return nil
}
