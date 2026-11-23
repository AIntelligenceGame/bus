// Package wechat 实现微信支付 APIv3 的支付能力。
//
// 支持场景:
//
//	TradeTypeNative 扫码支付(返回 code_url,渲染成二维码给手机扫)
//	TradeTypeApp    APP 拉起支付(返回 prepayid 与客户端签名参数)
//	TradeTypeJSAPI  微信内网页支付(公众号/小程序)
//	TradeTypeH5     微信 H5 支付(非微信内浏览器,返回 h5_url)
//
// 密钥准备:
//   - APIv3 密钥:32 字节字符串,用于回调解密与平台证书下载;
//   - 商户 API 证书:apiclient_key.pem(私钥)与 apiclient_cert.pem;
//   - SerialNo:apiclient_cert.pem 的证书序列号;
//   - 平台证书:可用 AutoFetchCerts 自动下载并缓存到 CertDir。
package wechat

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 默认配置值。
const (
	// DefaultBaseURL 微信支付生产环境地址。
	DefaultBaseURL = "https://api.mch.weixin.qq.com"
	// defaultTimeout 默认 HTTP 超时。
	defaultTimeout = 10 * time.Second
)

// Config 微信支付配置(APIv3)。
type Config struct {
	// AppID 公众号/小程序/开放平台 AppID(移动端 APP 支付填 APP 的 AppID)。
	AppID string `mapstructure:"app_id" json:"app_id"`
	// MchID 商户号。
	MchID string `mapstructure:"mch_id" json:"mch_id"`
	// SerialNo 商户 API 证书序列号。
	SerialNo string `mapstructure:"serial_no" json:"serial_no"`
	// PrivateKeyPEM 商户 API 证书私钥 PEM 内容(PKCS#1 或 PKCS#8)。
	PrivateKeyPEM string `mapstructure:"private_key_pem" json:"private_key_pem"`
	// APIv3Key APIv3 密钥,必须为 32 字节。
	APIv3Key string `mapstructure:"api_v3_key" json:"api_v3_key"`
	// NotifyURL 默认异步回调地址,订单未指定时使用。
	NotifyURL string `mapstructure:"notify_url" json:"notify_url"`
	// BaseURL 接口地址,默认 https://api.mch.weixin.qq.com。
	BaseURL string `mapstructure:"base_url" json:"base_url"`
	// HTTPClient 自定义 HTTP 客户端(不参与配置解析)。
	HTTPClient *http.Client `mapstructure:"-"`
	// PlatformCerts 平台证书:序列号 -> 证书。手动指定时不会自动下载。
	PlatformCerts map[string]*x509.Certificate `mapstructure:"-"`
	// CertDir 平台证书缓存目录,用于自动下载后落盘。
	CertDir string `mapstructure:"cert_dir" json:"cert_dir"`
	// AutoFetchCerts 为 true 且本地没有可用平台证书时,自动调用 /v3/certificates 下载。
	AutoFetchCerts bool `mapstructure:"auto_fetch_certs" json:"auto_fetch_certs"`
	// Timeout HTTP 超时,默认 10s。
	Timeout time.Duration `mapstructure:"timeout"`
}

// validate 校验配置。
func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil wechat config", payment.ErrInvalidConfig)
	}
	if c.AppID == "" {
		return fmt.Errorf("%w: wechat AppID is required", payment.ErrInvalidConfig)
	}
	if c.MchID == "" {
		return fmt.Errorf("%w: wechat MchID is required", payment.ErrInvalidConfig)
	}
	if c.SerialNo == "" {
		return fmt.Errorf("%w: wechat SerialNo is required", payment.ErrInvalidConfig)
	}
	if c.PrivateKeyPEM == "" {
		return fmt.Errorf("%w: wechat PrivateKeyPEM is required", payment.ErrInvalidConfig)
	}
	if len(c.APIv3Key) != 32 {
		return fmt.Errorf("%w: wechat APIv3Key must be 32 bytes", payment.ErrInvalidConfig)
	}
	if c.BaseURL == "" {
		c.BaseURL = DefaultBaseURL
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	return nil
}
