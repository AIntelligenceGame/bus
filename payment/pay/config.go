package pay

import (
	"fmt"

	"github.com/spf13/viper"

	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/applepay"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
	"github.com/AIntelligenceGame/bus/payment/providers/googlepay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

// Config 聚合各渠道配置,只填需要使用的渠道即可。
//
// 支持从 YAML/JSON 配置文件载入(见 LoadConfigFile)。
type Config struct {
	// Wechat 微信支付配置。
	Wechat *wechat.Config `mapstructure:"wechat"`
	// Alipay 支付宝配置。
	Alipay *alipay.Config `mapstructure:"alipay"`
	// ApplePay Apple Pay 配置。
	ApplePay *applepay.Config `mapstructure:"applepay"`
	// GooglePay Google Pay 配置。
	GooglePay *googlepay.Config `mapstructure:"googlepay"`
}

// walletFileConfig 钱包渠道的配置文件结构。
//
// 钱包需要绑定收单机构,配置文件里通过 stripe 段声明,加载时自动构造网关。
type walletFileConfig struct {
	MerchantID  string                `mapstructure:"merchant_id"`
	DisplayName string                `mapstructure:"display_name"`
	DomainName  string                `mapstructure:"domain_name"`
	Currency    string                `mapstructure:"currency"`
	Stripe      *gateway.StripeConfig `mapstructure:"stripe"`
}

// fileConfig 支付配置文件结构。
type fileConfig struct {
	Wechat    *wechat.Config    `mapstructure:"wechat"`
	Alipay    *alipay.Config    `mapstructure:"alipay"`
	ApplePay  *walletFileConfig `mapstructure:"applepay"`
	GooglePay *walletFileConfig `mapstructure:"googlepay"`
}

// 配置文件示例:
//
//	payment:
//	  wechat:
//	    app_id: "wx1234567890"
//	    mch_id: "1900000109"
//	    serial_no: "1DDE55AD98ED71D6EDD4A4A16996DE7B47773A8C"
//	    private_key_pem: "-----BEGIN PRIVATE KEY-----\n..."
//	    api_v3_key: "0123456789abcdef0123456789abcdef"
//	    notify_url: "https://pay.example.com/notify/wechat"
//	    cert_dir: "./certs"
//	    auto_fetch_certs: true
//	  alipay:
//	    app_id: "2021000000000000"
//	    private_key_pem: "-----BEGIN RSA PRIVATE KEY-----\n..."
//	    alipay_public_key_pem: "-----BEGIN PUBLIC KEY-----\n..."
//	    notify_url: "https://pay.example.com/notify/alipay"
//	  applepay:
//	    merchant_id: "merchant.com.example"
//	    display_name: "示例商城"
//	    stripe:
//	      secret_key: "sk_test_xxx"
//	      publishable_key: "pk_test_xxx"
//	      webhook_secret: "whsec_xxx"
//
// LoadConfigFile 从 YAML/JSON 配置文件载入支付客户端。
//
// 文件路径需带扩展名(.yaml / .yml / .json / .toml)。
func LoadConfigFile(path string) (*Client, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return New(*cfg)
}

// LoadConfig 从 YAML/JSON 配置文件载入各渠道配置(高级用法)。
//
// 文件路径需带扩展名(.yaml / .yml / .json / .toml)。
//
// 配置文件有两种常见写法,两种都支持:
//
//	wechat:      # 渠道直接写在根下
//	  app_id: ...
//
//	payment:     # 或者像 config.example.yaml 那样套一层(payment:)
//	  wechat:
//	    app_id: ...
//
// 根下没有渠道字段时自动下沉到 payment 段 —— 否则照着示例文件
// LoadConfigFile("./payment/config.yaml") 会**一个渠道都没读到**,
// 报的还是 "channel not registered",根本看不出是配置结构不对。
func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("pay: read config %s: %w", path, err)
	}
	if !v.IsSet("wechat") && !v.IsSet("alipay") &&
		!v.IsSet("applepay") && !v.IsSet("googlepay") {
		if sub := v.Sub("payment"); sub != nil {
			v = sub
		}
	}
	var fc fileConfig
	if err := v.Unmarshal(&fc); err != nil {
		return nil, fmt.Errorf("pay: parse config %s: %w", path, err)
	}
	cfg := &Config{
		Wechat: fc.Wechat,
		Alipay: fc.Alipay,
	}
	if fc.ApplePay != nil && fc.ApplePay.Stripe != nil {
		stripe, err := gateway.NewStripe(fc.ApplePay.Stripe)
		if err != nil {
			return nil, fmt.Errorf("pay: init stripe gateway for applepay: %w", err)
		}
		cfg.ApplePay = &applepay.Config{
			MerchantID:  fc.ApplePay.MerchantID,
			DisplayName: fc.ApplePay.DisplayName,
			DomainName:  fc.ApplePay.DomainName,
			Currency:    fc.ApplePay.Currency,
			Gateway:     stripe,
		}
	}
	if fc.GooglePay != nil && fc.GooglePay.Stripe != nil {
		stripe, err := gateway.NewStripe(fc.GooglePay.Stripe)
		if err != nil {
			return nil, fmt.Errorf("pay: init stripe gateway for googlepay: %w", err)
		}
		cfg.GooglePay = &googlepay.Config{
			MerchantID:   fc.GooglePay.MerchantID,
			MerchantName: fc.GooglePay.DisplayName,
			Currency:     fc.GooglePay.Currency,
			Gateway:      stripe,
		}
	}
	return cfg, nil
}
