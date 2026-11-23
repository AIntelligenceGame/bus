// Package main 演示 bus/payment 的用法。
//
// 运行前请通过环境变量提供渠道密钥,未配置时示例会直接跳过对应演示:
//
//	WECHAT_APP_ID / WECHAT_MCH_ID / WECHAT_SERIAL_NO / WECHAT_PRIVATE_KEY / WECHAT_API_V3_KEY
//	ALIPAY_APP_ID / ALIPAY_PRIVATE_KEY / ALIPAY_PUBLIC_KEY
//	STRIPE_SECRET_KEY / STRIPE_PUBLISHABLE_KEY / STRIPE_WEBHOOK_SECRET
//	  —— 两个钱包共用一个 Stripe 账户时配这三个就够;
//	     要 Apple Pay / Google Pay 各用各的 Stripe 账户,改配
//	     APPLE_STRIPE_SECRET_KEY / GOOGLE_STRIPE_SECRET_KEY(同名变量优先)
//
//	go run ./payment/examples/basic
//
// 校验:本文件引用了所有渠道包的 API,可用于快速检查包间签名是否一致。
//
// 安全提示:环境变量只是**示例**的取密钥方式,便于一条命令跑通。
// 生产环境不要把密钥写在代码或镜像里,建议:
//   - 用平台( bus/payment/platform )在网页上配置,密钥加密入库、页面不回显、敏感操作二次验证;
//   - 或者用 KMS / K8s Secret 以文件形式挂载,再由程序读取文件。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/pay"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/applepay"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
	"github.com/AIntelligenceGame/bus/payment/providers/googlepay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

func main() {
	ctx := context.Background()

	client, err := pay.New(pay.Config{
		Wechat:    wechatConfig(),
		Alipay:    alipayConfig(),
		ApplePay:  applePayConfig(),
		GooglePay: googlePayConfig(),
	})
	if err != nil {
		log.Fatalf("创建支付客户端失败: %v", err)
	}
	fmt.Println("已启用渠道:", client.Channels())

	order := pay.Order{
		OutTradeNo: fmt.Sprintf("BIZ20260101%06d", time.Now().UnixNano()%1000000),
		Subject:    "iPhone 17 Pro 256G",
		Body:       "商品订单",
		Amount:     19900, // 单位:分,即 199.00 元
		ClientIP:   "127.0.0.1",
		Attach:     "shop-1001",
	}

	// 1)扫码支付:返回二维码内容,可直接渲染给用户扫码
	session, err := client.PayQRCode(ctx, pay.Wechat, order)
	if err != nil {
		fmt.Println("微信扫码支付失败(未配置密钥或网络异常):", err)
	} else {
		fmt.Printf("微信扫码支付二维码内容: %s\n", session.QRCode)
		png, err := pay.QRCodePNG(session, 8)
		if err != nil {
			fmt.Println("二维码渲染失败:", err)
		} else {
			fmt.Printf("二维码 PNG 已生成, %d 字节\n", len(png))
		}
	}

	// 2)移动端拉起支付:把 Session.AppParams 交给客户端 SDK
	appSession, err := client.PayApp(ctx, pay.Alipay, order)
	if err != nil {
		fmt.Println("支付宝 APP 支付失败:", err)
	} else {
		fmt.Printf("支付宝 APP 支付 orderString: %v\n", appSession.AppParams["orderString"])
	}

	// 3)钱包支付(Apple Pay / Google Pay)
	walletSession, err := client.PayWallet(ctx, pay.ApplePay, order)
	if err != nil {
		fmt.Println("Apple Pay 会话创建失败:", err)
	} else {
		fmt.Printf("Apple Pay 客户端令牌: %s\n", walletSession.Token)
	}

	// 4)查单与退款
	result, err := client.Query(ctx, pay.Wechat, order.OutTradeNo)
	if err != nil {
		fmt.Println("查询订单失败:", err)
	} else {
		fmt.Printf("订单状态: %s, 金额: %s\n", result.Status, payment.FormatCents(result.Amount))
	}

	refund, err := client.Refund(ctx, pay.Wechat, pay.RefundRequest{
		OutTradeNo:  order.OutTradeNo,
		OutRefundNo: order.OutTradeNo + "-R",
		Reason:      "用户申请退款",
	})
	if err != nil {
		fmt.Println("退款失败:", err)
	} else {
		fmt.Printf("退款单号: %s, 状态: %s\n", refund.RefundID, refund.Status)
	}

	// 5)回调处理示例:验签 -> 入账
	// notify, err := client.HandleNotify(ctx, pay.Wechat, req)
	// if err == nil && notify.Status == payment.TradeStatusSuccess {
	//     // 幂等入账:以 notify.OutTradeNo 为幂等键
	// }
}

// envFirst 依次取第一个非空的环境变量。
//
// 用途:钱包类渠道可以各自一套收单密钥(APPLE_STRIPE_* / GOOGLE_STRIPE_*),
// 也可以两个钱包共用一个 Stripe 账户(只配 STRIPE_*)——
// 前者优先,没配才回退到后面那个。
func envFirst(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// wechatConfig 从环境变量读取微信支付配置,未配置返回 nil。
func wechatConfig() *wechat.Config {
	if os.Getenv("WECHAT_APP_ID") == "" {
		return nil
	}
	return &wechat.Config{
		AppID:          os.Getenv("WECHAT_APP_ID"),
		MchID:          os.Getenv("WECHAT_MCH_ID"),
		SerialNo:       os.Getenv("WECHAT_SERIAL_NO"),
		PrivateKeyPEM:  os.Getenv("WECHAT_PRIVATE_KEY"),
		APIv3Key:       os.Getenv("WECHAT_API_V3_KEY"),
		NotifyURL:      "https://pay.example.com/notify/wechat",
		CertDir:        "./certs",
		AutoFetchCerts: true,
	}
}

// alipayConfig 从环境变量读取支付宝配置,未配置返回 nil。
func alipayConfig() *alipay.Config {
	if os.Getenv("ALIPAY_APP_ID") == "" {
		return nil
	}
	return &alipay.Config{
		AppID:              os.Getenv("ALIPAY_APP_ID"),
		PrivateKeyPEM:      os.Getenv("ALIPAY_PRIVATE_KEY"),
		AlipayPublicKeyPEM: os.Getenv("ALIPAY_PUBLIC_KEY"),
		NotifyURL:          "https://pay.example.com/notify/alipay",
		ReturnURL:          "https://pay.example.com/result",
	}
}

// applePayConfig 从环境变量读取 Apple Pay 配置,未配置返回 nil。
func applePayConfig() *applepay.Config {
	secret := envFirst("APPLE_STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY")
	if secret == "" {
		return nil
	}
	stripe, err := gateway.NewStripe(&gateway.StripeConfig{
		SecretKey:      secret,
		PublishableKey: envFirst("APPLE_STRIPE_PUBLISHABLE_KEY", "STRIPE_PUBLISHABLE_KEY"),
		WebhookSecret:  envFirst("APPLE_STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"),
	})
	if err != nil {
		return nil
	}
	return &applepay.Config{
		MerchantID:  os.Getenv("APPLE_MERCHANT_ID"),
		DisplayName: "示例商城",
		DomainName:  "pay.example.com",
		Gateway:     stripe,
	}
}

// googlePayConfig 从环境变量读取 Google Pay 配置,未配置返回 nil。
func googlePayConfig() *googlepay.Config {
	secret := envFirst("GOOGLE_STRIPE_SECRET_KEY", "STRIPE_SECRET_KEY")
	if secret == "" {
		return nil
	}
	stripe, err := gateway.NewStripe(&gateway.StripeConfig{
		SecretKey:      secret,
		PublishableKey: envFirst("GOOGLE_STRIPE_PUBLISHABLE_KEY", "STRIPE_PUBLISHABLE_KEY"),
		WebhookSecret:  envFirst("GOOGLE_STRIPE_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"),
	})
	if err != nil {
		return nil
	}
	return &googlepay.Config{
		MerchantID:   os.Getenv("GOOGLE_MERCHANT_ID"),
		MerchantName: "示例商城",
		Gateway:      stripe,
	}
}
