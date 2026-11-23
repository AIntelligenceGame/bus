// Command multiapp 演示「一个商户有多个产品(App)」时怎么接入支付平台。
//
// # 为什么应用维度要走平台,而不是自己写
//
// 库(`payment/pay`)是「一个商户一套密钥」的最简模型,**没有应用维度**。
// 「每个产品用自己的一套渠道密钥」这件事平台已经实现了:
//
//  1. 控制台「应用管理」页建应用(比如「iOS 客户端」「安卓 App」);
//  2. 控制台「渠道配置」页把顶部的「配置范围」切到这个应用,给这个应用配**专属**渠道密钥。
//     四个渠道都支持:微信 AppID / 商户号、支付宝 AppID、Apple Pay / Google Pay 的收单账户。
//     哪个渠道没单独配,就回退到全局那套(不会因为漏配就整个应用用不了);
//  3. 控制台「开放接口」页建密钥,建的时候**绑定应用**。
//
// 之后业务后端拿这把密钥下单就行,**app_id 不用传**:
//
//   - 密钥绑了应用 A → 订单自动归到 A;退款 / 查单 / 关单 / 回调验签
//     自动用「当初下这笔单的那套凭证」,不会串到别的产品;
//   - 密钥没绑应用 → 下单**必须**带 app_id(账户开了「订单必须绑定应用」时,
//     不带直接 400);
//   - 密钥绑了 A 却传 app_id=B → **400**,不会静默落到 B。
//
// # 运行
//
//	cd bus && go run ./payment/examples/multiapp
//
// 需要一个**已经在跑**的平台(另开一个终端 `cd bus && go run ./payment/examples/platform`)。
//
// # 环境变量
//
//	PLATFORM_BASE   平台地址,默认 http://127.0.0.1:8089
//	PAY_KEY_ID      开放接口密钥 ID(pk_xxx)
//	PAY_KEY_SECRET  开放接口密钥明文(sk_xxx,创建时只显示一次)
//	PAY_CHANNEL     下单渠道,默认 wechat(wechat / alipay / applepay / googlepay)
//	PAY_TRADE_TYPE  交易类型,留空按渠道自动推断(app / native / wallet / jsapi)
//	PAY_AMOUNT      金额(单位:分),默认 19900
//
// # 说明
//
// 这个示例**不碰密钥**:渠道密钥在控制台配、加密入库、页面不回显。
// 示例里只有开放接口密钥,而且是商户自己的服务端拿来做签名的 ——
// 千万别把它塞进 App 客户端里。
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/AIntelligenceGame/bus/payment/platform/openapi"
)

// config 示例的配置(全部来自环境变量)。
type config struct {
	// Base 平台对外地址。
	Base string
	// KeyID 开放接口密钥 ID。
	KeyID string
	// Secret 开放接口密钥明文。
	Secret string
	// Channel 下单用的渠道。
	Channel string
	// TradeType 交易类型,留空按渠道自动推断。
	TradeType string
	// Amount 金额(分)。
	Amount int64
	// OutTradeNo 商户订单号,留空自动生成。
	OutTradeNo string
}

// configFromEnv 从环境变量读配置。
func configFromEnv() config {
	cfg := config{
		Base:       envOr("PLATFORM_BASE", "http://127.0.0.1:8089"),
		KeyID:      os.Getenv("PAY_KEY_ID"),
		Secret:     os.Getenv("PAY_KEY_SECRET"),
		Channel:    envOr("PAY_CHANNEL", "wechat"),
		TradeType:  os.Getenv("PAY_TRADE_TYPE"),
		OutTradeNo: os.Getenv("PAY_OUT_TRADE_NO"),
	}
	cfg.Amount = 19900
	if v := os.Getenv("PAY_AMOUNT"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.Amount = n
		}
	}
	return cfg
}

// missingEnv 返回还缺哪些必填的环境变量(全齐了返回空切片)。
func (c config) missingEnv() []string {
	var missing []string
	if c.KeyID == "" {
		missing = append(missing, "PAY_KEY_ID")
	}
	if c.Secret == "" {
		missing = append(missing, "PAY_KEY_SECRET")
	}
	return missing
}

// result 示例跑完之后的可核对结果(给测试用)。
type result struct {
	// OrderNo 下单返回的商户订单号。
	OrderNo string
	// OrderAppID 这笔订单归属的应用(平台按密钥绑定自动填)。
	OrderAppID string
	// MismatchErr 故意传错应用时平台返回的错误(为空 = 没报错)。
	MismatchErr string
	// Groups 按应用分组的统计。
	Groups []openapi.StatGroup
}

func main() {
	cfg := configFromEnv()
	if missing := cfg.missingEnv(); len(missing) > 0 {
		log.Fatalf("缺少环境变量: %s\n\n"+
			"请先在平台控制台「开放接口」页建一把密钥(建的时候可以绑定应用),\n"+
			"把密钥 ID 与明文填进环境变量再跑。\n"+
			"平台没起的话先另开一个终端:cd bus && go run ./payment/examples/platform",
			strings.Join(missing, " / "))
	}

	client := openapi.New(cfg.Base, cfg.KeyID, cfg.Secret)
	res, err := run(context.Background(), os.Stdout, client, cfg)
	if err != nil {
		log.Fatalf("示例执行失败: %v", err)
	}
	if res.OrderNo == "" {
		fmt.Println("\n跑完了(这笔单没下成,原因见上面)。")
		return
	}
	fmt.Printf("\n完成。订单 %s 归属应用 %s。\n", res.OrderNo, orDefault(res.OrderAppID, "(未分类)"))
}

// run 跑一遍完整流程,过程写到 out。
//
// 顺序是刻意的:先演示「应用指定」是怎么被强制的,再走正常下单。
// 因为平台默认开着「订单必须绑定应用」,一把没绑应用的密钥下单会被直接拒,
// 这条规则正是这个示例要讲清楚的东西。
func run(ctx context.Context, out io.Writer, client *openapi.Client, cfg config) (*result, error) {
	res := &result{}

	// ---- 1)「应用指定」是被强制的:故意传一个别的应用下单 --------------------------
	fmt.Fprintf(out, "== 1) 应用指定是被强制的:故意传一个别的应用下单 ==\n")
	_, mismatchErr := client.Pay(ctx, openapi.PayRequest{
		Channel:   cfg.Channel,
		TradeType: cfg.TradeType,
		Subject:   "故意传错应用",
		Amount:    cfg.Amount,
		AppID:     "APP_NOT_THE_BOUND_ONE",
	})
	if mismatchErr != nil {
		res.MismatchErr = mismatchErr.Error()
		fmt.Fprintf(out, "  被平台拒绝(这是对的): %v\n", mismatchErr)
	} else {
		fmt.Fprintf(out, "  ⚠️ 竟然下单成功了 —— 这把密钥没绑应用,app_id 只是普通字段\n")
	}

	// ---- 2)正常下单:不传 app_id,靠密钥绑定自动归属应用 --------------------------
	fmt.Fprintf(out, "\n== 2) 下单(不传 app_id,靠密钥绑定自动归属应用)==\n")
	payReq := openapi.PayRequest{
		Channel:    cfg.Channel,
		TradeType:  cfg.TradeType,
		OutTradeNo: cfg.OutTradeNo,
		Subject:    "多产品示例订单",
		Amount:     cfg.Amount,
	}
	payResult, payErr := client.Pay(ctx, payReq)
	if payErr != nil {
		// 渠道没配是最常见的原因(还没在控制台配这个应用的渠道密钥)。
		// 这不该让整个示例中断 —— 上面那步已经把「应用指定」的规则演示完了。
		fmt.Fprintf(out, "  下单没成功: %v\n", payErr)
		fmt.Fprintf(out, "  (最常见的原因是这个渠道还没配密钥:去控制台「渠道配置」页配一下,"+
			"或者把「配置范围」切到密钥绑定的那个应用再配)\n")
	} else {
		res.OrderNo = payResult.OrderNo
		fmt.Fprintf(out, "  订单号: %s  金额: %d 分(%s 元)\n", payResult.OrderNo, payResult.Amount, payResult.Yuan)
		fmt.Fprintf(out, "  回调地址: %s\n", payResult.NotifyURL)
		for _, u := range payResult.Usage {
			fmt.Fprintf(out, "  · %s\n", u)
		}
		if cfg.Channel == "applepay" || cfg.Channel == "googlepay" {
			fmt.Fprintf(out, "  (钱包渠道还要客户端授权后回传 payment_token,再调 /api/open/wallet/charge 扣款)\n")
		}
	}

	// ---- 3)这笔单归属哪个应用 --------------------------------------------------
	fmt.Fprintf(out, "\n== 3) 订单归属的应用 ==\n")
	if res.OrderNo != "" {
		// 用平台自己的订单记录(不向渠道发请求),看 app_id 落在哪。
		detail, err := client.Order(ctx, res.OrderNo)
		if err != nil {
			fmt.Fprintf(out, "  查订单记录失败: %v\n", err)
		} else {
			res.OrderAppID, _ = detail["app_id"].(string)
			fmt.Fprintf(out, "  app_id: %s\n", orDefault(res.OrderAppID, "(未分类)"))
		}
	}

	// ---- 4)订单列表:看各笔单落在哪个应用 ---------------------------------------
	fmt.Fprintf(out, "\n== 4) 最近订单(看 app_id)==\n")
	orders, err := client.Orders(ctx, openapi.OrdersQuery{Size: 5})
	if err != nil {
		fmt.Fprintf(out, "  拉订单列表失败: %v\n", err)
	} else {
		for _, o := range orders.Orders {
			fmt.Fprintf(out, "  %s  渠道=%s  应用=%s  金额=%d 分  状态=%s\n",
				o.OutTradeNo, o.Channel, orDefault(o.AppID, "(未分类)"), o.Amount, o.Status)
			if o.OutTradeNo == res.OrderNo {
				res.OrderAppID = o.AppID
			}
		}
	}

	// ---- 5)按应用分组统计:各产品的钱分开算 -------------------------------------
	fmt.Fprintf(out, "\n== 5) 按应用分组统计==\n")
	stats, err := client.StatsGrouped(ctx, openapi.StatsQuery{GroupBy: "app"})
	if err != nil {
		fmt.Fprintf(out, "  拉分组统计失败: %v\n", err)
	} else {
		res.Groups = stats.Groups
		for _, g := range stats.Groups {
			fmt.Fprintf(out, "  应用 %s(%s): 订单 %d 笔, 成功金额 %d 分\n",
				g.Label, g.Key, g.OrderCount, g.TotalAmount)
		}
		if stats.Truncated {
			fmt.Fprintf(out, "  ⚠️ 结果被截断(按天分组时平台会限量扫描),别拿它当全量算\n")
		}
	}

	return res, nil
}

// orDefault 空串时给个默认显示。
func orDefault(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

// envOr 读环境变量,没配就用默认值。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
