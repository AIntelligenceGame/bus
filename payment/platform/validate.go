package platform

import (
	"fmt"
	"strings"

	"github.com/AIntelligenceGame/bus/payment"
)

// 下单参数校验:按各渠道**官方接口的字段约束**先拦一道。
//
// 为什么要自己先校验:
// 官方的约束散落在各家文档里,商户填错了只会拿到渠道一句
// "invalid parameter" / "参数不合法",定位成本全推给商户。
// 平台按同一套约束先拦一遍,报错直接说明「哪个字段、超了哪条限制」,
// 商户一看就知道改哪里 —— 而不是去翻微信/支付宝文档。
//
// 只卡**与商户等级无关**的那部分约束(长度、字符集、必填):
//
//   - 不卡「单笔金额上限」—— 那是跟着商户签约等级浮动的,
//     平台一刀切会把本来能付的大额商户挡在门外;
//   - 不卡「商户号白名单」之类只有渠道侧才知道的东西。
//
// 金额本身的合法性(> 0、必须是整数分)在 payRequest 的类型上就已经保证了:
// Amount 是 int64(分),前端传 19.9(元)会在 JSON 反序列化阶段直接被拒,
// 不会变成「19 分」这种 100 倍误差。

// 约束本身在 payment 包里(见 payment/constraints.go):
// 平台和渠道实现共用一份,免得改了一边忘了另一边。
//
// 这里保留同名的一层薄封装,是为了让平台这一层的调用点读起来清楚,
// 也让「平台拦的」和「渠道兜的」用的是同一条规则。

// validatePayRequest 按渠道的官方约束校验下单参数。
//
// 返回的 error 是给商户看的:要说清楚是哪个字段、违反了哪条限制。
// 校验不通过时**不能**已经写了订单记录 —— 所以它必须在落库之前调用。
func validatePayRequest(
	channel payment.Channel,
	tradeType payment.TradeType,
	supported []payment.TradeType,
	req payRequest,
) error {
	// 1. 交易类型必须是该渠道支持的。
	//
	// 放在落库之前校验,是为了不留下「一条永远付不成的半成品订单」——
	// 商户看到订单列表里多了一条垃圾记录,会以为平台出过单。
	if len(supported) > 0 && !tradeTypeSupported(supported, tradeType) {
		names := make([]string, 0, len(supported))
		for _, t := range supported {
			names = append(names, string(t))
		}
		return fmt.Errorf("渠道 %s 不支持交易类型 %s(支持:%s)",
			channel, tradeType, strings.Join(names, ", "))
	}
	// 2. 商户订单号。
	if req.OutTradeNo != "" {
		if err := validateOutTradeNo(channel, req.OutTradeNo); err != nil {
			return err
		}
	}
	// 3. 订单标题。
	if req.Subject != "" {
		if err := validateSubject(channel, req.Subject); err != nil {
			return err
		}
	}
	// 4. 商户附加数据。
	if req.Attach != "" {
		if err := validateAttach(channel, req.Attach); err != nil {
			return err
		}
	}
	// 5. 金额:必须是正整数分。
	//
	// 微信/支付宝的接口都要求金额是**整数分**(微信是数字,支付宝是字符串),
	// 平台内部统一用 int64 分,这里只挡非正的。
	if req.Amount <= 0 {
		return fmt.Errorf("金额必须大于 0(单位:分)")
	}
	return nil
}

// validateOutTradeNo 校验商户订单号(长度 + 字符集)。
//
// 规则见 payment.ValidateOutTradeNo:微信下单类是 6~32 个字符,
// 支付宝最多 64 个字符,两边都不允许空格、中文与标点。
func validateOutTradeNo(channel payment.Channel, no string) error {
	return payment.ValidateOutTradeNo(channel, no)
}

// validateSubject 校验订单标题长度(按字符数算,不是字节数)。
func validateSubject(channel payment.Channel, subject string) error {
	return payment.ValidateSubject(channel, subject)
}

// validateAttach 校验商户附加数据长度。
//
// 微信 attach 最多 128 字符;支付宝发出去的是 UrlEncoded 后的
// passback_params,最多 512 字符。
func validateAttach(channel payment.Channel, attach string) error {
	return payment.ValidateAttach(channel, attach)
}

// tradeTypeSupported 判断渠道是否支持该交易类型。
func tradeTypeSupported(supported []payment.TradeType, tt payment.TradeType) bool {
	for _, t := range supported {
		if t == tt {
			return true
		}
	}
	return false
}
