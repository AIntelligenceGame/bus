package payment

import (
	"fmt"
	"net/url"
	"unicode/utf8"
)

// 各渠道**官方**的字段约束,以及按这些约束做的校验。
//
// 为什么放在 payment 包(而不是 platform 包或各渠道包里):
// 同一份约束要卡两道 ——
//  1. 平台在下单**之前**先拦一道,给商户一个说得清楚的 400;
//  2. 渠道实现再兜一道,给**直接用库**(不经过平台)的调用方一个说得清楚的 error。
//
// 两边各写一份的话,迟早会走样(改了一边忘了另一边),所以约束只有这一处。
//
// 数字与字符集的出处(官方文档):
//
//	微信支付 APIv3:
//	  - description:string[1,127](**按字符**算,一个汉字算一个)
//	  - out_trade_no:string[6,32](下单类接口;查单类是 [1,32])
//	  - attach:string(128)
//	  - notify_url:string(255)
//	支付宝:
//	  - subject:string(256)
//	  - body:string(128)(biz_content 顶层的那个 body;
//	    「1000 字符」说的是 goods_detail[].body,不是这里)
//	  - passback_params:string(512)
//	  - out_trade_no:string(64)
const (
	// WechatDescriptionMax 微信 description(商品描述)最大字符数。
	WechatDescriptionMax = 127
	// WechatOutTradeNoMin 微信商户订单号最小字符数(下单类接口)。
	WechatOutTradeNoMin = 6
	// WechatOutTradeNoMax 微信商户订单号最大字符数。
	WechatOutTradeNoMax = 32
	// WechatAttachMax 微信 attach(附加数据)最大字符数。
	WechatAttachMax = 128
	// AlipaySubjectMax 支付宝 subject(订单标题)最大字符数。
	AlipaySubjectMax = 256
	// AlipayBodyMax 支付宝 body(商品描述)最大字符数。
	AlipayBodyMax = 128
	// AlipayAttachMax 支付宝 passback_params(商户附加数据)最大字符数。
	AlipayAttachMax = 512
	// AlipayOutTradeNoMax 支付宝商户订单号最大字符数。
	AlipayOutTradeNoMax = 64
)

// WechatDescription 拼出微信实际发出去的 description。
//
// 微信的 description 是**一个字段**,平台发过去的就是这个拼接结果 ——
// 所以校验也得校验拼接后的长度,不能只校验其中一段。
func WechatDescription(subject, body string) string {
	if body == "" {
		return subject
	}
	return subject + " - " + body
}

// AlipayPassbackParams 把商户的附加数据编码成支付宝的 passback_params。
//
// 官方要求:passback_params 必须 UrlEncoded,且**不能是 JSON 格式**
// (值里带双引号会直接报「订单参数异常」),没编码会报「订单信息无法识别」。
//
// 顺带说明: 商户附加数据在支付宝那边的正确字段就是 passback_params,
// **不是** extend_params.sys_service_provider_id —— 后者是「服务商自己的 PID」,
// 用它做返佣计算,填错���返佣损失不可申诉。
func AlipayPassbackParams(attach string) string {
	if attach == "" {
		return ""
	}
	return url.QueryEscape(attach)
}

// ValidateOutTradeNo 校验商户订单号(按渠道的官方约束)。
//
// no 为空返回 nil —— 平台允许商户不传订单号(由平台生成),
// 「必填」由 Order.Validate() 负责。
func ValidateOutTradeNo(channel Channel, no string) error {
	if no == "" {
		return nil
	}
	switch channel {
	case ChannelWechat:
		n := utf8.RuneCountInString(no)
		if n < WechatOutTradeNoMin {
			return fmt.Errorf("商户订单号 %s 过短(%d 个字符),渠道 %s 至少 %d 个字符",
				no, n, channel, WechatOutTradeNoMin)
		}
		if n > WechatOutTradeNoMax {
			return fmt.Errorf("商户订单号 %s 过长(%d 个字符),渠道 %s 最多 %d 个字符",
				no, n, channel, WechatOutTradeNoMax)
		}
		if bad := firstBadRune(no, wechatTradeNoAllowed); bad >= 0 {
			return fmt.Errorf("商户订单号 %q 含有非法字符(第 %d 个字符),渠道 %s 只允许数字、字母与 _ - *",
				no, bad+1, channel)
		}
	case ChannelAlipay:
		n := utf8.RuneCountInString(no)
		if n > AlipayOutTradeNoMax {
			return fmt.Errorf("商户订单号 %s 过长(%d 个字符),渠道 %s 最多 %d 个字符",
				no, n, channel, AlipayOutTradeNoMax)
		}
		if bad := firstBadRune(no, alipayTradeNoAllowed); bad >= 0 {
			return fmt.Errorf("商户订单号 %q 含有非法字符(第 %d 个字符),渠道 %s 只允许数字、字母与 _",
				no, bad+1, channel)
		}
	}
	return nil
}

// ValidateSubject 校验订单标题长度(按**字符数**算,不是字节数)。
func ValidateSubject(channel Channel, subject string) error {
	if subject == "" {
		return nil
	}
	max := 0
	switch channel {
	case ChannelWechat:
		max = WechatDescriptionMax
	case ChannelAlipay:
		max = AlipaySubjectMax
	default:
		return nil
	}
	n := utf8.RuneCountInString(subject)
	if n > max {
		return fmt.Errorf("订单标题过长(%d 个字符),渠道 %s 最多 %d 个字符", n, channel, max)
	}
	return nil
}

// ValidateOrderBody 校验订单描述(body)长度。
//
// 微信的 description 由 subject + body 拼成,单独校验 body 没意义
// (见 ValidateChannelOrder 里对拼接结果的校验);
// 支付宝的 body 是独立字段,单独限长。
func ValidateOrderBody(channel Channel, body string) error {
	if body == "" {
		return nil
	}
	switch channel {
	case ChannelAlipay:
		n := utf8.RuneCountInString(body)
		if n > AlipayBodyMax {
			return fmt.Errorf("订单描述过长(%d 个字符),渠道 %s 最多 %d 个字符", n, channel, AlipayBodyMax)
		}
	}
	return nil
}

// ValidateAttach 校验商户附加数据(attach)的长度。
//
// 微信 attach:string(128);支付宝 passback_params:string(512)
// —— 支付宝那边算的是 **UrlEncoded 之后**的长度(发出去的就是编码后的串)。
func ValidateAttach(channel Channel, attach string) error {
	if attach == "" {
		return nil
	}
	switch channel {
	case ChannelWechat:
		n := utf8.RuneCountInString(attach)
		if n > WechatAttachMax {
			return fmt.Errorf("附加数据过长(%d 个字符),渠道 %s 最多 %d 个字符", n, channel, WechatAttachMax)
		}
	case ChannelAlipay:
		n := len(AlipayPassbackParams(attach))
		if n > AlipayAttachMax {
			return fmt.Errorf("附加数据编码后过长(%d 个字符),渠道 %s 最多 %d 个字符", n, channel, AlipayAttachMax)
		}
	}
	return nil
}

// ValidateChannelOrder 按渠道的官方约束校验整笔订单。
//
// 渠道实现在**发请求之前**调用它:直接用库的调用方拿到的就是这个 error,
// 而不是一个来自渠道的、看不出是哪个字段超了的 "invalid parameter"。
//
// 注意 order.Validate() 只管「非空 + 金额为正」,长度与字符集都不管。
func ValidateChannelOrder(channel Channel, o *Order) error {
	if o == nil {
		return fmt.Errorf("payment: order is nil")
	}
	if err := ValidateOutTradeNo(channel, o.OutTradeNo); err != nil {
		return err
	}
	if err := ValidateSubject(channel, o.Subject); err != nil {
		return err
	}
	if err := ValidateOrderBody(channel, o.Body); err != nil {
		return err
	}
	if err := ValidateAttach(channel, o.Attach); err != nil {
		return err
	}
	if channel == ChannelWechat {
		// 微信的 description 是拼出来的,必须校验**拼接后**的长度。
		desc := WechatDescription(o.Subject, o.Body)
		if n := utf8.RuneCountInString(desc); n > WechatDescriptionMax {
			return fmt.Errorf("订单标题与描述拼接后过长(%d 个字符),渠道 %s 的 description 最多 %d 个字符",
				n, channel, WechatDescriptionMax)
		}
	}
	return nil
}

// firstBadRune 返回第一个不满足 allowed 的字符的下标;全都合法时返回 -1。
func firstBadRune(s string, allowed func(rune) bool) int {
	for i, r := range s {
		if !allowed(r) {
			return i
		}
	}
	return -1
}

// wechatTradeNoAllowed 微信商户订单号允许的字符。
//
// 官方(直连商户)的说法是「只能是数字、大小写字母 _ - *」;
// 服务商/合单的文档写的是「数字、大小写字母 _ - | *」,查单文档写「_ - | * @」。
//
// 这里取各版本文档的**并集**,理由:
//   - 并集之外的字符(. / : # 之类)微信一定不收 —— 早拦下来是净收益;
//   - 并集之内的字符,万一某个微信版本不收,顶多是晚一步拿到渠道的原文报错,
//     不会把**本来能付成功**的订单挡在门外。
//
// 宁可漏拦,不可错拦。
func wechatTradeNoAllowed(r rune) bool {
	switch {
	case r >= '0' && r <= '9':
		return true
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r == '_' || r == '-' || r == '*' || r == '|' || r == '@':
		return true
	}
	return false
}

// alipayTradeNoAllowed 支付宝商户订单号允许的字符。
//
// 官方接口文档写的是「仅支持字母、数字、下划线」。但官方另一篇
// 「out_trade_no 参数详解」用的是「可以包含」的宽松说法,而且线上大量商户
// 用「日期-流水」这种**带横杠**的单号是能付成功的 —— 也没查到官方给出
// 「横杠被拒」的错误码。
//
// 所以这里保留横杠,只按官方拒掉其它符号(. / : 空格之类 —— 这些在支付宝
// 一定过不了)。同样是宁可漏拦,不可错拦。
func alipayTradeNoAllowed(r rune) bool {
	switch {
	case r >= '0' && r <= '9':
		return true
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r == '_' || r == '-':
		return true
	}
	return false
}
