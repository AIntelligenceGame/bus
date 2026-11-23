package platform

import (
	"strings"

	"github.com/AIntelligenceGame/bus/payment"
)

// 订单状态机 + 退款状态归一。
//
// 为什么需要状态机:
//
// 订单状态会被**多个来源**改写——渠道异步回调、主动查单、关单、退款。
// 如果每个入口都直接覆盖状态,就会出现「已退款的订单被一次迟到的
// 查询/回调改回 SUCCESS」这类脏数据:钱退掉了,状态却显示成功,
// 商户对账时钱和状态对不上,还可能据此再退一次。
//
// 规则只有一条:**钱到账优先,状态只能向前走,不能倒退**。

// advanceStatus 判断订单状态能否从 from 变到 to。
//
// 返回变更后的状态与是否生效;ok=false 表示这次变更不该生效,
// 调用方要保留原状态(只记日志),不要覆盖。
func advanceStatus(from, to payment.TradeStatus) (payment.TradeStatus, bool) {
	if to == "" {
		return from, false
	}
	if from == "" {
		// 空状态按「待支付」处理(老数据可能有空状态)。
		from = payment.TradeStatusPending
	}
	if from == to {
		return from, true
	}
	switch from {
	case payment.TradeStatusPending:
		// 待支付 → 成功 / 失败 / 已关闭。
		switch to {
		case payment.TradeStatusSuccess, payment.TradeStatusFailed, payment.TradeStatusClosed:
			return to, true
		}
	case payment.TradeStatusClosed, payment.TradeStatusFailed:
		// 已关闭 / 已失败 → 只有「钱到账」才允许翻案:
		// 用户在关单的同时把钱付了,渠道说成功,那就是成功了。
		if to == payment.TradeStatusSuccess {
			return to, true
		}
	case payment.TradeStatusSuccess:
		// 已成功 → 只能进退款态(退款只能由平台自己的退款流程推进)。
		switch to {
		case payment.TradeStatusRefunded, payment.TradeStatusPartiallyRefunded:
			return to, true
		}
	case payment.TradeStatusRefunded, payment.TradeStatusPartiallyRefunded:
		// 退款态 → 允许重复回调(幂等),但不能再退回「成功」:
		// 迟到的成功通知不能把已退款的订单改回去,否则会诱导商户二次退款。
		switch to {
		case payment.TradeStatusRefunded, payment.TradeStatusPartiallyRefunded:
			return to, true
		}
	}
	return from, false
}

// ---------------------------------------------------------------- 退款状态

// 平台侧退款流水状态。
//
// 退款是「先占位、再调渠道」:占位时渠道还没返回结果,所以必须有
// 一个「处理中」的状态,否则中途崩溃后这笔钱既查不到也退不了。
const (
	// refundStatusPending 已占位,渠道还没给结果。
	refundStatusPending = "PENDING"
	// refundStatusSuccess 退款成功(钱已退回)。
	refundStatusSuccess = "SUCCESS"
	// refundStatusProcessing 渠道处理中(钱已经提交出去,结果未定)。
	refundStatusProcessing = "PROCESSING"
	// refundStatusFailed 退款失败(钱没退,商户可以重试)。
	refundStatusFailed = "FAILED"
)

// refundCountedStatuses 计入「已退金额」的退款状态。
//
// 占位(PENDING)与处理中(PROCESSING)都计入:
// 钱一旦提交给渠道就可能已经出去了,这时再放一笔新的退款就是超额出款。
// 失败的(FAILED)不计入 —— 那笔钱没退,商户应该还能退。
//
// 空串是历史数据(早期版本直接存渠道原始状态),一并计入,避免老账目算少。
var refundCountedStatuses = []string{
	refundStatusPending, refundStatusSuccess, refundStatusProcessing, "",
}

// refundCountsTowardTotal 判断某退款状态是否计入「已退金额」。
func refundCountsTowardTotal(status string) bool {
	for _, s := range refundCountedStatuses {
		if s == status {
			return true
		}
	}
	return false
}

// normalizeRefundStatus 把渠道返回的退款状态归一到平台状态。
//
// 渠道的状态字符串五花八门(SUCCESS/REFUND_SUCCESS/PROCESSING/CLOSED/ABNORMAL…),
// 平台自己定义一套,账目才对得上。
func normalizeRefundStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "":
		// 渠道没给状态:支付宝的退款接口是同步返回成功的,
		// 不按成功记账就会出现「钱退了、账上没有」的对不上账。
		return refundStatusSuccess
	case "SUCCESS", "REFUND_SUCCESS", "REFUNDED":
		return refundStatusSuccess
	case "PROCESSING", "REFUNDING", "PENDING":
		return refundStatusProcessing
	default:
		// CLOSED / ABNORMAL / FAILED 等一律按失败记账(钱没退)。
		return refundStatusFailed
	}
}
