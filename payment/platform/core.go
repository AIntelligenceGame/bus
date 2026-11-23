package platform

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 业务核心逻辑(控制台接口与开放接口共用)。
//
// 为什么单独抽一层:
// 控制台接口靠「浏览器会话 + 二次验证票据」鉴权,开放接口(给商户自己的业务后端用)
// 靠「API 密钥签名」鉴权——两者鉴权方式不同,但**业务规则必须完全一致**:
// 退款金额上限、订单归属、回调地址同源校验……
// 如果各写一份,迟早会出现「控制台里退不了,接口里能退」这种绕过。
//
// 所以这里只放业务规则,鉴权在各自的 handler 里做。
//
// 错误用 *httpError 携带 HTTP 状态码:不同入口对同一个业务错误
// 应该给同样的状态码与提示。

// httpError 带 HTTP 状态码的业务错误。
type httpError struct {
	status int
	msg    string
}

// Error 实现 error。
func (e *httpError) Error() string { return e.msg }

// fail 构造一个带状态码的业务错误。
func fail(status int, format string, args ...any) error {
	return &httpError{status: status, msg: fmt.Sprintf(format, args...)}
}

// statusOf 取错误的 HTTP 状态码(默认 500)。
func statusOf(err error) int {
	if he, ok := err.(*httpError); ok && he.status > 0 {
		return he.status
	}
	return http.StatusInternalServerError
}

// applyOrderStatus 用「比较并交换」的方式把订单状态往前推。
//
// 流程:读当前状态 → 用 advance 算出新状态 → **条件更新**
// (只当库里状态还是读到的那一个时才写)。
//
// 为什么要这么绕:订单状态会被好几条路径同时改 —— 支付回调、查单、
// 关单、退款回填。以前都是「读-改-写」,并发时后写的会盖掉先写的:
// 典型后果是「钱收了,订单显示已关闭」(关单读到了 PENDING,回调改成 SUCCESS,
// 关单又把 SUCCESS 盖成 CLOSED,而且不会有第二个回调来纠正)。
//
// 条件更新写不进去(说明状态被别人改了)就重读一次再算,最多三次。
// 三次都撞上并发,就如实报错 —— 宁可让调用方重试,也不写一个错的状态。
//
// advance 返回 (新状态, 是否需要写)。extra 用来顺带写别的字段
// (渠道流水号、回调原文),它在拿到「本次读到的状态」之后才调用。
func (p *Platform) applyOrderStatus(
	userID, outTradeNo string,
	advance func(current string) (string, bool),
	extra func(current string) map[string]any,
) (string, error) {
	for i := 0; i < 3; i++ {
		record, err := p.store.OrderByTradeNo(userID, outTradeNo)
		if err != nil {
			return "", err
		}
		if record == nil {
			return "", fmt.Errorf("订单不存在: %s", outTradeNo)
		}
		next, needWrite := advance(record.Status)
		if !needWrite {
			return record.Status, nil
		}
		updates := map[string]any{}
		if extra != nil {
			updates = extra(record.Status)
		}
		applied, err := p.store.CompareAndSwapOrderStatus(userID, outTradeNo, record.Status, next, updates)
		if err != nil {
			return "", err
		}
		if applied {
			return next, nil
		}
		// 状态被别人改了 → 重读一次再算。
	}
	return "", fmt.Errorf("订单状态并发更新失败(状态一直在变): %s", outTradeNo)
}

// corePay 下单。
//
// 返回支付会话信息;失败时返回带状态码的错误。
func (p *Platform) corePay(r *http.Request, user *User, req payRequest) (map[string]any, error) {
	channel := req.Channel
	var err error

	// 应用维度:先确认这个应用确实属于本商户。
	//
	// 不校验归属就能把订单挂到别人的应用上(或者挂到一个不存在的应用上),
	// 后面「按应用统计 / 出报表」就全乱了 —— 商户会看到一个自己都没建过的应用
	// 下面挂着他的钱。
	appID := strings.TrimSpace(req.AppID)
	if appID != "" {
		app, aerr := p.store.AppByID(user.UserID, appID)
		if aerr != nil {
			return nil, fail(http.StatusInternalServerError, "%v", aerr)
		}
		if app == nil {
			return nil, fail(http.StatusBadRequest, "应用不存在: %s", appID)
		}
	} else if user.RequireAppBinding {
		// 该商户开了「订单必须绑定应用」。
		//
		// 这时候放行 app_id 为空的下单,等于让钱进到一个说不清是哪个产品的
		// 口袋里 —— 报表对不上、也追不回是哪次调用造成的。
		// 宁可当场拒绝,也不要事后靠猜。
		return nil, fail(http.StatusBadRequest,
			"%s", requireAppBindingMessage())
	}
	if channel == "" {
		channel, err = p.store.DefaultChannelFor(user.UserID, appID)
		if err != nil {
			return nil, fail(http.StatusInternalServerError, "%v", err)
		}
		if channel == "" {
			return nil, fail(http.StatusBadRequest, "尚未配置任何可用渠道，请先在「渠道配置」页完成配置")
		}
	}
	// Provider 按应用取:应用专属的渠道配置覆盖全局的。
	providers, err := p.appProviders(user.UserID, appID)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		return nil, fail(http.StatusBadRequest, "渠道 %s 未启用或配置无效", channel)
	}
	if req.OutTradeNo == "" {
		// 商户没给单号时自己生成一个。
		//
		// 必须够唯一:商户单号在渠道那边是幂等键,撞号就等于两笔不同的
		// 支付被渠道当成同一笔。随机后缀比「时间戳取模」靠谱得多。
		// (微信的 out_trade_no 最长 32 位,所以随机部分取 16 位十六进制。)
		req.OutTradeNo = fmt.Sprintf("PAY%s%s", time.Now().Format("20060102"), newShortID(8))
	}
	if req.Subject == "" {
		req.Subject = "支付订单 " + req.OutTradeNo
	}
	if req.Amount <= 0 {
		return nil, fail(http.StatusBadRequest, "金额必须大于 0(单位:分)")
	}
	// 商户单号重复要**在渠道下单之前**挡住。
	//
	// 但有一个例外:上次下单**没成功**(渠道那边没建单,平台这边是条
	// 「半成品」记录,比如进程正好崩在调渠道那一步)。这种单号要能重试,
	// 否则商户一次网络抖动就永久丢了这个单号。
	if existing, err := p.store.OrderByTradeNo(user.UserID, req.OutTradeNo); err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	} else if existing != nil {
		if existing.ChannelCreated {
			return nil, fail(http.StatusConflict, "商户单号已存在: %s", req.OutTradeNo)
		}
		// 半成品订单(渠道侧没有):清掉,重新走一遍。
		if _, derr := p.store.DeleteOrder(user.UserID, req.OutTradeNo); derr != nil {
			return nil, fail(http.StatusInternalServerError, "%v", derr)
		}
	}
	// 回调地址前缀:商户自己配的对外地址优先,没配就用平台默认地址(-base)。
	base := p.notifyBaseFor(user)
	notifyURL := p.notifyURLFor(payment.Channel(channel), user, user.UserID, appID)
	// 回调地址由平台生成:商户不能自己指定。
	//
	// 原因:回调地址决定了「渠道把支付结果告诉谁」,也决定了平台按谁去验签。
	// 让商户自由填写,一方面能把回调引到别处(平台就再也收不到通知、
	// 订单状态永远停在 pending),另一方面等于给了 SSRF 的跳板。
	//
	// 允许的例外只有「平台(或商户自己)生成的那个域名」—— 也就是商户在控制台里
	// 配的对外地址;别的域名一律拒绝。
	if req.NotifyURL != "" && !sameOrigin(req.NotifyURL, base) {
		return nil, fail(http.StatusBadRequest, "回调地址必须使用平台生成的地址,不能自行指定")
	}

	order := &payment.Order{
		OutTradeNo: req.OutTradeNo,
		Subject:    req.Subject,
		Amount:     req.Amount,
		ClientIP:   req.ClientIP,
		NotifyURL:  notifyURL,
		Attach:     req.Attach,
		PayerID:    req.PayerID,
	}

	tradeType := payment.TradeType(req.TradeType)
	if tradeType == "" {
		tradeType = payment.AutoTradeType(payment.Channel(channel), order)
	}

	// 按渠道的**官方字段约束**先校验一遍,再落库。
	//
	// 顺序很要紧:必须在写订单记录之前。校验没过就不该在库里留下任何痕迹 ——
	// 留一条「永远付不成」的半成品订单,商户在订单列表里看到会以为平台出过单。
	//
	// 报错要说清楚是哪个字段违反了哪条限制(而不是渠道那句 invalid parameter),
	// 商户一看就知道改哪里。
	if err := validatePayRequest(payment.Channel(channel), tradeType,
		provider.SupportedTradeTypes(), req); err != nil {
		return nil, fail(http.StatusBadRequest, "%v", err)
	}

	// **先落库,再调渠道**。
	//
	// 以前是「先调渠道、成功后落库」:两步之间进程挂掉 / 落库失败,
	// 就会留下一笔「渠道那边真的下了单、平台这边查不到」的订单 ——
	// 商户既查不到也退不了款,回调到了也只能收到「订单不存在」。
	//
	// 现在先把订单记下来(channel_created=0,表示「渠道侧还没有」),
	// 再去调渠道:
	//   - 调成功 → 回填二维码,并把 channel_created 置 1;
	//   - 调失败 → 把这条半成品记录删掉,商户可以拿同一个单号重试;
	//   - 进程正好崩在中间 → 库里留下一条 channel_created=0 的记录,
	//     商户看得见、关得掉(关单时不去调渠道),也能用同一个单号重试。
	record := OrderRecord{
		OutTradeNo: order.OutTradeNo,
		UserID:     user.UserID,
		AppID:      appID,
		Channel:    channel,
		TradeType:  string(tradeType),
		Subject:    order.Subject,
		Amount:     order.Amount,
		AmountYuan: payment.CentsToYuan(order.Amount),
		Status:     string(payment.TradeStatusPending),
	}
	if err := p.store.AddOrder(record); err != nil {
		p.opts.Logger.Printf("写入订单失败: %v", err)
		if err == ErrOrderExists {
			return nil, fail(http.StatusConflict, "商户单号已存在: %s", record.OutTradeNo)
		}
		return nil, fail(http.StatusInternalServerError, "写入订单失败: %v", err)
	}

	session, err := provider.Create(r.Context(), order, tradeType)
	if err != nil {
		// 回滚:删掉半成品,让商户能拿同一个商户单号重试。
		if _, derr := p.store.DeleteOrder(user.UserID, record.OutTradeNo); derr != nil {
			p.opts.Logger.Printf("回滚未落成的订单失败(%s): %v", record.OutTradeNo, derr)
		}
		return nil, fail(http.StatusBadGateway, "发起支付失败: %v", err)
	}
	// 回填渠道侧创建成功:二维码 + channel_created=1。
	if _, uerr := p.store.UpdateOrderFields(user.UserID, record.OutTradeNo, map[string]any{
		"qrcode":          session.QRCode,
		"channel_created": 1,
	}); uerr != nil {
		// 回填失败不影响这笔支付本身(渠道那边已经建单了),
		// 但订单状态会一直停在「渠道侧没有」,关单时就不会去渠道关单 —— 记日志。
		p.opts.Logger.Printf("回填订单渠道信息失败(%s): %v", record.OutTradeNo, uerr)
	}
	p.audit(r, user.UserID, auditOrderPay, order.OutTradeNo,
		fmt.Sprintf("渠道=%s 金额=%s", channel, record.AmountYuan), "")

	return map[string]any{
		"session":    session,
		"order_no":   order.OutTradeNo,
		"amount":     order.Amount,
		"yuan":       payment.CentsToYuan(order.Amount),
		"notify_url": notifyURL,
		"qrcode_url": qrcodeURL(session.QRCode),
		"usage":      usageGuide(channel, string(tradeType)),
	}, nil
}

// coreRefund 退款。
//
// 三条硬规则:
//
//  1. **幂等**:商户退款单号(out_refund_no)是幂等键,同一个单号重复提交
//     只会退一次钱,第二次直接返回上次的结果。
//  2. **先占位再动钱**:平台先在事务里占一行退款流水(计入已退金额),
//     再去调渠道,最后回填结果。顺序反了就会出现「钱退了、账上没有」。
//  3. **金额有上限**:退款金额受「订单金额」和「已退累计」双重约束,
//     否则「退 1 元、再退 1 元」能一直退到超过订单金额。
//
// 另外:**渠道固定用订单自己的渠道**。退错渠道等于把钱退到别人的商户号上,
// 这种错平台自己没法兜住,所以宁可直接报错。
func (p *Platform) coreRefund(r *http.Request, user *User, req struct {
	Channel      string `json:"channel"`
	OutTradeNo   string `json:"out_trade_no"`
	OutRefundNo  string `json:"out_refund_no"`
	RefundAmount int64  `json:"refund_amount"`
	Reason       string `json:"reason"`
}) (map[string]any, error) {
	// 退款只允许退**自己商户**的订单:先确认归属,再动钱。
	record, err := p.ownOrder(user, req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusNotFound, "%v", err)
	}
	// 幂等先行:同一个退款单号重复提交,**不管订单现在是什么状态**都直接
	// 返回上次的结果。
	//
	// 顺序很关键:第一次退款成功后订单已经变成「部分退款」,这时再走
	// 下面的状态校验就会把商户的重试挡掉 —— 商户拿不到结果,只能以为
	// 退款失败了,于是换一个单号再退一次,那就真重复出款了。
	if req.OutRefundNo != "" {
		existing, err := p.store.RefundByNo(user.UserID, req.OutRefundNo)
		if err != nil {
			return nil, fail(http.StatusInternalServerError, "%v", err)
		}
		if existing != nil {
			if existing.OutTradeNo != req.OutTradeNo {
				return nil, fail(http.StatusBadRequest,
					"退款单号 %s 已经用于订单 %s,不能用于订单 %s",
					req.OutRefundNo, existing.OutTradeNo, req.OutTradeNo)
			}
			// 上一笔是失败的 → 钱没退出去,允许用**同一个退款单号重试**
			// (网关惯例:幂等键不变 = 重试)。这时不短路返回,继续往下走,
			// 由 ReserveRefund 把原来那行重新置为 PENDING 再出款。
			// 非失败状态(占位/成功/处理中)才短路幂等返回。
			if existing.Status != refundStatusFailed {
				total, err := p.store.RefundedAmount(user.UserID, req.OutTradeNo)
				if err != nil {
					return nil, fail(http.StatusInternalServerError, "%v", err)
				}
				p.audit(r, user.UserID, auditOrderRefund, req.OutTradeNo,
					fmt.Sprintf("退款单号 %s 重复提交,幂等返回(金额=%s)",
						req.OutRefundNo, payment.CentsToYuan(existing.Amount)), "")
				return map[string]any{
					"result": map[string]any{
						"channel":       existing.Channel,
						"out_trade_no":  existing.OutTradeNo,
						"out_refund_no": existing.OutRefundNo,
						"status":        existing.Status,
					},
					"refund_amount":  existing.Amount,
					"refunded_total": total,
					"order_status":   record.Status,
					"idempotent":     true,
				}, nil
			}
		}
	}
	// 只有支付成功的订单才能退。
	if record.Status != string(payment.TradeStatusSuccess) {
		return nil, fail(http.StatusBadRequest,
			"订单当前状态为 %s,只有支付成功的订单才能退款", record.Status)
	}
	// 渠道固定用**订单自己的渠道**。
	channel := record.Channel
	if req.Channel != "" && req.Channel != channel {
		return nil, fail(http.StatusBadRequest,
			"订单 %s 属于渠道 %s,不能指定渠道 %s", req.OutTradeNo, channel, req.Channel)
	}
	// 先算已退金额,再决定退多少。
	//
	// 顺序很关键:「全额退款」指的是**剩下还没退的那部分**,
	// 不是订单的原始金额。100 元的订单已经退过 30 元,再点「全额退款」
	// 应该退 70 元,而不是 100 元 —— 后者会被下面的校验直接拒掉
	// (30 + 100 > 100),于是「全额退款」这条主路径在部分退款过的订单上是坏的。
	refunded, err := p.store.RefundedAmount(user.UserID, req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	remaining := record.Amount - refunded
	if remaining < 0 {
		remaining = 0
	}
	if remaining == 0 {
		return nil, fail(http.StatusBadRequest,
			"订单 %s 已经退完了,不能再退", req.OutTradeNo)
	}
	// 退款金额:0 / 不传 表示「把剩下的全退了」。
	amount := req.RefundAmount
	if amount <= 0 {
		amount = remaining
	}
	if amount > remaining {
		return nil, fail(http.StatusBadRequest,
			"退款金额 %s 元超过可退金额 %s 元(订单 %s 元,已退 %s 元)",
			payment.CentsToYuan(amount), payment.CentsToYuan(remaining),
			record.AmountYuan, payment.CentsToYuan(refunded))
	}
	// Provider 必须按**订单归属的应用**取:这笔单当初是用哪套密钥下的,
	// 退款 / 查单 / 关单就得用哪套 —— 换一套商户号去退款,渠道那边认不出来。
	providers, err := p.appProviders(user.UserID, record.AppID)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		return nil, fail(http.StatusBadRequest, "渠道 %s 未启用或配置无效", channel)
	}
	if req.OutRefundNo == "" {
		// 商户没给退款单号时自己生成一个。
		//
		// 必须够唯一:这个单号是幂等键,撞号就等于「两笔不同的退款被当成
		// 同一笔」,第二笔会被静默吞掉(商户以为退了,其实没退)。
		req.OutRefundNo = req.OutTradeNo + "-R" + newShortID(8)
	}
	// 先占位:占位成功才允许去动渠道的钱。
	reserved, created, err := p.store.ReserveRefund(RefundRecord{
		UserID:      user.UserID,
		OutTradeNo:  req.OutTradeNo,
		OutRefundNo: req.OutRefundNo,
		Channel:     channel,
		Amount:      amount,
		Reason:      req.Reason,
	})
	if err != nil {
		var exceeds *refundExceedsError
		var conflict *refundConflictError
		switch {
		case errors.As(err, &exceeds):
			return nil, fail(http.StatusBadRequest,
				"累计退款金额将超过订单金额(已退 %s 元,订单 %s 元)",
				payment.CentsToYuan(exceeds.refunded),
				payment.CentsToYuan(exceeds.order))
		case errors.As(err, &conflict):
			return nil, fail(http.StatusBadRequest,
				"退款单号 %s 已经用于 %s 元的退款,不能再改成 %s 元",
				conflict.outRefundNo,
				payment.CentsToYuan(conflict.existing),
				payment.CentsToYuan(conflict.incoming))
		case errors.Is(err, ErrOrderNotFound):
			return nil, fail(http.StatusNotFound, "订单不存在: %s", req.OutTradeNo)
		default:
			return nil, fail(http.StatusInternalServerError, "%v", err)
		}
	}
	if !created {
		// 幂等命中:同一个退款单号已经处理过,不再动钱,原样返回上次的结果。
		total, err := p.store.RefundedAmount(user.UserID, req.OutTradeNo)
		if err != nil {
			return nil, fail(http.StatusInternalServerError, "%v", err)
		}
		p.audit(r, user.UserID, auditOrderRefund, req.OutTradeNo,
			fmt.Sprintf("退款单号 %s 重复提交,幂等返回(金额=%s)",
				req.OutRefundNo, payment.CentsToYuan(reserved.Amount)), "")
		return map[string]any{
			"result": map[string]any{
				"channel":       channel,
				"out_trade_no":  req.OutTradeNo,
				"out_refund_no": reserved.OutRefundNo,
				"status":        reserved.Status,
			},
			"refund_amount":  reserved.Amount,
			"refunded_total": total,
			"order_status":   record.Status,
			"idempotent":     true,
		}, nil
	}
	// 调渠道退款。
	result, err := provider.Refund(r.Context(), &payment.RefundRequest{
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  req.OutRefundNo,
		RefundAmount: amount,
		TotalAmount:  record.Amount,
		Reason:       req.Reason,
	})
	if err != nil {
		// 渠道没退成功:占位作废(不计入已退金额),商户可以换个退款单号重试。
		if ferr := p.store.FinishRefund(user.UserID, req.OutRefundNo, refundStatusFailed); ferr != nil {
			p.opts.Logger.Printf("回填退款状态失败: %v", ferr)
		}
		return nil, fail(http.StatusBadGateway, "退款失败: %v", err)
	}
	// 回填退款结果(渠道没给状态时按成功记账,否则会出现「钱退了、账上没有」)。
	status := normalizeRefundStatus(result.Status)
	if ferr := p.store.FinishRefund(user.UserID, req.OutRefundNo, status); ferr != nil {
		p.opts.Logger.Printf("回填退款状态失败: %v", ferr)
	}
	total, err := p.store.RefundedAmount(user.UserID, req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	// 部分退款不把订单标成「已退款」。
	newStatus := string(payment.TradeStatusRefunded)
	if total < record.Amount {
		newStatus = string(payment.TradeStatusPartiallyRefunded)
	}
	// 条件更新:只在订单状态还是「读到的那个」时写,避免和并发的支付回调互相覆盖。
	if _, err := p.applyOrderStatus(user.UserID, req.OutTradeNo,
		func(current string) (string, bool) {
			if current == newStatus {
				return current, false
			}
			return newStatus, true
		}, nil); err != nil {
		p.opts.Logger.Printf("退款后回写订单状态失败: %v", err)
	}
	p.audit(r, user.UserID, auditOrderRefund, req.OutTradeNo,
		fmt.Sprintf("退款金额=%s 累计=%s 状态=%s", payment.CentsToYuan(amount),
			payment.CentsToYuan(total), status), "")
	return map[string]any{
		"result":         result,
		"refund_amount":  amount,
		"refunded_total": total,
		"order_status":   newStatus,
	}, nil
}

// coreClose 关闭订单。
func (p *Platform) coreClose(r *http.Request, user *User, req struct {
	Channel    string `json:"channel"`
	OutTradeNo string `json:"out_trade_no"`
}) error {
	// 关单只允许关**自己商户**的订单。
	record, err := p.ownOrder(user, req.OutTradeNo)
	if err != nil {
		return fail(http.StatusNotFound, "%v", err)
	}
	// 只有待支付的订单能关:对已支付/已退款的订单再「关」一次,
	// 只会把状态改乱(钱明明收了,平台却说已关闭)。
	if record.Status != string(payment.TradeStatusPending) {
		return fail(http.StatusBadRequest,
			"订单当前状态为 %s,只有待支付的订单才能关闭", record.Status)
	}
	// 渠道固定用**订单自己的渠道**。
	channel := record.Channel
	if req.Channel != "" && req.Channel != channel {
		return fail(http.StatusBadRequest,
			"订单 %s 属于渠道 %s,不能指定渠道 %s", req.OutTradeNo, channel, req.Channel)
	}
	// Provider 必须按**订单归属的应用**取:这笔单当初是用哪套密钥下的,
	// 退款 / 查单 / 关单就得用哪套 —— 换一套商户号去退款,渠道那边认不出来。
	providers, err := p.appProviders(user.UserID, record.AppID)
	if err != nil {
		return fail(http.StatusInternalServerError, "%v", err)
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		return fail(http.StatusBadRequest, "渠道 %s 未启用或配置无效", channel)
	}
	// 渠道侧**没有**这笔单(上次下单没成功):不用去渠道关单
	// (调了必然报「订单不存在」),直接在平台这边关掉。
	if !record.ChannelCreated {
		applied, cerr := p.store.CompareAndSwapOrderStatus(user.UserID, req.OutTradeNo,
			string(payment.TradeStatusPending), string(payment.TradeStatusClosed), nil)
		if cerr != nil {
			return fail(http.StatusInternalServerError, "%v", cerr)
		}
		if !applied {
			return fail(http.StatusConflict, "订单状态已变化,关单没有生效: %s", req.OutTradeNo)
		}
		p.audit(r, user.UserID, auditOrderClose, req.OutTradeNo, "关闭订单(渠道侧无此单)", "")
		return nil
	}
	if err := provider.Close(r.Context(), req.OutTradeNo); err != nil {
		return fail(http.StatusBadGateway, "关闭订单失败: %v", err)
	}
	// 写回状态用**条件更新**:只在订单仍然是「待支付」时才改成「已关闭」。
	//
	// 以前是无条件写 —— 期间如果支付成功的回调到了(钱已经收了),
	// 这次无条件写会把 SUCCESS 盖成 CLOSED:订单显示已关闭,钱却到账了,
	// 而且不会再有第二个回调来纠正它。
	applied, err := p.store.CompareAndSwapOrderStatus(user.UserID, req.OutTradeNo,
		string(payment.TradeStatusPending), string(payment.TradeStatusClosed), nil)
	if err != nil {
		return fail(http.StatusInternalServerError, "%v", err)
	}
	if !applied {
		// 状态在这期间变了:多半是支付成功的回调先到了。
		// 这时**不能**盖成已关闭,如实告诉调用方现在是什么状态。
		current, cerr := p.store.OrderByTradeNo(user.UserID, req.OutTradeNo)
		if cerr == nil && current != nil {
			p.opts.Logger.Printf("关单时订单状态已变为 %s(订单 %s),保持原状态",
				current.Status, req.OutTradeNo)
			return fail(http.StatusConflict,
				"订单 %s 当前状态为 %s,没有改成已关闭(可能支付结果刚刚到达)",
				req.OutTradeNo, current.Status)
		}
		return fail(http.StatusConflict, "订单状态已变化,关单没有生效: %s", req.OutTradeNo)
	}
	p.audit(r, user.UserID, auditOrderClose, req.OutTradeNo, "关闭订单", "")
	return nil
}

// coreQuery 查单(向渠道查询后回写状态)。
func (p *Platform) coreQuery(r *http.Request, user *User, req struct {
	Channel    string `json:"channel"`
	OutTradeNo string `json:"out_trade_no"`
}) (map[string]any, error) {
	if req.OutTradeNo == "" {
		return nil, fail(http.StatusBadRequest, "缺少 out_trade_no")
	}
	// 查单只允许查**自己商户**的订单。
	record, err := p.ownOrder(user, req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusNotFound, "%v", err)
	}
	// 渠道固定用**订单自己的渠道**。
	channel := record.Channel
	if req.Channel != "" && req.Channel != channel {
		return nil, fail(http.StatusBadRequest,
			"订单 %s 属于渠道 %s,不能指定渠道 %s", req.OutTradeNo, channel, req.Channel)
	}
	// Provider 必须按**订单归属的应用**取:这笔单当初是用哪套密钥下的,
	// 退款 / 查单 / 关单就得用哪套 —— 换一套商户号去退款,渠道那边认不出来。
	providers, err := p.appProviders(user.UserID, record.AppID)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		return nil, fail(http.StatusBadRequest, "渠道 %s 未启用或配置无效", channel)
	}
	result, err := provider.Query(r.Context(), req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusBadGateway, "查询失败: %v", err)
	}
	// 渠道返回的金额必须与平台记录的一致:不一致说明查的不是同一笔单,
	// 这种情况下写回状态只会把订单改错。
	if result.Amount > 0 && result.Amount != record.Amount {
		return nil, fail(http.StatusBadGateway,
			"渠道返回金额与订单金额不一致(订单 %s 元,渠道 %s 元)",
			record.AmountYuan, payment.CentsToYuan(result.Amount))
	}
	// 状态只能向前走:一次迟到的查单不能把已退款的订单改回「成功」。
	effective, ok := advanceStatus(payment.TradeStatus(record.Status), result.Status)
	if !ok {
		p.opts.Logger.Printf("查单状态未生效: 订单 %s 当前 %s,渠道返回 %s",
			req.OutTradeNo, record.Status, result.Status)
	}
	// 条件更新(读→算→原子写):和并发的支付回调撞在一起时不会互相覆盖。
	if _, err := p.applyOrderStatus(user.UserID, req.OutTradeNo,
		func(current string) (string, bool) {
			next, _ := advanceStatus(payment.TradeStatus(current), result.Status)
			return string(next), true
		},
		func(current string) map[string]any {
			out := map[string]any{}
			if result.TransactionID != "" {
				out["transaction_id"] = result.TransactionID
			}
			return out
		}); err != nil {
		p.opts.Logger.Printf("查单后回写订单状态失败: %v", err)
	}
	return map[string]any{
		"status":         string(effective),
		"transaction_id": result.TransactionID,
		"channel":        string(result.Channel),
	}, nil
}

// coreWalletCharge 钱包渠道扣款(Apple Pay / Google Pay 的最后一步)。
func (p *Platform) coreWalletCharge(r *http.Request, user *User, req struct {
	Channel      string `json:"channel"`
	OutTradeNo   string `json:"out_trade_no"`
	PaymentToken string `json:"payment_token"`
	Amount       int64  `json:"amount"`
}) (map[string]any, error) {
	if req.OutTradeNo == "" || req.PaymentToken == "" {
		return nil, fail(http.StatusBadRequest, "out_trade_no 与 payment_token 必填")
	}
	// 扣款只允许扣**自己商户**的订单。
	record, err := p.ownOrder(user, req.OutTradeNo)
	if err != nil {
		return nil, fail(http.StatusNotFound, "%v", err)
	}
	// 渠道固定用**订单自己的渠道**。
	channel := record.Channel
	if req.Channel != "" && req.Channel != channel {
		return nil, fail(http.StatusBadRequest,
			"订单 %s 属于渠道 %s,不能指定渠道 %s", req.OutTradeNo, channel, req.Channel)
	}
	// Provider 必须按**订单归属的应用**取:这笔单当初是用哪套密钥下的,
	// 退款 / 查单 / 关单就得用哪套 —— 换一套商户号去退款,渠道那边认不出来。
	providers, err := p.appProviders(user.UserID, record.AppID)
	if err != nil {
		return nil, fail(http.StatusInternalServerError, "%v", err)
	}
	provider, ok := providers[payment.Channel(channel)]
	if !ok {
		return nil, fail(http.StatusBadRequest, "渠道 %s 未启用或配置无效", channel)
	}
	wallet, ok := provider.(payment.WalletProvider)
	if !ok {
		return nil, fail(http.StatusBadRequest, "渠道 %s 不是钱包渠道，不支持钱包扣款", channel)
	}
	// 金额以平台记录的订单金额为准:客户端传了不一样的金额就是篡改,直接拒绝。
	if req.Amount > 0 && req.Amount != record.Amount {
		return nil, fail(http.StatusBadRequest,
			"金额与订单不一致(订单 %s 元,请求 %s 元)",
			record.AmountYuan, payment.CentsToYuan(req.Amount))
	}
	amount := record.Amount
	result, err := wallet.ChargeWallet(r.Context(), req.OutTradeNo, req.PaymentToken, amount)
	if err != nil {
		return nil, fail(http.StatusBadGateway, "钱包扣款失败: %v", err)
	}
	effective, ok := advanceStatus(payment.TradeStatus(record.Status), result.Status)
	if !ok {
		p.opts.Logger.Printf("扣款状态未生效: 订单 %s 当前 %s,渠道返回 %s",
			req.OutTradeNo, record.Status, result.Status)
	}
	// 条件更新(读→算→原子写):和并发的支付回调撞在一起时不会互相覆盖。
	if _, err := p.applyOrderStatus(user.UserID, req.OutTradeNo,
		func(current string) (string, bool) {
			next, _ := advanceStatus(payment.TradeStatus(current), result.Status)
			return string(next), true
		},
		func(current string) map[string]any {
			out := map[string]any{}
			if result.TransactionID != "" {
				out["transaction_id"] = result.TransactionID
			}
			return out
		}); err != nil {
		p.opts.Logger.Printf("钱包扣款后回写订单状态失败: %v", err)
	}
	p.audit(r, user.UserID, auditOrderCharge, req.OutTradeNo,
		fmt.Sprintf("渠道=%s 金额=%s", channel, payment.CentsToYuan(amount)), "")
	return map[string]any{
		"status":         string(effective),
		"transaction_id": result.TransactionID,
		"channel":        channel,
	}, nil
}
