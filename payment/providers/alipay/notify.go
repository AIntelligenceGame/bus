package alipay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// ParseNotify 验签并解析支付宝异步通知(form 表单)。
//
// 通知为 application/x-www-form-urlencoded 表单,先验签再解析业务字段。
// 兼容 JSON 格式的通知体。
func (p *Provider) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	if req == nil {
		return nil, fmt.Errorf("alipay: nil notify request")
	}
	params := make(map[string]string)
	for k, v := range req.Query {
		params[k] = v
	}
	for k, v := range parseNotifyParams(req.Body) {
		params[k] = v
	}
	if err := p.verifyParams(params); err != nil {
		return nil, err
	}

	// 官方异步通知处理步骤:验签之后要校验「通知里的 app_id 是自己的」。
	// 少了这一步,别的应用的交易通知也会被当成本应用的通知记进去。
	if got := params["app_id"]; got != "" && p.cfg != nil && p.cfg.AppID != "" && got != p.cfg.AppID {
		return nil, fmt.Errorf("%w: alipay notify app_id %s is not ours",
			payment.ErrInvalidSignature, got)
	}

	outTradeNo := params["out_trade_no"]
	if outTradeNo == "" {
		return nil, fmt.Errorf("%w: alipay notify missing out_trade_no", payment.ErrInvalidSignature)
	}
	// 金额必须能解析:解析不出来说明报文不完整,不能当「支付成功」记账。
	// (以前这里忽略了错误,金额永远是 0,平台就少了金额这一道校验。)
	amount := int64(0)
	if raw := params["total_amount"]; raw != "" {
		parsed, err := payment.YuanToCents(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: alipay notify invalid total_amount %q",
				payment.ErrInvalidSignature, raw)
		}
		amount = parsed
	}

	notify := &payment.Notify{
		Channel:       payment.ChannelAlipay,
		OutTradeNo:    outTradeNo,
		TransactionID: params["trade_no"],
		Amount:        amount,
		Currency:      "CNY",
		Status:        mapTradeStatus(params["trade_status"]),
		Raw:           params,
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", params["gmt_payment"], time.Local); err == nil {
		notify.EventTime = t
	}
	return notify, nil
}

// VerifyNotify 单独验签(用于同步跳转回单校验)。
//
// 返回验签是否通过,便于调用方按需处理。
func (p *Provider) VerifyNotify(req *payment.NotifyRequest) (bool, error) {
	if req == nil {
		return false, fmt.Errorf("alipay: nil notify request")
	}
	params := make(map[string]string)
	for k, v := range req.Query {
		params[k] = v
	}
	for k, v := range parseNotifyParams(req.Body) {
		params[k] = v
	}
	if err := p.verifyParams(params); err != nil {
		return false, err
	}
	return true, nil
}

// parseNotifyParams 解析回调体里的参数:表单与 JSON 都支持。
//
// 注意:url.ParseQuery 对 JSON 文本**不会报错**(只是解析不出键值),
// 所以必须先看首字符是不是 '{' 来判断是不是 JSON,否则 JSON 分支永远走不到。
func parseNotifyParams(body []byte) map[string]string {
	params := make(map[string]string)
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return params
	}
	if trimmed[0] == '{' {
		var jsonParams map[string]any
		if err := json.Unmarshal(trimmed, &jsonParams); err != nil {
			return params
		}
		for k, v := range jsonParams {
			params[k] = str(v)
		}
		return params
	}
	if values, err := url.ParseQuery(string(trimmed)); err == nil {
		for k, v := range values {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
	}
	return params
}
