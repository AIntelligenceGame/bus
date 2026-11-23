package alipay

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// Provider 支付宝渠道实现。
type Provider struct {
	cfg        *Config
	client     *http.Client
	privKey    *rsa.PrivateKey
	alipayPub  *rsa.PublicKey
	appCertSn  string
	rootCertSn string
	alipayCert *x509.Certificate
}

// AppID 本商户配置的 AppID(用于回调校验:回调里的 app_id 必须等于它)。
func (p *Provider) AppID() string { return p.cfg.AppID }

// New 创建支付宝 Provider。
//
// 两种密钥模式(与官方 SDK 一致):
//
//	公钥模式:只配支付宝公钥 → 请求里不带 app_cert_sn / alipay_root_cert_sn。
//	证书模式:配了支付宝公钥证书 → **必须**同时配应用公钥证书和支付宝根证书,
//	  请求里带 app_cert_sn(应用公钥证书的序列号)和 alipay_root_cert_sn(根证书的序列号)。
//
// 证书模式下这三个序列号都要参与签名(官方《签名》:
// opendocs.alipay.com/apis/02khjm —— 「若使用公钥证书模式签名,在拼接待签名字符串时
// 需添加解析出来的应用公钥证书 SN(app_cert_sn)和支付宝根证书 SN(alipay_root_cert_sn)」)。
func New(cfg *Config) (*Provider, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	priv, err := payment.LoadRSAPrivateKey([]byte(cfg.PrivateKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("alipay: load private key: %w", err)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	p := &Provider{cfg: cfg, client: client, privKey: priv}

	switch {
	case cfg.AlipayPublicCertPath != "":
		// 证书模式:支付宝公钥证书用于验签,应用公钥证书用于 app_cert_sn,
		// 支付宝根证书用于 alipay_root_cert_sn。
		pub, cert, err := loadPublicKeyFromCert(cfg.AlipayPublicCertPath)
		if err != nil {
			return nil, err
		}
		// 证书过期/未生效要当场说清楚:否则每个请求都被网关判
		// 40002 / isv.invalid-signature,看起来像签名算法错了。
		if err := checkCertNotExpired(cert, "支付宝公钥证书"); err != nil {
			return nil, err
		}
		p.alipayPub = pub
		p.alipayCert = cert

		appCert, err := loadCertFromFile(cfg.AppCertPath)
		if err != nil {
			return nil, err
		}
		if err := checkCertNotExpired(appCert, "应用公钥证书"); err != nil {
			return nil, err
		}
		sn, err := certSN(appCert)
		if err != nil {
			return nil, fmt.Errorf("alipay: compute app_cert_sn: %w", err)
		}
		p.appCertSn = sn

		rootSN, err := loadRootCertSNFromFile(cfg.RootCertPath)
		if err != nil {
			return nil, err
		}
		p.rootCertSn = rootSN
	default:
		pub, err := payment.LoadRSAPublicKey([]byte(cfg.AlipayPublicKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("alipay: load public key: %w", err)
		}
		p.alipayPub = pub
	}
	return p, nil
}

// Channel 渠道标识。
func (p *Provider) Channel() payment.Channel { return payment.ChannelAlipay }

// SupportedTradeTypes 支持的交易类型。
func (p *Provider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{
		payment.TradeTypeNative,
		payment.TradeTypeApp,
		payment.TradeTypeWap,
		payment.TradeTypeWeb,
	}
}

// buildParams 组装公共参数 + 业务参数并签名。
func (p *Provider) buildParams(method string, bizContent map[string]any) (map[string]string, error) {
	biz, err := json.Marshal(bizContent)
	if err != nil {
		return nil, fmt.Errorf("alipay: marshal biz_content: %w", err)
	}
	params := map[string]string{
		"app_id":      p.cfg.AppID,
		"method":      method,
		"format":      "JSON",
		"charset":     "utf-8",
		"sign_type":   p.cfg.SignType,
		"timestamp":   time.Now().Format("2006-01-02 15:04:05"),
		"version":     "1.0",
		"biz_content": string(biz),
	}
	if p.cfg.NotifyURL != "" {
		params["notify_url"] = p.cfg.NotifyURL
	}
	if p.cfg.ReturnURL != "" {
		params["return_url"] = p.cfg.ReturnURL
	}
	if p.appCertSn != "" {
		params["app_cert_sn"] = p.appCertSn
	}
	if p.rootCertSn != "" {
		params["alipay_root_cert_sn"] = p.rootCertSn
	}
	return p.signParams(params)
}

// encodeQuery 把参数编码为 application/x-www-form-urlencoded 字符串。
func encodeQuery(params map[string]string) string {
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	return values.Encode()
}

// call 调用支付宝开放接口,返回 method_response 节点。
func (p *Provider) call(ctx context.Context, method string, bizContent map[string]any) (map[string]any, error) {
	signed, err := p.buildParams(method, bizContent)
	if err != nil {
		return nil, err
	}
	body := encodeQuery(signed)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.Gateway, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("alipay: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bus-payment-alipay/1.0")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("alipay: request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("alipay: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("alipay: http status %d body=%s", resp.StatusCode, string(raw))
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("alipay: decode response: %w", err)
	}
	node, ok := result[method+"_response"]
	if !ok {
		return nil, fmt.Errorf("alipay: response missing %s_response: %s", method, string(raw))
	}
	m, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("alipay: malformed response: %s", string(raw))
	}
	code := str(m["code"])
	if code != "" && code != "10000" {
		return nil, fmt.Errorf("alipay: api error code=%s msg=%s sub_code=%s sub_msg=%s",
			code, str(m["msg"]), str(m["sub_code"]), str(m["sub_msg"]))
	}
	return m, nil
}

// str 安全地把 any 转成字符串。
func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

// bizCommon 业务公共字段。
func (p *Provider) bizCommon(order *payment.Order) map[string]any {
	biz := map[string]any{
		"out_trade_no": order.OutTradeNo,
		"subject":      order.Subject,
		"total_amount": payment.CentsToYuan(order.Amount),
	}
	if order.Body != "" {
		biz["body"] = order.Body
	}
	notify := order.NotifyURL
	if notify == "" {
		notify = p.cfg.NotifyURL
	}
	if notify != "" {
		biz["notify_url"] = notify
	}
	ret := order.ReturnURL
	if ret == "" {
		ret = p.cfg.ReturnURL
	}
	if ret != "" {
		biz["return_url"] = ret
	}
	if order.PayerID != "" {
		biz["buyer_id"] = order.PayerID
	}
	// 商户附加数据 → passback_params。
	//
	// 之前这里是把 Attach 塞进 extend_params.sys_service_provider_id ——
	// 那是**服务商自己的 PID**(用来算返佣),不是商户的业务数据:
	//   - 官方要求该值对应已签约的返佣/协作关系,填错返佣直接算不出来,
	//     而且**不可申诉**;
	//   - 商户的业务附加数据(如 "shop-1001")根本不会原样回传,
	//     而 Order.Attach 的语义恰恰就是「原样回传」。
	//
	// 官方给商户自定义透传数据用的字段是 passback_params(必 UrlEncoded,
	// 不能是 JSON 格式)。
	if order.Attach != "" {
		biz["passback_params"] = payment.AlipayPassbackParams(order.Attach)
	}
	if order.Expire > 0 {
		biz["timeout_express"] = fmt.Sprintf("%ds", int(order.Expire.Seconds()))
	}
	// extend_params 只能来自 Extra(显式指定),与 Attach 不再抢同一个字段。
	if raw := order.Extra["extend_params"]; raw != "" {
		var ext map[string]string
		if err := json.Unmarshal([]byte(raw), &ext); err == nil && len(ext) > 0 {
			biz["extend_params"] = ext
		}
	}
	return biz
}

// Create 发起支付(当面付 / APP / 手机网站 / 电脑网站)。
//
//	native:当面付,返回二维码内容;
//	app:APP 支付,返回 AppParams["orderString"];
//	wap/web:网页支付,返回 RedirectURL。
func (p *Provider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	if err := order.Validate(); err != nil {
		return nil, err
	}
	// 按支付宝官方的字段约束先拦一道(subject 256、body 128、out_trade_no 64)。
	if err := payment.ValidateChannelOrder(payment.ChannelAlipay, order); err != nil {
		return nil, err
	}
	switch tt {
	case payment.TradeTypeNative:
		return p.createNative(ctx, order, tt)
	case payment.TradeTypeApp:
		return p.createApp(order, tt)
	case payment.TradeTypeWap:
		return p.createRedirect(order, tt, "alipay.trade.wap.pay", "QUICK_WAP_WAY")
	case payment.TradeTypeWeb:
		return p.createRedirect(order, tt, "alipay.trade.page.pay", "FAST_INSTANT_TRADE_PAY")
	default:
		return nil, fmt.Errorf("%w: alipay does not support %s", payment.ErrUnsupportedTradeType, tt)
	}
}

// createNative 当面付(扫码支付)。
func (p *Provider) createNative(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	biz := p.bizCommon(order)
	biz["product_code"] = "FACE_TO_FACE_PAYMENT"
	resp, err := p.call(ctx, "alipay.trade.precreate", biz)
	if err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:    payment.ChannelAlipay,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		QRCode:     str(resp["qr_code"]),
		Extra: map[string]any{
			"trade_no": str(resp["trade_no"]),
		},
	}, nil
}

// createApp APP 支付:返回可直接交给支付宝 SDK 的 orderString。
func (p *Provider) createApp(order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	biz := p.bizCommon(order)
	biz["product_code"] = "QUICK_MSECURITY_PAY"
	signed, err := p.buildParams("alipay.trade.app.pay", biz)
	if err != nil {
		return nil, err
	}
	orderString := encodeQuery(signed)
	return &payment.PaymentSession{
		Channel:    payment.ChannelAlipay,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		AppParams: map[string]any{
			"orderString": orderString,
			"appID":       p.cfg.AppID,
		},
		ClientParams: &payment.ClientParams{
			Alipay: &payment.AlipayClientParams{
				AppID:       p.cfg.AppID,
				OrderString: orderString,
				OutTradeNo:  order.OutTradeNo,
				Amount:      order.Amount,
			},
		},
	}, nil
}

// createRedirect 网页/手机网站支付:返回跳转地址。
func (p *Provider) createRedirect(order *payment.Order, tt payment.TradeType, method, productCode string) (*payment.PaymentSession, error) {
	biz := p.bizCommon(order)
	biz["product_code"] = productCode
	signed, err := p.buildParams(method, biz)
	if err != nil {
		return nil, err
	}
	redirect := p.cfg.Gateway
	if strings.Contains(redirect, "?") {
		redirect += "&" + encodeQuery(signed)
	} else {
		redirect += "?" + encodeQuery(signed)
	}
	return &payment.PaymentSession{
		Channel:     payment.ChannelAlipay,
		TradeType:   tt,
		OutTradeNo:  order.OutTradeNo,
		RedirectURL: redirect,
	}, nil
}

// Query 查询订单状态。
func (p *Provider) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	biz := map[string]any{"out_trade_no": outTradeNo}
	resp, err := p.call(ctx, "alipay.trade.query", biz)
	if err != nil {
		if strings.Contains(err.Error(), "ACQ.TRADE_NOT_EXIST") {
			return nil, fmt.Errorf("%w: alipay order %s not exist", payment.ErrOrderNotFound, outTradeNo)
		}
		return nil, err
	}
	status := str(resp["trade_status"])
	result := &payment.TradeResult{
		Channel:       payment.ChannelAlipay,
		OutTradeNo:    str(resp["out_trade_no"]),
		TransactionID: str(resp["trade_no"]),
		Status:        mapTradeStatus(status),
		Currency:      "CNY",
		Raw: map[string]string{
			"trade_status":       status,
			"buyer_user_id":      str(resp["buyer_user_id"]),
			"send_pay_date":      str(resp["send_pay_date"]),
			"payment_method":     str(resp["payment_method"]),
			"out_business_no":    str(resp["out_business_no"]),
			"fund_bill_list":     str(resp["fund_bill_list"]),
			"transaction_amount": str(resp["total_amount"]),
		},
	}
	if amount, err := payment.YuanToCents(str(resp["total_amount"])); err == nil {
		result.Amount = amount
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", str(resp["send_pay_date"]), time.Local); err == nil {
		result.PaidAt = t
	}
	return result, nil
}

// mapTradeStatus 支付宝交易状态映射。
func mapTradeStatus(status string) payment.TradeStatus {
	switch status {
	case "TRADE_SUCCESS", "TRADE_FINISHED":
		return payment.TradeStatusSuccess
	case "WAIT_BUYER_PAY":
		return payment.TradeStatusPending
	case "TRADE_CLOSED":
		return payment.TradeStatusClosed
	default:
		return payment.TradeStatusPending
	}
}

// Close 关闭订单。
func (p *Provider) Close(ctx context.Context, outTradeNo string) error {
	biz := map[string]any{"out_trade_no": outTradeNo}
	_, err := p.call(ctx, "alipay.trade.close", biz)
	return err
}

// Refund 发起退款。
//
// req.RefundAmount 为 0 表示全额退款(会先查询原单金额)。
func (p *Provider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	if req == nil || req.OutTradeNo == "" {
		return nil, fmt.Errorf("alipay: out_trade_no is required")
	}
	amount := req.RefundAmount
	if amount <= 0 {
		q, err := p.Query(ctx, req.OutTradeNo)
		if err != nil {
			return nil, err
		}
		amount = q.Amount
	}
	if amount <= 0 {
		return nil, fmt.Errorf("%w: alipay refund amount unknown", payment.ErrInvalidConfig)
	}
	biz := map[string]any{
		"out_trade_no":   req.OutTradeNo,
		"out_request_no": req.OutRefundNo,
		"refund_amount":  payment.CentsToYuan(amount),
	}
	if req.Reason != "" {
		biz["refund_reason"] = req.Reason
	}
	resp, err := p.call(ctx, "alipay.trade.refund", biz)
	if err != nil {
		return nil, err
	}
	refundFee, _ := payment.YuanToCents(str(resp["refund_fee"]))
	return &payment.RefundResult{
		Channel:      payment.ChannelAlipay,
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  str(resp["out_request_no"]),
		RefundID:     str(resp["trade_no"]),
		Status:       str(resp["refund_status"]),
		RefundAmount: refundFee,
	}, nil
}
