package wechat

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// Provider 微信支付 APIv3 渠道实现。
type Provider struct {
	cfg     *Config
	client  *http.Client
	privKey *rsa.PrivateKey

	mu            sync.RWMutex
	platformCerts map[string]*x509.Certificate
	// certFetchAt 上次自动刷新平台证书的时间(限流用)。
	certFetchAt time.Time
}

// New 创建微信支付 Provider。
//
// 会校验配置、解析商户私钥、加载本地缓存的平台证书;
// 若配置了 AutoFetchCerts 且本地没有平台证书,会立即下载一次。
func New(cfg *Config) (*Provider, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	priv, err := payment.LoadRSAPrivateKey([]byte(cfg.PrivateKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("wechat: load private key: %w", err)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	p := &Provider{
		cfg:           cfg,
		client:        client,
		privKey:       priv,
		platformCerts: make(map[string]*x509.Certificate),
	}
	for serial, cert := range cfg.PlatformCerts {
		if cert != nil {
			p.platformCerts[serial] = cert
		}
	}
	if err := p.loadCachedCerts(); err != nil {
		return nil, err
	}
	if cfg.AutoFetchCerts && len(p.platformCerts) == 0 {
		if err := p.fetchPlatformCerts(context.Background()); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Channel 渠道标识。
func (p *Provider) Channel() payment.Channel { return payment.ChannelWechat }

// AppID 本商户配置的 AppID(用于回调校验:回调里的 appid 必须等于它)。
func (p *Provider) AppID() string { return p.cfg.AppID }

// MchID 本商户配置的商户号(用于回调校验:回调里的 mchid 必须等于它)。
func (p *Provider) MchID() string { return p.cfg.MchID }

// SupportedTradeTypes 支持的交易类型。
func (p *Provider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{
		payment.TradeTypeNative,
		payment.TradeTypeApp,
		payment.TradeTypeJSAPI,
		payment.TradeTypeH5,
	}
}

// amountPayload 微信金额结构。
type amountPayload struct {
	Total    int64  `json:"total"`
	Currency string `json:"currency"`
}

// Create 发起支付(按交易类型分发到不同下单接口)。
//
// 根据交易类型调用不同的下单接口:
//   - native:POST /v3/pay/transactions/native,返回 code_url(二维码内容);
//   - app:POST /v3/pay/transactions/app,返回 prepayid 与客户端签名参数;
//   - jsapi:POST /v3/pay/transactions/jsapi,返回 prepayid 与 paySign;
//   - h5:POST /v3/pay/transactions/h5,返回 h5_url。
func (p *Provider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	if err := order.Validate(); err != nil {
		return nil, err
	}
	// 按微信官方的字段约束先拦一道(description 127 字符、out_trade_no 6~32、
	// attach 128)。不拦的话,商户填错只会拿到微信一句 "invalid parameter",
	// 定位成本全推给调用方。
	if err := payment.ValidateChannelOrder(payment.ChannelWechat, order); err != nil {
		return nil, err
	}
	switch tt {
	case payment.TradeTypeNative:
		return p.createNative(ctx, order, tt)
	case payment.TradeTypeApp:
		return p.createApp(ctx, order, tt)
	case payment.TradeTypeJSAPI:
		return p.createJSAPI(ctx, order, tt)
	case payment.TradeTypeH5:
		return p.createH5(ctx, order, tt)
	default:
		return nil, fmt.Errorf("%w: wechat does not support %s", payment.ErrUnsupportedTradeType, tt)
	}
}

// transactionBody 统一下单请求体。
type transactionBody struct {
	AppID       string            `json:"appid"`
	MchID       string            `json:"mchid"`
	Description string            `json:"description"`
	OutTradeNo  string            `json:"out_trade_no"`
	NotifyURL   string            `json:"notify_url,omitempty"`
	Amount      amountPayload     `json:"amount"`
	TimeExpire  string            `json:"time_expire,omitempty"`
	Payer       *payerPayload     `json:"payer,omitempty"`
	SceneInfo   *sceneInfoPayload `json:"scene_info,omitempty"`
	Attach      string            `json:"attach,omitempty"`
	GoodsTag    string            `json:"goods_tag,omitempty"`
}

type payerPayload struct {
	OpenID string `json:"openid"`
}

type sceneInfoPayload struct {
	PayerClientIP string         `json:"payer_client_ip,omitempty"`
	DeviceID      string         `json:"device_id,omitempty"`
	H5Info        *h5InfoPayload `json:"h5_info,omitempty"`
}

type h5InfoPayload struct {
	Type string `json:"type"`
}

// prepayResponse 下单响应。
type prepayResponse struct {
	PrepayID string `json:"prepay_id"`
	CodeURL  string `json:"code_url"`
	H5URL    string `json:"h5_url"`
}

// baseBody 构造下单请求公共字段。
func (p *Provider) baseBody(order *payment.Order) transactionBody {
	currency := order.Currency
	if currency == "" {
		currency = "CNY"
	}
	notify := order.NotifyURL
	if notify == "" {
		notify = p.cfg.NotifyURL
	}
	// description 是**一个字段**,微信限制它最多 127 个字符。
	// 拼法放在 payment.WechatDescription 里,校验用的是同一处 ——
	// 否则「拼完才超限」这种情况就漏过去了。
	description := payment.WechatDescription(order.Subject, order.Body)
	body := transactionBody{
		AppID:       p.cfg.AppID,
		MchID:       p.cfg.MchID,
		Description: description,
		OutTradeNo:  order.OutTradeNo,
		NotifyURL:   notify,
		Amount:      amountPayload{Total: order.Amount, Currency: currency},
		Attach:      order.Attach,
	}
	if order.Expire > 0 {
		body.TimeExpire = time.Now().Add(order.Expire).UTC().Format(time.RFC3339)
	}
	return body
}

// createNative 扫码支付(微信 Native)。
func (p *Provider) createNative(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	body := p.baseBody(order)
	if order.ClientIP != "" {
		body.SceneInfo = &sceneInfoPayload{PayerClientIP: order.ClientIP}
	}
	var resp prepayResponse
	if err := p.do(ctx, http.MethodPost, "/v3/pay/transactions/native", nil, body, &resp); err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:    payment.ChannelWechat,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		QRCode:     resp.CodeURL,
	}, nil
}

// createApp APP 拉起支付。
func (p *Provider) createApp(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	body := p.baseBody(order)
	if order.ClientIP != "" {
		body.SceneInfo = &sceneInfoPayload{PayerClientIP: order.ClientIP}
	}
	var resp prepayResponse
	if err := p.do(ctx, http.MethodPost, "/v3/pay/transactions/app", nil, body, &resp); err != nil {
		return nil, err
	}
	params, clientParams, err := p.appSignParams(resp.PrepayID)
	if err != nil {
		return nil, err
	}
	clientParams.OutTradeNo = order.OutTradeNo
	clientParams.Amount = order.Amount
	return &payment.PaymentSession{
		Channel:      payment.ChannelWechat,
		TradeType:    tt,
		OutTradeNo:   order.OutTradeNo,
		PrepayID:     resp.PrepayID,
		AppParams:    params,
		ClientParams: &payment.ClientParams{Wechat: clientParams},
	}, nil
}

// createJSAPI 微信内网页支付。
func (p *Provider) createJSAPI(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	if order.PayerID == "" {
		return nil, fmt.Errorf("wechat: jsapi pay requires payer_id (openid)")
	}
	body := p.baseBody(order)
	body.Payer = &payerPayload{OpenID: order.PayerID}
	if order.ClientIP != "" {
		body.SceneInfo = &sceneInfoPayload{PayerClientIP: order.ClientIP}
	}
	var resp prepayResponse
	if err := p.do(ctx, http.MethodPost, "/v3/pay/transactions/jsapi", nil, body, &resp); err != nil {
		return nil, err
	}
	params, err := p.jsapiSignParams(resp.PrepayID)
	if err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:    payment.ChannelWechat,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		PrepayID:   resp.PrepayID,
		AppParams:  params,
	}, nil
}

// createH5 微信 H5 支付。
func (p *Provider) createH5(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	body := p.baseBody(order)
	scene := &sceneInfoPayload{PayerClientIP: order.ClientIP, H5Info: &h5InfoPayload{Type: "Wap"}}
	if raw := order.Extra["h5_info"]; raw != "" {
		var info h5InfoPayload
		if err := json.Unmarshal([]byte(raw), &info); err == nil && info.Type != "" {
			scene.H5Info = &info
		}
	}
	body.SceneInfo = scene
	var resp prepayResponse
	if err := p.do(ctx, http.MethodPost, "/v3/pay/transactions/h5", nil, body, &resp); err != nil {
		return nil, err
	}
	return &payment.PaymentSession{
		Channel:     payment.ChannelWechat,
		TradeType:   tt,
		OutTradeNo:  order.OutTradeNo,
		RedirectURL: resp.H5URL,
	}, nil
}

// appSignParams 生成 APP 支付客户端调起参数。
//
//	签名串:appid\ntimestamp\nnoncestr\nprepayid\n
//
// 同时返回面向客户端 SDK 的结构化参数(Swift WXApi / Kotlin IWXAPI)。
func (p *Provider) appSignParams(prepayID string) (map[string]any, *payment.WechatClientParams, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce, err := payment.RandomNonce(16)
	if err != nil {
		return nil, nil, err
	}
	nonceStr := fmt.Sprintf("%x", nonce)
	payload := p.cfg.AppID + "\n" + timestamp + "\n" + nonceStr + "\n" + prepayID + "\n"
	sign, err := signClientPayload(p.privKey, payload)
	if err != nil {
		return nil, nil, err
	}
	flat := map[string]any{
		"appid":     p.cfg.AppID,
		"partnerid": p.cfg.MchID,
		"prepayid":  prepayID,
		"package":   "Sign=WXPay",
		"noncestr":  nonceStr,
		"timestamp": timestamp,
		"sign":      sign,
	}
	structured := &payment.WechatClientParams{
		AppID:        p.cfg.AppID,
		PartnerID:    p.cfg.MchID,
		PrepayID:     prepayID,
		NonceStr:     nonceStr,
		TimeStamp:    timestamp,
		PackageValue: "Sign=WXPay",
		Sign:         sign,
	}
	return flat, structured, nil
}

// jsapiSignParams 生成 JSAPI 支付参数。
func (p *Provider) jsapiSignParams(prepayID string) (map[string]any, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce, err := payment.RandomNonce(16)
	if err != nil {
		return nil, err
	}
	nonceStr := fmt.Sprintf("%x", nonce)
	payload := p.cfg.AppID + "\n" + timestamp + "\n" + nonceStr + "\n" + prepayID + "\n"
	sign, err := signClientPayload(p.privKey, payload)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"appId":     p.cfg.AppID,
		"timeStamp": timestamp,
		"nonceStr":  nonceStr,
		"package":   "prepay_id=" + prepayID,
		"signType":  "RSA",
		"paySign":   sign,
	}, nil
}

// queryResponse 订单查询响应。
type queryResponse struct {
	AppID          string        `json:"appid"`
	MchID          string        `json:"mchid"`
	OutTradeNo     string        `json:"out_trade_no"`
	TransactionID  string        `json:"transaction_id"`
	TradeState     string        `json:"trade_state"`
	TradeStateDesc string        `json:"trade_state_desc"`
	BankType       string        `json:"bank_type"`
	Attach         string        `json:"attach"`
	SuccessTime    string        `json:"success_time"`
	Amount         amountPayload `json:"amount"`
}

// Query 查询订单状态。
func (p *Provider) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	query := url.Values{}
	query.Set("mchid", p.cfg.MchID)
	var resp queryResponse
	if err := p.do(ctx, http.MethodGet, "/v3/pay/transactions/out-trade-no/"+url.PathEscape(outTradeNo), query, nil, &resp); err != nil {
		return nil, err
	}
	result := &payment.TradeResult{
		Channel:       payment.ChannelWechat,
		OutTradeNo:    resp.OutTradeNo,
		TransactionID: resp.TransactionID,
		Amount:        resp.Amount.Total,
		Currency:      resp.Amount.Currency,
		Status:        mapTradeState(resp.TradeState),
		Raw: map[string]string{
			"trade_state":      resp.TradeState,
			"trade_state_desc": resp.TradeStateDesc,
			"bank_type":        resp.BankType,
			"attach":           resp.Attach,
			"appid":            resp.AppID,
		},
	}
	if t, err := time.Parse(time.RFC3339, resp.SuccessTime); err == nil {
		result.PaidAt = t
	}
	return result, nil
}

// mapTradeState 微信交易状态映射为统一状态。
func mapTradeState(state string) payment.TradeStatus {
	switch state {
	case "SUCCESS":
		return payment.TradeStatusSuccess
	case "CLOSED", "REVOKED":
		return payment.TradeStatusClosed
	case "NOTPAY", "USERPAYING":
		return payment.TradeStatusPending
	case "PAYERROR":
		return payment.TradeStatusFailed
	default:
		return payment.TradeStatusPending
	}
}

// Close 关闭未支付订单。
func (p *Provider) Close(ctx context.Context, outTradeNo string) error {
	body := map[string]string{"mchid": p.cfg.MchID}
	return p.do(ctx, http.MethodPost, "/v3/pay/transactions/out-trade-no/"+url.PathEscape(outTradeNo)+"/close", nil, body, nil)
}

// refundResponse 退款响应。
type refundResponse struct {
	RefundID    string `json:"refund_id"`
	OutRefundNo string `json:"out_refund_no"`
	Status      string `json:"status"`
	Amount      struct {
		Refund   int64  `json:"refund"`
		Total    int64  `json:"total"`
		Currency string `json:"currency"`
	} `json:"amount"`
}

// Refund 发起退款。
//
// req.RefundAmount 为 0 表示全额退款;req.TotalAmount 为 0 时会先查询订单补齐原单金额。
func (p *Provider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	if req == nil || req.OutTradeNo == "" {
		return nil, fmt.Errorf("wechat: out_trade_no is required")
	}
	total := req.TotalAmount
	if total <= 0 {
		q, err := p.Query(ctx, req.OutTradeNo)
		if err != nil {
			return nil, err
		}
		total = q.Amount
	}
	refund := req.RefundAmount
	if refund <= 0 {
		refund = total
	}
	if refund > total {
		return nil, fmt.Errorf("wechat: refund amount %d exceeds total %d", refund, total)
	}
	body := map[string]any{
		"out_trade_no":  req.OutTradeNo,
		"out_refund_no": req.OutRefundNo,
		"amount": map[string]any{
			"refund":   refund,
			"total":    total,
			"currency": "CNY",
		},
	}
	if req.Reason != "" {
		body["reason"] = req.Reason
	}
	if req.NotifyURL != "" {
		body["notify_url"] = req.NotifyURL
	}
	var resp refundResponse
	if err := p.do(ctx, http.MethodPost, "/v3/refund/domestic/refunds", nil, body, &resp); err != nil {
		return nil, err
	}
	return &payment.RefundResult{
		Channel:      payment.ChannelWechat,
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  resp.OutRefundNo,
		RefundID:     resp.RefundID,
		Status:       resp.Status,
		RefundAmount: resp.Amount.Refund,
	}, nil
}
