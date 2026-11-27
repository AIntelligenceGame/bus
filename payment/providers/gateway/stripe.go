package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/hashicorp/golang-lru/v2"
)

// StripeConfig Stripe 收单网关配置。
//
// 结构体带 mapstructure 标签,可通过 pay.LoadConfigFile 从 YAML/JSON 载入。
type StripeConfig struct {
	// SecretKey Stripe 私钥(sk_...),必填。
	SecretKey string `mapstructure:"secret_key"`
	// PublishableKey 可发布密钥(pk_...),用于客户端初始化钱包 SDK。
	PublishableKey string `mapstructure:"publishable_key"`
	// WebhookSecret Webhook 签名密钥(whsec_...),用于校验回调签名。
	WebhookSecret string `mapstructure:"webhook_secret"`
	// APIBase 接口地址,默认 https://api.stripe.com。
	APIBase string `mapstructure:"api_base"`
	// HTTPClient 自定义 HTTP 客户端(不参与配置解析)。
	HTTPClient *http.Client `mapstructure:"-"`
}

// stripeRefCacheSize 「商户订单号 -> PaymentIntent ID」本地映射的容量上限。
//
// 这张表必须有上限:它只增不减,一个长期运行、订单量大的进程会把它撑到
// 内存里放不下(每条几十字节,几百万单就是几百 MB)。
//
// 淘汰掉条目不要紧 —— 查不到时会退回 Stripe 的 Search API
// (按 metadata 里的 out_trade_no 找),结果一样,只是多一次网络往返。
const stripeRefCacheSize = 10000

// Stripe Stripe 网关实现(PaymentIntent + Webhook 验签)。
//
// 仅使用标准库调用 Stripe REST API,支持 PaymentIntent 创建/扣款/查询/退款与 Webhook 验签。
type Stripe struct {
	cfg    *StripeConfig
	client *http.Client

	// refs 商户订单号 -> PaymentIntent ID 的本地缓存(有容量上限,LRU 淘汰)。
	//
	// 用 hashicorp/golang-lru 而不是裸 map:裸 map 只增不减,是内存泄漏;
	// lru.Cache 自带锁,读写都是 O(1),不需要额外的 mutex。
	refs *lru.Cache[string, string]
}

// newStripeRefs 造一个容量受限的订单号映射缓存。
func newStripeRefs() *lru.Cache[string, string] {
	// lru.New 只在 size <= 0 时报错,这里传的是正数常量,不会失败。
	c, _ := lru.New[string, string](stripeRefCacheSize)
	return c
}

// NewStripe 创建 Stripe 网关。
func NewStripe(cfg *StripeConfig) (*Stripe, error) {
	if cfg == nil || cfg.SecretKey == "" {
		return nil, fmt.Errorf("%w: stripe SecretKey is required", payment.ErrInvalidConfig)
	}
	if cfg.APIBase == "" {
		cfg.APIBase = "https://api.stripe.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &Stripe{cfg: cfg, client: client, refs: newStripeRefs()}, nil
}

// Name 网关名称。
func (s *Stripe) Name() string { return "stripe" }

// stripeObject Stripe 返回对象。
type stripeObject struct {
	ID           string `json:"id"`
	ClientSecret string `json:"client_secret"`
	Amount       int64  `json:"amount"`
	// AmountRefunded 已退金额(Charge 对象自带,官方字段)。
	// charge.refunded 事件里 amount 是**原扣款金额**,amount_refunded 才是已退多少。
	AmountRefunded int64             `json:"amount_refunded"`
	Currency       string            `json:"currency"`
	Status         string            `json:"status"`
	Description    string            `json:"description"`
	Paid           bool              `json:"paid"`
	Metadata       map[string]string `json:"metadata"`
	Created        int64             `json:"created"`
	Charge         string            `json:"charge"`
	LatestCharge   *struct {
		ID       string `json:"id"`
		Refunded bool   `json:"refunded"`
	} `json:"latest_charge"`
	PaymentMethod string `json:"payment_method"`
	Error         *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// request 发送一次 Stripe 请求。
func (s *Stripe) request(ctx context.Context, method, path string, form url.Values, out any) error {
	return s.requestIdem(ctx, method, path, form, out, "")
}

// requestIdem 发一次请求;idemKey 非空时带上 Idempotency-Key 头。
//
// 官方幂等规则(https://docs.stripe.com/api/idempotent_requests):
// 「创建或更新对象时带上幂等键,连接出错时可以安全重试,不会创建出第二个对象」;
// 幂等键放在 Idempotency-Key 请求头,API v1 至少保留 24 小时。
func (s *Stripe) requestIdem(ctx context.Context, method, path string, form url.Values, out any, idemKey string) error {
	endpoint := strings.TrimSuffix(s.cfg.APIBase, "/") + path

	// GET / DELETE 的参数必须放 **query string**。
	//
	// 官方文档里 search 接口的例子就是 curl -G(把数据放进 URL 而不是 body):
	// GET https://api.stripe.com/v1/payment_intents/search?query=...
	// 以前这里把参数塞进 GET 的请求体,Stripe 直接忽略 → 搜不到任何东西,
	// 按商户订单号查单永远返回「订单不存在」。
	var body io.Reader
	if form != nil {
		if method == http.MethodGet || method == http.MethodDelete {
			if len(form) > 0 {
				endpoint += "?" + form.Encode()
			}
		} else {
			body = strings.NewReader(form.Encode())
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return fmt.Errorf("stripe: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.SecretKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if form == nil {
		req.Header.Del("Content-Type")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("stripe: request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("stripe: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("stripe: api error status=%d body=%s", resp.StatusCode, string(raw))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("stripe: decode response: %w", err)
	}
	return nil
}

// idempotencyKey 生成 Stripe 幂等键。
//
// 同一笔订单的同一次操作永远得到同一个键:商户重试(超时重发、用户重复点击)
// 会命中 Stripe 的幂等层,直接拿到第一次的结果,不会真的再扣一次钱。
//
// 官方建议用「有足够随机性的串」并且「别把敏感信息放进键里」
// (https://docs.stripe.com/api/idempotent_requests)—— 取哈希正好两条都满足:
// 定值、不含任何原始信息。
func idempotencyKey(op, tradeNo string) string {
	sum := sha256.Sum256([]byte(op + ":" + tradeNo))
	return hex.EncodeToString(sum[:])
}

// escapeSearchValue 转义 search 查询串里的值。
//
// Stripe 的查询语法里,单引号和反斜杠是特殊字符,不转义的话
// 一个带引号的订单号能把查询语义带偏,搜出别人的单子。
func escapeSearchValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return r.Replace(v)
}

// CreateSession 创建 PaymentIntent,返回客户端 SDK 需要的 client_secret。
//
// 移动端(Apple Pay / Google Pay)拿到 client_secret 后由钱包 SDK 完成支付,
// 再把钱包令牌回传服务端调用 Charge。
func (s *Stripe) CreateSession(ctx context.Context, req SessionRequest) (*Session, error) {
	if req.OutTradeNo == "" {
		return nil, fmt.Errorf("stripe: out_trade_no is required")
	}
	form := url.Values{}
	form.Set("amount", strconv.FormatInt(req.Amount, 10))
	form.Set("currency", currencyCode(req.Currency))
	if req.Description != "" {
		form.Set("description", req.Description)
	}
	methods := req.PaymentMethodTypes
	if len(methods) == 0 {
		methods = []string{"card"}
	}
	for _, m := range methods {
		form.Add("payment_method_types[]", m)
	}
	form.Set("metadata[out_trade_no]", req.OutTradeNo)
	if req.NotifyURL != "" {
		form.Set("metadata[notify_url]", req.NotifyURL)
	}
	for k, v := range req.Extra {
		form.Set("metadata["+k+"]", v)
	}

	var obj stripeObject
	if err := s.requestIdem(ctx, http.MethodPost, "/v1/payment_intents", form, &obj,
		idempotencyKey("session", req.OutTradeNo)); err != nil {
		return nil, err
	}
	s.remember(req.OutTradeNo, obj.ID)
	return &Session{
		Token:          obj.ClientSecret,
		Gateway:        s.Name(),
		Reference:      obj.ID,
		PublishableKey: s.cfg.PublishableKey,
		Extra: map[string]any{
			"payment_method_types": methods,
			"currency":             obj.Currency,
		},
	}, nil
}

// Charge 用钱包令牌直接扣款。
//
// 官方流程是「**确认建会话时那一个** PaymentIntent」,不是再新建一个:
//   - 官方:amount 与 currency 在建 intent 时就定死了,客户端改不了
//     (https://docs.stripe.com/apple-pay);
//   - 客户端拿到的 client_secret **自己就能确认**那个 intent(官方移动端
//     流程正是让客户端拿 client_secret 去确认)—— 服务端要是再新建一个
//     intent 去扣,用户就被扣了两次钱。
//
// 所以这里:先找之前建的那个 intent,找到了就确认它;
// 已经成功了(多半是客户端自己确认的)就直接返回,不能再扣一次;
// 实在找不到才退回「新建并确认」(从没建过会话的兜底路径)。
func (s *Stripe) Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error) {
	if req.PaymentToken == "" {
		return nil, fmt.Errorf("stripe: payment token is required")
	}
	id, err := s.resolveIntentID(ctx, req.OutTradeNo)
	if err != nil {
		// 「查挂了」不等于「查不到」。查询失败时**绝不能**当成「没建过会话」
		// 去新建一个 intent:客户端拿的是建会话时那个 intent 的 client_secret,
		// 服务端另建一个去扣,用户就被扣了两次钱。
		// (建会话和扣款用的是不同的幂等键,Stripe 挡不住第二笔。)
		return nil, fmt.Errorf("stripe: 查询订单 %s 的 PaymentIntent 失败: %w", req.OutTradeNo, err)
	}
	if id != "" {
		res, err := s.confirmIntent(ctx, id, req)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, errIntentGone) {
			return nil, err
		}
		// 之前那个 intent 已经作废了(没扣过钱)—— 重新建一个是安全的。
	}
	return s.createAndConfirm(ctx, req)
}

// errIntentGone 之前的 PaymentIntent 作废了,不能确认了。
var errIntentGone = errors.New("stripe: payment intent is canceled")

// resolveIntentID 找出这笔订单之前建出来的 PaymentIntent。
//
// 先看本地缓存(同一进程里建的会话立刻就能拿到),再退回 Search API
// (进程重启、多实例部署、或者本地缓存条目被淘汰时靠它找回来)。
//
// 查不到时返回空串;err 只表示**查询过程**出了问题(网络、鉴权等),
// 与「Stripe 上确实没有这笔单」是两回事 —— 调用方要区分对待,
// 别把「查挂了」当成「订单不存在」。
func (s *Stripe) resolveIntentID(ctx context.Context, outTradeNo string) (string, error) {
	if outTradeNo == "" {
		return "", nil
	}
	if id := s.lookup(outTradeNo); id != "" {
		return id, nil
	}
	// 直接给了 PaymentIntent ID 的情况。
	if strings.HasPrefix(outTradeNo, "pi_") {
		return outTradeNo, nil
	}
	return s.searchByTradeNo(ctx, outTradeNo)
}

// confirmIntent 确认已有的 PaymentIntent。
func (s *Stripe) confirmIntent(ctx context.Context, id string, req ChargeRequest) (*ChargeResult, error) {
	var obj stripeObject
	// 先看一眼状态:已经扣过款的绝不能再扣一次。
	if err := s.request(ctx, http.MethodGet, "/v1/payment_intents/"+id, nil, &obj); err != nil {
		return nil, err
	}
	switch obj.Status {
	case "succeeded", "processing", "requires_action", "requires_capture":
		// 已经成功 / 在途 / 等客户做动作(3DS)—— 都不能再发起一次扣款,
		// 结果以回调为准。
		return chargeResultOf(obj, req.OutTradeNo), nil
	case "requires_payment_method", "requires_confirmation":
		// 可以确认,继续往下走。
	case "canceled":
		return nil, fmt.Errorf("%w: %s", errIntentGone, id)
	default:
		return nil, fmt.Errorf("stripe: payment intent %s is in status %s, cannot confirm", id, obj.Status)
	}
	form := url.Values{}
	form.Set("payment_method", req.PaymentToken)
	// 幂等键:同一个 intent 重复确认,Stripe 直接返回第一次的结果,
	// 不会真的再扣一次钱。
	if err := s.requestIdem(ctx, http.MethodPost, "/v1/payment_intents/"+id+"/confirm",
		form, &obj, idempotencyKey("confirm", id)); err != nil {
		return nil, err
	}
	s.remember(req.OutTradeNo, obj.ID)
	return chargeResultOf(obj, req.OutTradeNo), nil
}

// createAndConfirm 新建并立即确认一个 PaymentIntent(没建过会话时的兜底路径)。
func (s *Stripe) createAndConfirm(ctx context.Context, req ChargeRequest) (*ChargeResult, error) {
	form := url.Values{}
	form.Set("amount", strconv.FormatInt(req.Amount, 10))
	form.Set("currency", currencyCode(req.Currency))
	form.Set("payment_method", req.PaymentToken)
	form.Set("confirm", "true")
	if req.Description != "" {
		form.Set("description", req.Description)
	}
	form.Set("metadata[out_trade_no]", req.OutTradeNo)
	if req.PaymentMethod != "" {
		form.Set("metadata[payment_method]", req.PaymentMethod)
	}
	for k, v := range req.Extra {
		form.Set("metadata["+k+"]", v)
	}

	var obj stripeObject
	// 幂等键:同一笔订单重复扣款时,Stripe 直接返回第一次的结果,
	// 不会真的再扣一次钱(官方:重试不会创建第二个对象)。
	if err := s.requestIdem(ctx, http.MethodPost, "/v1/payment_intents", form, &obj,
		idempotencyKey("charge", req.OutTradeNo)); err != nil {
		return nil, err
	}
	s.remember(req.OutTradeNo, obj.ID)
	return chargeResultOf(obj, req.OutTradeNo), nil
}

// chargeResultOf 把 PaymentIntent 响应转成统一结果。
func chargeResultOf(obj stripeObject, outTradeNo string) *ChargeResult {
	result := &ChargeResult{
		Reference:  obj.ID,
		OutTradeNo: outTradeNo,
		Status:     obj.Status,
		Amount:     obj.Amount,
		Currency:   obj.Currency,
		Paid:       obj.Status == "succeeded",
		Raw:        map[string]string{"status": obj.Status, "payment_method": obj.PaymentMethod},
	}
	if result.OutTradeNo == "" {
		result.OutTradeNo = obj.Metadata["out_trade_no"]
	}
	return result
}

// Query 查询扣款结果。
//
// reference 可以是 PaymentIntent ID,也可以是商户订单号(内部通过 Search API 定位)。
func (s *Stripe) Query(ctx context.Context, reference string) (*ChargeResult, error) {
	id, err := s.resolveIntentID(ctx, reference)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, fmt.Errorf("%w: stripe order %s not found", payment.ErrOrderNotFound, reference)
	}
	var obj stripeObject
	if err := s.request(ctx, http.MethodGet, "/v1/payment_intents/"+id, nil, &obj); err != nil {
		return nil, err
	}
	return &ChargeResult{
		Reference:  obj.ID,
		OutTradeNo: obj.Metadata["out_trade_no"],
		Status:     obj.Status,
		Amount:     obj.Amount,
		Currency:   obj.Currency,
		Paid:       obj.Status == "succeeded",
		Raw:        map[string]string{"status": obj.Status, "created": strconv.FormatInt(obj.Created, 10)},
	}, nil
}

// searchByTradeNo 通过 Search API 按商户订单号查找 PaymentIntent。
func (s *Stripe) searchByTradeNo(ctx context.Context, outTradeNo string) (string, error) {
	form := url.Values{}
	form.Set("query", fmt.Sprintf("metadata['out_trade_no']:'%s'", escapeSearchValue(outTradeNo)))
	form.Set("limit", "1")
	var resp struct {
		Data []stripeObject `json:"data"`
	}
	if err := s.request(ctx, http.MethodGet, "/v1/payment_intents/search", form, &resp); err != nil {
		return "", err
	}
	if len(resp.Data) == 0 {
		return "", nil
	}
	s.remember(outTradeNo, resp.Data[0].ID)
	return resp.Data[0].ID, nil
}

// Refund 退款。
func (s *Stripe) Refund(ctx context.Context, req RefundRequest) (*RefundResult, error) {
	form := url.Values{}
	if req.RefundID != "" {
		form.Set("metadata[out_refund_no]", req.RefundID)
	}
	if req.OutTradeNo != "" {
		form.Set("metadata[out_trade_no]", req.OutTradeNo)
		form.Set("metadata[reason]", req.Reason)
		// 退款**必须**指明退的是哪一笔:官方要求 payment_intent 或 charge
		// 二选一必填(https://docs.stripe.com/api/refunds/create),
		// 两个都不给会报 "One of the following params should be provided
		// for this request: payment_intent or charge"。
		// metadata 只是备注,Stripe 不拿它定位订单 —— 光给 metadata 一定退不了。
		//
		// 所以这里不能只查本地缓存:进程重启、多实例部署、或者缓存条目
		// 被 LRU 淘汰之后,缓存里就没这笔单了,只查缓存会让退款**直接失败**。
		// 查不到就退回 Search API 再找一次(见 resolveIntentID)。
		id, err := s.resolveIntentID(ctx, req.OutTradeNo)
		if err != nil {
			// 查询失败 ≠ 订单不存在:把网络/鉴权错误如实报出去,
			// 别让商户以为「这单没付过」而去重复下单。
			return nil, fmt.Errorf("stripe: 查询订单 %s 的 PaymentIntent 失败: %w", req.OutTradeNo, err)
		}
		if id == "" {
			return nil, fmt.Errorf("%w: stripe 找不到订单 %s 对应的 PaymentIntent,无法退款",
				payment.ErrOrderNotFound, req.OutTradeNo)
		}
		form.Set("payment_intent", id)
	}
	if req.Amount > 0 {
		form.Set("amount", strconv.FormatInt(req.Amount, 10))
	}
	var obj struct {
		ID     string `json:"id"`
		Amount int64  `json:"amount"`
		Status string `json:"status"`
	}
	if err := s.requestIdem(ctx, http.MethodPost, "/v1/refunds", form, &obj,
		idempotencyKey("refund", req.OutTradeNo+"|"+req.RefundID)); err != nil {
		return nil, err
	}
	return &RefundResult{
		RefundID:  req.RefundID,
		Reference: obj.ID,
		Status:    obj.Status,
		Amount:    obj.Amount,
	}, nil
}

// stripeEvent Stripe Webhook 事件。
type stripeEvent struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Created int64  `json:"created"`
	Data    struct {
		Object stripeObject `json:"object"`
	} `json:"data"`
}

// ParseWebhook 校验 Stripe-Signature 并解析事件。
//
// 签名串为 "<timestamp>.<payload>",HMAC-SHA256 后十六进制。
func (s *Stripe) ParseWebhook(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	if req == nil {
		return nil, fmt.Errorf("stripe: nil webhook request")
	}
	if s.cfg.WebhookSecret == "" {
		return nil, fmt.Errorf("%w: stripe WebhookSecret is required to verify webhook", payment.ErrInvalidConfig)
	}
	if err := s.verifySignature(req.Header("Stripe-Signature"), req.Body); err != nil {
		return nil, err
	}
	var event stripeEvent
	if err := json.Unmarshal(req.Body, &event); err != nil {
		return nil, fmt.Errorf("stripe: decode webhook: %w", err)
	}
	obj := event.Data.Object
	notify := &payment.Notify{
		// Channel 由上层渠道包(applepay / googlepay)改写,网关本身不感知渠道。
		OutTradeNo:    obj.Metadata["out_trade_no"],
		TransactionID: obj.ID,
		Amount:        obj.Amount,
		Currency:      obj.Currency,
		Raw: map[string]string{
			"event_type": event.Type,
			"event_id":   event.ID,
			"status":     obj.Status,
			// 已退金额:上层据此判断「全额退款」还是「部分退款」。
			"amount_refunded": strconv.FormatInt(obj.AmountRefunded, 10),
		},
	}
	switch event.Type {
	case "payment_intent.succeeded", "charge.succeeded":
		notify.Status = payment.TradeStatusSuccess
	case "payment_intent.payment_failed", "charge.failed":
		notify.Status = payment.TradeStatusFailed
	case "payment_intent.canceled":
		notify.Status = payment.TradeStatusClosed
	case "charge.refunded":
		notify.Status = payment.TradeStatusRefunded
	default:
		notify.Status = payment.TradeStatusPending
	}
	if event.Created > 0 {
		notify.EventTime = time.Unix(event.Created, 0)
	}
	return notify, nil
}

// verifySignature 校验 Stripe 回调签名。
func (s *Stripe) verifySignature(header string, body []byte) error {
	if header == "" {
		return fmt.Errorf("%w: stripe signature header missing", payment.ErrInvalidSignature)
	}
	var timestamp string
	var signatures []string
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "t="):
			timestamp = strings.TrimPrefix(part, "t=")
		case strings.HasPrefix(part, "v1="):
			signatures = append(signatures, strings.TrimPrefix(part, "v1="))
		}
	}
	if timestamp == "" || len(signatures) == 0 {
		return fmt.Errorf("%w: malformed stripe signature header", payment.ErrInvalidSignature)
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: malformed stripe signature timestamp", payment.ErrInvalidSignature)
	}
	if time.Since(time.Unix(ts, 0)) > 5*time.Minute {
		return fmt.Errorf("%w: stripe signature timestamp expired", payment.ErrInvalidSignature)
	}
	payload := timestamp + "." + string(body)
	expected := payment.HMACSHA256Hex([]byte(s.cfg.WebhookSecret), []byte(payload))
	for _, sig := range signatures {
		if payment.HMACEqual(expected, sig) {
			return nil
		}
	}
	return fmt.Errorf("%w: stripe signature mismatch", payment.ErrInvalidSignature)
}

// remember 记录订单号与 PaymentIntent 的映射。
func (s *Stripe) remember(outTradeNo, id string) {
	if outTradeNo == "" || id == "" || s.refs == nil {
		return
	}
	s.refs.Add(outTradeNo, id)
}

// lookup 查询本地缓存的映射。
//
// 查不到**不代表**没有这单 —— 只是本地缓存里没有(进程重启过、
// 多实例部署、或者条目被 LRU 淘汰了)。调用方要拿它当「事实」用的话,
// 必须走 findIntentID(会退回 Stripe 的 Search API 再找一次)。
func (s *Stripe) lookup(outTradeNo string) string {
	if s.refs == nil {
		return ""
	}
	v, ok := s.refs.Get(outTradeNo)
	if !ok {
		return ""
	}
	return v
}

// currencyCode 归一化币种(Stripe 使用小写)。
func currencyCode(currency string) string {
	if currency == "" {
		return "cny"
	}
	return strings.ToLower(currency)
}
