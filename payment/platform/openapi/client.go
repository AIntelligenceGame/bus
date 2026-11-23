// Package openapi 是「支付配置选择平台」开放接口的 Go 客户端。
//
// 商户自己的业务后端用它直接调平台的下单 / 查单 / 退款接口,
// **不需要自己实现签名** —— 签名算法在这里实现了一遍,和平台端
// (payment/platform/apikey.go)保持一致,改了一边记得改另一边。
//
// 用法:
//
//	c := openapi.New("https://pay.example.com", "pk_xxx", "sk_xxx")
//
//	// 下单(APP 拉起微信)
//	res, err := c.Pay(ctx, openapi.PayRequest{
//		Channel:   "wechat",
//		TradeType: "app",
//		Subject:   "iPhone 17 Pro",
//		Amount:    19900, // 单位:分
//	})
//	if err != nil {
//		return err
//	}
//	// res.Session.ClientParams.Wechat 就是给客户端 SDK 用的字段
//
//	// 查单
//	q, err := c.Query(ctx, openapi.QueryRequest{
//		Channel:    "wechat",
//		OutTradeNo: res.OrderNo,
//	})
//
//	// 退款(同一个 out_refund_no 重复提交只会退一次钱)
//	rf, err := c.Refund(ctx, openapi.RefundRequest{
//		OutTradeNo:  res.OrderNo,
//		OutRefundNo: res.OrderNo + "-R1",
//		RefundAmount: 19900,
//		Reason:       "用户申请退款",
//	})
//
// 密钥明文只在控制台创建时显示一次,拿不到就重新建一把。
package openapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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

// Client 开放接口客户端。
type Client struct {
	baseURL string
	keyID   string
	secret  string
	http    *http.Client
}

// New 创建客户端。
//
//	baseURL 平台对外地址,例如 https://pay.example.com(不要带尾斜杠)。
//	keyID   密钥 ID(形如 pk_xxx),控制台创建密钥时给。
//	secret  密钥明文(形如 sk_xxx),只显示一次。
func New(baseURL, keyID, secret string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		keyID:   keyID,
		secret:  secret,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// WithHTTPClient 自定义 HTTP 客户端(设置代理、超时等)。
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	if h != nil {
		c.http = h
	}
	return c
}

// ---------------------------------------------------------------- 请求 / 响应

// PayRequest 下单请求。
type PayRequest struct {
	// Channel 渠道:wechat / alipay / applepay / googlepay。
	Channel string `json:"channel,omitempty"`
	// TradeType 交易类型:app / native / wallet / jsapi 等,留空由平台按渠道推断。
	TradeType string `json:"trade_type,omitempty"`
	// OutTradeNo 商户订单号,留空则平台自动生成。
	// 强烈建议自己传:它是渠道侧的幂等键,重复传同一个单号不会重复下单。
	OutTradeNo string `json:"out_trade_no,omitempty"`
	// Subject 订单标题。
	Subject string `json:"subject,omitempty"`
	// Amount 金额,单位:分。
	Amount int64 `json:"amount"`
	// PayerID 微信 openid / 支付宝 buyer_id(JSAPI 场景需要)。
	PayerID string `json:"payer_id,omitempty"`
	// Attach 业务附加数据,渠道回调时回传给商户。
	//
	// 各渠道落在哪个字段上不一样(平台负责映射,不用自己拼):
	//   - 微信:attach(最多 128 字符);
	//   - 支付宝:passback_params(最多 512 字符,平台会自动 UrlEncoded;
	//     官方要求必须编码,且不能是 JSON 格式)。
	Attach string `json:"attach,omitempty"`
	// AppID 订单归属的应用(产品),留空 = 未分类。
	//
	// 一个商户有多个产品时,每个产品可以用自己的一套渠道配置
	// (各自的微信 appid / 商户号)。订单会记下 app_id,
	// 退款 / 查单 / 关单会自动用**这笔单当初那套**凭证,不会串。
	//
	// 密钥绑定了应用时**不用传**:平台自动用密钥绑定的那个应用;
	// 传了别的 app_id 会被直接拒绝(400)。
	AppID string `json:"app_id,omitempty"`
}

// PayResult 下单结果。
type PayResult struct {
	// OrderNo 平台侧订单号(= 请求里的 out_trade_no)。
	OrderNo string `json:"order_no"`
	// Amount 金额(分)。
	Amount int64 `json:"amount"`
	// Yuan 金额(元),方便直接展示。
	Yuan string `json:"yuan"`
	// NotifyURL 平台给渠道的回调地址。
	NotifyURL string `json:"notify_url"`
	// QRCodeURL 二维码图片地址(扫码场景)。
	QRCodeURL string `json:"qrcode_url"`
	// Session 支付会话。APP 拉起用 Session.ClientParams,
	// 扫码用 Session.QRCode。
	Session *payment.PaymentSession `json:"session"`
	// Usage 接入说明(按渠道与交易类型给出的提示文案)。
	Usage []string `json:"usage"`
}

// QueryRequest 查单请求(同步渠道)。
type QueryRequest struct {
	Channel    string `json:"channel"`
	OutTradeNo string `json:"out_trade_no"`
}

// QueryResult 查单结果。
type QueryResult struct {
	// Status 订单状态:PENDING / SUCCESS / FAILED / CLOSED / REFUNDED / PARTIALLY_REFUNDED。
	Status string `json:"status"`
	// TransactionID 渠道流水号。
	TransactionID string `json:"transaction_id"`
	// Channel 订单所属渠道。
	Channel string `json:"channel"`
}

// RefundRequest 退款请求。
type RefundRequest struct {
	// OutTradeNo 原订单号。
	OutTradeNo string `json:"out_trade_no"`
	// OutRefundNo 商户退款单号 —— **幂等键**。
	// 同一个单号重复提交只会退一次钱;渠道失败后用同一个单号重试也可以。
	OutRefundNo string `json:"out_refund_no"`
	// RefundAmount 退款金额(分),0 表示全额。
	RefundAmount int64 `json:"refund_amount,omitempty"`
	// Reason 退款原因。
	Reason string `json:"reason,omitempty"`
}

// RefundResult 退款结果。
type RefundResult struct {
	Result struct {
		Channel     string `json:"channel"`
		OutTradeNo  string `json:"out_trade_no"`
		OutRefundNo string `json:"out_refund_no"`
		Status      string `json:"status"`
	} `json:"result"`
	// RefundAmount 本次退款金额(分)。
	RefundAmount int64 `json:"refund_amount"`
	// RefundedTotal 累计已退金额(分)。
	RefundedTotal int64 `json:"refunded_total"`
	// OrderStatus 退款后的订单状态。
	OrderStatus string `json:"order_status"`
	// Idempotent true 表示这是重复提交,平台原样返回上次的结果,没有再次出款。
	Idempotent bool `json:"idempotent"`
}

// WalletRequest 钱包渠道扣款请求(Apple Pay / Google Pay 的最后一步)。
type WalletRequest struct {
	Channel string `json:"channel,omitempty"`
	// OutTradeNo 原订单号。
	OutTradeNo string `json:"out_trade_no"`
	// PaymentToken 客户端钱包 SDK 拿到的支付令牌。
	PaymentToken string `json:"payment_token"`
	// Amount 金额(分),0 表示按订单金额。
	Amount int64 `json:"amount,omitempty"`
}

// WalletResult 钱包扣款结果。
type WalletResult struct {
	Status        string `json:"status"`
	TransactionID string `json:"transaction_id"`
	Channel       string `json:"channel"`
}

// OrdersQuery 订单列表查询条件。
type OrdersQuery struct {
	Page    int
	Size    int
	Status  string
	Channel string
	// AppID 只看某个应用(留空 = 全部)。
	AppID   string
	Keyword string
	From    string
	To      string
}

// Order 订单记录(字段与平台 /api/open/orders 返回的一致)。
//
// 刻意**不**复用平台包里的类型:开放接口 SDK 要能单独拿去用,
// 不能拖着整个服务端(GORM、加密、依赖)走。
type Order struct {
	OutTradeNo string `json:"out_trade_no"`
	UserID     string `json:"user_id"`
	// AppID 订单归属的应用(留空 = 未分类)。
	AppID     string `json:"app_id"`
	Channel   string `json:"channel"`
	TradeType string `json:"trade_type"`
	Subject   string `json:"subject"`
	// Amount 金额,单位:分。
	Amount int64 `json:"amount"`
	// AmountYuan 金额(元),直接展示用。
	AmountYuan    string `json:"amount_yuan"`
	Status        string `json:"status"`
	TransactionID string `json:"transaction_id,omitempty"`
	QRCode        string `json:"qrcode,omitempty"`
	// CreatedAt / UpdatedAt 是 RFC3339 时间字符串。
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	// ChannelCreated 渠道侧是否真的有这笔单。
	ChannelCreated bool `json:"channel_created"`
}

// Stats 交易统计。
type Stats struct {
	// OrderCount 订单总数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// FailedCount 失败 / 已关闭订单数。
	FailedCount int64 `json:"failed_count"`
	// RefundCount 退款笔数(只算真的退出去的那些)。
	RefundCount int64 `json:"refund_count"`
	// TotalAmount 支付成功金额合计,单位:分。
	TotalAmount int64 `json:"total_amount"`
	// RefundAmount 退款金额合计,单位:分。
	RefundAmount int64 `json:"refund_amount"`
	// NetAmount 净收入 = 成功金额 - 退款金额,单位:分。
	NetAmount int64 `json:"net_amount"`
	// TodayCount 今日订单数。
	TodayCount int64 `json:"today_count"`
	// TodayAmount 今日成功金额,单位:分。
	TodayAmount int64 `json:"today_amount"`
	// SuccessRate 成功率(0~1)。
	SuccessRate float64 `json:"success_rate"`
}

// OrdersResult 订单列表。
type OrdersResult struct {
	Orders []Order `json:"orders"`
	Page   int     `json:"page"`
	Size   int     `json:"size"`
	// Total 总条数。平台那边是 int64,这里也用 int64:
	// 用 int 的话在 32 位平台上会溢出(订单量大的商户迟早踩到)。
	Total int64 `json:"total"`
	Pages int   `json:"pages"`
}

// ---------------------------------------------------------------- 接口

// Pay 下单。
func (c *Client) Pay(ctx context.Context, req PayRequest) (*PayResult, error) {
	var out PayResult
	if err := c.do(ctx, http.MethodPost, "/api/open/pay", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Query 查单(会向渠道发一次查询请求)。
func (c *Client) Query(ctx context.Context, req QueryRequest) (*QueryResult, error) {
	var out QueryResult
	if err := c.do(ctx, http.MethodPost, "/api/open/query", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Order 查订单详情(只查平台自己的记录,不向渠道发请求)。
func (c *Client) Order(ctx context.Context, outTradeNo string) (map[string]any, error) {
	out := map[string]any{}
	if err := c.do(ctx, http.MethodPost, "/api/open/order", nil,
		map[string]string{"out_trade_no": outTradeNo}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Close 关闭未支付订单。
func (c *Client) Close(ctx context.Context, channel, outTradeNo string) error {
	return c.do(ctx, http.MethodPost, "/api/open/close", nil,
		map[string]string{"channel": channel, "out_trade_no": outTradeNo}, nil)
}

// Refund 退款。同一个 OutRefundNo 重复提交只会退一次钱。
func (c *Client) Refund(ctx context.Context, req RefundRequest) (*RefundResult, error) {
	var out RefundResult
	if err := c.do(ctx, http.MethodPost, "/api/open/refund", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// WalletCharge 钱包渠道扣款。
func (c *Client) WalletCharge(ctx context.Context, req WalletRequest) (*WalletResult, error) {
	var out WalletResult
	if err := c.do(ctx, http.MethodPost, "/api/open/wallet/charge", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Orders 订单列表。
func (c *Client) Orders(ctx context.Context, q OrdersQuery) (*OrdersResult, error) {
	query := url.Values{}
	if q.Page > 0 {
		query.Set("page", strconv.Itoa(q.Page))
	}
	if q.Size > 0 {
		query.Set("size", strconv.Itoa(q.Size))
	}
	if q.Status != "" {
		query.Set("status", q.Status)
	}
	if q.Channel != "" {
		query.Set("channel", q.Channel)
	}
	if q.AppID != "" {
		query.Set("app_id", q.AppID)
	}
	if q.Keyword != "" {
		query.Set("keyword", q.Keyword)
	}
	if q.From != "" {
		query.Set("from", q.From)
	}
	if q.To != "" {
		query.Set("to", q.To)
	}
	var out OrdersResult
	if err := c.do(ctx, http.MethodGet, "/api/open/orders", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Stats 交易统计。
func (c *Client) Stats(ctx context.Context) (*Stats, error) {
	var out Stats
	if err := c.do(ctx, http.MethodGet, "/api/open/stats", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StatsQuery 分组统计的查询条件。
type StatsQuery struct {
	// AppID 只统计某个应用(留空 = 全部)。
	AppID string
	// GroupBy 分组维度:app / channel / day。留空 = 不分组(只要总数)。
	GroupBy string
}

// StatGroup 一个分组上的统计。
type StatGroup struct {
	// Key 分组键(应用 ID / 渠道 / 日期 YYYY-MM-DD)。
	Key string `json:"key"`
	// Label 分组显示名(应用名 / 渠道名 / 日期)。
	Label string `json:"label"`
	// OrderCount 订单总数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// RefundCount 退款笔数。
	RefundCount int64 `json:"refund_count"`
	// TotalAmount 支付成功金额合计(分)。
	TotalAmount int64 `json:"total_amount"`
	// RefundAmount 退款金额合计(分)。
	RefundAmount int64 `json:"refund_amount"`
	// NetAmount 净收入(分)。
	NetAmount int64 `json:"net_amount"`
}

// StatsResult 分组统计的结果。
type StatsResult struct {
	// Stats 合计口径的统计(与不分组时一致)。
	Stats Stats `json:"stats"`
	// GroupBy 实际采用的分组维度。
	GroupBy string `json:"group_by"`
	// Groups 各分组的明细。
	Groups []StatGroup `json:"groups"`
	// Truncated 结果是否被截断。
	//
	// 按天分组时平台会限制单次扫描的行数,超了就把标志置成 true ——
	// 这时 Groups 里只有最近的一部分数据,拿它当全量算会算错。
	// 调用方要自己判断:要么提示用户,要么改用按应用 / 按渠道分组。
	Truncated bool `json:"truncated"`
}

// StatsGrouped 分组统计(按应用 / 渠道 / 天)。
//
// GroupBy 传空串时,行为和 Stats() 一样(平台返回的就是总数)。
func (c *Client) StatsGrouped(ctx context.Context, q StatsQuery) (*StatsResult, error) {
	query := url.Values{}
	if q.AppID != "" {
		query.Set("app_id", q.AppID)
	}
	if q.GroupBy != "" {
		query.Set("group_by", q.GroupBy)
	}
	var out StatsResult
	if err := c.do(ctx, http.MethodGet, "/api/open/stats", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------- 内部

// do 发一个带签名的请求。
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	if c.keyID == "" || c.secret == "" {
		return fmt.Errorf("openapi: 密钥未配置(key_id 与 secret 都不能为空)")
	}
	var raw []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("openapi: 编码请求体: %w", err)
		}
		raw = encoded
	}
	// TARGET = 路径 + "?" + 原始查询串。
	//
	// 查询串也参与签名,是为了防止「改一下分页参数就能多看别人的订单」。
	// 所以这里必须把**发出去的**那份查询串原样拿去签名,不能重新编码一遍。
	target := path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce, err := randomNonce()
	if err != nil {
		return err
	}
	signature := signRequest(c.secret, method, target, ts, nonce, raw)

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+target, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("openapi: 构造请求: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Pay-Key", c.keyID)
	req.Header.Set("X-Pay-Timestamp", ts)
	req.Header.Set("X-Pay-Nonce", nonce)
	req.Header.Set("X-Pay-Signature", signature)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("openapi: 请求失败: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("openapi: 读取响应: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("openapi: 解析响应: %w (原始内容: %s)", err, string(data))
	}
	return nil
}

// APIError 平台返回了错误状态码。
type APIError struct {
	Status int
	Body   string
}

// Error 实现 error。
func (e *APIError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("openapi: 平台返回 HTTP %d", e.Status)
	}
	return fmt.Sprintf("openapi: 平台返回 HTTP %d: %s", e.Status, e.Body)
}

// Sign 计算请求签名(hex,小写)。
//
// 与平台端 (payment/platform/apikey.go 的 signRequest) 一一对应:
//
//	待签名字符串 = METHOD \n TARGET \n TIMESTAMP \n NONCE \n sha256(body) 的 hex
//	签名 = hex(HMAC-SHA256(密钥明文, 待签名字符串))
//
// 导出它是为了让**别的语言**的接入方也能照着实现同一套算法:
// 用同样的输入调 Sign,拿到一样的结果就说明实现对了。
//
//	TARGET = 路径 + ("?" + 原始查询串),查询串没有时只有路径。
//	body 为空时取空字节串的 sha256。
func Sign(secret, method, target, timestamp, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	canonical := strings.Join([]string{
		strings.ToUpper(method),
		target,
		timestamp,
		nonce,
		hex.EncodeToString(sum[:]),
	}, "\n")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

// signRequest 计算请求签名(内部用,等价于 Sign)。
func signRequest(secret, method, target, timestamp, nonce string, body []byte) string {
	return Sign(secret, method, target, timestamp, nonce, body)
}

// randomNonce 生成 16 字节随机串(hex)。
//
// 平台在时间窗口内记住所用过的随机串,重复出现会被判成重放并拒绝。
func randomNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("openapi: 生成随机串: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
