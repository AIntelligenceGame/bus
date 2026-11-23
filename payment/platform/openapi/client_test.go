package openapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// ---------------------------------------------------------------- 测试脚手架

// captured 记录服务端收到的请求,供断言用。
type captured struct {
	Method string
	Path   string
	RawURL string
	Query  url.Values
	Body   []byte
	Header http.Header
}

// capture 收请求的记录。
//
// handler 跑在 httptest 的 goroutine 里,测试在另一个 goroutine 里读 ——
// 直接读字段属于跨 goroutine 共享内存访问(跑 -race 会报)。
// 所以写和读都过同一把锁,读的时候拿一份**快照**。
type capture struct {
	mu sync.Mutex
	c  captured
}

// snapshot 返回请求记录的一份独立拷贝。
func (c *capture) snapshot() captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	return captured{
		Method: c.c.Method,
		Path:   c.c.Path,
		RawURL: c.c.RawURL,
		Query:  c.c.Query,
		Body:   c.c.Body,
		Header: c.c.Header,
	}
}

// newServer 起一个假平台:handler 收到请求后返回 statusCode 与 body。
func newServer(t *testing.T, statusCode int, body string) (*httptest.Server, *capture) {
	t.Helper()
	got := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got.mu.Lock()
		got.c = captured{
			Method: r.Method,
			Path:   r.URL.Path,
			RawURL: r.URL.String(),
			Query:  r.URL.Query(),
			Body:   raw,
			Header: r.Header.Clone(),
		}
		got.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func newClient(srv *httptest.Server) *Client {
	return New(srv.URL, "pk_test", "sk_test")
}

// ---------------------------------------------------------------- New / 配置

func TestNewTrimsTrailingSlash(t *testing.T) {
	c := New("https://pay.example.com/", "pk", "sk")
	if c.baseURL != "https://pay.example.com" {
		t.Fatalf("baseURL 没去掉尾斜杠: %q", c.baseURL)
	}
}

func TestWithHTTPClientKeepsReceiver(t *testing.T) {
	c := New("https://x", "pk", "sk")
	custom := &http.Client{}
	got := c.WithHTTPClient(custom)
	if got != c {
		t.Fatal("WithHTTPClient 必须返回自身,支持链式调用")
	}
	if c.http != custom {
		t.Fatal("自定义的 HTTP 客户端没生效")
	}
}

func TestWithHTTPClientIgnoresNil(t *testing.T) {
	c := New("https://x", "pk", "sk")
	before := c.http
	c.WithHTTPClient(nil)
	if c.http != before {
		t.Fatal("传 nil 不应该把 HTTP 客户端清空")
	}
}

// ---------------------------------------------------------------- 密钥缺失

func TestMissingKeyIDIsRejectedBeforeRequest(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := New(srv.URL, "", "sk_test")
	if _, err := c.Stats(context.Background()); err == nil {
		t.Fatal("缺 key_id 必须报错")
	} else if !strings.Contains(err.Error(), "密钥未配置") {
		t.Fatalf("错误信息不对: %v", err)
	}
	if got.snapshot().Method != "" {
		t.Fatal("密钥没配就不该发出请求")
	}
}

func TestMissingSecretIsRejectedBeforeRequest(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := New(srv.URL, "pk_test", "")
	if _, err := c.Stats(context.Background()); err == nil {
		t.Fatal("缺 secret 必须报错")
	}
	if got.snapshot().Method != "" {
		t.Fatal("密钥没配就不该发出请求")
	}
}

// ---------------------------------------------------------------- Pay

func TestPaySendsSignedRequestAndParsesResult(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{
		"order_no": "NO-1",
		"amount": 19900,
		"yuan": "199.00",
		"notify_url": "https://pay.example.com/notify",
		"session": {"client_params": {"wechat": {"app_id": "wx1"}}},
		"usage": ["用微信 SDK 拉起"]
	}`)
	c := newClient(srv)

	res, err := c.Pay(context.Background(), PayRequest{
		Channel:   "wechat",
		TradeType: "app",
		Subject:   "iPhone",
		Amount:    19900,
		AppID:     "app-1",
	})
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	if res.OrderNo != "NO-1" || res.Amount != 19900 || res.Yuan != "199.00" {
		t.Fatalf("返回值解析不对: %+v", res)
	}
	if res.Session == nil || res.Session.ClientParams == nil {
		t.Fatalf("session 没解析出来: %+v", res.Session)
	}
	if res.Session.ClientParams.Wechat == nil || res.Session.ClientParams.Wechat.AppID != "wx1" {
		t.Fatalf("client_params.wechat 没解析出来: %+v", res.Session.ClientParams)
	}
	if len(res.Usage) != 1 || res.Usage[0] != "用微信 SDK 拉起" {
		t.Fatalf("usage 没解析出来: %+v", res.Usage)
	}

	if got.snapshot().Method != http.MethodPost || got.snapshot().Path != "/api/open/pay" {
		t.Fatalf("请求路径不对: %s %s", got.snapshot().Method, got.snapshot().Path)
	}
	// 请求体里必须带上 app_id —— 这是「订单归属哪个应用」的唯一来源。
	var sent map[string]any
	if err := json.Unmarshal(got.snapshot().Body, &sent); err != nil {
		t.Fatalf("请求体不是合法 JSON: %v (%s)", err, got.snapshot().Body)
	}
	if sent["app_id"] != "app-1" {
		t.Fatalf("app_id 没带上: %v", sent)
	}
	if sent["amount"] != float64(19900) {
		t.Fatalf("金额不对: %v", sent["amount"])
	}
	// 空字段不该出现(omitempty),避免平台把空串当成有效值。
	if _, ok := sent["payer_id"]; ok {
		t.Fatalf("空字段不该被序列化: %v", sent)
	}
}

func TestPaySignatureHeadersPresent(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"order_no":"NO-1"}`)
	c := newClient(srv)
	if _, err := c.Pay(context.Background(), PayRequest{Amount: 1}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	for _, h := range []string{"X-Pay-Key", "X-Pay-Timestamp", "X-Pay-Nonce", "X-Pay-Signature"} {
		if got.snapshot().Header.Get(h) == "" {
			t.Fatalf("请求头 %s 缺失", h)
		}
	}
	if got.snapshot().Header.Get("X-Pay-Key") != "pk_test" {
		t.Fatalf("key 头不对: %s", got.snapshot().Header.Get("X-Pay-Key"))
	}
	if got.snapshot().Header.Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type 不对: %s", got.snapshot().Header.Get("Content-Type"))
	}
	// 签名必须能验签通过(用同一个算法重算一遍)。
	ts := got.snapshot().Header.Get("X-Pay-Timestamp")
	nonce := got.snapshot().Header.Get("X-Pay-Nonce")
	want := Sign("sk_test", http.MethodPost, "/api/open/pay", ts, nonce, got.snapshot().Body)
	if got := got.snapshot().Header.Get("X-Pay-Signature"); got != want {
		t.Fatalf("签名不对:\n实际 %s\n应为 %s", got, want)
	}
}

func TestPayNonceIsUniquePerRequest(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := newClient(srv)
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		if _, err := c.Pay(context.Background(), PayRequest{Amount: 1}); err != nil {
			t.Fatalf("下单失败: %v", err)
		}
		n := got.snapshot().Header.Get("X-Pay-Nonce")
		if seen[n] {
			t.Fatalf("随机串重复了(会被平台当重放拒绝): %s", n)
		}
		seen[n] = true
	}
}

// ---------------------------------------------------------------- Query / Order / Close

func TestQueryReturnsStatus(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"status":"SUCCESS","transaction_id":"tx-1","channel":"wechat"}`)
	c := newClient(srv)
	res, err := c.Query(context.Background(), QueryRequest{Channel: "wechat", OutTradeNo: "NO-1"})
	if err != nil {
		t.Fatalf("查单失败: %v", err)
	}
	if res.Status != "SUCCESS" || res.TransactionID != "tx-1" || res.Channel != "wechat" {
		t.Fatalf("查单结果不对: %+v", res)
	}
	if got.snapshot().Path != "/api/open/query" {
		t.Fatalf("路径不对: %s", got.snapshot().Path)
	}
}

func TestOrderReturnsRawMap(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"out_trade_no":"NO-1","status":"PENDING"}`)
	c := newClient(srv)
	res, err := c.Order(context.Background(), "NO-1")
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if res["status"] != "PENDING" {
		t.Fatalf("结果不对: %+v", res)
	}
	if got.snapshot().Path != "/api/open/order" {
		t.Fatalf("路径不对: %s", got.snapshot().Path)
	}
	var sent map[string]string
	_ = json.Unmarshal(got.snapshot().Body, &sent)
	if sent["out_trade_no"] != "NO-1" {
		t.Fatalf("订单号没带上: %+v", sent)
	}
}

func TestCloseSendsChannelAndOrderNo(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := newClient(srv)
	if err := c.Close(context.Background(), "wechat", "NO-1"); err != nil {
		t.Fatalf("关单失败: %v", err)
	}
	if got.snapshot().Path != "/api/open/close" {
		t.Fatalf("路径不对: %s", got.snapshot().Path)
	}
	var sent map[string]string
	_ = json.Unmarshal(got.snapshot().Body, &sent)
	if sent["channel"] != "wechat" || sent["out_trade_no"] != "NO-1" {
		t.Fatalf("请求体不对: %+v", sent)
	}
}

// ---------------------------------------------------------------- Refund / Wallet

func TestRefundReturnsAmounts(t *testing.T) {
	srv, _ := newServer(t, http.StatusOK, `{
		"result": {"channel":"wechat","out_trade_no":"NO-1","out_refund_no":"R-1","status":"SUCCESS"},
		"refund_amount": 5000,
		"refunded_total": 5000,
		"order_status": "PARTIALLY_REFUNDED",
		"idempotent": false
	}`)
	c := newClient(srv)
	res, err := c.Refund(context.Background(), RefundRequest{
		OutTradeNo:   "NO-1",
		OutRefundNo:  "R-1",
		RefundAmount: 5000,
	})
	if err != nil {
		t.Fatalf("退款失败: %v", err)
	}
	if res.RefundAmount != 5000 || res.RefundedTotal != 5000 {
		t.Fatalf("金额不对: %+v", res)
	}
	if res.OrderStatus != "PARTIALLY_REFUNDED" {
		t.Fatalf("订单状态不对: %+v", res)
	}
	if res.Result.OutRefundNo != "R-1" || res.Result.Status != "SUCCESS" {
		t.Fatalf("退款结果不对: %+v", res.Result)
	}
	if res.Idempotent {
		t.Fatal("第一次退款不该是幂等命中")
	}
}

func TestRefundOmitsZeroAmount(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := newClient(srv)
	// RefundAmount = 0 表示全额,不该把 0 发出去(omitempty),让平台按订单金额算。
	if _, err := c.Refund(context.Background(), RefundRequest{
		OutTradeNo:  "NO-1",
		OutRefundNo: "R-1",
	}); err != nil {
		t.Fatalf("退款失败: %v", err)
	}
	var sent map[string]any
	_ = json.Unmarshal(got.snapshot().Body, &sent)
	if _, ok := sent["refund_amount"]; ok {
		t.Fatalf("refund_amount=0 不该出现在请求体里: %v", sent)
	}
	if sent["out_refund_no"] != "R-1" {
		t.Fatalf("幂等键没带上: %v", sent)
	}
}

func TestWalletChargeReturnsStatus(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"status":"SUCCESS","transaction_id":"tx-9","channel":"applepay"}`)
	c := newClient(srv)
	res, err := c.WalletCharge(context.Background(), WalletRequest{
		Channel:      "applepay",
		OutTradeNo:   "NO-1",
		PaymentToken: "tok",
	})
	if err != nil {
		t.Fatalf("钱包扣款失败: %v", err)
	}
	if res.Status != "SUCCESS" || res.TransactionID != "tx-9" {
		t.Fatalf("结果不对: %+v", res)
	}
	if got.snapshot().Path != "/api/open/wallet/charge" {
		t.Fatalf("路径不对: %s", got.snapshot().Path)
	}
}

// ---------------------------------------------------------------- Orders / Stats

func TestOrdersOnlySendsNonEmptyFilters(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"orders":[],"page":2,"size":20,"total":0,"pages":0}`)
	c := newClient(srv)
	res, err := c.Orders(context.Background(), OrdersQuery{Page: 2, Size: 20, AppID: "app-1"})
	if err != nil {
		t.Fatalf("订单列表失败: %v", err)
	}
	if res.Page != 2 || res.Size != 20 {
		t.Fatalf("分页没回显: %+v", res)
	}
	if got.snapshot().Method != http.MethodGet {
		t.Fatalf("订单列表应该用 GET: %s", got.snapshot().Method)
	}
	if got.snapshot().Query.Get("app_id") != "app-1" {
		t.Fatalf("app_id 没带上: %s", got.snapshot().RawURL)
	}
	if got.snapshot().Query.Get("page") != "2" || got.snapshot().Query.Get("size") != "20" {
		t.Fatalf("分页参数不对: %s", got.snapshot().RawURL)
	}
	// 没填的筛选条件不该出现(否则平台会收到 status= 这样的空值)。
	for _, k := range []string{"status", "channel", "keyword", "from", "to"} {
		if _, ok := got.snapshot().Query[k]; ok {
			t.Fatalf("空条件 %s 不该出现在查询串里: %s", k, got.snapshot().RawURL)
		}
	}
}

func TestOrdersSignatureCoversQueryString(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{}`)
	c := newClient(srv)
	if _, err := c.Orders(context.Background(), OrdersQuery{Page: 1, Size: 20, Keyword: "订单"}); err != nil {
		t.Fatalf("订单列表失败: %v", err)
	}
	ts := got.snapshot().Header.Get("X-Pay-Timestamp")
	nonce := got.snapshot().Header.Get("X-Pay-Nonce")
	// 签名用的 target 必须和平台端算的一样:路径 + "?" + 原始查询串。
	target := got.snapshot().Path
	if got.snapshot().RawURL != got.snapshot().Path {
		target = got.snapshot().RawURL
	}
	want := Sign("sk_test", http.MethodGet, target, ts, nonce, nil)
	if got := got.snapshot().Header.Get("X-Pay-Signature"); got != want {
		t.Fatalf("查询串没参与签名:\n实际 %s\n应为 %s", got, want)
	}
}

func TestStatsParsesCounters(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{
		"order_count": 10, "success_count": 8, "failed_count": 2, "refund_count": 1,
		"total_amount": 199000, "refund_amount": 5000, "net_amount": 194000,
		"today_count": 3, "today_amount": 30000, "success_rate": 0.8
	}`)
	c := newClient(srv)
	res, err := c.Stats(context.Background())
	if err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if res.OrderCount != 10 || res.SuccessCount != 8 || res.NetAmount != 194000 {
		t.Fatalf("统计不对: %+v", res)
	}
	if res.SuccessRate != 0.8 {
		t.Fatalf("成功率不对: %+v", res)
	}
	if got.snapshot().Path != "/api/open/stats" {
		t.Fatalf("路径不对: %s", got.snapshot().Path)
	}
}

func TestStatsGroupedParsesGroupsAndTruncated(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{
		"stats": {"order_count": 5},
		"group_by": "day",
		"groups": [{"key":"2026-01-01","label":"2026-01-01","order_count":5,"total_amount":1000,"net_amount":1000}],
		"truncated": true
	}`)
	c := newClient(srv)
	res, err := c.StatsGrouped(context.Background(), StatsQuery{GroupBy: "day"})
	if err != nil {
		t.Fatalf("分组统计失败: %v", err)
	}
	if res.GroupBy != "day" || len(res.Groups) != 1 {
		t.Fatalf("分组不对: %+v", res)
	}
	if res.Groups[0].Key != "2026-01-01" || res.Groups[0].TotalAmount != 1000 {
		t.Fatalf("分组明细不对: %+v", res.Groups[0])
	}
	// truncated 必须原样透传给调用方,否则调用方会把截断结果当全量。
	if !res.Truncated {
		t.Fatal("truncated 标志丢了")
	}
	if got.snapshot().Query.Get("group_by") != "day" {
		t.Fatalf("group_by 没带上: %s", got.snapshot().RawURL)
	}
}

func TestStatsGroupedWithoutParams(t *testing.T) {
	srv, got := newServer(t, http.StatusOK, `{"stats":{"order_count":0}}`)
	c := newClient(srv)
	if _, err := c.StatsGrouped(context.Background(), StatsQuery{}); err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if got.snapshot().RawURL != "/api/open/stats" {
		t.Fatalf("没填条件时不该带查询串: %s", got.snapshot().RawURL)
	}
}

// ---------------------------------------------------------------- 错误处理

func TestAPIErrorOnNon2xx(t *testing.T) {
	srv, _ := newServer(t, http.StatusForbidden, `{"error":"签名不正确"}`)
	c := newClient(srv)
	_, err := c.Stats(context.Background())
	if err == nil {
		t.Fatal("非 2xx 必须报错")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("错误类型不对: %T", err)
	}
	if apiErr.Status != http.StatusForbidden {
		t.Fatalf("状态码不对: %d", apiErr.Status)
	}
	if apiErr.Body != `{"error":"签名不正确"}` {
		t.Fatalf("响应体不对: %q", apiErr.Body)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("错误信息里应带状态码: %v", err)
	}
}

func TestAPIErrorWithoutBody(t *testing.T) {
	srv, _ := newServer(t, http.StatusInternalServerError, "")
	c := newClient(srv)
	_, err := c.Stats(context.Background())
	if err == nil {
		t.Fatal("非 2xx 必须报错")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("错误信息里应带状态码: %v", err)
	}
}

func TestAPIErrorErrorMethod(t *testing.T) {
	e := &APIError{Status: 404}
	if e.Error() != "openapi: 平台返回 HTTP 404" {
		t.Fatalf("错误文案不对: %s", e.Error())
	}
	e2 := &APIError{Status: 400, Body: "坏参数"}
	if e2.Error() != "openapi: 平台返回 HTTP 400: 坏参数" {
		t.Fatalf("错误文案不对: %s", e2.Error())
	}
}

func TestBadJSONResponseIsReported(t *testing.T) {
	srv, _ := newServer(t, http.StatusOK, `not-json`)
	c := newClient(srv)
	_, err := c.Stats(context.Background())
	if err == nil {
		t.Fatal("响应不是合法 JSON 必须报错")
	}
	if !strings.Contains(err.Error(), "解析响应") {
		t.Fatalf("错误信息不对: %v", err)
	}
	// 原始内容要带在错误里,方便排查。
	if !strings.Contains(err.Error(), "not-json") {
		t.Fatalf("错误信息里应带原始内容: %v", err)
	}
}

func TestServerUnreachableIsReported(t *testing.T) {
	c := New("http://127.0.0.1:1", "pk_test", "sk_test")
	_, err := c.Stats(context.Background())
	if err == nil {
		t.Fatal("连不上必须报错")
	}
	if !strings.Contains(err.Error(), "请求失败") {
		t.Fatalf("错误信息不对: %v", err)
	}
}

func TestContextCancellationIsReported(t *testing.T) {
	srv, _ := newServer(t, http.StatusOK, `{}`)
	c := newClient(srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Stats(ctx); err == nil {
		t.Fatal("取消的 context 必须报错")
	}
}

// ---------------------------------------------------------------- Sign

func TestSignIsStable(t *testing.T) {
	got := Sign("sk", "POST", "/api/open/pay", "1700000000", "n1", []byte(`{"a":1}`))
	want := Sign("sk", "POST", "/api/open/pay", "1700000000", "n1", []byte(`{"a":1}`))
	if got != want {
		t.Fatalf("同样的输入必须得到同样的签名:\n%s\n%s", got, want)
	}
	if len(got) != 64 {
		t.Fatalf("签名应该是 32 字节的 hex(64 字符),实际 %d: %s", len(got), got)
	}
	if strings.ToLower(got) != got {
		t.Fatalf("签名必须是小写 hex: %s", got)
	}
}

func TestSignDistinguishesEveryInput(t *testing.T) {
	base := Sign("sk", "POST", "/api/open/pay", "1700000000", "n1", []byte(`{"a":1}`))
	variants := map[string]string{
		"方法不同":  Sign("sk", "GET", "/api/open/pay", "1700000000", "n1", []byte(`{"a":1}`)),
		"路径不同":  Sign("sk", "POST", "/api/open/query", "1700000000", "n1", []byte(`{"a":1}`)),
		"查询串不同": Sign("sk", "POST", "/api/open/pay?page=2", "1700000000", "n1", []byte(`{"a":1}`)),
		"时间戳不同": Sign("sk", "POST", "/api/open/pay", "1700000001", "n1", []byte(`{"a":1}`)),
		"随机串不同": Sign("sk", "POST", "/api/open/pay", "1700000000", "n2", []byte(`{"a":1}`)),
		"请求体不同": Sign("sk", "POST", "/api/open/pay", "1700000000", "n1", []byte(`{"a":2}`)),
		"密钥不同":  Sign("sk2", "POST", "/api/open/pay", "1700000000", "n1", []byte(`{"a":1}`)),
	}
	for name, v := range variants {
		if v == base {
			t.Fatalf("%s 时签名没变,说明它没参与签名", name)
		}
	}
}

func TestSignUppercasesMethod(t *testing.T) {
	// 平台端用 strings.ToUpper(method),小写方法名也要能对上。
	if Sign("sk", "post", "/x", "1", "n", nil) != Sign("sk", "POST", "/x", "1", "n", nil) {
		t.Fatal("方法名大小写不该影响签名")
	}
}

func TestSignEmptyBody(t *testing.T) {
	// 空 body 与 nil body 必须等价,否则 GET 请求会验签失败。
	if Sign("sk", "GET", "/x", "1", "n", nil) != Sign("sk", "GET", "/x", "1", "n", []byte{}) {
		t.Fatal("nil body 和空 body 的签名应该一样")
	}
}

// ---------------------------------------------------------------- randomNonce

func TestRandomNonceLengthAndUniqueness(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		n, err := randomNonce()
		if err != nil {
			t.Fatalf("生成随机串失败: %v", err)
		}
		if len(n) != 32 {
			t.Fatalf("随机串应该是 16 字节的 hex(32 字符),实际 %d: %s", len(n), n)
		}
		if seen[n] {
			t.Fatalf("随机串重复了: %s", n)
		}
		seen[n] = true
	}
}
