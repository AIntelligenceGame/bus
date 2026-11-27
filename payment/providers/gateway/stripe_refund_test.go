package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// 这组测试是「代码复查」那一轮对 Stripe 渠道的回归用例。
//
// 关键背景(官方文档):
//   - 退款接口 POST /v1/refunds **必须**给出 payment_intent 或 charge 之一,
//     两个都不给会报 InvalidRequestError:
//     "One of the following params should be provided for this request:
//      payment_intent or charge"。
//     metadata 只是备注,Stripe 不拿它定位订单。
//   - 查单 GET /v1/payment_intents/search?query=metadata['out_trade_no']:'<单号>'
//     是「按商户订单号找回 PaymentIntent」的官方途径。
//   - 幂等键只在**同一个操作**内有效:建会话和扣款用的是不同的幂等键,
//     Stripe 挡不住「服务端另建一个 intent 去扣」造成的重复扣款。

// TestStripeRefundSendsPaymentIntent 退款请求必须带上 payment_intent。
func TestStripeRefundSendsPaymentIntent(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/refunds" {
			_ = r.ParseForm()
			got = r.PostForm
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"re_1","amount":1000,"status":"succeeded"}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   refsWith(map[string]string{"ORDER-1": "pi_1"}),
	}
	if _, err := s.Refund(context.Background(), RefundRequest{
		OutTradeNo: "ORDER-1",
		RefundID:   "R-1",
		Amount:     1000,
	}); err != nil {
		t.Fatalf("Refund 失败: %v", err)
	}
	if got.Get("payment_intent") != "pi_1" {
		t.Fatalf("退款请求没有带 payment_intent: %v", got)
	}
}

// TestStripeRefundFallsBackToSearch ���存被淘汰(进程重启 / 多实例)之后,
// 退款仍要能通过 Search API 找回 PaymentIntent。
//
// 以前退款**只查本地缓存**:进程一重启、或条目被 LRU 淘汰,
// payment_intent 就发不出去,Stripe 直接拒单 —— 商户看着「订单已支付」却退不了款。
func TestStripeRefundFallsBackToSearch(t *testing.T) {
	var (
		searched bool
		got      url.Values
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/payment_intents/search":
			searched = true
			_, _ = w.Write([]byte(`{"data":[{"id":"pi_found","status":"succeeded","amount":5000,"currency":"cny","metadata":{"out_trade_no":"ORDER-2"}}]}`))
		case "/v1/refunds":
			_ = r.ParseForm()
			got = r.PostForm
			_, _ = w.Write([]byte(`{"id":"re_1","amount":1000,"status":"succeeded"}`))
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer srv.Close()

	// 缓存是空的(模拟进程重启后本地映射丢失)。
	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   newStripeRefs(),
	}
	if _, err := s.Refund(context.Background(), RefundRequest{
		OutTradeNo: "ORDER-2",
		RefundID:   "R-1",
		Amount:     1000,
	}); err != nil {
		t.Fatalf("Refund 失败: %v", err)
	}
	if !searched {
		t.Fatal("本地缓存没有命中时应该退回 Search API")
	}
	if got.Get("payment_intent") != "pi_found" {
		t.Fatalf("退款请求没有带 search 找到的 payment_intent: %v", got)
	}
}

// TestStripeRefundFailsWhenOrderNotFound Stripe 上确实没有这笔单时,
// 必须明确报错(而不是发一个缺 payment_intent 的请求让 Stripe 拒)。
func TestStripeRefundFailsWhenOrderNotFound(t *testing.T) {
	var refunds int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/refunds" {
			refunds++
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   newStripeRefs(),
	}
	if _, err := s.Refund(context.Background(), RefundRequest{
		OutTradeNo: "ORDER-MISSING",
		RefundID:   "R-1",
		Amount:     1000,
	}); err == nil {
		t.Fatal("订单不存在时应该报错")
	}
	if refunds != 0 {
		t.Fatalf("订单不存在时不该发出退款请求,实际发了 %d 次", refunds)
	}
}

// TestStripeChargeDoesNotCreateSecondIntentWhenLookupFails 查询失败时
// **绝不能**当成「没建过会话」去新建一个 intent。
//
// 建会话和扣款用的是**不同**的幂等键,Stripe 的幂等层挡不住第二次创建 ——
// 客户端拿的是建会话时那个 intent 的 client_secret,服务端另建一个去扣,
// 用户就被扣了两次钱。
func TestStripeChargeDoesNotCreateSecondIntentWhenLookupFails(t *testing.T) {
	var creates int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/payment_intents" && r.Method == http.MethodPost {
			creates++
		}
		if r.URL.Path == "/v1/payment_intents/search" {
			// 模拟查询挂了(网络/鉴权错误)。
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"bad key"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"pi_1","status":"succeeded","amount":19900,"currency":"cny"}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   newStripeRefs(),
	}
	if _, err := s.Charge(context.Background(), ChargeRequest{
		OutTradeNo:   "ORDER-1",
		Amount:       19900,
		Currency:     "CNY",
		PaymentToken: "pm_tok",
	}); err == nil {
		t.Fatal("查询失败时必须报错,不能当成「订单不存在」")
	}
	if creates != 0 {
		t.Fatalf("查询失败时不该新建 PaymentIntent,实际建了 %d 个", creates)
	}
}

// TestStripeRefsCacheIsBounded 「订单号 -> PaymentIntent」映射必须有上限。
//
// 这张表只增不减:长期运行、订单量大的进程会把它撑到内存里放不下。
// 淘汰掉不要紧 —— 查不到时会退回 Stripe 的 Search API,结果一样。
func TestStripeRefsCacheIsBounded(t *testing.T) {
	c := newStripeRefs()
	for i := 0; i < stripeRefCacheSize+500; i++ {
		c.Add("ORDER-"+itoa(i), "pi_"+itoa(i))
	}
	if got := c.Len(); got > stripeRefCacheSize {
		t.Fatalf("订单号映射缓存超过了上限: %d > %d", got, stripeRefCacheSize)
	}
}

// itoa 把整数转成字符串(测试里用,免得为一处引用 strconv)。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
