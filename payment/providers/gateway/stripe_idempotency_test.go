package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// decodeQueryValue 从 URL query 里取一个参数。
func decodeQueryValue(rawQuery, key string) (string, error) {
	v, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", err
	}
	return v.Get(key), nil
}

// TestStripeChargeUsesStableIdempotencyKey 建 intent 的请求必须带幂等键。
//
// 官方(https://docs.stripe.com/api/idempotent_requests):
// 「创建或更新对象时带上幂等键,连接出错时可以安全重试,不会创建出第二个对象」;
// 幂等键放 Idempotency-Key 头,API v1 至少保留 24 小时。
//
// 没有幂等键的后果:商户超时重发一次,用户就被扣两次钱。
func TestStripeChargeUsesStableIdempotencyKey(t *testing.T) {
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/payment_intents" {
			keys = append(keys, r.Header.Get("Idempotency-Key"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_1","status":"succeeded","amount":19900,"currency":"cny"}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   make(map[string]string),
	}
	for i := 0; i < 2; i++ {
		if _, err := s.Charge(context.Background(), ChargeRequest{
			OutTradeNo:   "ORDER-1",
			Amount:       19900,
			Currency:     "CNY",
			PaymentToken: "pm_tok",
		}); err != nil {
			t.Fatalf("Charge 失败: %v", err)
		}
	}
	if len(keys) != 1 {
		t.Fatalf("重试扣款不该再建第二个 PaymentIntent,实际建了 %d 个", len(keys))
	}
	if keys[0] == "" {
		t.Fatal("扣款请求没有带 Idempotency-Key 头")
	}
	if keys[0] != idempotencyKey("charge", "ORDER-1") {
		t.Fatalf("幂等键不对: %q", keys[0])
	}
}

// TestStripeChargeConfirmsExistingIntent 扣款必须确认**建会话时那一个** intent。
//
// 官方流程(https://docs.stripe.com/apple-pay):amount 与 currency 在建 intent
// 时就定死了;客户端拿到的 client_secret 自己就能确认那个 intent。
// 服务端要是再新建一个 intent 去扣,客户端也确认过的话,用户就被扣两次钱。
func TestStripeChargeConfirmsExistingIntent(t *testing.T) {
	var (
		created []string
		confirm []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/payment_intents" && r.Method == http.MethodPost:
			created = append(created, r.URL.Path)
			_, _ = w.Write([]byte(`{"id":"pi_1","client_secret":"cs_1","status":"requires_payment_method","amount":19900,"currency":"usd"}`))
		case r.URL.Path == "/v1/payment_intents/pi_1" && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"id":"pi_1","status":"requires_payment_method","amount":19900,"currency":"usd"}`))
		case r.URL.Path == "/v1/payment_intents/pi_1/confirm":
			confirm = append(confirm, r.Header.Get("Idempotency-Key"))
			_, _ = w.Write([]byte(`{"id":"pi_1","status":"succeeded","amount":19900,"currency":"usd","payment_method":"pm_1"}`))
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   make(map[string]string),
	}
	// 建会话(客户端拿 client_secret 去拉起收银台)。
	if _, err := s.CreateSession(context.Background(), SessionRequest{
		OutTradeNo: "ORDER-1",
		Amount:     19900,
		Currency:   "USD",
	}); err != nil {
		t.Fatalf("CreateSession 失败: %v", err)
	}
	// 建会话本身就会建一个 intent,那是应该的;下面看的是**扣款时**有没有再建。
	created = nil
	// 服务端扣款。
	res, err := s.Charge(context.Background(), ChargeRequest{
		OutTradeNo:   "ORDER-1",
		Amount:       19900,
		Currency:     "USD",
		PaymentToken: "pm_1",
	})
	if err != nil {
		t.Fatalf("Charge 失败: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("扣款时不该再建 PaymentIntent(会多出一笔待付单): %v", created)
	}
	if len(confirm) != 1 {
		t.Fatalf("应该确认一次已有的 intent,实际 %d 次", len(confirm))
	}
	if confirm[0] == "" {
		t.Fatal("确认请求没有带 Idempotency-Key 头")
	}
	if !res.Paid {
		t.Fatalf("扣款结果没标记成已支付: %+v", res)
	}
}

// TestStripeChargeDoesNotChargeTwiceWhenAlreadySucceeded 已经扣成功的单不能再扣一次。
//
// 客户端可能已经拿 client_secret 自己确认过了(官方移动端流程就是这么做的),
// 服务端这时再扣一次就是重复扣款。
func TestStripeChargeDoesNotChargeTwiceWhenAlreadySucceeded(t *testing.T) {
	var posts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posts++
		}
		_, _ = w.Write([]byte(`{"id":"pi_1","status":"succeeded","amount":19900,"currency":"cny","metadata":{"out_trade_no":"ORDER-1"}}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   map[string]string{"ORDER-1": "pi_1"},
	}
	res, err := s.Charge(context.Background(), ChargeRequest{
		OutTradeNo:   "ORDER-1",
		Amount:       19900,
		Currency:     "CNY",
		PaymentToken: "pm_tok",
	})
	if err != nil {
		t.Fatalf("Charge 失败: %v", err)
	}
	if posts != 0 {
		t.Fatalf("已经成功的单不该再发扣款请求,实际发了 %d 次", posts)
	}
	if !res.Paid {
		t.Fatalf("结果没标记成已支付: %+v", res)
	}
	if res.OutTradeNo != "ORDER-1" {
		t.Fatalf("订单号不对: %q", res.OutTradeNo)
	}
}

// TestStripeChargeRecreatesCanceledIntent 之前的 intent 作废了(没扣过钱)时,
// 重新建一个 —— 作废的 intent 不能确认。
func TestStripeChargeRecreatesCanceledIntent(t *testing.T) {
	var creates int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/payment_intents" && r.Method == http.MethodPost:
			creates++
			_, _ = w.Write([]byte(`{"id":"pi_2","status":"succeeded","amount":19900,"currency":"cny"}`))
		case r.URL.Path == "/v1/payment_intents/pi_1":
			_, _ = w.Write([]byte(`{"id":"pi_1","status":"canceled","amount":19900,"currency":"cny"}`))
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   map[string]string{"ORDER-1": "pi_1"},
	}
	res, err := s.Charge(context.Background(), ChargeRequest{
		OutTradeNo:   "ORDER-1",
		Amount:       19900,
		Currency:     "CNY",
		PaymentToken: "pm_tok",
	})
	if err != nil {
		t.Fatalf("Charge 失败: %v", err)
	}
	if creates != 1 {
		t.Fatalf("作废的 intent 应该重新建一个,实际建了 %d 个", creates)
	}
	if res.Reference != "pi_2" {
		t.Fatalf("应该用新建的 intent: %q", res.Reference)
	}
}

// TestStripeIdempotencyKeyHidesOrderNo 幂等键里不能出现原始订单号。
//
// 官方建议别把敏感信息放进幂等键;订单号对商户是业务标识。
func TestStripeIdempotencyKeyHidesOrderNo(t *testing.T) {
	key := idempotencyKey("charge", "ORDER-SECRET-1")
	if strings.Contains(key, "ORDER-SECRET-1") {
		t.Fatalf("幂等键里不该出现原始订单号: %q", key)
	}
	if len(key) > 255 {
		t.Fatalf("幂等键最长 255 字符,实际 %d", len(key))
	}
	if idempotencyKey("charge", "A") == idempotencyKey("session", "A") {
		t.Fatal("不同操作必须得到不同的幂等键")
	}
}

// TestStripeSearchUsesQueryString search 的参数必须放 query string。
//
// 官方文档里 search 的例子就是 curl -G(参数进 URL 而不是 body):
// GET https://api.stripe.com/v1/payment_intents/search?query=...
// 参数放请求体的话 Stripe 直接忽略 → 按商户订单号查单永远「订单不存在」。
func TestStripeSearchUsesQueryString(t *testing.T) {
	var (
		gotQuery string
		gotBody  string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   make(map[string]string),
	}
	if _, err := s.searchByTradeNo(context.Background(), "ORDER-1"); err != nil {
		t.Fatalf("searchByTradeNo 失败: %v", err)
	}
	if gotQuery == "" {
		t.Fatalf("search 请求的 URL query 为空: body=%q", gotBody)
	}
	if gotBody != "" {
		t.Fatalf("search 请求把参数放进了 GET 请求体: %q", gotBody)
	}
	if !strings.Contains(gotQuery, "query=") {
		t.Fatalf("search 的 query 参数不对: %q", gotQuery)
	}
}

// TestStripeQueryByPaymentIntentID reference 是 PaymentIntent ID 时直接查它。
func TestStripeQueryByPaymentIntentID(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_123","status":"succeeded","amount":19900,"currency":"cny","metadata":{"out_trade_no":"ORDER-1"}}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   make(map[string]string),
	}
	res, err := s.Query(context.Background(), "pi_123")
	if err != nil {
		t.Fatalf("用 PaymentIntent ID 查单失败: %v", err)
	}
	if res.Reference != "pi_123" {
		t.Fatalf("查到的 PaymentIntent 不对: %q", res.Reference)
	}
	if len(paths) != 1 || paths[0] != "/v1/payment_intents/pi_123" {
		t.Fatalf("请求路径不对: %v", paths)
	}
}

// TestStripeSearchEscapesValue 订单号里的引号必须转义。
//
// Stripe 的查询语法里单引号是特殊字符,不转义的话一个带引号的订单号
// 能把查询语义带偏,搜出别人的单子。
func TestStripeSearchEscapesValue(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	s := &Stripe{
		cfg:    &StripeConfig{SecretKey: "sk_test_x", APIBase: srv.URL},
		client: srv.Client(),
		refs:   make(map[string]string),
	}
	if _, err := s.searchByTradeNo(context.Background(), "x' or '1'='1"); err != nil {
		t.Fatalf("searchByTradeNo 失败: %v", err)
	}
	// 解码后应当还是原样的订单号,引号被转义,没有多出引号。
	decoded, err := decodeQueryValue(gotQuery, "query")
	if err != nil {
		t.Fatalf("解析 query 失败: %v", err)
	}
	if !strings.Contains(decoded, `\'`) {
		t.Fatalf("订单号里的引号没有转义: %q", decoded)
	}
}
