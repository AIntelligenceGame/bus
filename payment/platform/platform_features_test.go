package platform

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// targetRecordingSender 记录验证码的接收地址(测试用)。
type targetRecordingSender struct {
	targets []string
	codes   []string
}

func (s *targetRecordingSender) Send(_ context.Context, _, target, code string) error {
	s.targets = append(s.targets, target)
	s.codes = append(s.codes, code)
	return nil
}

// amountRecorder 记录每次退款的金额(测试用的假渠道)。
type amountRecorder struct {
	amounts []int64
}

func (f *amountRecorder) Channel() payment.Channel { return payment.ChannelWechat }

func (f *amountRecorder) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeNative}
}

func (f *amountRecorder) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	return &payment.PaymentSession{Channel: payment.ChannelWechat, QRCode: "weixin://pay?x=1"}, nil
}

func (f *amountRecorder) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	return &payment.TradeResult{Channel: payment.ChannelWechat}, nil
}

func (f *amountRecorder) Close(ctx context.Context, outTradeNo string) error { return nil }

func (f *amountRecorder) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	f.amounts = append(f.amounts, req.RefundAmount)
	return &payment.RefundResult{
		Channel:      payment.ChannelWechat,
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  req.OutRefundNo,
		RefundAmount: req.RefundAmount,
		Status:       string(payment.TradeStatusSuccess),
	}, nil
}

func (f *amountRecorder) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	return &payment.Notify{Channel: payment.ChannelWechat}, nil
}

// newFeaturePlatform 造一个带假渠道的测试平台。
func newFeaturePlatform(t *testing.T) (*Platform, *Store, *targetRecordingSender, *amountRecorder) {
	t.Helper()
	store := newTestStore(t)
	sender := &targetRecordingSender{}
	recorder := &amountRecorder{}
	p := &Platform{
		opts: Options{
			Logger:            log.New(io.Discard, "", 0),
			VerifyTTL:         10 * time.Minute,
			MFASender:         sender,
			PublicBaseURL:     "https://pay.example.com",
			SessionTTL:        time.Hour,
			TrustForwardedFor: false,
		},
		store:        store,
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
	}
	return p, store, sender, recorder
}

// TestMFACodeCannotBeRedirected 二次验证验证码不能被改发到别的地址。
//
// 否则攻击者只要知道口令,就能把验证码发到自己邮箱完成二次验证。
func TestMFACodeCannotBeRedirected(t *testing.T) {
	p, store, sender, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户甲")
	if err := store.SetMFA(user.UserID, MFATypeEmail, "owner@example.com", true); err != nil {
		t.Fatalf("设置二次验证失败: %v", err)
	}

	// 指定别的接收地址 → 必须拒绝。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/account/mfa/code",
		strings.NewReader(`{"type":"email","target":"attacker@evil.com","purpose":"sensitive"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleSendMFACode(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("改发验证码到别的地址应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(sender.targets) != 0 {
		t.Fatalf("被拒绝的请求仍然下发了验证码: %v", sender.targets)
	}

	// 不指定地址 → 发到已绑定的地址。
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/account/mfa/code",
		strings.NewReader(`{"purpose":"sensitive"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleSendMFACode(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("发验证码应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(sender.targets) != 1 || sender.targets[0] != "owner@example.com" {
		t.Fatalf("验证码没有发到已绑定的地址: %v", sender.targets)
	}
}

// TestMFABindRequiresPassword 绑定新地址必须先验口令。
func TestMFABindRequiresPassword(t *testing.T) {
	p, store, sender, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户乙")
	hash, err := hashPassword("supersecret")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if err := store.SetPassword(user.UserID, hash); err != nil {
		t.Fatalf("设置口令失败: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/account/mfa/code",
		strings.NewReader(`{"type":"email","target":"new@example.com","purpose":"bind","code":"wrong-password"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleSendMFACode(rec, req)
	// 403 而不是 401:401 在这套 API 里表示「会话没了」,前端会清掉令牌
	// 把用户踢回登录页 —— 口令打错一次不该导致登出。
	if rec.Code != http.StatusForbidden {
		t.Fatalf("口令错误时绑定验证码应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(sender.targets) != 0 {
		t.Fatalf("口令错误仍然下发了验证码: %v", sender.targets)
	}
}

// TestLoginCodeOnlyGoesToBoundTarget 登录验证码只发到已绑定的地址,且不泄露账户是否存在。
func TestLoginCodeOnlyGoesToBoundTarget(t *testing.T) {
	p, store, sender, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户丙")
	if err := store.SetMFA(user.UserID, MFATypeEmail, "owner@example.com", true); err != nil {
		t.Fatalf("设置二次验证失败: %v", err)
	}

	// 已开启二次验证的账户:验证码发到绑定地址。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login/code",
		strings.NewReader(`{"name":"商户丙"}`))
	req.Header.Set("Content-Type", "application/json")
	p.handleSendLoginCode(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("请求登录验证码应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(sender.targets) != 1 || sender.targets[0] != "owner@example.com" {
		t.Fatalf("登录验证码没有发到绑定地址: %v", sender.targets)
	}

	// 不存在的账户:响应与上面完全一致(不能用来枚举商户名),但不发验证码。
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/login/code",
		strings.NewReader(`{"name":"查无此户"}`))
	req2.Header.Set("Content-Type", "application/json")
	p.handleSendLoginCode(rec2, req2)
	if rec2.Code != rec.Code {
		t.Fatalf("账户存在与否的响应状态不一致: %d vs %d", rec.Code, rec2.Code)
	}
	body1 := map[string]any{}
	body2 := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &body1)
	_ = json.Unmarshal(rec2.Body.Bytes(), &body2)
	if body1["message"] != body2["message"] {
		t.Fatalf("账户存在与否的提示语不一致: %v vs %v", body1["message"], body2["message"])
	}
	if len(sender.targets) != 1 {
		t.Fatalf("不存在的账户也下发了验证码: %v", sender.targets)
	}
}

// TestLoginCodeIsPublic 登录验证码接口必须免令牌,否则开了二次验证就登不进去。
func TestLoginCodeIsPublic(t *testing.T) {
	p, _, _, _ := newFeaturePlatform(t)
	if !p.isPublicPath("/api/login/code") {
		t.Fatalf("/api/login/code 必须是无需令牌即可访问的公开路径")
	}
}

// TestRefundCannotExceedOrderAmount 退款金额受订单金额与已退累计双重约束,并留下流水。
func TestRefundCannotExceedOrderAmount(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户丁")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "REFUND-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		TradeType:  "native",
		Subject:    "可退订单",
		Amount:     10000,
		AmountYuan: "100.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	refund := func(amount int64) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		body, _ := json.Marshal(map[string]any{
			"channel":       "wechat",
			"out_trade_no":  "REFUND-1",
			"refund_amount": amount,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/refund", strings.NewReader(string(body)))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("X-Verify-Token", verifyToken)
		req.Header.Set("Content-Type", "application/json")
		p.handleRefund(rec, req)
		return rec
	}

	// 超过订单金额 → 拒绝。
	if rec := refund(20000); rec.Code != http.StatusBadRequest {
		t.Fatalf("退款超过订单金额应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(recorder.amounts) != 0 {
		t.Fatalf("被拒绝的退款竟然调用了渠道: %v", recorder.amounts)
	}

	// 部分退款 30 元 → 成功,订单变成「部分退款」。
	rec := refund(3000)
	if rec.Code != http.StatusOK {
		t.Fatalf("部分退款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["order_status"] != string(payment.TradeStatusPartiallyRefunded) {
		t.Fatalf("部分退款后订单状态应为 PARTIALLY_REFUNDED,实际 %v", out["order_status"])
	}

	// 再退 80 元 → 累计 110 元超过订单金额 → 拒绝。
	if rec := refund(8000); rec.Code != http.StatusBadRequest {
		t.Fatalf("累计退款超过订单金额应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 退款流水只有一条,金额 30 元。
	refunds, err := store.Refunds(user.UserID, 100)
	if err != nil {
		t.Fatalf("查退款流水失败: %v", err)
	}
	if len(refunds) != 1 {
		t.Fatalf("应有 1 条退款流水,实际 %d 条", len(refunds))
	}
	if refunds[0].Amount != 3000 || refunds[0].OutTradeNo != "REFUND-1" {
		t.Fatalf("退款流水内容不对: %+v", refunds[0])
	}
	total, err := store.RefundedAmount(user.UserID, "REFUND-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 3000 {
		t.Fatalf("累计退款金额应为 3000,实际 %d", total)
	}
}

// TestRefundRejectedForUnpaidOrder 未支付成功的订单不能退款。
func TestRefundRejectedForUnpaidOrder(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户戊")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "PENDING-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: "100.00",
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/refund",
		strings.NewReader(`{"channel":"wechat","out_trade_no":"PENDING-1"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleRefund(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("未支付成功的订单退款应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(recorder.amounts) != 0 {
		t.Fatalf("被拒绝的退款竟然调用了渠道: %v", recorder.amounts)
	}
}

// TestCloseRequiresVerifyToken 关单属于资金操作,必须有二次验证票据。
func TestCloseRequiresVerifyToken(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户己")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "CLOSE-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: "100.00",
		Status:     string(payment.TradeStatusPending),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/close",
		strings.NewReader(`{"channel":"wechat","out_trade_no":"CLOSE-1"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleClose(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("没有二次验证票据时关单应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestPayRejectsForeignNotifyURL 商户不能把回调地址指向别处。
func TestPayRejectsForeignNotifyURL(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户庚")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/pay",
		strings.NewReader(`{"channel":"wechat","out_trade_no":"PAY-1","subject":"x","amount":100,"notify_url":"http://evil.example.com/notify"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handlePay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("自定回调地址应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestMerchantCallbackBaseURL 商户可以在控制台里改自己的回调域名(不用重启平台)。
//
// 以前回调域名只能靠启动参数 -base 给,改一次要改命令行、重启一次进程。
// 现在商户能在页面上填自己的域名:存库后立即生效,平台默认地址只当兜底。
func TestMerchantCallbackBaseURL(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户壬")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}

	post := func(body string, token string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/account/notify-base", strings.NewReader(body))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("X-Verify-Token", token)
		}
		p.handleSaveNotifyBase(rec, req)
		return rec
	}

	// 没有二次验证票据 → 拒绝:这设置决定「渠道把支付结果告诉谁」。
	if rec := post(`{"notify_base_url":"https://shop.example.org"}`, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("没有二次验证票据时改回调地址应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}

	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}

	// 非法地址一律拒绝。
	for _, bad := range []string{
		`{"notify_base_url":"ftp://shop.example.org"}`,
		`{"notify_base_url":"shop.example.org"}`,
		`{"notify_base_url":"https://user:pw@shop.example.org"}`,
		`{"notify_base_url":"https://shop.example.org?a=1"}`,
	} {
		if rec := post(bad, verifyToken); rec.Code != http.StatusBadRequest {
			t.Fatalf("非法回调地址 %s 应被拒绝,实际 %d: %s", bad, rec.Code, rec.Body.String())
		}
	}

	// 正常保存(结尾多余的斜杠会被规整掉)。
	if rec := post(`{"notify_base_url":"https://shop.example.org/"}`, verifyToken); rec.Code != http.StatusOK {
		t.Fatalf("保存回调地址失败: %d %s", rec.Code, rec.Body.String())
	}
	want := "https://shop.example.org/notify/wechat/" + user.UserID
	if got := p.NotifyURL(payment.ChannelWechat, user.UserID); got != want {
		t.Fatalf("回调地址应换成商户自己的域名,实际 %q,期望 %q", got, want)
	}

	// /api/me 下发的也是新地址(页面上显示的就是它)。
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleMe(rec, req)
	var me struct {
		NotifyURLs map[string]string `json:"notify_urls"`
		NotifyBase string            `json:"notify_base_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &me); err != nil {
		t.Fatalf("解析 /api/me 响应失败: %v", err)
	}
	if me.NotifyURLs["wechat"] != want {
		t.Fatalf("/api/me 的回调地址应是 %q,实际 %q", want, me.NotifyURLs["wechat"])
	}
	if me.NotifyBase != "https://shop.example.org" {
		t.Fatalf("回显的对外地址不对: %q", me.NotifyBase)
	}

	// 下单:自己域名下的回调地址允许,别的域名仍然拒绝。
	pay := func(tradeNo, notify string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		body := `{"channel":"wechat","out_trade_no":"` + tradeNo + `","subject":"x","amount":100`
		if notify != "" {
			body += `,"notify_url":"` + notify + `"`
		}
		body += `}`
		req := httptest.NewRequest(http.MethodPost, "/api/pay", strings.NewReader(body))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("X-Verify-Token", verifyToken)
		req.Header.Set("Content-Type", "application/json")
		p.handlePay(rec, req)
		return rec
	}
	if rec := pay("PAY-BASE-1", "https://shop.example.org/notify/wechat"); rec.Code != http.StatusOK {
		t.Fatalf("用自己域名的回调地址下单应被允许,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if rec := pay("PAY-BASE-2", "https://evil.example.com/notify"); rec.Code != http.StatusBadRequest {
		t.Fatalf("指向别人的回调地址应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 清空 → 回到平台默认地址(-base)。
	if rec := post(`{"notify_base_url":""}`, verifyToken); rec.Code != http.StatusOK {
		t.Fatalf("恢复平台默认失败: %d %s", rec.Code, rec.Body.String())
	}
	wantDefault := "https://pay.example.com/notify/wechat/" + user.UserID
	if got := p.NotifyURL(payment.ChannelWechat, user.UserID); got != wantDefault {
		t.Fatalf("清空后应回到平台默认地址,实际 %q,期望 %q", got, wantDefault)
	}
}

// TestSameOrigin 回调地址同源判断。
func TestSameOrigin(t *testing.T) {
	cases := []struct {
		raw, base string
		want      bool
	}{
		{"https://pay.example.com/notify/wechat", "https://pay.example.com", true},
		{"http://pay.example.com/notify", "https://pay.example.com", false},
		{"https://evil.com/notify", "https://pay.example.com", false},
		{"https://pay.example.com:8443/notify", "https://pay.example.com", false},
		{"", "https://pay.example.com", false},
		{"https://pay.example.com/notify", "", false},
	}
	for _, c := range cases {
		if got := sameOrigin(c.raw, c.base); got != c.want {
			t.Fatalf("sameOrigin(%q,%q) = %v,期望 %v", c.raw, c.base, got, c.want)
		}
	}
}

// TestClientIPStripsPort 限流用的 IP 不能带端口,否则每次连接都是新 key。
func TestClientIPStripsPort(t *testing.T) {
	p, _, _, _ := newFeaturePlatform(t)
	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.RemoteAddr = "10.0.0.7:54321"
	if ip := p.clientIPOf(req); ip != "10.0.0.7" {
		t.Fatalf("来源 IP 应去掉端口,实际 %q", ip)
	}
	// 默认不信任 X-Forwarded-For(任何人都能伪造)。
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	if ip := p.clientIPOf(req); ip != "10.0.0.7" {
		t.Fatalf("默认不应采信 X-Forwarded-For,实际 %q", ip)
	}
	// 显式信任时才采信。
	p.opts.TrustForwardedFor = true
	if ip := p.clientIPOf(req); ip != "1.2.3.4" {
		t.Fatalf("开启信任后应采信 X-Forwarded-For,实际 %q", ip)
	}
}

// TestOrdersPaginationAndFilter 订单列表支持分页与筛选。
func TestOrdersPaginationAndFilter(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户辛")
	for i, status := range []string{
		string(payment.TradeStatusSuccess),
		string(payment.TradeStatusPending),
		string(payment.TradeStatusSuccess),
	} {
		if err := store.AddOrder(OrderRecord{
			OutTradeNo: "PAGE-" + string(rune('A'+i)),
			UserID:     user.UserID,
			Channel:    "wechat",
			Subject:    "订单",
			Amount:     1000,
			AmountYuan: "10.00",
			Status:     status,
		}); err != nil {
			t.Fatalf("下单失败: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/orders?page=1&size=2", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleOrdersAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("查订单应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	out := map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析订单列表失败: %v", err)
	}
	if numField(t, out, "total") != 3 {
		t.Fatalf("总数应为 3,实际 %v", out["total"])
	}
	if numField(t, out, "pages") != 2 {
		t.Fatalf("总页数应为 2,实际 %v", out["pages"])
	}
	orders, ok := out["orders"].([]any)
	if !ok {
		t.Fatalf("orders 字段类型不对: %T", out["orders"])
	}
	if len(orders) != 2 {
		t.Fatalf("每页应返回 2 条,实际 %d 条", len(orders))
	}

	// 按状态筛选。
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/orders?status=SUCCESS", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleOrdersAPI(rec, req)
	out = map[string]any{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析订单列表失败: %v", err)
	}
	if numField(t, out, "total") != 2 {
		t.Fatalf("按状态筛选后应为 2 条,实际 %v", out["total"])
	}
}

// numField 从解析出来的 JSON 里取一个数字字段。
//
// 不能直接写 out["total"].(float64) —— 字段缺失或类型不对时那是 panic,
// 而不是一条能看懂为什么失败的报错;而且把 Unmarshal 的错误吞掉的话,
// 解析失败时 out 是空 map,断言会给出误导性的结论。
func numField(t *testing.T, out map[string]any, key string) float64 {
	t.Helper()
	raw, ok := out[key]
	if !ok {
		t.Fatalf("响应里没有 %q 字段: %v", key, out)
	}
	num, ok := raw.(float64)
	if !ok {
		t.Fatalf("%q 字段不是数字: %v (%T)", key, raw, raw)
	}
	return num
}

// TestOrdersExportCSV 订单导出返回带 BOM 的 CSV。
func TestOrdersExportCSV(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户壬")
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "EXPORT-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Subject:    "导出订单",
		Amount:     1000,
		AmountYuan: "10.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/orders/export", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleOrdersExport(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("导出应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.Bytes()
	if len(body) < 3 || body[0] != 0xEF || body[1] != 0xBB || body[2] != 0xBF {
		t.Fatalf("CSV 缺少 UTF-8 BOM")
	}
	if !strings.Contains(string(body), "EXPORT-1") {
		t.Fatalf("CSV 里没有订单号: %s", string(body))
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Fatalf("导出应作为附件下载,实际 %q", cd)
	}
	// 导出必须留审计。
	logs, err := store.AuditLogs(user.UserID, 50)
	if err != nil {
		t.Fatalf("查审计日志失败: %v", err)
	}
	found := false
	for _, l := range logs {
		if l.Action == auditOrderExport {
			found = true
		}
	}
	if !found {
		t.Fatalf("导出没有写审计日志")
	}
}

// TestStatsReflectOrdersAndRefunds 统计口径:成交额、已退、净收入。
func TestStatsReflectOrdersAndRefunds(t *testing.T) {
	_, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户癸")
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: "STAT-1",
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     10000,
		AmountYuan: "100.00",
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	if err := store.AddRefund(RefundRecord{
		UserID:      user.UserID,
		OutTradeNo:  "STAT-1",
		OutRefundNo: "STAT-1-R1",
		Channel:     "wechat",
		Amount:      3000,
		Status:      "SUCCESS",
	}); err != nil {
		t.Fatalf("写退款流水失败: %v", err)
	}
	stats, err := store.Stats(user.UserID)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.OrderCount != 1 || stats.SuccessCount != 1 {
		t.Fatalf("订单/成功数不对: %+v", stats)
	}
	if stats.TotalAmount != 10000 || stats.RefundAmount != 3000 || stats.NetAmount != 7000 {
		t.Fatalf("金额统计不对: %+v", stats)
	}
	if stats.RefundCount != 1 {
		t.Fatalf("退款笔数不对: %+v", stats)
	}
	if stats.SuccessRate != 1 {
		t.Fatalf("成功率不对: %+v", stats)
	}
}

// TestHealthAndVersionArePublic 探针与版本接口免令牌。
func TestHealthAndVersionArePublic(t *testing.T) {
	p, _, _, _ := newFeaturePlatform(t)
	if !p.isPublicPath("/healthz") || !p.isPublicPath("/api/version") {
		t.Fatalf("探针与版本接口应当是公开路径")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	p.handleHealthz(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("健康检查应返回 200,实际 %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/version", nil)
	p.handleVersion(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["name"] != PlatformName || out["version"] != PlatformVersion {
		t.Fatalf("版本信息不对: %v", out)
	}
}

// TestSessionTokenExpires 访问令牌到期后立即失效。
func TestSessionTokenExpires(t *testing.T) {
	store := newTestStore(t)
	// 令牌本身用的是 store 的默认 TTL;
	// 过期是手工把库里的过期时间往前拨出来的(等真过期要等一整分钟)。
	box, err := NewSecretBox("test-master-key")
	if err != nil {
		t.Fatalf("NewSecretBox: %v", err)
	}
	short, err := NewStoreWithTTL(store.db, box, time.Hour)
	if err != nil {
		t.Fatalf("NewStoreWithTTL: %v", err)
	}
	user := newTestUser(t, short, "商户子")
	token, err := short.RotateAccessToken(user.UserID)
	if err != nil {
		t.Fatalf("RotateAccessToken: %v", err)
	}
	if u, err := short.UserByToken(token); err != nil {
		t.Fatalf("UserByToken: %v", err)
	} else if u == nil {
		t.Fatalf("刚签发的令牌应该有效")
	}
	// 手工把过期时间改到过去。
	if err := store.db.Model(&UserModel{}).Where("user_id = ?", user.UserID).
		Update("token_expires_at", time.Now().Add(-time.Minute).Format(time.RFC3339Nano)).Error; err != nil {
		t.Fatalf("改过期时间失败: %v", err)
	}
	if u, err := short.UserByToken(token); err != nil {
		t.Fatalf("UserByToken: %v", err)
	} else if u != nil {
		t.Fatalf("过期的令牌仍然有效")
	}
}

// TestCertDirIsPlatformManaged 商户不能自己指定证书缓存目录。
func TestCertDirIsPlatformManaged(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	p.opts.CertDir = t.TempDir()
	user := newTestUser(t, store, "商户丑")
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/channels/wechat",
		strings.NewReader(`{"enabled":false,"wechat":{"app_id":"wx1","cert_dir":"/etc"}}`))
	// 路由参数在直接调用 handler 时要自己塞(正常路径由 ServeMux 填)。
	req.SetPathValue("channel", "wechat")
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleSaveChannel(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("保存渠道配置应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	channels, err := store.Channels(user.UserID)
	if err != nil {
		t.Fatalf("读渠道配置失败: %v", err)
	}
	if len(channels) != 1 || channels[0].Wechat == nil {
		t.Fatalf("渠道配置没保存: %+v", channels)
	}
	if channels[0].Wechat.CertDir != p.certDirFor(user.UserID, "") {
		t.Fatalf("证书目录应由平台分配,实际 %q", channels[0].Wechat.CertDir)
	}
}
