package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// fakeProvider 可配置的假渠道(测试用)。
//
// 平台自带的 amountRecorder 只能记录退款金额,这里把各个入口的返回
// 都做成可配置的,方便覆盖「渠道报错 / 渠道返回别的状态」这类分支。
type fakeProvider struct {
	channel     payment.Channel
	refundErr   error
	refundSts   string
	queryResult *payment.TradeResult
	notify      *payment.Notify
	refundCalls int
	closeCalls  int
}

func (f *fakeProvider) Channel() payment.Channel { return f.channel }

func (f *fakeProvider) SupportedTradeTypes() []payment.TradeType {
	return []payment.TradeType{payment.TradeTypeNative}
}

func (f *fakeProvider) Create(ctx context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	return &payment.PaymentSession{Channel: f.channel, QRCode: "pay://x=1"}, nil
}

func (f *fakeProvider) Query(ctx context.Context, outTradeNo string) (*payment.TradeResult, error) {
	if f.queryResult != nil {
		return f.queryResult, nil
	}
	return &payment.TradeResult{Channel: f.channel}, nil
}

func (f *fakeProvider) Close(ctx context.Context, outTradeNo string) error {
	f.closeCalls++
	return nil
}

func (f *fakeProvider) Refund(ctx context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	f.refundCalls++
	if f.refundErr != nil {
		return nil, f.refundErr
	}
	sts := f.refundSts
	if sts == "" {
		sts = string(payment.TradeStatusSuccess)
	}
	return &payment.RefundResult{
		Channel:      f.channel,
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  req.OutRefundNo,
		RefundAmount: req.RefundAmount,
		Status:       sts,
	}, nil
}

func (f *fakeProvider) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	if f.notify != nil {
		return f.notify, nil
	}
	return &payment.Notify{Channel: f.channel}, nil
}

// newRefundTestPlatform 造一个带假渠道的平台 + 一个已支付成功的订单。
func newRefundTestPlatform(t *testing.T, orderNo string, amount int64) (*Platform, *Store, *User, *fakeProvider) {
	t.Helper()
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户测试")
	fake := &fakeProvider{channel: payment.ChannelWechat}
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: fake,
	}
	if err := store.AddOrder(OrderRecord{
		OutTradeNo: orderNo,
		UserID:     user.UserID,
		Channel:    "wechat",
		Amount:     amount,
		AmountYuan: payment.CentsToYuan(amount),
		Status:     string(payment.TradeStatusSuccess),
	}); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	return p, store, user, fake
}

// refundCall 发起一次退款请求。
func refundCall(t *testing.T, p *Platform, user *User, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/refund", strings.NewReader(string(raw)))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleRefund(rec, req)
	return rec
}

// TestRefundIdempotentByOutRefundNo 同一个退款单号重复提交只退一次钱。
//
// 这是资金安全的底线:商户系统重试(超时重发、用户重复点击)不该导致重复出款。
func TestRefundIdempotentByOutRefundNo(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "IDEM-1", 10000)

	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "IDEM-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("首次退款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}

	// 同一个退款单号再退一次 → 幂等返回,不重复出款。
	rec = refundCall(t, p, user, map[string]any{
		"out_trade_no":  "IDEM-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("重复退款应幂等成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["idempotent"] != true {
		t.Fatalf("重复退款应标记为幂等返回,实际 %v", out["idempotent"])
	}
	if fake.refundCalls != 1 {
		t.Fatalf("重复退款单号只能调用渠道一次,实际 %d 次", fake.refundCalls)
	}
	refunds, err := store.Refunds(user.UserID, 100)
	if err != nil {
		t.Fatalf("查退款流水失败: %v", err)
	}
	if len(refunds) != 1 {
		t.Fatalf("应有 1 条退款流水,实际 %d 条", len(refunds))
	}
	total, err := store.RefundedAmount(user.UserID, "IDEM-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 3000 {
		t.Fatalf("累计退款金额应为 3000,实际 %d", total)
	}
}

// TestRefundIdempotentSameKeyDifferentAmountReturnsFirstRefund
// 同一个退款单号换金额:已成功的那笔直接原样返回,绝不能再出一笔。
//
// 这里的行为分成两种,别混在一起:
//   - 上一笔**成功/在途**:幂等命中,原样返回那一笔(改金额无效,也不会多退)。
//   - 上一笔**失败**:允许拿同一个单号重试,但金额必须一致 —— 见下面那条测试。
func TestRefundIdempotentSameKeyDifferentAmountReturnsFirstRefund(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "IDEM-2", 10000)
	if rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "IDEM-2",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	}); rec.Code != http.StatusOK {
		t.Fatalf("首次退款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	// 同一个退款单号,金额改成 5000 → 幂等命中(返回原来的 3000 那笔),
	// 绝不能再退一次 5000。
	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "IDEM-2",
		"out_refund_no": "R-1",
		"refund_amount": 5000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("重复退款单号应幂等返回,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.refundCalls != 1 {
		t.Fatalf("重复退款单号只能调用渠道一次,实际 %d 次", fake.refundCalls)
	}
	// 返回的必须还是原来那笔 3000,不能被这次的 5000 覆盖。
	var out struct {
		RefundAmount int64 `json:"refund_amount"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析退款响应失败: %v", err)
	}
	if out.RefundAmount != 3000 {
		t.Fatalf("幂等返回的退款金额应是原来的 3000,实际 %d", out.RefundAmount)
	}
	total, err := store.RefundedAmount(user.UserID, "IDEM-2")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 3000 {
		t.Fatalf("累计退款金额应为 3000,实际 %d", total)
	}
}

// TestRefundFailedRetryDifferentAmountRejected
// 上一笔退款**失败**时,拿同一个单号重试可以(钱没退出去,额度也没占),
// 但金额必须一致 —— 同一个单号代表同一笔退款,不许中途改金额。
func TestRefundFailedRetryDifferentAmountRejected(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "IDEM-3", 10000)
	// 直接造一笔失败的退款流水(渠道退款失败时会落成这样)。
	if err := store.AddRefund(RefundRecord{
		UserID:      user.UserID,
		OutTradeNo:  "IDEM-3",
		OutRefundNo: "R-9",
		Channel:     "wechat",
		Amount:      3000,
		Status:      refundStatusFailed,
	}); err != nil {
		t.Fatalf("造失败退款流水失败: %v", err)
	}
	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "IDEM-3",
		"out_refund_no": "R-9",
		"refund_amount": 5000,
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("失败退款换金额重试应被拒,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "R-9") {
		t.Fatalf("错误信息里应带上退款单号: %s", rec.Body.String())
	}
	if fake.refundCalls != 0 {
		t.Fatalf("被拒的退款不该调渠道,实际调了 %d 次", fake.refundCalls)
	}
}

// TestRefundChannelFailureKeepsQuotaFree 渠道退款失败不该占掉可退额度。
func TestRefundChannelFailureKeepsQuotaFree(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "FAIL-1", 10000)
	fake.refundErr = fmt.Errorf("渠道超时")

	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "FAIL-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("渠道失败应返回 502,实际 %d: %s", rec.Code, rec.Body.String())
	}
	total, err := store.RefundedAmount(user.UserID, "FAIL-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 0 {
		t.Fatalf("失败的退款不该计入已退金额,实际 %d", total)
	}
	// 失败的流水状态应为 FAILED。
	rec2, err := store.RefundByNo(user.UserID, "R-1")
	if err != nil {
		t.Fatalf("RefundByNo: %v", err)
	}
	if rec2 == nil || rec2.Status != refundStatusFailed {
		t.Fatalf("失败的退款流水状态应为 FAILED,实际 %+v", rec2)
	}
	// 换个退款单号可以重试(额度没被占)。
	fake.refundErr = nil
	rec = refundCall(t, p, user, map[string]any{
		"out_trade_no":  "FAIL-1",
		"out_refund_no": "R-2",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("重试退款应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestRefundRetrySameOutRefundNoAfterFailure 渠道失败后用**同一个退款单号**
// 重试应该能退成功(网关惯例:幂等键不变 = 重试)。
//
// 如果失败后同单号被幂等逻辑永久挡死,商户就只能换单号;商户换了单号又
// 容易重试到「其实已经出款」的旧单上,反而更危险。
func TestRefundRetrySameOutRefundNoAfterFailure(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "RETRY-1", 10000)
	fake.refundErr = fmt.Errorf("渠道超时")

	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "RETRY-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("渠道失败应返回 502,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.refundCalls != 1 {
		t.Fatalf("失败前应调用渠道 1 次,实际 %d", fake.refundCalls)
	}

	// 渠道恢复后,用同一个退款单号重试。
	fake.refundErr = nil
	rec = refundCall(t, p, user, map[string]any{
		"out_trade_no":  "RETRY-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("同单号重试应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.refundCalls != 2 {
		t.Fatalf("重试应再调用渠道 1 次,实际调用 %d 次", fake.refundCalls)
	}
	// 只应有一行流水(重试复用原来那行,不新插)。
	one, err := store.RefundByNo(user.UserID, "R-1")
	if err != nil {
		t.Fatalf("RefundByNo: %v", err)
	}
	if one == nil || one.Status != refundStatusSuccess {
		t.Fatalf("重试成功后流水状态应为 SUCCESS,实际 %+v", one)
	}
	total, err := store.RefundedAmount(user.UserID, "RETRY-1")
	if err != nil {
		t.Fatalf("RefundedAmount: %v", err)
	}
	if total != 3000 {
		t.Fatalf("重试成功后已退金额应为 3000,实际 %d", total)
	}
}

// TestRefundSameOutRefundNoDifferentAmountRejected 同一个退款单号不允许
// 中途改金额(否则幂等键就失去了意义)。
func TestRefundSameOutRefundNoDifferentAmountRejected(t *testing.T) {
	p, _, user, fake := newRefundTestPlatform(t, "AMT-1", 10000)
	fake.refundErr = fmt.Errorf("渠道超时")

	rec := refundCall(t, p, user, map[string]any{
		"out_trade_no":  "AMT-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("渠道失败应返回 502,实际 %d: %s", rec.Code, rec.Body.String())
	}
	fake.refundErr = nil
	rec = refundCall(t, p, user, map[string]any{
		"out_trade_no":  "AMT-1",
		"out_refund_no": "R-1",
		"refund_amount": 5000,
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("同单号改金额应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.refundCalls != 1 {
		t.Fatalf("被拒绝的重试不该调用渠道,实际调用 %d 次", fake.refundCalls)
	}
}

// TestRefundUsesOrderOwnChannel 退款必须走订单自己的渠道。
//
// 退错渠道等于把钱退到别人的商户号上,平台自己兜不住,只能直接拒绝。
func TestRefundUsesOrderOwnChannel(t *testing.T) {
	p, _, user, fake := newRefundTestPlatform(t, "CH-1", 10000)
	rec := refundCall(t, p, user, map[string]any{
		"channel":       "alipay",
		"out_trade_no":  "CH-1",
		"out_refund_no": "R-1",
		"refund_amount": 3000,
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("指定与订单不一致的渠道应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.refundCalls != 0 {
		t.Fatalf("被拒绝的退款不该调用渠道,实际 %d 次", fake.refundCalls)
	}
}

// TestCloseRejectedForPaidOrder 已支付的订单不能关单。
func TestCloseRejectedForPaidOrder(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "CLOSE-PAID", 10000)
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/close",
		strings.NewReader(`{"out_trade_no":"CLOSE-PAID"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleClose(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("已支付订单关单应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	if fake.closeCalls != 0 {
		t.Fatalf("被拒绝的关单不该调用渠道,实际 %d 次", fake.closeCalls)
	}
}

// TestQueryDoesNotRegressRefundedOrder 查单不能把已退款订单改回成功。
func TestQueryDoesNotRegressRefundedOrder(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "QUERY-1", 10000)
	fake.queryResult = &payment.TradeResult{
		Channel: payment.ChannelWechat,
		Status:  payment.TradeStatusSuccess,
		Amount:  10000,
	}
	// 把订单改成「已退款」。
	if _, err := store.UpdateOrder(user.UserID, "QUERY-1", func(rec *OrderRecord) {
		rec.Status = string(payment.TradeStatusRefunded)
	}); err != nil {
		t.Fatalf("UpdateOrder: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query",
		strings.NewReader(`{"out_trade_no":"QUERY-1"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("查单应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	order, err := store.OrderByTradeNo(user.UserID, "QUERY-1")
	if err != nil || order == nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusRefunded) {
		t.Fatalf("已退款订单不能被查单改回 %s,实际 %s", payment.TradeStatusSuccess, order.Status)
	}
}

// TestQueryRejectsAmountMismatch 渠道返回的金额与订单不一致要报错。
func TestQueryRejectsAmountMismatch(t *testing.T) {
	p, _, user, fake := newRefundTestPlatform(t, "QUERY-2", 10000)
	fake.queryResult = &payment.TradeResult{
		Channel: payment.ChannelWechat,
		Status:  payment.TradeStatusSuccess,
		Amount:  999,
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query",
		strings.NewReader(`{"out_trade_no":"QUERY-2"}`))
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleQuery(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("金额不一致的查单应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// notifyCall 送一条渠道回调。
func notifyCall(t *testing.T, p *Platform, user *User) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/notify/wechat/"+user.UserID, strings.NewReader(`{"event_type":"TRANSACTION.SUCCESS"}`))
	req.SetPathValue("channel", "wechat")
	req.SetPathValue("user_id", user.UserID)
	req.Header.Set("Content-Type", "application/json")
	p.handleNotify(rec, req)
	return rec
}

// TestNotifyRejectsAmountMismatch 回调金额与订单金额不一致必须拒收。
func TestNotifyRejectsAmountMismatch(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "NOTIFY-1", 10000)
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: "NOTIFY-1",
		Amount:     5000,
		Status:     payment.TradeStatusSuccess,
	}
	// 先把订单改成「待支付」,这样「被拒绝的回调不改状态」才真的能验出东西 ——
	// 用一个本来就是 SUCCESS 的订单来断言「状态不是 SUCCESS」,那个断言永远不会失败。
	if err := store.db.Model(&OrderModel{}).
		Where("user_id = ? AND out_trade_no = ?", user.UserID, "NOTIFY-1").
		UpdateColumn("order_status", string(payment.TradeStatusPending)).Error; err != nil {
		t.Fatalf("改订单状态失败: %v", err)
	}
	rec := notifyCall(t, p, user)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("金额不一致的回调应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
	order, err := store.OrderByTradeNo(user.UserID, "NOTIFY-1")
	if err != nil || order == nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusPending) {
		t.Fatalf("被拒绝的回调不该改订单状态,实际 %s", order.Status)
	}
}

// TestNotifyRejectsUnknownOrder 回调的订单不存在要报错。
func TestNotifyRejectsUnknownOrder(t *testing.T) {
	p, _, user, fake := newRefundTestPlatform(t, "NOTIFY-2", 10000)
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: "NOT-EXIST",
		Amount:     10000,
		Status:     payment.TradeStatusSuccess,
	}
	rec := notifyCall(t, p, user)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("订单不存在的回调应被拒绝,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestNotifyDoesNotRegressRefundedOrder 迟到的成功回调不能把已退款订单改回去。
func TestNotifyDoesNotRegressRefundedOrder(t *testing.T) {
	p, store, user, fake := newRefundTestPlatform(t, "NOTIFY-3", 10000)
	if _, err := store.UpdateOrder(user.UserID, "NOTIFY-3", func(rec *OrderRecord) {
		rec.Status = string(payment.TradeStatusRefunded)
	}); err != nil {
		t.Fatalf("UpdateOrder: %v", err)
	}
	fake.notify = &payment.Notify{
		Channel:    payment.ChannelWechat,
		OutTradeNo: "NOTIFY-3",
		Amount:     10000,
		Status:     payment.TradeStatusSuccess,
	}
	rec := notifyCall(t, p, user)
	if rec.Code != http.StatusOK {
		t.Fatalf("回调应被接收,实际 %d: %s", rec.Code, rec.Body.String())
	}
	order, err := store.OrderByTradeNo(user.UserID, "NOTIFY-3")
	if err != nil || order == nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if order.Status != string(payment.TradeStatusRefunded) {
		t.Fatalf("已退款订单不能被回调改回 %s,实际 %s", payment.TradeStatusSuccess, order.Status)
	}
}

// TestRevealChannelNoLongerReturnsSecrets 密钥明文不再回显到浏览器。
func TestRevealChannelNoLongerReturnsSecrets(t *testing.T) {
	p, store, user, _ := newRefundTestPlatform(t, "REVEAL-1", 10000)
	verifyToken, err := store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/channels/wechat/reveal", strings.NewReader("{}"))
	req.SetPathValue("channel", "wechat")
	req.Header.Set("X-User-Token", user.AccessToken)
	req.Header.Set("X-Verify-Token", verifyToken)
	req.Header.Set("Content-Type", "application/json")
	p.handleRevealChannel(rec, req)
	if rec.Code != http.StatusGone {
		t.Fatalf("密钥回显接口应已下线(410),实际 %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "private_key") ||
		strings.Contains(rec.Body.String(), "api_v3_key") {
		t.Fatalf("回显接口不该返回任何密钥字段: %s", rec.Body.String())
	}
}

// TestAdvanceStatus 订单状态机的规则。
func TestAdvanceStatus(t *testing.T) {
	cases := []struct {
		from, to payment.TradeStatus
		want     payment.TradeStatus
		ok       bool
	}{
		{payment.TradeStatusPending, payment.TradeStatusSuccess, payment.TradeStatusSuccess, true},
		{payment.TradeStatusPending, payment.TradeStatusClosed, payment.TradeStatusClosed, true},
		{payment.TradeStatusSuccess, payment.TradeStatusPending, payment.TradeStatusSuccess, false},
		{payment.TradeStatusSuccess, payment.TradeStatusRefunded, payment.TradeStatusRefunded, true},
		// 已关闭的订单如果钱到账了,以钱为准。
		{payment.TradeStatusClosed, payment.TradeStatusSuccess, payment.TradeStatusSuccess, true},
		// 已退款的订单不能再被改回成功。
		{payment.TradeStatusRefunded, payment.TradeStatusSuccess, payment.TradeStatusRefunded, false},
		{payment.TradeStatusPartiallyRefunded, payment.TradeStatusPartiallyRefunded,
			payment.TradeStatusPartiallyRefunded, true},
		// 空目标状态不生效。
		{payment.TradeStatusPending, "", payment.TradeStatusPending, false},
	}
	for _, c := range cases {
		got, ok := advanceStatus(c.from, c.to)
		if got != c.want || ok != c.ok {
			t.Fatalf("advanceStatus(%s,%s) = (%s,%v),期望 (%s,%v)",
				c.from, c.to, got, ok, c.want, c.ok)
		}
	}
}

// TestNormalizeRefundStatus 渠道退款状态归一。
func TestNormalizeRefundStatus(t *testing.T) {
	cases := map[string]string{
		"":           refundStatusSuccess,
		"SUCCESS":    refundStatusSuccess,
		"success":    refundStatusSuccess,
		"PROCESSING": refundStatusProcessing,
		"CLOSED":     refundStatusFailed,
		"ABNORMAL":   refundStatusFailed,
	}
	for in, want := range cases {
		if got := normalizeRefundStatus(in); got != want {
			t.Fatalf("normalizeRefundStatus(%q) = %s,期望 %s", in, got, want)
		}
	}
	if refundCountsTowardTotal(refundStatusFailed) {
		t.Fatal("失败的退款不该计入已退金额")
	}
	if !refundCountsTowardTotal(refundStatusPending) {
		t.Fatal("占位的退款必须计入已退金额(否则会被重复退款)")
	}
}
