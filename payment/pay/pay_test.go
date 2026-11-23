package pay

import (
	"bytes"
	"context"
	"image"
	_ "image/png" // 注册 PNG 解码器
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// fakeProvider 记录调用的假渠道(门面测试用,不打真实网络)。
type fakeProvider struct {
	channel    payment.Channel
	tradeTypes []payment.TradeType

	created  []payment.TradeType
	queried  []string
	closed   []string
	refunded []string
	notified []string
}

func (f *fakeProvider) Channel() payment.Channel { return f.channel }

func (f *fakeProvider) SupportedTradeTypes() []payment.TradeType { return f.tradeTypes }

func (f *fakeProvider) Create(_ context.Context, order *payment.Order, tt payment.TradeType) (*payment.PaymentSession, error) {
	f.created = append(f.created, tt)
	return &payment.PaymentSession{
		Channel:    f.channel,
		TradeType:  tt,
		OutTradeNo: order.OutTradeNo,
		QRCode:     "weixin://wxpay/bizpayurl?pr=fake",
	}, nil
}

func (f *fakeProvider) Query(_ context.Context, outTradeNo string) (*payment.TradeResult, error) {
	f.queried = append(f.queried, outTradeNo)
	return &payment.TradeResult{Channel: f.channel, OutTradeNo: outTradeNo, Status: payment.TradeStatusSuccess}, nil
}

func (f *fakeProvider) Close(_ context.Context, outTradeNo string) error {
	f.closed = append(f.closed, outTradeNo)
	return nil
}

func (f *fakeProvider) Refund(_ context.Context, req *payment.RefundRequest) (*payment.RefundResult, error) {
	f.refunded = append(f.refunded, req.OutTradeNo)
	return &payment.RefundResult{OutTradeNo: req.OutTradeNo, Channel: f.channel}, nil
}

func (f *fakeProvider) ParseNotify(_ context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	f.notified = append(f.notified, req.URL)
	return &payment.Notify{Channel: f.channel, OutTradeNo: "ORDER-1", Status: payment.TradeStatusSuccess}, nil
}

// newFakeClient 造一个只注册了假渠道的门面客户端。
func newFakeClient(t *testing.T) (*Client, *fakeProvider) {
	t.Helper()
	inner, err := payment.NewClient()
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	fp := &fakeProvider{
		channel:    payment.ChannelWechat,
		tradeTypes: []payment.TradeType{payment.TradeTypeNative, payment.TradeTypeJSAPI, payment.TradeTypeApp},
	}
	if err := inner.Register(fp); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return &Client{inner: inner}, fp
}

// TestFacadeRoutesToProvider 门面方法都要落到对应渠道。
func TestFacadeRoutesToProvider(t *testing.T) {
	client, fp := newFakeClient(t)
	ctx := context.Background()
	order := payment.Order{
		OutTradeNo: "ORDER-1",
		Subject:    "测试商品",
		Amount:     19900,
	}

	session, err := client.PayQRCode(ctx, Wechat, order)
	if err != nil {
		t.Fatalf("PayQRCode: %v", err)
	}
	if session.OutTradeNo != "ORDER-1" {
		t.Fatalf("订单号不对: %q", session.OutTradeNo)
	}
	if len(fp.created) != 1 || fp.created[0] != payment.TradeTypeNative {
		t.Fatalf("没有按扫码方式发起: %v", fp.created)
	}

	if _, err := client.PayApp(ctx, Wechat, order); err != nil {
		t.Fatalf("PayApp: %v", err)
	}
	if len(fp.created) != 2 || fp.created[1] != payment.TradeTypeApp {
		t.Fatalf("没有按 APP 方式发起: %v", fp.created)
	}

	// Pay 会按订单内容自动推断交易类型。
	if _, err := client.Pay(ctx, Wechat, order); err != nil {
		t.Fatalf("Pay: %v", err)
	}
	if len(fp.created) != 3 || fp.created[2] != payment.TradeTypeNative {
		t.Fatalf("自动推断的交易类型不对: %v", fp.created)
	}
	// 带 payer_id 的微信订单应走 JSAPI。
	jsapiOrder := order
	jsapiOrder.PayerID = "openid-123"
	if _, err := client.Pay(ctx, Wechat, jsapiOrder); err != nil {
		t.Fatalf("Pay(jsapi): %v", err)
	}
	if len(fp.created) != 4 || fp.created[3] != payment.TradeTypeJSAPI {
		t.Fatalf("带 payer_id 时应走 JSAPI: %v", fp.created)
	}

	// 查单 / 关单 / 退款。
	if _, err := client.Query(ctx, Wechat, "ORDER-1"); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(fp.queried) != 1 || fp.queried[0] != "ORDER-1" {
		t.Fatalf("查单没落到渠道: %v", fp.queried)
	}
	if err := client.Close(ctx, Wechat, "ORDER-1"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(fp.closed) != 1 {
		t.Fatalf("关单没落到渠道: %v", fp.closed)
	}
	if _, err := client.Refund(ctx, Wechat, payment.RefundRequest{
		OutTradeNo:   "ORDER-1",
		RefundAmount: 19900,
	}); err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if len(fp.refunded) != 1 {
		t.Fatalf("退款没落到渠道: %v", fp.refunded)
	}
}

// TestFacadeUnknownChannel 未注册的渠道必须报错,不能静默成功。
func TestFacadeUnknownChannel(t *testing.T) {
	client, _ := newFakeClient(t)
	ctx := context.Background()
	order := payment.Order{OutTradeNo: "ORDER-1", Subject: "x", Amount: 1}
	if _, err := client.PayQRCode(ctx, Alipay, order); err == nil {
		t.Fatalf("未注册的渠道竟然下单成功")
	}
	if _, err := client.Query(ctx, Alipay, "ORDER-1"); err == nil {
		t.Fatalf("未注册的渠道竟然查单成功")
	}
}

// TestFacadeHandleNotify 回调处理:从 HTTP 请求到统一模型。
func TestFacadeHandleNotify(t *testing.T) {
	client, fp := newFakeClient(t)
	req := httptest.NewRequest(http.MethodPost, "/notify/wechat/u1", strings.NewReader("body"))
	req.Header.Set("Content-Type", "application/json")
	notify, err := client.HandleNotify(context.Background(), Wechat, req)
	if err != nil {
		t.Fatalf("HandleNotify: %v", err)
	}
	if notify.OutTradeNo != "ORDER-1" {
		t.Fatalf("回调单号不对: %q", notify.OutTradeNo)
	}
	if len(fp.notified) != 1 {
		t.Fatalf("回调没落到渠道: %v", fp.notified)
	}
}

// TestFacadeQRCode 二维码渲染。
func TestFacadeQRCode(t *testing.T) {
	session := &payment.PaymentSession{QRCode: "weixin://wxpay/bizpayurl?pr=abc"}
	png, err := QRCodePNG(session, 8)
	if err != nil {
		t.Fatalf("QRCodePNG: %v", err)
	}
	if len(png) == 0 {
		t.Fatalf("二维码 PNG 为空")
	}
	// 必须是**真的 PNG**:只断言「非空」的话,返回一段任意字节也算过。
	if !bytes.HasPrefix(png, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		t.Fatalf("不是合法的 PNG(缺 magic bytes): % x", png[:min(8, len(png))])
	}
	// 再用标准库解一次,确认结构没问题。
	if _, _, err := image.Decode(bytes.NewReader(png)); err != nil {
		t.Fatalf("PNG 解码失败: %v", err)
	}
	svg, err := QRCodeSVG(session, 8)
	if err != nil {
		t.Fatalf("QRCodeSVG: %v", err)
	}
	if !strings.Contains(string(svg), "<svg") {
		t.Fatalf("SVG 内容不对")
	}
	// SVG 是用 path 画出来的(内容编码在路径数据里,不是明文),
	// 所以这里只能验结构:有方块路径、画布是正方形。
	if !strings.Contains(string(svg), "<path") {
		t.Fatalf("SVG 里没有二维码路径: %s", string(svg))
	}
	if !strings.Contains(string(svg), `width="296"`) || !strings.Contains(string(svg), `height="296"`) {
		t.Fatalf("SVG 画布尺寸不对(8 倍模块 * 37 模块 = 296): %s", string(svg)[:120])
	}
	// 空二维码应报错。
	if _, err := QRCodePNG(&payment.PaymentSession{}, 8); err == nil {
		t.Fatalf("空二维码竟然渲染成功")
	}
}

// TestFromHTTPRequest HTTP 请求转回调请求结构。
func TestFromHTTPRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/notify/wechat/u1?a=1", strings.NewReader("hello"))
	req.Header.Set("Wechatpay-Serial", "SERIAL1")
	got, err := FromHTTPRequest(req)
	if err != nil {
		t.Fatalf("FromHTTPRequest: %v", err)
	}
	if string(got.Body) != "hello" {
		t.Fatalf("请求体不对: %q", got.Body)
	}
	if got.Query["a"] != "1" {
		t.Fatalf("查询参数不对: %v", got.Query)
	}
	if got.Headers["Wechatpay-Serial"] != "SERIAL1" {
		t.Fatalf("请求头不对: %v", got.Headers)
	}
	if got.Method != http.MethodPost {
		t.Fatalf("方法不对: %q", got.Method)
	}
}
