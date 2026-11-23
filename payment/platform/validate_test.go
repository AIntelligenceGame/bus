package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// 下单参数校验(按渠道官方字段约束)的回归测试。
//
// 每条都对应一个「商户填错了、只有渠道才会报错」的坑:
// 平台先拦一道,报错误说的是哪个字段违反了哪条限制。

// TestValidateOutTradeNo 商户订单号的长度与字符集要按渠道校验。
func TestValidateOutTradeNo(t *testing.T) {
	cases := []struct {
		name    string
		channel payment.Channel
		no      string
		wantErr bool
		why     string
	}{
		{"微信 32 位", payment.ChannelWechat, strings.Repeat("A", 32), false, "32 位是上限"},
		{"微信 33 位", payment.ChannelWechat, strings.Repeat("A", 33), true, "超了微信的 32 位上限"},
		// 微信下单单号官方是 string[6,32]:少于 6 位,微信直接参数校验失败。
		{"微信 5 位", payment.ChannelWechat, "OT001", true, "微信下单单号至少 6 位"},
		{"微信 6 位", payment.ChannelWechat, "OT0001", false, "6 位是下限"},
		{"支付宝 64 位", payment.ChannelAlipay, strings.Repeat("A", 64), false, "64 位是上限"},
		{"支付宝 65 位", payment.ChannelAlipay, strings.Repeat("A", 65), true, "超了支付宝的 64 位上限"},
		{"含中文", payment.ChannelWechat, "订单20261120", true, "微信的商户订单号不能有中文"},
		{"含空格", payment.ChannelAlipay, "ORDER 123", true, "两边都不允许空格"},
		{"含控制字符", payment.ChannelWechat, "ORDER\x01", true, "控制字符一律非法"},
		{"含点", payment.ChannelWechat, "OT.2026", true, "点不在微信允许的字符集里"},
		{"普通单号", payment.ChannelWechat, "OT202611200001", false, "正常单号"},
		{"带横杠下划线", payment.ChannelWechat, "OT-2026_1120", false, "横杠下划线都合法"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateOutTradeNo(c.channel, c.no)
			if c.wantErr && err == nil {
				t.Fatalf("应该报错却没报(%s)", c.why)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("不该报错却报了: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "商户订单号") {
				t.Fatalf("报错要说清楚是商户订单号的问题: %v", err)
			}
		})
	}
}

// TestValidateSubject 订单标题长度按**字符数**算(不是字节数)。
//
// 按字节算的话 60 个汉字就超了微信的 127 字上限,明显比官方严格。
func TestValidateSubject(t *testing.T) {
	if err := validateSubject(payment.ChannelWechat, strings.Repeat("订", 127)); err != nil {
		t.Fatalf("127 个汉字不该超限: %v", err)
	}
	if err := validateSubject(payment.ChannelWechat, strings.Repeat("订", 128)); err == nil {
		t.Fatal("128 个汉字应该超限")
	}
	if err := validateSubject(payment.ChannelAlipay, strings.Repeat("订", 256)); err != nil {
		t.Fatalf("支付宝 256 字不该超限: %v", err)
	}
	if err := validateSubject(payment.ChannelAlipay, strings.Repeat("订", 257)); err == nil {
		t.Fatal("支付宝 257 字应该超限")
	}
}

// TestValidateAttach 商户附加数据的长度按渠道限制。
//
// 微信 attach 最多 128 字符;支付宝那边发出去的是 UrlEncoded 后的
// passback_params,最多 512 字符 —— 超了都是渠道的参数校验失败,
// 商户只看得见一句 "invalid parameter"。
func TestValidateAttach(t *testing.T) {
	if err := validateAttach(payment.ChannelWechat, strings.Repeat("a", 128)); err != nil {
		t.Fatalf("微信 attach 128 字不该超限: %v", err)
	}
	if err := validateAttach(payment.ChannelWechat, strings.Repeat("a", 129)); err == nil {
		t.Fatal("微信 attach 129 字应该超限")
	}
	if err := validateAttach(payment.ChannelAlipay, strings.Repeat("a", 512)); err != nil {
		t.Fatalf("支付宝 passback_params 512 字不该超限: %v", err)
	}
	if err := validateAttach(payment.ChannelAlipay, strings.Repeat("a", 513)); err == nil {
		t.Fatal("支付宝 passback_params 513 字应该超限")
	}
}

// TestValidateTradeType 渠道不支持的交易类型要提前报错。
func TestValidateTradeType(t *testing.T) {
	supported := []payment.TradeType{payment.TradeTypeNative, payment.TradeTypeApp}
	err := validatePayRequest(payment.ChannelWechat, payment.TradeTypeWallet, supported, payRequest{
		OutTradeNo: "OT2026",
		Subject:    "x",
		Amount:     100,
	})
	if err == nil {
		t.Fatal("微信不支持 wallet,应该报错")
	}
	if !strings.Contains(err.Error(), "wallet") {
		t.Fatalf("报错里要点名交易类型: %v", err)
	}
	if err := validatePayRequest(payment.ChannelWechat, payment.TradeTypeNative, supported, payRequest{
		OutTradeNo: "OT2026",
		Subject:    "x",
		Amount:     100,
	}); err != nil {
		t.Fatalf("原生支付不该报错: %v", err)
	}
}

// TestValidateAmount 金额必须为正(单位:分)。
func TestValidateAmount(t *testing.T) {
	err := validatePayRequest(payment.ChannelWechat, payment.TradeTypeNative,
		[]payment.TradeType{payment.TradeTypeNative},
		payRequest{OutTradeNo: "OT2026", Subject: "x", Amount: 0})
	if err == nil {
		t.Fatal("金额 0 应该被拒")
	}
	err = validatePayRequest(payment.ChannelWechat, payment.TradeTypeNative,
		[]payment.TradeType{payment.TradeTypeNative},
		payRequest{OutTradeNo: "OT2026", Subject: "x", Amount: -1})
	if err == nil {
		t.Fatal("负金额应该被拒")
	}
}

// TestInvalidOrderLeavesNoRecord 校验不过时**不能在库里留下订单记录**。
//
// 留一条「永远付不成」的半成品订单,商户在订单列表里看到会以为平台出过单,
// 还会去拿这个单号重试(然后撞上「商户单号已存在」)。
func TestInvalidOrderLeavesNoRecord(t *testing.T) {
	p, store, _, recorder := newFeaturePlatform(t)
	user := newTestUser(t, store, "m1")
	p.providers[user.UserID] = map[payment.Channel]payment.Provider{
		payment.ChannelWechat: recorder,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/pay", nil)
	_, err := p.corePay(req, user, payRequest{
		Channel:    "wechat",
		OutTradeNo: "订单号带中文",
		Subject:    "测试",
		Amount:     100,
	})
	if err == nil {
		t.Fatal("非法商户订单号应该被拒")
	}
	if statusOf(err) != http.StatusBadRequest {
		t.Fatalf("应该返回 400,实际 %d", statusOf(err))
	}
	// 库里不能有任何订单。
	rows, _, qerr := store.OrdersPage(user.UserID, OrderFilter{Page: 1, Size: 20})
	if qerr != nil {
		t.Fatalf("查订单失败: %v", qerr)
	}
	if len(rows) != 0 {
		t.Fatalf("校验没过却留下了 %d 条订单记录", len(rows))
	}
}
