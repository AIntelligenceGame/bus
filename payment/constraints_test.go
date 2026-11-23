package payment

import (
	"strings"
	"testing"
)

// 各渠道官方字段约束的回归测试。
//
// 每条都对应一个「商户填错了、只有渠道才会报错」的坑:
// 平台先拦一道,渠道实现再兜一道,两边用的是同一份规则(payment/constraints.go)。

// TestValidateOutTradeNoRules 商户订单号的长度与字符集按渠道校验。
func TestValidateOutTradeNoRules(t *testing.T) {
	cases := []struct {
		name    string
		channel Channel
		no      string
		wantErr bool
		why     string
	}{
		// 微信下单类是 string[6,32]:少于 6 位直接参数校验失败。
		{"微信 5 位", ChannelWechat, "OT001", true, "微信下单单号至少 6 位"},
		{"微信 6 位", ChannelWechat, "OT0001", false, "6 位是下限"},
		{"微信 32 位", ChannelWechat, strings.Repeat("A", 32), false, "32 位是上限"},
		{"微信 33 位", ChannelWechat, strings.Repeat("A", 33), true, "超了微信的 32 位上限"},
		{"微信含点", ChannelWechat, "OT.2026", true, "点不在微信允许的字符集里"},
		{"微信含斜杠", ChannelWechat, "OT/2026", true, "斜杠不在微信允许的字符集里"},
		{"微信含横杠下划线星号", ChannelWechat, "OT-2026_1*2", false, "这几个是微信文档里列明的"},
		{"微信含中文", ChannelWechat, "订单20261120", true, "微信的单号不能有中文"},
		{"微信含空格", ChannelWechat, "ORDER 123", true, "空格一律非法"},
		{"微信含控制字符", ChannelWechat, "ORDER\x01", true, "控制字符一律非法"},
		// 支付宝:最多 64 位,官方写「仅支持字母、数字、下划线」。
		{"支付宝 64 位", ChannelAlipay, strings.Repeat("A", 64), false, "64 位是上限"},
		{"支付宝 65 位", ChannelAlipay, strings.Repeat("A", 65), true, "超了支付宝的 64 位上限"},
		{"支付宝含点", ChannelAlipay, "OT.2026", true, "点不在支付宝允许的字符集里"},
		{"支付宝含横杠", ChannelAlipay, "OT-2026", false, "横杠官方说法不一,线上确实能付,不拦"},
		{"支付宝含下划线", ChannelAlipay, "OT_2026", false, "下划线是官方明确允许的"},
		{"支付宝含空格", ChannelAlipay, "ORDER 123", true, "空格一律非法"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateOutTradeNo(c.channel, c.no)
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

// TestValidateSubjectChars 标题长度按**字符数**算,不是字节数。
//
// 按字节算的话 60 个汉字就超了微信的 127 字上限,明显比官方严格。
func TestValidateSubjectChars(t *testing.T) {
	if err := ValidateSubject(ChannelWechat, strings.Repeat("订", 127)); err != nil {
		t.Fatalf("127 个汉字不该超限: %v", err)
	}
	if err := ValidateSubject(ChannelWechat, strings.Repeat("订", 128)); err == nil {
		t.Fatal("128 个汉字应该超限")
	}
	if err := ValidateSubject(ChannelAlipay, strings.Repeat("订", 256)); err != nil {
		t.Fatalf("支付宝 256 字不该超限: %v", err)
	}
	if err := ValidateSubject(ChannelAlipay, strings.Repeat("订", 257)); err == nil {
		t.Fatal("支付宝 257 字应该超限")
	}
}

// TestValidateOrderBodyChars 支付宝 body 最多 128 字。
//
// 官方「1000 字符」那个数字说的是 goods_detail[].body,不是 biz_content 顶层的 body。
func TestValidateOrderBodyChars(t *testing.T) {
	if err := ValidateOrderBody(ChannelAlipay, strings.Repeat("描", 128)); err != nil {
		t.Fatalf("128 个字不该超限: %v", err)
	}
	if err := ValidateOrderBody(ChannelAlipay, strings.Repeat("描", 129)); err == nil {
		t.Fatal("129 个字应该超限")
	}
}

// TestValidateAttachLengths 附加数据的长度按渠道限制。
func TestValidateAttachLengths(t *testing.T) {
	if err := ValidateAttach(ChannelWechat, strings.Repeat("a", 128)); err != nil {
		t.Fatalf("微信 attach 128 字不该超限: %v", err)
	}
	if err := ValidateAttach(ChannelWechat, strings.Repeat("a", 129)); err == nil {
		t.Fatal("微信 attach 129 字应该超限")
	}
	if err := ValidateAttach(ChannelAlipay, strings.Repeat("a", 512)); err != nil {
		t.Fatalf("支付宝 passback_params 512 字不该超限: %v", err)
	}
	if err := ValidateAttach(ChannelAlipay, strings.Repeat("a", 513)); err == nil {
		t.Fatal("支付宝 passback_params 513 字应该超限")
	}
}

// TestWechatDescriptionComposedLength 微信的 description 是**拼出来的一个字段**。
//
// 之前平台只校验 subject 自己的长度,而渠道发出去的是
// subject + " - " + body —— 拼接后超了 127 字符,微信只回一句
// "invalid parameter",商户完全看不出是哪个字段超了。
//
// 这个用例直接打校验入口:拼接后超限必须被拦下来。
func TestWechatDescriptionComposedLength(t *testing.T) {
	subject := strings.Repeat("标", 120)
	body := strings.Repeat("描", 30)
	// 单独看,两段都没超 127。
	if err := ValidateSubject(ChannelWechat, subject); err != nil {
		t.Fatalf("120 字的标题不该超限: %v", err)
	}
	// 拼起来 120 + 3 + 30 = 153 字,超了。
	order := &Order{
		OutTradeNo: "OT20260001",
		Subject:    subject,
		Body:       body,
		Amount:     100,
	}
	err := ValidateChannelOrder(ChannelWechat, order)
	if err == nil {
		t.Fatal("拼接后超 127 字符应该被拦下来")
	}
	if !strings.Contains(err.Error(), "127") {
		t.Fatalf("报错里要写明上限: %v", err)
	}
	// 拼接刚好 127 字时必须放行。
	order.Subject = strings.Repeat("标", 94)
	order.Body = strings.Repeat("描", 30) // 94 + 3 + 30 = 127
	if err := ValidateChannelOrder(ChannelWechat, order); err != nil {
		t.Fatalf("刚好 127 字不该被拒: %v", err)
	}
}

// TestWechatDescriptionHelper 拼接规则只有一处,校验和发送用的是同一个函数。
func TestWechatDescriptionHelper(t *testing.T) {
	if got := WechatDescription("标题", ""); got != "标题" {
		t.Fatalf("没有 body 时不该加后缀: %q", got)
	}
	if got := WechatDescription("标题", "描述"); got != "标题 - 描述" {
		t.Fatalf("拼接结果不对: %q", got)
	}
}

// TestAlipayPassbackParamsEncoded 支付宝的 passback_params 必须 UrlEncoded。
//
// 官方明确要求:不编码会报「订单信息无法识别」;值里带双引号(JSON 格式)
// 会报「订单参数异常」。
func TestAlipayPassbackParamsEncoded(t *testing.T) {
	if got := AlipayPassbackParams(""); got != "" {
		t.Fatalf("空值不该编码成非空: %q", got)
	}
	got := AlipayPassbackParams(`{"shop":"1001"}`)
	if strings.ContainsAny(got, `{}"`) {
		t.Fatalf("编码后不该还有 JSON 的括号与引号: %q", got)
	}
	if !strings.Contains(got, "%22") {
		t.Fatalf("双引号应该被编码成 %%22: %q", got)
	}
}
