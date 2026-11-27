package payment

import "testing"

// TestValidateCurrencyRejectsForeignCurrency 非人民币的币种必须被拒。
//
// 官方约束:
//   - 微信支付 APIv3 境内商户号:「CNY:人民币,境内商户号仅支持人民币」。
//   - 支付宝境内交易接口**没有币种字段**,total_amount 的单位就是「元」(人民币)。
//
// 支付宝这条尤其要紧:不拦的话,传 USD 会被**当成人民币**扣款 ——
// 金额直接差 7 倍,而且不会有任何报错。
func TestValidateCurrencyRejectsForeignCurrency(t *testing.T) {
	cases := []struct {
		channel  Channel
		currency string
		wantErr  bool
	}{
		{ChannelWechat, "", false},
		{ChannelWechat, "CNY", false},
		{ChannelWechat, "cny", false},
		{ChannelWechat, " CNY ", false},
		{ChannelWechat, "USD", true},
		{ChannelWechat, "HKD", true},
		{ChannelAlipay, "", false},
		{ChannelAlipay, "CNY", false},
		{ChannelAlipay, "USD", true},
		{ChannelAlipay, "EUR", true},
		// 其它渠道(Apple Pay / Google Pay 走 Stripe)支持多币种,不在这里限制。
		{ChannelApplePay, "USD", false},
		{ChannelGooglePay, "USD", false},
	}
	for _, c := range cases {
		err := ValidateCurrency(c.channel, c.currency)
		if c.wantErr && err == nil {
			t.Fatalf("渠道 %s 币种 %q 应该被拒,却通过了", c.channel, c.currency)
		}
		if !c.wantErr && err != nil {
			t.Fatalf("渠道 %s 币种 %q 不该被拒: %v", c.channel, c.currency, err)
		}
	}
}

// TestValidateChannelOrderChecksCurrency 整单校验里必须带上币种这一条。
//
// 渠道实现是在 Create 里调 ValidateChannelOrder 的,币种不在里面的话
// 直接用库的调用方就绕过了这道拦截。
func TestValidateChannelOrderChecksCurrency(t *testing.T) {
	order := &Order{
		OutTradeNo: "ORDER-1",
		Subject:    "测试订单",
		Amount:     100,
		Currency:   "USD",
	}
	if err := ValidateChannelOrder(ChannelWechat, order); err == nil {
		t.Fatal("微信订单带 USD 应该被拒")
	}
	if err := ValidateChannelOrder(ChannelAlipay, order); err == nil {
		t.Fatal("支付宝订单带 USD 应该被拒")
	}
	// 人民币必须放行。
	order.Currency = "CNY"
	if err := ValidateChannelOrder(ChannelWechat, order); err != nil {
		t.Fatalf("人民币订单不该被拒: %v", err)
	}
	if err := ValidateChannelOrder(ChannelAlipay, order); err != nil {
		t.Fatalf("人民币订单不该被拒: %v", err)
	}
}
