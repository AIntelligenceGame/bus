package wechat

import (
	"context"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// 微信官方字段约束的回归测试(渠道这一层是**兜底**:
// 平台在下单前会先拦一道,直接用库的调用方走的是这里)。

// TestCreateRejectsInvalidFields 违反官方约束的订单,必须在下单前就报错。
//
// 报错要说得清是哪个字段:渠道只会回一句 "invalid parameter",
// 商户拿到那句话根本不知道该改哪里。
func TestCreateRejectsInvalidFields(t *testing.T) {
	p := &Provider{cfg: &Config{MchID: "1900000109"}}
	cases := []struct {
		name  string
		order *payment.Order
		want  string
	}{
		{
			name: "单号太短",
			order: &payment.Order{
				OutTradeNo: "OT01",
				Subject:    "测试",
				Amount:     100,
			},
			want: "商户订单号",
		},
		{
			name: "单号含非法字符",
			order: &payment.Order{
				OutTradeNo: "OT.2026",
				Subject:    "测试",
				Amount:     100,
			},
			want: "商户订单号",
		},
		{
			name: "拼接后 description 超 127 字",
			order: &payment.Order{
				OutTradeNo: "OT20260001",
				Subject:    strings.Repeat("标", 120),
				Body:       strings.Repeat("描", 30),
				Amount:     100,
			},
			want: "127",
		},
		{
			name: "附加数据超 128 字",
			order: &payment.Order{
				OutTradeNo: "OT20260001",
				Subject:    "测试",
				Amount:     100,
				Attach:     strings.Repeat("a", 129),
			},
			want: "附加数据",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := p.Create(context.Background(), c.order, payment.TradeTypeNative)
			if err == nil {
				t.Fatal("应该报错却没报")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("报错里要提到 %q: %v", c.want, err)
			}
		})
	}
}

// TestBaseBodyDescription 拼出来的 description 就是发出去的那个字段。
func TestBaseBodyDescription(t *testing.T) {
	p := &Provider{cfg: &Config{MchID: "1900000109"}}
	body := p.baseBody(&payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "iPhone 17 Pro",
		Body:       "商品订单",
		Amount:     100,
	})
	if body.Description != "iPhone 17 Pro - 商品订单" {
		t.Fatalf("description 拼错了: %q", body.Description)
	}
}
