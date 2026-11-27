package alipay

import (
	"context"
	"strings"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// 商户附加数据的字段映射:attach 必须落在 passback_params 上。
//
// 之前 attach 被塞进了 extend_params.sys_service_provider_id ——
// 那是「服务商自己的 PID」(用来算返佣),不是商户的业务数据:
//   - 官方要求该值对应已签约的返佣/协作关系,填错返佣算不出来且**不可申诉**;
//   - 商户的业务数据根本不会原样回传,而 Order.Attach 的语义恰恰是「原样回传」。
//
// 官方给商户自定义透传数据用的字段是 passback_params(必 UrlEncoded)。

// TestBizCommonAttachUsesPassbackParams 商户附加数据走 passback_params。
func TestBizCommonAttachUsesPassbackParams(t *testing.T) {
	p := &Provider{cfg: &Config{AppID: "2021000000000001"}}
	biz := p.bizCommon(&payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "测试",
		Amount:     100,
		Attach:     "shop-1001",
	})
	if got, _ := biz["passback_params"].(string); got != "shop-1001" {
		t.Fatalf("passback_params 不对: %q", got)
	}
	// 绝对不能再落进 extend_params.sys_service_provider_id。
	ext, ok := biz["extend_params"].(map[string]string)
	if ok {
		if v, has := ext["sys_service_provider_id"]; has {
			t.Fatalf("附加数据被错误地映射到了 sys_service_provider_id=%q:"+
				"那是服务商返佣用的 PID,填错返佣不可申诉", v)
		}
	}
}

// TestBizCommonAttachEncoded passback_params 必须 UrlEncoded。
//
// 官方明确要求:不编码会报「订单信息无法识别」;
// 值里带双引号(JSON 格式)会报「订单参数异常」。
func TestBizCommonAttachEncoded(t *testing.T) {
	p := &Provider{cfg: &Config{AppID: "2021000000000001"}}
	biz := p.bizCommon(&payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "测试",
		Amount:     100,
		Attach:     `{"shop":"1001"}`,
	})
	got, _ := biz["passback_params"].(string)
	if strings.ContainsAny(got, `{}"`) {
		t.Fatalf("passback_params 必须 UrlEncoded,不该还有 JSON 的括号与引号: %q", got)
	}
}

// TestBizCommonExtendParamsNotClobbered Extra 里显式给的 extend_params 不该被覆盖。
//
// 之前 attach 与 Extra["extend_params"] 抢同一个字段,后者直接整个替换前者,
// 商户给的 extend_params 与 attach 只能活一个。
func TestBizCommonExtendParamsNotClobbered(t *testing.T) {
	p := &Provider{cfg: &Config{AppID: "2021000000000001"}}
	biz := p.bizCommon(&payment.Order{
		OutTradeNo: "OT20260001",
		Subject:    "测试",
		Amount:     100,
		Attach:     "shop-1001",
		Extra: map[string]string{
			"extend_params": `{"sys_service_provider_id":"2088123456789012"}`,
		},
	})
	ext, ok := biz["extend_params"].(map[string]string)
	if !ok {
		t.Fatalf("extend_params 不在请求里: %+v", biz)
	}
	if ext["sys_service_provider_id"] != "2088123456789012" {
		t.Fatalf("extend_params 被覆盖了: %+v", ext)
	}
	// attach 也要在 —— 两者不该再抢同一个字段。
	if got, _ := biz["passback_params"].(string); got != "shop-1001" {
		t.Fatalf("passback_params 不对: %q", got)
	}
}

// TestCreateRejectsInvalidFields 违反官方约束的订单,必须在下单前就报错。
func TestCreateRejectsInvalidFields(t *testing.T) {
	p := &Provider{cfg: &Config{AppID: "2021000000000001"}}
	cases := []struct {
		name  string
		order *payment.Order
		want  string
	}{
		{
			name: "单号超 64 位",
			order: &payment.Order{
				OutTradeNo: strings.Repeat("A", 65),
				Subject:    "测试",
				Amount:     100,
			},
			want: "商户订单号",
		},
		{
			name: "标题超 256 字",
			order: &payment.Order{
				OutTradeNo: "OT20260001",
				Subject:    strings.Repeat("标", 257),
				Amount:     100,
			},
			want: "订单标题",
		},
		{
			name: "描述超 128 字",
			order: &payment.Order{
				OutTradeNo: "OT20260001",
				Subject:    "测试",
				Body:       strings.Repeat("描", 129),
				Amount:     100,
			},
			want: "订单描述",
		},
		{
			// 支付宝境内交易接口没有币种字段,total_amount 的单位就是「元」。
			// 传 USD 会被当成人民币扣款 —— 金额差 7 倍还不会有任何报错,
			// 所以必须在本地就拦下来。
			name: "非人民币币种",
			order: &payment.Order{
				OutTradeNo: "OT20260001",
				Subject:    "测试",
				Amount:     100,
				Currency:   "USD",
			},
			want: "币种",
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
