package wechat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AIntelligenceGame/bus/payment"
)

// TestCreateRejectsForeignCurrency 非人民币的订单必须在**发请求之前**被拒。
//
// 官方(申请下单接口 amount.currency):「CNY:人民币,境内商户号仅支持人民币」。
// 跨境(境外收单中心 api.wxpayglobal.com)才支持 ISO 4217 全量币种 ——
// 本渠道走的是境内接口,所以只能是人民币。
//
// 这条拦在本地是因为:传过去微信只会回一句含糊的 PARAM_ERROR,
// 商户根本不知道是「币种不对」,定位成本全推给调用方。
func TestCreateRejectsForeignCurrency(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code_url":"weixin://wxpay/bizpayurl?pr=xxx"}`))
	}))
	defer srv.Close()

	p := &Provider{
		cfg: &Config{
			AppID: "wx123", MchID: "1900000109", SerialNo: "SERIAL1",
			BaseURL: srv.URL,
		},
		client: srv.Client(),
	}
	order := &payment.Order{
		OutTradeNo: "ORDER-CUR-1",
		Subject:    "测试订单",
		Amount:     100,
		Currency:   "USD",
	}
	_, err := p.Create(context.Background(), order, payment.TradeTypeNative)
	if err == nil {
		t.Fatal("非人民币的微信订单应该被拒")
	}
	if called {
		t.Fatal("不该真的发出请求")
	}
}

// TestCreateSendsCNY 下单请求里的 currency 必须是 CNY。
func TestCreateSendsCNY(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code_url":"weixin://wxpay/bizpayurl?pr=xxx"}`))
	}))
	defer srv.Close()

	_, key := testPrivateKeyPEM(t)
	p := &Provider{
		cfg: &Config{
			AppID: "wx123", MchID: "1900000109", SerialNo: "SERIAL1",
			BaseURL: srv.URL,
		},
		client:  srv.Client(),
		privKey: key,
	}
	order := &payment.Order{
		OutTradeNo: "ORDER-CUR-2",
		Subject:    "测试订单",
		Amount:     100,
	}
	if _, err := p.Create(context.Background(), order, payment.TradeTypeNative); err != nil {
		t.Fatalf("人民币订单不该被拒: %v", err)
	}
	amount, _ := body["amount"].(map[string]any)
	if amount == nil {
		t.Fatalf("请求体里没有 amount: %v", body)
	}
	if amount["currency"] != "CNY" {
		t.Fatalf("下单的币种不对: %v", amount["currency"])
	}
}
