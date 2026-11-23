package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/AIntelligenceGame/bus/payment/platform/openapi"
)

// boundApp 假平台里「这把密钥绑定的应用」。
const boundApp = "APP_IOS"

// recordedReq 记录下来的一个请求。
type recordedReq struct {
	Method string
	Path   string
	Body   map[string]any
	// Signed 签名头齐不齐。
	Signed bool
}

// fakePlatform 是一个「开放接口」的假平台。
//
// 它模拟平台真正会做的事:
//   - 密钥绑定了应用 → 不传 app_id 的订单自动归到那个应用;
//   - 传了别的 app_id → 400。
//
// 这正是示例要演示的行为,所以这里必须照着平台的行为写,不然测的就不是示例了。
type fakePlatform struct {
	mu       sync.Mutex
	requests []recordedReq
}

func (f *fakePlatform) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/open/pay", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := decodeBody(raw)
		appID, _ := body["app_id"].(string)
		if appID != "" && appID != boundApp {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "该密钥绑定应用 " + boundApp + ",不能用于应用 " + appID,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"order_no":   "BIZ20260101000001",
			"amount":     19900,
			"yuan":       "199.00",
			"notify_url": "https://pay.example.com/notify/wechat/u1/" + boundApp,
			"session":    map[string]any{"token": "tok_1"},
			"usage":      []string{"APP 端:把 session.app_params 原样传给微信 SDK。"},
		})
	})

	mux.HandleFunc("POST /api/open/order", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"out_trade_no": "BIZ20260101000001",
			"app_id":       boundApp,
			"channel":      "wechat",
			"amount":       19900,
			"status":       "PENDING",
		})
	})

	mux.HandleFunc("POST /api/open/query", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":         "PENDING",
			"transaction_id": "tx_1",
			"channel":        "wechat",
		})
	})

	mux.HandleFunc("GET /api/open/orders", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"orders": []map[string]any{{
				"out_trade_no": "BIZ20260101000001",
				"user_id":      "u1",
				"app_id":       boundApp,
				"channel":      "wechat",
				"trade_type":   "app",
				"subject":      "多产品示例订单",
				"amount":       19900,
				"amount_yuan":  "199.00",
				"status":       "PENDING",
			}},
			"page": 1, "size": 5, "total": 1, "pages": 1,
		})
	})

	mux.HandleFunc("GET /api/open/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"stats":    map[string]any{"order_count": 1, "total_amount": 19900},
			"group_by": "app",
			"groups": []map[string]any{{
				"key": boundApp, "label": "iOS 客户端",
				"order_count": 1, "total_amount": 19900,
			}},
		})
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 先把请求体读出来存档,再**放回去** ——
		// 不放回去的话,后面真正处理请求的 handler 读到的就是空体,
		// app_id 就丢了(测试会假绿)。
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			raw = nil
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		f.mu.Lock()
		f.requests = append(f.requests, recordedReq{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   decodeBody(raw),
			Signed: r.Header.Get("X-Pay-Signature") != "" &&
				r.Header.Get("X-Pay-Key") != "" &&
				r.Header.Get("X-Pay-Timestamp") != "" &&
				r.Header.Get("X-Pay-Nonce") != "",
		})
		f.mu.Unlock()
		mux.ServeHTTP(w, r)
	})
}

func (f *fakePlatform) payRequests() []recordedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recordedReq
	for _, r := range f.requests {
		if r.Path == "/api/open/pay" {
			out = append(out, r)
		}
	}
	return out
}

func decodeBody(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// TestRunExample 整个示例流程跑通。
func TestRunExample(t *testing.T) {
	fake := &fakePlatform{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	client := openapi.New(srv.URL, "pk_test", "sk_test")
	cfg := config{
		Base:    srv.URL,
		KeyID:   "pk_test",
		Secret:  "sk_test",
		Channel: "wechat",
		Amount:  19900,
	}

	var buf strings.Builder
	res, err := run(context.Background(), &buf, client, cfg)
	if err != nil {
		t.Fatalf("示例执行失败: %v\n输出:\n%s", err, buf.String())
	}

	if res.OrderNo != "BIZ20260101000001" {
		t.Fatalf("订单号不对: %s", res.OrderNo)
	}
	if res.OrderAppID != boundApp {
		t.Fatalf("订单归属应用不对: %q", res.OrderAppID)
	}
	if len(res.Groups) != 1 || res.Groups[0].Key != boundApp {
		t.Fatalf("分组统计不对: %+v", res.Groups)
	}

	// 关键:第一次下单**不带** app_id(靠密钥绑定自动归属)。
	pays := fake.payRequests()
	if len(pays) != 2 {
		t.Fatalf("应该有两次下单(正常 + 故意传错),实际 %d", len(pays))
	}
	// 第一次是故意传错应用的:平台要拒绝。
	if got, _ := pays[0].Body["app_id"].(string); got != "APP_NOT_THE_BOUND_ONE" {
		t.Fatalf("第一次下单的 app_id 不对: %q", got)
	}
	// 第二次是正常下单:**不该**带 app_id(靠密钥绑定自动归属)。
	if _, ok := pays[1].Body["app_id"]; ok {
		t.Fatalf("正常下单不该带 app_id: %+v", pays[1].Body)
	}
	if !pays[1].Signed {
		t.Fatal("请求没带签名头")
	}
	if res.MismatchErr == "" {
		t.Fatal("传错应用时平台应该报错,示例却没拿到错误")
	}
	if !strings.Contains(res.MismatchErr, "不能用于应用") {
		t.Fatalf("错误信息不对: %s", res.MismatchErr)
	}

	// 输出要能看明白。
	out := buf.String()
	for _, want := range []string{"不传 app_id", "按应用分组统计", "被平台拒绝"} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出里少了 %q:\n%s", want, out)
		}
	}
}

// TestConfigMissingEnv 缺密钥时要说得清楚缺哪个。
func TestConfigMissingEnv(t *testing.T) {
	cfg := config{}
	missing := cfg.missingEnv()
	if len(missing) != 2 {
		t.Fatalf("应该报缺两个环境变量: %v", missing)
	}
	if missing[0] != "PAY_KEY_ID" || missing[1] != "PAY_KEY_SECRET" {
		t.Fatalf("报错的变量名不对: %v", missing)
	}
	cfg.KeyID = "pk"
	cfg.Secret = "sk"
	if got := cfg.missingEnv(); len(got) != 0 {
		t.Fatalf("配齐了不该再报错: %v", got)
	}
}
