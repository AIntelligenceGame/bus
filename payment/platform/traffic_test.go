package platform

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// 这组测试盯的是「页面操作会不会把流量/资源打爆」这类问题。
//
// 每一条都对应一个真实缺陷(见注释),回归时不能删。

// newTrafficPlatform 建一个带限流器与缓存的测试平台。
func newTrafficPlatform(t *testing.T) (*Platform, *Store) {
	t.Helper()
	store := newTestStore(t)
	p := &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
		},
		store:         store,
		providers:     map[string]map[payment.Channel]payment.Provider{},
		loginLimiter:  newRateLimiter(),
		mfaLimiter:    newRateLimiter(),
		apiLimiter:    newRateLimiter(),
		exportLimiter: newRateLimiter(),
		qrcodeLimiter: newRateLimiter(),
		qrcodeCache:   newQRCodeCache(300),
	}
	return p, store
}

// seedOrders 造 n 笔订单。
func seedOrders(t *testing.T, store *Store, userID string, n int) {
	t.Helper()
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		rec := OrderRecord{
			OutTradeNo: fmt.Sprintf("OT202611200000%06d", i),
			UserID:     userID,
			Channel:    "wechat",
			TradeType:  "native",
			Subject:    "测试商品",
			Amount:     1990,
			AmountYuan: "19.90",
			Status:     string(payment.TradeStatusSuccess),
			// 带一份渠道回调原文:这是列表响应里最占体积的部分。
			Notify: &payment.Notify{
				Channel:    payment.ChannelWechat,
				OutTradeNo: fmt.Sprintf("OT202611200000%06d", i),
				Amount:     1990,
				Status:     payment.TradeStatusSuccess,
				Raw: map[string]string{
					"appid": "wx1234567890abcdef", "mch_id": "1600000000",
					"nonce_str": "abcdef1234567890abcdef1234567890",
					"sign":      "MD5HASHVALUE0123456789ABCDEF",
					"total_fee": "1990", "trade_type": "NATIVE",
					"trade_state": "SUCCESS", "result_code": "SUCCESS",
					"return_code": "SUCCESS", "openid": "oABCDEFGHIJKLMNOPQRSTUV",
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := store.AddOrder(rec); err != nil {
			t.Fatalf("造订单失败: %v", err)
		}
	}
}

// TestOrdersListDropsNotifyRaw 列表接口不该把渠道回调原文带出来。
//
// 缺陷:列表页不需要 notify_raw,但它一单就占好几百字节 ——
// 实测 200 条订单里光它就占 105 KB(整个响应的 60%)。
func TestOrdersListDropsNotifyRaw(t *testing.T) {
	_, store := newTrafficPlatform(t)
	seedOrders(t, store, "m1", 200)

	full, _, err := store.OrdersPage("m1", OrderFilter{Page: 1, Size: 200})
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	// 库里确实存了回调原文(否则这个测试就没意义了)。
	detail, err := store.OrderByTradeNo("m1", "OT202611200000000000")
	if err != nil {
		t.Fatalf("查订单详情失败: %v", err)
	}
	if detail == nil || detail.Notify == nil {
		t.Fatal("订单详情里应该有回调原文 —— 列表与详情读的不是同一份数据?")
	}
	// 列表里不该有。
	for _, o := range full {
		if o.Notify != nil {
			t.Fatal("列表接口不该返回渠道回调原文(notify)")
		}
	}
}

// TestOrdersListSizeStaysReasonable 列表响应体要有上限。
func TestOrdersListSizeStaysReasonable(t *testing.T) {
	_, store := newTrafficPlatform(t)
	seedOrders(t, store, "m1", 200)

	rows, _, err := store.OrdersPage("m1", OrderFilter{Page: 1, Size: 200})
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	body, _ := json.Marshal(map[string]any{"orders": rows})
	// 之前是 176 KB,现在应该掉到 100 KB 以内。
	if len(body) > 100*1024 {
		t.Fatalf("列表响应体仍然偏大: %d 字节", len(body))
	}
	t.Logf("列表响应体 = %d 字节", len(body))
}

// TestExportLimitIsActuallyReachable 导出的 5000 条上限必须真的生效。
//
// 缺陷:导出走的是 OrdersPage,而它的 size 会被 normalizePage 砍到 200 ——
// 于是「导出上限 5000」形同虚设,实际只能导出 200 条,而且**不报错**。
// 财务拿一份少了一半的账单去对账,比报错难查得多。
func TestExportLimitIsActuallyReachable(t *testing.T) {
	p, store := newTrafficPlatform(t)
	user := newTestUser(t, store, "m1")

	// 种满上限 + 1 条,直接批量插(逐条 AddOrder 太慢)。
	// 批量插的是最底层的模型 —— 这里要验的是「导出真的只出 5000 条」,
	// 绕开 AddOrder 是故意的:AddOrder 本身另有测试覆盖。
	now := formatTime(time.Now())
	models := make([]OrderModel, 0, maxExportRows+1)
	for i := 0; i < maxExportRows+1; i++ {
		models = append(models, OrderModel{
			OutTradeNo: fmt.Sprintf("BULK%08d", i),
			UserID:     user.UserID,
			Channel:    "wechat",
			TradeType:  "native",
			Subject:    "批量订单",
			Amount:     100,
			AmountYuan: "1.00",
			Status:     "SUCCESS",
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	if err := store.db.CreateInBatches(&models, 500).Error; err != nil {
		t.Fatalf("批量造订单失败: %v", err)
	}

	// 走真实的导出接口,解析 CSV,数数据行。
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/orders/export", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleOrdersExport(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("导出应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	reader := csv.NewReader(bytes.NewReader(stripBOM(w.Body.Bytes())))
	// 末尾那行「已达上限」的提示只有一列,和数据行的 10 列不一样长 ——
	// csv 默认要求每行字段数一致,这里放开。
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("解析 CSV 失败: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("CSV 至少要有表头")
	}
	// 表头 + 5000 条数据 + 1 行「已达上限」的提示。
	if len(rows) != maxExportRows+2 {
		t.Fatalf("导出行数不对: 期望 %d(表头+5000 条+1 行提示),实际 %d",
			maxExportRows+2, len(rows))
	}
	if !strings.Contains(rows[len(rows)-1][0], "上限") {
		t.Fatalf("最后一行应该是「已达上限」的提示,实际 %q", rows[len(rows)-1][0])
	}
	// 数据行数正好是 5000,不多不少。
	if got := len(rows) - 2; got != maxExportRows {
		t.Fatalf("数据行数不对: 期望 %d,实际 %d", maxExportRows, got)
	}
}

// stripBOM 去掉 UTF-8 BOM(Excel 用的),方便 csv.Reader 解析。
func stripBOM(b []byte) []byte {
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		return b[3:]
	}
	return b
}

// TestExportStreamMatchesAll 导出的流式结果要和一次性取的一致。
func TestExportStreamMatchesAll(t *testing.T) {
	_, store := newTrafficPlatform(t)
	seedOrders(t, store, "m1", 50)

	all, err := store.AllOrdersForExport("m1", OrderFilter{})
	if err != nil {
		t.Fatalf("导出失败: %v", err)
	}
	var streamed []OrderRecord
	err = store.StreamOrdersForExport("m1", OrderFilter{}, func(o OrderRecord) error {
		streamed = append(streamed, o)
		return nil
	})
	if err != nil {
		t.Fatalf("流式导出失败: %v", err)
	}
	if len(streamed) != len(all) {
		t.Fatalf("流式导出条数与一次性取不一致: %d vs %d", len(streamed), len(all))
	}
	for i := range all {
		if streamed[i].OutTradeNo != all[i].OutTradeNo {
			t.Fatalf("第 %d 条对不上: %s vs %s", i, streamed[i].OutTradeNo, all[i].OutTradeNo)
		}
	}
}

// TestExportRateLimited 导出必须限流(它是最贵的读操作)。
//
// 必须打真实的导出接口:限流键是 "export:"+用户 ID,直接调限流器的话
// 键得在测试里重写一遍,两边一改就各说各话,测不出任何东西。
func TestExportRateLimited(t *testing.T) {
	p, store := newTrafficPlatform(t)
	user := newTestUser(t, store, "m1")
	seedOrders(t, store, user.UserID, 5)

	allowed, blocked := 0, 0
	for i := 0; i < exportRateLimitPerUser+5; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/orders/export", nil)
		req.Header.Set("X-User-Token", user.AccessToken)
		p.handleOrdersExport(w, req)
		switch w.Code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			blocked++
			if !strings.Contains(w.Body.String(), "导出太频繁") {
				t.Fatalf("限流提示文案不对: %s", w.Body.String())
			}
		default:
			t.Fatalf("第 %d 次导出状态码不对: %d: %s", i, w.Code, w.Body.String())
		}
	}
	if allowed != exportRateLimitPerUser {
		t.Fatalf("限流放行数不对: 期望 %d,实际 %d", exportRateLimitPerUser, allowed)
	}
	if blocked != 5 {
		t.Fatalf("限流拦截数不对: 期望 5,实际 %d", blocked)
	}
}

// TestGzipCompressesLargeJSON 大 JSON 响应必须被压缩。
func TestGzipCompressesLargeJSON(t *testing.T) {
	p, store := newTrafficPlatform(t)
	seedOrders(t, store, "m1", 200)

	rows, _, err := store.OrdersPage("m1", OrderFilter{Page: 1, Size: 200})
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	payload, _ := json.Marshal(map[string]any{"orders": rows})

	handler := p.withGzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(payload)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("响应没有被压缩: Content-Encoding = %q", got)
	}
	// 头里不能还留着压缩前的长度(那会让客户端读错)。
	if cl := rec.Header().Get("Content-Length"); cl != "" {
		t.Fatalf("压缩后不该还带 Content-Length: %q", cl)
	}
	// 压缩后的大小要在读之前记下来(读 gzip 流会消耗掉 body)。
	compressedSize := rec.Body.Len()
	// 解压回来的内容必须和原来一模一样。
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("解压失败: %v", err)
	}
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("读解压内容失败: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("解压后的内容与原始内容不一致")
	}
	if compressedSize >= len(payload) {
		t.Fatalf("压缩后反而更大了: %d >= %d", compressedSize, len(payload))
	}
	t.Logf("原始 %d 字节 -> 压缩后 %d 字节", len(payload), compressedSize)
}

// TestGzipSkipsSmallResponses 小响应不压(压了反而变大)。
func TestGzipSkipsSmallResponses(t *testing.T) {
	p, _ := newTrafficPlatform(t)
	payload := []byte(`{"ok":true}`)

	handler := p.withGzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(payload)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("小响应不该被压缩: Content-Encoding = %q", got)
	}
	if rec.Body.String() != string(payload) {
		t.Fatalf("小响应内容被改坏了: %q", rec.Body.String())
	}
}

// TestGzipSkipsBinary 二进制(已压缩过的格式)不压。
func TestGzipSkipsBinary(t *testing.T) {
	p, _ := newTrafficPlatform(t)
	payload := bytes.Repeat([]byte{0x89, 0x50, 0x4E, 0x47}, 1000)

	handler := p.withGzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write(payload)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/qrcode", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("PNG 不该被压缩: Content-Encoding = %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), payload) {
		t.Fatal("PNG 内容被改坏了")
	}
}

// TestGzipNotAccepted 客户端不支持 gzip 时原样返回。
func TestGzipNotAccepted(t *testing.T) {
	p, _ := newTrafficPlatform(t)
	payload := bytes.Repeat([]byte("x"), 5000)

	handler := p.withGzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("不该压缩: Content-Encoding = %q", got)
	}
	if rec.Body.Len() != len(payload) {
		t.Fatalf("内容长度不对: %d vs %d", rec.Body.Len(), len(payload))
	}
}

// TestQRCodeCacheHits 二维码内容不变时不该重复渲染。
func TestQRCodeCacheHits(t *testing.T) {
	c := newQRCodeCache(300)
	c.put("k", []byte("png"))

	got, ok := c.get("k")
	if !ok {
		t.Fatal("应该命中缓存")
	}
	if string(got) != "png" {
		t.Fatalf("缓存内容不对: %q", got)
	}
	if _, ok := c.get("missing"); ok {
		t.Fatal("不该命中不存在的键")
	}
}

// TestQRCodeCacheEvicts 缓存满了要按 LRU 淘汰(不能无限涨)。
func TestQRCodeCacheEvicts(t *testing.T) {
	c := newQRCodeCache(300)
	for i := 0; i < qrcodeCacheMaxEntries+10; i++ {
		c.put(fmt.Sprintf("k%d", i), []byte("x"))
	}
	if c.order.Len() > qrcodeCacheMaxEntries {
		t.Fatalf("缓存没淘汰: %d 条", c.order.Len())
	}
	if len(c.entries) != c.order.Len() {
		t.Fatalf("索引和链表长度不一致: %d vs %d", len(c.entries), c.order.Len())
	}
}

// TestQRCodeCacheExpires 过期的条目要失效。
func TestQRCodeCacheExpires(t *testing.T) {
	c := newQRCodeCache(1)
	c.put("k", []byte("x"))

	// 把时间往前拨 10 秒,条目就该过期。
	orig := nowUnix
	nowUnix = func() int64 { return orig() + 10 }
	defer func() { nowUnix = orig }()

	if _, ok := c.get("k"); ok {
		t.Fatal("过期条目不该命中")
	}
}

// TestQRCodeRejectsTooLongContent 超长的二维码内容要拒掉。
//
// 不拒的话,超长输入会让编码器白烧 CPU,而且扫出来也没意义。
func TestQRCodeRejectsTooLongContent(t *testing.T) {
	p, _ := newTrafficPlatform(t)

	handler := p.handleQRCode
	req := httptest.NewRequest(http.MethodGet, "/api/qrcode?content="+strings.Repeat("a", 2000), nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("超长内容应该被拒: 状态码 %d", rec.Code)
	}
}

// TestCSVExportEscapesFormula CSV 注入防护要覆盖所有字符串列。
//
// 缺陷:之前只给「标题」加了转义,订单号/渠道流水号这些字段没管 ——
// 而订单号是**商户自己填的**,照样能被塞进 "=cmd|..." 让 Excel 执行公式。
//
// 必须走真实的导出接口:手写一份 CSV 再自己调 csvSafe,验的是「我抄的那份
// 转义逻辑」,生产代码少转义一列照样绿。而且导出有 10 列,少写一列就漏一列。
func TestCSVExportEscapesFormula(t *testing.T) {
	p, store := newTrafficPlatform(t)
	user := newTestUser(t, store, "m1")

	// 应用名也是商户自己填的,同样能被塞进公式 —— 导出里有一列「应用」。
	app, err := store.SaveApp(user.UserID, "", `=HYPERLINK("http://evil","app")`, "备注")
	if err != nil {
		t.Fatalf("造应用失败: %v", err)
	}

	now := time.Now().UTC()
	rec := OrderRecord{
		OutTradeNo:    `=cmd|'/c calc'!A1`,
		UserID:        user.UserID,
		AppID:         app.AppID,
		Channel:       "wechat",
		TradeType:     "native",
		Subject:       `=HYPERLINK("http://evil","click")`,
		Amount:        100,
		AmountYuan:    "1.00",
		Status:        "SUCCESS",
		TransactionID: `+1+1`,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.AddOrder(rec); err != nil {
		t.Fatalf("造订单失败: %v", err)
	}

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/orders/export", nil)
	req.Header.Set("X-User-Token", user.AccessToken)
	p.handleOrdersExport(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("导出应成功,实际 %d: %s", w.Code, w.Body.String())
	}
	rows, err := csv.NewReader(bytes.NewReader(stripBOM(w.Body.Bytes()))).ReadAll()
	if err != nil {
		t.Fatalf("解析 CSV 失败: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("行数不对: 期望 2(表头 + 1 条数据),实际 %d", len(rows))
	}
	data := rows[1]
	// 导出有 10 列,每一列都要查 —— 少查一列就等于没测那一列。
	if len(data) != 10 {
		t.Fatalf("数据列数不对: 期望 10,实际 %d: %q", len(data), data)
	}
	for i, cell := range data {
		if cell == "" {
			continue
		}
		switch cell[0] {
		case '=', '+', '-', '@':
			t.Fatalf("第 %d 列还是公式形态: %q", i, cell)
		}
	}
	// 明确点名那几列确实被转义了(而不是恰好没落到危险字符上)。
	if data[0] != `'=cmd|'/c calc'!A1` {
		t.Fatalf("订单号列没转义: %q", data[0])
	}
	if data[1] != `'=HYPERLINK("http://evil","app")` {
		t.Fatalf("应用名列没转义: %q", data[1])
	}
	if data[4] != `'=HYPERLINK("http://evil","click")` {
		t.Fatalf("标题列没转义: %q", data[4])
	}
	if data[7] != "'+1+1" {
		t.Fatalf("渠道流水号列没转义: %q", data[7])
	}
}

// TestEscapeLike 关键字里的 LIKE 通配符要被转义。
//
// 不转义的话,搜 "100%" 会匹配到 "1000"、"1001"…;
// 搜 "_" 会匹配任何单字符(等于全表扫)。
func TestEscapeLike(t *testing.T) {
	cases := map[string]string{
		"100%":  `100\%`,
		"a_b":   `a\_b`,
		`a\b`:   `a\\b`,
		"普通":    "普通",
		"a%b_c": `a\%b\_c`,
	}
	for in, want := range cases {
		if got := escapeLike(in); got != want {
			t.Fatalf("escapeLike(%q) = %q,期望 %q", in, got, want)
		}
	}
}

// TestEscapeLikeInQuery 转义后的关键字在查询里要能真的匹配到。
func TestEscapeLikeInQuery(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	for _, no := range []string{"100%OFF", "1000OFF", "1001OFF"} {
		if err := s.AddOrder(OrderRecord{
			OutTradeNo: no, UserID: "m1", Channel: "wechat", TradeType: "native",
			Subject: "s", Amount: 1, AmountYuan: "0.01", Status: "SUCCESS",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("造订单失败: %v", err)
		}
	}
	rows, _, err := s.OrdersPage("m1", OrderFilter{Page: 1, Size: 20, Keyword: "100%"})
	if err != nil {
		t.Fatalf("查订单失败: %v", err)
	}
	if len(rows) != 1 || rows[0].OutTradeNo != "100%OFF" {
		t.Fatalf("含 %% 的关键字匹配错了: %d 条", len(rows))
	}
}
