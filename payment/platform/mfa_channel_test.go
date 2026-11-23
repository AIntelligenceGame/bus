package platform

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// fakeSMTP 一个最小的 SMTP 服务器(明文模式)。
//
// 用来证明「页面里填了 SMTP 参数 → 验证码真的能发出去」这条链路是通的,
// 而不是只测到「配置存进了数据库」。
type fakeSMTP struct {
	ln       net.Listener
	messages chan string
}

// newFakeSMTP 起一个本地 SMTP 服务器,返回地址与收到的邮件正文。
func newFakeSMTP(t *testing.T) (host string, port int, messages chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	msgs := make(chan string, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				w := bufio.NewWriter(c)
				_, _ = w.WriteString("220 fake ESMTP\r\n")
				_ = w.Flush()
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					cmd := strings.ToUpper(strings.TrimSpace(line))
					switch {
					case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
						_, _ = w.WriteString("250 fake\r\n")
					case strings.HasPrefix(cmd, "MAIL FROM"), strings.HasPrefix(cmd, "RCPT TO"):
						_, _ = w.WriteString("250 OK\r\n")
					case cmd == "DATA":
						_, _ = w.WriteString("354 go ahead\r\n")
						_ = w.Flush()
						var body strings.Builder
						for {
							dl, err := r.ReadString('\n')
							if err != nil {
								return
							}
							if strings.TrimRight(dl, "\r\n") == "." {
								break
							}
							body.WriteString(dl)
						}
						_, _ = w.WriteString("250 queued\r\n")
						select {
						case msgs <- body.String():
						default:
						}
					case cmd == "QUIT":
						_, _ = w.WriteString("221 bye\r\n")
						_ = w.Flush()
						return
					default:
						_, _ = w.WriteString("250 OK\r\n")
					}
					_ = w.Flush()
				}
			}(conn)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	t.Cleanup(func() { _ = ln.Close() })
	return "127.0.0.1", addr.Port, msgs
}

// TestSMTPChannelReallySends 配了 SMTP 通道 → 验证码真的发得出去。
func TestSMTPChannelReallySends(t *testing.T) {
	host, port, messages := newFakeSMTP(t)

	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户SMTP", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户SMTP")

	// 用真实接口保存通道配置(走页面走的那条路)。
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleSaveMFAChannel, "/api/account/mfa/channels", token, verifyToken,
		map[string]any{
			"channel": "email",
			"config": map[string]any{
				"host": host, "port": port,
				"from_addr": "noreply@example.com",
				"tls_mode":  "none",
			},
		})
	if code != http.StatusOK {
		t.Fatalf("保存 SMTP 通道失败: %d %v", code, resp)
	}

	// 发一个绑定验证码:应当真的送到 SMTP 服务器。
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "email", "target": "user@example.com", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusOK {
		t.Fatalf("发送绑定验证码失败: %d %v", code, resp)
	}
	if resp["delivered"] != true {
		t.Fatalf("配好通道后应当真的发出去: %v", resp)
	}
	select {
	case raw := <-messages:
		header, body := mailRaw(t, raw)
		if !strings.Contains(header, "user@example.com") {
			t.Fatalf("邮件里没有收件人: %q", header)
		}
		// 主题是 RFC 2047 编码的(中文主题不编码在部分客户端里会乱码)。
		if subject := mailSubject(t, header); subject != "支付平台验证码" {
			t.Fatalf("邮件主题异常: %q", subject)
		}
		text := mailBody(t, header, body)
		if !strings.Contains(text, "验证码") {
			t.Fatalf("正文里没有验证码: %q", text)
		}
		// 验证码得真的在正文里(6 位数字),不能只是「有个验证码字样」。
		if !codePattern.MatchString(text) {
			t.Fatalf("正文里没有验证码: %q", text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SMTP 服务器没有收到邮件")
	}
}

// TestSMSGatewayReallySends 短信网关:字段名可配,网关报错要如实返回。
func TestSMSGatewayReallySends(t *testing.T) {
	// 网关请求体在 httptest 的 goroutine 里写、测试 goroutine 里读 ——
	// 用 channel 传回来,别跨 goroutine 共享一个 map。
	bodies := make(chan map[string]string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got := map[string]string{}
		_ = json.Unmarshal(raw, &got)
		select {
		case bodies <- got:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"OK"}`))
	}))
	defer srv.Close()

	p, _ := newTestPlatform(t)
	code, resp := login(t, p, "商户短信", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	token := resp["token"].(string)
	user, _ := p.store.UserByName("商户短信")

	if err := p.store.saveMFAChannel(user.UserID, mfaChannelSMS, map[string]any{
		"url":          srv.URL,
		"code_field":   "smsCode",
		"target_field": "phone",
	}); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "sms", "target": "13800138000", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusOK {
		t.Fatalf("发送短信验证码失败: %d %v", code, resp)
	}
	if resp["delivered"] != true {
		t.Fatalf("配好网关后应当真的发出去: %v", resp)
	}
	var gotBody map[string]string
	select {
	case gotBody = <-bodies:
	case <-time.After(5 * time.Second):
		t.Fatal("网关没有收到请求")
	}
	if gotBody["smsCode"] == "" {
		t.Fatalf("网关没有收到验证码字段: %v", gotBody)
	}
	if gotBody["phone"] != "13800138000" {
		t.Fatalf("网关没有收到接收地址: %v", gotBody)
	}

	// 网关返回失败(很多网关失败也返回 200,错误码在 body 里):
	// 必须如实报错,否则页面会显示「测试发送成功」而短信根本没发出去。
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":1001,"msg":"余额不足"}`))
	}))
	defer bad.Close()
	if err := p.store.saveMFAChannel(user.UserID, mfaChannelSMS, map[string]any{
		"url": bad.URL,
	}); err != nil {
		t.Fatal(err)
	}
	code, resp = mfaPost(t, p.handleSendMFACode, "/api/account/mfa/code", token, "",
		map[string]any{
			"type": "sms", "target": "13800138000", "purpose": "bind", "password": "supersecret",
		})
	if code != http.StatusBadGateway {
		t.Fatalf("网关报错时应当如实失败,实际 %d: %v", code, resp)
	}
	msg, _ := resp["error"].(string)
	if !strings.Contains(msg, "余额不足") {
		t.Fatalf("错误提示里应当带上网关的原因: %q", msg)
	}
}

// TestLogOnlyPlatformNeedsOption 平台默认只有日志通道:不是「有通道」。
func TestLogOnlyPlatformNeedsOption(t *testing.T) {
	p := &Platform{
		opts: Options{
			Logger:    log.New(io.Discard, "", 0),
			VerifyTTL: 10 * time.Minute,
		},
		store:        newTestStore(t),
		providers:    map[string]map[payment.Channel]payment.Provider{},
		loginLimiter: newRateLimiter(),
		mfaLimiter:   newRateLimiter(),
	}
	code, resp := login(t, p, "商户默认通道", "supersecret")
	if code != http.StatusOK {
		t.Fatalf("首次登录失败: %d %v", code, resp)
	}
	// 必须用**真实存在的用户 ID**:之前查的是 "someone" 这个根本不存在的用户,
	// 于是只覆盖了「这个用户没有任何通道配置」这一条路径,
	// 漏掉了「用户自己配了 SMTP → ready 应该是 true」那条。
	user, err := p.store.UserByName("商户默认通道")
	if err != nil || user == nil {
		t.Fatalf("查用户失败: %v", err)
	}
	ready, err := p.mfaChannelReady(user.UserID, mfaChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("默认的日志通道不该算「有通道」")
	}
	// 显式打开之后才算。
	p.opts.AllowLogOnlyMFA = true
	ready, err = p.mfaChannelReady(user.UserID, mfaChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("AllowLogOnlyMFA 打开后应当算「有通道」")
	}

	// 用户自己配了 SMTP → 不管 AllowLogOnlyMFA 开没开,都应该算「有通道」。
	p.opts.AllowLogOnlyMFA = false
	if err := p.store.saveMFAChannel(user.UserID, mfaChannelEmail, map[string]any{
		"host": "smtp.example.com", "port": 587,
		"username": "u", "password": "p", "from_addr": "a@example.com",
	}); err != nil {
		t.Fatal(err)
	}
	ready, err = p.mfaChannelReady(user.UserID, mfaChannelEmail)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("自己配了 SMTP 之后应当算「有通道」")
	}
}

// stripWhitespace 去掉 base64 正文里的折行空白(RFC 2045 折行后,解码前要先合并)。
func stripWhitespace(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\r', '\n':
			return -1
		}
		return r
	}, s)
}

// mailRaw 拆一封邮件:头部与正文(第一个空行分隔)。
func mailRaw(t *testing.T, raw string) (header, body string) {
	t.Helper()
	parts := strings.SplitN(raw, "\r\n\r\n", 2)
	if len(parts) != 2 {
		t.Fatalf("邮件结构异常: %q", raw)
	}
	return parts[0], parts[1]
}

// mailHeaderValue 取头部里的某个字段(大小写不敏感)。
//
// 先把折行的头部展开:像 "Content-Type: multipart/alternative;\r\n boundary=xxx"
// 这种续行,不展开就只拿到 "multipart/alternative;",参数全丢了。
func mailHeaderValue(header, field string) string {
	for _, line := range strings.Split(unfoldHeader(header), "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), strings.ToLower(field)+":") {
			return strings.TrimSpace(line[len(field)+1:])
		}
	}
	return ""
}

// unfoldHeader 展开邮件头部的折行(续行以空格 / 制表符开头)。
func unfoldHeader(header string) string {
	lines := strings.Split(header, "\r\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if len(out) > 0 && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
			out[len(out)-1] += " " + strings.TrimSpace(line)
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\r\n")
}

// mailSubject 解出可读的主题。
//
// RFC 2047 有两种编码(base64 / quoted-printable),这里两种都认 ——
// 测试不该绑死邮件库选了哪一种。
func mailSubject(t *testing.T, header string) string {
	t.Helper()
	raw := mailHeaderValue(header, "Subject")
	if raw == "" {
		t.Fatalf("邮件没有主题: %q", header)
	}
	out, err := new(mime.WordDecoder).DecodeHeader(raw)
	if err != nil {
		t.Fatalf("主题解码失败(%v): %q", err, raw)
	}
	return out
}

// mailBody 按 Content-Transfer-Encoding 解出正文。
//
// 多段正文(multipart/alternative:纯文本 + HTML)要逐段解开再拼起来:
// 只看顶层的 Content-Transfer-Encoding,中文会留在 =E9=AA=8C 这种转义形态里,
// 断言「正文里含验证码」就会莫名其妙地失败。
func mailBody(t *testing.T, header, body string) string {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(mailHeaderValue(header, "Content-Type"))
	if err == nil && strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		if boundary == "" {
			t.Fatalf("multipart 邮件缺 boundary: %q", header)
		}
		var out strings.Builder
		mr := multipart.NewReader(strings.NewReader(body), boundary)
		for {
			part, nextErr := mr.NextPart()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				t.Fatalf("解析 multipart 正文失败: %v", nextErr)
			}
			// mime/multipart 会对 quoted-printable / base64 分段透明解码。
			raw, readErr := io.ReadAll(part)
			if readErr != nil {
				t.Fatalf("读取 multipart 分段失败: %v", readErr)
			}
			out.Write(raw)
			out.WriteString("\n")
		}
		return out.String()
	}
	enc := strings.ToLower(mailHeaderValue(header, "Content-Transfer-Encoding"))
	switch {
	case strings.Contains(enc, "base64"):
		raw, err := base64.StdEncoding.DecodeString(stripWhitespace(body))
		if err != nil {
			t.Fatalf("正文不是合法的 base64: %v", err)
		}
		return string(raw)
	case strings.Contains(enc, "quoted-printable"):
		raw, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(body)))
		if err != nil {
			t.Fatalf("正文不是合法的 quoted-printable: %v", err)
		}
		return string(raw)
	default:
		return body
	}
}
