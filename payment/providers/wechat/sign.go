package wechat

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// authorization 构造 APIv3 的 Authorization 请求头(WECHATPAY2-SHA256-RSA2048)。
//
// 签名串:HTTPMethod\nURL(含 query)\nTimestamp\nNonce\nBody\n
// 签名算法:SHA256withRSA,结果 Base64 后放入 signature。
func (p *Provider) authorization(method, requestURL string, body []byte) (string, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce, err := payment.RandomNonce(16)
	if err != nil {
		return "", err
	}
	nonceStr := fmt.Sprintf("%x", nonce)

	msg := buildAuthMessage(method, requestURL, timestamp, nonceStr, string(body))
	sig, err := payment.RSASignSHA256(p.privKey, []byte(msg))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`WECHATPAY2-SHA256-RSA2048 mchid="%s",nonce_str="%s",timestamp="%s",serial_no="%s",signature="%s"`,
		p.cfg.MchID, nonceStr, timestamp, p.cfg.SerialNo, base64Std(sig),
	), nil
}

// buildAuthMessage 拼接待签名字符串。
func buildAuthMessage(method, requestURL, timestamp, nonce, body string) string {
	return method + "\n" + requestURL + "\n" + timestamp + "\n" + nonce + "\n" + body + "\n"
}

// signClientPayload 生成客户端调起支付所需的签名。
//
// 微信 APP 支付与 JSAPI 支付签名串均为:appid\ntimestamp\nnoncestr\nprepayid\n
func signClientPayload(priv *rsa.PrivateKey, payload string) (string, error) {
	sig, err := payment.RSASignSHA256(priv, []byte(payload))
	if err != nil {
		return "", err
	}
	return base64Std(sig), nil
}

// base64Std Base64 编码(标准字母表)。
func base64Std(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// do 发送一次 APIv3 请求:签名、发送、解析响应。
//
// out 为 nil 时忽略响应体。
func (p *Provider) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var raw []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("wechat: marshal request body: %w", err)
		}
		raw = b
	}

	requestURL := path
	if len(query) > 0 {
		requestURL = path + "?" + query.Encode()
	}
	fullURL := strings.TrimSuffix(p.cfg.BaseURL, "/") + requestURL

	auth, err := p.authorization(method, requestURL, raw)
	if err != nil {
		return fmt.Errorf("wechat: build authorization: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("wechat: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", auth)
	req.Header.Set("User-Agent", "bus-payment-wechat/1.0")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("wechat: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("wechat: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return p.apiError(resp.StatusCode, respBody)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("wechat: decode response: %w", err)
	}
	return nil
}

// apiError 把微信错误响应转换为 Go error。
func (p *Provider) apiError(status int, body []byte) error {
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &e); err == nil && e.Code != "" {
		switch e.Code {
		case "RESOURCE_NOT_EXISTS", "ORDER_NOT_EXIST":
			return fmt.Errorf("%w: wechat %s: %s", payment.ErrOrderNotFound, e.Code, e.Message)
		case "ORDERPAID", "ORDER_CLOSED":
			return fmt.Errorf("%w: wechat %s: %s", payment.ErrOrderClosed, e.Code, e.Message)
		}
		return fmt.Errorf("wechat: api error status=%d code=%s message=%s", status, e.Code, e.Message)
	}
	return fmt.Errorf("wechat: api error status=%d body=%s", status, string(body))
}
