package wechat

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// notifyPayload 回调报文结构。
type notifyPayload struct {
	ID        string `json:"id"`
	EventType string `json:"event_type"`
	Resource  struct {
		Algorithm      string `json:"algorithm"`
		Ciphertext     string `json:"ciphertext"`
		AssociatedData string `json:"associated_data"`
		Nonce          string `json:"nonce"`
		OriginalType   string `json:"original_type"`
	} `json:"resource"`
}

// transactionResource 交易回调解密后的明文结构。
type transactionResource struct {
	AppID          string `json:"appid"`
	MchID          string `json:"mchid"`
	OutTradeNo     string `json:"out_trade_no"`
	TransactionID  string `json:"transaction_id"`
	TradeType      string `json:"trade_type"`
	TradeState     string `json:"trade_state"`
	TradeStateDesc string `json:"trade_state_desc"`
	BankType       string `json:"bank_type"`
	Attach         string `json:"attach"`
	SuccessTime    string `json:"success_time"`
	Payer          struct {
		OpenID string `json:"openid"`
	} `json:"payer"`
	Amount struct {
		Total         int64  `json:"total"`
		PayerTotal    int64  `json:"payer_total"`
		Currency      string `json:"currency"`
		PayerCurrency string `json:"payer_currency"`
	} `json:"amount"`
}

// ParseNotify 验签并解析微信异步回调(APIv3 回调格式)。
//
// 验签串:Wechatpay-Timestamp\nWechatpay-Nonce\nbody\n
// 解密:AES-256-GCM,密钥为 APIv3Key。
func (p *Provider) ParseNotify(ctx context.Context, req *payment.NotifyRequest) (*payment.Notify, error) {
	if req == nil {
		return nil, fmt.Errorf("wechat: nil notify request")
	}
	timestamp := req.Header("Wechatpay-Timestamp")
	nonce := req.Header("Wechatpay-Nonce")
	signature := req.Header("Wechatpay-Signature")
	serial := req.Header("Wechatpay-Serial")
	if timestamp == "" || nonce == "" || signature == "" || serial == "" {
		return nil, fmt.Errorf("%w: wechat notify missing signature headers", payment.ErrInvalidSignature)
	}

	// 平台证书会轮换:序列号不认识时自动刷新一次(限流,见 ensurePlatformCert)。
	cert, err := p.ensurePlatformCert(ctx, serial)
	if err != nil {
		return nil, err
	}
	rsaPub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("wechat: platform certificate %s is not rsa", serial)
	}
	message := timestamp + "\n" + nonce + "\n" + string(req.Body) + "\n"
	sigBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return nil, fmt.Errorf("%w: decode wechat signature: %v", payment.ErrInvalidSignature, err)
	}
	if err := payment.RSAVerifySHA256(rsaPub, []byte(message), sigBytes); err != nil {
		return nil, fmt.Errorf("%w: wechat notify verify failed", payment.ErrInvalidSignature)
	}

	var payload notifyPayload
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		return nil, fmt.Errorf("wechat: decode notify body: %w", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(payload.Resource.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("wechat: decode notify ciphertext: %w", err)
	}
	plain, err := payment.AESGCMDecrypt(
		[]byte(p.cfg.APIv3Key),
		[]byte(payload.Resource.Nonce),
		ciphertext,
		[]byte(payload.Resource.AssociatedData),
	)
	if err != nil {
		return nil, fmt.Errorf("wechat: decrypt notify resource: %w", err)
	}
	var res transactionResource
	if err := json.Unmarshal(plain, &res); err != nil {
		return nil, fmt.Errorf("wechat: decode notify resource: %w", err)
	}

	// 验签只能证明「报文没被篡改」,不能证明「这笔钱是付给这个商户的」。
	// 官方回调处理步骤里也有这一步:核对通知里的 appid / mchid 是不是自己的。
	// 少了它,别的商户号的交易通知也会被记到我们账上。
	if p.cfg != nil {
		if got := res.AppID; got != "" && got != p.cfg.AppID {
			return nil, fmt.Errorf("%w: wechat notify appid %s is not ours",
				payment.ErrInvalidSignature, got)
		}
		if got := res.MchID; got != "" && got != p.cfg.MchID {
			return nil, fmt.Errorf("%w: wechat notify mchid %s is not ours",
				payment.ErrInvalidSignature, got)
		}
	}

	// 关于「回调时间戳窗口」:这里**不**做硬性窗口校验,是刻意的。
	//
	// 微信的回调重试策略最长会拖到 24 小时之后(官方:15s/15s/30m/3h/6h/12h/24h),
	// 支付宝更久(官方:立即 3 次 + 4m/10m/10m/1h/2h/6h/15h,跨 24 小时 22 分)。
	// 卡一个时间窗口,渠道的**正常**重投反而会被拒,订单就永远停在「待支付」。
	// 真正的防线是幂等 + 状态机单调:重放一条旧通知,不会把已退款的订单改回成功。

	notify := &payment.Notify{
		Channel:       payment.ChannelWechat,
		OutTradeNo:    res.OutTradeNo,
		TransactionID: res.TransactionID,
		Amount:        res.Amount.Total,
		Currency:      res.Amount.Currency,
		Status:        mapTradeState(res.TradeState),
		Raw: map[string]string{
			"event_type":       payload.EventType,
			"trade_state":      res.TradeState,
			"trade_state_desc": res.TradeStateDesc,
			"bank_type":        res.BankType,
			"attach":           res.Attach,
			"appid":            res.AppID,
			"mchid":            res.MchID,
			"trade_type":       res.TradeType,
			"id":               payload.ID,
		},
	}
	if t, err := time.Parse(time.RFC3339, res.SuccessTime); err == nil {
		notify.EventTime = t
	}
	return notify, nil
}
