package wechat

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AIntelligenceGame/bus/payment"
)

// platformCert 按序列号获取平台证书。
func (p *Provider) platformCert(serial string) (*x509.Certificate, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cert, ok := p.platformCerts[serial]
	return cert, ok
}

// loadCachedCerts 从 CertDir 加载已缓存的平台证书。
func (p *Provider) loadCachedCerts() error {
	if p.cfg.CertDir == "" {
		return nil
	}
	entries, err := os.ReadDir(p.cfg.CertDir)
	if err != nil {
		// 目录不存在属于正常情况,等待自动下载。
		return nil
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".pem") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(p.cfg.CertDir, e.Name()))
		if err != nil {
			continue
		}
		block, _ := pem.Decode(data)
		if block == nil {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		p.mu.Lock()
		p.platformCerts[cert.SerialNumber.String()] = cert
		p.mu.Unlock()
	}
	return nil
}

// certificatesResponse 平台证书列表响应。
type certificatesResponse struct {
	Data []struct {
		SerialNo           string `json:"serial_no"`
		EffectiveTime      string `json:"effective_time"`
		ExpireTime         string `json:"expire_time"`
		EncryptCertificate struct {
			Algorithm      string `json:"algorithm"`
			Nonce          string `json:"nonce"`
			AssociatedData string `json:"associated_data"`
			Ciphertext     string `json:"ciphertext"`
		} `json:"encrypt_certificate"`
	} `json:"data"`
}

// fetchPlatformCerts 下载并解析微信平台证书(加密内容用 APIv3Key 解密)。
//
// 平台证书用于验证回调签名;下载内容用 APIv3Key 以 AES-256-GCM 解密。
func (p *Provider) fetchPlatformCerts(ctx context.Context) error {
	var resp certificatesResponse
	if err := p.do(ctx, http.MethodGet, "/v3/certificates", nil, nil, &resp); err != nil {
		return fmt.Errorf("wechat: fetch platform certs: %w", err)
	}
	// 本次**真的解析出来**的证书数。
	//
	// 不能只看 map 里还剩几张:目录里缓存的旧证书也算在里面,
	// 于是「APIv3 密钥填错 → 一张都解不开」会被当成刷新成功,
	// 回调那边只会看到一句没头没脑的「未知证书序列号」。
	parsed := 0
	for _, item := range resp.Data {
		ciphertext, err := base64.StdEncoding.DecodeString(item.EncryptCertificate.Ciphertext)
		if err != nil {
			continue
		}
		plain, err := payment.AESGCMDecrypt(
			[]byte(p.cfg.APIv3Key),
			[]byte(item.EncryptCertificate.Nonce),
			ciphertext,
			[]byte(item.EncryptCertificate.AssociatedData),
		)
		if err != nil {
			continue
		}
		block, _ := pem.Decode(plain)
		if block == nil {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		parsed++
		p.mu.Lock()
		p.platformCerts[item.SerialNo] = cert
		p.mu.Unlock()
		if p.cfg.CertDir != "" {
			// 目录可能还不存在(第一次用就没建过)。
			// 以前这里直接 WriteFile,目录不存在就报错,连累整个 Provider 建不起来。
			if err := saveCertFile(p.cfg.CertDir, item.SerialNo, plain); err != nil {
				return err
			}
		}
	}
	if parsed == 0 {
		return fmt.Errorf("wechat: 平台证书下载后一张都解不开" +
			"(请检查 APIv3 密钥是否正确;微信用 APIv3 密钥加密证书内容)")
	}
	return nil
}

// saveCertFile 把平台证书存到 CertDir;目录不存在时自动创建。
func saveCertFile(dir, serial string, pemBytes []byte) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("wechat: create cert dir: %w", err)
	}
	name := filepath.Join(dir, serial+".pem")
	if err := os.WriteFile(name, pemBytes, 0o600); err != nil {
		return fmt.Errorf("wechat: save platform cert: %w", err)
	}
	return nil
}

// FetchPlatformCerts 导出手动刷新平台证书(微信平台证书会定期轮换)。
func (p *Provider) FetchPlatformCerts(ctx context.Context) error {
	return p.fetchPlatformCerts(ctx)
}

// certRefreshWindow 自动刷新平台证书的最小间隔。
const certRefreshWindow = 10 * time.Minute

// ensurePlatformCert 取平台证书;序列号不认识时自动刷新一次再找。
//
// 微信的平台证书会定期轮换:证书换了而本地还是旧的,回调就验不过签,
// 订单状态永远停在「待支付」—— 商户的钱收了,平台却不知道。
//
// 自动刷新必须限流:回调是外部触发的,不限流的话伪造的回调
// (随便编一个证书序列号)能把平台打成一直去请求微信。
func (p *Provider) ensurePlatformCert(ctx context.Context, serial string) (*x509.Certificate, error) {
	cert, ok := p.platformCert(serial)
	if ok {
		return cert, nil
	}
	if !p.allowCertRefresh() {
		return nil, fmt.Errorf("%w: unknown wechat platform certificate %s",
			payment.ErrInvalidSignature, serial)
	}
	if err := p.fetchPlatformCerts(ctx); err != nil {
		return nil, fmt.Errorf("%w: unknown wechat platform certificate %s(自动刷新失败: %v)",
			payment.ErrInvalidSignature, serial, err)
	}
	// 刷新「成功」但仍然没有这张证书:说明微信给的不是这张(或者证书被换掉了)。
	// 这里不能写成「自动刷新失败: <nil>」—— 那句话会把人带偏。
	cert, ok = p.platformCert(serial)
	if !ok {
		return nil, fmt.Errorf("%w: unknown wechat platform certificate %s",
			payment.ErrInvalidSignature, serial)
	}
	return cert, nil
}

// allowCertRefresh 判断现在是否允许自动刷新平台证书(同一段时间内最多一次)。
func (p *Provider) allowCertRefresh() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.certFetchAt.IsZero() && time.Since(p.certFetchAt) < certRefreshWindow {
		return false
	}
	p.certFetchAt = time.Now()
	return true
}

// PlatformCerts 导出当前已加载的平台证书(序列号 -> PEM 内容)。
func (p *Provider) PlatformCerts() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]string, len(p.platformCerts))
	for serial, cert := range p.platformCerts {
		out[serial] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
	}
	return out
}
