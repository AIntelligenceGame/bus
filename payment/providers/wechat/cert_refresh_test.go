package wechat

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestFetchPlatformCertsFailsWhenDecryptFails APIv3 密钥不对时要报错。
//
// 微信把平台证书用商户的 APIv3 密钥加密下发,解不开就一张证书都拿不到。
// 之前的实现只检查「map 里还剩几张」—— 目录里缓存的旧证书也算数,
// 于是「密钥填错」会被当成刷新成功,回调那边只剩一句没头没脑的
// 「未知证书序列号」,商户根本想不到是 APIv3 密钥的问题。
func TestFetchPlatformCertsFailsWhenDecryptFails(t *testing.T) {
	// 假微信服务器:返回一份「解不开」的证书内容。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v3/certificates") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body := map[string]any{
			"data": []map[string]any{{
				"serial_no":      "SERIAL1",
				"effective_time": "2026-01-01T00:00:00+08:00",
				"expire_time":    "2027-01-01T00:00:00+08:00",
				"encrypt_certificate": map[string]any{
					"algorithm":       "AEAD_AES_256_GCM",
					"nonce":           "0123456789ab",
					"associated_data": "certificate",
					// 解不开的内容(密钥不对时就是这样)。
					"ciphertext": base64.StdEncoding.EncodeToString([]byte("garbage-ciphertext")),
				},
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	privPEM, privKey := testPrivateKeyPEM(t)
	p := &Provider{
		privKey: privKey,
		client:  &http.Client{Timeout: 10 * time.Second},
		cfg: &Config{
			AppID:         "wx1234567890",
			MchID:         "1900000109",
			SerialNo:      "SERIAL1",
			PrivateKeyPEM: privPEM,
			APIv3Key:      "0123456789abcdef0123456789abcdef",
			BaseURL:       srv.URL,
			CertDir:       t.TempDir(),
		},
		platformCerts: map[string]*x509.Certificate{},
	}

	err := p.fetchPlatformCerts(context.Background())
	if err == nil {
		t.Fatal("一张证书都解不开,应该报错")
	}
	if !strings.Contains(err.Error(), "APIv3") {
		t.Fatalf("报错没提到 APIv3 密钥: %v", err)
	}
}

// TestFetchPlatformCertsFailsEvenWithCachedCerts 缓存里还有旧证书时,
// 解密全失败也必须报错(别被缓存骗了)。
func TestFetchPlatformCertsFailsEvenWithCachedCerts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{
			"data": []map[string]any{{
				"serial_no": "SERIAL2",
				"encrypt_certificate": map[string]any{
					"algorithm":       "AEAD_AES_256_GCM",
					"nonce":           "0123456789ab",
					"associated_data": "certificate",
					"ciphertext":      base64.StdEncoding.EncodeToString([]byte("garbage")),
				},
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	privPEM, _ := testPrivateKeyPEM(t)
	old, _ := testPlatformCert(t)
	p := &Provider{
		cfg: &Config{
			AppID:         "wx1234567890",
			MchID:         "1900000109",
			SerialNo:      "SERIAL1",
			PrivateKeyPEM: privPEM,
			APIv3Key:      "0123456789abcdef0123456789abcdef",
			BaseURL:       srv.URL,
			CertDir:       t.TempDir(),
		},
		platformCerts: map[string]*x509.Certificate{old.SerialNumber.String(): old},
	}

	if err := p.fetchPlatformCerts(context.Background()); err == nil {
		t.Fatal("本次一张都没解出来,不该因为缓存里有旧证书就当成功")
	}
}
