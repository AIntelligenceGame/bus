package pay

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfigAcceptsPaymentPrefix 配置文件套一层 "payment:" 也要能读出来。
//
// config.example.yaml 就是这个写法,而且用法注释里写的是
// LoadConfigFile("./payment/config.yaml") —— 之前按根节点解析,
// 结果一个渠道都没读到,运行时报的是 "channel not registered",
// 完全看不出是配置结构不对。
func TestLoadConfigAcceptsPaymentPrefix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `
payment:
  alipay:
    app_id: "2021000000000000"
    private_key_pem: "-----BEGIN RSA PRIVATE KEY-----\nxxx\n-----END RSA PRIVATE KEY-----"
    alipay_public_key_pem: "-----BEGIN PUBLIC KEY-----\nyyy\n-----END PUBLIC KEY-----"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置文件失败: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("载入配置失败: %v", err)
	}
	if cfg.Alipay == nil {
		t.Fatal("payment: 前缀下没读到 alipay 配置")
	}
	if cfg.Alipay.AppID != "2021000000000000" {
		t.Fatalf("alipay app_id 不对: %q", cfg.Alipay.AppID)
	}
}

// TestLoadConfigAcceptsRootLevel 根节点直接写渠道(不带 payment: 前缀)照常能用。
func TestLoadConfigAcceptsRootLevel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `
alipay:
  app_id: "2021000000000001"
  private_key_pem: "priv"
  alipay_public_key_pem: "pub"
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置文件失败: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("载入配置失败: %v", err)
	}
	if cfg.Alipay == nil {
		t.Fatal("根节点下没读到 alipay 配置")
	}
	if cfg.Alipay.AppID != "2021000000000001" {
		t.Fatalf("alipay app_id 不对: %q", cfg.Alipay.AppID)
	}
}

// TestLoadConfigExampleFile 仓库里的 config.example.yaml 必须能被 LoadConfig 读出来。
//
// 之前这个文件按文档的用法加载是「读了个寂寞」,这里钉死住。
func TestLoadConfigExampleFile(t *testing.T) {
	path := filepath.Join("..", "config.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("找不到示例配置文件: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("载入示例配置失败: %v", err)
	}
	if cfg.Wechat == nil {
		t.Fatal("示例配置里没读到 wechat")
	}
	if cfg.Alipay == nil {
		t.Fatal("示例配置里没读到 alipay")
	}
	if cfg.ApplePay == nil {
		t.Fatal("示例配置里没读到 applepay")
	}
	if cfg.GooglePay == nil {
		t.Fatal("示例配置里没读到 googlepay")
	}
}
