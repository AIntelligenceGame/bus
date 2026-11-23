package platform

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 渠道密钥静态加密(AES-256-GCM)。
//
// 目的:即使数据库被拖库、备份泄露、SQL 注入被读到,商户的私钥 / APIv3 密钥
// 也不会以明文形式躺在表里;解密只在服务端内存中进行,且需要主密钥。
//
// 注意边界:这解决的是「数据泄露」,不解决「服务器被完全攻陷」——
// 主密钥必须与数据库分开存放(环境变量 / KMS / 独立机器),否则攻陷服务器等于拿到全部密钥。
// 平台另外提供「敏感操作二次验证 + 审计日志」来降低单点失陷的影响面。
const (
	// masterKeyEnv 环境变量名:存放加密主密钥。
	masterKeyEnv = "PAY_PLATFORM_MASTER_KEY"
	// masterKeyFileEnv 环境变量名:存放主密钥文件路径。
	masterKeyFileEnv = "PAY_PLATFORM_MASTER_KEY_FILE"
	// defaultMasterKeyFile 默认主密钥文件路径。
	defaultMasterKeyFile = "./payment-data/master.key"
	// aad 附加认证数据:防止密文被跨用途搬运(渠道配置专用)。
	aad = "pay-platform/channel-config/v1"
)

// ErrSecretNotFound 主密钥不存在。
var ErrSecretNotFound = errors.New("platform: 未找到加密主密钥")

// SecretBox 渠道密钥加密盒。
type SecretBox struct {
	// key 32 字节主密钥。
	key []byte
}

// NewSecretBox 用主密钥构造加密盒。
//
// masterKey 可以是任意非空字符串;内部用 SHA-256 派生出 32 字节密钥。
// 推荐传入高熵随机串(见 LoadOrCreateMasterKey)。
func NewSecretBox(masterKey string) (*SecretBox, error) {
	if strings.TrimSpace(masterKey) == "" {
		return nil, fmt.Errorf("%w: 主密钥为空", ErrSecretNotFound)
	}
	sum := sha256.Sum256([]byte(masterKey))
	return &SecretBox{key: sum[:]}, nil
}

// Encrypt 加密明文,返回 base64 密文(含随机 nonce)。
func (b *SecretBox) Encrypt(plain []byte) (string, error) {
	return b.EncryptWithAAD(plain, aad)
}

// EncryptWithAAD 用指定的附加认证数据加密。
//
// 不同用途要用不同的 AAD:否则「渠道配置密文」和「验证码通道密文」可以互相
// 搬运 —— 拿 A 的密文去 B 的位置解,只要主密钥一样就能解出来。
func (b *SecretBox) EncryptWithAAD(plain []byte, aad string) (string, error) {
	if b == nil || len(b.key) == 0 {
		return "", fmt.Errorf("%w: 加密盒未初始化", ErrSecretNotFound)
	}
	gcm, err := b.gcm()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("platform: 生成随机数失败: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, plain, []byte(aad))
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt 解密密文。
func (b *SecretBox) Decrypt(encoded string) (string, error) {
	return b.DecryptWithAAD(encoded, aad)
}

// DecryptWithAAD 用指定的附加认证数据解密。
func (b *SecretBox) DecryptWithAAD(encoded string, aad string) (string, error) {
	if b == nil || len(b.key) == 0 {
		return "", fmt.Errorf("%w: 加密盒未初始化", ErrSecretNotFound)
	}
	// 用严格模式解码:拒绝「补位比特非零」的非规范 base64。
	// 标准模式会把末位分组里的补位比特当成不关心位,同一段密文可以有多种
	// base64 写法,那等于给了篡改者免费改写密文而不被发现的机会。
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("platform: 密文格式错误: %w", err)
	}
	gcm, err := b.gcm()
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("platform: 密文长度不足")
	}
	nonce, body := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	out, err := gcm.Open(nil, nonce, body, []byte(aad))
	if err != nil {
		return "", fmt.Errorf("platform: 解密失败(主密钥不匹配或数据被篡改): %w", err)
	}
	return string(out), nil
}

// gcm 创建 AES-GCM 实例。
func (b *SecretBox) gcm() (cipher.AEAD, error) {
	block, err := aes.NewCipher(b.key)
	if err != nil {
		return nil, fmt.Errorf("platform: 创建 AES 失败: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("platform: 创建 GCM 失败: %w", err)
	}
	return gcm, nil
}

// LoadOrCreateMasterKey 读取主密钥;不存在时生成一个随机密钥并写入文件(0600)。
//
// 优先级:
//
//  1. 环境变量 PAY_PLATFORM_MASTER_KEY
//  2. 环境变量 PAY_PLATFORM_MASTER_KEY_FILE 指向的文件
//  3. 默认文件 ./payment-data/master.key(不存在则生成)
func LoadOrCreateMasterKey() (string, string, error) {
	if key := strings.TrimSpace(os.Getenv(masterKeyEnv)); key != "" {
		return key, "环境变量 " + masterKeyEnv, nil
	}
	if path := strings.TrimSpace(os.Getenv(masterKeyFileEnv)); path != "" {
		key, err := readMasterKeyFile(path)
		if err != nil {
			return "", "", err
		}
		return key, "主密钥文件 " + path, nil
	}
	path := defaultMasterKeyFile
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", "", fmt.Errorf("platform: 创建主密钥目录失败: %w", err)
		}
	}
	key, err := readMasterKeyFile(path)
	if err != nil {
		return "", "", err
	}
	return key, "主密钥文件 " + path + "(首次自动生成,请妥善备份)", nil
}

// LoadOrCreateMasterKeyFile 读取指定路径的主密钥文件;不存在时生成一个随机密钥并写入(0600)。
//
// 给「示例程序 / 独立部署」用:主密钥文件路径必须**跟着数据目录走**,
// 不能用当前工作目录推导——换个目录启动就会生成一把新密钥,
// 之前加密入库的渠道密钥就全部解不开了。
//
// 环境变量优先级仍然高于本函数:PAY_PLATFORM_MASTER_KEY /
// PAY_PLATFORM_MASTER_KEY_FILE 由 LoadOrCreateMasterKey 统一处理,
// 调用方应在没有这两个环境变量时再用本函数。
func LoadOrCreateMasterKeyFile(path string) (string, string, error) {
	if strings.TrimSpace(path) == "" {
		return LoadOrCreateMasterKey()
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", "", fmt.Errorf("platform: 创建主密钥目录失败: %w", err)
		}
	}
	key, err := readMasterKeyFile(path)
	if err != nil {
		return "", "", err
	}
	return key, "主密钥文件 " + path + "(首次自动生成,请妥善备份)", nil
}

// readMasterKeyFile 读取主密钥文件,不存在时生成随机密钥并保存。
func readMasterKeyFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		key := strings.TrimSpace(string(data))
		if key == "" {
			return "", fmt.Errorf("%w: 主密钥文件为空: %s", ErrSecretNotFound, path)
		}
		return key, nil
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("platform: 读取主密钥文件失败: %w", err)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("platform: 生成主密钥失败: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(key+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("platform: 写入主密钥文件失败: %w", err)
	}
	return key, nil
}
