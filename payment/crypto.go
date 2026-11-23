package payment

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
)

// cryptoHashSHA256 是 SHA256 摘要算法常量,供 crypto/rsa 使用。
const cryptoHashSHA256 = crypto.SHA256

// LoadRSAPrivateKey 从 PEM 解析 RSA 私钥,支持 PKCS#1 与 PKCS#8(微信/支付宝通用)。
func LoadRSAPrivateKey(pemData []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("%w: rsa private key is not valid pem", ErrInvalidConfig)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse rsa private key: %v", ErrInvalidConfig, err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: pem is not a rsa private key", ErrInvalidConfig)
	}
	return key, nil
}

// LoadRSAPublicKey 从 PEM 解析 RSA 公钥,支持 PKIX 与 PKCS#1。
func LoadRSAPublicKey(pemData []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("%w: rsa public key is not valid pem", ErrInvalidConfig)
	}
	if key, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse rsa public key: %v", ErrInvalidConfig, err)
	}
	key, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%w: pem is not a rsa public key", ErrInvalidConfig)
	}
	return key, nil
}

// RSASignSHA256 使用 RSA PKCS#1 v1.5 + SHA256 签名(微信 APIv3 / 支付宝 RSA2 同款算法)。
func RSASignSHA256(priv *rsa.PrivateKey, content []byte) ([]byte, error) {
	if priv == nil {
		return nil, fmt.Errorf("%w: nil rsa private key", ErrInvalidConfig)
	}
	sum := sha256.Sum256(content)
	return rsa.SignPKCS1v15(rand.Reader, priv, cryptoHashSHA256, sum[:])
}

// RSAVerifySHA256 校验 RSA SHA256 签名。
func RSAVerifySHA256(pub *rsa.PublicKey, content, sig []byte) error {
	if pub == nil {
		return fmt.Errorf("%w: nil rsa public key", ErrInvalidConfig)
	}
	sum := sha256.Sum256(content)
	return rsa.VerifyPKCS1v15(pub, cryptoHashSHA256, sum[:], sig)
}

// RSA2Sign 返回 Base64 编码的 RSA2 签名(支付宝常用)。
func RSA2Sign(priv *rsa.PrivateKey, content string) (string, error) {
	sig, err := RSASignSHA256(priv, []byte(content))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// RSA2Verify 校验 Base64 编码的 RSA2 签名。
func RSA2Verify(pub *rsa.PublicKey, content, signBase64 string) error {
	sig, err := base64.StdEncoding.DecodeString(signBase64)
	if err != nil {
		return fmt.Errorf("%w: decode signature: %v", ErrInvalidSignature, err)
	}
	if err := RSAVerifySHA256(pub, []byte(content), sig); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSignature, err)
	}
	return nil
}

// ECDSASignSHA256 返回 ASN.1 编码的 ECDSA-SHA256 签名(微信 APIv3 使用)。
func ECDSASignSHA256(priv *ecdsa.PrivateKey, data []byte) ([]byte, error) {
	if priv == nil {
		return nil, fmt.Errorf("%w: nil ecdsa private key", ErrInvalidConfig)
	}
	sum := sha256.Sum256(data)
	return ecdsa.SignASN1(rand.Reader, priv, sum[:])
}

// ECDSAVerifySHA256 校验 ASN.1 编码的 ECDSA-SHA256 签名。
func ECDSAVerifySHA256(pub *ecdsa.PublicKey, data, sig []byte) error {
	if pub == nil {
		return fmt.Errorf("%w: nil ecdsa public key", ErrInvalidConfig)
	}
	sum := sha256.Sum256(data)
	if !ecdsa.VerifyASN1(pub, sum[:], sig) {
		return fmt.Errorf("%w: ecdsa verify failed", ErrInvalidSignature)
	}
	return nil
}

// AESGCMEncrypt 使用 AES-256-GCM 加密(微信 APIv3 回调解密)。
func AESGCMEncrypt(key, nonce, plaintext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("payment: invalid gcm nonce size")
	}
	return gcm.Seal(nil, nonce, plaintext, aad), nil
}

// AESGCMDecrypt 使用 AES-256-GCM 解密。
func AESGCMDecrypt(key, nonce, ciphertext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("payment: invalid gcm nonce size")
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("payment: aes-gcm decrypt failed: %w", err)
	}
	return plain, nil
}

// RandomNonce 返回 n 字节随机串。
func RandomNonce(n int) ([]byte, error) {
	if n <= 0 {
		n = 16
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}
