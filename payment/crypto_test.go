package payment

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
)

func TestRSA2SignVerify(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	content := "app_id=2021&biz_content={}"
	sign, err := RSA2Sign(key, content)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if err := RSA2Verify(&key.PublicKey, content, sign); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := RSA2Verify(&key.PublicKey, content+"x", sign); err == nil {
		t.Fatal("verify should fail on modified content")
	}
}

func TestAESGCMRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	nonce := []byte("123456789012")
	plain := []byte(`{"out_trade_no":"ORDER001"}`)

	ciphertext, err := AESGCMEncrypt(key, nonce, plain, []byte("aad"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := AESGCMDecrypt(key, nonce, ciphertext, []byte("aad"))
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(got) != string(plain) {
		t.Fatalf("decrypt = %q, want %q", string(got), string(plain))
	}
	if _, err := AESGCMDecrypt(key, nonce, ciphertext, []byte("other")); err == nil {
		t.Fatal("decrypt should fail with wrong aad")
	}
}
