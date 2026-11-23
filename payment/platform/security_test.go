package platform

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// TestSecretBoxRoundTrip 加密解密闭环 + 错误主密钥 / 篡改数据都要失败。
func TestSecretBoxRoundTrip(t *testing.T) {
	box, err := NewSecretBox("master-key-for-test")
	if err != nil {
		t.Fatalf("NewSecretBox: %v", err)
	}
	plain := `{"private_key_pem":"-----BEGIN PRIVATE KEY-----","api_v3_key":"0123456789abcdef0123456789abcdef"}`
	cipherText, err := box.Encrypt([]byte(plain))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if strings.Contains(cipherText, "PRIVATE KEY") {
		t.Fatalf("密文里出现了明文: %s", cipherText)
	}
	got, err := box.Decrypt(cipherText)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if got != plain {
		t.Fatalf("解密结果不匹配:\n got=%s\nwant=%s", got, plain)
	}

	// 错误主密钥
	other, _ := NewSecretBox("another-master-key")
	if _, err := other.Decrypt(cipherText); err == nil {
		t.Fatalf("用错误的主密钥解密竟然成功了")
	}

	// 篡改数据:在原始密文字节上翻一个 bit(保证真的改动了内容)。
	raw, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		t.Fatalf("密文不是合法 base64: %v", err)
	}
	raw[len(raw)-1] ^= 0x01
	if _, err := box.Decrypt(base64.StdEncoding.EncodeToString(raw)); err == nil {
		t.Fatalf("篡改后的密文竟然解密成功了")
	}

	// 篡改 base64 文本中间的一个字符。
	//
	// 注意:翻转的是**base64 文本层**的字符,有可能翻出字母表
	// ('Z'→'['、'+'→',' 等),那样报错原因就成了「密文格式错误」而不是
	// 「密文被篡改」—— 断言只要求 err != nil,就会因为错误的理由通过。
	// 所以这里先确认改完仍然是合法 base64,再翻转原始字节。
	bad := []byte(cipherText)
	bad[len(bad)/2] ^= 0x01
	badRaw, err := base64.StdEncoding.DecodeString(string(bad))
	if err != nil {
		// 翻出了字母表:那就退回到原始字节层面翻,保证测的是「内容被改」。
		badRaw = make([]byte, len(raw))
		copy(badRaw, raw)
		badRaw[len(badRaw)/2] ^= 0x01
	}
	if _, err := box.Decrypt(base64.StdEncoding.EncodeToString(badRaw)); err == nil {
		t.Fatalf("篡改后的密文竟然解密成功了")
	}

	// 篡改 base64 末位数据字符:末位分组里可能有「不关心」的补位比特,
	// 严格模式必须把它当成非法编码直接拒绝,而不是当成没改过。
	bad = []byte(cipherText)
	bad[len(bad)-2] ^= 0x01
	if _, err := box.Decrypt(string(bad)); err == nil {
		t.Fatalf("篡改后的密文竟然解密成功了")
	}
}

// TestMaskSecrets 敏感字段必须被脱敏,非敏感字段保持原样。
func TestMaskSecrets(t *testing.T) {
	in := map[string]any{
		"app_id": "wx123",
		"wechat": map[string]any{
			"private_key_pem": "-----BEGIN PRIVATE KEY-----",
			"api_v3_key":      "0123456789abcdef0123456789abcdef",
			"mch_id":          "1900000109",
		},
	}
	out := maskSecrets(in)
	raw, _ := json.Marshal(out)
	if strings.Contains(string(raw), "BEGIN PRIVATE KEY") ||
		strings.Contains(string(raw), "0123456789abcdef") {
		t.Fatalf("敏感字段没有被脱敏: %s", raw)
	}
	if !strings.Contains(string(raw), "wx123") || !strings.Contains(string(raw), "1900000109") {
		t.Fatalf("非敏感字段被误改: %s", raw)
	}
	// 原入参不能被修改
	if !strings.Contains(in["wechat"].(map[string]any)["private_key_pem"].(string), "BEGIN PRIVATE KEY") {
		t.Fatalf("脱敏修改了原始配置")
	}
}

// TestMergeSecrets 新提交为空时保留已保存的密钥。
func TestMergeSecrets(t *testing.T) {
	existing := map[string]any{
		"app_id":          "wx-old",
		"private_key_pem": "OLD-KEY",
	}
	incoming := map[string]any{
		"app_id":          "wx-new",
		"private_key_pem": "",
	}
	merged := mergeSecrets(existing, incoming)
	if merged["private_key_pem"] != "OLD-KEY" {
		t.Fatalf("旧密钥没有保留: %v", merged["private_key_pem"])
	}
	if merged["app_id"] != "wx-new" {
		t.Fatalf("非敏感字段应使用新值: %v", merged["app_id"])
	}

	// 新提交有值时用新值
	incoming["private_key_pem"] = "NEW-KEY"
	merged = mergeSecrets(existing, incoming)
	if merged["private_key_pem"] != "NEW-KEY" {
		t.Fatalf("应使用新提交的密钥: %v", merged["private_key_pem"])
	}
}

// TestPasswordHash 口令散列可校验,错误口令不通过。
func TestPasswordHash(t *testing.T) {
	hash, err := hashPassword("correct-horse")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if strings.Contains(hash, "correct-horse") {
		t.Fatalf("散列里出现了明文口令")
	}
	if !verifyPassword(hash, "correct-horse") {
		t.Fatalf("正确口令校验失败")
	}
	if verifyPassword(hash, "wrong") {
		t.Fatalf("错误口令竟然校验通过")
	}
	if verifyPassword("", "anything") {
		t.Fatalf("空散列竟然校验通过")
	}
}

// TestMFACode 验证码是 6 位数字。
func TestMFACode(t *testing.T) {
	for i := 0; i < 20; i++ {
		code, err := generateMFACode()
		if err != nil {
			t.Fatalf("generateMFACode: %v", err)
		}
		if len(code) != 6 {
			t.Fatalf("验证码长度不对: %q", code)
		}
		for _, r := range code {
			if r < '0' || r > '9' {
				t.Fatalf("验证码不是数字: %q", code)
			}
		}
	}
}

// TestNormalizeMFAType 二次验证类型归一化。
func TestNormalizeMFAType(t *testing.T) {
	cases := map[string]string{
		"email":  MFATypeEmail,
		"EMAIL":  MFATypeEmail,
		"sms":    MFATypeSMS,
		"phone":  MFATypeSMS,
		"mobile": MFATypeSMS,
		"":       "",
		"weird":  "",
	}
	for in, want := range cases {
		if got := normalizeMFAType(in); got != want {
			t.Fatalf("normalizeMFAType(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMaskTarget 目标脱敏不泄露完整手机号/邮箱。
func TestMaskTarget(t *testing.T) {
	got := maskTarget("13800001111")
	if strings.Contains(got, "13800001111") {
		t.Fatalf("脱敏后仍包含完整目标: %s", got)
	}
	if !strings.Contains(got, "****") {
		t.Fatalf("脱敏结果里没有掩码: %s", got)
	}
}
