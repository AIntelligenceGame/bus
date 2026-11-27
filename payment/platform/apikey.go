package platform

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// 开放接口密钥(API Key)。
//
// 为什么需要它:控制台里的所有敏感操作都要「浏览器会话 + 口令/验证码票据」,
// 那是给人用的。商户自己的**业务后端**要下单、查单、退款,不可能弹浏览器弹窗,
// 所以必须另开一条机器对机器的通道:商户拿一把密钥,对请求签名,平台验签后放行。
//
// 安全设计:
//   - 密钥**只在创建时显示一次**,之后库里只存 AES-256-GCM 密文;
//   - 请求用 HMAC-SHA256 签名,密钥本身不出现在请求里(只出现在签名计算中);
//   - 签名带时间戳窗口与随机串,防止重放;
//   - 权限按 scope 划分:密钥**不能**改渠道配置、不能查看密钥明文,
//     那两件事仍然只允许在控制台里做(口令 + 二次验证);

// 开放接口的请求头。
const (
	// apiKeyHeaderKey 密钥 ID(公开标识,形如 pk_xxx)。
	apiKeyHeaderKey = "X-Pay-Key"
	// apiKeyHeaderSignature 请求签名(hex,小写)。
	apiKeyHeaderSignature = "X-Pay-Signature"
	// apiKeyHeaderTimestamp Unix 秒。
	apiKeyHeaderTimestamp = "X-Pay-Timestamp"
	// apiKeyHeaderNonce 随机串(同一次请求内不重复)。
	apiKeyHeaderNonce = "X-Pay-Nonce"
)

// 开放接口的权限(scope)。
const (
	// apiKeyScopePay 下单 / 查单 / 关单 / 订单与统计查询。
	apiKeyScopePay = "pay"
	// apiKeyScopeRefund 退款(资金操作,单独授权)。
	apiKeyScopeRefund = "refund"
	// apiKeyScopeWallet 钱包渠道扣款(Apple Pay / Google Pay 的最后一步)。
	apiKeyScopeWallet = "wallet"
)

const (
	// apiKeyTimeWindow 允许的时间戳偏差(秒)。超过就判定为重放或时钟漂移过大。
	apiKeyTimeWindow = 300
	// apiKeyMaxBodySize 参与签名的请求体上限(1MB)。
	apiKeyMaxBodySize = 1 << 20
	// apiKeyDefaultTTL 密钥默认有效期(不做期限设置)。
	apiKeyDefaultTTL = 0
)

// APIKeyRecord 一把开放接口密钥。
type APIKeyRecord struct {
	// KeyID 密钥 ID(公开,可放进请求头)。
	KeyID string `json:"key_id"`
	// UserID 所属商户。
	UserID string `json:"user_id"`
	// Name 备注名。
	Name string `json:"name"`
	// AppID 绑定的应用(空 = 不绑定)。
	//
	// 绑定了应用时,用这把密钥下单会自动落到该应用(不用再传 app_id),
	// 并且只能用该应用自己的渠道配置;请求里传了别的 app_id 会被拒绝。
	AppID string `json:"app_id"`
	// Scopes 权限列表。
	Scopes []string `json:"scopes"`
	// Enabled 是否启用。
	Enabled bool `json:"enabled"`
	// ExpiresAt 过期时间,零值表示不过期。
	ExpiresAt time.Time `json:"expires_at"`
	// LastUsedAt 最近一次使用时间。
	LastUsedAt time.Time `json:"last_used_at"`
	// CreatedAt 创建时间。
	CreatedAt time.Time `json:"created_at"`
}

// HasScope 判断密钥是否拥有某个权限。
func (k *APIKeyRecord) HasScope(scope string) bool {
	if k == nil {
		return false
	}
	for _, s := range k.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// normalizeScopes 规整权限列表:去重、过滤非法值。
func normalizeScopes(raw []string) []string {
	allowed := map[string]bool{
		apiKeyScopePay:    true,
		apiKeyScopeRefund: true,
		apiKeyScopeWallet: true,
	}
	out := make([]string, 0, len(raw))
	seen := map[string]bool{}
	for _, s := range raw {
		s = strings.ToLower(strings.TrimSpace(s))
		if !allowed[s] || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// CreateAPIKey 为商户创建一把开放接口密钥。
//
// 返回的 secret 明文**只在这一刻存在**,之后库里只保存密文。
//
// 这把密钥不绑定应用:下单时走全局渠道配置,订单算「未分类」。
// 想让密钥固定服务于某个产品,用 CreateAPIKeyForApp。
func (s *Store) CreateAPIKey(userID, name string, scopes []string, ttl time.Duration) (*APIKeyRecord, string, error) {
	return s.CreateAPIKeyForApp(userID, name, scopes, "", ttl)
}

// CreateAPIKeyForApp 创建一把**绑定到指定应用**的开放接口密钥。
//
// appID 为空 = 不绑定(行为和 CreateAPIKey 一样)。
//
// 绑定的意义:商户的业务后端只有一把密钥,但它要同时服务好几个产品。
// 不绑定的话,客户端忘了传 app_id 就会**静默**落到全局渠道配置上,
// 订单全进了「未分类」,报表对不上,而且当场没有任何报错。
// 绑定之后 app_id 变成「密钥的一部分」——不用传就不会传错,
// 传错了(传了别的应用)平台会直接拒绝。
//
// 这是在**同一个服务进程内**实现的,不需要为每个产品单独起一套服务端。
func (s *Store) CreateAPIKeyForApp(userID, name string, scopes []string, appID string, ttl time.Duration) (*APIKeyRecord, string, error) {
	if userID == "" {
		return nil, "", fmt.Errorf("platform: user id is required")
	}
	appID = strings.TrimSpace(appID)
	if appID != "" {
		app, err := s.AppByID(userID, appID)
		if err != nil {
			return nil, "", fmt.Errorf("platform: 查询应用失败: %w", err)
		}
		if app == nil {
			return nil, "", fmt.Errorf("platform: 应用不存在: %s", appID)
		}
	}
	list := normalizeScopes(scopes)
	if len(list) == 0 {
		list = []string{apiKeyScopePay}
	}
	keyID := "pk_" + randomHex(12)
	secret := "sk_" + randomBase64(32)
	enc, err := s.box.Encrypt([]byte(secret))
	if err != nil {
		return nil, "", fmt.Errorf("platform: encrypt api key: %w", err)
	}
	now := time.Now()
	expires := ""
	if ttl > 0 {
		expires = formatTime(now.Add(ttl))
	}
	model := APIKeyModel{
		KeyID:      keyID,
		UserID:     userID,
		Name:       name,
		AppID:      appID,
		SecretEnc:  enc,
		Scopes:     strings.Join(list, ","),
		Enabled:    1,
		ExpiresAt:  expires,
		LastUsedAt: "",
		CreatedAt:  formatTime(now),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return nil, "", fmt.Errorf("platform: insert api key: %w", err)
	}
	return &APIKeyRecord{
		KeyID:     model.KeyID,
		UserID:    userID,
		Name:      name,
		AppID:     appID,
		Scopes:    list,
		Enabled:   true,
		ExpiresAt: parseNanoTime(expires),
		CreatedAt: parseNanoTime(model.CreatedAt),
	}, secret, nil
}

// auditAppLabel 把应用 ID 变成审计日志里好读的样子(空 = 未绑定)。
func auditAppLabel(appID string) string {
	if strings.TrimSpace(appID) == "" {
		return "未绑定"
	}
	return appID
}

// APIKeys 返回某商户的全部密钥(不含密钥明文)。
func (s *Store) APIKeys(userID string) ([]APIKeyRecord, error) {
	var rows []APIKeyModel
	if err := s.db.Where("user_id = ?", userID).Order("created_at DESC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: query api keys: %w", err)
	}
	out := make([]APIKeyRecord, 0, len(rows))
	for i := range rows {
		out = append(out, apiKeyFromModel(&rows[i]))
	}
	return out, nil
}

// RevokeAPIKey 吊销一把密钥(只能吊销自己的)。
func (s *Store) RevokeAPIKey(userID, keyID string) error {
	res := s.db.Model(&APIKeyModel{}).
		Where("user_id = ? AND key_id = ?", userID, keyID).
		Update("enabled", int32(0))
	if res.Error != nil {
		return fmt.Errorf("platform: revoke api key: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("platform: 密钥不存在")
	}
	return nil
}

// apiKeySecret 按密钥 ID 取出密钥明文(解密)。
//
// 找不到、已吊销、已过期都返回错误。
func (s *Store) apiKeySecret(keyID string) (*APIKeyRecord, string, error) {
	if keyID == "" {
		return nil, "", fmt.Errorf("platform: 缺少密钥 ID")
	}
	var model APIKeyModel
	err := s.db.Where("key_id = ?", keyID).First(&model).Error
	if err != nil {
		return nil, "", fmt.Errorf("platform: 密钥不存在")
	}
	if model.Enabled == 0 {
		return nil, "", fmt.Errorf("platform: 密钥已被吊销")
	}
	if model.ExpiresAt != "" {
		exp, perr := parseTime(model.ExpiresAt)
		if perr != nil || time.Now().After(exp) {
			return nil, "", fmt.Errorf("platform: 密钥已过期")
		}
	}
	secret, derr := s.box.Decrypt(model.SecretEnc)
	if derr != nil {
		return nil, "", fmt.Errorf("platform: 解密密钥失败: %w", derr)
	}
	rec := apiKeyFromModel(&model)
	return &rec, secret, nil
}

// touchAPIKey 记录密钥最近一次使用时间(失败不影响主流程)。
func (s *Store) touchAPIKey(keyID string) {
	_ = s.db.Model(&APIKeyModel{}).Where("key_id = ?", keyID).
		Update("last_used_at", formatTime(time.Now())).Error
}

// apiKeyFromModel 把数据模型转成对外结构。
func apiKeyFromModel(m *APIKeyModel) APIKeyRecord {
	scopes := []string{}
	for _, s := range strings.Split(m.Scopes, ",") {
		if s = strings.TrimSpace(s); s != "" {
			scopes = append(scopes, s)
		}
	}
	return APIKeyRecord{
		KeyID:      m.KeyID,
		UserID:     m.UserID,
		Name:       m.Name,
		AppID:      m.AppID,
		Scopes:     scopes,
		Enabled:    m.Enabled != 0,
		ExpiresAt:  parseNanoTime(m.ExpiresAt),
		LastUsedAt: parseNanoTime(m.LastUsedAt),
		CreatedAt:  parseNanoTime(m.CreatedAt),
	}
}

// ---------------------------------------------------------------- 签名

// canonicalString 拼接待签名字符串。
//
// 参与签名的内容:方法、请求目标(路径 + 查询串)、时间戳、随机串、请求体摘要。
// 把请求体也纳入签名,是为了防止「换了 body 复用签名」的攻击。
// 把查询串纳入签名,是为了防止「改一下分页参数就多看别人的订单」。
//
//	<METHOD>\n<TARGET>\n<TIMESTAMP>\n<NONCE>\n<sha256(body) hex>
//
// TARGET 的算法:路径后面接 "?" + 原始查询串(没有查询串时只有路径)。
func canonicalString(method, target, timestamp, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	return strings.Join([]string{
		strings.ToUpper(method),
		target,
		timestamp,
		nonce,
		hex.EncodeToString(sum[:]),
	}, "\n")
}

// signTarget 取参与签名的请求目标。
//
// path + ("?" + RawQuery)。客户端必须用**完全相同**的算法计算,
// 否则验签失败——这是刻意的:多一种约定就多一种实现出错的可能。
func signTarget(r *http.Request) string {
	path := r.URL.Path
	if r.URL.RawQuery == "" {
		return path
	}
	return path + "?" + r.URL.RawQuery
}

// signRequest 计算请求签名(hex,小写)。
func signRequest(secret, method, target, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(canonicalString(method, target, timestamp, nonce, body)))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifySignature 校验签名(常量时间比较)。
func verifySignature(secret, method, target, timestamp, nonce string, body []byte, got string) bool {
	want := signRequest(secret, method, target, timestamp, nonce, body)
	return subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(strings.TrimSpace(got)))) == 1
}

// nonceCache 重放防护:记住时间窗口内用过的随机串。
//
// 零值可用(内部懒建),所以 Platform 不需要额外初始化。
type nonceCache struct {
	mu   sync.Mutex
	seen *expirable.LRU[string, struct{}]
}

// nonceCacheMaxEntries 缓存的随机串上限。
//
// 上限必须存在:随机串是**调用方给的**,理论上可以无限多。
// 没有上限的话,攻击者拿着合法密钥疯狂换随机串,内存就被撑爆了。
//
// 取 100000:正常商户 5 分钟窗口内的请求量远小于这个数,
// 就算被撑满,超限的也只是「最久没用」的那些,窗口内的重放防护基本不受影响。
const nonceCacheMaxEntries = 100000

// allow 记住一个随机串;时间窗口内重复出现返回 false(即判定为重放)。
//
// ttl 只在第一次调用时生效(缓存建好后 TTL 是固定的)。
// 调用方目前只传 apiKeyTimeWindow,不存在「同一缓存用不同窗口」的情况。
func (c *nonceCache) allow(key string, ttl time.Duration) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		// expirable.LRU 自带 TTL 过期 + LRU 淘汰,查询是 O(1)。
		//
		// 以前这里是「map + 每次请求全表扫一遍删过期项」:
		// 高频调用时表越滚越大,单次请求的代价是 O(表大小),
		// 总代价 O(请求数 × 表大小) —— 一次 DDoS 就能把 CPU 打满。
		//
		// 注意:expirable.LRU 的清理协程**永不退出**(库的限制),
		// 但整个进程只建一个 Platform,也只建一个缓存,泄漏可以忽略。
		c.seen = expirable.NewLRU[string, struct{}](nonceCacheMaxEntries, nil, ttl)
	}
	// Get 对已过期的条目返回 false(等同于「没见过」),正好符合语义:
	// 窗口外的随机串可以再次使用,窗口内的必须被拒。
	if _, ok := c.seen.Get(key); ok {
		return false
	}
	c.seen.Add(key, struct{}{})
	return true
}

// ---------------------------------------------------------------- 鉴权

// Principal 一次请求的认证主体。
type Principal struct {
	// User 认证对应的商户。
	User *User
	// Key 开放接口密钥(控制台会话时为 nil)。
	Key *APIKeyRecord
	// Method 认证方式:session / apikey。
	Method string
}

// HasScope 判断当前主体是否拥有某个权限。
//
// 控制台会话拥有全部权限(它已经过了口令 + 二次验证)。
func (p *Principal) HasScope(scope string) bool {
	if p == nil || p.User == nil {
		return false
	}
	if p.Key == nil {
		return true
	}
	return p.Key.HasScope(scope)
}

// authenticateAPIKey 用开放接口密钥认证请求。
//
// body 是**原始请求体**:签名覆盖了请求体,验签必须在解析之前完成。
func (p *Platform) authenticateAPIKey(r *http.Request, body []byte) (*Principal, error) {
	keyID := strings.TrimSpace(r.Header.Get(apiKeyHeaderKey))
	if keyID == "" {
		return nil, fmt.Errorf("缺少密钥头 %s", apiKeyHeaderKey)
	}
	timestamp := strings.TrimSpace(r.Header.Get(apiKeyHeaderTimestamp))
	if timestamp == "" {
		return nil, fmt.Errorf("缺少时间戳头 %s", apiKeyHeaderTimestamp)
	}
	nonce := strings.TrimSpace(r.Header.Get(apiKeyHeaderNonce))
	if nonce == "" {
		return nil, fmt.Errorf("缺少随机串头 %s", apiKeyHeaderNonce)
	}
	signature := strings.TrimSpace(r.Header.Get(apiKeyHeaderSignature))
	if signature == "" {
		return nil, fmt.Errorf("缺少签名头 %s", apiKeyHeaderSignature)
	}
	sec, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("时间戳格式错误")
	}
	skew := time.Since(time.Unix(sec, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > apiKeyTimeWindow*time.Second {
		return nil, fmt.Errorf("请求时间戳超出允许范围(±%d 秒)", apiKeyTimeWindow)
	}
	rec, secret, err := p.store.apiKeySecret(keyID)
	if err != nil {
		return nil, err
	}
	// 先验签,再查重放。
	//
	// 顺序很重要:如果先记随机串,任何拿到密钥 ID 的人(不需要知道密钥本身)
	// 都能用伪造的签名把随机串「用掉」,让真实请求被误判成重放。
	// 先验签就只让**持有密钥**的调用方消耗随机串。
	if !verifySignature(secret, r.Method, signTarget(r), timestamp, nonce, body, signature) {
		return nil, fmt.Errorf("签名校验失败")
	}
	if !p.nonces.allow(keyID+":"+nonce, apiKeyTimeWindow*time.Second) {
		return nil, fmt.Errorf("检测到重复的请求随机串(可能是重放)")
	}
	user, err := p.store.UserByID(rec.UserID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("密钥所属商户不存在")
	}
	// 商户被平台禁用后,它的密钥也必须立刻失效。
	if user.Disabled {
		return nil, fmt.Errorf("该商户已被平台禁用")
	}
	p.store.touchAPIKey(keyID)
	return &Principal{User: user, Key: rec, Method: "apikey"}, nil
}

// requireScope 校验当前主体是否有某个权限。
func (p *Platform) requireScope(principal *Principal, scope string) error {
	if principal == nil || principal.User == nil {
		return fmt.Errorf("未认证")
	}
	if !principal.HasScope(scope) {
		return fmt.Errorf("密钥缺少 %s 权限", scope)
	}
	return nil
}

// ---------------------------------------------------------------- 随机串

// randomHex 返回 n 字节的随机十六进制串。
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败是不可恢复的:宁可返回不可用也不要退化成弱随机。
		return ""
	}
	return hex.EncodeToString(buf)
}

// randomBase64 返回 n 字节随机串的 base64url 编码。
func randomBase64(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
