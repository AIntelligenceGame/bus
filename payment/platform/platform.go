// Package platform 提供一个多用户的「支付配置选择平台」。
//
// 每个用户(商户)在自己的控制台里:
//   - 填写并保存自己的支付渠道密钥(微信 / 支付宝 / Apple Pay / Google Pay),
//     渠道配置保存在数据库(SQLite 默认,可切换 MySQL);
//   - 启用 / 停用某个渠道,并指定默认渠道;
//   - 在「支付体验」页发起扫码支付或移动端拉起支付,查看二维码与客户端参数;
//   - 在「订单」页查看订单与回调记录。
//
// 控制台前端是独立的 Vite + TypeScript 工程(bus/payment-web),
// 打包产物位于 web/dist/,由本包通过 //go:embed 打进二进制。
//
// 平台同时是一个可嵌入的 HTTP 服务:
//
//	platform, err := platform.New(platform.Options{
//	    Driver: "sqlite",
//	    DSN:    "./payment-data/pay.db",
//	    Addr:   ":8080",
//	    PublicBaseURL: "https://pay.example.com",
//	})
//	log.Fatal(platform.Start())
//
// 也可以挂到已有服务:
//
//	http.Handle("/payment/", platform.Handler())
//
// 访问控制说明:控制台使用简单的用户令牌(X-User-Token 头或 token 参数),
// 适合内部工具;对外生产环境请在网关层再叠加自己的鉴权。
package platform

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/AIntelligenceGame/bus/payment"
	"github.com/AIntelligenceGame/bus/payment/providers/alipay"
	"github.com/AIntelligenceGame/bus/payment/providers/applepay"
	"github.com/AIntelligenceGame/bus/payment/providers/gateway"
	"github.com/AIntelligenceGame/bus/payment/providers/googlepay"
	"github.com/AIntelligenceGame/bus/payment/providers/wechat"
)

// Options 平台配置。
type Options struct {
	// DB 已打开的数据库连接(GORM);为空时使用 Driver + DSN 自动打开。
	DB *gorm.DB
	// Driver 数据库驱动:sqlite(默认) / mysql。
	Driver string
	// DSN 数据源名称。SQLite 传文件路径,MySQL 传 user:pass@tcp(host:3306)/db。
	DSN string
	// Dialect SQL 方言,留空时按 Driver 推断。
	Dialect string
	// Addr 监听地址,默认 :8080。
	Addr string
	// AdminToken 平台管理员令牌,用于 /api/admin/* 平台管理接口。
	//
	// 留空表示**不启用**平台管理接口(请求会返回 404)。
	//
	// 注意:它**只**管平台管理接口,不再影响商户控制台——
	// 早先的实现是「设了这个令牌,整个控制台都要带它」,结果商户自己的
	// 控制台反而谁都进不去。控制台一律用商户令牌。
	AdminToken string
	// RegistrationCode 新商户注册码,留空表示允许自助注册。
	//
	// 首次登录会按名称自动建账户:不设注册码时,任何人都可以抢先注册
	// 别人想用的商户名称(账号抢注)。设置注册码后,首次登录必须带上它。
	RegistrationCode string
	// DisableSelfRegistration 关闭「首次登录自动建账户」。
	//
	// 开启后,没注册过的商户名**不能**靠登录建号,必须由平台管理员通过
	// POST /api/admin/merchants 建号 —— 谁是商户由平台说了算,
	// 不会出现「有人抢先把你的商户名注册了」的情况。
	//
	// 需要配合 AdminToken 使用(不然谁也建不了号)。
	DisableSelfRegistration bool
	// PublicBaseURL 对外访问地址,用于生成回调地址。
	//
	// 这是**平台级默认值**:所有商户没在控制台里自己填「回调地址」时用它。
	// 商户可以在控制台(渠道配置页)里填自己的对外地址来覆盖它 —— 那样
	// 换域名不用改命令行、不用重启平台。
	PublicBaseURL string
	// TrustForwardedFor 是否信任 X-Forwarded-For / X-Real-IP。
	//
	// 默认 **不信任**:这两个头任何人都能伪造,直接采信会让「按 IP 限流」
	// 形同虚设(攻击者每次换一个头就能绕过)。
	// 只有在确实部署在可信反向代理(Nginx / Ingress)后面时才打开,
	// 并且要保证代理会**覆写**而不是透传客户端送来的同名头。
	TrustForwardedFor bool
	// SessionTTL 登录会话有效期,默认 24 小时;0 表示不自动过期。
	SessionTTL time.Duration
	// CertDir 平台托管的微信平台证书缓存根目录,默认 ./payment-data/certs。
	//
	// 每个商户的证书放在 <CertDir>/<user_id>/ 下:商户不能自己指定目录,
	// 否则就能借「证书缓存」往服务器任意目录写文件。
	CertDir string
	// Logger 日志,默认标准错误。
	Logger *log.Logger
	// MasterKey 渠道密钥的加密主密钥。
	//
	// 留空时按 PAY_PLATFORM_MASTER_KEY / PAY_PLATFORM_MASTER_KEY_FILE /
	// ./payment-data/master.key 依次查找(没有则自动生成一个随机密钥文件)。
	// 生产环境请用环境变量或 KMS 注入,并与数据库分开存放。
	MasterKey string
	// MFASender 二次验证验证码下发通道,留空时使用 LogMFASender(仅日志,仅限开发)。
	MFASender MFASender
	// OutboundAllowPrivate 允许平台把请求发到内网 / 回环 / 链路本地地址。
	//
	// 默认 **false**。商户自己填的短信网关地址、SMTP 服务器地址都是
	// 「平台替商户去连」的目标:不拦的话,填 http://127.0.0.1:6379/ 或
	// http://169.254.169.254/ 就能借平台探测内网 / 读云元数据(SSRF)。
	//
	// 确实要用内网自建网关(企业内网短信网关、内网 SMTP)时打开它。
	OutboundAllowPrivate bool
	// OutboundAllowedHosts 出站请求的主机名白名单(不区分大小写)。
	//
	// 命中白名单的主机不做内网地址检查 —— 用来精确放开某几个内网网关,
	// 比整体打开 OutboundAllowPrivate 安全得多。
	OutboundAllowedHosts []string
	// AllowLogOnlyMFA 允许「验证码只写进服务器日志」的通道(仅开发 / 内网联调)。
	//
	// 默认 false —— 生产环境不该出现「验证码发不出去,页面却说已发送」的事:
	// 用户在等一个永远不会来的邮件,而开启二次验证之后登录又要这个码,
	// 等于把自己锁在门外。
	//
	// 打开它之后,平台方可以直接从服务器日志里取验证码(联调方便),
	// 页面上的「验证码已写入服务器日志」提示也会照常出现。
	AllowLogOnlyMFA bool
	// VerifyTTL 敏感操作票据有效期,默认 10 分钟。
	VerifyTTL time.Duration
}

// Platform 支付平台。
type Platform struct {
	opts  Options
	store *Store

	mu        sync.RWMutex
	providers map[string]map[payment.Channel]payment.Provider

	// loginLimiter 登录接口限流(按 IP / 账户)。
	loginLimiter *rateLimiter
	// mfaLimiter 验证码相关接口限流(按用户 / IP)。
	mfaLimiter *rateLimiter
	// apiLimiter 开放接口限流(按密钥 / IP)。
	apiLimiter *rateLimiter
	// exportLimiter 订单导出限流(按商户)。
	//
	// 导出是**最贵的一个接口**:一次要扫几千行订单、拼几 MB 的 CSV。
	// 不限流的话,一个商户(或拿到令牌的人)连点几下就能把数据库和带宽打满,
	// 别的商户跟着一起卡。
	exportLimiter *rateLimiter
	// qrcodeLimiter 二维码渲染限流(按 IP)。
	//
	// 渲染一张二维码要编码 + 光栅化(实测 5~35 ms CPU),
	// 拿令牌的人循环请求就能把 CPU 打满。
	qrcodeLimiter *rateLimiter
	// qrcodeCache 二维码图片缓存(内容 → PNG)。
	qrcodeCache *qrcodeCache
	// outbound 出站请求的地址策略(SSRF 防护)。
	outbound outboundPolicy
	// nonces 开放接口的随机串缓存(防重放)。
	nonces *nonceCache
}

// New 创建支付平台。
func New(opts Options) (*Platform, error) {
	if opts.Driver == "" {
		opts.Driver = DialectSQLite
	}
	if opts.DB == nil {
		if opts.DSN == "" {
			return nil, fmt.Errorf("platform: 需要提供 DB 或 DSN")
		}
		db, err := OpenDB(opts.Driver, opts.DSN)
		if err != nil {
			return nil, err
		}
		opts.DB = db
	}
	if opts.Dialect == "" {
		opts.Dialect = DialectFromDriver(opts.Driver)
	}
	if opts.Addr == "" {
		opts.Addr = ":8080"
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, "[payment-platform] ", log.LstdFlags)
	}
	if opts.VerifyTTL <= 0 {
		opts.VerifyTTL = defaultVerifyTTL
	}
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = defaultSessionTTL
	}
	if opts.CertDir == "" {
		opts.CertDir = defaultCertDir
	}
	if opts.MFASender == nil {
		opts.MFASender = LogMFASender{Logger: opts.Logger}
	}
	// 渠道密钥加密主密钥
	masterKey := opts.MasterKey
	keySource := "Options.MasterKey"
	if masterKey == "" {
		k, src, err := LoadOrCreateMasterKey()
		if err != nil {
			return nil, err
		}
		masterKey, keySource = k, src
	}
	box, err := NewSecretBox(masterKey)
	if err != nil {
		return nil, err
	}
	opts.Logger.Printf("渠道密钥加密主密钥来源: %s", keySource)
	store, err := NewStoreWithTTL(opts.DB, box, opts.SessionTTL)
	if err != nil {
		return nil, err
	}
	return &Platform{
		opts:          opts,
		store:         store,
		providers:     make(map[string]map[payment.Channel]payment.Provider),
		loginLimiter:  newRateLimiter(),
		mfaLimiter:    newRateLimiter(),
		apiLimiter:    newRateLimiter(),
		exportLimiter: newRateLimiter(),
		qrcodeLimiter: newRateLimiter(),
		qrcodeCache:   newQRCodeCache(300),
		nonces:        &nonceCache{},
		outbound:      outboundPolicyFrom(opts),
	}, nil
}

// Store 暴露存储,便于上层做统计等扩展。
func (p *Platform) Store() *Store { return p.store }

// logStartupWarnings 启动自检:把「配置上看着没事、实际上有洞」的地方喊出来。
//
// 这些问题在页面上看不出来(页面只会在用户撞上的时候提示一次),
// 运维不主动看一眼日志就永远发现不了:
//
//  1. 没设注册码 → 任何人都能抢注商户名;
//  2. 有账户开了二次验证,但验证码发不出去 → 平台会**自动跳过**二次验证
//     (fail-open),等于这些账户的 2FA 是摆设,却没人知道。
func (p *Platform) logStartupWarnings() {
	if p.opts.RegistrationCode == "" && !p.opts.DisableSelfRegistration {
		p.opts.Logger.Printf("[安全提示] 没有设置注册码(RegistrationCode)," +
			"任何人都可以用任意商户名自助注册并抢先占名;" +
			"生产环境请设置注册码,或打开 DisableSelfRegistration 由平台方统一开户")
	}
	users, err := p.store.AllUsers()
	if err != nil {
		p.opts.Logger.Printf("[安全提示] 读取账户列表失败,无法检查二次验证下发通道: %v", err)
		return
	}
	for i := range users {
		u := users[i]
		if !u.MFAEnabled || u.MFAType == MFATypeTOTP {
			continue
		}
		ready, cerr := p.mfaChannelReady(u.UserID, u.MFAType)
		if cerr != nil {
			p.opts.Logger.Printf("[安全提示] 检查账户 %s 的 %s 通道失败: %v", u.UserID, u.MFAType, cerr)
			continue
		}
		if !ready {
			p.opts.Logger.Printf(
				"[安全提示] 账户 %s(%s)开启了 %s 二次验证,但验证码**发不出去**"+
					"(没配下发通道,或平台以 -log-mfa 启动):平台登录时会自动跳过它,"+
					"这条二次验证等于没有,请配置通道或改用验证器 App",
				u.UserID, u.UserName, u.MFAType)
		}
	}
}

// userProviders 返回某用户**全局**渠道配置构建的 Provider 集合(带缓存)。
func (p *Platform) userProviders(userID string) (map[payment.Channel]payment.Provider, error) {
	p.mu.RLock()
	cached, ok := p.providers[userID]
	p.mu.RUnlock()
	if ok {
		return cached, nil
	}

	channels, err := p.store.Channels(userID)
	if err != nil {
		return nil, err
	}
	providers := p.buildProviders(userID, "", channels)
	p.mu.Lock()
	p.providers[userID] = providers
	p.mu.Unlock()
	return providers, nil
}

// appProviders 返回某用户在**某个应用下**实际可用的 Provider 集合。
//
// 规则:应用专属配置覆盖同名渠道的全局配置;没配过的渠道用全局那套。
// 应用**没有任何**专属配置时直接复用全局那套(不重复构建)。
//
// 为什么要按应用区分:一个商户可能有多个产品,各用各的微信 AppID / 商户号。
// 用全局配置去调另一个产品的渠道,会打到别人的商户号上 ——
// 那就不是「订单归错类」而是「钱进错账户」。
func (p *Platform) appProviders(userID, appID string) (map[payment.Channel]payment.Provider, error) {
	if appID == "" {
		return p.userProviders(userID)
	}
	key := providerCacheKey(userID, appID)
	p.mu.RLock()
	cached, ok := p.providers[key]
	p.mu.RUnlock()
	if ok {
		return cached, nil
	}

	global, err := p.userProviders(userID)
	if err != nil {
		return nil, err
	}
	specific, err := p.store.AppChannels(userID, appID)
	if err != nil {
		return nil, err
	}
	// 没有专属配置:直接用全局那套,不用另建一遍。
	if len(specific) == 0 {
		p.mu.Lock()
		p.providers[key] = global
		p.mu.Unlock()
		return global, nil
	}
	// 以全局为底,逐个渠道覆盖。
	merged := make(map[payment.Channel]payment.Provider, len(global)+len(specific))
	for ch, provider := range global {
		merged[ch] = provider
	}
	overrides := make(map[string]ChannelConfig, len(specific))
	for _, cfg := range specific {
		overrides[cfg.Channel] = cfg
	}
	for channel, cfg := range overrides {
		if !cfg.Enabled {
			// 应用专属这行「不启用」= 这个产品先别用这个渠道
			//(即使全局是启用的)。删掉就是禁用。
			delete(merged, payment.Channel(channel))
			continue
		}
		provider, err := buildProvider(cfg, p.certDirFor(userID, appID))
		if err != nil {
			p.opts.Logger.Printf("构建应用 %s 的渠道 %s 失败: %v", appID, channel, err)
			continue
		}
		merged[payment.Channel(channel)] = provider
	}
	p.mu.Lock()
	p.providers[key] = merged
	p.mu.Unlock()
	return merged, nil
}

// providerCacheKey 生成 Provider 缓存的键。
//
// 全局配置用裸 userID(保持老行为,也让老测试不受影响);
// 应用专属配置用 userID + 分隔符 + appID,两者不会撞。
func providerCacheKey(userID, appID string) string {
	return userID + "\x00" + appID
}

// buildProviders 按渠道配置构建 Provider 集合(跳过没启用 / 构建失败的)。
func (p *Platform) buildProviders(userID, appID string, channels []ChannelConfig) map[payment.Channel]payment.Provider {
	certDir := p.certDirFor(userID, appID)
	providers := make(map[payment.Channel]payment.Provider)
	for _, cfg := range channels {
		if !cfg.Enabled {
			continue
		}
		provider, err := buildProvider(cfg, certDir)
		if err != nil {
			p.opts.Logger.Printf("构建渠道 %s 失败: %v", cfg.Channel, err)
			continue
		}
		providers[payment.Channel(cfg.Channel)] = provider
	}
	return providers
}

// invalidateUser 清掉某用户的 Provider 缓存(配置变更后调用)。
//
// 要连**应用专属**的那几份一起清:改了全局配置,各应用的合并结果也变了。
func (p *Platform) invalidateUser(userID string) {
	p.mu.Lock()
	delete(p.providers, userID)
	for key := range p.providers {
		if strings.HasPrefix(key, userID+"\x00") {
			delete(p.providers, key)
		}
	}
	p.mu.Unlock()
}

// certDirFor 返回某商户的平台证书缓存目录。
//
// 目录由平台统一分配(<CertDir>/<user_id>),**不接受商户自填**:
// 商户自填目录等于给了「往服务器任意目录写文件」的能力。
//
// appID 非空时再分一层子目录(<CertDir>/<user_id>/<app_id>):
// 一个商户的不同产品可能用**不同的微信商户号**,而平台证书是按商户号
// 下载并缓存的 —— 混在一个目录里会让某个产品拿另一个产品的证书去验签。
//
// user_id / app_id 会拼进文件路径,所以**必须先过 safeDirSegment**:
// 这两个值有一部分来自 URL 路径(/notify/{channel}/{user_id}/{app_id}),
// 而 Go 的 ServeMux 只保证「一段里不含 /」,`..` 与编码过的斜杠仍可能混进来。
// 不消毒就直接 filepath.Join,再经 buildProvider 的 MkdirAll 一写,
// 就等于给了「往服务器任意目录建目录」的能力。
func (p *Platform) certDirFor(userID, appID string) string {
	if userID == "" {
		return p.opts.CertDir
	}
	dir := filepath.Join(p.opts.CertDir, safeDirSegment(userID))
	if appID == "" {
		return dir
	}
	return filepath.Join(dir, safeDirSegment(appID))
}

// safeDirSegment 把标识符消毒成可以安全拼进路径的一段。
//
// 只放行字母、数字、下划线与连字符(平台生成的 user_id / app_id 就是
// 「app」+ 十六进制,天然落在白名单里)。其它字符一律换成下划线 ——
// 用替换而不是直接拒绝,是为了不因为一个畸形 ID 就把整个渠道搞挂。
func safeDirSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// buildProvider 依据渠道配置构建 Provider。
//
// certDir 是平台分配的证书缓存目录(为空则用配置里的)。
func buildProvider(cfg ChannelConfig, certDir string) (payment.Provider, error) {
	switch cfg.Channel {
	case string(payment.ChannelWechat):
		if cfg.Wechat == nil {
			return nil, fmt.Errorf("缺少微信支付配置")
		}
		conf := *cfg.Wechat
		if certDir != "" {
			conf.CertDir = certDir
			// 证书缓存目录由平台创建,避免首次下载证书时因为目录不存在而失败。
			if err := os.MkdirAll(certDir, 0o700); err != nil {
				return nil, fmt.Errorf("创建证书缓存目录失败: %w", err)
			}
		}
		return wechat.New(&conf)
	case string(payment.ChannelAlipay):
		if cfg.Alipay == nil {
			return nil, fmt.Errorf("缺少支付宝配置")
		}
		return alipay.New(cfg.Alipay)
	case string(payment.ChannelApplePay):
		if cfg.Wallet == nil {
			return nil, fmt.Errorf("缺少 Apple Pay 配置")
		}
		stripe, err := gateway.NewStripe(&gateway.StripeConfig{
			SecretKey:      cfg.Wallet.StripeSecretKey,
			PublishableKey: cfg.Wallet.StripePublishableKey,
			WebhookSecret:  cfg.Wallet.StripeWebhookSecret,
		})
		if err != nil {
			return nil, err
		}
		return applepay.New(&applepay.Config{
			MerchantID:  cfg.Wallet.MerchantID,
			DisplayName: cfg.Wallet.DisplayName,
			DomainName:  cfg.Wallet.DomainName,
			Currency:    cfg.Wallet.Currency,
			Gateway:     stripe,
		})
	case string(payment.ChannelGooglePay):
		if cfg.Wallet == nil {
			return nil, fmt.Errorf("缺少 Google Pay 配置")
		}
		stripe, err := gateway.NewStripe(&gateway.StripeConfig{
			SecretKey:      cfg.Wallet.StripeSecretKey,
			PublishableKey: cfg.Wallet.StripePublishableKey,
			WebhookSecret:  cfg.Wallet.StripeWebhookSecret,
		})
		if err != nil {
			return nil, err
		}
		return googlepay.New(&googlepay.Config{
			MerchantID:   cfg.Wallet.MerchantID,
			MerchantName: cfg.Wallet.DisplayName,
			Currency:     cfg.Wallet.Currency,
			Gateway:      stripe,
		})
	default:
		return nil, fmt.Errorf("未知渠道 %s", cfg.Channel)
	}
}

// defaultBase 平台默认的对外访问地址。
//
// 来自启动参数 -base;没给就退回本机地址(只适合本机调试)。
func (p *Platform) defaultBase() string {
	if p.opts.PublicBaseURL != "" {
		return p.opts.PublicBaseURL
	}
	return "http://127.0.0.1" + p.opts.Addr
}

// notifyBaseFor 返回某个商户的回调地址前缀。
//
// 优先用商户在控制台里自己填的对外访问地址(渠道配置页 →「回调地址」),
// 没填就用平台默认地址(-base)。
//
// 分成两级是有意的:-base 是**平台级**的部署参数(所有商户共用一个域名),
// 而商户自己的域名可能不一样(每个商户一个独立域名反代到本平台)。
// 两者都在时以商户自己配的为准,改完立即生效,不用重启平台。
func (p *Platform) notifyBaseFor(user *User) string {
	if user != nil && user.NotifyBaseURL != "" {
		return user.NotifyBaseURL
	}
	return p.defaultBase()
}

// notifyURLFor 生成某个商户的渠道回调地址。
//
// appID 非空时地址里多带一段应用标识:
//
//	/notify/wechat/<user_id>          ← 用全局渠道配置
//	/notify/wechat/<user_id>/<app_id> ← 用该应用的渠道配置
//
// 为什么必须带:一个商户的不同产品可能用**不同的微信 AppID / 商户号**,
// 回调验签要用的正是当初下单时的那套密钥。地址里不区分,
// 平台就不知道该用哪一套 —— 轻则验签失败(订单停在待支付),重则用错商户号
// 验签通过(钱记到别人的账上)。
func (p *Platform) notifyURLFor(channel payment.Channel, user *User, userID, appID string) string {
	base := p.notifyBaseFor(user)
	if userID == "" {
		return fmt.Sprintf("%s/notify/%s", base, channel)
	}
	if appID == "" {
		return fmt.Sprintf("%s/notify/%s/%s", base, channel, userID)
	}
	return fmt.Sprintf("%s/notify/%s/%s/%s", base, channel, userID, appID)
}

// notifyURLForUser 生成某个商户的渠道回调地址(不带应用维度)。
func (p *Platform) notifyURLForUser(channel payment.Channel, user *User, userID string) string {
	return p.notifyURLFor(channel, user, userID, "")
}

// notifyURLMap 返回该商户各渠道的回调地址(控制台展示用)。
func (p *Platform) notifyURLMap(user *User) map[string]string {
	return map[string]string{
		string(payment.ChannelWechat):    p.notifyURLForUser(payment.ChannelWechat, user, user.UserID),
		string(payment.ChannelAlipay):    p.notifyURLForUser(payment.ChannelAlipay, user, user.UserID),
		string(payment.ChannelApplePay):  p.notifyURLForUser(payment.ChannelApplePay, user, user.UserID),
		string(payment.ChannelGooglePay): p.notifyURLForUser(payment.ChannelGooglePay, user, user.UserID),
	}
}

// NotifyURL 生成某个渠道的回调地址。
//
// 回调地址带上用户标识,平台据此找到对应商户的渠道配置完成验签。
func (p *Platform) NotifyURL(channel payment.Channel, userID string) string {
	var user *User
	if userID != "" {
		// 查不到就退回平台默认地址:回调地址只影响渠道通知往哪发,
		// 查库失败不该让整笔支付失败(平台默认地址仍然可用)。
		if u, err := p.store.UserByID(userID); err == nil {
			user = u
		} else {
			p.opts.Logger.Printf("读取商户 %s 的回调地址配置失败: %v", userID, err)
		}
	}
	return p.notifyURLForUser(channel, user, userID)
}

// Handler 返回 HTTP 处理器,可挂载到已有服务。
func (p *Platform) Handler() http.Handler {
	mux := http.NewServeMux()

	// 前端:Vite + TypeScript 打包产物
	mux.HandleFunc("GET /{$}", p.handleSPA)
	mux.HandleFunc("GET /assets/{path...}", p.handleSPA)
	mux.HandleFunc("GET /channels", p.handleSPA)
	mux.HandleFunc("GET /playground", p.handleSPA)
	mux.HandleFunc("GET /orders", p.handleSPA)
	mux.HandleFunc("GET /reports", p.handleSPA)
	mux.HandleFunc("GET /apps", p.handleSPA)
	mux.HandleFunc("GET /security", p.handleSPA)
	mux.HandleFunc("GET /keys", p.handleSPA)

	mux.HandleFunc("POST /api/login", p.handleLogin)
	mux.HandleFunc("POST /api/login/code", p.handleSendLoginCode)
	mux.HandleFunc("POST /api/logout", p.handleLogout)
	mux.HandleFunc("GET /api/me", p.handleMe)
	mux.HandleFunc("GET /api/orders", p.handleOrdersAPI)
	mux.HandleFunc("GET /api/orders/export", p.handleOrdersExport)
	mux.HandleFunc("GET /api/refunds", p.handleRefundsAPI)
	mux.HandleFunc("GET /api/stats", p.handleStatsAPI)
	mux.HandleFunc("GET /api/audit-logs", p.handleAuditLogs)
	mux.HandleFunc("POST /api/pay", p.handlePay)
	mux.HandleFunc("POST /api/wallet/charge", p.handleWalletCharge)
	mux.HandleFunc("POST /api/query", p.handleQuery)
	mux.HandleFunc("POST /api/refund", p.handleRefund)
	mux.HandleFunc("POST /api/close", p.handleClose)
	mux.HandleFunc("GET /api/qrcode", p.handleQRCode)
	mux.HandleFunc("POST /api/channels/{channel}", p.handleSaveChannel)
	mux.HandleFunc("POST /api/channels/{channel}/test", p.handleTestChannel)
	mux.HandleFunc("POST /api/channels/{channel}/reveal", p.handleRevealChannel)

	// 应用 / 产品维度。
	mux.HandleFunc("GET /api/apps", p.handleApps)
	mux.HandleFunc("POST /api/apps", p.handleSaveApp)
	mux.HandleFunc("POST /api/apps/{app_id}", p.handleSaveApp)
	mux.HandleFunc("DELETE /api/apps/{app_id}", p.handleDeleteApp)
	mux.HandleFunc("GET /api/apps/{app_id}/channels", p.handleAppChannels)
	mux.HandleFunc("DELETE /api/apps/{app_id}/channels/{channel}", p.handleDeleteAppChannel)
	mux.HandleFunc("POST /api/apps/require-binding", p.handleSetRequireAppBinding)

	// 开放接口:给商户自己的业务后端调用(API 密钥 + 签名)。
	mux.HandleFunc("POST "+openPathPay, p.handleOpenPay)
	mux.HandleFunc("POST "+openPathQuery, p.handleOpenQuery)
	mux.HandleFunc("POST "+openPathQueryOrder, p.handleOpenOrder)
	mux.HandleFunc("POST "+openPathRefund, p.handleOpenRefund)
	mux.HandleFunc("POST "+openPathClose, p.handleOpenClose)
	mux.HandleFunc("POST "+openPathWallet, p.handleOpenWallet)
	mux.HandleFunc("GET "+openPathOrders, p.handleOpenOrders)
	mux.HandleFunc("GET "+openPathStats, p.handleOpenStats)

	// 开放接口密钥管理(控制台内,创建/吊销需要二次验证)。
	mux.HandleFunc("GET /api/keys", p.handleAPIKeys)
	mux.HandleFunc("POST /api/keys", p.handleCreateAPIKey)
	mux.HandleFunc("POST /api/keys/{key_id}/revoke", p.handleRevokeAPIKey)
	// 开放接口的真实代码示例(页面直接展示仓库里的那份代码)。
	mux.HandleFunc("GET /api/openapi/sample", p.handleOpenAPISample)

	// 平台管理接口(管理员令牌)。
	mux.HandleFunc("GET /api/admin/merchants", p.handleAdminMerchants)
	mux.HandleFunc("POST /api/admin/merchants", p.handleAdminCreateMerchant)
	mux.HandleFunc("GET /api/admin/stats", p.handleAdminStats)
	mux.HandleFunc("POST /api/admin/merchants/{user_id}/status", p.handleAdminSetMerchant)
	mux.HandleFunc("POST /api/admin/merchants/{user_id}/mfa/reset", p.handleAdminResetMFA)

	// 运维探针:不需要令牌,只暴露存活与版本信息。
	mux.HandleFunc("GET /healthz", p.handleHealthz)
	mux.HandleFunc("GET /api/version", p.handleVersion)

	// 账户安全
	mux.HandleFunc("GET /api/account/security", p.handleAccountSecurity)
	mux.HandleFunc("POST /api/account/password", p.handleSetPassword)
	mux.HandleFunc("POST /api/account/mfa/code", p.handleSendMFACode)
	mux.HandleFunc("POST /api/account/mfa/enable", p.handleEnableMFA)
	mux.HandleFunc("POST /api/account/mfa/disable", p.handleDisableMFA)
	mux.HandleFunc("POST /api/account/mfa/totp/setup", p.handleTOTPSetup)
	mux.HandleFunc("POST /api/account/mfa/backup", p.handleBindMFABackup)
	mux.HandleFunc("DELETE /api/account/mfa/backup", p.handleDeleteMFABackup)
	mux.HandleFunc("GET /api/account/mfa/channels", p.handleMFAChannels)
	mux.HandleFunc("POST /api/account/mfa/channels", p.handleSaveMFAChannel)
	mux.HandleFunc("POST /api/account/mfa/channels/delete", p.handleDeleteMFAChannel)
	mux.HandleFunc("POST /api/account/mfa/channels/test", p.handleTestMFAChannel)
	mux.HandleFunc("POST /api/account/verify", p.handleVerify)
	// 商户自己的对外访问地址(回调地址前缀):控制台里改,不用重启平台。
	mux.HandleFunc("POST /api/account/notify-base", p.handleSaveNotifyBase)

	// 回调地址对外公开,不做用户令牌校验(第三方平台不会带令牌)
	mux.HandleFunc("POST /notify/{channel}/{user_id}", p.handleNotify)
	mux.HandleFunc("GET /notify/{channel}/{user_id}", p.handleNotify)
	// 应用维度的回调地址:验签要用该产品自己的渠道密钥。
	mux.HandleFunc("POST /notify/{channel}/{user_id}/{app_id}", p.handleNotify)
	mux.HandleFunc("GET /notify/{channel}/{user_id}/{app_id}", p.handleNotify)

	// 顺序很关键:
	//   - gzip 在最外层:连 401/403 这类错误响应也压(它们也是 JSON)。
	//   - 安全响应头在最内层:确保每个响应(哪怕是 404)都带上安全头。
	return p.withGzip(p.withSecurityHeaders(p.withAuth(mux)))
}

// withSecurityHeaders 给每个响应加上安全响应头。
//
// 这些头挡的是「浏览器端」的攻击面:
//   - nosniff:禁止浏览器猜 MIME 类型(防 MIME 混淆型 XSS);
//   - DENY / frame-ancestors 'none':禁止被嵌进 iframe(防点击劫持);
//   - CSP:限制页面只能加载自己的脚本与资源(防 XSS 引入外部脚本);
//   - no-referrer:不把带令牌的 URL 泄漏给第三方(令牌不再出现在 URL 里,
//     这一条是兜底);
//   - Permissions-Policy:关掉用不上的浏览器能力。
//
// HTTPS 下再补 HSTS。
func (p *Platform) withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cross-Origin-Resource-Policy", "same-origin")
		h.Set("Permissions-Policy", "geolocation=(), camera=(), microphone=(), payment=()")
		h.Set("Content-Security-Policy", cspHeader)
		if isHTTPS(r) {
			h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

// cspHeader 内容安全策略。
//
// 前端是 Vite 打包的静态资源:脚本与样式都来自同源,只有内联样式(style 属性)
// 需要放行;二维码图片来自同源的 /api/qrcode。
const cspHeader = "default-src 'self'; " +
	"script-src 'self'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data: blob:; " +
	"connect-src 'self'; " +
	"font-src 'self' data:; " +
	"object-src 'none'; " +
	"base-uri 'self'; " +
	"form-action 'self'; " +
	"frame-ancestors 'none'"

// isHTTPS 判断请求是否走 HTTPS(含反向代理的 X-Forwarded-Proto)。
func isHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	proto := strings.TrimSpace(strings.ToLower(r.Header.Get("X-Forwarded-Proto")))
	return proto == "https"
}

// withAuth 令牌校验中间件。
//
// 三种鉴权方式,按路径前缀分流:
//
//	/api/open/*   开放接口:用 API 密钥签名自证身份(中间件放行,handler 里验签)
//	/api/admin/*  平台管理:用管理员令牌
//	其他          商户控制台:用用户令牌
//
// 令牌**只从请求头或 Cookie 取**,不再支持 URL 查询参数:
// URL 会进浏览器历史、Referer、反代日志和监控,等于把令牌公开。
func (p *Platform) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if p.isPublicPath(path) {
			next.ServeHTTP(w, r)
			return
		}
		// 开放接口自己验签:这里放行,别用用户令牌把它挡掉。
		if strings.HasPrefix(path, openAPIPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		// 平台管理接口:管理员令牌。handler 里还会再校验一次
		// (中间件只是提前挡掉明显错误的请求)。
		if strings.HasPrefix(path, "/api/admin/") {
			if err := p.requireAdmin(r); err != nil {
				writeError(w, http.StatusForbidden, err.Error())
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if _, err := p.currentUser(r); err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isPublicPath 判断路径是否无需令牌即可访问。
//
// 首页、登录接口与支付回调始终放行;前端静态资源在 SPA 构建后放行。
func (p *Platform) isPublicPath(path string) bool {
	if path == "/" || strings.HasPrefix(path, "/notify/") || path == "/api/login" {
		return true
	}
	// 登录验证码:未登录时才能拿到,否则开了二次验证就登不进去了。
	if path == "/api/login/code" || path == "/healthz" || path == "/api/version" {
		return true
	}
	if strings.HasPrefix(path, "/assets/") ||
		path == "/favicon.ico" ||
		path == "/channels" || path == "/playground" ||
		path == "/orders" || path == "/security" ||
		path == "/keys" {
		return true
	}
	return false
}

// clientIPOf 取来源 IP(限流与审计用)。
//
// RemoteAddr 里带着端口,**必须去掉端口**:留着端口的话每次连接都是新的
// key,「按 IP 限流」等于完全失效。
//
// X-Forwarded-For / X-Real-IP 只有在明确部署在可信代理后面时才采信
// (Options.TrustForwardedFor):这两个头任何人都能伪造,
// 采信它等于让攻击者用一个头就绕过 IP 限流。
func (p *Platform) clientIPOf(r *http.Request) string {
	if p.opts.TrustForwardedFor {
		if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
			return strings.TrimSpace(strings.Split(ip, ",")[0])
		}
		if ip := r.Header.Get("X-Real-IP"); ip != "" {
			return strings.TrimSpace(ip)
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return host
}

// Start 启动 HTTP 服务(阻塞)。
//
// 超时必须配齐:只配 ReadHeaderTimeout 的话,一个连上不发数据、
// 或者发一半就停住的客户端能一直占着连接不放(慢连接攻击)。
func (p *Platform) Start() error {
	p.logStartupWarnings()
	server := &http.Server{
		Addr:              p.opts.Addr,
		Handler:           p.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// ReadTimeout 覆盖「读完整请求」的时间(请求体大小由 MaxBytesReader 兜住)。
		ReadTimeout: 30 * time.Second,
		// WriteTimeout 覆盖「写完整响应」的时间。
		// 不能给太长:查单、退款这些要等渠道,渠道慢的话连接会一直挂着。
		WriteTimeout: 60 * time.Second,
		// IdleTimeout 是 keep-alive 连接的空闲上限。
		IdleTimeout: 90 * time.Second,
	}
	p.opts.Logger.Printf("支付平台已启动: http://127.0.0.1%s", p.opts.Addr)
	return server.ListenAndServe()
}
