package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/AIntelligenceGame/bus/payment"
)

// ErrOrderExists 商户单号已被占用(同一商户内不允许重复)。
var ErrOrderExists = errors.New("platform: 商户单号已存在")

// ErrOrderNotFound 订单不存在(或不属于该商户)。
var ErrOrderNotFound = errors.New("platform: 订单不存在")

// Store 平台数据存储(GORM)。
//
// 渠道密钥**不以明文落库**:cfg_enc 列保存 AES-256-GCM 密文,
// 解密只在服务端内存中进行,主密钥来自 Options.MasterKey / 主密钥文件。
//
// 访问令牌与一次性验证码同样**不以明文落库**:只存带密钥散列(HMAC-SHA256),
// 校验时把传入的令牌/验证码做同样的散列再比对。
type Store struct {
	db  *gorm.DB
	box *SecretBox
	// hasher 令牌/验证码的带密钥散列器。
	hasher *tokenHasher
	// sessionTTL 访问令牌有效期,0 表示永不过期。
	sessionTTL time.Duration
	// refundLocks 订单级退款锁(key = user_id|out_trade_no)。
	refundLocks sync.Map
}

// NewStore 基于已打开的数据库连接创建存储并初始化表结构。
//
// box 为密钥加密盒;为空时渠道配置退化为明文存储(仅建议本地调试)。
func NewStore(db *gorm.DB, box *SecretBox) (*Store, error) {
	return NewStoreWithTTL(db, box, 0)
}

// NewStoreWithTTL 与 NewStore 相同,但可以指定访问令牌有效期。
//
// sessionTTL 为 0 时令牌永不过期(老库兼容);大于 0 时登录/退出签发的令牌
// 到期后立即失效。
func NewStoreWithTTL(db *gorm.DB, box *SecretBox, sessionTTL time.Duration) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("platform: database connection is required")
	}
	s := &Store{db: db, box: box, hasher: newTokenHasher(box), sessionTTL: sessionTTL}
	if err := s.initSchema(); err != nil {
		return nil, err
	}
	return s, nil
}

// DB 暴露底层 GORM 连接,便于上层做统计等扩展。
func (s *Store) DB() *gorm.DB { return s.db }

// dialect 当前方言(sqlite / mysql)。
func (s *Store) dialect() string {
	if s.db == nil || s.db.Dialector == nil {
		return DialectSQLite
	}
	if s.db.Dialector.Name() == DialectMySQL {
		return DialectMySQL
	}
	return DialectSQLite
}

// initSchema 建表(幂等)并补齐新增列。
//
// GORM 的 AutoMigrate 只做「加列 / 加表」,不会改主键,
// 所以订单表主键迁移要在它之前单独做(见 migrate.go)。
func (s *Store) initSchema() error {
	if err := s.migrateOrderPrimaryKey(); err != nil {
		return err
	}
	// 唯一索引 (user_id, out_refund_no) 建之前先把重复行清掉,
	// 否则老库里的重复行会让索引创建失败、平台起不来。
	if err := s.migrateRefundDuplicates(); err != nil {
		return err
	}
	if err := s.db.AutoMigrate(
		&UserModel{}, &ChannelModel{}, &OrderModel{},
		&MFACodeModel{}, &VerifyTokenModel{}, &AuditLogModel{},
		&RefundModel{}, &APIKeyModel{}, &MFAChannelModel{},
		// 应用 / 产品维度(方案 A):新表由 AutoMigrate 直接建出来,
		// 老的 pay_user_channels 一行不动 —— 老商户完全不受影响。
		&AppModel{}, &AppChannelModel{},
	); err != nil {
		return fmt.Errorf("platform: 初始化表结构失败: %w", err)
	}
	// 老库里访问令牌是明文,迁移成带密钥散列(迁移完清空明文列)。
	if err := s.migrateAccessTokens(); err != nil {
		return err
	}
	// 时间列统一成固定宽度 UTC(老数据是本地时区的 RFC3339Nano)。
	if err := s.migrateTimeColumns(); err != nil {
		return err
	}
	// 老订单都是老流程(先调渠道、后落库)建的,渠道侧确实有单。
	// 不标记的话,关单时会被当成「渠道侧没有这笔单」而**不去渠道关单**,
	// 渠道那边的单会一直挂着(用户还能继续付钱)。
	if err := s.migrateChannelCreatedFlag(); err != nil {
		return err
	}
	return nil
}

// migrateChannelCreatedFlag 把**老库里的存量订单**标记成「渠道侧已创建」。
//
// 老流程是「先调渠道、成功后落库」:库里只要有一行,渠道那边就一定有单。
// 不标记的话,关单时会被当成「渠道侧没有这笔单」而**不去渠道关单**,
// 渠道那边的单会一直挂着(用户还能继续付钱)。
//
// 判据是 `channel_created IS NULL`,不是 `= 0`:
//
//   - 老库加这一列时是 AutoMigrate 加的 —— 加列**不会给存量行填默认值**,
//     老行拿到的是 **NULL**(SQLite 与 MySQL 都是)。
//   - 新流程写出来的订单,这一列是**显式**写 0 或 1 的
//     (见 AddOrder:ChannelCreated: boolToInt32(...))。
//
// 所以 NULL 精确地等于「这一行是老库留下来的」,而 0 精确地等于
// 「这一行是新流程建的、渠道侧**还没**建单(半成品)」。两者不能混:
//
//   - 用 `= 0` 会漏掉老行(NULL ≠ 0)—— 老订单永远关不到渠道;
//   - 用 `= 0` 还会**误伤**新流程的半成品订单 —— 平台重启一次,
//     那笔「渠道侧没建单」的订单就被误标成「渠道侧有单」,
//     于是商户用同一个商户单号重试会被判成「单号已存在」(409),
//     关单也会去渠道关一个根本不存在的单。
//
// 用 NULL 做判据还顺带解决了「这个迁移只能跑一次」的问题:
// 跑完之后没有 NULL 行了,再跑就是空操作 —— 不需要额外的迁移标记表。
func (s *Store) migrateChannelCreatedFlag() error {
	if !s.db.Migrator().HasTable(&OrderModel{}) {
		return nil
	}
	if !s.db.Migrator().HasColumn(&OrderModel{}, "channel_created") {
		return nil
	}
	res := s.db.Model(&OrderModel{}).
		Where("channel_created IS NULL").
		Update("channel_created", 1)
	if res.Error != nil {
		return fmt.Errorf("platform: 迁移订单渠道标记失败: %w", res.Error)
	}
	return nil
}

// ---------------------------------------------------------------- 用户

// CreateUser 创建用户。
//
// 返回的 User.AccessToken 是**明文令牌**(只在这一刻存在),库里只存它的散列。
func (s *Store) CreateUser(name, passwordHash string) (*User, error) {
	if name == "" {
		return nil, fmt.Errorf("platform: user name is required")
	}
	now := time.Now()
	token := newID("t")
	model := UserModel{
		UserID:          newID("u"),
		UserName:        name,
		AccessTokenHash: s.hasher.sum(token),
		PasswordHash:    passwordHash,
		// 新商户默认「订单必须绑定应用」。
		//
		// 从第一天起就不让出现「未分类」的订单:钱进来了却分不清是哪个产品的,
		// 事后报表对不上,只能靠人工翻日志猜。老账户不受影响(那一列是 0)。
		RequireAppBinding: 1,
		CreatedAt:         formatTime(now),
		UpdatedAt:         formatTime(now),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return nil, fmt.Errorf("platform: insert user: %w", err)
	}
	user := userFromModel(&model)
	user.AccessToken = token
	return user, nil
}

// UserByName 按名称查询用户,不存在返回 (nil, nil)。
func (s *Store) UserByName(name string) (*User, error) {
	var model UserModel
	err := s.db.Where("user_name = ?", name).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("platform: query user: %w", err)
	}
	return userFromModel(&model), nil
}

// UserByToken 通过访问令牌查找用户,不存在返回 (nil, nil)。
//
// 库里只存令牌的带密钥散列,查询时对传入令牌做同样的散列再比对。
// 令牌已过期时同样按「查不到」处理。
func (s *Store) UserByToken(token string) (*User, error) {
	if token == "" {
		return nil, nil
	}
	var model UserModel
	err := s.db.Where("access_token_hash = ?", s.hasher.sum(token)).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("platform: query user: %w", err)
	}
	if model.TokenExpiresAt != "" {
		exp, perr := parseTime(model.TokenExpiresAt)
		if perr != nil || time.Now().After(exp) {
			return nil, nil
		}
	}
	// 被平台禁用的商户:已有令牌立即失效。
	if model.Disabled != 0 {
		return nil, nil
	}
	return userFromModel(&model), nil
}

// UserByID 按用户 ID 查询用户,不存在返回 (nil, nil)。
func (s *Store) UserByID(userID string) (*User, error) {
	if userID == "" {
		return nil, nil
	}
	var model UserModel
	err := s.db.Where("user_id = ?", userID).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("platform: query user: %w", err)
	}
	return userFromModel(&model), nil
}

// SetUserDisabled 禁用 / 启用某个商户。
//
// 被禁用的商户不能登录,已有令牌也立即失效。
func (s *Store) SetUserDisabled(userID string, disabled bool) error {
	flag := int32(0)
	if disabled {
		flag = 1
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"disabled":     flag,
			"locked_until": "",
			"updated_at":   formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update user disabled: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("platform: 商户不存在")
	}
	return nil
}

// SetRequireAppBinding 设置是否强制订单绑定应用。
//
// 打开之后:这个商户的每一笔支付都必须归属某个应用。
// 打开前建议先把应用建好、密钥绑上,否则商户会突然没法下单。
func (s *Store) SetRequireAppBinding(userID string, require bool) error {
	if userID == "" {
		return fmt.Errorf("platform: user id is required")
	}
	flag := int32(0)
	if require {
		flag = 1
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"require_app_binding": flag,
			"updated_at":          formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update require_app_binding: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("platform: 商户不存在")
	}
	return nil
}

// SetNotifyBaseURL 设置 / 清除商户的对外访问地址(回调地址前缀)。
//
// base 为空字符串表示「清除」,回到平台默认地址(启动参数 -base)。
func (s *Store) SetNotifyBaseURL(userID, base string) error {
	if userID == "" {
		return fmt.Errorf("platform: 商户标识为空")
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"notify_base_url": base,
			"updated_at":      formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update notify base url: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("platform: 商户不存在")
	}
	return nil
}

// AllUsers 返回全部商户(平台管理员用,按创建时间排序)。
func (s *Store) AllUsers() ([]User, error) {
	var rows []UserModel
	if err := s.db.Order("created_at DESC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: query users: %w", err)
	}
	out := make([]User, 0, len(rows))
	for i := range rows {
		out = append(out, *userFromModel(&rows[i]))
	}
	return out, nil
}

// PlatformStats 平台整体统计(平台管理员用)。
type PlatformStats struct {
	// MerchantCount 商户总数。
	MerchantCount int64 `json:"merchant_count"`
	// DisabledCount 被禁用的商户数。
	DisabledCount int64 `json:"disabled_count"`
	// OrderCount 全平台订单数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 全平台支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// TotalAmount 全平台成交金额(分)。
	TotalAmount int64 `json:"total_amount"`
	// RefundAmount 全平台退款金额(分)。
	RefundAmount int64 `json:"refund_amount"`
}

// PlatformStats 汇总全平台的商户与交易数据。
func (s *Store) PlatformStats() (PlatformStats, error) {
	var out PlatformStats
	if err := s.db.Model(&UserModel{}).Count(&out.MerchantCount).Error; err != nil {
		return out, fmt.Errorf("platform: count users: %w", err)
	}
	if err := s.db.Model(&UserModel{}).Where("disabled = 1").Count(&out.DisabledCount).Error; err != nil {
		return out, fmt.Errorf("platform: count disabled users: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).Count(&out.OrderCount).Error; err != nil {
		return out, fmt.Errorf("platform: count orders: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).
		Where("order_status = ?", string(payment.TradeStatusSuccess)).
		Count(&out.SuccessCount).Error; err != nil {
		return out, fmt.Errorf("platform: count success orders: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).
		Where("order_status = ?", string(payment.TradeStatusSuccess)).
		Select("COALESCE(SUM(amount), 0)").Scan(&out.TotalAmount).Error; err != nil {
		return out, fmt.Errorf("platform: sum amount: %w", err)
	}
	if err := s.db.Model(&RefundModel{}).
		Select("COALESCE(SUM(amount), 0)").Scan(&out.RefundAmount).Error; err != nil {
		return out, fmt.Errorf("platform: sum refunds: %w", err)
	}
	return out, nil
}

// RotateAccessToken 更换用户的访问令牌(登录 / 退出登录用,旧令牌立即失效)。
//
// 返回新令牌明文;库里仍然只存散列。
func (s *Store) RotateAccessToken(userID string) (string, error) {
	token := newID("t")
	updates := map[string]any{
		"access_token_hash": s.hasher.sum(token),
		"access_token":      "",
		"updated_at":        formatTime(time.Now()),
	}
	if s.sessionTTL > 0 {
		updates["token_expires_at"] = formatTime(time.Now().Add(s.sessionTTL))
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).Updates(updates)
	if res.Error != nil {
		return "", fmt.Errorf("platform: rotate access token: %w", res.Error)
	}
	return token, nil
}

// RegisterLoginFailure 记一次登录失败。
//
// lockAccount 为 true 时,连续失败达到上限会锁定账户(返回锁定截止时间);
// 为 false 时只累计失败次数,不锁账户——
// 「口令猜错就锁账户」等于给攻击者一个「知道商户名就能把商户锁死」的工具,
// 所以只有「口令已正确、只是验证码没过」这种说明对方**已经掌握了口令**的情况
// 才值得锁定账户。
func (s *Store) RegisterLoginFailure(userID string, maxAttempts int, lockFor time.Duration, lockAccount bool) (string, error) {
	if userID == "" || maxAttempts <= 0 {
		return "", nil
	}
	var model UserModel
	if err := s.db.Where("user_id = ?", userID).First(&model).Error; err != nil {
		return "", fmt.Errorf("platform: query user: %w", err)
	}
	failed := model.FailedLogins + 1
	updates := map[string]any{
		"failed_logins": failed,
		"updated_at":    formatTime(time.Now()),
	}
	lockedUntil := ""
	if lockAccount && int(failed) >= maxAttempts {
		lockedUntil = formatTime(time.Now().Add(lockFor))
		updates["locked_until"] = lockedUntil
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).Updates(updates)
	if res.Error != nil {
		return "", fmt.Errorf("platform: update login failures: %w", res.Error)
	}
	return lockedUntil, nil
}

// ResetLoginFailures 清零登录失败计数(登录成功后调用)。
func (s *Store) ResetLoginFailures(userID string) error {
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"failed_logins": int32(0),
			"locked_until":  "",
			"updated_at":    formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: reset login failures: %w", res.Error)
	}
	return nil
}

// SetPassword 设置用户口令。
func (s *Store) SetPassword(userID, passwordHash string) error {
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"password_hash": passwordHash,
			"updated_at":    formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update password: %w", res.Error)
	}
	return nil
}

// SetMFA 设置用户的二次验证信息。
//
// 同时作废「待确认」的 TOTP 密钥。
//
// 为什么:待确认密钥是「换绑流程的半成品」,它的合法性取决于
// **生成它的时候**账户是什么状态。绑定一旦成立(或被关闭),
// 之前留下的待确认密钥就不能再用来绑定 —— 否则一份在**绑定之前**
// 拿到的密钥,可以在绑定之后被用来把验证器换成自己的。
//
// 配合「生成密钥时必须由当前绑定的人确认」这条规则,就能保证:
// 账户已经开着二次验证时,待确认密钥只可能来自一次**通过了确认**的生成。
func (s *Store) SetMFA(userID, mfaType, target string, enabled bool) error {
	flag := int32(0)
	if enabled {
		flag = 1
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"mfa_type":    mfaType,
			"mfa_target":  target,
			"mfa_enabled": flag,
			"updated_at":  formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update mfa: %w", res.Error)
	}
	// 绑定变了 → 之前那份「待确认密钥」不再算数。
	if err := s.SetMFATOTPPendingSecret(userID, ""); err != nil {
		return err
	}
	return nil
}

// SetMFABackup 设置备用二次验证(邮箱/短信)。
//
// 用途:验证器 App 丢了 / 手机被偷 / App 被误删时,登录还能走备用地址。
// 只是**多一条**路,主方式的要求一点没降低。
func (s *Store) SetMFABackup(userID, mfaType, target string) error {
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"mfa_backup_type":   mfaType,
			"mfa_backup_target": target,
			"updated_at":        formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update mfa backup: %w", res.Error)
	}
	return nil
}

// ClearMFABackup 清掉备用二次验证。
func (s *Store) ClearMFABackup(userID string) error {
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"mfa_backup_type":   "",
			"mfa_backup_target": "",
			"updated_at":        formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: clear mfa backup: %w", res.Error)
	}
	return nil
}

// SetMFATOTPSecret 保存 TOTP 共享密钥(加密入库)。
//
// 换密钥时顺手把「已用掉的时间片」清零:新旧密钥的码没有可比性,
// 留着旧计数会让新密钥在同一时间片里的码被误判成重放。
func (s *Store) SetMFATOTPSecret(userID, secret string) error {
	enc := ""
	if secret != "" {
		if s.box == nil {
			return fmt.Errorf("platform: 未配置主密钥,无法安全保存 TOTP 密钥")
		}
		cipher, err := s.box.EncryptWithAAD([]byte(secret), aadMFAChannel)
		if err != nil {
			return err
		}
		enc = cipher
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"mfa_secret_enc":   enc,
			"mfa_totp_counter": 0,
			"updated_at":       formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update mfa totp secret: %w", res.Error)
	}
	return nil
}

// ConsumeTOTPCode 校验 TOTP 动态码,并保证**同一个码只能用一次**。
//
// RFC 6238 §5.2:「The verifier MUST NOT accept the second attempt of the OTP
// after the successful validation has been issued for the first OTP,
// which ensures one-time only use of an OTP」。
//
// 不做一次性处理的话,一个 6 位码在它的时间片(允许 ±1 片,最长 90 秒)内
// 可以被反复使用 —— 被瞄一眼、被抓包一次,就能重放。
//
// 实现:把「已经用掉的时间片」记在用户行上,校验通过时用一条**带条件的 UPDATE**
// 原子地推进它(只有新时间片比已记录的大时才更新得动)。
// 并发提交同一个码时只有一个请求能推上去,另一个被判成重放。
func (s *Store) ConsumeTOTPCode(userID, secret, code string, at time.Time) bool {
	if userID == "" {
		return false
	}
	counter, ok := verifyTOTPCodeCounter(secret, code, at)
	if !ok {
		return false
	}
	res := s.db.Model(&UserModel{}).
		Where("user_id = ? AND (mfa_totp_counter IS NULL OR mfa_totp_counter < ?)",
			userID, counter).
		UpdateColumn("mfa_totp_counter", counter)
	if res.Error != nil {
		return false
	}
	return res.RowsAffected == 1
}

// SetMFATOTPPendingSecret 暂存「待确认」的 TOTP 密钥。
//
// 启用时用它校验动态码,校验通过才转正 —— 中途放弃不会影响正在用的绑定。
func (s *Store) SetMFATOTPPendingSecret(userID, secret string) error {
	enc := ""
	if secret != "" {
		if s.box == nil {
			return fmt.Errorf("platform: 未配置主密钥,无法安全保存 TOTP 密钥")
		}
		cipher, err := s.box.EncryptWithAAD([]byte(secret), aadMFAChannel)
		if err != nil {
			return err
		}
		enc = cipher
	}
	res := s.db.Model(&UserModel{}).Where("user_id = ?", userID).
		Updates(map[string]any{
			"mfa_pending_secret_enc": enc,
			"updated_at":             formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update pending mfa totp secret: %w", res.Error)
	}
	return nil
}

// MFATOTPSecret 读取并解密用户的 TOTP 共享密钥(生效中的)。
//
// 没绑定或解密失败返回空串(调用方按「无法校验」处理)。
func (s *Store) MFATOTPSecret(userID string) string {
	var model UserModel
	if err := s.db.Where("user_id = ?", userID).First(&model).Error; err != nil {
		return ""
	}
	return s.decryptMFASecret(model.MFASecretEnc)
}

// MFATOTPPendingSecret 读取待确认的 TOTP 密钥。
func (s *Store) MFATOTPPendingSecret(userID string) string {
	var model UserModel
	if err := s.db.Where("user_id = ?", userID).First(&model).Error; err != nil {
		return ""
	}
	return s.decryptMFASecret(model.MFAPendingSecretEnc)
}

// decryptMFASecret 解密一段 TOTP 密钥密文。
func (s *Store) decryptMFASecret(enc string) string {
	if enc == "" || s.box == nil {
		return ""
	}
	plain, err := s.box.DecryptWithAAD(enc, aadMFAChannel)
	if err != nil {
		return ""
	}
	return plain
}

// ---------------------------------------------------------------- 渠道配置

// Channels 返回某用户的全部渠道配置(解密后的明文,仅服务端内部使用)。
func (s *Store) Channels(userID string) ([]ChannelConfig, error) {
	var rows []ChannelModel
	if err := s.db.Where("user_id = ?", userID).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: query channels: %w", err)
	}
	out := make([]ChannelConfig, 0, len(rows))
	for i := range rows {
		cfg := ChannelConfig{
			Channel:   rows[i].Channel,
			Enabled:   rows[i].Enabled != 0,
			IsDefault: rows[i].IsDefault != 0,
		}
		if err := s.decryptConfig(&cfg, rows[i].CfgEnc); err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, nil
}

// decryptConfig 解密渠道配置并填充到 cfg。
//
// 兼容早期版本:密文解不开时按明文 JSON 解析(旧数据),下次保存会自动加密。
func (s *Store) decryptConfig(cfg *ChannelConfig, raw string) error {
	if raw == "" {
		return nil
	}
	plain := raw
	if s.box != nil {
		if decoded, err := s.box.Decrypt(raw); err == nil {
			plain = decoded
		}
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(plain), &m); err != nil {
		return fmt.Errorf("platform: parse channel config: %w", err)
	}
	return configFromMap(m, cfg)
}

// SaveChannel 保存某用户的渠道配置(密钥加密后落库)。
func (s *Store) SaveChannel(userID string, cfg ChannelConfig) error {
	m, err := configToMap(cfg)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("platform: marshal channel config: %w", err)
	}
	enc := string(raw)
	if s.box != nil {
		enc, err = s.box.Encrypt(raw)
		if err != nil {
			return err
		}
	}
	enabled, isDefault := int32(0), int32(0)
	if cfg.Enabled {
		enabled = 1
	}
	if cfg.IsDefault {
		isDefault = 1
	}
	row := ChannelModel{
		UserID:    userID,
		Channel:   cfg.Channel,
		Enabled:   enabled,
		IsDefault: isDefault,
		CfgEnc:    enc,
		UpdatedAt: formatTime(time.Now()),
	}
	// upsert:(user_id, channel) 冲突就更新。
	// GORM 会分别翻译成 SQLite 的 ON CONFLICT 与 MySQL 的 ON DUPLICATE KEY UPDATE。
	err = s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "channel"}},
		DoUpdates: clause.AssignmentColumns([]string{"enabled", "is_default", "cfg_enc", "updated_at"}),
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("platform: save channel: %w", err)
	}
	// 默认渠道唯一:保存时清掉同用户其他渠道的默认标记
	if cfg.IsDefault {
		if err := s.db.Model(&ChannelModel{}).
			Where("user_id = ? AND channel <> ?", userID, cfg.Channel).
			Update("is_default", int32(0)).Error; err != nil {
			return fmt.Errorf("platform: reset default channel: %w", err)
		}
	}
	return nil
}

// DefaultChannel 返回用户默认渠道(没有默认值时返回第一个启用的渠道)。
func (s *Store) DefaultChannel(userID string) (string, error) {
	channels, err := s.Channels(userID)
	if err != nil {
		return "", err
	}
	for _, c := range channels {
		if c.IsDefault && c.Enabled {
			return c.Channel, nil
		}
	}
	for _, c := range channels {
		if c.Enabled {
			return c.Channel, nil
		}
	}
	return "", nil
}

// ---------------------------------------------------------------- 订单

// AddOrder 新增订单记录。
//
// 同一商户内商户单号必须唯一:重复时返回 ErrOrderExists。
func (s *Store) AddOrder(order OrderRecord) error {
	now := time.Now()
	order.CreatedAt = now
	order.UpdatedAt = now
	notifyRaw := ""
	if order.Notify != nil {
		if raw, err := json.Marshal(order.Notify); err == nil {
			notifyRaw = string(raw)
		}
	}
	// 先查一次,给出友好错误(而不是把数据库的 UNIQUE 约束错误甩给用户)。
	var exist OrderModel
	err := s.db.Where("user_id = ? AND out_trade_no = ?", order.UserID, order.OutTradeNo).First(&exist).Error
	if err == nil {
		return ErrOrderExists
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("platform: query order: %w", err)
	}
	model := OrderModel{
		OutTradeNo:     order.OutTradeNo,
		UserID:         order.UserID,
		AppID:          order.AppID,
		Channel:        order.Channel,
		TradeType:      order.TradeType,
		Subject:        order.Subject,
		Amount:         order.Amount,
		AmountYuan:     order.AmountYuan,
		Status:         order.Status,
		TransactionID:  order.TransactionID,
		QRCode:         order.QRCode,
		NotifyRaw:      notifyRaw,
		ChannelCreated: boolToInt32(order.ChannelCreated),
		CreatedAt:      formatTime(order.CreatedAt),
		UpdatedAt:      formatTime(order.UpdatedAt),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return fmt.Errorf("platform: insert order: %w", err)
	}
	return nil
}

// UpdateOrder 更新**某个商户自己**的订单记录,找不到返回 false。
//
// userID 是硬性条件:商户单号只在商户内唯一,不带商户过滤就等于放开跨商户改单。
func (s *Store) UpdateOrder(userID, outTradeNo string, fn func(*OrderRecord)) (bool, error) {
	var model OrderModel
	err := s.db.Where("user_id = ? AND out_trade_no = ?", userID, outTradeNo).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("platform: query order: %w", err)
	}
	order := orderFromModel(&model)
	fn(order)
	notifyRaw := ""
	if order.Notify != nil {
		if raw, err := json.Marshal(order.Notify); err == nil {
			notifyRaw = string(raw)
		}
	}
	res := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND out_trade_no = ?", userID, outTradeNo).
		Updates(map[string]any{
			"order_status":   order.Status,
			"transaction_id": order.TransactionID,
			"qrcode":         order.QRCode,
			"notify_raw":     notifyRaw,
			"updated_at":     formatTime(time.Now()),
		})
	if res.Error != nil {
		return false, fmt.Errorf("platform: update order: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// UpdateOrderFields 只更新订单的若干字段(不改状态)。
//
// 用于「先落库、再调渠道」的新流程:渠道下单成功后回填二维码等。
func (s *Store) UpdateOrderFields(userID, outTradeNo string, fields map[string]any) (bool, error) {
	if len(fields) == 0 {
		return true, nil
	}
	updates := map[string]any{"updated_at": formatTime(time.Now())}
	for k, v := range fields {
		updates[k] = v
	}
	res := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND out_trade_no = ?", userID, outTradeNo).
		Updates(updates)
	if res.Error != nil {
		return false, fmt.Errorf("platform: update order: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// DeleteOrder 删除某个商户自己的订单记录。
//
// 只用于「下单失败回滚」:平台先落了订单,调渠道失败 → 把这条记录删掉,
// 商户才能拿**同一个商户单号**重试(否则商户单号被占住,重试报「已存在」)。
//
// 只删「渠道侧还没创建成功」的订单(channel_created = 0),
// 已经在渠道那边建了单的一律不删 —— 那种订单是有钱有单据的。
func (s *Store) DeleteOrder(userID, outTradeNo string) (bool, error) {
	res := s.db.Where("user_id = ? AND out_trade_no = ? AND channel_created = 0", userID, outTradeNo).
		Delete(&OrderModel{})
	if res.Error != nil {
		return false, fmt.Errorf("platform: delete order: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// CompareAndSwapOrderStatus 原子地把订单状态从 expect 改成 next。
//
// 为什么需要它:UpdateOrder 是「读 - 改 - 写」三步,中间任何一步都可能被
// 并发写入插队。真实场景:用户正在「关单」,支付成功的回调同时到了 ——
// 关单先读到 PENDING,回调把状态改成 SUCCESS,关单再写回去就把 SUCCESS
// 盖成了 CLOSED:钱收了,订单却显示已关闭,而且**再也不会有第二个回调**来纠正。
//
// 条件更新把这件事变成原子的:只有「库里状态仍然是我读到的那一个」时才写。
// 写不进去(条件不匹配)就返回 false,调用方重读一次再算。
//
// expect 为空表示不限制当前状态(只写别的字段时用)。
func (s *Store) CompareAndSwapOrderStatus(userID, outTradeNo, expect, next string, extra map[string]any) (bool, error) {
	updates := map[string]any{"updated_at": formatTime(time.Now())}
	if next != "" {
		updates["order_status"] = next
	}
	for k, v := range extra {
		updates[k] = v
	}
	q := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND out_trade_no = ?", userID, outTradeNo)
	if expect != "" {
		q = q.Where("order_status = ?", expect)
	}
	res := q.Updates(updates)
	if res.Error != nil {
		return false, fmt.Errorf("platform: update order: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// Orders 返回某用户的订单列表(最新在前)。
func (s *Store) Orders(userID string) ([]OrderRecord, error) {
	var rows []OrderModel
	err := s.db.Where("user_id = ?", userID).Order("created_at DESC").Limit(200).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("platform: query orders: %w", err)
	}
	out := make([]OrderRecord, 0, len(rows))
	for i := range rows {
		out = append(out, *orderFromModel(&rows[i]))
	}
	s.fillRefundedAmounts(userID, out)
	return out, nil
}

// OrdersPage 按条件分页查询订单,返回当页数据与总条数。
//
// userID 是硬性条件:任何筛选条件都不能越过商户边界。
func (s *Store) OrdersPage(userID string, f OrderFilter) ([]OrderRecord, int64, error) {
	q := s.orderQuery(userID, f)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("platform: count orders: %w", err)
	}
	page, size := normalizePage(f.Page, f.Size)
	rows, err := s.queryOrders(userID, f, page, size)
	if err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}

// orderQuery 拼订单筛选条件(user_id 是硬性条件,任何筛选都越不过商户边界)。
func (s *Store) orderQuery(userID string, f OrderFilter) *gorm.DB {
	q := s.db.Model(&OrderModel{}).Where("user_id = ?", userID)
	if f.Status != "" {
		q = q.Where("order_status = ?", f.Status)
	}
	if f.Channel != "" {
		q = q.Where("channel = ?", f.Channel)
	}
	// 按应用筛选:一个商户多个产品时要能分开看。
	if f.AppID != "" {
		q = q.Where("app_id = ?", f.AppID)
	}
	if f.Keyword != "" {
		like := "%" + escapeLike(f.Keyword) + "%"
		q = q.Where(
			"out_trade_no LIKE ? ESCAPE '\\' OR subject LIKE ? ESCAPE '\\' OR transaction_id LIKE ? ESCAPE '\\'",
			like, like, like)
	}
	// 时间边界归一成 UTC 再比:库里存的是固定宽度 UTC 字符串,
	// 调用方却可能传 UTC 或带偏移的本地时间(不归一就是差 8 小时)。
	if from := normalizeTimeBound(f.From, false); from != "" {
		q = q.Where("created_at >= ?", from)
	}
	if to := normalizeTimeBound(f.To, true); to != "" {
		q = q.Where("created_at <= ?", to)
	}
	return q
}

// queryOrders 按条件取一页订单(不做 Count)。
//
// **不取 notify_raw**:那是渠道回调的原始报文,一单能有好几百字节。
// 列表页(控制台订单页、开放接口订单列表)用不着它 —— 实测 200 条订单里
// 光 notify_raw 就占了 105 KB(占整个响应 60%)。
// 需要回调原文的场合走「订单详情」接口(那条路会取全字段)。
func (s *Store) queryOrders(userID string, f OrderFilter, page, size int) ([]OrderRecord, error) {
	q := s.orderQuery(userID, f).Select(orderListColumns)
	var rows []OrderModel
	err := q.Order("created_at DESC").Limit(size).Offset((page - 1) * size).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("platform: query orders: %w", err)
	}
	out := make([]OrderRecord, 0, len(rows))
	for i := range rows {
		out = append(out, *orderFromModel(&rows[i]))
	}
	s.fillRefundedAmounts(userID, out)
	return out, nil
}

// fillRefundedAmounts 批量填「已退金额 / 可退金额」。
//
// 一次 group by 查询搞定整页 —— 逐单查退款是 N+1,订单页一翻就是几十次查询。
func (s *Store) fillRefundedAmounts(userID string, orders []OrderRecord) {
	if len(orders) == 0 {
		return
	}
	nos := make([]string, 0, len(orders))
	for i := range orders {
		nos = append(nos, orders[i].OutTradeNo)
	}
	type row struct {
		OutTradeNo string
		Total      int64
	}
	var rows []row
	err := s.db.Model(&RefundModel{}).
		Select("out_trade_no AS out_trade_no, COALESCE(SUM(amount), 0) AS total").
		Where("user_id = ? AND out_trade_no IN ? AND (refund_status IN ? OR refund_status IS NULL)",
			userID, nos, refundCountedStatuses).
		Group("out_trade_no").Scan(&rows).Error
	if err != nil {
		// 查不到就当没退过:退款金额只是个参考值,不该让整个列表页挂掉。
		s.db.Logger.Error(nil, "统计订单已退金额失败: "+err.Error())
		return
	}
	byNo := make(map[string]int64, len(rows))
	for _, r := range rows {
		byNo[r.OutTradeNo] = r.Total
	}
	for i := range orders {
		refunded := byNo[orders[i].OutTradeNo]
		orders[i].RefundedAmount = refunded
		remaining := orders[i].Amount - refunded
		if remaining < 0 {
			remaining = 0
		}
		orders[i].RefundableAmount = remaining
	}
}

// orderListColumns 订单**列表**要取的列。
//
// 刻意不含 notify_raw(渠道回调原文):列表页不需要它,而它占响应体积的一大半。
var orderListColumns = []string{
	"out_trade_no", "user_id", "app_id", "channel", "trade_type", "subject",
	"amount", "amount_yuan", "order_status", "transaction_id",
	"qrcode", "channel_created", "created_at", "updated_at",
}

// escapeLike 转义 LIKE 通配符。
//
// 不转义的话,关键字里的 % 和 _ 会被当成通配符:
//   - 搜 "100%" 会匹配到「1000」「1001」…… 完全不相关的订单;
//   - 搜 "_" 会匹配**任何**单字符,等于全表扫。
//
// 转义符用 '\',所以查询里必须同时写 ESCAPE '\'。
func escapeLike(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return r.Replace(s)
}

// AllOrdersForExport 按筛选条件取全部订单(导出用,上限 maxExportRows 条)。
//
// 注意:**不能**靠 OrdersPage 来拿 —— 它的 size 会被 normalizePage 砍到 200,
// 导出上限形同虚设(要 5000 条只会拿到 200 条,还不报错)。
// 这里自己拼查询,自己控上限。
func (s *Store) AllOrdersForExport(userID string, f OrderFilter) ([]OrderRecord, error) {
	return s.queryOrders(userID, f, 1, maxExportRows)
}

// maxExportRows 单次导出的最大行数。
const maxExportRows = 5000

// todayStart 取「今日」的起点(返回库里那种固定宽度的 UTC 字符串)。
//
// 注意不能用 Truncate(24*time.Hour):那是按 UTC 对齐的,在东八区要等到早上 8 点
// 才换日,「今日成交」会莫名其妙地对不上。这里按本地时区取当天零点,
// 再转成 UTC 存的样子去比(时间列统一是 UTC)。
//
// 单独抽成函数是为了让测试能直接比它 —— 之前测试里复刻了一份算法,
// 生产代码改了口径测试照样绿。
func todayStart(now time.Time) string {
	return formatTime(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()))
}

// StreamOrdersForExport 按筛选条件**流式**遍历订单(导出用)。
//
// 为什么不一次性取回:5000 条订单连同回调原文,一次性进内存是好几 MB。
// 导出是「边查边写」的活儿,流式更省内存,响应也能更早开始下载。
//
// 回调函数返回错误就停止遍历(写失败、客户端断连都会走到这里)。
func (s *Store) StreamOrdersForExport(userID string, f OrderFilter, fn func(OrderRecord) error) error {
	q := s.orderQuery(userID, f)
	rows, err := q.Order("created_at DESC").Limit(maxExportRows).Rows()
	if err != nil {
		return fmt.Errorf("platform: stream orders for export: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m OrderModel
		if err := s.db.ScanRows(rows, &m); err != nil {
			return fmt.Errorf("platform: scan order for export: %w", err)
		}
		if err := fn(*orderFromModel(&m)); err != nil {
			return err
		}
	}
	return rows.Err()
}

// normalizePage 规整分页参数(页码从 1 开始,每页最多 200 条)。
func normalizePage(page, size int) (int, int) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	if size > 200 {
		size = 200
	}
	return page, size
}

// ---------------------------------------------------------------- 退款

// AddRefund 记录一笔退款流水。
//
// 业务上的退款请走 ReserveRefund(占位 + 幂等 + 事务),这个方法只用于
// 补录与测试:它不做占位,也不检查金额上限。
func (s *Store) AddRefund(rec RefundRecord) error {
	now := formatTime(time.Now())
	model := RefundModel{
		RefundID:    newID("r"),
		UserID:      rec.UserID,
		OutTradeNo:  rec.OutTradeNo,
		OutRefundNo: rec.OutRefundNo,
		Channel:     rec.Channel,
		AppID:       rec.AppID,
		Amount:      rec.Amount,
		Status:      rec.Status,
		Reason:      rec.Reason,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.db.Create(&model).Error; err != nil {
		return fmt.Errorf("platform: insert refund: %w", err)
	}
	rec.RefundID = model.RefundID
	rec.CreatedAt = parseNanoTime(model.CreatedAt)
	return nil
}

// refundExceedsError 退款金额超过可退余额(订单金额 - 已退金额)。
type refundExceedsError struct {
	refunded int64
	amount   int64
	order    int64
}

// Error 实现 error。
func (e *refundExceedsError) Error() string {
	return fmt.Sprintf("platform: 累计退款金额将超过订单金额(已退 %d 分,本次 %d 分,订单 %d 分)",
		e.refunded, e.amount, e.order)
}

// refundConflictError 同一个退款单号被用于了不同的金额。
type refundConflictError struct {
	outRefundNo string
	existing    int64
	incoming    int64
}

// Error 实现 error。
func (e *refundConflictError) Error() string {
	return fmt.Sprintf("platform: 退款单号 %s 已经用于 %d 分的退款,不能再改成 %d 分",
		e.outRefundNo, e.existing, e.incoming)
}

// ReserveRefund 在事务里「占位」一笔退款。
//
// 为什么必须先占位:退款是**先给渠道、后退账**的,中间任何一步崩了
// (进程被杀、渠道超时、连接断掉)都会出现「钱已经退了,平台账上没有」——
// 商户按账上记录再退一次就是二次出款。所以顺序必须是:
// 先占一行(状态 PENDING,计入已退金额)→ 再调渠道 → 最后回填结果。
//
// 幂等:同一个 (user_id, out_refund_no) 已经有流水时,直接返回那笔,
// created=false —— 调用方据此知道「不用再调渠道」,原样返回上次的结果。
//
// 金额上限在事务里校验(订单金额 - 已退金额),并发退款会被数据库行锁串行化。
func (s *Store) ReserveRefund(rec RefundRecord) (RefundRecord, bool, error) {
	// 同一订单的退款在进程内串行:
	// SQLite 只有一个写者,并发写事务会直接报 database is locked;
	// MySQL 多实例部署靠事务里的行锁(InnoDB 行级锁)保证。
	unlock := s.lockRefund(rec.UserID, rec.OutTradeNo)
	defer unlock()

	var out RefundRecord
	created := false
	err := s.db.Transaction(func(tx *gorm.DB) error {
		// 1) 幂等:同一个退款单号只允许一笔。
		var exist RefundModel
		var reopen *RefundModel
		err := tx.Where("user_id = ? AND out_refund_no = ?", rec.UserID, rec.OutRefundNo).
			First(&exist).Error
		if err == nil {
			// 上一笔是**失败**的:钱没退出去,额度也没占。
			// 这时允许用同一个退款单号重试(网关惯例:幂等键不变=重试),
			// 把这行重新置为 PENDING 再走一遍渠道。
			// 金额必须一致 —— 同一个单号代表同一笔退款,不许中途改金额。
			if exist.Status == refundStatusFailed {
				if exist.Amount != rec.Amount {
					return &refundConflictError{
						outRefundNo: rec.OutRefundNo,
						existing:    exist.Amount,
						incoming:    rec.Amount,
					}
				}
				reopen = &exist
			} else {
				// 已经有一笔在途/成功的退款:原样返回,不再出款。
				out = refundFromModel(&exist)
				return nil
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("platform: query refund: %w", err)
		}
		now := formatTime(time.Now())
		// 2) 锁住订单行:同一订单的并发退款在这里排队(MySQL 行锁)。
		res := tx.Model(&OrderModel{}).
			Where("user_id = ? AND out_trade_no = ?", rec.UserID, rec.OutTradeNo).
			UpdateColumn("updated_at", now)
		if res.Error != nil {
			return fmt.Errorf("platform: lock order: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			// 更新时间可能恰好没变(MySQL 只统计「实际改变」的行),再确认一次。
			var probe OrderModel
			err := tx.Where("user_id = ? AND out_trade_no = ?", rec.UserID, rec.OutTradeNo).
				First(&probe).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrOrderNotFound
			}
			if err != nil {
				return fmt.Errorf("platform: query order: %w", err)
			}
		}
		// 3) 读订单金额:以库里的为准,不信任调用方传的值。
		var order OrderModel
		if err := tx.Where("user_id = ? AND out_trade_no = ?", rec.UserID, rec.OutTradeNo).
			First(&order).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrOrderNotFound
			}
			return fmt.Errorf("platform: query order: %w", err)
		}
		// 4) 累计已退(含占位与渠道处理中的)。
		var refunded int64
		if err := tx.Model(&RefundModel{}).
			Where("user_id = ? AND out_trade_no = ? AND (refund_status IN ? OR refund_status IS NULL)",
				rec.UserID, rec.OutTradeNo, refundCountedStatuses).
			Select("COALESCE(SUM(amount), 0)").Scan(&refunded).Error; err != nil {
			return fmt.Errorf("platform: sum refunds: %w", err)
		}
		if refunded+rec.Amount > order.Amount {
			return &refundExceedsError{refunded: refunded, amount: rec.Amount, order: order.Amount}
		}
		// 5) 写占位流水。
		//    重试(上一笔 FAILED)时复用原来那行,不新插 —— 唯一索引
		//    (user_id, out_refund_no) 也不允许同一单号出现第二行。
		if reopen != nil {
			res := tx.Model(&RefundModel{}).
				Where("refund_id = ?", reopen.RefundID).
				Updates(map[string]any{
					"refund_status": refundStatusPending,
					"reason":        rec.Reason,
					"channel":       rec.Channel,
					"updated_at":    now,
				})
			if res.Error != nil {
				return fmt.Errorf("platform: reopen refund: %w", res.Error)
			}
			reopen.Status = refundStatusPending
			reopen.Reason = rec.Reason
			reopen.Channel = rec.Channel
			reopen.UpdatedAt = now
			out = refundFromModel(reopen)
			created = true
			return nil
		}
		model := RefundModel{
			RefundID:    newID("r"),
			UserID:      rec.UserID,
			OutTradeNo:  rec.OutTradeNo,
			OutRefundNo: rec.OutRefundNo,
			// 应用归属**以订单为准**:退款是订单的下游,
			// 订单属于哪个应用,退款就属于哪个应用。
			AppID:     order.AppID,
			Channel:   rec.Channel,
			Amount:    rec.Amount,
			Status:    refundStatusPending,
			Reason:    rec.Reason,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := tx.Create(&model).Error; err != nil {
			return fmt.Errorf("platform: insert refund: %w", err)
		}
		out = refundFromModel(&model)
		created = true
		return nil
	})
	if err != nil {
		return RefundRecord{}, false, err
	}
	return out, created, nil
}

// FinishRefund 回填退款结果(占位 → 成功 / 处理中 / 失败)。
func (s *Store) FinishRefund(userID, outRefundNo, status string) error {
	res := s.db.Model(&RefundModel{}).
		Where("user_id = ? AND out_refund_no = ?", userID, outRefundNo).
		Updates(map[string]any{
			"refund_status": status,
			"updated_at":    formatTime(time.Now()),
		})
	if res.Error != nil {
		return fmt.Errorf("platform: update refund: %w", res.Error)
	}
	return nil
}

// RefundByNo 按商户退款单号取退款流水,找不到返回 nil。
func (s *Store) RefundByNo(userID, outRefundNo string) (*RefundRecord, error) {
	var model RefundModel
	err := s.db.Where("user_id = ? AND out_refund_no = ?", userID, outRefundNo).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("platform: query refund: %w", err)
	}
	rec := refundFromModel(&model)
	return &rec, nil
}

// RefundedAmount 返回某订单的累计退款金额(分)。
//
// 只统计**已经提交出去**的退款(占位 / 成功 / 渠道处理中):
// 失败的退款钱没退,不该占额度;占位与处理中的钱可能已经出了,必须占额度。
func (s *Store) RefundedAmount(userID, outTradeNo string) (int64, error) {
	var total int64
	err := s.db.Model(&RefundModel{}).
		Where("user_id = ? AND out_trade_no = ? AND (refund_status IN ? OR refund_status IS NULL)",
			userID, outTradeNo, refundCountedStatuses).
		Select("COALESCE(SUM(amount), 0)").Scan(&total).Error
	if err != nil {
		return 0, fmt.Errorf("platform: sum refunds: %w", err)
	}
	return total, nil
}

// lockRefund 拿订单级退款锁,返回释放函数。
//
// 用 sync.Map 而不是一把全局锁:不同订单的退款互不阻塞。
func (s *Store) lockRefund(userID, outTradeNo string) func() {
	key := userID + "|" + outTradeNo
	value, _ := s.refundLocks.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// Refunds 返回某用户的退款流水(最新在前)。
func (s *Store) Refunds(userID string, limit int) ([]RefundRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []RefundModel
	err := s.db.Where("user_id = ?", userID).Order("created_at DESC").Limit(limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("platform: query refunds: %w", err)
	}
	out := make([]RefundRecord, 0, len(rows))
	for i := range rows {
		out = append(out, refundFromModel(&rows[i]))
	}
	return out, nil
}

// Stats 统计某商户的交易情况(全部应用)。
func (s *Store) Stats(userID string) (Stats, error) {
	return s.StatsForApp(userID, "")
}

// StatsForApp 统计某商户的交易情况,可限定在某个应用内。
//
// appID 为空 = 全部应用(含未分类)。订单与退款**两边都要按 app_id 过滤** ——
// 只过滤一边的话,退款金额会算到别的应用头上,净收入直接是错的。
func (s *Store) StatsForApp(userID, appID string) (Stats, error) {
	var out Stats
	base := s.db.Model(&OrderModel{}).Where("user_id = ?", userID)
	successQ := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND order_status = ?", userID, string(payment.TradeStatusSuccess))
	failedQ := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND order_status IN ?", userID,
			[]string{string(payment.TradeStatusFailed), string(payment.TradeStatusClosed)})
	refundQ := s.db.Model(&RefundModel{}).
		Where("user_id = ? AND refund_status IN ?", userID, refundCountedStatuses)
	if appID != "" {
		base = base.Where("app_id = ?", appID)
		successQ = successQ.Where("app_id = ?", appID)
		failedQ = failedQ.Where("app_id = ?", appID)
		refundQ = refundQ.Where("app_id = ?", appID)
	}
	if err := base.Count(&out.OrderCount).Error; err != nil {
		return out, fmt.Errorf("platform: count orders: %w", err)
	}
	if err := successQ.Count(&out.SuccessCount).Error; err != nil {
		return out, fmt.Errorf("platform: count success orders: %w", err)
	}
	if err := failedQ.Count(&out.FailedCount).Error; err != nil {
		return out, fmt.Errorf("platform: count failed orders: %w", err)
	}
	if err := successQ.Select("COALESCE(SUM(amount), 0)").Scan(&out.TotalAmount).Error; err != nil {
		return out, fmt.Errorf("platform: sum paid amount: %w", err)
	}
	// 退款笔数 / 退款金额:只算**真的退了钱**的那些。
	//
	// 口径必须和「超额退款保护」(ReserveRefund 用的同一份
	// refundCountedStatuses)保持一致:
	// 失败的退款(FAILED)那笔钱**还在商户手里**,把它算进「已退款」,
	// 净收入会被凭空抹掉一块 —— 报表直接是错的。
	if err := refundQ.Count(&out.RefundCount).Error; err != nil {
		return out, fmt.Errorf("platform: count refunds: %w", err)
	}
	if err := refundQ.Select("COALESCE(SUM(amount), 0)").Scan(&out.RefundAmount).Error; err != nil {
		return out, fmt.Errorf("platform: sum refunds: %w", err)
	}
	// 今日:按订单创建时间的自然日统计(时间列存的是固定宽度 UTC 字符串)。
	//
	// 注意不能用 Truncate(24*time.Hour):那是按 UTC 对齐的,在东八区要等到早上 8 点
	// 才换日,「今日成交」会莫名其妙地对不上。这里按本地时区取当天零点,
	// 再转成 UTC 存的样子去比(时间列统一是 UTC)。
	now := time.Now()
	todayFrom := todayStart(now)
	todayQ := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND created_at >= ?", userID, todayFrom)
	todaySuccessQ := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND created_at >= ? AND order_status = ?",
			userID, todayFrom, string(payment.TradeStatusSuccess))
	if appID != "" {
		todayQ = todayQ.Where("app_id = ?", appID)
		todaySuccessQ = todaySuccessQ.Where("app_id = ?", appID)
	}
	if err := todayQ.Count(&out.TodayCount).Error; err != nil {
		return out, fmt.Errorf("platform: count today orders: %w", err)
	}
	if err := todaySuccessQ.Select("COALESCE(SUM(amount), 0)").Scan(&out.TodayAmount).Error; err != nil {
		return out, fmt.Errorf("platform: sum today amount: %w", err)
	}
	out.NetAmount = out.TotalAmount - out.RefundAmount
	if out.OrderCount > 0 {
		out.SuccessRate = float64(out.SuccessCount) / float64(out.OrderCount)
	}
	return out, nil
}

// OrderByTradeNo 查询某用户自己的订单(找不到返回 nil,nil)。
//
// 用于钱包扣款时按订单记录取金额,避免客户端传错金额。
func (s *Store) OrderByTradeNo(userID, outTradeNo string) (*OrderRecord, error) {
	var model OrderModel
	err := s.db.Where("user_id = ? AND out_trade_no = ?", userID, outTradeNo).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("platform: query order: %w", err)
	}
	order := orderFromModel(&model)
	// 单条也要填已退金额 —— 查单接口返回的 OrderRecord 要和列表口径一致。
	one := []OrderRecord{*order}
	s.fillRefundedAmounts(userID, one)
	*order = one[0]
	return order, nil
}

// ---------------------------------------------------------------- 二次验证

// CreateMFACode 保存一次性验证码(只存带密钥散列)。
func (s *Store) CreateMFACode(userID, purpose, code string, ttl time.Duration) error {
	now := time.Now()
	model := MFACodeModel{
		CodeHash:  s.hasher.sum(code),
		UserID:    userID,
		Purpose:   purpose,
		ExpiresAt: formatTime(now.Add(ttl)),
		Used:      0,
		Attempts:  0,
		CreatedAt: formatTime(now),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return fmt.Errorf("platform: insert mfa code: %w", err)
	}
	return nil
}

// ConsumeMFACode 校验并作废一次性验证码。
//
// 校验通过返回 true;验证码不存在、已使用、已过期、错误次数超限都返回 false。
//
// 错误次数会计到「该用户该用途当前生效的验证码」上:超过 maxAttempts 次
// 直接把验证码作废,挡住 6 位数字的暴力枚举。
func (s *Store) ConsumeMFACode(userID, purpose, code string) bool {
	if code == "" {
		return false
	}
	hash := s.hasher.sum(code)
	var model MFACodeModel
	err := s.db.Where("code_hash = ?", hash).First(&model).Error
	if err != nil {
		// 没匹配上:多半是验证码输错了,给当前生效的验证码记一次失败。
		s.registerMFAAttempt(userID, purpose)
		return false
	}
	if model.UserID != userID || model.Purpose != purpose || model.Used != 0 {
		return false
	}
	exp, err := parseTime(model.ExpiresAt)
	if err != nil || time.Now().After(exp) {
		return false
	}
	if int(model.Attempts) >= maxMFAAttempts {
		return false
	}
	// 带 used = 0 条件更新:并发下只有一个请求能把验证码消费掉。
	res := s.db.Model(&MFACodeModel{}).
		Where("code_hash = ? AND used = 0", hash).
		Update("used", 1)
	if res.Error != nil {
		return false
	}
	return res.RowsAffected == 1
}

// PeekMFACode 校验一次性验证码但**不作废**它。
//
// 用在哪:「换绑二次验证」是两步 —— 先生成新密钥(此时用户还没绑定完),
// 再提交绑定。中间那一步要确认「当前绑定的人」,但那一步不该把验证码用掉:
// 真正落地绑定时还要再校验一次(那时才作废)。
//
// 失败次数照常计数(和 ConsumeMFACode 一样),挡住暴力枚举。
func (s *Store) PeekMFACode(userID, purpose, code string) bool {
	if code == "" {
		return false
	}
	hash := s.hasher.sum(code)
	var model MFACodeModel
	err := s.db.Where("code_hash = ?", hash).First(&model).Error
	if err != nil {
		s.registerMFAAttempt(userID, purpose)
		return false
	}
	if model.UserID != userID || model.Purpose != purpose || model.Used != 0 {
		return false
	}
	exp, err := parseTime(model.ExpiresAt)
	if err != nil || time.Now().After(exp) {
		return false
	}
	if int(model.Attempts) >= maxMFAAttempts {
		return false
	}
	return true
}

// registerMFAAttempt 给该用户该用途的生效验证码记一次错误尝试。
//
// 超过上限就把验证码作废(used = 1),用户需要重新获取。
func (s *Store) registerMFAAttempt(userID, purpose string) {
	if userID == "" || purpose == "" {
		return
	}
	s.db.Model(&MFACodeModel{}).
		Where("user_id = ? AND purpose = ? AND used = 0", userID, purpose).
		UpdateColumn("attempts", gorm.Expr("attempts + 1"))
	// 错误次数到上限:作废当前验证码。
	s.db.Model(&MFACodeModel{}).
		Where("user_id = ? AND purpose = ? AND used = 0 AND attempts >= ?", userID, purpose, maxMFAAttempts).
		Update("used", 1)
}

// CreateVerifyToken 生成敏感操作票据(返回明文令牌,库里只存带密钥散列)。
func (s *Store) CreateVerifyToken(userID string, ttl time.Duration) (string, error) {
	token := newID("v") + newID("")
	now := time.Now()
	model := VerifyTokenModel{
		TokenHash: s.hasher.sum(token),
		UserID:    userID,
		ExpiresAt: formatTime(now.Add(ttl)),
		CreatedAt: formatTime(now),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return "", fmt.Errorf("platform: insert verify token: %w", err)
	}
	return token, nil
}

// verifyReuseGrace 票据首次使用后的短暂复用窗口。
//
// 为什么要留这个窗口:控制台里「保存配置 → 立刻测试」是一串连续操作,
// 每一步都要重新输一遍口令 + 验证码的话体验很差。
// 但完全不复用又不行:一枚 10 分钟有效、可反复使用的票据一旦泄露
// (浏览器插件、XSS、代理日志),攻击者就能在 10 分钟内反复改配置、退款。
//
// 所以:票据**用后即焚**,只在首次使用后的 60 秒内允许再连用几笔,
// 超过就不再认 —— 泄露窗口从 10 分钟压到 1 分钟。
const verifyReuseGrace = 60 * time.Second

// VerifyToken 校验敏感操作票据,返回所属用户。
//
// 票据用后即焚:第一次用掉之后只保留一个很短的复用窗口(见 verifyReuseGrace)。
func (s *Store) VerifyToken(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	hash := s.hasher.sum(token)
	var model VerifyTokenModel
	err := s.db.Where("token_hash = ?", hash).First(&model).Error
	if err != nil {
		return "", false
	}
	now := time.Now()
	exp, err := parseTime(model.ExpiresAt)
	if err != nil || now.After(exp) {
		// 过期票据顺手清掉,别在库里堆着。
		s.db.Where("token_hash = ?", hash).Delete(&VerifyTokenModel{})
		return "", false
	}
	if model.UsedAt != "" {
		// 已经用过了:只在复用窗口内认。
		usedAt, err := parseTime(model.UsedAt)
		if err != nil || now.Sub(usedAt) > verifyReuseGrace {
			return "", false
		}
		return model.UserID, true
	}
	// 首次使用:标记成「已用」。
	res := s.db.Model(&VerifyTokenModel{}).
		Where("token_hash = ? AND (used_at IS NULL OR used_at = '')", hash).
		Update("used_at", formatTime(now))
	if res.Error != nil || res.RowsAffected == 0 {
		// 并发请求:另一个请求已经把这枚票据用掉了。
		return "", false
	}
	return model.UserID, true
}

// ---------------------------------------------------------------- 审计

// AddAudit 写审计日志。
func (s *Store) AddAudit(userID, action, target, detail, clientIP string) error {
	model := AuditLogModel{
		LogID:     newID("a"),
		UserID:    userID,
		Action:    action,
		Target:    target,
		Detail:    detail,
		ClientIP:  clientIP,
		CreatedAt: formatTime(time.Now()),
	}
	if err := s.db.Create(&model).Error; err != nil {
		return fmt.Errorf("platform: insert audit log: %w", err)
	}
	return nil
}

// AuditLogs 返回某用户的审计日志(最新在前)。
func (s *Store) AuditLogs(userID string, limit int) ([]AuditLog, error) {
	logs, _, err := s.AuditLogsPage(userID, limit, 0, "")
	return logs, err
}

// AuditLogsPage 分页查询审计日志,可按动作过滤。
//
// action 非空时只返回该动作的记录;page/size 为 0 时按「取最新 limit 条」处理。
func (s *Store) AuditLogsPage(userID string, limit, page int, action string) ([]AuditLog, int64, error) {
	q := s.db.Model(&AuditLogModel{}).Where("user_id = ?", userID)
	if action != "" {
		q = q.Where("action = ?", action)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("platform: count audit logs: %w", err)
	}
	size := limit
	if size <= 0 {
		size = 50
	}
	if size > 200 {
		size = 200
	}
	if page <= 0 {
		page = 1
	}
	var rows []AuditLogModel
	err := q.Order("created_at DESC").Limit(size).Offset((page - 1) * size).Find(&rows).Error
	if err != nil {
		return nil, 0, fmt.Errorf("platform: query audit logs: %w", err)
	}
	out := make([]AuditLog, 0, len(rows))
	for i := range rows {
		out = append(out, AuditLog{
			LogID:     rows[i].LogID,
			UserID:    rows[i].UserID,
			Action:    rows[i].Action,
			Target:    rows[i].Target,
			Detail:    rows[i].Detail,
			ClientIP:  rows[i].ClientIP,
			CreatedAt: parseNanoTime(rows[i].CreatedAt),
		})
	}
	return out, total, nil
}

// ---------------------------------------------------------------- 模型转换

// userFromModel 把数据模型转成对外的 User。
//
// 库里不存明文令牌,所以 AccessToken 只有在「刚生成」时才有值。
func userFromModel(m *UserModel) *User {
	return &User{
		UserID:            m.UserID,
		UserName:          m.UserName,
		PasswordHash:      m.PasswordHash,
		NotifyBaseURL:     m.NotifyBaseURL,
		MFAType:           m.MFAType,
		MFATarget:         m.MFATarget,
		MFABackupType:     m.MFABackupType,
		MFABackupTarget:   m.MFABackupTarget,
		MFAEnabled:        m.MFAEnabled != 0,
		RequireAppBinding: m.RequireAppBinding != 0,
		FailedLogins:      m.FailedLogins,
		LockedUntil:       m.LockedUntil,
		Disabled:          m.Disabled != 0,
		CreatedAt:         parseNanoTime(m.CreatedAt),
		UpdatedAt:         parseNanoTime(m.UpdatedAt),
	}
}

// refundFromModel 把数据模型转成对外的 RefundRecord。
func refundFromModel(m *RefundModel) RefundRecord {
	return RefundRecord{
		RefundID:    m.RefundID,
		UserID:      m.UserID,
		OutTradeNo:  m.OutTradeNo,
		OutRefundNo: m.OutRefundNo,
		AppID:       m.AppID,
		Channel:     m.Channel,
		Amount:      m.Amount,
		AmountYuan:  payment.CentsToYuan(m.Amount),
		Status:      m.Status,
		Reason:      m.Reason,
		CreatedAt:   parseNanoTime(m.CreatedAt),
	}
}

// orderFromModel 把数据模型转成对外的 OrderRecord。
func orderFromModel(m *OrderModel) *OrderRecord {
	order := OrderRecord{
		OutTradeNo:     m.OutTradeNo,
		UserID:         m.UserID,
		AppID:          m.AppID,
		Channel:        m.Channel,
		TradeType:      m.TradeType,
		Subject:        m.Subject,
		Amount:         m.Amount,
		AmountYuan:     m.AmountYuan,
		Status:         m.Status,
		TransactionID:  m.TransactionID,
		QRCode:         m.QRCode,
		CreatedAt:      parseNanoTime(m.CreatedAt),
		UpdatedAt:      parseNanoTime(m.UpdatedAt),
		ChannelCreated: m.ChannelCreated == 1,
	}
	if m.NotifyRaw != "" {
		var notify payment.Notify
		if err := json.Unmarshal([]byte(m.NotifyRaw), &notify); err == nil {
			order.Notify = &notify
		}
	}
	return &order
}

// parseNanoTime 见 util.go(统一时间格式的解析入口)。
