package platform

// 数据模型(GORM)。
//
// 表名与列名与早期手写 SQL 版本完全一致,已有数据库可以直接复用,不需要导数据。
//
// 约定:
//   - 时间列沿用 VARCHAR(40) 存 RFC3339Nano 字符串(与旧表一致,避免时区/类型差异);
//   - 布尔列沿用 INT(与旧表一致),避免 AutoMigrate 把 INT 改成 tinyint 而动到存量表;
//   - 订单表主键是 (user_id, out_trade_no):商户单号只在**本商户内**唯一,
//     多租户隔离落在存储层,而不是只靠代码记得带 user_id 过滤。

// UserModel 用户(商户)。
type UserModel struct {
	UserID   string `gorm:"column:user_id;primaryKey;size:64" json:"user_id"`
	UserName string `gorm:"column:user_name;size:64" json:"user_name"`
	// AccessToken 旧版明文令牌列(已废弃,只保留列以便老库迁移,新数据一律为空)。
	AccessToken string `gorm:"column:access_token;size:64" json:"-"`
	// AccessTokenHash 访问令牌的带密钥散列(HMAC-SHA256),库里不存明文令牌。
	AccessTokenHash string `gorm:"column:access_token_hash;size:64" json:"-"`
	PasswordHash    string `gorm:"column:password_hash;size:255" json:"-"`
	// NotifyBaseURL 商户自己配置的对外访问地址(回调地址前缀),空表示用平台默认地址。
	NotifyBaseURL string `gorm:"column:notify_base_url;size:255" json:"notify_base_url"`
	MFAType       string `gorm:"column:mfa_type;size:16" json:"mfa_type"`
	MFATarget     string `gorm:"column:mfa_target;size:128" json:"mfa_target"`
	// MFABackupType 备用二次验证方式(email/sms),空表示没绑。
	//
	// 用途:验证器 App 丢了、手机换了、App 被误删时的「第二条腿」——
	// 登录时可以改用发到备用地址的验证码,不至于把账户彻底锁死。
	// 只在**主**方式之外多一条路,不会降低主方式的要求。
	MFABackupType string `gorm:"column:mfa_backup_type;size:16" json:"mfa_backup_type"`
	// MFABackupTarget 备用二次验证的接收地址。
	MFABackupTarget string `gorm:"column:mfa_backup_target;size:128" json:"mfa_backup_target"`
	// MFASecretEnc TOTP 共享密钥的密文(仅 MFAType=totp 时有值)。
	//
	// 加密入库:共享密钥等价于「能生成全部动态码」的东西,明文落库等于
	// 二次验证形同虚设。
	MFASecretEnc string `gorm:"column:mfa_secret_enc;type:text" json:"-"`
	// MFAPendingSecretEnc 待确认的 TOTP 密钥密文。
	//
	// 「生成密钥 → 扫码 → 输入动态码 → 启用」是四步,中间态必须暂存。
	// 用独立的列存,免得用户点了一下「生成密钥」就把**正在用**的密钥冲掉,
	// 结果旧 App 立刻失效、新的又还没启用,账户直接进不去。
	MFAPendingSecretEnc string `gorm:"column:mfa_pending_secret_enc;type:text" json:"-"`
	MFAEnabled          int32  `gorm:"column:mfa_enabled" json:"mfa_enabled"`
	// MFATotpCounter 已经用掉的 TOTP 时间片(counter)。
	//
	// 用途:保证同一个动态码只能用一次。RFC 6238 §5.2 明确要求:
	// 「The verifier MUST NOT accept the second attempt of the OTP after the
	// successful validation has been issued for the first OTP」。
	// 不记的话,同一个 6 位码在它的时间片(±1 片,最多 90 秒)内可以被反复重放。
	MFATotpCounter int64 `gorm:"column:mfa_totp_counter" json:"-"`
	// FailedLogins 连续登录失败次数,达到上限后锁定一段时间。
	FailedLogins int32 `gorm:"column:failed_logins"`
	// LockedUntil 锁定截止时间(RFC3339Nano),空表示未锁定。
	LockedUntil string `gorm:"column:locked_until;size:40"`
	// TokenExpiresAt 访问令牌过期时间(RFC3339Nano),空表示永不过期。
	TokenExpiresAt string `gorm:"column:token_expires_at;size:40"`
	// Disabled 是否被平台禁用(1 = 已禁用)。被禁用的商户不能登录,已有令牌也失效。
	Disabled int32 `gorm:"column:disabled"`
	// RequireAppBinding 是否强制订单绑定应用(1 = 强制)。
	//
	// 强制之后,这个商户的**每一笔**支付都必须归属某个应用(产品):
	// 没带 app_id 的下单直接拒绝,没绑应用的密钥也不能用来下单。
	// 不强制的话订单会落进「未分类」,事后根本分不清是哪个产品的钱。
	//
	// 老账户这一列是 0(不强制),避免升级就把别人的下单打断;
	// 新建的账户默认 1(见 CreateUser)。
	RequireAppBinding int32  `gorm:"column:require_app_binding"`
	CreatedAt         string `gorm:"column:created_at;size:40" json:"created_at"`
	UpdatedAt         string `gorm:"column:updated_at;size:40" json:"updated_at"`
}

// TableName 表名。
func (UserModel) TableName() string { return "pay_users" }

// ChannelModel 商户渠道配置(密文存 cfg_enc)。
type ChannelModel struct {
	UserID    string `gorm:"column:user_id;primaryKey;size:64"`
	Channel   string `gorm:"column:channel;primaryKey;size:32"`
	Enabled   int32  `gorm:"column:enabled"`
	IsDefault int32  `gorm:"column:is_default"`
	CfgEnc    string `gorm:"column:cfg_enc;type:text"`
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (ChannelModel) TableName() string { return "pay_user_channels" }

// AppModel 商户的应用 / 产品。
//
// 一个商户经常有多个产品(APP、小程序、H5……),每个产品下单时都可能带不同的
// 应用标识。没有这一层的话,所有产品的订单混在一张表里,
// 「按产品看订单 / 看统计 / 出报表」全都做不了 ——
// 只能靠商户自己在 out_trade_no 里编产品号,再在导出后人工拆。
//
// AppID 是平台生成的应用标识(商户可以自己指定名称,不能自己指定 ID)。
type AppModel struct {
	AppID string `gorm:"column:app_id;primaryKey;size:64"`
	// UserID 所属商户。
	UserID string `gorm:"column:user_id;size:64;index:idx_app_user,priority:1;uniqueIndex:uk_app_user_name,priority:1"`
	// Name 应用名称(同一商户下唯一)。
	Name string `gorm:"column:name;size:128;uniqueIndex:uk_app_user_name,priority:2"`
	// Remark 备注。
	Remark    string `gorm:"column:remark;size:255"`
	CreatedAt string `gorm:"column:created_at;size:40"`
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (AppModel) TableName() string { return "pay_apps" }

// AppChannelModel 某个应用**专属**的渠道配置。
//
// 为什么单独一张表,而不是给 pay_user_channels 加一列:
//   - pay_user_channels 的主键是 (user_id, channel),加列就得改主键,
//     而改主键在 SQLite/MySQL 上都要重建表(风险大、老数据要搬);
//   - 单独一张表的话,「全局渠道配置」一行不动,老商户完全不受影响,
//     新表由 AutoMigrate 直接建出来。
//
// 查找规则:某个应用下单时,先用**这个应用专属**的配置;
// 没配过的渠道回退到全局配置(同一套密钥先凑合用)。
type AppChannelModel struct {
	AppID     string `gorm:"column:app_id;primaryKey;size:64"`
	UserID    string `gorm:"column:user_id;primaryKey;size:64"`
	Channel   string `gorm:"column:channel;primaryKey;size:32"`
	Enabled   int32  `gorm:"column:enabled"`
	IsDefault int32  `gorm:"column:is_default"`
	CfgEnc    string `gorm:"column:cfg_enc;type:text"`
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (AppChannelModel) TableName() string { return "pay_app_channels" }

// OrderModel 订单。
type OrderModel struct {
	OutTradeNo    string `gorm:"column:out_trade_no;primaryKey;size:64"`
	UserID        string `gorm:"column:user_id;primaryKey;size:64;index:idx_order_user_created,priority:1;index:idx_order_user_app,priority:1"`
	Channel       string `gorm:"column:channel;size:32"`
	TradeType     string `gorm:"column:trade_type;size:32"`
	Subject       string `gorm:"column:subject;size:255"`
	Amount        int64  `gorm:"column:amount"`
	AmountYuan    string `gorm:"column:amount_yuan;size:32"`
	Status        string `gorm:"column:order_status;size:32"`
	TransactionID string `gorm:"column:transaction_id;size:128"`
	QRCode        string `gorm:"column:qrcode;type:text"`
	NotifyRaw     string `gorm:"column:notify_raw;type:text"`
	// ChannelCreated 这笔单**是否已经在渠道侧创建成功**。
	//
	// 0 = 平台先落了订单记录,但调渠道还没成功(或者进程正好崩在那一步)。
	// 这种单子渠道那边**没有**,所以关单时不用去调渠道(调了必然报「订单不存在」),
	// 商户也可以拿同一个商户单号重试。
	//
	// 1 = 渠道侧确实有这笔单了。
	ChannelCreated int32 `gorm:"column:channel_created"`
	// AppID 订单归属的应用(空 = 未分类 / 用的是全局渠道配置)。
	//
	// 有了它才能「按产品分开看订单、分开统计、分开出报表」。
	// 老订单没这一列,一律当「未分类」。
	AppID     string `gorm:"column:app_id;size:64;index:idx_order_user_app,priority:2"`
	CreatedAt string `gorm:"column:created_at;size:40;index:idx_order_user_created,priority:2"`
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (OrderModel) TableName() string { return "pay_orders" }

// MFACodeModel 一次性验证码(只存带密钥散列)。
type MFACodeModel struct {
	CodeHash  string `gorm:"column:code_hash;primaryKey;size:64"`
	UserID    string `gorm:"column:user_id;size:64"`
	Purpose   string `gorm:"column:purpose;size:32"`
	ExpiresAt string `gorm:"column:expires_at;size:40"`
	Used      int32  `gorm:"column:used"`
	// Attempts 错误尝试次数,超过上限该验证码直接作废(挡验证码爆破)。
	Attempts  int32  `gorm:"column:attempts"`
	CreatedAt string `gorm:"column:created_at;size:40"`
}

// TableName 表名。
func (MFACodeModel) TableName() string { return "pay_mfa_codes" }

// MFAChannelModel 账户的验证码下发通道配置(密文存 cfg_enc)。
//
// 每个账户自己填 SMTP / 短信网关参数:平台不内置任何厂商 SDK,
// 也不写死某一家 —— 否则换服务商就得改代码重新发版。
type MFAChannelModel struct {
	UserID    string `gorm:"column:user_id;primaryKey;size:64"`
	Channel   string `gorm:"column:channel;primaryKey;size:16"`
	CfgEnc    string `gorm:"column:cfg_enc;type:text"`
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (MFAChannelModel) TableName() string { return "pay_mfa_channels" }

// RefundModel 退款流水。
//
// 平台自己记流水,才能判断「累计退款金额」并防止超额退款。
//
// (user_id, out_refund_no) 上有**唯一索引**:商户退款单号是幂等键,
// 同一个退款单号只允许对应一笔退款。没有这个约束,同一个 out_refund_no
// 重发两次就会退两次钱(钱出去了,账上只有一笔)。
type RefundModel struct {
	RefundID    string `gorm:"column:refund_id;primaryKey;size:64"`
	UserID      string `gorm:"column:user_id;size:64;index:idx_refund_order,priority:1;uniqueIndex:uk_refund_no,priority:1;index:idx_refund_user_created,priority:1;index:idx_refund_user_app,priority:1"`
	OutTradeNo  string `gorm:"column:out_trade_no;size:64;index:idx_refund_order,priority:2"`
	OutRefundNo string `gorm:"column:out_refund_no;size:64;uniqueIndex:uk_refund_no,priority:2"`
	Channel     string `gorm:"column:channel;size:32"`
	Amount      int64  `gorm:"column:amount"`
	Status      string `gorm:"column:refund_status;size:32"`
	Reason      string `gorm:"column:reason;size:255"`
	// AppID 退款归属的应用(继承自订单)。
	AppID     string `gorm:"column:app_id;size:64;index:idx_refund_user_app,priority:2"`
	CreatedAt string `gorm:"column:created_at;size:40;index:idx_refund_user_created,priority:2"`
	// UpdatedAt 最后更新时间(占位 → 成功/失败时写一次)。
	UpdatedAt string `gorm:"column:updated_at;size:40"`
}

// TableName 表名。
func (RefundModel) TableName() string { return "pay_refunds" }

// VerifyTokenModel 敏感操作票据(只存散列)。
type VerifyTokenModel struct {
	TokenHash string `gorm:"column:token_hash;primaryKey;size:64"`
	UserID    string `gorm:"column:user_id;size:64"`
	ExpiresAt string `gorm:"column:expires_at;size:40"`
	// UsedAt 首次使用时间(用后即焚:第一笔敏感操作会写这里)。
	UsedAt    string `gorm:"column:used_at;size:40"`
	CreatedAt string `gorm:"column:created_at;size:40"`
}

// TableName 表名。
func (VerifyTokenModel) TableName() string { return "pay_verify_tokens" }

// APIKeyModel 开放接口密钥。
//
// secret_enc 保存密钥的 AES-256-GCM 密文:签名校验需要拿到密钥原文,
// 所以它不能像令牌那样「只存散列」——但也绝不能明文入库。
type APIKeyModel struct {
	KeyID     string `gorm:"column:key_id;primaryKey;size:64"`
	UserID    string `gorm:"column:user_id;size:64;index:idx_api_key_user,priority:1"`
	Name      string `gorm:"column:name;size:64"`
	SecretEnc string `gorm:"column:secret_enc;type:text"`
	// AppID 绑定的应用(空 = 不绑定,按全局渠道配置走)。
	//
	// 绑定之后,用这把密钥下单就**不用再传 app_id**了:平台自动落到这个应用,
	// 也就自动用这个应用自己的渠道密钥。传了别的 app_id 一律拒绝。
	AppID string `gorm:"column:app_id;size:64;index:idx_api_key_app"`
	// Scopes 权限列表(逗号分隔)。
	Scopes     string `gorm:"column:scopes;size:128"`
	Enabled    int32  `gorm:"column:enabled"`
	ExpiresAt  string `gorm:"column:expires_at;size:40"`
	LastUsedAt string `gorm:"column:last_used_at;size:40"`
	CreatedAt  string `gorm:"column:created_at;size:40"`
}

// TableName 表名。
func (APIKeyModel) TableName() string { return "pay_api_keys" }

// AuditLogModel 操作审计日志。
type AuditLogModel struct {
	LogID     string `gorm:"column:log_id;primaryKey;size:64"`
	UserID    string `gorm:"column:user_id;size:64"`
	Action    string `gorm:"column:action;size:64"`
	Target    string `gorm:"column:target;size:128"`
	Detail    string `gorm:"column:detail;size:512"`
	ClientIP  string `gorm:"column:client_ip;size:64"`
	CreatedAt string `gorm:"column:created_at;size:40"`
}

// TableName 表名。
func (AuditLogModel) TableName() string { return "pay_audit_logs" }
