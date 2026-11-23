package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/AIntelligenceGame/bus/payment"
)

// 应用 / 产品维度的数据层。
//
// 背景:一个商户往往有多个产品(APP、小程序、H5……),每个产品:
//
//   - 可能用**不同的支付渠道配置**(两个微信 App 各配各的 appid/mchid);
//   - 出来的订单、退款、统计、报表都要能**分开**看。
//
// 没有这一层的话,所有产品的订单混在一张表里,只能靠商户自己在
// out_trade_no 里编产品号,再导出后人工拆 —— 报表基本没法看。
//
// 兼容性:老商户一个应用都没有也完全能用 ——
//   - app_id 为空 = 「未分类」,老订单、老退款都是这个值;
//   - 渠道配置默认走**全局**那套(pay_user_channels),一行没动过。
//
// 应用专属的渠道配置放在**新表** pay_app_channels 里,而不是给
// pay_user_channels 加一列:加列就得改主键,而改主键在 SQLite/MySQL 上
// 都要重建表(老数据要搬,风险大)。新表由 AutoMigrate 直接建出来。

// ErrAppNotFound 应用不存在(或不属于该商户)。
var ErrAppNotFound = errors.New("应用不存在")

// ErrAppHasOrders 应用下还有订单,不能删。
var ErrAppHasOrders = errors.New("应用下还有订单")

// ErrAppHasKeys 还有开放接口密钥绑定在这个应用上,不能删。
//
// 删了应用,那把密钥就指向一个**不存在的应用**:它每次下单都会被拒
// (「应用不存在: app_xxx」),而密钥本身在列表里看起来一切正常 ——
// 商户只会一头雾水,完全想不到是删应用删掉了。
var ErrAppHasKeys = errors.New("还有开放接口密钥绑定在这个应用上")

// requireAppBindingMessage 「订单必须绑定应用」开启时的下单提示。
//
// 提示要写清楚**下一步该做什么**,不然商户只会看到一句「失败」。
func requireAppBindingMessage() string {
	return "该账户已开启「订单必须绑定应用」:订单必须归属某个应用。" +
		"请先在「应用管理」页创建应用,下单时带上 app_id," +
		"或使用绑定了该应用的开放接口密钥(密钥绑了应用就不用再传 app_id)"
}

// requireAppBindingKeyMessage 「订单必须绑定应用」开启时的建密钥提示。
func requireAppBindingKeyMessage() string {
	return "该账户已开启「订单必须绑定应用」:创建的密钥必须绑定一个应用," +
		"否则用它下单会被拒绝"
}

// ErrAppNameExists 同一个商户下应用名重复。
//
// 不用「看错误字符串里有没有 UNIQUE」来判断重复:SQLite 报
// "UNIQUE constraint failed: ...",MySQL 报 "Error 1062: Duplicate entry ...",
// 字符串匹配在 MySQL 上根本匹配不上 —— 同样的操作换个数据库就变成 400
// 而不是 409。这里显式查一次并返回哨兵错误,两种数据库行为一致。
var ErrAppNameExists = errors.New("应用名称已存在")

// Apps 列出某商户的全部应用(按创建时间排序)。
func (s *Store) Apps(userID string) ([]App, error) {
	var rows []AppModel
	if err := s.db.Where("user_id = ?", userID).
		Order("created_at ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: query apps: %w", err)
	}
	out := make([]App, 0, len(rows))
	for i := range rows {
		out = append(out, appFromModel(&rows[i]))
	}
	return out, nil
}

// AppByID 查某个应用(带订单数与成功金额合计)。
//
// 传空 appID 返回 nil,nil —— 表示「未分类」这一档,不是一个真实应用。
func (s *Store) AppByID(userID, appID string) (*App, error) {
	if appID == "" {
		return nil, nil
	}
	var row AppModel
	if err := s.db.Where("user_id = ? AND app_id = ?", userID, appID).
		First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("platform: query app: %w", err)
	}
	app := appFromModel(&row)
	if err := s.fillAppStats(&app); err != nil {
		return nil, err
	}
	return &app, nil
}

// fillAppStats 回填应用的订单数与成功金额合计。
func (s *Store) fillAppStats(app *App) error {
	if app == nil || app.AppID == "" {
		return nil
	}
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND app_id = ?", app.UserID, app.AppID).
		Count(&app.OrderCount).Error; err != nil {
		return fmt.Errorf("platform: count app orders: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND app_id = ? AND order_status = ?",
			app.UserID, app.AppID, string(payment.TradeStatusSuccess)).
		Select("COALESCE(SUM(amount), 0)").Scan(&app.Amount).Error; err != nil {
		return fmt.Errorf("platform: sum app amount: %w", err)
	}
	return nil
}

// SaveApp 新建或更新一个应用。
//
// appID 为空 = 新建(平台生成 ID);否则按 ID 更新。
// 同一个商户下应用名必须唯一 —— 名字是商户自己看的标识,
// 重名会让「按产品对账」时搞混是哪两个产品。
func (s *Store) SaveApp(userID, appID, name, remark string) (*App, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("应用名称不能为空")
	}
	if len([]rune(name)) > 64 {
		return nil, errors.New("应用名称过长(最多 64 个字符)")
	}
	now := formatTime(time.Now())
	// 名称唯一性先查一次(SQLite 与 MySQL 的重复键报错文案不一样,
	// 靠匹配字符串判断不可靠 —— MySQL 的 "Duplicate entry" 里没有 UNIQUE)。
	//
	// 改名时要把**自己**排除掉:不改名保存同名是合法的。
	var dupCount int64
	dupQuery := s.db.Model(&AppModel{}).Where("user_id = ? AND name = ?", userID, name)
	if appID != "" {
		dupQuery = dupQuery.Where("app_id <> ?", appID)
	}
	if err := dupQuery.Count(&dupCount).Error; err != nil {
		return nil, fmt.Errorf("platform: check app name: %w", err)
	}
	if dupCount > 0 {
		return nil, fmt.Errorf("%w: %s", ErrAppNameExists, name)
	}
	if appID == "" {
		appID = newID("app")
		model := AppModel{
			AppID:     appID,
			UserID:    userID,
			Name:      name,
			Remark:    strings.TrimSpace(remark),
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := s.db.Create(&model).Error; err != nil {
			return nil, fmt.Errorf("platform: create app: %w", err)
		}
		created := appFromModel(&model)
		return &created, nil
	}
	// 更新:先确认这个应用确实属于该商户(不能改别人的)。
	var exist AppModel
	if err := s.db.Where("user_id = ? AND app_id = ?", userID, appID).
		First(&exist).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAppNotFound
		}
		return nil, fmt.Errorf("platform: query app: %w", err)
	}
	updates := map[string]any{
		"name":       name,
		"remark":     strings.TrimSpace(remark),
		"updated_at": now,
	}
	if err := s.db.Model(&AppModel{}).
		Where("user_id = ? AND app_id = ?", userID, appID).
		Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("platform: update app: %w", err)
	}
	exist.Name = name
	exist.Remark = strings.TrimSpace(remark)
	exist.UpdatedAt = now
	app := appFromModel(&exist)
	if err := s.fillAppStats(&app); err != nil {
		return nil, err
	}
	return &app, nil
}

// DeleteApp 删除应用。
//
// 应用下还有订单时**拒绝删除**:订单是钱的事实,不能因为删了个应用就没了
// (那会让历史订单变成查不到、算不出报表的孤儿)。
// 真要清理,先让商户把订单导走。
func (s *Store) DeleteApp(userID, appID string) error {
	if appID == "" {
		return errors.New("不能删除默认应用")
	}
	var count int64
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND app_id = ?", userID, appID).
		Count(&count).Error; err != nil {
		return fmt.Errorf("platform: count app orders: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("%w:还有 %d 笔订单", ErrAppHasOrders, count)
	}
	// 绑在这个应用上的开放接口密钥也要挡。
	//
	// 密钥绑应用之后,下单的 app_id 由密钥决定(不用客户端传)。应用没了,
	// 这把密钥就成了「指向空应用」的废密钥:列表里看着正常,一发请求就被拒。
	// 与其留一把废密钥给商户,不如当场说清楚:先把这把密钥吊销,再删应用。
	//
	// 已经吊销的密钥不算 —— 它本来就用不了,不该挡着删应用。
	var keyCount int64
	if err := s.db.Model(&APIKeyModel{}).
		Where("user_id = ? AND app_id = ? AND enabled <> 0", userID, appID).
		Count(&keyCount).Error; err != nil {
		return fmt.Errorf("platform: count app api keys: %w", err)
	}
	if keyCount > 0 {
		return fmt.Errorf("%w:还有 %d 把密钥(请先吊销它们,或改用新建的密钥)", ErrAppHasKeys, keyCount)
	}
	if err := s.db.Where("user_id = ? AND app_id = ?", userID, appID).
		Delete(&AppChannelModel{}).Error; err != nil {
		return fmt.Errorf("platform: delete app channels: %w", err)
	}
	res := s.db.Where("user_id = ? AND app_id = ?", userID, appID).Delete(&AppModel{})
	if res.Error != nil {
		return fmt.Errorf("platform: delete app: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrAppNotFound
	}
	return nil
}

// AppChannels 某个应用**专属**的渠道配置(不合并全局)。
func (s *Store) AppChannels(userID, appID string) ([]ChannelConfig, error) {
	if appID == "" {
		return nil, nil
	}
	var rows []AppChannelModel
	if err := s.db.Where("user_id = ? AND app_id = ?", userID, appID).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("platform: query app channels: %w", err)
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

// SaveAppChannel 保存应用专属的渠道配置。
func (s *Store) SaveAppChannel(userID, appID string, cfg ChannelConfig) error {
	if appID == "" {
		return errors.New("不能给默认应用单独配置渠道")
	}
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
	row := AppChannelModel{
		AppID:     appID,
		UserID:    userID,
		Channel:   cfg.Channel,
		Enabled:   boolToInt32(cfg.Enabled),
		IsDefault: boolToInt32(cfg.IsDefault),
		CfgEnc:    enc,
		UpdatedAt: formatTime(time.Now()),
	}
	// upsert:同一个 (app_id, channel) 只有一行。
	//
	// 删 + 插必须放在一个事务里:否则插失败(比如唯一索引冲突)时,
	// 原来那份能用的配置已经被删掉了 —— 商户改个配置改到一半,
	// 渠道就整个不可用了。事务保证「要么换成功,要么还是原来那份」。
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("user_id = ? AND app_id = ? AND channel = ?",
			userID, appID, cfg.Channel).
			Delete(&AppChannelModel{}).Error; err != nil {
			return fmt.Errorf("platform: replace app channel: %w", err)
		}
		if err := tx.Create(&row).Error; err != nil {
			return fmt.Errorf("platform: insert app channel: %w", err)
		}
		return nil
	})
}

// DeleteAppChannel 删除应用专属的渠道配置(回退到全局配置)。
func (s *Store) DeleteAppChannel(userID, appID, channel string) error {
	if appID == "" {
		return errors.New("不能删除默认应用的渠道配置")
	}
	res := s.db.Where("user_id = ? AND app_id = ? AND channel = ?",
		userID, appID, channel).Delete(&AppChannelModel{})
	if res.Error != nil {
		return fmt.Errorf("platform: delete app channel: %w", res.Error)
	}
	return nil
}

// EffectiveChannels 合并全局与应用专属的渠道配置。
//
// 规则:**应用专属的配置覆盖同名渠道的全局配置**。
// 应用专属那行存在,就以它为准(哪怕它是「不启用」——
// 这正好是「这个产品先别用这个渠道」的意思)。
// 应用没配过的渠道,继续用全局那套。
func (s *Store) EffectiveChannels(userID, appID string) ([]ChannelConfig, error) {
	global, err := s.Channels(userID)
	if err != nil {
		return nil, err
	}
	if appID == "" {
		return global, nil
	}
	specific, err := s.AppChannels(userID, appID)
	if err != nil {
		return nil, err
	}
	if len(specific) == 0 {
		return global, nil
	}
	overrides := make(map[string]ChannelConfig, len(specific))
	for _, c := range specific {
		overrides[c.Channel] = c
	}
	out := make([]ChannelConfig, 0, len(global)+len(specific))
	seen := make(map[string]bool, len(global))
	for _, c := range global {
		if override, ok := overrides[c.Channel]; ok {
			out = append(out, override)
			seen[c.Channel] = true
			continue
		}
		out = append(out, c)
	}
	// 应用专属里有、但全局没有的渠道(全局那套被删了)也要带上。
	for _, c := range specific {
		if !seen[c.Channel] {
			out = append(out, c)
		}
	}
	return out, nil
}

// DefaultChannelFor 取默认渠道(应用专属优先,再退全局)。
func (s *Store) DefaultChannelFor(userID, appID string) (string, error) {
	// 应用专属的默认必须先看。
	// EffectiveChannels 里全局那批排在前面,直接在合并结果里找 is_default,
	// 全局默认会赢过应用默认 —— 商户在应用里勾了「设为默认」却不生效,
	// 和本函数注释里写的「应用专属优先」正好相反。
	if appID != "" {
		specific, err := s.AppChannels(userID, appID)
		if err != nil {
			return "", err
		}
		for _, c := range specific {
			if c.IsDefault && c.Enabled {
				return c.Channel, nil
			}
		}
	}
	channels, err := s.EffectiveChannels(userID, appID)
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

// appFromModel 把数据模型转成对外的 App。
func appFromModel(m *AppModel) App {
	return App{
		AppID:     m.AppID,
		UserID:    m.UserID,
		Name:      m.Name,
		Remark:    m.Remark,
		CreatedAt: parseNanoTime(m.CreatedAt),
		UpdatedAt: parseNanoTime(m.UpdatedAt),
	}
}

// appLabel 返回应用在报表 / 导出里的展示名。
//
// 空 app_id = 未分类(老订单),写「未分类」比写空值清楚 ——
// 财务看到空格子会以为是导出漏了。
func appLabel(names map[string]string, appID string) string {
	if appID == "" {
		return "未分类"
	}
	if name, ok := names[appID]; ok && name != "" {
		return name
	}
	// 应用被删了(理论上不该发生:有订单的应用不许删),退化成显示 ID。
	return appID
}

// unclassifiedSummary 统计「未分类」(app_id 为空)的订单。
//
// 老订单都在这一档里。商户建了应用之后,页面要能同时看到
// 「已分类的各产品」和「还没归类的这批」—— 不然那笔钱就凭空消失了。
func (s *Store) unclassifiedSummary(userID string, out *OrderCountSummary) error {
	if out == nil {
		return nil
	}
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND (app_id IS NULL OR app_id = '')", userID).
		Count(&out.OrderCount).Error; err != nil {
		return fmt.Errorf("platform: count unclassified orders: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND (app_id IS NULL OR app_id = '') AND order_status = ?",
			userID, string(payment.TradeStatusSuccess)).
		Count(&out.SuccessCount).Error; err != nil {
		return fmt.Errorf("platform: count unclassified success orders: %w", err)
	}
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND (app_id IS NULL OR app_id = '') AND order_status = ?",
			userID, string(payment.TradeStatusSuccess)).
		Select("COALESCE(SUM(amount), 0)").Scan(&out.Amount).Error; err != nil {
		return fmt.Errorf("platform: sum unclassified amount: %w", err)
	}
	return nil
}

// AppChannelOrderCount 统计某应用下走某渠道的订单数。
//
// 用在「删除应用专属渠道配置」前的检查:那批订单是用这套密钥下的,
// 配置删了就退不了款了。
func (s *Store) AppChannelOrderCount(userID, appID, channel string) (int64, error) {
	var count int64
	if err := s.db.Model(&OrderModel{}).
		Where("user_id = ? AND app_id = ? AND channel = ?", userID, appID, channel).
		Count(&count).Error; err != nil {
		return 0, fmt.Errorf("platform: count app channel orders: %w", err)
	}
	return count, nil
}
