package platform

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/AIntelligenceGame/bus/payment"
)

// 应用 / 产品维度的控制台接口。
//
// 一个商户常常有多个产品(APP、小程序、H5……)。每个产品:
//   - 可以配自己的一套渠道密钥(两个微信 App 各配各的 appid/mchid);
//   - 出来的订单、退款、统计、报表都能分开看。
//
// 没有这一层,所有产品的订单混在一起,只能靠商户自己在 out_trade_no 里
// 编产品号,导出后再人工拆 —— 报表基本没法看。
//
// 兼容性:老商户一个应用都没有也完全能用 ——
// app_id 为空 = 「未分类」,老订单、老退款都是这个值;
// 渠道配置默认走全局那套(pay_user_channels),一行没动过。

// handleApps 列出本商户的应用。
//
// GET /api/apps
//
// 响应里带上每个应用的订单数与成功金额合计,前端不用再逐个查。
func (p *Platform) handleApps(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	apps, err := p.store.Apps(user.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 回填统计数字(逐个查会变成 N 次往返,这里一次性算)。
	for i := range apps {
		if err := p.store.fillAppStats(&apps[i]); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	// 「未分类」这一档:没有应用但有订单的商户,页面上也该看得见这批钱。
	var unclassified OrderCountSummary
	if err := p.store.unclassifiedSummary(user.UserID, &unclassified); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"apps":         apps,
		"unclassified": unclassified,
	})
}

// OrderCountSummary 「未分类」订单的汇总。
type OrderCountSummary struct {
	// OrderCount 订单数。
	OrderCount int64 `json:"order_count"`
	// SuccessCount 支付成功订单数。
	SuccessCount int64 `json:"success_count"`
	// Amount 成功金额合计(分)。
	Amount int64 `json:"amount"`
}

// handleSetRequireAppBinding 设置是否强制订单绑定应用。
//
// POST /api/apps/require-binding {"require":true}
//
// 需要二次验证:这个开关一开,没带 app_id 的下单就全被拒了,
// 商户的业务可能当场中断,不能只凭会话令牌就改。
func (p *Platform) handleSetRequireAppBinding(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		Require bool `json:"require"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	// 打开之前先看一眼:这个商户一个应用都没有的话,开了就等于自断手脚。
	// 直接拒掉并说明原因,比事后商户发现下不了单再来问强。
	if req.Require {
		apps, err := p.store.Apps(user.UserID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(apps) == 0 {
			writeError(w, http.StatusBadRequest,
				"还没有任何应用,无法开启「订单必须绑定应用」:请先在「应用管理」页创建一个应用")
			return
		}
	}
	if err := p.store.SetRequireAppBinding(user.UserID, req.Require); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.audit(r, user.UserID, auditAppRequireBinding, "",
		fmt.Sprintf("强制绑定应用=%v", req.Require), "")
	writeJSON(w, http.StatusOK, map[string]any{
		"require_app_binding": req.Require,
	})
}

// handleSaveApp 新建或更新一个应用。
//
// POST /api/apps        {"name":"iOS App","remark":"主客户端"}
// POST /api/apps/{app_id} {"name":"...","remark":"..."}
//
// 新建 / 修改应用本身不动钱,但它决定了订单往哪归、报表怎么拆,
// 所以和渠道配置一样要二次验证票据。
func (p *Platform) handleSaveApp(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	var req struct {
		AppID  string `json:"app_id"`
		Name   string `json:"name"`
		Remark string `json:"remark"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	// 应用 ID 以**路径**为准:路径才是「要改哪个应用」的那句话。
	// 之前是请求体优先,于是 POST /api/apps/A 带着 body{app_id:"B"} 会去改 B ——
	// URL 说改 A、表单说改 B,改错了还不报错。
	appID := strings.TrimSpace(r.PathValue("app_id"))
	if appID == "" {
		appID = strings.TrimSpace(req.AppID)
	}
	app, err := p.store.SaveApp(user.UserID, appID, req.Name, req.Remark)
	if err != nil {
		if errors.Is(err, ErrAppNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		// 名称重复:给一句说人话的提示。
		if errors.Is(err, ErrAppNameExists) {
			writeError(w, http.StatusConflict, "应用名称已存在: "+strings.TrimSpace(req.Name))
			return
		}
		// 兜底:并发提交时两个请求可能都通过了上面的查重,靠唯一索引兜住。
		// 这时错误来自数据库,文案随数据库而变(SQLite 说 UNIQUE,
		// MySQL 说 Duplicate entry),两种都认。
		msg := err.Error()
		if strings.Contains(msg, "UNIQUE") || strings.Contains(msg, "unique") ||
			strings.Contains(msg, "Duplicate entry") {
			writeError(w, http.StatusConflict, "应用名称已存在: "+strings.TrimSpace(req.Name))
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	action := "创建应用"
	if appID != "" {
		action = "修改应用"
	}
	p.audit(r, user.UserID, auditAppSave, app.AppID, action+" "+app.Name, "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "app": app})
}

// handleDeleteApp 删除一个应用。
//
// DELETE /api/apps/{app_id}
//
// 应用下还有订单时**拒绝删除**:订单是钱的事实,删了应用会让这批订单
// 变成查不到、算不出报表的孤儿。
func (p *Platform) handleDeleteApp(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	appID := strings.TrimSpace(r.PathValue("app_id"))
	if appID == "" {
		appID = strings.TrimSpace(r.URL.Query().Get("app_id"))
	}
	if appID == "" {
		// 直接调用 handler(单测、或者平台挂在别的路由上)时 PathValue 是空的,
		// 这时从路径本身解析,和 handleDeleteAppChannel 的兜底顺序一致。
		appID = pathSegment(r.URL.Path, "/api/apps/")
	}
	if appID == "" {
		writeError(w, http.StatusBadRequest, "缺少应用 ID")
		return
	}
	// 强制绑定应用时,不能把最后一个应用删掉 ——
	// 删了这个账户就再也发不了支付(没有应用 → 没法建绑应用的密钥,
	// 下单也会因为没有 app_id 被拒),等于把自己锁死。
	if user.RequireAppBinding {
		apps, err := p.store.Apps(user.UserID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(apps) <= 1 {
			writeError(w, http.StatusConflict,
				"该账户已开启「订单必须绑定应用」,不能删掉最后一个应用"+
					"(否则再也发不了支付)。请先建好另一个应用,或先关闭强制绑定。")
			return
		}
	}
	if err := p.store.DeleteApp(user.UserID, appID); err != nil {
		switch {
		case errors.Is(err, ErrAppNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, ErrAppHasOrders):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, ErrAppHasKeys):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	p.invalidateUser(user.UserID)
	p.audit(r, user.UserID, auditAppDelete, appID, "删除应用", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleAppChannels 返回某个应用下的渠道配置。
//
// GET /api/apps/{app_id}/channels
//
// 返回**合并后**的结果(应用专属覆盖全局),并额外标出每个渠道是
// 「专属配置」还是「继承全局」—— 页面上要能一眼看出哪些是单独配过的。
func (p *Platform) handleAppChannels(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	appID := strings.TrimSpace(r.PathValue("app_id"))
	if appID == "" {
		appID = strings.TrimSpace(r.URL.Query().Get("app_id"))
	}
	if appID == "" {
		// 直接调用 handler(单测、或者平台挂在别的路由上)时 PathValue 是空的,
		// 这时从路径本身解析,和 handleDeleteAppChannel 的兜底顺序一致。
		appID = pathSegment(r.URL.Path, "/api/apps/")
	}
	if appID == "" {
		writeError(w, http.StatusBadRequest, "缺少应用 ID")
		return
	}
	app, err := p.store.AppByID(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if app == nil {
		writeError(w, http.StatusNotFound, "应用不存在: "+appID)
		return
	}
	merged, err := p.store.EffectiveChannels(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	specific, err := p.store.AppChannels(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	own := make(map[string]bool, len(specific))
	for _, cfg := range specific {
		own[cfg.Channel] = true
	}
	views := make([]map[string]any, 0, len(merged))
	for _, cfg := range merged {
		view := maskChannelConfig(cfg)
		view["app_specific"] = own[cfg.Channel]
		views = append(views, view)
	}
	// 回调地址要带上应用标识:验签用的是该应用自己的密钥。
	notify := map[string]string{}
	for _, ch := range []payment.Channel{
		payment.ChannelWechat, payment.ChannelAlipay,
		payment.ChannelApplePay, payment.ChannelGooglePay,
	} {
		notify[string(ch)] = p.notifyURLFor(ch, user, user.UserID, appID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"app":         app,
		"channels":    views,
		"notify_urls": notify,
	})
}

// handleDeleteAppChannel 删除某个应用的专属渠道配置(回退到全局配置)。
//
// DELETE /api/apps/{app_id}/channels/{channel}
//
// 应用下还有该渠道的订单时**拒绝删除**:那批订单是用这套密钥下的,
// 删掉配置之后退款 / 查单就没法用原来的商户号去操作了。
func (p *Platform) handleDeleteAppChannel(w http.ResponseWriter, r *http.Request) {
	user, err := p.currentUser(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if err := p.requireVerified(r, user); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return
	}
	appID := strings.TrimSpace(r.PathValue("app_id"))
	channel := strings.TrimSpace(r.PathValue("channel"))
	// PathValue 只有经过 ServeMux 路由时才有值。直接调用 handler(单测、
	// 或者把平台挂到别的路由上)时它是空的,所以这里按「路径值 → 查询参数 →
	// 从路径本身解析」的顺序再找一遍,和 handleDeleteApp / handleAppChannels
	// 的兜底顺序保持一致。
	if appID == "" {
		appID = strings.TrimSpace(r.URL.Query().Get("app_id"))
	}
	if appID == "" {
		appID = pathSegment(r.URL.Path, "/api/apps/")
	}
	if channel == "" {
		channel = strings.TrimSpace(r.URL.Query().Get("channel"))
	}
	if channel == "" {
		channel = pathSegment(r.URL.Path, "/channels/")
	}
	if appID == "" || channel == "" {
		writeError(w, http.StatusBadRequest, "缺少应用 ID 或渠道")
		return
	}
	if !isKnownChannel(channel) {
		writeError(w, http.StatusBadRequest, "未知渠道: "+channel)
		return
	}
	// 应用必须属于当前用户,否则这次删除是删了空气。
	app, err := p.store.AppByID(user.UserID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if app == nil {
		writeError(w, http.StatusNotFound, "应用不存在: "+appID)
		return
	}
	count, err := p.store.AppChannelOrderCount(user.UserID, appID, channel)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if count > 0 {
		writeError(w, http.StatusConflict,
			"该应用下还有 "+strconv.FormatInt(count, 10)+" 笔走这个渠道的订单,不能删除专属配置"+
				"(那些订单是用这套密钥下的,删了就退不了)")
		return
	}
	if err := p.store.DeleteAppChannel(user.UserID, appID, channel); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	p.invalidateUser(user.UserID)
	p.audit(r, user.UserID, auditAppSave, appID, "删除应用专属渠道配置 "+channel, "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
