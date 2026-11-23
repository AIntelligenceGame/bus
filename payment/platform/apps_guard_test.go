package platform

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// TestSaveAppRejectsDuplicateName 同一个商户下应用名不能重复。
//
// 这条以前是靠「错误字符串里有没有 UNIQUE」判断的 —— SQLite 报
// "UNIQUE constraint failed",MySQL 报 "Duplicate entry",后者根本不含
// UNIQUE,换个数据库就变成 400 而不是 409 了。现在改成显式查重。
func TestSaveAppRejectsDuplicateName(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")

	app, err := store.SaveApp(user.UserID, "", "iOS 客户端", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if _, err := store.SaveApp(user.UserID, "", "iOS 客户端", ""); !errors.Is(err, ErrAppNameExists) {
		t.Fatalf("重名应该被拒: %v", err)
	}
	// 改名成**别的应用**的名字也要被拒。
	otherApp, err := store.SaveApp(user.UserID, "", "小程序", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if _, err := store.SaveApp(user.UserID, otherApp.AppID, "iOS 客户端", ""); !errors.Is(err, ErrAppNameExists) {
		t.Fatalf("改成重名应该被拒: %v", err)
	}
	// 改名成**自己**原来的名字是合法的(不改名直接保存)。
	if _, err := store.SaveApp(user.UserID, app.AppID, "iOS 客户端", "备注"); err != nil {
		t.Fatalf("保存同名不该被拒: %v", err)
	}
	// 别的商户可以叫同一个名字(唯一索引是 (user_id, name))。
	other := newTestUser(t, store, "别的商户")
	if _, err := store.SaveApp(other.UserID, "", "iOS 客户端", ""); err != nil {
		t.Fatalf("别的商户用同名不该被拒: %v", err)
	}
}

// TestSaveAppDuplicateNameHTTP409 重名要返回 409(而不是 400)。
func TestSaveAppDuplicateNameHTTP409(t *testing.T) {
	p, store, _, _ := newFeaturePlatform(t)
	user := newTestUser(t, store, "商户")
	verifyToken, err := p.store.CreateVerifyToken(user.UserID, 10*time.Minute)
	if err != nil {
		t.Fatalf("CreateVerifyToken: %v", err)
	}
	post := func(name string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/apps",
			strings.NewReader(`{"name":"`+name+`"}`))
		req.Header.Set("X-User-Token", user.AccessToken)
		req.Header.Set("X-Verify-Token", verifyToken)
		req.Header.Set("Content-Type", "application/json")
		p.handleSaveApp(rec, req)
		return rec
	}
	if rec := post("重复名"); rec.Code != http.StatusOK {
		t.Fatalf("第一次创建应成功,实际 %d: %s", rec.Code, rec.Body.String())
	}
	rec := post("重复名")
	if rec.Code != http.StatusConflict {
		t.Fatalf("重名应返回 409,实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestCertDirForSanitizesSegments 证书目录不能被路径穿越。
//
// user_id / app_id 有一部分来自 URL 路径(/notify/{channel}/{user_id}/{app_id}),
// 而 buildProvider 会对算出来的目录做 MkdirAll —— 不消毒就是
// 「往服务器任意目录建目录」。
func TestCertDirForSanitizesSegments(t *testing.T) {
	p, _, _, _ := newFeaturePlatform(t)
	base := t.TempDir()
	p.opts.CertDir = base

	dir := p.certDirFor("u1", "../../../etc")
	if !strings.HasPrefix(dir, base) {
		t.Fatalf("证书目录跑出 CertDir 了: %s", dir)
	}
	rel, err := filepath.Rel(base, dir)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if strings.HasPrefix(rel, "..") {
		t.Fatalf("证书目录越界: %s", rel)
	}
	if strings.ContainsRune(dir, '/') && strings.Contains(dir, "..") {
		t.Fatalf("目录里还有 ..: %s", dir)
	}
	// 正常 ID 应当原样保留(别把目录名改得没法认)。
	if got := p.certDirFor("u1", "app_abc"); got != filepath.Join(base, "u1", "app_abc") {
		t.Fatalf("正常 ID 不该被改写: %s", got)
	}
}

// TestSaveAppChannelKeepsOldConfigOnFailure 保存失败时不能把旧配置弄丢。
//
// 之前是「先删后插」两步,插失败(唯一索引冲突之类)时原来那份能用的
// 配置已经被删掉了 —— 商户改个配置改到一半,渠道就整个不可用了。
//
// 失败注入:给 GORM 挂一个 Create 回调,往 pay_app_channels 插行时直接报错,
// 模拟「插不进去」。
func TestSaveAppChannelKeepsOldConfigOnFailure(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
		Wechat:  wechatCfgForTest("wx-first"),
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}

	// 注入失败。
	const hookName = "test:fail_app_channel_create"
	if err := store.db.Callback().Create().Before("gorm:create").Register(hookName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema != nil && tx.Statement.Schema.Table == "pay_app_channels" {
				tx.AddError(errors.New("注入的插入失败"))
			}
		}); err != nil {
		t.Fatalf("注册失败注入钩子失败: %v", err)
	}
	t.Cleanup(func() {
		_ = store.db.Callback().Create().Remove(hookName)
	})

	err = store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
		Wechat:  wechatCfgForTest("wx-second"),
	})
	if err == nil {
		t.Fatal("注入失败后保存应该报错")
	}

	// 关键:旧配置必须还在,而且还是原来那份。
	list, err := store.AppChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("AppChannels: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("保存失败后旧配置不该丢,实际 %d 行", len(list))
	}
	if list[0].Wechat == nil || list[0].Wechat.AppID != "wx-first" {
		t.Fatalf("保存失败后旧配置被改掉了: %+v", list[0].Wechat)
	}
}

// TestSaveAppChannelIsAtomic 重复保存同一 (app, channel) 应该替换而不是变成两份。
func TestSaveAppChannelIsAtomic(t *testing.T) {
	store := newTestStore(t)
	user := newTestUser(t, store, "商户")
	app, err := store.SaveApp(user.UserID, "", "应用", "")
	if err != nil {
		t.Fatalf("建应用失败: %v", err)
	}
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
		Wechat:  wechatCfgForTest("wx-first"),
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	// 再存一次(走 upsert 路径),确认配置被替换而不是变成两份。
	if err := store.SaveAppChannel(user.UserID, app.AppID, ChannelConfig{
		Channel: "wechat",
		Enabled: true,
		Wechat:  wechatCfgForTest("wx-second"),
	}); err != nil {
		t.Fatalf("保存应用渠道失败: %v", err)
	}
	list, err := store.AppChannels(user.UserID, app.AppID)
	if err != nil {
		t.Fatalf("AppChannels: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("同一 (app, channel) 应该只有一行,实际 %d 行", len(list))
	}
	if list[0].Wechat == nil || list[0].Wechat.AppID != "wx-second" {
		t.Fatalf("配置没被替换: %+v", list[0].Wechat)
	}
}
