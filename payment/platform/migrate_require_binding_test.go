package platform

import (
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 老库升级后**不能**突然变成强制绑定应用。
//
// 场景:平台已经跑了一段时间,库里有一批老商户 —— 他们没建应用、密钥也没绑应用。
// 这次升级给 pay_users 加了 require_app_binding 列,如果老行被当成「强制」,
// 这些商户一启动就下不了单(所有不带 app_id 的下单全被拒)。
//
// 所以:新列对老行必须是「不强制」,强制只对**升级之后新建的账户**生效。
func TestMigrationKeepsOldAccountsNonForced(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "pay.db")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("打开测试数据库失败: %v", err)
	}
	// 老版本的 pay_users:没有 require_app_binding 这一列。
	if err := db.Exec(`CREATE TABLE pay_users (
		user_id       VARCHAR(64) NOT NULL,
		user_name     VARCHAR(64) NOT NULL,
		access_token  VARCHAR(64) NOT NULL,
		password_hash VARCHAR(255) NOT NULL,
		notify_base_url VARCHAR(255),
		mfa_type      VARCHAR(16),
		mfa_target    VARCHAR(128),
		created_at    VARCHAR(40) NOT NULL,
		updated_at    VARCHAR(40) NOT NULL,
		PRIMARY KEY (user_id)
	)`).Error; err != nil {
		t.Fatalf("建老表失败: %v", err)
	}
	if err := db.Exec(
		"INSERT INTO pay_users (user_id,user_name,access_token,password_hash,created_at,updated_at) "+
			"VALUES (?,?,?,?,?,?)",
		"u_old", "老商户", "tok", "hash",
		"2026-01-01T00:00:00.000000000Z", "2026-01-01T00:00:00.000000000Z").Error; err != nil {
		t.Fatalf("插老行失败: %v", err)
	}

	box, err := NewSecretBox("test-master-key")
	if err != nil {
		t.Fatalf("NewSecretBox: %v", err)
	}
	// NewStore 会跑 initSchema(含 AutoMigrate 加列)。
	store, err := NewStore(db, box)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	if !db.Migrator().HasColumn(&UserModel{}, "require_app_binding") {
		t.Fatal("升级后还没加上 require_app_binding 列")
	}
	user, err := store.UserByID("u_old")
	if err != nil {
		t.Fatalf("读老用户失败: %v", err)
	}
	if user == nil {
		t.Fatal("老用户读不到")
	}
	if user.RequireAppBinding {
		t.Fatal("老账户升级后不该被变成强制绑定应用(会当场打断他们的下单)")
	}
}
