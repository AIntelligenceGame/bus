package platform

import (
	"fmt"
	"strings"

	// SQLite 驱动用纯 Go 实现(glebarez/sqlite,基于 modernc.org/sqlite),
	// 不需要 cgo,交叉编译也省事。
	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 支持的数据库方言。
const (
	// DialectSQLite SQLite(默认,单文件,适合单机部署)。
	DialectSQLite = "sqlite"
	// DialectMySQL MySQL(适合多实例部署)。
	DialectMySQL = "mysql"
)

// OpenDB 打开数据库连接(GORM)。
//
// driver 支持 "sqlite" / "sqlite3" / "mysql"。
func OpenDB(driver, dsn string) (*gorm.DB, error) {
	cfg := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}
	switch driver {
	case DialectSQLite, "sqlite3":
		db, err := gorm.Open(sqlite.Open(dsn), cfg)
		if err != nil {
			return nil, fmt.Errorf("platform: 打开 SQLite 失败: %w", err)
		}
		return db, nil
	case DialectMySQL:
		db, err := gorm.Open(mysql.Open(dsn), cfg)
		if err != nil {
			return nil, fmt.Errorf("platform: 打开 MySQL 失败: %w", err)
		}
		return db, nil
	default:
		return nil, fmt.Errorf("platform: unsupported database driver %q", driver)
	}
}

// DialectFromDriver 根据驱动名推断方言。
func DialectFromDriver(driver string) string {
	if driver == DialectMySQL {
		return DialectMySQL
	}
	return DialectSQLite
}

// NormalizeDialect 归一化方言名(sqlite3 / mysql 都接受)。
func NormalizeDialect(dialect string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case DialectMySQL:
		return DialectMySQL
	case DialectSQLite, "sqlite3":
		return DialectSQLite
	default:
		return ""
	}
}
