package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestModuleRootFindsBusModule 保证「从任意目录启动」时数据目录仍然固定在
// bus 模块根目录下。
//
// 背景:IDE 里点「Run main.go」时工作目录往往是工作区根目录(gowork/),
// 那里没有 go.mod —— 用工作目录推导数据目录会把 SQLite 库、主密钥、
// 证书散落到别处,甚至换个目录启动就生成新主密钥,导致旧渠道密钥解不开。
func TestModuleRootFindsBusModule(t *testing.T) {
	root := moduleRoot()
	if root == "" {
		t.Fatalf("moduleRoot() 没找到模块根目录")
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("moduleRoot()=%q 下面没有 go.mod: %v", root, err)
	}
	if filepath.Base(root) != "bus" {
		t.Fatalf("moduleRoot()=%q,期望是 bus 模块根目录", root)
	}
}

// TestDefaultDataDirUnderModuleRoot 保证默认数据目录在模块根下面。
func TestDefaultDataDirUnderModuleRoot(t *testing.T) {
	dir := defaultDataDir()
	root := moduleRoot()
	if root == "" {
		t.Skip("拿不到模块根目录,跳过")
	}
	want := filepath.Join(root, "payment-data")
	if dir != want {
		t.Fatalf("defaultDataDir()=%q,期望 %q", dir, want)
	}
	if !strings.HasSuffix(dir, "payment-data") {
		t.Fatalf("默认数据目录名字不对: %q", dir)
	}
}
