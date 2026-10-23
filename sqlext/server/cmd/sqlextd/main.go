// sqlextd 是 sqlext 插件的解析服务。
//
// 它做三件事：
//  1. 用户注册/登录，发放登录令牌
//  2. 管理余额与充值订单
//  3. 提供 SQL 解析接口（底层复用 bus/sqltree）
//
// 插件端**不含任何解析逻辑**，只负责把页面的 SQL 转成一次 HTTP 请求。
// 这样解析能力可以随时更新，插件发版不受影响。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AIntelligenceGame/bus/sqlext/server/internal/billing"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/config"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/server"
	"github.com/AIntelligenceGame/bus/sqlext/server/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "[sqlextd] ", log.LstdFlags)

	cfg := config.Load()

	// 静态页面目录：落地页 + 隐私政策。
	// Chrome Web Store 要求隐私政策有公网 URL，让本服务自己托管最省事。
	if cfg.WebDir == "" {
		for _, cand := range []string{"sqlext/web", "../web", "../../web"} {
			if st, err := os.Stat(cand); err == nil && st.IsDir() {
				cfg.WebDir = cand
				break
			}
		}
	}
	if cfg.WebDir != "" {
		logger.Printf("静态页面目录：%s", cfg.WebDir)
	} else {
		logger.Println("未找到静态页面目录（sqlext/web），隐私政策 URL 需要另外部署")
	}

	// TODO: 生产环境换成 MySQL 实现（store.Store 接口已就绪）。
	// 内存实现重启即丢余额，只适合开发/自用/内网。
	st := store.NewMemory()

	channels := billing.NewRegistry(
		billing.ManualChannel{
			Payee:  os.Getenv("SQLEXT_PAYEE"),
			Notice: os.Getenv("SQLEXT_PAY_NOTICE"),
		},
	)

	srv := server.New(cfg, st, channels, logger)

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// 优雅退出
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Println("收到退出信号，正在关闭……")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	logger.Printf("sqlextd 启动，监听 %s（BaseURL=%s，解析单价=%d 积分，免费额度=%d）",
		cfg.Addr, cfg.BaseURL, cfg.CostPerParse, cfg.FreeQuota)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("服务退出: %v", err)
	}
	logger.Println("已退出")
}
