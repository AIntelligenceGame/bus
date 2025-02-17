package main

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/AIntelligenceGame/bus/cors"
	"github.com/AIntelligenceGame/bus/example/fc/handler"
	"github.com/AIntelligenceGame/bus/logger"
	"github.com/gin-gonic/gin"
	timeout "github.com/vearne/gin-timeout"
	"go.uber.org/zap"
)

var (
	router     = gin.Default()
	defaultMsg = `{"code": -1, "msg":"http: Handler timeout"}`
	MaxProces  = runtime.NumCPU()
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	_ = logger.InitLogger(logger.LoggerConfig{})
	// 设置gin启动模式为生产模式

	gin.SetMode(gin.ReleaseMode)

	//跨域
	router.Use(cors.ECorsPlus([]string{"*"}))

	router.Use(timeout.Timeout(
		timeout.WithTimeout(20*time.Second),
		timeout.WithErrorHttpCode(http.StatusRequestTimeout), // optional
		timeout.WithDefaultMsg(defaultMsg),                   // optional
		timeout.WithCallBack(func(r *http.Request) {
			fmt.Println("timeout happen, url:", r.URL.String())
		}))) // optional

	router.Use(logger.GinLogger(), logger.GinRecovery(true))

	// 管理API
	v1 := router.Group("api")
	{
		v1.GET("/hello", handler.HelloWorld)
	}

	zap.L().Info("Start server", zap.String("listen", ":8080"))
	err := router.Run(":8080")
	if err != nil {
		zap.L().Error("Start server", zap.String("error", err.Error()))
	}

}
