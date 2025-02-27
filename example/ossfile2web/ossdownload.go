package main

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/AIntelligenceGame/bus/config"
	"github.com/AIntelligenceGame/bus/cors"
	"github.com/AIntelligenceGame/bus/example/ossfile2web/handler"
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
	_ = logger.InitLogger(logger.LoggerConfig{})
	//并发能力控制

	if MaxProces > 2 {
		MaxProces -= 1
	}
	runtime.GOMAXPROCS(MaxProces)

	// 设置gin启动模式为生产模式

	gin.SetMode(gin.ReleaseMode)

	//跨域
	router.Use(cors.ECors())

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
		//通过二进制流从 oss 发送到浏览器
		v1.GET("/do2wb", handler.Do2wb)
	}
	fmt.Println()

	// 启动服务，获取配置文件config.yaml的IP和端口：listen_ip和listen_port

	addr := fmt.Sprintf("%v:%v", config.Config.V.GetString("server.listen_ip"), config.Config.V.GetString("server.listen_port"))

	//输出一个名为message的自定义内容值、{"message":"Start server"}，以及自定义key：value 的输出
	//{"level":"INFO","timestamp":"2021-12-22 13:38:09:000","caller":"example/main.go:68","message":"Start server","listen":"0.0.0.0:80"}

	zap.L().Info("Start server", zap.String("listen", addr))
	err := router.Run(fmt.Sprintf("%v", addr))
	if err != nil {
		zap.L().Error("Start server", zap.String("error", err.Error()))
	}
	//zap.L().Info("Start server success", zap.String("listen", addr))

}
