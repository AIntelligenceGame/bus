package app

import (
	"fmt"
	"runtime"

	"github.com/AIntelligenceGame/bus/config"
	"github.com/AIntelligenceGame/bus/cors"
	"github.com/AIntelligenceGame/bus/example/makeapp/handler"
	"github.com/AIntelligenceGame/bus/logger"
	"github.com/AIntelligenceGame/bus/pool"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

var (
	router     = gin.Default()
	defaultMsg = `{"code": -1, "msg":"http: Handler timeout"}`
	MaxProces  = runtime.NumCPU()
)

func Bus() {
	_ = logger.InitLogger(logger.LoggerConfig{})
	e()
	//并发能力控制

	if MaxProces > 2 {
		MaxProces -= 1
	}
	runtime.GOMAXPROCS(MaxProces)

	// 设置gin启动模式为生产模式

	gin.SetMode(gin.ReleaseMode)

	//跨域
	router.Use(cors.ECors())

	router.Use(logger.GinLogger(), logger.GinRecovery(true))

	//在线任务数
	config.Work = pool.NewPool(config.Config.V.GetInt("pool.max"))

	// 管理API
	v1 := router.Group("api")
	{
		v1.GET("/hello", handler.HelloWorld)
		v1.POST("/msi", handler.Gus)
	}
	config.Work.Wait()

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
func e() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("*** 异常:", err)
			zap.L().Error("*** 异常:", zap.String("remote", fmt.Sprintf("%v", err)))
		}
	}()
}
