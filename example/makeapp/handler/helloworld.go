package handler

import (
	"fmt"
	"net/http"

	"github.com/AIntelligenceGame/bus/config"
	"github.com/AIntelligenceGame/bus/logger"
	"github.com/AIntelligenceGame/bus/msi"
	"github.com/AIntelligenceGame/bus/xshell"
	"github.com/axgle/mahonia"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func HelloWorld(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"msg":    "Success",
		"status": 200,
	})
}

type Message struct {
	msi.Msi
}

var (
	enc = mahonia.NewEncoder("gbk")
)

func Gus(ctx *gin.Context) {
	//全局异常抓捕

	e()
	//参数转 struct 对象

	var msg Message

	//定义一个chan,用作同步返回任务结果

	ch := make(chan bool, 1)
	//参数获取

	if err := ctx.ShouldBindJSON(&msg); err != nil {
		//	参数错误

		ctx.JSON(http.StatusBadRequest, gin.H{
			"msg": err.Error(),
		})
		//  程序退出

		return
	}

	//开始任务
	//需要注意 chan 的使用方式

	go makeApp(&msg, func(result int, reason string) {
		ctx.JSON(http.StatusOK, gin.H{
			"msg":   reason,
			"statu": result,
		})
		config.Work.Add(1)
		ch <- true
	})
	//结束任务

	config.Work.Done()
	<-ch
}

func makeApp(v interface{}, res func(result int, reason string)) {
	switch v.(type) {
	//构建MSI
	case *Message:
		objMsg := v.(*Message)
		objMsi := objMsg.Msi

		//MSI 参数信息不正确
		if objMsi.Task <= 0 || objMsi.Svc == "" || objMsi.Display == "" {
			res(-1, "参数不正确，或者缺失必要参数！")
			return
		}
		err, out := doMsi(objMsi)
		if err != nil {
			res(-1, fmt.Sprintf("Make MSI File Fail: %v", err))
			return
		}
		fmt.Println("make msi installer file done.....")
		logger.Log.Info("Make MSI File", zap.String("MSI", "成功构建MSI！"))

		//MSI2 参数信息不正确
		//do msi2
		//time.Sleep(time.Second * 1)

		//返回任务处理状态
		res(1, fmt.Sprintf("MakeApp完成. 操作日志：%v", out))

	default:
		res(-1, "没有找到合适的任务与，请检查传入参数，或者查看README.md")
	}
}
func e() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("*** 异常:", err)
			logger.Log.Error("*** 异常:", zap.String("remote", fmt.Sprintf("%v", err)))
		}
	}()
}
func doMsi(m msi.Msi) (error, string) {
	var outStr string
	shell, err := xshell.Powershell()
	if err != nil {
		return err, ""
	}
	defer shell.Exit()

	// ... 交互 in
	for i := 0; i < len(m.Commands); i++ {
		stdout, stderr, err := shell.Execute(m.Commands[i])
		//中文解码

		stdout = enc.ConvertString(stdout)
		stderr = enc.ConvertString(stderr)

		outStr = fmt.Sprintf("%v", stdout)
		if err != nil {
			logger.Log.Error("Making MSI File Error", zap.String("MSI stderr", stderr))
			return err, ""
		}
		logger.Log.Info("Making MSI File ", zap.String("MSI stdout", stdout))
	}
	return nil, outStr
}
