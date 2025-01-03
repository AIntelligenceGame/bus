package logger

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LoggerConfig struct {
	EnvVar     string
	MaxSize    int
	MaxBackups int
	MaxAge     int
}

// InitLogger 初始化日志库，支持日志增强和日志轮转
func InitLogger(config LoggerConfig) *zap.Logger {
	// 默认使用 LOG_DIR 环境变量，如果传递了自定义的环境变量名，则使用该名称
	if config.EnvVar == "" {
		config.EnvVar = "LOG_DIR"
	}
	if config.MaxSize == 0 {
		config.MaxSize = 1
	}
	if config.MaxBackups == 0 {
		config.MaxBackups = 1
	}
	if config.MaxAge == 0 {
		config.MaxAge = 1
	}
	// 获取环境变量 (例如: LOG_DIR 或 LOG_DIR222)
	logDir := os.ExpandEnv("${" + config.EnvVar + "}")

	// 如果环境变量为空，或者解析后的路径无效，则使用当前工作目录
	if logDir == "" {
		var err error
		logDir, err = os.Getwd()
		if err != nil {
			log.Fatal("获取当前工作目录失败", err)
		}
	}

	// 检查目录是否存在，如果不存在则使用默认路径 'debug.log'
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		// 如果目录不存在，使用当前工作目录
		logDir = "."
	}

	// 创建日志文件路径，使用 'debug.log' 作为默认日志文件名
	logFilePath := filepath.Join(logDir, "debug.log")

	// 配置日志轮转
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFilePath,       // 日志文件路径
		MaxSize:    config.MaxSize,    // 每个日志文件的最大尺寸，单位MB
		MaxBackups: config.MaxBackups, // 保留的旧日志文件个数
		MaxAge:     config.MaxAge,     // 保留旧日志文件的天数
		Compress:   true,              // 是否压缩旧日志
	}

	// 创建日志级别配置
	atom := zap.NewAtomicLevel()
	atom.SetLevel(zap.InfoLevel) // 设置默认日志级别为 Info

	// 设置日志输出配置
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // 设置时间戳格式
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// encoderConfig.EncodeCaller = zapcore.FullCallerEncoder  //显示完整路径
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder //仅显示文件名和行号

	// 创建日志输出器
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // 使用 JSON 格式输出
		zapcore.AddSync(lumberjackLogger),     // 设置日志输出到文件，支持日志轮转
		atom,                                  // 设置日志级别
	)

	// 创建生产环境的日志配置，并指定输出到文件
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))

	// 替换全局日志记录器
	zap.ReplaceGlobals(logger)

	return logger
}

// GinLogger 接收gin框架默认的日志
func GinLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()

		cost := time.Since(start)
		zap.L().Info(
			path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.String("errors", c.Errors.ByType(gin.ErrorTypePrivate).String()),
			zap.Duration("cost", cost),
		)
	}
}

// GinRecovery recover掉项目可能出现的panic，并使用zap记录相关日志
func GinRecovery(stack bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Check for a broken connection, as it is not really a
				// condition that warrants a panic stack trace.
				var brokenPipe bool
				if ne, ok := err.(*net.OpError); ok {
					if se, ok := ne.Err.(*os.SyscallError); ok {
						if strings.Contains(strings.ToLower(se.Error()), "broken pipe") || strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
							brokenPipe = true
						}
					}
				}

				httpRequest, _ := httputil.DumpRequest(c.Request, false)
				if brokenPipe {
					zap.L().Error(c.Request.URL.Path,
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
					// If the connection is dead, we can't write a status to it.
					c.Error(err.(error)) // nolint: errcheck
					c.Abort()
					return
				}

				if stack {
					zap.L().Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
						zap.String("stack", string(debug.Stack())),
					)
				} else {
					zap.L().Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
				}
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}
