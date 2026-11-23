// Command payplatform 启动支付配置选择平台(控制台 + 支付 API)。
//
// # 怎么启动
//
// 必须在 **bus 模块根目录**(就是有 go.mod 的那一层)执行:
//
//	cd bus
//	go run ./payment/examples/platform
//
// 为什么强调目录:bus/payment 属于 bus 模块(gowork 工作区里还有 dbhouse、xterm
// 等别的模块,工作区根目录**没有** go.mod / go.work)。在 gowork/ 那一层执行
// go run 会直接报 "go.mod file not found in current directory or any parent directory"。
//
// 在 IDE 里点「Run main.go」同理:请把 **Working Directory** 设成 bus 模块根目录,
// 否则要么报同样的 go.mod 错误,要么把下面的相对路径数据目录落到别的地方。
//
// 最省心的办法(不用记目录,工作目录由 Makefile 保证):
//
//	cd bus && make build-run-platform
//
// # 数据目录
//
// 默认的 SQLite 库、主密钥文件、微信平台证书写在 **bus 模块根目录**下的
// payment-data/ 里(已加入 bus/.gitignore),**与启动目录无关**:
// 无论你在哪个目录执行 go run(甚至在 IDE 里点 Run main.go),
// 数据都落在同一个 bus/payment-data/ 里,不会散落到源码树中,
// 也不会因为换个目录启动就生成新主密钥导致旧渠道密钥解不开。
//
// # 参数
//
//	-addr      监听地址,默认 :8089
//	-driver    数据库驱动,默认 sqlite
//	-dsn       数据库连接串,默认 <模块根>/payment-data/pay.db
//	-base      对外访问地址(用于生成回调地址),默认 http://127.0.0.1<addr>;
//	           这是**平台默认值**,商户可以在控制台「渠道配置 → 回调地址」里
//	           填自己的域名覆盖它(页面里改不用重启平台)
//	-regcode   新商户注册码,默认空(允许自助注册)
//	-session   登录会话有效期(小时),默认 24;0 表示不过期
//	-certdir   微信平台证书缓存根目录,默认 <模块根>/payment-data/certs
//	-masterkey 主密钥文件,默认 <模块根>/payment-data/master.key
//	-trust-xff 是否采信 X-Forwarded-For(只在可信反代后开启),默认 false
//	-log-mfa  是否允许「验证码只写进服务器日志」(仅开发:平台方从日志里取码),默认 false
//
// # 二次验证(邮箱 / 短信 / 验证器 App)
//
// 平台**不内置**任何邮件或短信厂商 —— 通道参数由每个商户在控制台的
// 「账户安全 → 验证码下发通道」里自己填(SMTP 服务器、端口、登录账号、授权码,
// 或短信网关地址与令牌),**加密入库**,平台只负责按配置发送。
//
// 所以这个服务开箱即用,但要真正收到验证码,需要在页面上配一次:
//
//  1. 邮件:在邮箱服务商后台开启 SMTP 并生成「授权码」(不是登录密码),
//     然后填服务器 / 端口 / 加密方式 / 账号 / 授权码,点「发送测试邮件」实测;
//  2. 短信:填自己的短信网关地址(平台会 POST {"code","target"}),点「发送测试短信」实测;
//  3. 验证器 App:不需要任何通道,手机上装个 App 扫码即可。
//
// 开发阶段也可以用 -log-mfa:验证码只写进服务器日志,平台方从日志里取码
// (这时页面会如实提示「验证码已写入服务器日志」)。
//
// # 用 MySQL
//
//	go run ./payment/examples/platform -driver mysql -dsn "user:pass@tcp(127.0.0.1:3306)/pay?charset=utf8mb4"
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/AIntelligenceGame/bus/payment/platform"
)

// moduleRoot 返回 bus 模块根目录(有 go.mod 的那一层)。
//
// 依据是**编译期**的源码路径(runtime.Caller):go run 与本地构建出来的二进制
// 都带着它,所以无论从哪个工作目录启动,都能定位到同一个模块根。
// 找不到就返回空字符串(调用方退回当前工作目录)。
func moduleRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(file)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// defaultDataDir 返回默认数据目录:优先 <模块根>/payment-data,否则 ./payment-data。
func defaultDataDir() string {
	if root := moduleRoot(); root != "" {
		return filepath.Join(root, "payment-data")
	}
	return "payment-data"
}

func main() {
	dataDir := defaultDataDir()

	addr := flag.String("addr", ":8089", "监听地址")
	driver := flag.String("driver", "sqlite", "数据库驱动: sqlite / mysql")
	dsn := flag.String("dsn", filepath.Join(dataDir, "pay.db"), "数据库连接串")
	base := flag.String("base", "", "对外访问地址(平台默认的回调地址前缀),默认 http://127.0.0.1<addr>")
	regCode := flag.String("regcode", "", "新商户注册码,默认空(允许自助注册)")
	sessionHours := flag.Int("session", 24, "登录会话有效期(小时),0 表示不过期")
	certDir := flag.String("certdir", filepath.Join(dataDir, "certs"), "微信平台证书缓存根目录")
	masterKeyFile := flag.String("masterkey", filepath.Join(dataDir, "master.key"), "主密钥文件")
	trustXFF := flag.Bool("trust-xff", false, "是否采信 X-Forwarded-For(只在可信反代后开启)")
	logMFA := flag.Bool("log-mfa", false, "是否允许验证码只写进服务器日志(仅开发)")
	flag.Parse()

	if *driver == platform.DialectSQLite && *dsn != "" {
		if dir := filepath.Dir(*dsn); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				log.Fatalf("创建数据目录失败: %v", err)
			}
		}
	}

	publicBase := *base
	if publicBase == "" {
		publicBase = fmt.Sprintf("http://127.0.0.1%s", *addr)
	}

	// 主密钥:环境变量优先(PAY_PLATFORM_MASTER_KEY / PAY_PLATFORM_MASTER_KEY_FILE),
	// 都没设时用数据目录下的 master.key —— **跟着数据目录走**,不跟工作目录走。
	// 否则换个目录启动就会生成一把新密钥,之前加密入库的渠道密钥全部解不开。
	masterKey := ""
	if os.Getenv("PAY_PLATFORM_MASTER_KEY") == "" && os.Getenv("PAY_PLATFORM_MASTER_KEY_FILE") == "" {
		key, src, err := platform.LoadOrCreateMasterKeyFile(*masterKeyFile)
		if err != nil {
			log.Fatalf("准备主密钥失败: %v", err)
		}
		masterKey = key
		log.Printf("主密钥来源: %s", src)
	}

	p, err := platform.New(platform.Options{
		Driver:            *driver,
		DSN:               *dsn,
		Addr:              *addr,
		PublicBaseURL:     publicBase,
		RegistrationCode:  *regCode,
		SessionTTL:        time.Duration(*sessionHours) * time.Hour,
		CertDir:           *certDir,
		MasterKey:         masterKey,
		TrustForwardedFor: *trustXFF,
		// 验证码通道:默认只写日志(见 Options.AllowLogOnlyMFA 的说明)。
		// 真正的下发通道由每个商户在控制台页面上自己配。
		AllowLogOnlyMFA: *logMFA,
	})
	if err != nil {
		log.Fatalf("启动支付平台失败: %v", err)
	}
	log.Printf("控制台地址: %s/", publicBase)
	log.Printf("回调地址前缀: %s/notify/<channel>/<user_id>", publicBase)
	if *base == "" {
		// 没指定对外地址 → 回调地址默认指向本机。本机调试没问题,
		// 但这个地址填到微信 / 支付宝后台,对方根本访问不到,订单会永远停在 pending。
		// 部署到公网时忘了配对外地址是很容易犯的错,所以在这里明确喊一声。
		log.Printf("⚠️  未指定 -base,回调地址默认指向本机(%s),本机调试可以、生产部署请二选一:"+
			"用 -base https://你的域名 启动,或在控制台「渠道配置 → 回调地址」里填对外域名"+
			"(页面里填不用重启平台)。否则微信 / 支付宝的支付结果回调不进来", publicBase)
	}
	log.Printf("数据目录: %s", dataDir)
	log.Fatal(p.Start())
}
