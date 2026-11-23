normal: build-ios

# 格式化 / 静态检查 / 测试
# 目标名统一用 build- 前缀:与上层 make build * 的调用约定一致。
build-fmt:
	gofmt -l -w ./payment/
	go vet ./payment/...

build-test:
	go test ./payment/... -count=1

build:
	@$(MAKE) build-fmt
	@$(MAKE) build-test

# 启动支付平台控制台(SQLite,纯 Go 驱动,不需要 cgo)。
# 目标名带 build- 前缀是为了和上层 `make build *` 的调用约定一致;
# 工作目录被强制留在 bus 模块根目录,免得 go run 报 "go.mod file not found"。
# 数据落在 bus/payment-data/(已在 .gitignore 里)。
build-run-platform:
	go run ./payment/examples/platform

# 启动移动端示例服务端(协议参考实现,给 Swift / Kotlin 调),默认 :8088
build-run-server:
	go run ./payment/examples/server

# 启动多产品(App)维度示例:走平台 + 开放接口密钥,
# 需要另一个终端先把平台起起来,并在控制台建好绑定应用的密钥。
build-run-multiapp:
	go run ./payment/examples/multiapp

# 构建支付平台前端(bus/payment-web),产物输出到 payment/platform/web/dist 由 Go embed 打包
# 用 npm run build 而不是 npx vite build:后者会绕过 prebuild 钩子,
# 旧产物不清,会跟新产物一起被 //go:embed 打进二进制。
build-frontend:
	cd payment-web && npm run build
	ls -l payment/platform/web/dist payment/platform/web/dist/assets

#初始化 mobile 环境命令
install:
	cd $HOME/Documents/gomodworkspace/
	git clone https://github.com/golang/mobile.git
	g use 1.18.6
	go env -w GOPROXY=https://goproxy.cn,direct
	cd $HOME/Documents/gomodworkspace/mobile/cmd/gobind
	go build -o /usr/local/bin/gomobile
	gomobile --help
#构建 mobile 开发环境
gomobile:

build-ios:

git:
	@read -p "请输入提交说明: " msg; \
	if [ -z "$$msg" ]; then \
	  echo "提交说明不能为空，已取消提交。"; \
	  exit 1; \
	fi; \
	git config user.email "org-lib@163.com"; \
	git config user.name "org-lib"; \
	git pull; \
	git add .; \
	git commit -m "$$msg"; \
	git push

# 构建 Golang C 共享库
build-shared-lib:
	git config user.email "org-lib@163.com"
	git config user.name "org-lib"
	# go mod tidy
	# go work vendor
	g use 1.22.0 && go clean -i && go build -buildmode=c-shared -o ./example/sogo/libso.so ./so/so.go
	# go build -buildmode=c-shared -o ./example/sogo/libso.so ./so/so.go -ldflags="-rpath /service/home/devyuandeqiao/gomodworkspace/gowork/bus/example/sogo"
	echo "---------Golang shared library (libso.so) built successfully.---------"
	# cgo 方式调用运行时需要指定so文件目录到LD_LIBRARY_PATH。purego方式也就是syscall方式不需要指定
	# export LD_LIBRARY_PATH=/service/home/devyuandeqiao/gomodworkspace/gowork/bus/example/sogo:$LD_LIBRARY_PATH
	go run example/sogo/sogo.go
	# go build -ldflags="-s -w -X main.sharedLibrary=$(cat libexample.so)" -o app example/sogo/sogo.go
