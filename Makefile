normal: build-ios
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
	git config user.email "org-lib@163.com"
	git config user.name "org-lib"
	git pull
	git add .
	git commit -m 'migrate'
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
	# 运行时需要指定so文件目录
	# export LD_LIBRARY_PATH=/service/home/devyuandeqiao/gomodworkspace/gowork/bus/example/sogo:$LD_LIBRARY_PATH
	go run example/sogo/sogo.go
