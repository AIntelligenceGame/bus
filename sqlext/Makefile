# sqlext 的常用命令。
#
# 都在 bus 模块根目录下执行（sqlext 是 bus 模块的子包，没有独立的 go.mod）。

BUS := ..
API_BASE ?= http://127.0.0.1:8080

.PHONY: help run test test-ext build lint all pack clean

help:
	@echo "make run       启动服务端（:8080）"
	@echo "make test      跑服务端 Go 测试"
	@echo "make test-ext  跑扩展的桥接协议测试（Node）"
	@echo "make build     编译服务端"
	@echo "make lint      gofmt + go vet"
	@echo "make pack      打出可上传 Chrome 商店的 zip（API_BASE=... 指定正式地址）"
	@echo "make all       lint + 全部测试"

run:
	cd $(BUS) && go run ./sqlext/server/cmd/sqlextd

test:
	cd $(BUS) && go test ./sqlext/... -count=1

# 桥接协议测试：模拟主世界/隔离世界/service worker 三层，验证消息能穿透
# （用通配符而不是目录名——node --test <目录> 在某些版本上会当成模块路径）
test-ext:
	node --test extension/test/*.test.mjs

build:
	cd $(BUS) && go build -o /tmp/sqlextd ./sqlext/server/cmd/sqlextd

lint:
	cd $(BUS) && gofmt -l ./sqlext/ && go vet ./sqlext/...

all: lint test test-ext

# 打包扩展。上传 Chrome Web Store 用。
#   make pack API_BASE=https://api.你的域名
pack:
	./extension/package.sh --api-base $(API_BASE)

clean:
	rm -rf dist
