# bus/payment

通用支付库：**微信支付 / 支付宝 / Apple Pay / Google Pay**，
一套订单模型覆盖「**移动端拉起支付**」与「**二维码扫码支付**」两类场景。

- 纯 Go 标准库实现，**不引入任何第三方依赖**，可直接放进现有 `bus` 模块；
- 金额统一用「分」(int64)，杜绝浮点误差；
- 回调通知统一验签、统一模型，便于幂等入账；
- 二维码自带编码与 PNG/SVG 渲染，无需再引第三方库。

---

## 一、目录结构

```
payment/
├── README.md                 本文档
├── payment.go                数据模型：订单 / 会话 / 交易结果 / 退款 / 回调
├── provider.go               Provider 接口与通用错误
├── client.go                 统一客户端（多渠道注册与调度）
├── money.go                  金额换算（分 <-> 元）
├── crypto.go                 RSA / ECDSA / AES 加解密与签名
├── hash.go                   摘要与 HMAC
├── pay/                      易用门面：一行配置即可发起支付
│   └── pay.go
├── providers/                各支付渠道实现
│   ├── wechat/               微信支付 APIv3
│   │   ├── config.go         配置与校验
│   │   ├── wechat.go         下单 / 查单 / 关闭 / 退款
│   │   ├── sign.go           APIv3 请求签名与调用
│   │   ├── notify.go         回调验签与解密
│   │   └── cert.go           平台证书下载与缓存
│   ├── alipay/               支付宝开放平台
│   │   ├── config.go
│   │   ├── alipay.go         当面付 / APP / WAP / PC 下单、查单、退款
│   │   ├── sign.go           RSA2 签名与验签
│   │   └── notify.go         异步通知验签与解析
│   ├── applepay/             Apple Pay（走收单机构）
│   │   └── applepay.go       会话创建、令牌扣款、证书辅助
│   ├── googlepay/            Google Pay（走收单机构）
│   │   └── googlepay.go
│   └── gateway/              收单网关抽象
│       ├── gateway.go        Acquirer 接口
│       └── stripe.go         Stripe 实现（含 Webhook HMAC 验签）
├── qrcode/                   二维码
│   ├── qrcode.go             编码（ISO/IEC 18004，字节模式 + M 级纠错）
│   └── render.go             PNG / SVG 渲染
├── platform/                 多商户「支付配置选择平台」（网页配置 + 订单管理）
│   ├── README.md             平台说明文档
│   ├── platform.go           平台主体：配置、Provider 缓存、路由、鉴权
│   ├── store.go              多商户数据存储（SQLite / MySQL 通用 SQL）
│   ├── db.go                 数据库打开与方言判断
│   ├── handlers.go           HTTP 接口
│   ├── core.go               业务核心逻辑（控制台与开放接口共用）
│   ├── openapi.go            开放接口 / API 密钥管理 / 平台管理接口
│   ├── apikey.go             开放接口密钥与 HMAC 签名（防重放）
│   ├── account.go            账户安全（口令 / 二次验证 / 敏感操作票据）
│   ├── security.go           口令散列、验证码、密钥脱敏
│   ├── secrets.go            渠道密钥静态加密（AES-256-GCM）
│   ├── mfa.go                二次验证下发通道
│   ├── web.go                前端产物分发（embed）
│   └── web/dist/             前端打包产物（由 bus/payment-web 构建）
├── examples/                 所有可运行示例都在这里（不再有单独的 example 目录）
│   ├── README.md             示例总索引
│   ├── basic/main.go         库用法演示（不启 HTTP 服务）
│   ├── platform/             启动支付配置选择平台（main.go，SQLite 走纯 Go 驱动）
│   ├── server/main.go        移动端接入服务端（下单 / 钱包扣款 / 查单 / 回调）
│   ├── ios/                  Swift：微信 / 支付宝 / Apple Pay
│   └── android/              Kotlin：微信 / 支付宝 / Google Pay
└── ../payment-web/           控制台前端工程（Vite + TypeScript，源码与后端分离）
    ├── src/                  前端源码
    └── vite.config.ts        打包产物直接输出到 ../payment/platform/web/dist
```

职责边界：

| 目录 | 职责 |
|------|------|
| `payment`（根） | 只定义模型与接口，不依赖任何渠道 |
| `payment/pay` | 门面层，把各渠道配置聚合起来，对外只暴露一个 `Client` |
| `payment/providers/*` | 每个渠道一个包，实现 `payment.Provider` |
| `payment/providers/gateway` | Apple/Google Pay 依赖的收单机构抽象（Stripe 等） |
| `payment/qrcode` | 二维码编码与渲染，与支付流程解耦 |

---

## 二、快速开始

### 1. 创建客户端

```go
import (
    "github.com/AIntelligenceGame/bus/payment/pay"
    "github.com/AIntelligenceGame/bus/payment/providers/wechat"
    "github.com/AIntelligenceGame/bus/payment/providers/alipay"
)

client, err := pay.New(pay.Config{
    Wechat: &wechat.Config{
        AppID:         os.Getenv("WECHAT_APP_ID"),
        MchID:         os.Getenv("WECHAT_MCH_ID"),
        SerialNo:      os.Getenv("WECHAT_SERIAL_NO"),
        PrivateKeyPEM: os.Getenv("WECHAT_PRIVATE_KEY"),
        APIv3Key:      os.Getenv("WECHAT_API_V3_KEY"),
        NotifyURL:     "https://pay.example.com/notify/wechat",
        CertDir:       "./certs",       // 平台证书缓存目录
        AutoFetchCerts: true,           // 首次自动下载平台证书
    },
    Alipay: &alipay.Config{
        AppID:              os.Getenv("ALIPAY_APP_ID"),
        PrivateKeyPEM:      os.Getenv("ALIPAY_PRIVATE_KEY"),
        AlipayPublicKeyPEM: os.Getenv("ALIPAY_PUBLIC_KEY"),
        NotifyURL:          "https://pay.example.com/notify/alipay",
    },
})
```

只配置需要使用的渠道即可，未配置的渠道不会被注册。

也可以直接从配置文件载入（示例见 `config.example.yaml`）：

```go
client, err := pay.LoadConfigFile("./payment/config.yaml")
```

> 配置文件有两种写法都支持：渠道直接写在根下（`wechat:` / `alipay:` …），
> 或像 `config.example.yaml` 那样套一层 `payment:`（根下没有渠道字段时自动下沉到 `payment` 段）。

> 配置文件里放私钥只适合本地调试。生产环境建议用**支付平台**（见下节）：
> 密钥在网页上填写、加密入库、接口不回显、敏感操作二次验证；
> 或者用 KMS / K8s Secret 以文件形式挂载，再由程序读取文件。

### 2. 二维码扫码支付（PC 展示二维码，手机扫）

```go
session, err := client.PayQRCode(ctx, pay.Wechat, pay.Order{
    OutTradeNo: "BIZ202601010001",
    Subject:    "iPhone 17 Pro",
    Amount:     19900, // 199.00 元
})
if err != nil {
    return err
}
// session.QRCode 即二维码内容(微信 code_url / 支付宝 qr_code)
png, err := pay.QRCodePNG(session, 8)   // 直接返回给前端展示
```

### 3. 移动端拉起支付

```go
// 微信 APP 支付
session, err := client.PayApp(ctx, pay.Wechat, order)
// session.AppParams: appid / partnerid / prepayid / package / noncestr / timestamp / sign
// session.ClientParams.Wechat: 按客户端 SDK 字段名组织好的结构（partner_id / prepay_id / nonce_str ...）

// 支付宝 APP 支付
session, err = client.PayApp(ctx, pay.Alipay, order)
// session.AppParams["orderString"]: 交给支付宝 SDK 调起
// session.ClientParams.Alipay.OrderString: 同上，字段名已对齐支付宝 SDK

// 微信内网页（公众号 / 小程序），需带上 openid
session, err = client.PayJSAPI(ctx, pay.Order{ ... , PayerID: openid})
```

> `ClientParams` 是**给客户端 SDK 用的结构化参数**：微信 iOS 的 `PayReq.prepayId`、
> Android 的 `PayReq.prepayId`、支付宝的 `orderString` 等，字段名各不相同，
> 服务端按 SDK 的叫法填好，客户端只做一次映射，不用自己猜字段名。
> 完整接入示例见 [`examples/`](examples/README.md)（Swift + Kotlin）。

### 4. 钱包支付（Apple Pay / Google Pay）

```go
stripe, err := gateway.NewStripe(&gateway.StripeConfig{
    SecretKey:      os.Getenv("STRIPE_SECRET_KEY"),
    PublishableKey: os.Getenv("STRIPE_PUBLISHABLE_KEY"),
    WebhookSecret:  os.Getenv("STRIPE_WEBHOOK_SECRET"),
})

client, err := pay.New(pay.Config{
    ApplePay: &applepay.Config{
        MerchantID:  "merchant.com.example",
        DisplayName: "示例商城",
        DomainName:  "pay.example.com",
        Gateway:     stripe,
    },
})

session, err := client.PayWallet(ctx, pay.ApplePay, order)
// session.Token 即 client_secret,客户端用钱包 SDK 完成支付后把令牌回传服务端扣款
// session.ClientParams.ApplePay: merchant_id / country_code / amount / client_secret ...

// 客户端拿到钱包令牌后回传，服务端完成真实扣款
result, err := client.ChargeWallet(ctx, pay.ApplePay, order.OutTradeNo, paymentToken, order.Amount)
```

> 钱包支付是**两步**：先下单拿会话令牌 → 用户在系统钱包里授权 → 令牌回传服务端扣款。
> 客户端回调只是「用户已授权」信号，最终入账以渠道异步回调为准。

### 5. 回调处理（统一验签 + 幂等入账）

```go
func notifyHandler(w http.ResponseWriter, r *http.Request) {
    notify, err := client.HandleNotify(ctx, pay.Wechat, r)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        return
    }
    if notify.Status == payment.TradeStatusSuccess {
        // 幂等入账：以 notify.OutTradeNo 为幂等键
        settleOrder(notify.OutTradeNo, notify.Amount, notify.TransactionID)
    }
    w.Write([]byte("SUCCESS"))
}
```

### 6. 查单 / 退款

```go
result, err := client.Query(ctx, pay.Wechat, outTradeNo)

refund, err := client.Refund(ctx, pay.Wechat, pay.RefundRequest{
    OutTradeNo:  outTradeNo,
    OutRefundNo:  outTradeNo + "-R",
    RefundAmount: 0, // 0 表示全额退款
    Reason:       "用户申请退款",
})
```

---

## 三、支付配置选择平台（不想写配置文件就用它）

除了直接调用本库，还内置了一个**多商户支付控制台**：商户在网页上填写自己的渠道密钥、
启用/停用渠道、选定默认渠道、发起支付、查看订单；配置保存在数据库（SQLite 默认，可切 MySQL）。

### 怎么启动（先看这里）

> ⚠️ **必须在 `bus/` 目录下执行。** `bus/payment` 属于 bus 模块；
> 工作区根目录 `gowork/` 下面还有 dbhouse、xterm 等别的模块，而根目录**没有** `go.mod` / `go.work`，
> 在那一层执行 `go run ./payment/...` 必然报 `go.mod file not found in current directory or any parent directory`。

```bash
cd /service/home/devyuandeqiao/gomodworkspace/gowork/bus   # ← 模块根目录，别写成 gowork/
go run ./payment/examples/platform
```

不想记目录就用 Makefile（工作目录由它保证）：

```bash
cd /service/home/devyuandeqiao/gomodworkspace/gowork/bus
make build-run-platform
```

启动后：

```bash
curl http://127.0.0.1:8089/healthz      # 自检
# 浏览器打开 http://127.0.0.1:8089/      # 控制台
```

在 IDE 里点「Run main.go」也行，但要把 **Working Directory 设成 `bus` 模块根目录**，
否则会报同样的 go.mod 错误。

数据目录固定在 **bus 模块根目录**下的 `payment-data/`（SQLite 库 `pay.db`、主密钥 `master.key`、
微信平台证书缓存），**与启动目录无关**——换个目录启动不会生成新主密钥，
之前加密入库的渠道密钥照样解得开。已加入 `bus/.gitignore`，不会误提交。

| 场景 | 命令（在 `bus/` 下执行） |
|------|------------------------|
| 默认（SQLite） | `go run ./payment/examples/platform` |
| 换端口 | `go run ./payment/examples/platform -addr :9000` |
| 换数据库文件 | `go run ./payment/examples/platform -dsn /var/lib/pay/pay.db` |
| MySQL | `go run ./payment/examples/platform -driver mysql -dsn "user:pass@tcp(127.0.0.1:3306)/pay?charset=utf8mb4"` |
| 首次注册要码 | `go run ./payment/examples/platform -regcode 你的注册码` |
| 先编译再跑 | `go build -o /tmp/payplatform ./payment/examples/platform && /tmp/payplatform` |

常见报错：

| 报错 | 原因 / 解法 |
|------|------------|
| `go.mod file not found ...` | 目录不对，**必须在 `bus/` 下执行** |
| `cannot open database` / 目录不存在 | `-dsn` 的父目录不存在时程序会自动创建；手动指定路径时注意目录权限 |
| 端口被占用 | `-addr :9000` 换一个 |
| 渠道密钥解不开 | `payment-data/master.key` 丢了或换了；用 `PAY_PLATFORM_MASTER_KEY` 固定主密钥可避免 |

SQLite 走纯 Go 驱动（`github.com/glebarez/sqlite`），不需要 cgo、不需要额外装依赖。

控制台前端是独立的 Vite + TypeScript 工程，源码在 `bus/payment-web/`，打包产物
（`bus/payment/platform/web/dist/`）由 Go 端 `//go:embed` 打进二进制：

```bash
cd bus/payment-web
npm install && npm run build     # 产物写入 ../payment/platform/web/dist
```

或从 `bus/` 目录：`make build-frontend`（类型检查 + 打包）、`make build`（gofmt + vet + test）。

- 页面：`/` 概览与使用说明、`#/channels` 渠道配置、`#/playground` 支付体验、`#/orders` 订单
  （统计概览 / 筛选分页 / 导出 CSV / 退款流水）、`#/security` 账户安全、`#/keys` 开放接口
  （API 密钥管理与签名接入说明）（前端 hash 路由）；
- 接口：登录、渠道保存/测试、下单、查单、退款、关单、二维码，订单分页/筛选/导出、退款流水、
  交易统计、审计日志分页、健康检查 `/healthz`、版本信息 `/api/version`，以及各渠道异步回调
  `POST /notify/{channel}/{user_id}`（平台自动验签并回写订单状态）；
- **开放接口** `/api/open/*`：商户自己的**业务后端**用 API 密钥 + HMAC-SHA256 签名直接
  下单 / 查单 / 退款 / 关单，不必弹浏览器；密钥按 scope 授权（`pay` / `refund` / `wallet`），
  明文只显示一次、库里只存密文，签名带时间戳窗口与随机串防重放。另有平台管理接口
  `/api/admin/*`（商户列表 / 全平台统计 / 禁用商户）；
- **密钥安全**：库里存 AES-256-GCM 密文、接口读取一律脱敏、**不提供密钥明文回显**、改配置/支付/退款/关单需
  「口令 + 邮箱/短信验证码」二次验证（票据用后即焚）、退款金额受订单金额与累计退款双重约束、
  退款单号幂等（重复提交不会二次出款）、订单状态只向前走、全程审计留痕
  （详见 [`platform/README.md`](platform/README.md) 的安全模型一节）；
- 详情见 [`platform/README.md`](platform/README.md)。

---

## 四、各渠道接入准备清单

### 微信支付（APIv3）

| 项目 | 说明 |
|------|------|
| AppID | 公众号 / 小程序 / 开放平台 AppID |
| MchID | 商户号 |
| SerialNo | 商户 API 证书序列号 |
| PrivateKeyPEM | 商户 API 证书私钥（apiclient_key.pem） |
| APIv3Key | 32 字节 APIv3 密钥，用于回调解密与平台证书下载 |
| 平台证书 | `AutoFetchCerts: true` + `CertDir` 即可自动下载并缓存 |

支持场景：`native`（扫码）、`app`、`jsapi`、`h5`。

### 支付宝

| 项目 | 说明 |
|------|------|
| AppID | 开放平台应用 APPID |
| PrivateKeyPEM | 应用私钥（RSA2） |
| AlipayPublicKeyPEM | 支付宝公钥（公钥模式） |
| AlipayPublicCertPath / AppCertPath / RootCertPath | 证书模式：支付宝公钥证书 + 应用公钥证书 + 支付宝根证书（自动带 `app_cert_sn` 与 `alipay_root_cert_sn`） |

支持场景：`native`（当面付）、`app`、`wap`、`web`。

> **证书模式的运维提醒**：证书有有效期。本库只在启动时读一次这三份证书，
> 不会自动更新（官方 SDK 提供了「证书下载」接口 `alipay.open.cert.download`
> 可以拉最新的一套）。到期前请到支付宝开放平台重新下载并替换文件、
> 重启进程，否则请求会被网关判 `40002 / isv.invalid-signature`，
> 异步通知也会验签失败。控制台「渠道配置 → 支付宝 → 测试连通性」会发一次
> 真实签名请求，可以用来确认证书链还有效。

### Apple Pay / Google Pay

钱包本身不与商户结算，必须绑定收单机构：

1. 在收单机构（Stripe / Adyen / PayPal）开通 Apple Pay / Google Pay；
2. 本地实现 `gateway.Acquirer` 接口，或直接使用内置的 `gateway.Stripe`；
3. Apple Pay 还需向 Apple 申请 Merchant ID 与商户身份证书，
   `applepay.GenerateCSR()` 可生成 CSR。

---

## 五、约定

- **金额**：内部统一「分」(int64)；支付宝接口用「元」字符串，库内部自动换算。
- **幂等**：所有下单/退款接口都以 `OutTradeNo` / `OutRefundNo` 作为幂等键，
  同一订单号重复调用不会重复扣款。
  （平台侧再加一道：`pay_refunds` 上有 `(user_id, out_refund_no)` 唯一索引，
  退款**先占位再调渠道**——渠道失败不占额度，重试换单号即可。）
- **回调**：必须先验签再入账；验签失败返回 `payment.ErrInvalidSignature`。
  入账前还要过三道关：订单存在、**金额与订单一致**、**appid / mchid 与本商户配置一致**；
  订单状态走状态机，只向前走（迟到的回调不能把已退款订单改回成功）。
- **二维码**：字节模式 + 纠错等级 M，版本 1~20（最大 666 字节），
  足以容纳各类支付链接；带 4 模块静默区。

---

## 六、示例

所有示例都在 `payment/examples/`，**别把下面两个 HTTP 服务搞混**：

| 程序 | 命令 | 地址 | 界面 |
|------|------|------|------|
| **支付配置选择平台**（网页控制台） | `go run ./payment/examples/platform` | http://127.0.0.1:8089/ | 渠道配置 / 支付体验 / 订单 / 账户安全 / 开放接口 |
| **移动端示例服务端**（给 Swift / Kotlin 调） | `go run ./payment/examples/server` | http://127.0.0.1:8088/ | 接口清单 + 试下单表单 |

另有不启服务的库用法演示：

```bash
go run ./payment/examples/basic
```

示例通过环境变量读取密钥，未配置时自动跳过对应渠道的演示。

测试：

```bash
go test ./payment/...
```
