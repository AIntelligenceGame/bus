# 示例总索引

`bus/payment` 的所有可运行示例都在这一个目录里,不再散落 `example/`、`platform/example/`。

## 先分清两个 HTTP 服务(最容易搞混的地方)

这两个是**用途完全不同**的程序,不是同一个东西的两个版本:

| | **平台控制台** | **移动端示例服务端** |
|---|---|---|
| 目录 | `examples/platform/` | `examples/server/` |
| 启动 | `cd bus && go run ./payment/examples/platform` | `cd bus && go run ./payment/examples/server` |
| 默认地址 | http://127.0.0.1:8089/ | http://127.0.0.1:8088/ |
| 界面 | **网页管理控制台**(渠道配置 / 支付体验 / 订单 / 账户安全) | 一个接口清单 + 试下单表单 |
| 给谁用 | **商户管理员**:在网页上配渠道密钥、看订单、退款、管账户安全 | **App 客户端**:Swift / Kotlin 调它下单、扣款、查单 |
| 密钥从哪来 | 商户在**网页上填写**,加密入库、页面不回显、敏感操作二次验证 | **环境变量**(纯演示用) |
| 鉴权 | 商户令牌 + 二次验证票据,多商户数据隔离 | 无鉴权(示例程序,别直接上生产) |
| 数据落库 | SQLite / MySQL | 不落库 |

> **启动位置很重要**:所有 `go run` 都要在 **bus 模块根目录**(有 `go.mod` 的那一层)执行。
> 工作区根目录 `gowork/` 下面还有 dbhouse、xterm 等别的模块,而根目录没有 `go.mod` / `go.work`,
> 在那里执行会报 `go.mod file not found in current directory or any parent directory`。
> 实在记不住就用 `cd bus && make build-run-platform`(平台) / `make build-run-server`(示例服务端),
> Makefile 会把工作目录固定在 bus/。

一句话:
- `examples/platform` = **给「管钱的人」用的后台**,密钥在这里配、订单在这里看;
- `examples/server` = **给「写 App 的人」看协议用的**,把「服务端怎么配合客户端 SDK」这 4 个接口摊开给你看,
  让你照着写自己的业务后端(或直接照着调平台)。

> 你只需要平台的话,启动 `examples/platform` 就够了;`examples/server` 是**协议说明书 + 可运行对照**,
> 它的价值在于「不带任何鉴权/多租户噪音,能一眼看清客户端到底要调哪几个接口、每个接口返回什么」。

## 全部示例

| 目录 | 内容 | 运行方式(均在 **bus/** 目录下执行) |
|------|------|----------|
| `platform/` | **支付配置选择平台**(网页控制台 + 支付 API) | `go run ./payment/examples/platform`（SQLite 走纯 Go 驱动,不需要 cgo）或 `make build-run-platform` |
| `server/` | 移动端接入服务端(**协议参考实现**,给 Swift / Kotlin 调) | `go run ./payment/examples/server` 或 `make build-run-server` |
| `basic/` | 支付库用法演示(建客户端、下单、查单、退款,不启 HTTP 服务) | `go run ./payment/examples/basic` |
| `multiapp/` | **多产品(App)维度**接入演示:一个商户多个产品,每个产品用自己的一套渠道密钥(要走平台 + 开放接口密钥) | `go run ./payment/examples/multiapp` |
| `ios/` | **Swift** 客户端示例(微信 / 支付宝 / Apple Pay) | Xcode 工程(示例代码可直接拷进工程) |
| `android/` | **Kotlin** 客户端示例(微信 / 支付宝 / Google Pay) | Android Studio 工程(示例代码可直接拷进工程) |

下文主要讲 `server/` + `ios/` + `android/` 这条**移动端接入主线**;
平台控制台的能力清单见 [`../platform/README.md`](../platform/README.md)。

> 移动端不是 Go 写的:
> - **iOS** 用 Swift(`ios/`),配套微信 `WeChatOpenSDK`、支付宝 `AlipaySDK`、苹果钱包 `PassKit`。
> - **Android** 用 Kotlin(`android/`),配套微信 `IWXAPI`、支付宝 `AlipayClient`、Google Pay `PaymentsClient`。
> - Go 只负责**服务端**:下单、签名、验签、回调入账。

## 那平台能不能直接给 App 用?

能。平台本身也是一套 HTTP 接口,移动端直接调它即可,**不用自己写 `examples/server` 那套**:
`POST /api/pay`(下单,返回 `client_params`)、`POST /api/wallet/charge`(钱包扣款)、
`POST /api/query`(查单)、`POST /notify/{channel}/{user_id}`(渠道回调)。
区别只是平台要求带上商户令牌与二次验证票据,密钥由商户在网页上配置。

`examples/server` 存在的意义就是:**让你在没有平台的那 5 分钟里,也能先把客户端写完、把协议对齐**。

## 目录

```
examples/
├── README.md
├── basic/main.go              支付库用法演示
├── multiapp/main.go           多产品(App)维度接入演示(走平台 + 开放接口密钥)
├── platform/                  启动支付配置选择平台
│   └── main.go
├── server/main.go             服务端 HTTP 示例，Swift / Kotlin 都调它
├── ios/                       Swift 示例
│   ├── PaymentModels.swift      DTO（与服务端 client_params 一一对应）
│   ├── PaymentService.swift     网络层
│   ├── WechatPay.swift          微信 APP 支付
│   ├── AlipayPay.swift          支付宝 APP 支付
│   ├── ApplePayController.swift 苹果钱包
│   └── AppDelegate.swift        SDK 注册与回调转发
└── android/                   Kotlin 示例
    ├── PaymentModels.kt
    ├── PaymentService.kt
    ├── WechatPay.kt
    ├── AlipayPay.kt
    ├── GooglePay.kt
    └── PaymentActivity.kt       把三个渠道串起来的示例页面
```

## 两种支付形态

### 一、APP 拉起支付(微信 / 支付宝)

用户在自己的 App 里点「支付」,微信/支付宝 SDK 弹出收银台,用户在**微信/支付宝 App 内**输密码。

```
App                服务端(Go)              微信/支付宝
 |  POST /api/pay     |                     |
 |------------------->| 下单 + 签名          |
 |  session           |                     |
 |<-------------------|                     |
 |  调起 SDK          |                     |
 |---------------------------------------->|
 |  用户输密码        |                     |
 |<----------------------------------------|
 |  查 /api/query     |                     |
 |------------------->|                     |
 |                    |  异步回调 /api/notify/wechat
 |                    |<-------------------- 渠道服务器
 |                    |  验签 -> 幂等入账
```

服务端下发的 `client_params.wechat` / `client_params.alipay` 已经是**各 SDK 的字段名**,客户端只做一次映射:

| 服务端字段 | Swift | Kotlin |
|---|---|---|
| `wechat.partner_id` | `WXApi.PayReq.partnerId` | `PayReq.partnerId` |
| `wechat.prepay_id` | `WXApi.PayReq.prepayId` | `PayReq.prepayId` |
| `wechat.nonce_str` | `WXApi.PayReq.nonceStr` | `PayReq.nonceStr` |
| `wechat.time_stamp` | `WXApi.PayReq.timeStamp` | `PayReq.timeStamp` |
| `wechat.package_value` | `WXApi.PayReq.packageValue` | `PayReq.packageValue` |
| `wechat.sign` | `WXApi.PayReq.sign` | `PayReq.sign` |
| `alipay.order_string` | `ALIPAYClient.payOrder(_:scheme:options:)` | `AlipayClient.payV2(orderString)` |

### 二、钱包支付(Apple Pay / Google Pay)

用户点「支付」后弹出**系统钱包**面板(Apple Pay / Google Pay),用户在设备上用 Face ID / 密码授权。
这一步客户端拿到的只是「已授权」信号,**真正的扣款必须由服务端用回传的令牌完成**。

```
App                     服务端(Go)                 收单网关
 |  POST /api/pay         |                          |
 |----------------------->| CreateSession 拿令牌      |
 |  client_params         |                          |
 |<-----------------------|                          |
 |  拉起钱包 SDK          |                          |
 |  用户 FaceID/密码       |                          |
 |  拿到 paymentData      |                          |
 |  POST /api/wallet/charge                          |
 |----------------------->| Charge ----------------->|
 |                        |<------------------------|
 |  查 /api/query         |                          |
 |----------------------->|  异步回调入账             |
```

对应字段:

| 服务端字段 | 用途 |
|---|---|
| `apple_pay.merchant_id` / `apple_pay.display_name` | `PKPaymentRequest.merchantIdentifier` / 展示名 |
| `apple_pay.country_code` / `apple_pay.currency` | `PKPaymentRequest.countryCode` / `currencyCode` |
| `apple_pay.amount` | 金额(单位:分),除以 100 得到元 |
| `apple_pay.client_secret` | 收单网关的支付会话令牌(客户端用它做支付,不能下单) |
| `google_pay.gateway` / `google_pay.publishable_key` | `GooglePay(gateway, publishableKey, merchantName)` |
| `google_pay.client_secret` | `PaymentsApi(baseEncodedPaymentMethod = clientSecret)` |
| `google_pay.merchant_name` / `google_pay.currency` | 收银台展示名与币种 |

## 三、多产品(App)维度:每个产品一套渠道密钥

一个商户常常有多个产品:iOS App、安卓 App、小程序、H5……
它们要用**各自的**微信 AppID / 商户号 / 支付宝 AppID / 收单账户 ——
不然订单混在一起,退款时也说不清该退哪一套。

### 先分清:应用维度是**平台**的能力,不是库的能力

| | 库(`payment/pay`) | 平台(`payment/platform`) |
|---|---|---|
| 模型 | 一个商户一套密钥 | 一个商户多个「应用」,每个应用一套渠道密钥 |
| 怎么配 | 直接 `pay.New(...)` | 网页控制台配置 + 开放接口下单 |
| 订单归属应用 | 没有这一维 | 有(`app_id`) |

所以**别在业务代码里自己实现「按产品路由密钥」** —— 平台已经做了,而且做全了:
专属配置覆盖全局、没配过的渠道回退全局、订单 / 退款 / 查单 / 关单 / 回调验签
全都按**订单归属的应用**取凭证(不会出现「A 产品的单用 B 产品的密钥去退」)。

### 三步配好

1. **建应用**:控制台「应用管理」页 → 新建(比如「iOS 客户端」);
2. **配专属渠道密钥**:控制台「渠道配置」页 → 顶部「配置范围」切到这个应用 →
   填这个应用自己的密钥。**四个渠道都支持**(微信 AppID / 商户号、支付宝 AppID、
   Apple Pay / Google Pay 的收单账户);哪个渠道没单独配,就回退全局那套;
3. **建一把绑定应用的密钥**:控制台「开放接口」页 → 建密钥时选上这个应用。

### 然后业务后端正常下单,app_id 不用传

密钥绑了应用 A,订单就自动归到 A:

- 密钥没绑应用 → 下单**必须**带 `app_id`(账户开了「订单必须绑定应用」时,不带直接 400);
- 密钥绑了 A 却传 `app_id=B` → **400**,不会静默落到 B。

**客户端(Swift / Kotlin)一行都不用改** —— app_id 是商户服务端的事,
终端用户也不该手填应用 ID。

### 跑一下

```bash
# 终端 1:起平台
cd bus && go run ./payment/examples/platform

# 终端 2:先在控制台「开放接口」页建一把绑定应用的密钥,拿到 pk_xxx / sk_xxx
cd bus
PLATFORM_BASE=http://127.0.0.1:8089 \
PAY_KEY_ID=pk_xxx PAY_KEY_SECRET=sk_xxx \
go run ./payment/examples/multiapp
```

示例会依次做:

1. **故意传错一次应用** → 平台直接 400(`该密钥绑定应用 X,不能用于应用 Y`),先把规则看清楚;
2. **正常下单(不传 app_id)** → 订单自动归到密钥绑定的那个应用;
3. 看这笔单落在哪个应用、订单列表、按应用分组统计。

> 渠道还没配密钥时,第 2 步会失败(「渠道 xxx 未启用或配置无效」)—— 这是**预期的**:
> 先去控制台「渠道配置」页把密钥配上(或者把「配置范围」切到密钥绑定的那个应用再配)。
> 第 1 步的规则演示不受影响。

## 跑起来

### 1. 服务端

```bash
cd bus          # ← bus 模块根目录(有 go.mod 的那一层)
go run ./payment/examples/server
# 或: make build-run-server
```

起来之后**用浏览器打开 <http://127.0.0.1:8088/>**(端口可用 `ADDR` 环境变量改),
首页列出了全部接口,并带一个可以直接试下单的表单。

> 只写 `/api/*` 接口的话,直接访问根路径会是 `404 page not found`——
> 现在首页有页面,其它未知路径会返回带接口清单的 JSON 404。

密钥通过环境变量提供(见 `examples/server/main.go` 顶部注释),未配置的渠道会被跳过,
首页会显示「已启用渠道: []」并提示需要配置密钥。
**生产环境不要用环境变量放密钥**,用平台(`bus/payment/platform`)在网页上配置,密钥加密入库、页面不回显、敏感操作二次验证。

### 2. iOS

1. 引入依赖:`WeChatOpenSDK`、`AlipaySDK`;
2. `PaymentModels.swift` / `PaymentService.swift` 直接拖进工程;
3. `AppDelegate` 里注册微信、转发回跳;
4. 调 `PaymentService.createOrder` → `WechatPay.pay` / `AlipayPay.pay` / `ApplePayController.present`。

### 3. Android

1. 引入依赖:`com.tencent.mm.opensdk:wechat-sdk`、`com.alipay.sdk:alipay`、`com.google.android.gms:play-services-wallet`、`okhttp`、`gson`;
2. `AndroidManifest` 注册 `WXEntryActivity`、`PayActivity`,并声明
   `<meta-data android:name="com.google.android.gms.wallet.api.enabled" android:value="true" />`;
3. `Application.onCreate` 里 `WechatPay.register(this)`;
4. 调 `PaymentService.createOrder` → `WechatPay.pay` / `AlipayPay.pay` / `GooglePay.pay`。

## 几条容易踩的坑

- **金额单位统一是「分」**(`int64`),只有 Apple Pay / Google Pay 的 SDK 需要「元」时再除以 100。
- **客户端的成功回调不等于支付成功**。微信/支付宝/钱包都只是「用户已授权」,入账必须以**服务端收到渠道回调**为准,并用订单号做幂等键。
- **不要把渠道密钥下发到客户端**。`client_params` 里只有商户号、可发布密钥、会话令牌这类客户端可用的值。
- **钱包类渠道要两步**:先下单拿 `client_secret`,再在用户授权后回传令牌扣款;不要用客户端回调直接发货。
- **微信 APP 支付需要商户 AppID 注册**并在开放平台配置 Universal Link;支付宝需要配置 URL Scheme 回跳。
