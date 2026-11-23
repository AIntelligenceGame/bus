# 开放接口客户端(Go)

「支付配置选择平台」的开放接口给商户自己的**业务后端**调用:
用 API 密钥 + HMAC 签名,直接下单 / 查单 / 退款,不用弹浏览器。

这个包把签名算法实现了一遍(和平台端一致),商户接 Go 后端时直接用,
**不用自己算签名**。

```go
import "github.com/AIntelligenceGame/bus/payment/platform/openapi"

// 密钥在控制台「开放接口」页创建,明文只显示一次。
c := openapi.New("https://pay.example.com", "pk_xxx", "sk_xxx")

// 1) 下单
res, err := c.Pay(ctx, openapi.PayRequest{
    Channel:   "wechat",   // wechat / alipay / applepay / googlepay
    TradeType: "app",      // app / native / wallet / jsapi,留空按渠道推断
    Subject:   "iPhone 17 Pro",
    Amount:    19900,      // 单位:分
    OutTradeNo: "BIZ20260101000001", // 建议自己传:它是渠道侧的幂等键
    // AppID: "APP_IOS",  // 订单归属哪个产品;密钥绑定了应用就不用传
})
if err != nil {
    return err
}

// APP 拉起:把 res.Session.ClientParams.Wechat 交给客户端 SDK
// 扫码:用 res.Session.QRCode 渲染二维码
// 其它渠道见 res.Session.ClientParams.Alipay / ApplePay / GooglePay

// 2) 查单(向渠道同步查询)
q, err := c.Query(ctx, openapi.QueryRequest{
    Channel:    "wechat",
    OutTradeNo: res.OrderNo,
})
// q.Status: PENDING / SUCCESS / FAILED / CLOSED / REFUNDED / PARTIALLY_REFUNDED

// 3) 退款(同一个 OutRefundNo 重复提交只会退一次钱)
rf, err := c.Refund(ctx, openapi.RefundRequest{
    OutTradeNo:   res.OrderNo,
    OutRefundNo:  res.OrderNo + "-R1", // 幂等键,必填且要唯一
    RefundAmount: 19900,               // 0 表示全额
    Reason:       "用户申请退款",
})
// rf.Idempotent == true 表示这是重复提交,平台原样返回上次结果,没有再次出款
```

其它接口:

```go
c.Order(ctx, outTradeNo)                       // 查订单详情(只查平台记录)
c.Close(ctx, "wechat", outTradeNo)              // 关闭未支付订单
c.WalletCharge(ctx, openapi.WalletRequest{...}) // Apple Pay / Google Pay 扣款
c.Orders(ctx, openapi.OrdersQuery{Page: 1})    // 订单列表
c.Stats(ctx)                                   // 交易统计
c.StatsGrouped(ctx, openapi.StatsQuery{GroupBy: "app"}) // 分组统计
```

## 多产品(应用维度)

一个商户有多个产品(APP、小程序、H5……)时,每个产品可以在控制台
「应用管理」页建一个应用,给每个应用单独配一套渠道密钥
(两个微信 App 各配各的 appid / 商户号)。

下单时带上 `app_id`,订单就会归到那个应用下:

```go
res, err := c.Pay(ctx, openapi.PayRequest{
    AppID:  "app_xxx",   // 不传 = 未分类,用全局那套渠道配置
    Channel: "wechat",
    Amount:  19900,
})
```

**退款 / 查单 / 关单不用传 app_id**:平台会按**这笔订单当初归属的应用**
去找对应的凭证。用另一套商户号去退这笔单,渠道那边认不出来。

### 更省心的做法:把密钥绑到应用上

如果一把密钥只服务一个产品(或者一个产品一把),在控制台建密钥时
直接选「绑定应用」。之后下单**连 app_id 都不用传**:

```go
// 密钥绑了应用 app_xxx → 下面这样就够了,不用写 AppID
res, err := c.Pay(ctx, openapi.PayRequest{
    Channel: "wechat",
    Amount:  19900,
})
```

平台会自动把订单落到那个应用,并用**那个应用自己的**渠道配置。
万一代码里还传了别的 `app_id`,平台会直接返回 400 拒绝 ——
钱不会记到别的产品名下。

这么做的意义是消除「客户端忘了传 app_id」这种**静默降级**:
不绑定时,忘了传不会报错,订单会悄悄进「未分类」,报表对不上。
绑定之后,app_id 变成了密钥的一部分,传不了、也传不错。

> 这一切都在**同一个平台进程内**完成:不需要为每个产品单独起一套服务端。

### 「订单必须绑定应用」

控制台「应用管理」页有一个账户级开关:**订单必须绑定应用**。

- **开启**(新商户默认就是开的):这个账户的每一笔支付都必须归属某个应用。
  没带 `app_id` 的下单直接 400;没绑定应用的密钥也不能用来下单(建都建不出来)。
- **关闭**(老账户默认是关的,避免升级把别人的下单打断):没带 `app_id` 的下单
  会放行,订单落进「未分类」。

为什么默认对新商户开启:钱进来了却分不清是哪个产品的,事后只能靠人工翻日志猜,
报表也永远对不上。宁可当场拒绝,也不要事后靠猜。

> 注意:即使在关闭状态下,**密钥绑了应用**的调用方式仍然是推荐做法 ——
> 它不依赖客户端记得传 `app_id`。

统计可以按应用 / 渠道 / 天分组:

```go
r, err := c.StatsGrouped(ctx, openapi.StatsQuery{GroupBy: "app"})
// r.Groups[i].Label 是应用名,r.Groups[i].NetAmount 是净收入(分)
```

口径与不分组的总数一致:成功金额只算支付成功的订单,已退金额算占位与
处理中的退款(钱可能已经出去了),不算失败的退款。

**按天分组有上限**:平台单次最多扫描 20000 行订单,超了会把
`r.Truncated` 置成 true,这时 `r.Groups` 里只有最近的一部分数据。
按应用 / 按渠道分组走的是数据库聚合,不受这个上限影响。

## 接口一览

| 方法 | 路径 | 说明 | 需要的权限 |
|------|------|------|------------|
| POST | `/api/open/pay` | 下单 | `pay` |
| POST | `/api/open/query` | 查单(同步渠道) | `pay` |
| POST | `/api/open/order` | 查订单详情 | `pay` |
| POST | `/api/open/close` | 关闭订单 | `pay` |
| POST | `/api/open/refund` | 退款 | `refund` |
| POST | `/api/open/wallet/charge` | 钱包扣款 | `wallet` |
| GET  | `/api/open/orders` | 订单列表(`app_id` 可选过滤) | `pay` |
| GET  | `/api/open/stats` | 交易统计(`app_id` / `group_by` 可选) | `pay` |

## 字段约束(平台会先替渠道校验一遍)

平台按各家**官方接口的约束**先校验一遍再发给渠道,
所以下面这些填错会**当场拿到 400 和一句说人话的报错**(而不是渠道那句
`invalid parameter`):

| 字段 | 微信支付 | 支付宝 |
|------|----------|--------|
| `out_trade_no` | ≤ 32 字符,只能用可见 ASCII(不能有空格/中文) | ≤ 64 字符,同上 |
| `subject` | ≤ 127 **字符**(汉字算 1 个) | ≤ 256 字符 |
| `amount` | 整数分,必须 > 0 | 整数分,必须 > 0 |
| `trade_type` | 必须在该渠道支持的列表里(`native` / `app` / `jsapi` / `h5`) | 同理(`native` / `app` / `wap` / `web`) |

两处容易踩的坑:

- **金额单位是「分」**。传 `19.9` 想表达 19.9 元会被 JSON 解析直接拒掉
  (字段是整数),不会变成 19 分 —— 但也别传 `1990` 以为是 1990 元。
- **商户订单号是渠道侧的幂等键**:同一个单号重复下单不会重复扣款,
  但平台也会拒绝**用同一个单号改金额**。

`out_trade_no` 留空时平台会自动生成(`PAY` + 日期 + 随机后缀),
但**强烈建议自己传**:它是商户侧对账的唯一锚点。

## 用别的语言接?

签名算法是公开的,照着实现就行:

```
请求头:
  X-Pay-Key        密钥 ID(pk_xxx)
  X-Pay-Timestamp  Unix 秒(平台允许 ±300 秒偏差)
  X-Pay-Nonce      随机串(时间窗口内不可重复)
  X-Pay-Signature  HMAC-SHA256 签名(hex,小写)

待签名字符串(用 \n 连接):
  METHOD \n TARGET \n TIMESTAMP \n NONCE \n sha256(body) 的 hex
  METHOD   大写的请求方法
  TARGET   路径 + "?" + 原始查询串(没有查询串时只有路径)
  sha256(body) 为空时取空字节串的 sha256

签名:
  hex(HMAC-SHA256(密钥明文, 待签名字符串))
```

Go 里可以直接调 `openapi.Sign(secret, method, target, ts, nonce, body)`,
拿到的结果和这个包内部用的完全一样。

## 注意

- 密钥明文只在创建时返回一次,之后平台只保存密文,拿不到就重新建一把;
- 退款权限(`refund`)要单独授权,不能和只用来下单的密钥混用;
- 时间戳超窗、随机串重复(重放)、签名不符,一律拒绝;
- **不要**把密钥写进代码仓库,放环境变量或密钥管理系统。
