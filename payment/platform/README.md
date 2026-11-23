# bus/payment/platform —— 多商户「支付配置选择平台」

一个可以直接跑起来的支付控制台：**每个商户在网页上填写自己的支付渠道密钥、启用/停用渠道、
指定默认渠道、发起支付、查看订单**，配置全部落库（SQLite 默认，可切 MySQL）。

- 不用配置文件、不用改代码就能新增渠道配置；
- 页面同时展示「用户使用方式」，把接口调用方式直接讲清楚；
- 平台代收各渠道异步回调并自动验签，订单状态实时落库；
- **密钥加密存储**：私钥 / APIv3 密钥在库里是 AES-256-GCM 密文，接口读取一律脱敏，**密钥明文一律不回显浏览器**
  （没有「查看密钥原文」这类接口，要换密钥就重新填写后保存）；
- **敏感操作二次验证**：改配置、发起支付、退款、关单，都需要「登录口令 + 邮箱/短信/验证器 App 验证码」，
  换取票据（**用后即焚**，只在首次使用后 60 秒内允许连用几笔），并写入审计日志；
- **验证码通道页面可配**：SMTP / 短信网关参数在「账户安全」页里填，授权码与令牌 **AES-256-GCM 加密入库、永不回显**，
  不必写死在环境变量里。

---

## 〇、安全模型（先看这一节）

| 风险 | 处理方式 |
|------|---------|
| 密钥明文落库 | `pay_user_channels.cfg_enc` 存 AES-256-GCM 密文，主密钥来自 `PAY_PLATFORM_MASTER_KEY` / 独立密钥文件，**与数据库分开存放** |
| 密钥被接口读走 | 所有读取接口（`/api/me` 等）返回前统一脱敏（`********`），前端只看到「已保存」；改配置时提交空值 = 不修改；**没有密钥明文回显接口**（`/api/channels/{channel}/reveal` 已下线，返回 410） |
| 令牌明文落库 | `pay_users.access_token_hash` 只存 HMAC-SHA256 散列（密钥由主密钥派生），老库的明文令牌启动时自动迁移并清空 |
| 验证码被暴力猜 | 验证码 6 位、5 分钟有效、**只能用一次**；库里只存带密钥散列；**单个验证码最多错 5 次**，超了直接作废 |
| 口令被暴力猜 | 登录接口按 IP / 账户限流（10 次 / 5 次每分钟）；**口令错误只计数不锁账户**（否则知道商户名就能把别人锁死），只有「口令已对、验证码没过」才锁定账户 15 分钟 |
| 验证码被当轰炸器 | 下发验证码按用户（3 次/分）与 IP（10 次/分）限流；登录验证码按 IP（5 次/分）与账户名（3 次/分）限流 |
| 验证码被改发到攻击者邮箱 | 二次验证目标**强制**为已绑定的地址（`purpose=bind` 绑定新地址时才允许指定，且必须先验口令）；登录验证码也只发到已绑定地址 |
| 「邮件验证码」只写日志、用户永远收不到 | 平台内置下发器只把码写进日志，接口会**如实回 `delivered=false`** 并在提示里说明；商户可在「账户安全」页配置自己的 SMTP / 短信网关（加密入库），并用「发送测试邮件」实测 |
| 换二次验证方式要先关再开 | `POST /api/account/mfa/enable` 是**覆盖**绑定：直接选新方式 + 新地址发码 + 保存即可，旧的立即作废 |
| TOTP 密钥被前端伪造 | 密钥由**服务端**生成并加密暂存，启用时只认服务端那份；前端回传的密钥一律忽略 |
| TOTP 密钥留在库里被复用 | 关闭二次验证时一并清空 TOTP 密钥；「生成密钥」写的是「待确认」列，不会冲掉正在用的绑定 |
| 收不到码却把二次验证开了 | 邮箱 / 短信方式**没有可用下发通道时禁止开启**（后端 400 + 前端黄色警告）——开了之后登录就要这个码，收不到等于把自己锁在门外 |
| 已有绑定但通道没了 → 账户彻底锁死 | 通道不可用的绑定**不再被强制校验**：登录跳过并回 `mfa_skipped=true`，身份校验与关闭操作都只要口令（否则连通道都配不了）；页面上显示「已开启但无效」 |
| 短信网关回 200 其实是失败 | 平台会解析网关响应体（`{"code":1001,"msg":"余额不足"}` 之类），识别到失败就回 502 并带上原因，不会误报「发送成功」 |
| 接收地址写错、验证码发不出去还查不出原因 | 绑定与发码前校验地址格式（邮箱 / 手机号），格式不对当场报错 |
| 没开通邮箱/短信的商户被迫二次验证 | 二次验证**默认关闭**：账户没开时下拉默认停在「关闭(不启用)」，不显示任何绑定表单，「开启」按钮禁用 |
| 开了二次验证就登不进去 | 新增**公开**接口 `POST /api/login/code`：未登录时也能取登录验证码，响应不区分「账户不存在 / 没开二次验证 / 已发送」，不能用来枚举商户名 |
| 令牌出现在 URL | 令牌只从 `X-User-Token` 头或 `pay_token` Cookie 取，**不再支持 `?token=`**（URL 会进历史、Referer、反代日志） |
| 会话永不过期 | 访问令牌带有效期（`Options.SessionTTL`，默认 24 小时），到期后立即失效；退出登录即刻轮换令牌并清 Cookie |
| IP 限流被绕过 | 来源 IP 去掉端口（否则每次连接都是新 key）；`X-Forwarded-For` **默认不采信**，只在 `Options.TrustForwardedFor=true`（确实部署在可信代理后）时才用 |
| 任何人进别人空间 | 登录需要「商户名称 + 口令」；口令只存 PBKDF2-HMAC-SHA256（600000 次迭代）散列 |
| 商户之间互相改单 | 订单主键是 `(user_id, out_trade_no)`，**跨商户读写在存储层就被挡住**；查不到别人的订单一律按「不存在」处理 |
| 退款超发 | 平台自己记退款流水（`pay_refunds`）：退款金额不得超过订单金额，**累计退款也不得超过订单金额**（含「占位中 / 渠道处理中」的退款）；未支付成功的订单不能退；部分退款把订单标成 `PARTIALLY_REFUNDED` |
| 退款重试导致重复出款 | 退款单号 `out_refund_no` 是幂等键：`(user_id, out_refund_no)` 上有**唯一索引**，重复提交同一单号**不会二次出款**（原样返回上次结果）；退款**先占位再调渠道**（`PENDING` → 成功 / 失败），渠道失败不占额度 |
| 退款退错渠道 | 退款 / 关单 / 查单 / 钱包扣款**一律用订单自己的渠道**，请求里带了别的渠道直接报错（退错渠道 = 钱退到别人的商户号） |
| 订单状态被乱改 | 订单状态走**状态机**：状态只能向前走，迟到的回调 / 查单不能把「已退款」改回「成功」；关单只允许关「待支付」的订单 |
| 回调金额对不上 | 回调先校验金额与订单一致、appid / mchid 与本商户配置一致，不一致直接拒收（不写账） |
| 关单绕过验证 | 关单属于资金操作，与退款一样要求二次验证票据 |
| 回调被引到别处 | `notify_url` 由平台生成；商户自带的 `notify_url` 只有与 `PublicBaseURL` **同源**才接受，否则拒绝（否则既是 SSRF 跳板，又会让订单状态永远停在 pending） |
| 商户往任意目录写文件 | 微信平台证书缓存目录由平台分配（`<CertDir>/<user_id>/`），**忽略商户自填的 `cert_dir`** |
| 账号被抢注 | 可设置 `Options.RegistrationCode`：设置后首次登录必须带注册码（校验在查用户之前，避免变成探测商户名是否已注册的口子） |
| 页面被 XSS / 点击劫持 | 统一响应头：`Content-Security-Policy`（`default-src 'self'`）、`X-Frame-Options: DENY`、`X-Content-Type-Options: nosniff`、`Referrer-Policy: no-referrer`、`Permissions-Policy`；HTTPS 下补 HSTS |
| 页面被 XSS / 令牌泄露 | 改配置、发起支付、退款、关单还需要**二次验证票据**（口令 + 邮箱/短信验证码）；票据**用后即焚**（首次使用后 60 秒内可连用几笔，之后作废） |
| 操作无法追溯 | `pay_audit_logs` 记录登录 / 改配置 / 支付 / 退款 / 来源 IP |
| 商户后端接不进平台 | 新增**开放接口** `/api/open/*`：商户用自己的业务后端直接下单 / 查单 / 退款，不必弹浏览器，也不必走二次验证票据 |
| 开放接口密钥泄露 | 密钥**只在创建时显示一次**，库里只存 AES-256-GCM 密文；请求用 HMAC-SHA256 签名，密钥本身不出现在请求里 |
| 开放接口被重放 | 签名带时间戳（±300 秒窗口）与随机串；同一随机串在窗口内只接受一次（**先验签再查重放**，否则伪造签名就能占住随机串） |
| 开放接口越权 | 密钥按 scope 授权：`pay`（下单/查单/关单/查询）、`refund`（退款，**单独授权**）、`wallet`（钱包扣款） |
| 开放接口跨商户 | 密钥绑定所属商户，订单查询与写操作一律带 `user_id` 过滤；商户被平台禁用后密钥立即失效 |
| 平台方无法治理商户 | 新增**平台管理接口** `/api/admin/*`：商户列表、全平台统计、启用/禁用商户（禁用后令牌与密钥同时失效） |

**必须知道的边界**：静态加密解决的是「数据泄露」（拖库、备份、SQL 注入），**不解决「服务器被完全攻陷」**。
主密钥如果和数据库放在同一台机器上，攻陷服务器就等于拿到全部密钥——所以生产环境请：

1. 用环境变量 / KMS 注入 `PAY_PLATFORM_MASTER_KEY`，**不要**用默认的 `./payment-data/master.key`；
2. 该文件本身要备份（丢了里面的密钥就再也解不开了），权限设成 `0600`；
3. 平台本身只做「商户之间隔离 + 敏感操作二次验证」，对外还要在网关层加自己的鉴权与限流。

> 令牌 / 验证码的散列密钥由主密钥派生：**没有主密钥就没有真正的保护**（会退化成固定密钥，
> 仅够本地调试）。主密钥轮换会让所有已签发的令牌与验证码失效，需要重新登录。

主密钥的取值顺序：`Options.MasterKey` → 环境变量 `PAY_PLATFORM_MASTER_KEY` →
环境变量 `PAY_PLATFORM_MASTER_KEY_FILE` 指向的文件 → `./payment-data/master.key`（不存在时自动生成随机密钥）。

二次验证的下发通道有两条路：

1. **页面上配（推荐）**：在「账户安全 → 验证码下发通道」里填 SMTP（服务器 / 端口 / 加密方式 /
   账号 / 授权码 / 发件人）或短信网关（地址 / 令牌 / 字段名），保存后**加密入库**
   （`pay_mfa_channels.cfg_enc`，AES-256-GCM，AAD 与渠道配置分开，密文不能互相搬运），
   读取接口只回显「配没配」，**授权码永不回显浏览器**。
   填完点「发送测试邮件 / 发送测试短信」能立刻确认通道是否真的通。
2. **代码里注入**：`Options.MFASender`（`MFASender` 接口）：

   - 默认 `LogMFASender`：把验证码写到日志，**仅限开发/内网**；
   - 生产请实现 `MFASender` 接口对接自己的短信/邮件网关，或直接用内置的 `HTTPMFASender`
     （POST JSON `{"type","target","code"}` 到你的网关）。

> 账户自己配了通道时，**账户通道优先**于 `Options.MFASender`。

#### 通道参数（页面上填什么）

| 通道 | 字段 | 说明 |
|---|---|---|
| 邮件 | `host` / `port` | SMTP 服务器与端口。QQ：`smtp.qq.com` + 587；163：`smtp.163.com` + 465 |
| 邮件 | `tls_mode` | `starttls`（587，先握手再加密）/ `tls`（465，连接即加密）/ `none`（仅内网）。不填时按端口推断 |
| 邮件 | `username` / `password` | 登录账号与**授权码**（不是登录密码）；授权码加密入库，不回显 |
| 邮件 | `from_addr` | 发件人地址，留空则用登录账号 |
| 短信 | `url` | 短信网关地址，平台会 POST `{"code":"123456","target":"138…"}` |
| 短信 | `token` | 网关令牌（可选，`Authorization: Bearer`），加密入库，不回显 |
| 短信 | `code_field` / `target_field` | 网关取验证码 / 接收地址的字段名，默认 `code` / `target` |

> **邮件是怎么发的**：平台不自己拼 MIME，交给工作区里 `gapi` 的 `mail` 包（底层 gomail），
> 头部（Date / Message-ID / MIME-Version）与正文编码都由它按 RFC 生成 ——
> 自己手搓的报文会被网易 163 这类收件方**静默丢弃**（SMTP 照样回 250，用户就是收不到）。
> `starttls` 模式下服务器不支持 STARTTLS 会**直接报错**，绝不让验证码明文发出去；
> 加密通道会校验服务器证书（`tls` / `starttls`），`none` 模式不强制加密。
>
> `bus/go.mod` 里 `github.com/AIntelligenceGame/gapi` 用 `replace => ../gapi` 指向本地源码
> （同一个工作区里的兄弟模块）。等 gapi 发了正式版本，把 replace 换成版本号即可。

网关返回 2xx **不代表发送成功**：很多网关失败也回 200，错误码在 body 里
（`{"code":1001,"msg":"余额不足"}`）。平台会解析 body，识别到错误就如实报错（HTTP 502），
不会让页面显示「测试发送成功」而短信根本没发出去。

#### 「通道不可用」时的自愈规则

绑定还在、但验证码发不出去（通道没配 / 被删 / 平台没注入下发器）时，
这种二次验证**拦不住任何人**（码只写日志），却能挡住正主。平台的处理是：

| 场景 | 处理 |
|---|---|
| 开启邮箱 / 短信二次验证 | **拒绝**（400），并提示去配通道或改用验证器 App |
| 发送验证码 | **拒绝**（400），不再生成一个收不到的码 |
| 登录 | **跳过**二次验证，响应回 `mfa_skipped=true` + `mfa_note`，并记审计 |
| 身份校验（换敏感操作票据） | **只要口令**（否则连通道都配不了 = 死锁） |
| 关闭二次验证 | **只要口令** |
| 「账户安全」页 | 显示「已开启但无效」，提示改用验证器 App 或关掉 |

开发联调想在日志里取码，用 `Options.AllowLogOnlyMFA`（示例程序里是 `-log-mfa`）：
打开后日志通道就算「有通道」，接口回 `delivered=false` 并提示「验证码已写入服务器日志」。

---

## 一、快速启动

### 0. 怎么跑起来（目录别搞错）

> ⚠️ **必须在 `bus/` 模块根目录执行。** 工作区根目录 `gowork/` 里还有 dbhouse、xterm 等别的模块，
> 而根目录**没有** `go.mod` / `go.work`——在那里执行 `go run ./payment/...` 只会得到
> `go.mod file not found in current directory or any parent directory`。

```bash
cd /service/home/devyuandeqiao/gomodworkspace/gowork/bus   # ← 模块根目录
go run ./payment/examples/platform
```

或者不用记目录（Makefile 保证工作目录）：

```bash
cd /service/home/devyuandeqiao/gomodworkspace/gowork/bus
make build-run-platform
```

起来之后：

- 控制台：<http://127.0.0.1:8089/>
- 自检：`curl http://127.0.0.1:8089/healthz`
- 回调地址前缀：`http://127.0.0.1:8089/notify/<channel>/<user_id>`

**IDE 里点「Run main.go」**：把 Working Directory 设成 `bus` 模块根目录。
在 IDE 里点运行用的是「工作区根目录」当 cwd，就会踩上面那个 go.mod 错误。

**数据目录**：SQLite 库、主密钥、微信平台证书都写在 **bus 模块根目录**下的 `payment-data/`
（`pay.db` / `master.key` / `certs/`），**与启动目录无关**——示例程序用编译期的源码路径
反查模块根目录，所以无论从哪个目录启动，数据都在同一处，不会散落到源码树，
也不会因为换目录启动而生成新主密钥（旧渠道密钥就再也解不开了）。
已加入 `bus/.gitignore`。

### 1. SQLite（默认）

SQLite 走**纯 Go 驱动**（`github.com/glebarez/sqlite`），不需要 cgo、不需要额外装依赖，
**默认就是 SQLite**，不用加任何参数：

```bash
go run ./payment/examples/platform
# 控制台: http://127.0.0.1:8089/
```

常用参数：

| 参数 | 默认 | 说明 |
|------|------|------|
| `-addr` | `:8089` | 监听地址 |
| `-driver` | `sqlite` | `sqlite` / `mysql` |
| `-dsn` | `./payment-data/pay.db` | SQLite 文件路径（父目录会自动创建） |
| `-base` | `http://127.0.0.1<addr>` | 对外访问地址（**平台默认值**），用来生成回调地址；商户可在控制台里填自己的域名覆盖它 |
| `-regcode` | 空 | 新商户注册码，留空 = 允许自助注册 |
| `-session` | `24` | 登录会话有效期（小时），0 = 不过期 |
| `-certdir` | `./payment-data/certs` | 微信平台证书缓存根目录 |
| `-trust-xff` | `false` | 是否采信 `X-Forwarded-For`（只在可信反代后开启） |

MySQL 版：

```bash
go run ./payment/examples/platform -driver mysql -dsn "user:pass@tcp(127.0.0.1:3306)/pay?charset=utf8mb4"
```

也可以直接在自己的程序里嵌入：

```go
import (
    "github.com/AIntelligenceGame/bus/payment/platform"
)

p, err := platform.New(platform.Options{
    Driver:        "sqlite",
    DSN:           "./payment-data/pay.db",
    Addr:          ":8080",
    PublicBaseURL: "https://pay.example.com",   // 用于生成回调地址
    // MasterKey 留空则依次读 PAY_PLATFORM_MASTER_KEY /
    // PAY_PLATFORM_MASTER_KEY_FILE / ./payment-data/master.key(自动生成)
    MasterKey: os.Getenv("PAY_PLATFORM_MASTER_KEY"),
    // 注册码:设置后首次登录必须带上,防止商户名称被别人抢注
    RegistrationCode: os.Getenv("PAY_PLATFORM_REGISTRATION_CODE"),
    // 二次验证下发通道:生产环境对接自己的短信/邮件网关
    MFASender: platform.HTTPMFASender{
        URL:   "https://sms-gw.example.com/send",
        Token: os.Getenv("MFA_GW_TOKEN"),
    },
    // 登录会话有效期(默认 24 小时,0 表示不过期)
    SessionTTL: 24 * time.Hour,
    // 平台托管的微信平台证书缓存根目录(默认 ./payment-data/certs)
    CertDir: "./payment-data/certs",
    // 只有确实部署在可信反向代理后面时才打开(代理必须覆写而不是透传 X-Forwarded-For)
    TrustForwardedFor: false,
})
if err != nil { log.Fatal(err) }
log.Fatal(p.Start())
```

也可以传入自己已经打开的 GORM 连接（`Options.DB *gorm.DB`），此时 `Driver` / `DSN` 会被忽略。

### 2. MySQL

```go
p, err := platform.New(platform.Options{
    Driver: "mysql",
    DSN:    "user:pass@tcp(127.0.0.1:3306)/pay?charset=utf8mb4",
})
```

MySQL 驱动已随 `bus` 模块内置（`github.com/go-sql-driver/mysql`），无需额外引入。
建库即可，表结构由平台自动创建：

```sql
CREATE DATABASE pay DEFAULT CHARACTER SET utf8mb4;
```

### 3. 挂到已有服务

```go
http.Handle("/payment/", platform.New(...).Handler())
```

> `Handler()` 返回的是 `http.Handler`，路由使用 Go 1.22+ 的方法+路径匹配。

### 4. 密钥怎么管才安全（重点）

| 环节 | 做法 |
|------|------|
| 录入 | 全部在网页上填写，不写进代码、不写进配置文件 |
| 存储 | 数据库里存 **AES-256-GCM 密文**，主密钥与数据库分开（环境变量 / KMS） |
| 读取 | 所有读取接口返回前统一脱敏，**私钥不会回显到浏览器**（平台不提供任何密钥明文回显接口） |
| 使用 | 改配置 / 发起支付 / 退款 / 关单，都需要「口令 + 邮箱/短信验证码」换取的票据（用后即焚） |
| 追责 | `pay_audit_logs` 记录登录、改配置、支付、退款与来源 IP |

---

## 二、控制台页面

控制台前端是独立的 Vite + TypeScript 工程（源码在 `bus/payment-web/`），打包产物
`web/dist/` 由 `//go:embed` 打进二进制。页面用 hash 路由，直接访问下列路径也会
重定向到对应的 hash。

**界面形态**：登录后是「左侧边栏 + 顶栏 + 内容区」的后台布局（侧边栏按
「概览 / 交易 / 配置 / 账户」分组），未登录时是独立的居中登录页。
配色、间距、圆角全部走 CSS 变量，并跟随系统的深色模式自动切换。

| 页面 | 路径 | 说明 |
|------|------|------|
| 概览 | `/` · `#/` | 未登录：登录表单（**只有商户名称 + 口令**；验证码框在服务端回 `mfa_required` 时才出现，注册码框在平台设了注册码时才出现）；已登录：**交易统计卡**、**渠道状态卡**、快捷入口 |
| 渠道配置 | `#/channels` | **回调地址卡片（对外访问地址：控制台里改回调域名，不用重启平台；留空 = 用平台默认 `-base`；域名与当前平台地址不一致会提示）**、渠道总览卡（状态徽章）、微信 / 支付宝 / 钱包的密钥表单（**启用 / 设为默认开关就在各自的卡片里**，点该卡片自己的保存按钮生效）、微信 / 支付宝连通性测试（**结果里带实际请求的地址**）、钱包的本地配置校验、一键复制回调地址（**不提供密钥明文回显**） |
| 支付体验 | `#/playground` | 选渠道 + 选支付方式（**按渠道过滤**，支付宝没有 jsapi/h5、钱包只有 wallet）+ 填金额 → 发起支付，页面直接显示二维码与客户端参数 |
| 订单 | `#/orders` | **交易概览统计卡**、订单筛选（状态 / 渠道 / 关键字）、**分页列表**（状态用中文徽章）、**导出 CSV**、查单、**关闭待支付订单**、**退款**（金额弹窗 + 上限校验）、**退款流水**（状态变更后列表自动刷新） |
| 应用管理 | `#/apps` | **多产品（App）管理**：新建 / 修改 / 删除应用、**每个应用配自己的一套渠道密钥（专属覆盖全局）**、**「订单必须绑定应用」强制开关**（需二次验证）、应用维度的订单数与流水 |
| 报表 | `#/reports` | 按**应用 / 渠道 / 天**分组统计（应用维度报表、渠道维度报表），支持分页与导出 |
| 账户安全 | `#/security` | 安全状态卡（含邮件 / 短信通道是否已配）、口令修改、二次验证（**邮箱 / 短信 / 验证器 App 三选一**，已开启时可直接「更换接收方式或地址」，不用先关再开）、**验证码下发通道卡片**（SMTP / 短信网关：保存 + 发送测试 + 删除）、**审计日志分页 + 按动作筛选** |
| 开放接口 | `#/keys` | 创建 / 列出 / 吊销 **API 密钥**（密钥明文只显示一次，弹窗展示 + 一键复制）、**建密钥时可选「绑定应用」**（强制模式下必须选）、签名算法接入说明（可折叠） |

交互约定：

- **所有提交按钮都有 loading 态并禁止重复点击**——支付、退款这类操作允许连点会重复下单；
- **危险操作（吊销密钥、关闭二次验证、退款）先弹确认框**，退款还带金额上限校验；
- **错误就近显示**，成功提示走右上角 toast，不再把所有提示堆在表单底部；
- **空列表显示引导**（而不是一行灰字），例如「还没有配置任何支付渠道 → 去配置渠道」；
- **控件按账户/渠道的真实状态显示**：没开二次验证就不给「验证码」框，没开启的渠道不给「关闭」按钮，
  选错交易类型也不会让用户去试——凡是「点了必然报错」的控件一律不出现；
- **二次验证方式随账户状态切换**：选了「验证器 App」就没有「发送验证码」按钮（动态码由 App 本地算出，
  没有「下发」这回事）；选了邮箱 / 短信才显示接收地址与发送按钮。

控制台目前**不覆盖**的流程：钱包渠道的最后一步（`POST /api/wallet/charge`，需要客户端钱包 SDK 授权后回传支付令牌）只能在商户自己的客户端完成，控制台只能下单、不能替客户端扣款。

登录方式：首页输入商户名称与口令 → `POST /api/login` → 返回令牌，保存在浏览器
`localStorage.pay_token`，同时下发 `pay_token` Cookie（HttpOnly + SameSite=Lax，HTTPS 下加 Secure）。
所有接口用 `X-User-Token` 头或 `pay_token` Cookie 鉴权。

> **令牌只走请求头 / Cookie，不支持 `?token=` 查询参数**：URL 会进浏览器历史、Referer、
> 反向代理日志与监控看板，令牌出现在 URL 里等于公开令牌。
> 每次登录都会**换发新令牌**（旧令牌立即失效，防会话固定）。

> 首次登录会自动创建账户并要求设置不少于 8 位的口令；之后同名账户必须用口令登录。
> 平台设置了 `RegistrationCode` 时，首次登录还要带上注册码（登录框里的「注册码」）。

---

## 三、HTTP 接口

### 用户与配置

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/login` | `{"name":"商户A","password":"...","code":"..."}` → `{"user_id","name","token"}`。通道不可用导致跳过二次验证时，额外回 `mfa_skipped=true` 与 `mfa_note`（如实告知，而不是假装拦住了） |
| POST | `/api/login/code` | **公开**：`{"name":"商户A"}` → 下发登录用验证码（只发到该账户已绑定的接收地址） |
| POST | `/api/logout` | 作废当前令牌 |
| GET | `/api/me` | 当前用户、已配置渠道（**密钥脱敏**）、默认渠道、各渠道回调地址、**`notify_base_url`（商户自己配的对外地址）与 `notify_base_default`（平台默认 `-base`）** |
| POST | `/api/channels/{channel}` | 保存渠道配置（`wechat`/`alipay`/`applepay`/`googlepay`），**需二次验证票据** |
| POST | `/api/channels/{channel}/test` | 连通性测试（微信会真的去下载平台证书），**需二次验证票据** |
| POST | `/api/channels/{channel}/reveal` | **已下线（410 Gone）**：不再提供密钥明文回显。要换密钥就重新填写后保存，用「测试连通性」确认配置可用 |

### 账户安全

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/account/security` | 当前安全状态（口令是否设置、二次验证是否开启及方式、邮件 / 短信通道是否已配置、**`mfa_deliverable`**：当前绑定能不能真的把验证码发出去） |
| POST | `/api/account/password` | `{"old_password","new_password"}` 修改登录口令 |
| POST | `/api/account/mfa/code` | `{"type":"email\|sms","target":"...","purpose":"bind\|sensitive\|login\|disable","password":"..."}` 下发验证码。**只有 `purpose=bind` 可以指定接收地址**（且必须先验口令）；其余用途强制发到已绑定地址。响应含 `delivered`（是否真的发出去了）与 `message`。**没有可用下发通道时返回 400**（不再生成一个收不到的码） |
| POST | `/api/account/mfa/enable` | `{"type":"email\|sms\|totp","target","password","code"}` 开启 / **更换**二次验证（覆盖旧绑定）。`totp` 的 `code` 是验证器 App 的动态码，`target` 留空。**email/sms 没有可用下发通道时返回 400**（开了就收不到，等于锁死账户）；接收地址格式不对也返回 400 |
| POST | `/api/account/mfa/disable` | `{"password","code"}` 关闭二次验证。`totp` 的 `code` 是当前动态码（不需要下发验证码）；**通道不可用时 `code` 可留空**（那个码用户收不到，硬要就是死锁） |
| POST | `/api/account/mfa/totp/setup` | 生成 TOTP 密钥（服务端加密暂存），返回 `{"secret","uri","qrcode_url","digits","period"}` |
| GET | `/api/account/mfa/channels` | 读取本账户的验证码下发通道（**授权码 / 令牌只回显 `password_set` / `token_set`，不回显内容**） |
| POST | `/api/account/mfa/channels` | `{"channel":"email\|sms","config":{...}}` 保存通道配置，**需二次验证票据**；密钥留空 = 保持原值。邮件 `config`：`{host,port,tls_mode,username,password,from_addr}`；短信 `config`：`{url,token,code_field,target_field}` |
| POST | `/api/account/mfa/channels/delete` | `{"channel":"email\|sms"}` 删除通道配置，**需二次验证票据** |
| POST | `/api/account/mfa/channels/test` | `{"channel","target"}` 真的发一条验证码出去（确认通道通不通），**需二次验证票据**；只写日志时返回 502；网关返回错误码也返回 502 并带原因 |
| POST | `/api/account/verify` | `{"password","code"}` → `{"verify_token","expires_in"}` 敏感操作票据。**通道不可用时 `code` 可留空**（否则连通道都配不了） |
| POST | `/api/account/notify-base` | `{"notify_base_url":"https://pay.example.com"}` 保存**商户自己的对外访问地址**（回调地址前缀），**需二次验证票据**；空字符串 = 恢复平台默认（`-base`）。响应带新的 `notify_urls` |

票据用法：后续敏感请求带上 `X-Verify-Token: <verify_token>`，默认 10 分钟有效。
（票据同样**不放进 URL**。）
票据**用后即焚**：第一次用掉之后只在 60 秒内允许再连用几笔（控制台里「保存 → 立刻测试」这类连招），
超过就作废、要重新验证 —— 票据泄露的窗口从 10 分钟压到 1 分钟。
前端会自动识别「票据失效」并重新弹验证框后重试一次（403 一定发生在业务逻辑之前，重试不会重复扣款）。

保存渠道示例（`X-Verify-Token` 是二次验证票据，敏感操作必带）：

```bash
# 1) 取票据:口令 + 验证码
TOKEN=$(curl -s -X POST 'http://127.0.0.1:8080/api/account/verify' \
  -H 'X-User-Token: <token>' -H 'Content-Type: application/json' \
  -d '{"password":"你的登录口令","code":"123456"}' | sed -n 's/.*"verify_token":"\([^"]*\)".*/\1/p')

# 2) 保存渠道配置
curl -X POST 'http://127.0.0.1:8080/api/channels/wechat' \
  -H 'X-User-Token: <token>' -H "X-Verify-Token: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"is_default":true,"wechat":{
        "app_id":"wx...","mch_id":"190...","serial_no":"...",
        "private_key_pem":"-----BEGIN PRIVATE KEY-----\n...",
        "api_v3_key":"32位密钥","auto_fetch_certs":true}}'
```

> `cert_dir` 由平台统一分配（`<CertDir>/<user_id>/`），**忽略商户自填的值**。

> 敏感字段提交空字符串 = **不修改**已保存的值（页面上拿到的是脱敏结果，不会回传原文）。

### 支付与订单

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/pay` | 发起支付，返回会话、二维码地址、回调地址、接入说明（**需二次验证票据**） |
| POST | `/api/wallet/charge` | 钱包渠道扣款（Apple Pay / Google Pay），**需二次验证票据** |
| POST | `/api/query` | 向渠道查单并回写状态（**用订单自己的渠道**；状态走状态机，不会把已退款订单改回成功） |
| POST | `/api/refund` | 退款（`refund_amount` 为 0 表示全额），**需二次验证票据**；`out_refund_no` 是幂等键，重复提交同一单号**不会二次出款**（返回 `{"idempotent":true}`）；返回 `{"refund_amount","refunded_total","order_status"}` |
| POST | `/api/close` | 关闭订单，**需二次验证票据**；只有「待支付」的订单能关 |
| GET | `/api/orders` | 订单列表（分页 + 筛选）：`?page=1&size=20&status=&channel=&keyword=&from=&to=` → `{"orders","page","size","total","pages"}` |
| GET | `/api/orders/export` | 订单导出 CSV（同筛选条件，带 UTF-8 BOM，单次上限 5000 条，记审计） |
| GET | `/api/refunds` | 退款流水：`?limit=100` → `{"refunds":[...]}` |
| GET | `/api/stats` | 交易统计：订单数、成功数、成功率、成交额、已退金额、净收入、今日订单与今日成交额 |
| GET | `/api/audit-logs` | 操作审计日志（分页 + 按动作筛选）：`?page=1&size=50&action=account.login` |
| GET | `/api/qrcode?content=...` | 二维码 PNG（`/api/pay` 返回的 `qrcode_url`） |
| GET | `/healthz` | **公开**健康检查（部署探针） |
| GET | `/api/version` | **公开**版本信息（`{"name","version"}`） |
| POST | `/notify/{channel}/{user_id}` | **各渠道异步回调入口**（公开，不需要令牌） |

下单示例：

```bash
curl -X POST 'http://127.0.0.1:8080/api/pay' \
  -H 'X-User-Token: <token>' -H "X-Verify-Token: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"channel":"wechat","trade_type":"native","out_trade_no":"ORDER001",
       "subject":"iPhone 17 Pro","amount":19900}'
```

```json
{
  "session": {"qrcode": "weixin://wxpay/bizpayurl?pr=..."},
  "order_no": "ORDER001",
  "amount": 19900,
  "yuan": "199.00",
  "notify_url": "https://pay.example.com/notify/wechat/<user_id>",
  "qrcode_url": "/api/qrcode?content=weixin%3A%2F%2F...",
  "usage": ["扫码支付:把 session.qrcode 渲染成二维码图片给用户扫码。", "..."]
}
```

`channel` 省略时走该商户的默认渠道；`trade_type` 省略时自动判断
（微信：有 `payer_id` 走 JSAPI，否则扫码；支付宝：有 `payer_id` 走 WAP，否则扫码；钱包：wallet）。

### 开放接口（机器对机器）

控制台里的操作都要「浏览器会话 + 二次验证票据」，那是给人用的。
商户自己的**业务后端**要下单、查单、退款，不可能弹浏览器弹窗 ——
所以平台另开一条机器对机器的通道：`/api/open/*`，用 **API 密钥 + 请求签名**鉴权。

> 业务规则与控制台**完全一致**（退款上限、订单归属、回调地址同源校验……）：
> 两者共用 `core.go` 里的同一份实现，不会出现「控制台里退不了、接口里能退」这种绕过。

#### 1. 申请密钥

在控制台「开放接口」页创建，或调接口（**需二次验证票据**）：

```bash
curl -X POST 'http://127.0.0.1:8080/api/keys' \
  -H 'X-User-Token: <token>' -H "X-Verify-Token: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"订单服务","scopes":["pay","refund"],"ttl_days":0,"app_id":"APP_IOS"}'
```

`app_id` 是**可选**的绑定：绑了之后，这把密钥下的所有订单自动归到那个应用，
下单时**不用再传** `app_id`；传了别的 `app_id` 会被拒（400）。
账户开启了「订单必须绑定应用」时，`app_id` **必填**（见下面的强制模式一节）。

```json
{
  "key": {"key_id": "pk_3f2a...", "name": "订单服务", "scopes": ["pay", "refund"], "enabled": true, "app_id": "APP_IOS"},
  "secret": "sk_xxxxxxxxxxxxxxxx",
  "notice": "密钥明文只显示这一次,请立即保存;之后只能重新创建或吊销。"
}
```

**`secret` 只在这一次响应里出现**，之后平台只保存 AES-256-GCM 密文，无法再次查看。
拿不到就重新创建一把。

| 权限 | 含义 |
|------|------|
| `pay` | 下单、查单、关单、订单与统计查询 |
| `refund` | 退款（资金操作，**单独授权**） |
| `wallet` | 钱包渠道扣款（Apple Pay / Google Pay 的最后一步） |

密钥列表与吊销：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/keys` | 本商户的密钥列表（**不含明文**） |
| POST | `/api/keys` | 创建密钥，**需二次验证票据**；返回一次性明文 |
| POST | `/api/keys/{key_id}/revoke` | 吊销密钥，**需二次验证票据**（只能吊销自己的） |

#### 2. 签名算法

每个请求带四个头：

| 请求头 | 内容 |
|--------|------|
| `X-Pay-Key` | 密钥 ID（`pk_xxx`，公开标识） |
| `X-Pay-Timestamp` | Unix 秒（平台允许 **±300 秒**偏差） |
| `X-Pay-Nonce` | 随机串（时间窗口内不可重复） |
| `X-Pay-Signature` | HMAC-SHA256 签名（hex，小写） |

待签名字符串（用 `\n` 连接）：

```
<METHOD>\n<TARGET>\n<TIMESTAMP>\n<NONCE>\n<sha256(body) 的 hex>
```

- `METHOD`：大写的请求方法
- `TARGET`：`路径 + "?" + 原始查询串`（没有查询串时只有路径）
- `sha256(body)`：请求体的 SHA-256（空请求体取空字节串的摘要）

签名 = `hex(HMAC-SHA256(密钥明文, 待签名字符串))`。

Go 实现：

```go
target := r.URL.Path
if r.URL.RawQuery != "" {
    target += "?" + r.URL.RawQuery
}
sum := sha256.Sum256(body)
canonical := strings.Join([]string{
    strings.ToUpper(r.Method), target, ts, nonce, hex.EncodeToString(sum[:]),
}, "\n")
mac := hmac.New(sha256.New, []byte(secret))
mac.Write([]byte(canonical))
signature := hex.EncodeToString(mac.Sum(nil))
```

请求体与查询串**都参与签名**：改了金额、改了分页参数都会验签失败。

**重放防护**：平台记住所用过的 `X-Pay-Nonce`，同一个密钥在 ±300 秒窗口内
重复的随机串会被判为重放并拒绝（先验签再记随机串：只让**持有密钥**的调用方消耗随机串，
否则拿到密钥 ID 的人就能把真实请求的随机串「用掉」）。
和下面的限流一样，这个随机串缓存是**进程内**的：多实例部署时每个实例各记各的，
跨实例的重放不会被拦 —— 需要全局防重放请在网关层统一收口。

#### 3. 接口

| 方法 | 路径 | 权限 | 说明 |
|------|------|------|------|
| POST | `/api/open/pay` | `pay` | 下单，请求体与 `/api/pay` 相同 |
| POST | `/api/open/query` | `pay` | 向渠道查单并回写状态 |
| POST | `/api/open/order` | `pay` | 查订单详情（只查平台记录，不发渠道请求） |
| POST | `/api/open/close` | `pay` | 关闭订单 |
| POST | `/api/open/refund` | `refund` | 退款 |
| POST | `/api/open/wallet/charge` | `wallet` | 钱包渠道扣款 |
| GET | `/api/open/orders` | `pay` | 订单列表：`?page=1&size=20&status=&channel=&keyword=&from=&to=` |
| GET | `/api/open/stats` | `pay` | 交易统计 |

下单请求体支持 `app_id`（订单归属的应用）：

- 密钥**绑了应用** → 不传 `app_id` 也行，自动归到密钥绑的应用；传了别的 `app_id` → **400**；
- 密钥**没绑应用** → 可以传 `app_id` 显式指定；
- 账户开了「订单必须绑定应用」而密钥没绑 → **400**（不会静默落进「未分类」）。

调用示例：

```bash
TS=$(date +%s); NONCE=$(openssl rand -hex 8)
BODY='{"out_trade_no":"ORDER001","subject":"iPhone 17 Pro","amount":19900,"channel":"wechat"}'
TARGET='/api/open/pay'
CANONICAL="$(printf 'POST\n%s\n%s\n%s\n%s' "$TARGET" "$TS" "$NONCE" "$(printf '%s' "$BODY" | sha256sum | cut -d' ' -f1)")"
SIG=$(printf '%s' "$CANONICAL" | openssl dgst -sha256 -hmac 'sk_xxxx' | awk '{print $2}')

curl -X POST "http://127.0.0.1:8080/api/open/pay" \
  -H "X-Pay-Key: pk_3f2a..." -H "X-Pay-Timestamp: $TS" \
  -H "X-Pay-Nonce: $NONCE" -H "X-Pay-Signature: $SIG" \
  -H 'Content-Type: application/json' -d "$BODY"
```

失败时返回 `{"error":"..."}`，状态码与控制台接口一致
（`401` 鉴权失败、`403` 权限不足、`404` 订单不存在、`409` 商户单号重复、`429` 限流）。

#### 4. 限流

按密钥（600 次/分）与 IP（1200 次/分）各限一次。
限流是**进程内**的，多实例部署时每个实例各算各的 —— 需要全局配额请在网关层再做一层。

### 应用 / 产品维度接口

一个商户常常有多个产品（iOS App、安卓 App、小程序、H5……）。**每个产品建一个「应用」**，
订单就能按产品分开归类，统计和报表也拆得开 —— 否则所有产品的钱混在一起，
事后只能靠商户自己在 `out_trade_no` 里编产品号、人工拆报表。

> 可运行示例：[`../examples/multiapp`](../examples/multiapp)（建应用 → 建绑定应用的密钥 → 下单免传 app_id → 按应用分组统计）。

兼容性：**老商户一个应用都没有也照样能用**。`app_id` 为空 = 「未分类」，
渠道配置默认走全局那套（`pay_user_channels`），一行没动过。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/apps` | 应用列表（带每个应用的订单数与成功金额），另附「未分类」汇总 |
| POST | `/api/apps` | 新建应用 `{"name":"iOS 客户端","remark":"..."}`，**需二次验证票据** |
| POST | `/api/apps/{app_id}` | 修改应用；**需二次验证票据** |
| DELETE | `/api/apps/{app_id}` | 删除应用；**应用下还有订单时拒绝删除（409）**；强制绑定模式下**最后一个应用不许删（409）** |
| GET | `/api/apps/{app_id}/channels` | 该应用的渠道配置（**应用专属覆盖全局**的合并结果，标出哪些是单独配过的） |
| POST | `/api/channels/{channel}?app_id={app_id}` | 给该应用配**专属**渠道密钥（不带 `app_id` 就是改全局那套），**需二次验证票据** |
| DELETE | `/api/apps/{app_id}/channels/{channel}` | 删除该应用的专属配置（回退全局）；**该渠道下有订单时拒绝（409）** |
| POST | `/api/apps/require-binding` | `{"require":true}` 开关「订单必须绑定应用」，**需二次验证票据** |

> **四个渠道一视同仁**：微信、支付宝、Apple Pay、Google Pay 都支持应用专属配置。
> `{channel}` 可以是 `wechat` / `alipay` / `applepay` / `googlepay`，同一套规则 ——
> 应用专属覆盖全局、没配过的渠道回退全局、订单按归属应用取密钥。
> 钱包渠道的 Stripe 私钥 / Webhook 签名密钥也一样分应用各配各的。

#### 「订单必须绑定应用」强制模式

用户的原话：「反正就是必须指定应用，不然无法区分是哪些应用的支付。」

这是一个**账户级开关**（`pay_users.require_app_binding`），不是全局硬规则：

- **新建账户默认开启** —— 从今天起不再产生新的「未分类」订单；
- **老账户升级后仍然是关闭的** —— 升级不能打断正在跑的业务（老商户没建应用、密钥也没绑应用，
  一升级就全被拒等于事故）。有需要可以在「应用管理」页手动打开；
- 打开前必须**至少有一个应用**，否则开了等于自断手脚（400）；
- 强制模式下**删不掉最后一个应用**（409），避免账户把自己锁死。

打开后：

| 行为 | 结果 |
|------|------|
| 控制台 / 开放接口下单**不带** `app_id` | **400**：该账户已开启「订单必须绑定应用」…… |
| 开放接口用**没绑应用**的密钥下单 | **400**（不会静默落进「未分类」） |
| 创建**没绑应用**的密钥 | **400**（建密钥时必须选应用） |
| 密钥**绑了应用**、请求里**不传** `app_id` | 放行，订单自动归到密钥绑的那个应用 |
| 密钥绑了应用、请求里传了**别的** `app_id` | **400**（该密钥绑定应用 X，不能用于应用 Y） |
| 退款 / 查单 / 关单 | **不受影响** —— 这些按订单自身的应用走，本来就不需要再指定 |

> 强制绑定落在**商户服务端用的 API 密钥**上，不是让终端用户在自己手机上填应用 ID ——
> 那既是体验负担，也是安全漏洞（用户可以乱填）。

### 平台管理接口

给平台运营方用：看全平台数据、禁用违规商户。
需要 `X-Admin-Token` 请求头，值为 `Options.AdminToken`（留空则**整体不开放**）。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/admin/merchants` | 商户列表（名称、ID、是否启用二次验证、是否被禁用、创建时间） |
| GET | `/api/admin/stats` | 全平台统计：商户数、被禁用数、订单数、成交额、退款额 |
| POST | `/api/admin/merchants/{user_id}/status` | `{"disabled":true}` 启用 / 禁用商户 |

禁用一个商户之后：不能登录、已有会话令牌立即失效、开放接口密钥也用不了。

> `Options.AdminToken` **只**管 `/api/admin/*`，不再影响商户控制台 ——
> 早先的实现是「设了这个令牌，整个控制台都要带它」，结果商户自己的控制台反而谁都进不去。

---

## 四、回调地址设计

平台在下单时自动把 `notify_url` 指向：

```
https://pay.example.com/notify/{channel}/{user_id}
```

**为什么带 user_id**：微信回调里的商户单号是加密串，平台无法只凭回调内容判断订单属于哪个商户；
带上用户标识后，平台就能找到该商户的渠道配置完成验签（APIv3 密钥 + 平台证书），
再回写订单状态。

因此在微信 / 支付宝商户后台配置的「通知地址」直接填这个地址即可，无需自己写回调处理。

### 回调域名可以在控制台里改

`https://pay.example.com` 这一段（**回调前缀**）有两种来源，优先级从高到低：

1. **商户自己配的**：控制台「渠道配置 → 回调地址（对外访问地址）」里填，按商户存库
   （`pay_users.notify_base_url`），**改完立即生效，不用重启平台**；
2. **平台默认值**：启动参数 `-base`（没给就是 `http://127.0.0.1<addr>`）。

这样每个商户可以有自己的域名（把自己的域名反代到平台即可），
换域名时也不用去改命令行、重启进程。

> 填的域名必须能访问到本平台，否则微信 / 支付宝的回调没人接，订单会一直停在「待支付」。
> 控制台会在「填的域名 ≠ 当前平台地址」时给出提示。
>
> 商户填了自己的域名之后，下单时自带的 `notify_url` 也只允许用**这个域名**——
> 平台仍然要收到回调才能把订单置为已支付，不允许把回调引到别处。

接口：`POST /api/account/notify-base`，`{"notify_base_url":"https://pay.example.com"}`，
空字符串表示恢复平台默认。**需要二次验证票据**（这个设置决定「渠道把支付结果告诉谁」）。

---

## 五、数据表

| 表 | 说明 |
|----|------|
| `pay_users` | 商户（用户）：名称、**令牌散列**、口令散列、**对外访问地址（回调前缀，可空 = 用平台默认）**、二次验证方式/目标/开关、**TOTP 生效密钥与待确认密钥（密文）**、登录失败次数与锁定时间、**`require_app_binding`（是否强制订单绑定应用；老行为 0 = 不强制，新账户默认 1）** |
| `pay_user_channels` | 商户渠道配置：`enabled`、`is_default`、`cfg_enc`（**AES-256-GCM 密文**） |
| `pay_apps` | **应用（产品）**：所属商户、应用 ID、名称、备注。**同一商户下名称唯一**；`app_id` 为空 = 未分类（老订单、老商户都落这里） |
| `pay_app_channels` | **应用专属渠道配置**：应用 ID + 渠道 + `cfg_enc`（**AES-256-GCM 密文**）；没配过的渠道回退到全局那套 |
| `pay_orders` | 订单：渠道、支付方式、金额、状态、渠道流水号、回调原文、**`app_id`（订单归属的应用，空 = 未分类）**。**主键 `(user_id, out_trade_no)`** |
| `pay_refunds` | 退款流水：所属商户、订单号、退款单号、渠道、金额、状态、原因、时间 |
| `pay_mfa_codes` | 一次性验证码：只存带密钥散列、用途、有效期、使用标记、错误次数 |
| `pay_mfa_channels` | 验证码下发通道：`user_id`、`channel`（email/sms）、`cfg_enc`（**AES-256-GCM 密文**，AAD 单独） |
| `pay_verify_tokens` | 敏感操作票据：只存带密钥散列、所属用户、有效期 |
| `pay_audit_logs` | 审计日志：操作、对象、详情、来源 IP、时间 |
| `pay_api_keys` | 开放接口密钥：密钥 ID、所属商户、备注名、`secret_enc`（**AES-256-GCM 密文**）、权限列表、启用标记、过期与最近使用时间、**`app_id`（绑定的应用，空 = 不绑定）** |

表结构在启动时自动创建（GORM `AutoMigrate`），SQLite 与 MySQL 都可用；
新增的列会自动补齐（兼容旧库）。

> **订单主键迁移**：旧库的 `pay_orders` 主键是全局唯一的 `out_trade_no`，商户单号相当于全局资源，
> 恶意商户可以抢先占用别人的单号。平台启动时会把主键改成 `(user_id, out_trade_no)`
> （重建表，**数据不丢**），商户单号从此只在**本商户内**唯一。迁移是幂等的。

> `cfg_enc` 里是密文，不是原文。**主密钥丢了，数据就解不开了**，请务必备份
> `PAY_PLATFORM_MASTER_KEY`（或密钥文件）。

---

## 六、目录结构

```
payment/platform/
├── platform.go      平台主体：配置、Provider 缓存、路由、鉴权与安全响应头中间件
├── store.go         多商户数据存储（GORM）
├── gormmodels.go    数据模型（表名 / 列名与旧库一致）
├── db.go            数据库打开与方言判断（GORM 驱动）
├── migrate.go       订单主键迁移 + 老库明文令牌迁移
├── handlers.go      各 HTTP 接口处理
├── core.go          业务核心逻辑（控制台与开放接口**共用**同一份业务规则）
├── openapi.go       开放接口、API 密钥管理、平台管理接口
├── apikey.go        开放接口密钥：创建 / 吊销 / 解密取用、HMAC 签名与重放防护
├── apps.go          应用（产品）维度：应用 CRUD、应用专属渠道配置、合并生效配置
├── app_handlers.go  应用维度控制台接口（含「订单必须绑定应用」开关）
├── console.go       控制台/运维：订单导出 CSV、健康检查、版本信息
├── account.go       账户安全：口令、二次验证、敏感操作票据、登录验证码
├── security.go      口令散列、验证码、密钥脱敏与合并
├── hasher.go        令牌 / 验证码的带密钥散列（HMAC-SHA256）
├── ratelimit.go     限流器（登录 / 验证码下发 / 验证码校验）
├── secrets.go       渠道密钥静态加密（AES-256-GCM，AAD 可区分用途）+ 主密钥管理
├── mfa.go           二次验证下发通道（MFASender 接口 + 日志/HTTP 实现）
├── mfa_channel.go   账户级验证码下发通道（SMTP / 短信网关，配置加密入库）
├── mfa_handlers.go  TOTP 绑定准备、通道配置的增删查测接口
├── totp.go          TOTP（RFC 6238）动态口令：密钥生成、签发、校验、otpauth URI
├── web.go           前端产物分发（//go:embed all:web/dist + SPA 回退）
├── util.go          小工具（ID、JSON 响应）
├── web/dist/        **前端打包产物**（由 bus/payment-web 构建，embed 进二进制）
│   ├── index.html
│   └── assets/app-<hash>.js + style-<hash>.css
└── *_test.go        安全与多租户回归单测
```

> 可运行示例统一放在 `payment/examples/`：
> - `examples/platform/` —— 启动本平台（`go run ./payment/examples/platform`）
> - `examples/server/` —— 移动端接入服务端
> - `examples/basic/` —— 支付库用法演示


前端源码不在本目录，而在 `bus/payment-web/`（Vite + TypeScript 工程）。
构建产物落到 `payment/platform/web/dist/`，由 Go 端 `//go:embed` 打进二进制，
使用方拿到的是压缩后的 bundle 而不是明文模板。

### 前端构建

```bash
cd bus/payment-web
npm install
npm run build        # 产物写入 ../payment/platform/web/dist
```

或从 `bus/` 目录一次搞定（顺带做类型检查）：

```bash
cd bus
make build-frontend  # 类型检查 + 打包到 payment/platform/web/dist
make build           # gofmt + go vet + go test(payment 包)
```

前端尚未构建时，平台首页会返回一张「前端尚未构建」的提示页（HTTP 503），
后端 `/api/*` 接口不受影响，可直接调用。
