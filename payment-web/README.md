# payment-web — 支付平台前端

支付配置选择平台的前端工程,Vite 5 + TypeScript 5,**零运行时依赖**。

## 为什么要独立成一个前端工程

前端源码(`src/`)与后端(`bus/payment/`)分离:

- 打包产物输出到 `../payment/platform/web/dist`,由 Go 端 `//go:embed` 打进二进制;
- 使用方拿到的是压缩后的 bundle,不会看到一份明文的模板 + 内联脚本;
- 前端可以独立 `npm run dev` 联调,不需要重编 Go。

## 目录

```
payment-web/
├── index.html          # Vite 入口
├── package.json
├── tsconfig.json       # strict + verbatimModuleSyntax
├── vite.config.ts      # outDir 指向 ../payment/platform/web/dist
├── build.sh            # 一键安装依赖 + 打包
└── src/
    ├── main.ts         # 路由 + 应用骨架(侧边栏 / 顶栏 / 内容区)
    ├── styles.css      # 设计系统(token + 组件类)
    ├── lib/            # api / dom / store / ui / verify / types
    └── pages/          # home / playground / orders / channels / keys / security
```

## 界面结构

**登录后**是「左侧边栏 + 顶栏 + 内容区」的后台布局：

- 侧边栏按 **概览 / 交易 / 配置 / 账户** 分组，底部显示当前商户；
- 顶栏显示当前页面标题与说明，右侧是退出登录；
- 内容区由各个页面自行渲染。

**未登录**时是独立的居中登录页（品牌区 + 登录表单），不与后台布局混用。

### 登录表单是自适应的

登录页默认只显示「商户名称 + 登录口令」，**验证码框和注册码框默认不出现**：

| 输入框 | 什么时候出现 | 依据 |
|---|---|---|
| 注册码 | 平台设置了注册码时 | 公开接口 `GET /api/version` 返回 `registration_code_required` |
| 验证码 | 该账户开了二次验证时 | 登录接口返回 `mfa_required=true`（口令已对，只差验证码） |

没开二次验证的账户不会看到一个「填了也没用」的验证码框。
服务端在缺验证码时**不计入登录失败次数**，所以第一次忘了填验证码不会被锁账户。

## 设计系统

`src/styles.css` 里所有颜色、间距、圆角、阴影都是 CSS 变量（token），
页面里不写死值，并且跟随系统的深色模式自动切换（`prefers-color-scheme`）。

常用类：

| 类 | 用途 |
|---|---|
| `.shell` / `.sidebar` / `.topbar` / `.content` | 应用骨架 |
| `.card` / `.card-head` / `.section-title` | 区块卡片 |
| `.btn` `.btn-primary` `.btn-secondary` `.btn-ghost` `.btn-danger` `.btn-sm` | 按钮（`.is-busy` 为 loading 态） |
| `.field` `.label` `.hint-text` `.error-text` `.checkbox` `.switch-row` | 表单 |
| `.table-wrap` / `table` / `.badge-*` | 表格与状态徽章 |
| `.stat-grid` / `.stat` | 统计卡 |
| `.empty` | 空状态 |
| `.notice` `.notice-info/warn/danger/success` | 提示块 |
| `.toast-host` / `.toast` | 全局轻提示 |
| `.modal-mask` / `.modal` | 弹层 |

## 通用组件（`src/lib/ui.ts`）

| 函数 | 作用 |
|---|---|
| `toast(msg, kind)` | 右上角轻提示（成功 / 失败 / 信息） |
| `withBusy(btn, fn)` | 按钮进入 loading 态并**禁止重复点击**，执行完自动恢复 |
| `confirmDialog(title, desc, danger)` | 危险操作二次确认，返回 `Promise<boolean>` |
| `amountDialog({maxCents})` | 金额输入弹窗，界面用「元」、内部用「分」，并校验上限 |
| `openModal({title, body, onOk})` | 通用弹层（ESC / 点遮罩关闭，错误显示在弹层内） |
| `emptyState(title, desc, action)` | 空状态（带引导按钮） |
| `card(title, desc, actions, ...children)` | 区块卡片 |
| `stat(label, value, unit, accent)` | 统计卡 |
| `field(opts)` | 带标签 / 必填星号 / 辅助说明 / 错误位的输入框 |
| `switchRow(id, title, desc, checked)` | 开关行 |

> **约定**：所有提交类按钮都必须走 `withBusy`——支付、退款这类操作允许连点会重复下单。
> 危险操作（吊销密钥、关闭二次验证、退款）必须先过 `confirmDialog`。

### 控件按真实状态显示

**凡是「点了必然报错」的控件一律不出现**：

| 控件 | 什么时候出现 |
|---|---|
| 登录页「验证码」 | 该账户开了二次验证时（服务端回 `mfa_required`） |
| 登录页「重新获取验证码」 | 二次验证方式是邮箱 / 短信时；**验证器 App(TOTP) 没有这一栏**——动态码由 App 本地算出 |
| 登录页「二次验证无效」提示 | 服务端回 `mfa_skipped=true` 时（绑定在、但验证码发不出去，本次登录跳过了） |
| 登录页「注册码」 | 平台设置了注册码时（`/api/version` 的 `registration_code_required`） |
| 验证弹窗「验证码」 | 该账户开了二次验证时；TOTP 时标签改成「验证器 App 动态码」且不显示「获取验证码」；**通道不可用时整块隐藏**（那个码收不到，只要口令） |
| 二次验证页「关闭」区 | 只在已开启二次验证时出现；没开时给「关闭」按钮纯属摆设 |
| 二次验证页「接收地址 / 发送绑定验证码」 | 验证方式是邮箱 / 短信时；TOTP 分支改为「生成密钥 + 动态码」 |
| 二次验证页「开启二次验证」按钮 | 选了具体方式才可用；保持默认的「关闭」时禁用并提示「请先选择验证方式」 |
| 二次验证页「关闭验证码」输入框 | 绑定不可下发时禁用并提示「通道不可用，关闭时只需口令」 |
| 渠道卡片「启用 / 设为默认」 | 就在该渠道自己的卡片里，跟着该渠道的保存按钮一起生效 |
| 订单行「关闭订单」 | 只有 `PENDING` 的订单 |
| 支付体验「交易类型」 | 按所选渠道过滤（支付宝没有 jsapi/h5，钱包只有 wallet） |

### 二次验证的三种方式

**默认是「关闭」**：验证方式下拉的第一项是「关闭(不启用二次验证)」，账户没开二次验证时就选它。
不是每个商户都开通了邮箱、短信或验证器 App,平台不能默认替他们挑一种,更不能把
「三种都没有」变成一条走不通的死路。想开就明确选一种。

| 方式 | 绑定流程 | 登录时 |
|---|---|---|
| 邮箱 / 短信 | 填接收地址 → 「发送绑定验证码」→ 填码 → 「开启 / 保存新绑定」 | 输入收到的 6 位码 |
| 验证器 App(TOTP) | 「生成密钥」→ 扫码 → 填 App 里的动态码 → 「开启 / 保存新绑定」 | 输入 App 里的动态码 |

- **选「关闭」时不显示任何绑定表单**，「开启二次验证」按钮禁用并显示「请先选择验证方式」。
- **TOTP 二维码是自动出的**：选到「验证器 App」就自动生成密钥并把二维码画出来，
  不用先找「生成密钥」按钮。图片在**加载成功之前不显示**（`<img>` 没有 `src` 时浏览器会画一个
  空白方框，看起来就像「二维码不显示」）；图片加载失败时**密钥照样给出来**（带「复制密钥」），
  手动输入与扫码完全等价。
- **邮箱 / 短信没配下发通道时**,绑定区顶部会挂一条黄色警告:码只会写进服务器日志、用户实际收不到,
  并提示去下方配 SMTP/短信网关、或者改用验证器 App。
- **后端也会拦一道**:没有可用下发通道时,`POST /api/account/mfa/enable` 直接返回 400
  （「尚未配置邮件下发通道……等于把账户锁在门外」）。开启之后登录就要这个码,
  收不到就等于把自己锁在门外,这种绑定不该被允许。
- **换方式不用先关**：已开启时同一个卡片就是「更换接收方式或地址」，后端是覆盖绑定。
- **TOTP 密钥由服务端生成**，前端只展示二维码与密钥文本；启用时后端拿**自己暂存的那份**校验动态码，
  不信前端回传的密钥。
- **关闭流程**另有一套输入框：邮箱 / 短信要「发送关闭验证码」，TOTP 直接填当前动态码。

### 验证码下发通道（页面上配什么）

平台**不内置**任何邮件 / 短信厂商 —— 通道参数在「账户安全 → 验证码下发通道」卡片里自己填，
**加密入库**、授权码与令牌**永不回显**。卡片顶部直接写清了要填什么、以及配置顺序：

> 配好通道 → 点「发送测试」确认真的收到 → 再去上面开启二次验证。

| 通道 | 页面上的字段 | 说明 |
|---|---|---|
| 邮件 | SMTP 服务器 / 端口 | QQ：`smtp.qq.com` + 587；163：`smtp.163.com` + 465（有「填入 QQ / 163 邮箱参数」按钮一键填） |
| 邮件 | 加密方式 | `STARTTLS(587)` / `隐式 TLS(465)` / `不加密`；留空按端口推断 |
| 邮件 | 登录账号 / 授权码 | 授权码不是登录密码（在邮箱服务商后台生成），加密入库 |
| 邮件 | 发件人地址 | 留空则用登录账号 |
| 邮件 | 测试收件地址 | 留空则发到登录账号 |
| 短信 | 网关地址 / 网关令牌 | 平台会 POST `{"code":"123456","target":"138…"}`；令牌可选 |
| 短信 | 验证码字段名 / 接收地址字段名 | 默认 `code` / `target`，按厂商改 |
| 短信 | 测试手机号 | 留空则发到已绑定手机号 |

- **「发送测试邮件 / 发送测试短信」是真的发出去**：SMTP 会真的连服务器，短信会真的 POST 网关。
  网关回 2xx 但 body 里带错误码（`{"code":1001,"msg":"余额不足"}`）时后端会如实报错，
  页面不会显示「发送成功」而短信根本没发出去。
- 没配通道时平台只能把码写进日志，这时接口回 `delivered=false`，页面会**如实提示用户收不到**。
- **通道不可用的绑定（`mfa_deliverable=false`）在页面上显示「已开启但无效」**，
  并提示登录时会被跳过；此时「关闭二次验证」与身份校验都只要口令（那个码用户收不到，硬要就是死锁）。

> 输入框取值一律走 `val(id)` / `setVal(id, v)`（按 id 从 DOM 取）。
> **不要**在页面里另外 new 一个同 id 的 input —— 那个元素不进 DOM，读它永远是空值，
> 结果就是「界面上能填、后端收不到」。


## 构建

```bash
cd bus/payment-web
./build.sh          # 等价于 npm install && npm run build
```

或在 `bus/` 目录下：

```bash
make build-frontend   # 类型检查(npx tsc --noEmit) + vite build,产物落到 embed 目录
make build            # gofmt + go vet + go test ./payment/...
```

产物:

```
bus/payment/platform/web/dist/
├── index.html
└── assets/
    ├── app-<hash>.js
    └── style-<hash>.css
```

`bus/payment/platform/web/dist` 需要随仓库提交(Go 的 `//go:embed` 在编译期读取它),
`node_modules/` 与本目录的临时产物已在 `bus/.gitignore` 中忽略。

## 开发联调

```bash
# 终端 1:起后端(支付配置选择平台)
cd bus/payment && go run ./examples/platform

# 终端 2:起前端
cd bus/payment-web && npm run dev
```

Vite dev server 已配置代理:`/api` 与 `/notify` 转发到 `http://127.0.0.1:8080`。

## 路由

前端使用 hash 路由(挂到任意前缀都能工作):

| Hash | 页面 |
| --- | --- |
| `#/` | 概览 / 登录 |
| `#/channels` | 渠道配置 |
| `#/playground` | 支付体验 |
| `#/orders` | 订单 |
| `#/apps` | 应用管理（多产品各配各的渠道密钥 + 「订单必须绑定应用」开关） |
| `#/reports` | 报表（按应用 / 渠道 / 天分组） |
| `#/security` | 账户安全 |
| `#/keys` | 开放接口（API 密钥管理与签名说明，建密钥时可绑定应用） |

## 与后端的接口约定

前端全部通过 `bus/payment/platform` 的 JSON 接口通信,鉴权方式:

- `X-User-Token`:用户令牌(登录后拿到,存 localStorage);
- `X-Verify-Token`:敏感操作票据(口令 + 验证码换取,默认 10 分钟有效)。

敏感操作(改渠道配置 / 查看密钥原文 / 发起支付 / 退款)都会先弹验证框,
由 `src/lib/verify.ts` 统一处理。
