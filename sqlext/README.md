# sqlext —— SQL 解析浏览器插件

把 **bus/sqltree** 的 MySQL 解析能力做成一个浏览器插件：

- 别人在自己的网页里调用 `window.sqlext.parse(sql)`，就能拿到结构化的解析结果
- **解析逻辑在服务器端**（`sqlextd`），插件只做转发——解析能力随时更新，不用重新发版
- 插件自带注册 / 登录 / 充值页面，按余额提供解析能力

```
  网页里的页面脚本                    浏览器扩展                      你的服务器
  ────────────────                ───────────────                ──────────────
  window.sqlext.parse(sql)
        │  ① window.postMessage
        ▼
                          ② content script（隔离世界）
                             └─ chrome.runtime.sendMessage
                                  ▼
                              ③ service worker
                                 （持有令牌，唯一对外出口）
                                    │  ④ HTTPS + Bearer
                                    ▼
                                          ⑤ sqlextd
                                             ├─ 校验登录态、站点授权、余额
                                             ├─ 调 bus/sqltree 解析
                                             └─ 扣费 → 返回结果
```

## 目录

```
sqlext/
├── server/                    Go 服务端（sqlextd）
│   ├── cmd/sqlextd/main.go
│   └── internal/
│       ├── config/            环境变量配置
│       ├── store/             数据访问接口 + 内存实现
│       ├── auth/              口令散列、令牌
│       ├── billing/           支付渠道抽象（manual / 微信 / 支付宝 / Stripe）
│       └── server/            HTTP 路由与处理器
├── extension/                 Chrome 扩展（Manifest V3）
│   ├── manifest.json
│   ├── icons/
│   └── src/
│       ├── background/service-worker.js   后台：持令牌、调服务端、站点授权
│       ├── content/host.js                隔离世界：注入 + 转发
│       ├── content/page-bridge.js         主世界：暴露 window.sqlext
│       ├── lib/api.js                     服务端通信封装
│       └── pages/                         popup / login / register / recharge / settings / authorize
└── docs/
    ├── architecture.md        架构与安全设计
    └── market.md              市场调研：支付方案、商店政策、竞品
```

## 快速开始

### 1. 起服务端

```bash
cd bus
go run ./sqlext/server/cmd/sqlextd
```

默认监听 `:8080`，新用户赠送 20 积分，每次解析扣 1 积分。

```bash
# 注册
curl -X POST localhost:8080/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"a@b.com","password":"passw0rd123"}'

# 解析
curl -X POST localhost:8080/api/v1/parse \
  -H "Authorization: Bearer <token>" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, name FROM users WHERE status = 1"}'
```

### 2. 装插件

1. Chrome 打开 `chrome://extensions`
2. 右上角打开「开发者模式」
3. 「加载已解压的扩展程序」→ 选 `bus/sqlext/extension` 目录
4. 首次安装会打开设置页，把「解析服务地址」填成你的服务端地址（默认 `http://127.0.0.1:28081`）

### 3. 页面里调用

```js
// 页面的控制台里
const r = await window.sqlext.parse('SELECT id, name FROM users');
r.statement.details.from;          // "users"
r.statement.details.selected_exprs; // ["id", "name"]
r.balance;                          // 剩余积分
```

**前提**：这个网站必须先在插件里授权过。点插件图标 → 「授权当前网站」。
没授权的网站里 `window.sqlext` 根本不存在。

## HTTP API

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/v1/auth/register` | 注册，返回 token | 否 |
| POST | `/api/v1/auth/login` | 登录，返回 token | 否 |
| POST | `/api/v1/auth/logout` | 登出（吊销令牌） | 是 |
| GET | `/api/v1/auth/me` | 当前用户 + 余额 | 是 |
| GET | `/api/v1/billing/balance` | 余额 + 单价 | 是 |
| GET | `/api/v1/billing/channels` | 可用支付渠道 | 否 |
| POST | `/api/v1/billing/orders` | 创建充值订单 | 是 |
| GET | `/api/v1/billing/orders` | 订单列表 | 是 |
| GET | `/api/v1/billing/orders/{id}` | 单个订单 | 是 |
| GET | `/api/v1/sites` | 已授权网站 | 是 |
| POST | `/api/v1/parse` | **解析 SQL（扣费）** | 是 |

鉴权用 `Authorization: Bearer <token>`。

### 计费规则

- **先扣后算**：余额不足直接返回 `402 insufficient_balance`
- **解析失败退费**：SQL 语法错误不扣钱（返回 `400 parse_failed`）
- 每用户每分钟最多 60 次（`SQLEXT_RATE_LIMIT_PER_MIN`）

## 配置

服务端全部走环境变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SQLEXT_ADDR` | `:8080` | 监听地址 |
| `SQLEXT_BASE_URL` | `http://127.0.0.1:8080` | 对外地址 |
| `SQLEXT_TOKEN_TTL_HOURS` | `720` | 登录态有效期（30 天） |
| `SQLEXT_COST_PER_PARSE` | `1` | 每次解析扣多少积分 |
| `SQLEXT_FREE_QUOTA` | `20` | 新用户赠送积分 |
| `SQLEXT_SIGNUP_ENABLED` | `true` | 关掉就禁止注册 |
| `SQLEXT_RATE_LIMIT_PER_MIN` | `60` | 每用户每分钟调用上限 |
| `SQLEXT_CORS_ORIGINS` | 空 | 允许跨域的来源，逗号分隔 |
| `SQLEXT_PAYEE` | 空 | 收款方（人工转账渠道用） |
| `SQLEXT_PAY_NOTICE` | 空 | 支付说明 |

## 测试

```bash
cd bus/sqlext
make test       # 服务端 Go 测试（注册/登录/扣费/退费/站点授权/带注释的 SQL）
make test-ext   # 扩展桥接协议测试（Node，模拟三个世界）
make all        # lint + 全部测试
```

扩展的桥接协议测试用 Node 的 `vm` 模块搭了个最小模拟环境：
共享的 `window`（主世界 + 隔离世界）、假的 `chrome.runtime`、假的 service worker。
这是整个项目里最难在真浏览器外调试的部分，所以单独测。

## 目前的边界

- **存储是内存的**（`store.NewMemory()`），进程重启余额就没了。
  生产环境要实现 `store.Store` 接口换成 MySQL（bus 里已有 gorm 依赖）。
- **支付只有「人工转账」**（`manual` 渠道）：下单 → 用户转账 → 管理员确认到账。
  微信/支付宝/Stripe 的渠道接口留好了（`billing.Channel`），但没实现。
  选这条路的理由和后续怎么升级见 [市场调研 · 支付方案](docs/market.md#5-支付方案)——
  **注意：Chrome 商店不能收款，必须有自己的 Web 收银台，因此域名要 ICP 备案、
  支付主体要和备案主体一致。**
- **扩展只做了 Chrome/Edge**。Firefox 需要单独的 manifest（`background.scripts` 而不是 `service_worker`）。
- **没有做「用量统计」和「调用日志」**。
- **没做网页版**——但从市场调研看，网页版才是拉新入口，插件只是深度集成的加分项。

## 相关文档

- [架构与安全设计](docs/architecture.md) —— 为什么这么设计，尤其是"任意页面都能调用付费接口"这个坑
- [市场调研](docs/market.md) —— 支付方案对比、Chrome 商店政策风险、竞品
