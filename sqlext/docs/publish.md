# 上架流程

> 一句话回答"怎么让浏览器能搜到这个插件"：
> **发到 Chrome Web Store**。本地「加载已解压的扩展程序」只有自己能装，别人搜不到。

---

## 一、先搞清楚两种分发方式

| 方式 | 别人能装吗 | 别人能搜到吗 | 门槛 |
|---|---|---|---|
| 开发者模式加载已解压 | ❌ 只有你自己 | ❌ | 无 |
| **Chrome Web Store** | ✅ | ✅ | 开发者账号 $5 一次性 + 过审 |
| Edge 加载项 | ✅ | ✅ | 与 Chrome 同账号体系，审核较松 |
| Firefox 附加组件 | ✅ | ✅ | 要单独写 manifest（`background.scripts`） |
| 自建 CRX / 企业策略 | ✅（指定人群） | ❌ | 无 |

**先做 Chrome Web Store**，同时网页版（`web/index.html`）承担拉新——
市场调研的结论是：纯插件靠商店自然流量很难起量（同类插件安装量只有几十个），
必须有网页导流。

---

## 二、上架 Chrome Web Store

### 步骤 1：注册开发者账号

1. 打开 https://chrome.google.com/webstore/devconsole
2. 用 Google 账号登录
3. 一次性支付 **$5** 开发者注册费（Google 收款，支持信用卡）
4. 完成身份验证（需要公开的开发者联系方式——商店页会展示你的邮箱）

> 注意：这里只解决**分发**。Chrome Web Store 的内置支付（商店内收款/订阅）
> 在 2020/2021 年已被彻底废弃，**不能靠商店收钱**。要收银台必须自己有 Web 收银台，
> 详见 [market.md · 支付方案](market.md#5-支付方案)。

### 步骤 2：打包

```bash
cd bus/sqlext
make pack API_BASE=https://api.你的域名
```

产物在 `dist/sqlext-<版本>.zip`。

**`--api-base` 必须指定正式服务器地址**：商店版不能让用户自己填地址
（填错就白装），扩展内置的地址要指向你的生产服务器。

包的**第一层必须直接是 `manifest.json`**（不能多套一层目录），
`package.sh` 已经处理好了。校验：

```bash
unzip -l dist/sqlext-0.1.0.zip | head
# 第一行应该就是 manifest.json
```

### 步骤 3：填商店信息

后台「添加新商品」→ 上传 zip → 填各字段。

**每个字段怎么填、权限理由怎么写、审核员会问什么**，全在
[`extension/store/listing.md`](../extension/store/listing.md)。这里只列最容易踩的：

| 坑 | 说明 |
|---|---|
| 名称别起跟 SQL 无关的名字 | 商店搜索靠名称+说明+关键词匹配 |
| 权限理由要逐条写实 | 「读取和更改您在所有网站上的数据」这种宽泛权限必须解释清楚 |
| 隐私问卷不能漏填 | 审核会核对问卷与实际行为 |
| 隐私政策要有公网 URL | 见下一步 |
| 截图至少 1280×800 | 后台不接受小图 |

### 步骤 4：部署隐私政策页面

商店强制要求隐私政策有**公网可访问**的 URL。`sqlextd` 直接托管：

```bash
SQLEXT_WEB_DIR=sqlext/web SQLEXT_ADDR=:8080 go run ./sqlext/server/cmd/sqlextd
```

| 页面 | 地址 |
|---|---|
| 隐私政策 | `https://你的域名/privacy.html` |
| 落地页（商店"主页"） | `https://你的域名/` |
| 支持页面 | `https://你的域名/` |

公网部署需要域名 + ICP 备案（因为要接支付，见 market.md）。

### 步骤 5：提交审核

首次审核通常 **1–7 个工作日**。审核员重点看：

1. **单一用途** —— 本扩展就是"把 SQL 解析成结构化数据"，账号体系都是为它服务的
2. **远程代码** —— 服务端返回的是 JSON 数据不是可执行代码，符合 MV3 允许的
   "与远程服务器通信"场景
3. **权限最小化** —— 没有用 `<all_urls>`，内容脚本只动态注册在用户逐个授权的站点上
4. **数据采集** —— 只收 SQL、邮箱、订单；问卷如实填写

被拒了看 [listing.md · 审核不过的退路](../extension/store/listing.md#7-审核不通过的退路)。

### 步骤 6：上架后

- 更新：改 `manifest.json` 的 `version` → `make pack` → 重新上传，商店自动推给用户
- 看数据：开发者后台有安装量、评分、错误报告

---

## 三、自建分发（商店过不了审时的退路）

### 打包 CRX

```bash
cd /tmp && cp -r bus/sqlext/extension myext
google-chrome --pack-extension=/tmp/myext --no-message-box
# 生成 myext.crx 和 myext.pem
```

`myext.pem` 是扩展的签名私钥，**必须保管好**——丢了就没法给这个扩展发更新了。

### 自动更新

自建分发要自己实现更新检查。Chrome 支持 `update_url`：

```json
{
  "update_url": "https://你的域名/update.xml"
}
```

服务端的 `update.xml`：

```xml
<?xml version='1.0' encoding='UTF-8'?>
<gupdate xmlns='http://www.google.com/update2/response' protocol='2.0'>
  <app appid='你的扩展ID'>
    <updatecheck version='0.2.0' url='https://你的域名/sqlext-0.2.0.crx' />
  </app>
</gupdate>
```

分发渠道：企业策略推送（Windows 组策略 / macOS MDM）、
CRX 文件直接发、或者放进公司内部软件分发平台。

---

## 四、上架前的检查清单

```
□ make lint && make test && make test-ext      代码检查 + 测试全绿
□ make pack API_BASE=https://正式域名            打包
□ unzip -l dist/*.zip | head                   第一层是 manifest.json
□ 本地装一遍：chrome://extensions → 加载已解压 → 走通 授权网站 → 网页调 parse
□ 服务端已部署到公网，https 可访问
□ 隐私政策 URL 能打开（无痕窗口试一次，确认没被拦）
□ 落地页能打开
□ 商店后台：名称 / 简短说明 / 类别 / 详细说明
□ 商店后台：每个权限的理由
□ 商店后台：隐私实践问卷如实填写
□ 商店后台：截图 ≥1280×800
□ 商店后台：主页 / 支持页面 / 隐私政策 URL
□ 提交审核
```

---

## 五、别忘了这些

- **邮箱换掉**：隐私政策和落地页里的 `support@example.com` / `example.com` 是占位符，
  发布前必须替换成真实地址。
- **支付主体**：充值走自己的 Web 收银台，域名要 ICP 备案、支付主体要和备案主体一致。
  这个没搞定之前，别开放注册。
- **服务端是内存存储**：进程重启余额就没。开放注册前先接上 MySQL
  （`store.Store` 接口已经留好了）。
