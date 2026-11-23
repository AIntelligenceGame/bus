package platform

import (
	_ "embed"
	"net/http"
)

// 开放接口的真实代码示例。
//
// 页面上的「接入说明」如果只写接口路径,接入方还是得自己猜签名怎么拼、
// 金额单位是分还是元。这里直接把**仓库里那份真代码**发给页面:
// 改代码不用改页面,页面不会和代码对不上。
//
//go:embed openapi/client.go
var openapiClientSource string

//go:embed openapi/README.md
var openapiReadmeSource string

// openapiCurlSample curl 示例(不装 SDK 时用这个)。
const openapiCurlSample = `#!/bin/sh
# 开放接口调用示例(纯 shell,用 openssl 算签名,不需要装任何 SDK)
#
# 用法:
#   BASE=https://pay.example.com KEY=pk_xxx SECRET=sk_xxx sh openapi.sh
set -e

BASE="${BASE:-https://pay.example.com}"
KEY="${KEY:-pk_xxx}"
SECRET="${SECRET:-sk_xxx}"

BODY='{"channel":"wechat","trade_type":"app","subject":"示例商品","amount":19900}'
TARGET="/api/open/pay"
METHOD="POST"

TS=$(date +%s)
NONCE=$(openssl rand -hex 16)
BODY_SUM=$(printf '%s' "$BODY" | openssl dgst -sha256 -hex | sed 's/^.* //')
CANONICAL=$(printf '%s\n%s\n%s\n%s\n%s' "$METHOD" "$TARGET" "$TS" "$NONCE" "$BODY_SUM")
SIGNATURE=$(printf '%s' "$CANONICAL" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')

curl -sS -X POST "$BASE$TARGET" \
  -H "Content-Type: application/json" \
  -H "X-Pay-Key: $KEY" \
  -H "X-Pay-Timestamp: $TS" \
  -H "X-Pay-Nonce: $NONCE" \
  -H "X-Pay-Signature: $SIGNATURE" \
  -d "$BODY"
`

// handleOpenAPISample 开放接口的代码示例。
//
// GET /api/openapi/sample
//
// 返回 Go 客户端源码、接入说明与 curl 示例,供控制台页面展示。
// 只读文档,不需要 API 密钥,但仍然要求控制台会话(登录态)。
func (p *Platform) handleOpenAPISample(w http.ResponseWriter, r *http.Request) {
	if _, err := p.currentUser(r); err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"client": openapiClientSource,
		"readme": openapiReadmeSource,
		"curl":   openapiCurlSample,
	})
}
