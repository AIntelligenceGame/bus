#!/usr/bin/env bash
# 前端构建脚本:安装依赖并打包到 ../payment/platform/web/dist(由 Go 端 embed 进二进制)。
#
#   cd bus/payment-web && ./build.sh
#
# 构建完成后 ../payment/platform/web/dist 下会出现:
#   index.html
#   assets/app-<hash>.js
#   assets/app-<hash>.css
#
# 平台会自动切换到打包后的前端,不再使用 templates/ 下的服务端模板。
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v npm >/dev/null 2>&1; then
  echo "找不到 npm,请先安装 Node.js (>= 18)" >&2
  exit 1
fi

echo "==> 安装依赖"
npm install --no-audit --no-fund

echo "==> 类型检查 + 打包"
# 旧产物的清理由 npm prebuild 钩子负责(scripts/clean-dist.mjs),
# 这样 `npm run build` 和 ./build.sh 的行为完全一致。
npm run build

echo "==> 构建完成:"
ls -l ../payment/platform/web/dist
