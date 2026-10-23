#!/usr/bin/env bash
# package.sh —— 打出可以上传到 Chrome Web Store 的 zip。
#
# Chrome 商店要的是**扩展根目录**的 zip：
#   sqlext-0.1.0.zip
#     ├── manifest.json
#     ├── icons/
#     └── src/
# 解压后第一层必须直接是 manifest.json，不能多套一层目录。
#
# 用法：
#   ./package.sh                     # 用当前版本号
#   ./package.sh --api-base https://api.example.com
#
# 上传前请务必用 --api-base 指定正式的服务器地址：
# 商店版不能让人自己填地址（填了也没用，填错就白装），
# 扩展内置的地址必须是你的生产服务器。

set -euo pipefail

cd "$(dirname "$0")"

API_BASE=""
VERSION=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-base) API_BASE="$2"; shift 2 ;;
    --version)  VERSION="$2";  shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -f manifest.json ]]; then
  echo "找不到 manifest.json，请在 extension 目录下运行" >&2
  exit 1
fi

if [[ -z "$VERSION" ]]; then
  VERSION="$(grep -o '"version"[[:space:]]*:[[:space:]]*"[^"]*"' manifest.json | head -1 | sed 's/.*"\([^"]*\)"$/\1/')"
fi

if [[ -z "$API_BASE" ]]; then
  echo "警告：没有指定 --api-base，扩展会用内置的默认地址（127.0.0.1），"
  echo "      上传到商店后用户会连不上。商店版请务必指定："
  echo "      ./package.sh --api-base https://你的域名"
fi

OUT_DIR="../dist"
OUT_ZIP="$OUT_DIR/sqlext-$VERSION.zip"

rm -rf "$OUT_DIR/staging"
mkdir -p "$OUT_DIR/staging"

# 只拷发布需要的文件，测试和文档不进包
cp manifest.json "$OUT_DIR/staging/"
cp -r icons src "$OUT_DIR/staging/"

if [[ -n "$API_BASE" ]]; then
  # 把默认服务端地址替换成正式地址。
  # 用 sed 精确替换 api.js 里的 DEFAULT_API_BASE 常量。
  API_BASE_ESCAPED="$(printf '%s' "$API_BASE" | sed 's/[\/&|]/\\&/g')"
  sed -i "s|export const DEFAULT_API_BASE = '[^']*';|export const DEFAULT_API_BASE = '$API_BASE_ESCAPED';|" \
    "$OUT_DIR/staging/src/lib/api.js"
  echo "已把默认服务端地址设为 $API_BASE"
fi

# 包里不能有测试文件
rm -rf "$OUT_DIR/staging/test"

rm -f "$OUT_ZIP"
(cd "$OUT_DIR/staging" && zip -qr "../$(basename "$OUT_ZIP")" .)

echo
echo "打包完成：$OUT_ZIP"
echo
echo "上传步骤："
echo "  1. 打开 https://chrome.google.com/webstore/devconsole"
echo "  2. 「添加新商品」→ 上传这个 zip"
echo "  3. 填商店信息（见 store/listing.md）"
echo "  4. 填隐私政策 URL（sqlext/web/privacy.html，由 sqlextd 托管）"
echo "  5. 提交审核（首次审核通常 1–7 天）"
echo
echo "本地验证："
echo "  unzip -l $OUT_ZIP | head   # 确认第一层就是 manifest.json"
