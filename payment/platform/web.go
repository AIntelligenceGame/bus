package platform

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// webDistFS 内嵌的前端构建产物(Vite + TypeScript 打包结果)。
//
// 产物目录:web/dist,由 bus/payment-web 执行 `npm run build` 生成。
// 未构建时目录里只有 .gitkeep,平台会返回提示页,提醒先构建前端。
//
//go:embed all:web/dist
var webDistFS embed.FS

// spaAvailable 判断前端产物是否已构建。
func spaAvailable() bool {
	_, err := fs.Stat(webDistFS, "web/dist/index.html")
	return err == nil
}

// handleSPA 提供前端 SPA。
//
//   - "/" 及各页面路径  返回 index.html(前端自己做 hash 路由);
//   - "/assets/..."      返回带内容哈希的静态资源(可长期缓存)。
func (p *Platform) handleSPA(w http.ResponseWriter, r *http.Request) {
	if !spaAvailable() {
		p.handleNotBuilt(w, r)
		return
	}
	sub, err := fs.Sub(webDistFS, "web/dist")
	if err != nil {
		p.handleNotBuilt(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" || path == "index.html" || !strings.HasPrefix(path, "assets/") {
		data, err := fs.ReadFile(sub, "index.html")
		if err != nil {
			p.handleNotBuilt(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// index.html 不缓存,静态资源带哈希可长期缓存
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(data)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	http.FileServer(http.FS(sub)).ServeHTTP(w, r)
}

// handleNotBuilt 前端尚未构建时的提示页。
func (p *Platform) handleNotBuilt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>前端尚未构建</title>
<style>
 body{margin:0;font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:#f5f6f8;color:#222}
 .card{max-width:640px;margin:80px auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
 code{background:#eef2ff;padding:2px 6px;border-radius:4px;font-size:13px}
 pre{background:#0f172a;color:#e2e8f0;padding:12px;border-radius:6px;overflow:auto;font-size:12px}
</style></head><body>
<div class="card">
<h2>前端尚未构建</h2>
<p>控制台前端是 Vite + TypeScript 工程,需要先打包,再由 Go 端 embed 进二进制。</p>
<pre>cd bus/payment-web
npm install
npm run build</pre>
<p>产物会写入 <code>bus/payment/platform/web/dist/</code>,重新编译 Go 即可。</p>
<p>后端接口不受影响,可直接调用 <code>/api/*</code>。</p>
</div></body></html>`))
}
