package server

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// webHandler 返回托管落地页/隐私政策等静态文件的处理器。
//
// 为什么需要：Chrome Web Store 要求填一个**公网可访问**的隐私政策 URL，
// 商店后台的"主页""支持页面"也要有地址。最省事的做法就是让 sqlextd 自己托管。
//
// 目录约定（相对模块根目录）：
//
//	sqlext/web/index.html     → /            （落地页）
//	sqlext/web/privacy.html   → /privacy.html
//
// 目录不存在时返回 nil，调用方就不挂这个路由（本地开发不碍事）。
func webHandler(root string) http.Handler {
	if root == "" {
		return nil
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 解析成绝对路径，挡掉 ../ 之类的目录穿越
		rel := strings.TrimPrefix(filepath.Clean("/"+r.URL.Path), "/")
		target := filepath.Join(absRoot, rel)

		// 必须在 root 之内（filepath.Join 已经清掉了 ..，这里再确认一次）
		if target != absRoot && !strings.HasPrefix(target, absRoot+string(os.PathSeparator)) {
			http.NotFound(w, r)
			return
		}

		st, err := os.Stat(target)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		// 目录 → 找 index.html
		if st.IsDir() {
			target = filepath.Join(target, "index.html")
			if _, err := os.Stat(target); err != nil {
				http.NotFound(w, r)
				return
			}
		}

		// 用 ServeFile 而不是 FileServer：
		// FileServer 见到路径以 index.html 结尾会 301 到 "./"，
		// 而我们内部正是用 index.html 定位首页的。
		http.ServeFile(w, r, target)
	})
}
