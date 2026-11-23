package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSPAHandler 校验前端分发行为。
//
// 未构建时返回「前端尚未构建」提示页;已构建时返回 index.html。
func TestSPAHandler(t *testing.T) {
	p := &Platform{}
	rec := httptest.NewRecorder()
	p.handleSPA(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if spaAvailable() {
		if rec.Code != http.StatusOK {
			t.Fatalf("已构建时状态码 = %d, 期望 200", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "<html") {
			t.Fatalf("已构建时响应不是 HTML 页面")
		}
		return
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("未构建时状态码 = %d, 期望 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "前端尚未构建") {
		t.Fatalf("未构建时缺少提示文案")
	}
}

// TestSPAAssetsPath 校验静态资源路径不会被当成页面回退。
func TestSPAAssetsPath(t *testing.T) {
	p := &Platform{}
	rec := httptest.NewRecorder()
	p.handleSPA(rec, httptest.NewRequest(http.MethodGet, "/assets/app-nonexist.js", nil))
	// 必须精确是 404:之前只断言「不是 200」,理论上 500/503 也算通过。
	if rec.Code != http.StatusNotFound {
		t.Fatalf("不存在的静态资源应返回 404,实际 %d", rec.Code)
	}
	// 更要紧的是:**不能回退成 SPA 首页**。
	// 那样浏览器会把 JS 请求当成 HTML 解析,报一堆看不懂的错。
	if strings.Contains(rec.Body.String(), "<html") {
		t.Fatalf("不存在的静态资源不该回退成 SPA 首页: %s", rec.Body.String())
	}
}
