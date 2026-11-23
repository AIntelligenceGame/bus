package platform

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
)

// 这组测试盯的是「前端静态资源」这条链路:
// 压缩中间件夹在中间,不能把 go:embed 的静态资源发坏。
//
// 风险点:
//
//  1. http.FileServer 会自己设 Content-Length(按**未压缩**字节算),
//     压缩后必须把这个头删掉,否则浏览器会按旧长度读 → 页面加载到一半卡住。
//  2. Range 请求返回 206,Content-Range 是按未压缩字节算的,
//     压了范围就错位 → 206 一律不压。
//  3. 前端没构建时(只有 .gitkeep)要跳过,不能因为没产物就红。

// biggestAsset 找出内嵌产物里最大的一个静态资源,拿它当压缩测试样本。
func biggestAsset(t *testing.T) (string, []byte) {
	t.Helper()
	if !spaAvailable() {
		t.Skip("前端尚未构建,跳过静态资源压缩测试")
	}
	sub, err := fs.Sub(webDistFS, "web/dist")
	if err != nil {
		t.Fatalf("取子目录失败: %v", err)
	}
	var (
		bestPath string
		bestSize int64
	)
	_ = fs.WalkDir(sub, "assets", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if info.Size() > bestSize {
			bestSize = info.Size()
			bestPath = path
		}
		return nil
	})
	if bestPath == "" {
		t.Skip("内嵌产物里没有静态资源,跳过")
	}
	data, err := fs.ReadFile(sub, bestPath)
	if err != nil {
		t.Fatalf("读静态资源失败: %v", err)
	}
	return bestPath, data
}

// TestSPAAssetIsCompressed 静态资源必须被压缩,且解压后与原文件逐字节一致。
func TestSPAAssetIsCompressed(t *testing.T) {
	path, want := biggestAsset(t)
	p, _ := newTrafficPlatform(t)

	req := httptest.NewRequest(http.MethodGet, "/"+path, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("静态资源返回状态码不对: %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("大资源没有被压缩: Content-Encoding = %q", got)
	}
	// 关键:压缩后不能再带 Content-Length(那是压缩前的长度)。
	if cl := rec.Header().Get("Content-Length"); cl != "" {
		t.Fatalf("压缩后不该还带 Content-Length: %q", cl)
	}
	// 共享缓存必须能区分压缩/不压缩两种形态。
	if !headerContains(rec.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("缺 Vary: Accept-Encoding,共享缓存会把 gzip 版发给不支持的客户端: %q",
			rec.Header().Get("Vary"))
	}

	// 压缩后的大小要在读之前记下来(读 gzip 流会消耗掉 body)。
	compressedSize := rec.Body.Len()
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("解压失败: %v", err)
	}
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("读解压内容失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("解压后内容与原文件不一致: %d vs %d 字节", len(got), len(want))
	}
	t.Logf("%s: %d 字节 -> 压缩后 %d 字节", path, len(want), compressedSize)
}

// TestSPAAssetRangeNotCompressed Range 请求(206)不能压缩。
//
// Content-Range 是按未压缩字节算的;压了之后客户端截出来的字节全是乱的。
func TestSPAAssetRangeNotCompressed(t *testing.T) {
	path, want := biggestAsset(t)
	p, _ := newTrafficPlatform(t)

	req := httptest.NewRequest(http.MethodGet, "/"+path, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Range", "bytes=0-99")
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("Range 请求应该返回 206,实际 %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("206 响应不能压缩: Content-Encoding = %q", got)
	}
	if cr := rec.Header().Get("Content-Range"); cr == "" {
		t.Fatal("206 响应必须有 Content-Range")
	}
	if rec.Body.Len() != 100 {
		t.Fatalf("Range 响应体长度不对: %d", rec.Body.Len())
	}
	if !bytes.Equal(rec.Body.Bytes(), want[:100]) {
		t.Fatal("Range 响应体内容不对")
	}
}

// TestSPAIndexHTML index.html 要能正常返回且不缓存。
func TestSPAIndexHTML(t *testing.T) {
	if !spaAvailable() {
		t.Skip("前端尚未构建,跳过")
	}
	p, _ := newTrafficPlatform(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("首页返回状态码不对: %d", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("首页不该被缓存: Cache-Control = %q", cc)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("<html")) {
		t.Fatal("首页返回的不是 HTML")
	}
}

// headerContains 判断逗号分隔的头值里是否包含某一项。
func headerContains(value, want string) bool {
	for _, part := range bytes.Split([]byte(value), []byte(",")) {
		if string(bytes.TrimSpace(part)) == want {
			return true
		}
	}
	return false
}
