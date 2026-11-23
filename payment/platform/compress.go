package platform

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"strings"
	"sync"
)

// 响应压缩(gzip)。
//
// 为什么必须做:
//
//	订单列表 200 条 = 176 KB JSON,里面绝大部分是重复的字段名和渠道原始报文。
//	浏览器每次翻页都要完整下载一遍,商户在弱网/大列表下会明显感觉到「卡」。
//	实测 gzip 之后同一份数据只有 4.3 KB(2.4%)—— 流量降一个数量级。
//
// 三个约束:
//
//  1. **只压文本**。PNG/JPEG 这类已经压缩过的格式再压一遍是纯浪费 CPU。
//  2. **小响应不压**。{"ok":true} 这种几十字节的响应压了反而变大(还多两次 CPU),
//     所以先攒够一个阈值再决定要不要压。
//  3. **不能破坏流式响应**。导出 CSV 是边查边写的,必须支持「攒到阈值后
//     立刻切换成压缩流」,而不是等整个响应写完。
//
// 关键实现细节:**状态行和响应头必须等「决定压不压」之后才能发出去**。
// 因为 Content-Encoding / Content-Length 只有在写头那一刻才能定,
// 先发头再改是来不及的(头已经写到网卡上了)。
// 所以这里把 WriteHeader 拦下来,等攒够阈值(或响应结束)再真正发出去。

// gzipMinSize 触发压缩的最小响应体字节数。
//
// 小于这个大小直接原样返回:压缩头本身就要 18 字节,压不划算。
const gzipMinSize = 1024

// gzipCompressible 判断 Content-Type 是否值得压缩。
func gzipCompressible(contentType string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if ct == "" {
		// 没设 Content-Type 的响应不压:可能是二进制流(比如导出附件),
		// 压坏了反而出事。
		return false
	}
	// 已经压缩过的格式:压了也压不动,还白费 CPU。
	switch {
	case strings.HasPrefix(ct, "image/"),
		strings.HasPrefix(ct, "video/"),
		strings.HasPrefix(ct, "audio/"),
		strings.HasPrefix(ct, "application/zip"),
		strings.HasPrefix(ct, "application/gzip"),
		strings.HasPrefix(ct, "application/x-gzip"),
		strings.HasPrefix(ct, "application/pdf"):
		return false
	}
	// 文本类:JSON / JS / CSS / HTML / CSV / XML / 纯文本。
	switch {
	case strings.HasPrefix(ct, "text/"),
		strings.Contains(ct, "json"),
		strings.Contains(ct, "javascript"),
		strings.Contains(ct, "ecmascript"),
		strings.Contains(ct, "xml"),
		strings.Contains(ct, "csv"),
		strings.Contains(ct, "svg"):
		return true
	}
	return false
}

// gzipResponseWriter 是带缓冲的压缩响应写入器。
//
// 生命周期:
//
//	WriteHeader → 只记状态,不发出去(等决定要不要压)
//	Write       → 先攒进缓冲区;攒够 gzipMinSize 就决定并切换
//	Close       → 还没决定的话,此刻决定(小响应走「不压」),然后收尾
type gzipResponseWriter struct {
	http.ResponseWriter

	mu     sync.Mutex
	buf    bytes.Buffer
	gz     *gzip.Writer
	status int
	// headerWritten 表示状态行与响应头已经真的发出去了。
	headerWritten bool
	// decided 表示「压不压」已经定了。
	decided bool
	// compress 表示最终决定压缩。
	compress bool
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.headerWritten {
		return
	}
	w.status = status
	// 206 Partial Content / 304 Not Modified 不能压:
	//   - 206 的 Content-Range 是按**未压缩**字节算的,压了范围就错;
	//   - 304 没有响应体,压了反而可能带上正文。
	// 这两种状态直接定案「不压」。
	if status == http.StatusPartialContent || status == http.StatusNotModified {
		w.decided = true
		w.compress = false
		w.flushHeader()
	}
	// 其他状态:等 Write 攒够阈值再定(或者等 Close)。
}

func (w *gzipResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.decided {
		return w.writeDecided(p)
	}
	// 还没决定:先攒着。
	w.buf.Write(p)
	if w.buf.Len() < gzipMinSize {
		return len(p), nil
	}
	// 到阈值了:现在决定,并把攒的内容送出去。
	w.decide(false)
	return len(p), nil
}

// decide 决定压不压,并把头发出去。
//
// force=true 表示「调用方要求现在就定案」(Flush:流式响应不能再攒了),
// 这时即使内容还不到阈值也压 —— 因为流式响应一旦开始就不能反悔。
//
// 缓冲区里的内容在这一步被一次性送走(压 → 写进 gzip 流;不压 → 原样写)。
func (w *gzipResponseWriter) decide(force bool) {
	if w.decided {
		return
	}
	w.decided = true
	w.compress = gzipCompressible(w.Header().Get("Content-Type")) &&
		(force || w.buf.Len() >= gzipMinSize)
	if w.compress {
		h := w.Header()
		// Content-Length 是**压缩前**的长度,压了之后对不上,必须去掉。
		// 不去掉的话客户端会按旧长度读,多出来的字节被当成下一个响应的头 → 协议错乱。
		h.Del("Content-Length")
		// Accept-Ranges 是按**未压缩**字节算的;压过的响应不能宣称支持范围请求。
		h.Del("Accept-Ranges")
		h.Set("Content-Encoding", "gzip")
		gz, err := gzip.NewWriterLevel(w.ResponseWriter, gzip.BestSpeed)
		if err != nil {
			// 压缩器起不来(不应该发生):退回原样输出,不能把响应写坏。
			w.compress = false
			h.Del("Content-Encoding")
		} else {
			w.gz = gz
		}
	}
	w.flushHeader()
	pending := w.buf.Bytes()
	w.buf.Reset()
	if len(pending) == 0 {
		return
	}
	if w.gz != nil {
		_, _ = w.gz.Write(pending)
		return
	}
	_, _ = w.ResponseWriter.Write(pending)
}

// flushHeader 真正把状态行与响应头发出去。
func (w *gzipResponseWriter) flushHeader() {
	if w.headerWritten {
		return
	}
	w.headerWritten = true
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(status)
}

// writeDecided 已经定案之后的写入。
func (w *gzipResponseWriter) writeDecided(p []byte) (int, error) {
	if w.gz != nil {
		return w.gz.Write(p)
	}
	// 不压:把之前攒着的补写出去(定案可能发生在 Close 里)。
	if w.buf.Len() > 0 {
		pending := w.buf.Bytes()
		w.buf.Reset()
		if _, err := w.ResponseWriter.Write(pending); err != nil {
			return 0, err
		}
	}
	return w.ResponseWriter.Write(p)
}

// Flush 实现 http.Flusher(导出 CSV 是流式输出,中间要能刷)。
func (w *gzipResponseWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	// 还没定案就刷:必须先定案 + 把头发出去,
	// 否则底层 Flush 会隐式把 200 和裸头发出去(而我们之后又要加 gzip 头,来不及了)。
	if !w.decided {
		w.decide(true)
	}
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Close 收尾:决定还没定的话此刻定(小响应不压),然后关掉压缩流。
func (w *gzipResponseWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.decided {
		// 整个响应还没超过阈值:原样输出(小响应压了不划算)。
		w.decide(false)
	}
	if w.gz != nil {
		err := w.gz.Close()
		w.gz = nil
		return err
	}
	return nil
}

// withGzip 给响应加上 gzip 压缩(客户端支持时)。
func (p *Platform) withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 客户端不支持就原样返回。
		if !strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w}
		// Vary 一定要加:**即使这次没压**。
		// 共享缓存(CDN / 反向代理)看到「同一个 URL 有压缩和不压缩两种形态」时,
		// 必须靠 Vary 区分,否则会把 gzip 版发给不支持 gzip 的客户端,
		// 那个客户端直接解析失败。
		w.Header().Add("Vary", "Accept-Encoding")
		defer func() { _ = gw.Close() }()
		next.ServeHTTP(gw, r)
	})
}
