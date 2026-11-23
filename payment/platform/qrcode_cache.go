package platform

import (
	"container/list"
	"sync"
	"time"
)

// timeNow 取当前时间(抽出来是为了测试时能替换)。
var timeNow = time.Now

// qrcodeCache 二维码图片的进程内缓存(内容 → PNG 字节)。
//
// 为什么需要:
//
//	渲染一张二维码要编码 + 光栅化,实测 5~35 ms CPU(内容越长越慢)。
//	同一个支付链接被反复取(用户刷新页面、多个标签页、手机端重试)是常态,
//	每次都重算就是纯浪费。缓存命中就是一次 map 查找。
//
// 特点:
//
//   - **有上限**:最多缓存 qrcodeCacheMaxEntries 张,超出按 LRU 淘汰。
//     没有上限的话,攻击者用不同 content 刷一遍就能把内存吃光。
//   - **有 TTL**:二维码链接是有时效的(支付单会过期),缓存太久会返回过期链接。
//   - **线程安全**:并发请求会同时命中同一个 key。
type qrcodeCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = 最近使用,back = 最久未用
	ttl     int64      // 秒
	max     int
}

type qrcodeCacheEntry struct {
	key       string
	data      []byte
	expiresAt int64
}

// qrcodeCacheMaxEntries 缓存的最大条目数。
const qrcodeCacheMaxEntries = 512

// newQRCodeCache 创建二维码缓存。
func newQRCodeCache(ttlSeconds int64) *qrcodeCache {
	if ttlSeconds <= 0 {
		ttlSeconds = 300
	}
	return &qrcodeCache{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		ttl:     ttlSeconds,
		max:     qrcodeCacheMaxEntries,
	}
}

// nowUnix 取当前 Unix 秒(抽出来是为了测试时能替换)。
var nowUnix = func() int64 { return timeNow().Unix() }

// get 取缓存。命中且未过期返回数据,true。
func (c *qrcodeCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*qrcodeCacheEntry)
	if nowUnix() > entry.expiresAt {
		c.removeElement(el)
		return nil, false
	}
	c.order.MoveToFront(el)
	return entry.data, true
}

// put 写入缓存(超出上限时淘汰最久未用的)。
func (c *qrcodeCache) put(key string, data []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := nowUnix()
	// 同一个 key 再写一次:更新内容并续期。
	if el, ok := c.entries[key]; ok {
		entry := el.Value.(*qrcodeCacheEntry)
		entry.data = data
		entry.expiresAt = now + c.ttl
		c.order.MoveToFront(el)
		return
	}
	el := c.order.PushFront(&qrcodeCacheEntry{
		key:       key,
		data:      data,
		expiresAt: now + c.ttl,
	})
	c.entries[key] = el
	// 超出上限:淘汰最久未用的。
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.removeElement(oldest)
	}
}

// removeElement 从链表与索引里删掉一个条目。
func (c *qrcodeCache) removeElement(el *list.Element) {
	c.order.Remove(el)
	if entry, ok := el.Value.(*qrcodeCacheEntry); ok {
		delete(c.entries, entry.key)
	}
}
