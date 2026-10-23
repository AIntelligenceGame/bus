// Package store 定义 sqlextd 的数据访问层。
//
// 抽象成接口是为了让"本地零依赖跑起来"和"生产用 MySQL"两条路都通：
//   - NewMemory()  内存实现，进程重启即丢，够开发调试
//   - 生产环境换成 gorm/MySQL 实现即可，接口不用动
package store

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// 常见的存储层错误。
var (
	ErrNotFound            = errors.New("store: not found")
	ErrEmailTaken          = errors.New("store: email already registered")
	ErrInsufficientBalance = errors.New("store: insufficient balance")
	ErrOrderState          = errors.New("store: invalid order state")
)

// User 是用户。
type User struct {
	ID           string    `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Balance      int64     `json:"balance"`
	CreatedAt    time.Time `json:"created_at"`
}

// Order 是一笔充值订单。
type Order struct {
	ID        string     `json:"id"`
	UserID    string     `json:"user_id"`
	Amount    int64      `json:"amount"`
	Channel   string     `json:"channel"`
	Status    string     `json:"status"`
	Ref       string     `json:"ref,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	PaidAt    *time.Time `json:"paid_at,omitempty"`
}

// 订单状态。
const (
	OrderPending  = "pending"
	OrderPaid     = "paid"
	OrderCanceled = "canceled"
)

// Session 是一个登录态。
type Session struct {
	Token     string    `json:"-"`
	UserID    string    `json:"user_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store 是数据访问接口。
type Store interface {
	// 用户
	CreateUser(ctx context.Context, email, passwordHash string, bonus int64) (*User, error)
	UserByEmail(ctx context.Context, email string) (*User, error)
	UserByID(ctx context.Context, id string) (*User, error)

	// 余额。扣减必须是原子的，余额不足返回 ErrInsufficientBalance。
	AddBalance(ctx context.Context, userID string, amount int64) error
	DeductBalance(ctx context.Context, userID string, amount int64) error

	// 登录态
	CreateSession(ctx context.Context, token, userID string, expiresAt time.Time) error
	Session(ctx context.Context, token string) (*Session, error)
	DeleteSession(ctx context.Context, token string) error

	// 订单
	CreateOrder(ctx context.Context, o *Order) error
	Order(ctx context.Context, id string) (*Order, error)
	ListOrders(ctx context.Context, userID string, limit int) ([]*Order, error)
	// MarkOrderPaid 把订单置为已支付并给用户加余额。
	// 已经付过的订单直接返回 nil（幂等，防止重复回调重复加钱）。
	MarkOrderPaid(ctx context.Context, orderID, ref string) (*Order, error)

	// 站点授权：用户允许哪些网站调用解析接口
	AddSite(ctx context.Context, userID, origin string) error
	RemoveSite(ctx context.Context, userID, origin string) error
	Sites(ctx context.Context, userID string) ([]string, error)
}

// Memory 是内存实现（开发/自用）。
type Memory struct {
	mu       sync.Mutex
	users    map[string]*User
	byEmail  map[string]string
	sessions map[string]*Session
	orders   map[string]*Order
	sites    map[string]map[string]bool
	seq      int64
}

// NewMemory 创建内存存储。
func NewMemory() *Memory {
	return &Memory{
		users:    map[string]*User{},
		byEmail:  map[string]string{},
		sessions: map[string]*Session{},
		orders:   map[string]*Order{},
		sites:    map[string]map[string]bool{},
	}
}

func (m *Memory) CreateUser(ctx context.Context, email, passwordHash string, bonus int64) (*User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	email = normalizeEmail(email)
	if _, ok := m.byEmail[email]; ok {
		return nil, ErrEmailTaken
	}
	m.seq++
	u := &User{
		ID:           "u_" + itoa(m.seq),
		Email:        email,
		PasswordHash: passwordHash,
		Balance:      bonus,
		CreatedAt:    time.Now(),
	}
	m.users[u.ID] = u
	m.byEmail[email] = u.ID
	return cloneUser(u), nil
}

func (m *Memory) UserByEmail(ctx context.Context, email string) (*User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byEmail[normalizeEmail(email)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneUser(m.users[id]), nil
}

func (m *Memory) UserByID(ctx context.Context, id string) (*User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneUser(u), nil
}

func (m *Memory) AddBalance(ctx context.Context, userID string, amount int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[userID]
	if !ok {
		return ErrNotFound
	}
	u.Balance += amount
	return nil
}

func (m *Memory) DeductBalance(ctx context.Context, userID string, amount int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[userID]
	if !ok {
		return ErrNotFound
	}
	if u.Balance < amount {
		return ErrInsufficientBalance
	}
	u.Balance -= amount
	return nil
}

func (m *Memory) CreateSession(ctx context.Context, token, userID string, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[userID]; !ok {
		return ErrNotFound
	}
	m.sessions[token] = &Session{Token: token, UserID: userID, ExpiresAt: expiresAt}
	return nil
}

func (m *Memory) Session(ctx context.Context, token string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[token]
	if !ok {
		return nil, ErrNotFound
	}
	if time.Now().After(s.ExpiresAt) {
		delete(m.sessions, token)
		return nil, ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *Memory) DeleteSession(ctx context.Context, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, token)
	return nil
}

func (m *Memory) CreateOrder(ctx context.Context, o *Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.ID == "" {
		m.seq++
		o.ID = "o_" + itoa(m.seq)
	}
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now()
	}
	cp := *o
	m.orders[o.ID] = &cp
	return nil
}

func (m *Memory) Order(ctx context.Context, id string) (*Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *o
	return &cp, nil
}

func (m *Memory) ListOrders(ctx context.Context, userID string, limit int) ([]*Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Order, 0, limit)
	for _, o := range m.orders {
		if o.UserID == userID {
			cp := *o
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) MarkOrderPaid(ctx context.Context, orderID, ref string) (*Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[orderID]
	if !ok {
		return nil, ErrNotFound
	}
	if o.Status == OrderPaid {
		// 幂等：重复回调不重复加钱
		cp := *o
		return &cp, nil
	}
	if o.Status != OrderPending {
		return nil, ErrOrderState
	}
	now := time.Now()
	o.Status = OrderPaid
	o.PaidAt = &now
	if ref != "" {
		o.Ref = ref
	}
	u, ok := m.users[o.UserID]
	if !ok {
		return nil, ErrNotFound
	}
	u.Balance += o.Amount
	cp := *o
	return &cp, nil
}

func (m *Memory) AddSite(ctx context.Context, userID, origin string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[userID]; !ok {
		return ErrNotFound
	}
	if m.sites[userID] == nil {
		m.sites[userID] = map[string]bool{}
	}
	m.sites[userID][normalizeOrigin(origin)] = true
	return nil
}

func (m *Memory) RemoveSite(ctx context.Context, userID, origin string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if set, ok := m.sites[userID]; ok {
		delete(set, normalizeOrigin(origin))
	}
	return nil
}

func (m *Memory) Sites(ctx context.Context, userID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.sites[userID]
	out := make([]string, 0, len(set))
	for o := range set {
		out = append(out, o)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// normalizeOrigin 归一化站点来源：只保留 scheme://host[:port]。
func normalizeOrigin(origin string) string {
	o := strings.TrimSpace(strings.ToLower(origin))
	if i := strings.Index(o, "://"); i >= 0 {
		o = o[i+3:]
	}
	if i := strings.IndexAny(o, "/?#"); i >= 0 {
		o = o[:i]
	}
	return o
}

func cloneUser(u *User) *User {
	cp := *u
	return &cp
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
