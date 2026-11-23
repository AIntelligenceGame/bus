package platform

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 「平台主动往外发请求」的地址限制(SSRF 防护)。
//
// 背景:短信网关的 URL、SMTP 服务器地址都是**商户自己在控制台里填的**,
// 平台会照着去连。这等于把平台变成了攻击者的跳板:
// 填 http://127.0.0.1:6379/ 或 http://169.254.169.254/latest/meta-data/,
// 平台就会替攻击者去访问内网 / 云元数据,并把响应带回来。
//
// 防线有三层:
//
//  1. **保存配置时就校验**:填了非法地址当场就报错,不用等到发验证码才发现;
//  2. **真正连接时再校验一次**(DialContext):域名解析出来的 IP 逐个检查,
//     这一层还能挡住「DNS 重绑定」(先解析成公网 IP 通过校验,再解析成内网 IP);
//  3. **重定向也校验**:网关回一个 302 到内网地址,不能跟着跳过去。
//
// 允许访问内网的情况确实存在(企业自建短信网关、内网 SMTP),
// 所以留了开关:Options.OutboundAllowPrivate / Options.OutboundAllowedHosts。

// outboundPolicy 出站请求的地址策略。
type outboundPolicy struct {
	// allowPrivate 允许访问内网 / 回环 / 链路本地地址。
	allowPrivate bool
	// allowedHosts 主机名白名单(不区分大小写)。命中则跳过 IP 段检查。
	allowedHosts []string
}

// outboundPolicyFrom 从平台配置生成出站策略。
func outboundPolicyFrom(opts Options) outboundPolicy {
	hosts := make([]string, 0, len(opts.OutboundAllowedHosts))
	for _, h := range opts.OutboundAllowedHosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if h != "" {
			hosts = append(hosts, h)
		}
	}
	return outboundPolicy{allowPrivate: opts.OutboundAllowPrivate, allowedHosts: hosts}
}

// hostAllowed 判断主机名是否在白名单里。
func (p outboundPolicy) hostAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	for _, h := range p.allowedHosts {
		if h == host {
			return true
		}
	}
	return false
}

// validateOutboundURLSyntax 只做**不依赖 DNS** 的检查:协议、主机名、
// 以及「主机名本身就是内网 IP 字面量」的情况。
//
// 用在「保存配置」的时候:
// 平台不该因为对方 DNS 解析不了就把配置拒掉(比如内网 DNS、临时故障、
// 还没开通的域名),不然用户会卡在「明明地址是对的却保存不了」。
// 真正发出去之前的检查在 validateOutboundURL(带 DNS 解析)。
func (p outboundPolicy) validateOutboundURLSyntax(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("地址为空")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("地址格式不正确: %v", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return nil, fmt.Errorf("只允许 http / https 协议,当前是 %q", u.Scheme)
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		return nil, fmt.Errorf("地址里没有主机名")
	}
	if p.hostAllowed(host) || p.allowPrivate {
		return u, nil
	}
	if ip := net.ParseIP(host); ip != nil && isPrivateIP(ip) {
		return nil, fmt.Errorf(
			"目标地址 %s 是内网地址,平台禁止访问内网地址"+
				"(否则等于让商户借平台探测你的内网/云元数据);"+
				"确实需要内网网关请让平台方在启动参数里打开 OutboundAllowPrivate,"+
				"或把主机名加进 OutboundAllowedHosts", host)
	}
	return u, nil
}

// validateOutboundURL 校验一个出站 URL 是否可以访问(带 DNS 解析)。
//
// 检查项:
//
//   - 协议必须是 http / https(其它协议一律拒绝);
//   - 主机名不能为空;
//   - 主机名解析出来的 **每一个** IP 都必须是公网地址
//     (只要有一个落在内网段就整体拒绝:一个域名可以同时解析到多个 IP)。
func (p outboundPolicy) validateOutboundURL(raw string) (*url.URL, error) {
	u, err := p.validateOutboundURLSyntax(raw)
	if err != nil {
		return nil, err
	}
	host := strings.TrimSpace(u.Hostname())
	if p.hostAllowed(host) || p.allowPrivate {
		return u, nil
	}
	ips, err := lookupIPs(host)
	if err != nil {
		return nil, fmt.Errorf("解析主机名 %s 失败: %v", host, err)
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return nil, fmt.Errorf(
				"目标地址 %s 解析到内网地址 %s,平台禁止访问内网地址"+
					"(否则等于让商户借平台探测你的内网/云元数据);"+
					"确实需要内网网关请让平台方在启动参数里打开 OutboundAllowPrivate,"+
					"或把主机名加进 OutboundAllowedHosts", host, ip.String())
		}
	}
	return u, nil
}

// validateSMTPHostSyntax 只做不依赖 DNS 的检查(保存配置时用)。
func (p outboundPolicy) validateSMTPHostSyntax(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("SMTP 服务器地址为空")
	}
	if strings.ContainsAny(host, " \t\r\n/\\") {
		return fmt.Errorf("SMTP 服务器地址里有非法字符: %q", host)
	}
	if p.hostAllowed(host) || p.allowPrivate {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && isPrivateIP(ip) {
		return fmt.Errorf(
			"目标地址 %s 是内网地址,平台禁止连接内网地址;"+
				"确实需要内网 SMTP 请让平台方打开 OutboundAllowPrivate,"+
				"或把主机名加进 OutboundAllowedHosts", host)
	}
	return nil
}

// validateSMTPHost 校验 SMTP 服务器地址。
//
// SMTP 不是 HTTP,没法用 DialContext 卡住,所以做一次「预检」:
// 主机名解析出来的 IP 必须是公网地址(或在白名单里 / 允许内网)。
//
// 端口本身不拦:连到公网 IP 的任意端口最多算端口探测,危害远小于
// 「读内网服务的响应」。
func (p outboundPolicy) validateSMTPHost(host string) error {
	if err := p.validateSMTPHostSyntax(host); err != nil {
		return err
	}
	host = strings.TrimSpace(host)
	if p.hostAllowed(host) || p.allowPrivate {
		return nil
	}
	ips, err := lookupIPs(host)
	if err != nil {
		return fmt.Errorf("解析主机名 %s 失败: %v", host, err)
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return fmt.Errorf(
				"目标地址 %s 解析到内网地址 %s,平台禁止连接内网地址;"+
					"确实需要内网 SMTP 请让平台方打开 OutboundAllowPrivate,"+
					"或把主机名加进 OutboundAllowedHosts", host, ip.String())
		}
	}
	return nil
}

// lookupIPs 解析主机名,返回它所有的 IP。
func lookupIPs(host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	addrs, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("没有解析到任何 IP")
	}
	return addrs, nil
}

// isPrivateIP 判断一个 IP 是否属于「不该让商户访问」的范围。
//
// 覆盖:回环(127/8、::1)、私有网段(10/8、172.16/12、192.168/16)、
// 链路本地(169.254/16 —— **云元数据 169.254.169.254 就在这里**)、
// 未指定地址(0.0.0.0)、组播、以及 IPv6 的 ULA(fc00::/7)。
func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return true
	}
	// 100.64.0.0/10(CGNAT)与 IPv4 映射地址。
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return true
		}
		return false
	}
	// IPv6 ULA:fc00::/7。
	if len(ip) == net.IPv6len && (ip[0]&0xfe) == 0xfc {
		return true
	}
	return false
}

// newOutboundClient 造一个受策略约束的 HTTP 客户端。
//
//   - DialContext:真正建连接时校验目标 IP(挡住 DNS 重绑定);
//   - CheckRedirect:重定向目标也要过一遍校验(最多 5 跳)。
func (p outboundPolicy) newOutboundClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		// 代理一律不走:走代理就绕过了 IP 校验。
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			if !p.hostAllowed(host) && !p.allowPrivate {
				ips, lerr := lookupIPs(host)
				if lerr != nil {
					return nil, lerr
				}
				for _, ip := range ips {
					if isPrivateIP(ip) {
						return nil, fmt.Errorf("拒绝连接内网地址 %s(%s)", addr, ip.String())
					}
				}
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("重定向次数过多")
			}
			if _, err := p.validateOutboundURL(req.URL.String()); err != nil {
				return fmt.Errorf("重定向目标被拒绝: %v", err)
			}
			return nil
		},
	}
}
