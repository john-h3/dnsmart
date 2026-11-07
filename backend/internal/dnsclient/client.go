// Package dnsclient sends DNS queries over UDP or DoH through a SOCKS5 proxy. It keeps a fixed
// number of connections and reuses them across queries. A connection that breaks is replaced
// lazily by the next query that needs it.
package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"codeberg.org/miekg/dns"
	xproxy "golang.org/x/net/proxy"
)

// Protocol selects the transport used to reach the upstream.
type Protocol int

const (
	// UDP sends classic DNS over UDP (RFC 1035).
	UDP Protocol = iota + 1
	// DoH sends DNS over HTTPS (RFC 8484).
	DoH
)

const defaultTimeout = 10 * time.Second

// ErrClosed is returned by Query after Close.
var ErrClosed = errors.New("dnsclient: client closed")

// Config describes one upstream and how connections to it are managed.
type Config struct {
	Protocol Protocol

	// Upstream is host:port for UDP, e.g. "8.8.8.8:53", and an https URL for DoH,
	// e.g. "https://8.8.8.8/dns-query".
	Upstream string

	// Proxy is a SOCKS5 proxy URL, e.g. "socks5://10.0.0.1:1080". All traffic goes through it.
	Proxy string

	// ServerName is the TLS name checked against the DoH certificate. It is needed when Upstream
	// is an IP address, e.g. "dns.google" for 8.8.8.8. Empty means the host of Upstream. Unused for UDP.
	ServerName string

	// Conns is the number of underlying connections. Zero means 1.
	Conns int

	// Multiplex lets one connection carry many queries at once, matched by DNS message ID.
	// When false, a connection carries one query at a time, and extra queries wait for a free one.
	Multiplex bool

	// Timeout bounds one Query call, including waiting for a free connection and dialing.
	// Zero means 10s. Timeouts are not retried.
	Timeout time.Duration
}

// Client sends DNS queries through a pool of connections. It is safe for concurrent use.
type Client struct {
	cfg    Config
	pool   *pool
	closed atomic.Bool
}

// New validates cfg and returns a client. No connection is opened until the first query.
func New(cfg Config) (*Client, error) {
	if cfg.Conns < 0 {
		return nil, fmt.Errorf("dnsclient: conns must not be negative, got %d", cfg.Conns)
	}
	if cfg.Conns == 0 {
		cfg.Conns = 1
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("dnsclient: timeout must not be negative, got %v", cfg.Timeout)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	proxyAddr, err := parseProxy(cfg.Proxy)
	if err != nil {
		return nil, err
	}
	sd, err := xproxy.SOCKS5("tcp", proxyAddr, nil, xproxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("dnsclient: socks5 dialer: %w", err)
	}
	cd, ok := sd.(xproxy.ContextDialer)
	if !ok {
		return nil, errors.New("dnsclient: socks5 dialer has no DialContext")
	}

	var dial dialFunc
	switch cfg.Protocol {
	case UDP:
		target, err := udpTarget(cfg.Upstream)
		if err != nil {
			return nil, err
		}
		dial = func(ctx context.Context) (conn, error) {
			c, err := dialUDP(ctx, proxyAddr, target)
			if err != nil {
				return nil, err
			}
			return c, nil
		}
	case DoH:
		u, err := parseDoHURL(cfg.Upstream)
		if err != nil {
			return nil, err
		}
		serverName := cfg.ServerName
		if serverName == "" {
			serverName = u.Hostname()
		}
		dial = newDoHDialer(cd, u.String(), serverName, cfg.Multiplex)
	default:
		return nil, fmt.Errorf("dnsclient: unknown protocol %d", cfg.Protocol)
	}

	return &Client{cfg: cfg, pool: newPool(cfg.Conns, cfg.Multiplex, dial)}, nil
}

// Query sends m and returns the decoded reply. Query packs m, which writes m.Data, so the same
// *dns.Msg must not be passed to concurrent calls. The message ID is assigned per connection,
// and any ID set on m is overwritten. A reply with a non-zero Rcode is returned without error.
//
// Query does not retry. If the connection fails, the connection is replaced by the next query.
func (c *Client) Query(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	if err := m.Pack(); err != nil {
		return nil, fmt.Errorf("dnsclient: pack query: %w", err)
	}

	s, err := c.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.exchange(ctx, m.Data)
	c.pool.release(s)
	if err != nil {
		return nil, err
	}

	resp := new(dns.Msg)
	resp.Data = raw
	if err := resp.Unpack(); err != nil {
		return nil, fmt.Errorf("dnsclient: unpack reply: %w", err)
	}
	return resp, nil
}

// Close closes all connections. In-flight queries fail, and later queries return ErrClosed.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.pool.close()
	return nil
}
