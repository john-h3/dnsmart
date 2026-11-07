package dnsclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"

	xproxy "golang.org/x/net/proxy"
)

// parseDoHURL checks that raw is an https URL.
func parseDoHURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("dnsclient: parse doh upstream: %w", err)
	}
	if u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("dnsclient: doh upstream must be an https URL, got %q", raw)
	}
	return u, nil
}

// dohConn is one HTTP client with its own transport. With multiplexing it uses HTTP/2, so the
// transport keeps one connection and carries all its queries as streams. Without multiplexing it
// uses HTTP/1.1 with one connection, and the pool gives each query exclusive use of it.
type dohConn struct {
	client *http.Client
	tr     *http.Transport
	url    string
}

func newDoHDialer(d xproxy.ContextDialer, endpoint, serverName string, multiplex bool) dialFunc {
	return func(context.Context) (conn, error) {
		tr := &http.Transport{
			DialContext:       d.DialContext,
			TLSClientConfig:   &tls.Config{ServerName: serverName},
			ForceAttemptHTTP2: multiplex,
		}
		if !multiplex {
			tr.MaxConnsPerHost = 1
		}
		return &dohConn{client: &http.Client{Transport: tr}, tr: tr, url: endpoint}, nil
	}
}

func (c *dohConn) exchange(ctx context.Context, q []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(q))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/dns-message")
	req.Header.Set("Accept", "application/dns-message")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("doh request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("doh status %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// broken is always false. net/http replaces a dead connection on the next request by itself.
func (c *dohConn) broken() bool { return false }

func (c *dohConn) close() {
	c.tr.CloseIdleConnections()
}
