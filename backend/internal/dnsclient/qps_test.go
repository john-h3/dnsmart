package dnsclient

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/miekg/dns"
)

const (
	qpsWorkers  = 50
	qpsDuration = 30 * time.Second
	qpsConns    = 4
	qpsTimeout  = 3 * time.Second
	qpsName     = "google.com."

	// proxyEnv names the SOCKS5 proxy URL, e.g. socks5://host:1080. It is never hard-coded.
	proxyEnv = "DNSMART_SOCKS5"
)

// loadLocalEnv reads KEY=VALUE lines from .env.local in the module root (the nearest directory
// above the test with a go.mod). A variable already set in the process environment wins over
// the file, so a one-off override on the command line still works. A missing file is not an error.
func loadLocalEnv() {
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}
		dir = parent
	}
	data, err := os.ReadFile(filepath.Join(dir, ".env.local"))
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, set := os.LookupEnv(k); !set {
			os.Setenv(k, v)
		}
	}
}

// testProxy returns the SOCKS5 proxy URL from the environment or .env.local. It returns ""
// when neither is set, and the QPS test is then skipped.
func testProxy() string {
	return os.Getenv(proxyEnv)
}

// runQPS starts workers goroutines that query c in a loop until d elapses. It returns the
// number of successful and failed queries, the wall time, and the first failure seen.
// Queries cut short by the deadline are not counted as failures.
func runQPS(c *Client, workers int, d time.Duration) (ok, failed int64, elapsed time.Duration, firstErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	var okN, failN atomic.Int64
	var errOnce sync.Once
	var wg sync.WaitGroup

	start := time.Now()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				// Each query gets its own message: Query packs it, which writes m.Data.
				m := dns.NewMsg(qpsName, dns.TypeA)
				_, err := c.Query(ctx, m)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					failN.Add(1)
					errOnce.Do(func() { firstErr = err })
					continue
				}
				okN.Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed = time.Since(start)
	return okN.Load(), failN.Load(), elapsed, firstErr
}

// TestQPS measures average QPS for each protocol with and without multiplexing. Every case
// uses 4 connections, a 3s query timeout, and 50 goroutines querying google.com for 30s.
// It needs a reachable SOCKS5 proxy, see testProxy.
func TestQPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping QPS test in -short mode")
	}
	loadLocalEnv()
	if testProxy() == "" {
		t.Skipf("set %s or put it in .env.local to run the QPS test", proxyEnv)
	}
	cases := []struct {
		name string
		cfg  Config
	}{
		{"udp/mux=off", Config{Protocol: UDP, Upstream: "8.8.8.8:53", Multiplex: false}},
		{"udp/mux=on", Config{Protocol: UDP, Upstream: "8.8.8.8:53", Multiplex: true}},
		{"doh/mux=off", Config{Protocol: DoH, Upstream: "https://8.8.8.8/dns-query", ServerName: "dns.google", Multiplex: false}},
		{"doh/mux=on", Config{Protocol: DoH, Upstream: "https://8.8.8.8/dns-query", ServerName: "dns.google", Multiplex: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			cfg.Proxy = testProxy()
			cfg.Conns = qpsConns
			cfg.Timeout = qpsTimeout

			c, err := New(cfg)
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			defer c.Close()

			ok, failed, elapsed, firstErr := runQPS(c, qpsWorkers, qpsDuration)
			qps := float64(ok) / elapsed.Seconds()
			t.Logf("ok=%d failed=%d elapsed=%v avg_qps=%.1f", ok, failed, elapsed.Round(time.Millisecond), qps)
			if firstErr != nil {
				t.Logf("first error: %v", firstErr)
			}
			if ok == 0 {
				t.Fatalf("no successful queries")
			}
		})
	}
}
