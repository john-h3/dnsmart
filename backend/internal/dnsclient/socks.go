package dnsclient

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
)

// parseProxy returns host:port from a socks5:// URL.
func parseProxy(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("dnsclient: parse proxy: %w", err)
	}
	if u.Scheme != "socks5" {
		return "", fmt.Errorf("dnsclient: proxy scheme must be socks5, got %q", u.Scheme)
	}
	if u.Host == "" {
		return "", errors.New("dnsclient: proxy has no host")
	}
	return u.Host, nil
}

// udpTarget encodes the upstream host:port as a SOCKS5 address (ATYP, DST.ADDR, DST.PORT).
func udpTarget(upstream string) ([]byte, error) {
	host, portStr, err := net.SplitHostPort(upstream)
	if err != nil {
		return nil, fmt.Errorf("dnsclient: udp upstream %q: %w", upstream, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("dnsclient: udp upstream port %q: %w", portStr, err)
	}
	return socksAddr(host, uint16(port))
}

func socksAddr(host string, port uint16) ([]byte, error) {
	var b []byte
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			b = append([]byte{1}, v4...)
		} else {
			b = append([]byte{4}, ip.To16()...)
		}
	} else {
		if len(host) == 0 || len(host) > 255 {
			return nil, fmt.Errorf("dnsclient: host name length %d out of range", len(host))
		}
		b = append([]byte{3, byte(len(host))}, host...)
	}
	return binary.BigEndian.AppendUint16(b, port), nil
}

func readBoundAddr(r io.Reader, proxyAddr string) (*net.UDPAddr, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read associate reply: %w", err)
	}
	if hdr[1] != 0 {
		return nil, fmt.Errorf("udp associate failed, reply code %#x", hdr[1])
	}

	var ip net.IP
	switch hdr[3] {
	case 1:
		ip = make(net.IP, 4)
	case 4:
		ip = make(net.IP, 16)
	default:
		return nil, fmt.Errorf("unsupported bound atyp %d", hdr[3])
	}
	if _, err := io.ReadFull(r, ip); err != nil {
		return nil, fmt.Errorf("read bound addr: %w", err)
	}
	var port [2]byte
	if _, err := io.ReadFull(r, port[:]); err != nil {
		return nil, fmt.Errorf("read bound port: %w", err)
	}

	// Some proxies answer 0.0.0.0, which means the same host as the control connection.
	if ip.IsUnspecified() {
		host, _, err := net.SplitHostPort(proxyAddr)
		if err != nil {
			return nil, err
		}
		ip = net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("proxy host %q is not an IP", host)
		}
	}
	return &net.UDPAddr{IP: ip, Port: int(binary.BigEndian.Uint16(port[:]))}, nil
}

// socksUDPHeaderLen returns the length of the SOCKS5 UDP header at the start of b.
func socksUDPHeaderLen(b []byte) (int, error) {
	errShort := errors.New("short socks5 udp reply")
	if len(b) < 4 {
		return 0, errShort
	}
	var addrLen int
	switch b[3] {
	case 1:
		addrLen = 4
	case 4:
		addrLen = 16
	case 3:
		if len(b) < 5 {
			return 0, errShort
		}
		addrLen = 1 + int(b[4])
	default:
		return 0, fmt.Errorf("unsupported atyp %d in udp reply", b[3])
	}
	hl := 4 + addrLen + 2
	if len(b) < hl {
		return 0, errShort
	}
	return hl, nil
}
