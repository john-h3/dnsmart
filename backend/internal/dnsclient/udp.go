package dnsclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var errUDPClosed = errors.New("dnsclient: udp association closed")

// udpConn is one SOCKS5 UDP ASSOCIATE (RFC 1928 section 7). The proxy relays only while the TCP
// control connection stays open, so both live as long as the conn. A reader goroutine hands each
// reply to the query that sent the same DNS message ID.
type udpConn struct {
	ctrl   net.Conn
	pc     net.PacketConn
	relay  *net.UDPAddr
	header []byte // SOCKS5 UDP request header naming the upstream: RSV RSV FRAG ATYP DST.ADDR DST.PORT
	in     *pending
	done   chan struct{}
	once   sync.Once
}

func dialUDP(ctx context.Context, proxyAddr string, target []byte) (*udpConn, error) {
	var d net.Dialer
	ctrl, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}
	relay, err := associate(ctx, ctrl, proxyAddr)
	if err != nil {
		ctrl.Close()
		return nil, err
	}
	pc, err := net.ListenPacket("udp", ":0")
	if err != nil {
		ctrl.Close()
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	c := &udpConn{
		ctrl:   ctrl,
		pc:     pc,
		relay:  relay,
		header: append([]byte{0, 0, 0}, target...),
		in:     newPending(),
		done:   make(chan struct{}),
	}
	go c.readLoop()
	// The proxy closes the control connection when the association ends. Watching it lets
	// waiters fail at once instead of timing out.
	go func() {
		_, _ = io.Copy(io.Discard, ctrl)
		c.fail()
	}()
	return c, nil
}

// associate runs the SOCKS5 greeting and UDP ASSOCIATE on ctrl and returns the relay address.
func associate(ctx context.Context, ctrl net.Conn, proxyAddr string) (*net.UDPAddr, error) {
	if dl, ok := ctx.Deadline(); ok {
		if err := ctrl.SetDeadline(dl); err != nil {
			return nil, err
		}
	}
	// Greeting offers only "no authentication" (0x00).
	if _, err := ctrl.Write([]byte{5, 1, 0}); err != nil {
		return nil, fmt.Errorf("write greeting: %w", err)
	}
	var sel [2]byte
	if _, err := io.ReadFull(ctrl, sel[:]); err != nil {
		return nil, fmt.Errorf("read method selection: %w", err)
	}
	if sel[1] != 0 {
		return nil, fmt.Errorf("proxy selected auth method %#x", sel[1])
	}
	// UDP ASSOCIATE with 0.0.0.0:0 lets the proxy choose the client side.
	if _, err := ctrl.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return nil, fmt.Errorf("write udp associate: %w", err)
	}
	relay, err := readBoundAddr(ctrl, proxyAddr)
	if err != nil {
		return nil, err
	}
	// The handshake deadline must not carry over, or the association dies with it.
	if err := ctrl.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return relay, nil
}

func (c *udpConn) readLoop() {
	buf := make([]byte, 65535)
	for {
		n, _, err := c.pc.ReadFrom(buf)
		if err != nil {
			c.fail()
			return
		}
		hl, err := socksUDPHeaderLen(buf[:n])
		if err != nil || n-hl < 2 {
			continue
		}
		reply := buf[hl:n]
		c.in.deliver(binary.BigEndian.Uint16(reply[:2]), append([]byte(nil), reply...))
	}
}

func (c *udpConn) exchange(ctx context.Context, q []byte) ([]byte, error) {
	id, ch, err := c.in.add()
	if err != nil {
		return nil, err
	}
	defer c.in.remove(id)

	pkt := append(append(make([]byte, 0, len(c.header)+len(q)), c.header...), q...)
	binary.BigEndian.PutUint16(pkt[len(c.header):], id)
	if _, err := c.pc.WriteTo(pkt, c.relay); err != nil {
		c.fail()
		return nil, fmt.Errorf("write via relay: %w", err)
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-c.done:
		return nil, errUDPClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *udpConn) broken() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *udpConn) fail() {
	c.once.Do(func() { close(c.done) })
}

func (c *udpConn) close() {
	c.fail()
	c.pc.Close()
	c.ctrl.Close()
}

// pending maps in-use DNS message IDs to the waiter for each reply.
type pending struct {
	mu sync.Mutex
	m  map[uint16]chan []byte
}

func newPending() *pending {
	return &pending{m: make(map[uint16]chan []byte)}
}

// add picks a random free ID. Random IDs make replies harder to forge (RFC 5452).
func (p *pending) add() (uint16, chan []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.m) >= 1<<16 {
		return 0, nil, errors.New("dnsclient: no free dns message id")
	}
	var b [2]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, nil, fmt.Errorf("dnsclient: random id: %w", err)
		}
		id := binary.BigEndian.Uint16(b[:])
		if _, used := p.m[id]; !used {
			ch := make(chan []byte, 1)
			p.m[id] = ch
			return id, ch, nil
		}
	}
}

// deliver hands data to the waiter for id. It is dropped if nobody waits for id.
func (p *pending) deliver(id uint16, data []byte) {
	p.mu.Lock()
	ch, ok := p.m[id]
	if ok {
		delete(p.m, id)
	}
	p.mu.Unlock()
	if ok {
		ch <- data // buffered, and only this sender can reach it
	}
}

func (p *pending) remove(id uint16) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()
}
