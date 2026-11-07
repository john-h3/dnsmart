package dnsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// conn is one upstream connection. A conn may be shared by many goroutines when the pool is
// multiplexed, so implementations must be safe for concurrent exchange calls.
type conn interface {
	exchange(ctx context.Context, q []byte) ([]byte, error)
	// broken reports whether the conn is dead and must be replaced. A query that timed out
	// does not make the conn broken.
	broken() bool
	close()
}

type dialFunc func(ctx context.Context) (conn, error)

// slot holds one connection. It dials lazily and replaces the connection once it is broken.
type slot struct {
	dial dialFunc

	mu     sync.Mutex
	c      conn
	closed bool
}

// get returns the live connection, dialing a new one if there is none or the old one is broken.
// It holds the lock while dialing, so goroutines sharing this slot wait for the same dial.
func (s *slot) get(ctx context.Context) (conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.c != nil && !s.c.broken() {
		return s.c, nil
	}
	if s.c != nil {
		s.c.close()
		s.c = nil
	}
	c, err := s.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("dnsclient: dial: %w", err)
	}
	s.c = c
	return c, nil
}

// drop discards c if it is still the slot's connection. Another goroutine may already have
// replaced it.
func (s *slot) drop(c conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c == c {
		s.c.close()
		s.c = nil
	}
}

func (s *slot) exchange(ctx context.Context, q []byte) ([]byte, error) {
	c, err := s.get(ctx)
	if err != nil {
		return nil, err
	}
	reply, err := c.exchange(ctx, q)
	if err != nil && c.broken() {
		s.drop(c)
	}
	return reply, err
}

func (s *slot) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.c != nil {
		s.c.close()
		s.c = nil
	}
}

// pool owns the slots. Without multiplexing, a query takes a slot exclusively from idle. With
// multiplexing, queries are spread round-robin and slots are shared.
type pool struct {
	slots []*slot
	idle  chan *slot // nil when multiplexed
	next  atomic.Uint64
}

func newPool(n int, multiplex bool, dial dialFunc) *pool {
	p := &pool{slots: make([]*slot, n)}
	for i := range p.slots {
		p.slots[i] = &slot{dial: dial}
	}
	if !multiplex {
		// Capacity n and at most n slots in the channel, so release never blocks.
		p.idle = make(chan *slot, n)
		for _, s := range p.slots {
			p.idle <- s
		}
	}
	return p
}

func (p *pool) acquire(ctx context.Context) (*slot, error) {
	if p.idle == nil {
		i := p.next.Add(1) - 1
		return p.slots[i%uint64(len(p.slots))], nil
	}
	select {
	case s := <-p.idle:
		return s, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("dnsclient: waiting for free connection: %w", ctx.Err())
	}
}

func (p *pool) release(s *slot) {
	if p.idle != nil {
		p.idle <- s
	}
}

func (p *pool) close() {
	for _, s := range p.slots {
		s.close()
	}
}
