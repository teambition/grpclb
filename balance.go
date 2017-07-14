package grpclb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tevino/abool"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
)

type balance struct {
	addrsCh chan []grpc.Address
	waitL   sync.RWMutex
	waitCh  chan struct{}
	done    *abool.AtomicBool
	f       HasherFromContext
	h       ConsistentHasher
	r       naming.Resolver
	w       naming.Watcher
}

// NewKetamaBalance balance with ketama algorithm.
func NewKetamaBalance(r naming.Resolver, f ...HasherFromContext) grpc.Balancer {
	return NewBalance(newKetama(), r)
}

// NewBalance create a grpc.Balancer with given ConsistentHasher, watched key in Context and naming.Reslover.
func NewBalance(h ConsistentHasher, r naming.Resolver, f ...HasherFromContext) grpc.Balancer {
	b := &balance{
		addrsCh: make(chan []grpc.Address, 1),
		done:    abool.New(),
		h:       h,
		r:       r,
	}
	if len(f) > 0 {
		b.f = f[0]
	} else {
		b.f = strOrNumFromContext
	}
	return b
}

func (b *balance) watchAddrUpdates() error {
	us, err := b.w.Next()
	if err != nil {
		grpclog.Printf("grpc: The naming watcher stops working due to %v.\n", err)
		return err
	}

	for _, u := range us {
		if err := b.h.Update(*u); err != nil {
			grpclog.Printf("grpc: The name resolver updates fail due to %v by address(%s) and operation(%d).\n", err, u.Addr, u.Op)
		}
	}

	as := b.h.Servers()
	b.waitL.Lock()
	if len(as) == 0 {
		if b.waitCh == nil {
			b.waitCh = make(chan struct{})
		}
	} else {
		if b.waitCh != nil {
			close(b.waitCh)
			b.waitCh = nil
		}
	}
	b.waitL.Unlock()
	select {
	case <-b.addrsCh:
	default:
	}
	b.addrsCh <- as
	return nil
}

func (b *balance) Start(target string, _ grpc.BalancerConfig) (err error) {
	if b.done.IsSet() {
		return grpc.ErrClientConnClosing
	}

	if b.w, err = b.r.Resolve(target); err != nil {
		return
	}
	if err = b.watchAddrUpdates(); err != nil {
		return
	}
	go func() {
		for {
			if err := b.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return
}

func (b *balance) Up(addr grpc.Address) (down func(error)) {
	return nil
}

func (b *balance) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	h, ok := b.f(ctx)
	if !ok {
		err = fmt.Errorf("grpc: the activeHashKey is not in the context")
		return
	}
	if opts.BlockingWait {
		b.waitL.RLock()
		ch := b.waitCh
		b.waitL.RUnlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-ch:
				// wait util there is a new registry server.
			}
		}
	}
	if b.done.IsSet() {
		err = grpc.ErrClientConnClosing
		return
	}
	addr, err = b.h.Get(h)
	return
}

func (b *balance) Notify() <-chan []grpc.Address {
	return b.addrsCh
}

func (b *balance) Close() error {
	if !b.done.SetToIf(false, true) {
		return errors.New("grpc: balancer is closed")
	}
	if b.w != nil {
		b.w.Close()
	}
	b.waitL.Lock()
	if b.waitCh != nil {
		close(b.waitCh)
		b.waitCh = nil
	}
	b.waitL.Unlock()
	if b.addrsCh != nil {
		close(b.addrsCh)
	}
	return nil
}
