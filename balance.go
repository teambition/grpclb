package grpclb

import (
	"fmt"

	"github.com/tevino/abool"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
)

type balance struct {
	addrsCh chan []grpc.Address
	done    *abool.AtomicBool
	h       ConsistentHasher
	r       naming.Resolver
	w       naming.Watcher
	key     interface{}
}

// NewKetamaBalance balance with ketama algorithm.
func NewKetamaBalance(key interface{}, r naming.Resolver) grpc.Balancer {
	return NewBalance(newKetama(), key, r)
}

// NewBalance create a grpc.Balancer with given ConsistentHasher, watched key in Context and naming.Reslover.
func NewBalance(h ConsistentHasher, key interface{}, r naming.Resolver) grpc.Balancer {
	return &balance{
		addrsCh: make(chan []grpc.Address, 1),
		done:    abool.New(),
		h:       h,
		r:       r,
		key:     key,
	}
}

func (b *balance) watchAddrUpdates() error {
	us, err := b.w.Next()
	if err != nil {
		grpclog.Printf("grpc: The naming watcher stops working due to %v.\n", err)
		return err
	}

	for _, u := range us {
		if err := b.h.Update(u); err != nil {
			grpclog.Printf("grpc: The name resolver updates fail due to %v by address(%s) and operation(%d).\n", err, u.Addr, u.Op)
		}
	}

	select {
	case <-b.addrsCh:
	default:
	}
	b.addrsCh <- b.h.Servers()
	return nil
}

func (b *balance) Start(target string, _ grpc.BalancerConfig) (err error) {
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
	h, ok := fromContext(ctx, b.key)
	if !ok {
		err = fmt.Errorf("grpc: the key(%v) is not in the context", b.key)
		return
	}
	a, err := b.h.Get(h)
	if err != nil {
		return
	}
	addr = *a
	return
}

func (b *balance) Notify() <-chan []grpc.Address {
	return b.addrsCh
}

func (b *balance) Close() error {
	b.done.Set()
	return nil
}
