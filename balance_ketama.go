package grpclb

import (
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
)

type ketamaBalance struct {
	sync.RWMutex
	servers       map[string]*server
	replica       map[uint32]*server
	sortedHashSet []uint32
	addrsCh       chan []grpc.Address
	waitCh        chan struct{}
	done          bool
	f             HasherFromContext
	r             naming.Resolver
	w             naming.Watcher
}

// NewKetamaBalance balance with ketama algorithm.
func NewKetamaBalance(r naming.Resolver, f ...HasherFromContext) grpc.Balancer {
	kb := &ketamaBalance{
		servers:       map[string]*server{},
		replica:       map[uint32]*server{},
		sortedHashSet: []uint32{},
		addrsCh:       make(chan []grpc.Address, 1),
		waitCh:        make(chan struct{}),
		r:             r,
	}
	if len(f) > 0 {
		kb.f = f[0]
	} else {
		kb.f = strOrNumFromContext
	}
	return kb
}

func (kb *ketamaBalance) checkExisted(addr string) bool {
	_, ok := kb.servers[addr]
	return ok
}

func (kb *ketamaBalance) add(s *server) {
	addr := s.addr.Addr
	if _, ok := kb.servers[addr]; ok {
		grpclog.Printf("grpclb: The name resolver added an exisited server(%s).\n", addr)
		return
	}

	w := weightFromMetadata(s.addr.Metadata)
	step := math.MaxUint32 / w
	kb.servers[addr] = s

	h := fnv.New32a()
	h.Write([]byte(addr))
	serverHash := h.Sum32()
	h.Reset()

	for i := 1; i <= int(w); i++ {
		h.Write([]byte(strconv.FormatUint(uint64(serverHash)+uint64(i)*uint64(step), 10)))
		tmpH := h.Sum32()
		kb.sortedHashSet = append(kb.sortedHashSet, tmpH)
		kb.replica[tmpH] = s
		h.Reset()
	}

	sort.Slice(kb.sortedHashSet, func(i int, j int) bool {
		return kb.sortedHashSet[i] < kb.sortedHashSet[j]
	})
}

func (kb *ketamaBalance) delHash(h uint32) {
	for i, v := range kb.sortedHashSet {
		if v == h {
			kb.sortedHashSet = append(kb.sortedHashSet[:i], kb.sortedHashSet[i+1:]...)
			return
		}
	}
}

func (kb *ketamaBalance) delete(addr string) {
	s, ok := kb.servers[addr]
	if !ok {
		grpclog.Printf("grpclb: The name resolver deleted an unexistd server(%s).\n", addr)
		return
	}
	delete(kb.servers, addr)
	for k, v := range kb.replica {
		if v == s {
			delete(kb.replica, k)
			kb.delHash(k)
		}
	}
}

func (kb *ketamaBalance) get(ctx context.Context) (*server, error) {
	h, ok := kb.f(ctx)
	if !ok {
		return nil, errors.New("grpclb: The HashKey is not in the context")
	}

	length := len(kb.sortedHashSet)
	if length == 0 {
		return nil, ErrNoServer
	}

	hash32 := h.Hash32()
	idx := sort.Search(length, func(i int) bool {
		return kb.sortedHashSet[i] >= hash32
	})
	if idx >= length {
		idx = 0
	}
	return kb.replica[kb.sortedHashSet[idx]], nil
}

func (kb *ketamaBalance) watchAddrUpdates() error {
	us, err := kb.w.Next()
	if err != nil {
		grpclog.Printf("grpclb: The naming watcher stops working due to error(%v).\n", err)
		return err
	}

	kb.Lock()
	defer kb.Unlock()

	for _, u := range us {
		switch u.Op {
		case naming.Add:
			kb.add(&server{addr: grpc.Address{Addr: u.Addr, Metadata: u.Metadata}})
		case naming.Delete:
			kb.delete(u.Addr)
		default:
			grpclog.Printf("grpclb: The name resolver provided an unsupported operation(%v).\n", u)
		}
	}

	if len(kb.servers) == 0 {
		if kb.waitCh == nil {
			kb.waitCh = make(chan struct{})
		}
	} else {
		if kb.waitCh != nil {
			close(kb.waitCh)
			kb.waitCh = nil
		}
	}

	select {
	case <-kb.addrsCh:
	default:
	}
	var as []grpc.Address
	for _, s := range kb.servers {
		as = append(as, s.addr)
	}
	kb.addrsCh <- as
	return nil
}

func (kb *ketamaBalance) Start(target string, _ grpc.BalancerConfig) (err error) {
	kb.Lock()
	defer kb.Unlock()

	if kb.done {
		return grpc.ErrClientConnClosing
	}
	if kb.w, err = kb.r.Resolve(target); err != nil {
		return
	}
	go func() {
		for {
			if err := kb.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return
}

func (kb *ketamaBalance) Up(addr grpc.Address) (down func(error)) {
	kb.RLock()
	defer kb.RUnlock()

	if server, ok := kb.servers[addr.Addr]; ok {
		server.connected.Set()
	}
	return func(err error) {
		kb.RLock()
		defer kb.RUnlock()

		grpclog.Errorf("grpclb: The connection to(%s) is lost due to error(%v).\n", addr.Addr, err)
		if server, ok := kb.servers[addr.Addr]; ok {
			server.connected.UnSet()
		}
	}
}

func (kb *ketamaBalance) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	if opts.BlockingWait {
		kb.RLock()
		ch := kb.waitCh
		kb.RUnlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-ch:
				// wait util there is a new registry server.
			}
		}
	}
	kb.RLock()
	defer kb.RUnlock()
	if kb.done {
		err = grpc.ErrClientConnClosing
		return
	}
	s, err := kb.get(ctx)
	if err != nil {
		return
	}
	addr = s.addr
	atomic.AddUint64(&s.currConns, 1)

	put = func() {
		kb.RLock()
		defer kb.RUnlock()

		if s != nil {
			atomic.AddUint64(&s.currConns, ^uint64(0))
		}
	}
	return
}

func (kb *ketamaBalance) Notify() <-chan []grpc.Address {
	return kb.addrsCh
}

func (kb *ketamaBalance) Close() error {
	kb.Lock()
	defer kb.Unlock()

	if kb.done {
		return errors.New("grpclb: Balancer is closed")
	}
	if kb.w != nil {
		kb.w.Close()
	}
	if kb.waitCh != nil {
		close(kb.waitCh)
		kb.waitCh = nil
	}
	if kb.addrsCh != nil {
		close(kb.addrsCh)
	}
	return nil
}
