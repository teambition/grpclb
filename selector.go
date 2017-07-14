package grpclb

import (
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

var _ ServerSelector = new(ketama)

var (
	// ErrNoServer when no server has found then return error.
	ErrNoServer = errors.New("server selector: There is no server")
	// ErrServerExisted the server has existed when add to the ServerSelector.
	ErrServerExisted = errors.New("server selector: Server has existed")
	// ErrServerNotExisted the server has not existed when delete from ServerSelector.
	ErrServerNotExisted = errors.New("server selector: Server has not existed")
	// ErrUnsupportOp operation must be in Add or Delete
	ErrUnsupportOp = errors.New("server selector: Unsupport operation, must be one of Add or Delete")
)

// ServerSelector decide which server can be used.
type ServerSelector interface {
	// Update servers.
	Update(naming.Update) error
	// Servers get all servers in the ServerSelector.
	Servers() []grpc.Address
	// Get the target Address by Hasher
	// which is a hash factor for ensure the same key gets the same Node.
	Get(Hasher) (grpc.Address, error)
}

// ketama consisten-hash implment with ketama algorithm.
type ketama struct {
	sync.RWMutex
	servers       []*grpc.Address
	replica       map[uint32]*grpc.Address
	sortedHashSet []uint32
}

// newKetama init ketema consistent-hash
func newKetama() *ketama {
	return &ketama{
		servers:       []*grpc.Address{},
		replica:       map[uint32]*grpc.Address{},
		sortedHashSet: []uint32{},
	}
}

func (k *ketama) add(a *grpc.Address) {
	weight := getWeight(a.Metadata)
	step := math.MaxUint32 / weight
	k.servers = append(k.servers, a)

	h := fnv.New32a()
	h.Write([]byte(a.Addr))
	addrHash := h.Sum32()
	h.Reset()

	for i := 1; i <= int(weight); i++ {
		h.Write([]byte(strconv.FormatUint(uint64(addrHash)+uint64(i)*uint64(step), 10)))
		tmpH := h.Sum32()
		k.sortedHashSet = append(k.sortedHashSet, tmpH)
		k.replica[tmpH] = a
		h.Reset()
	}

	sort.Slice(k.sortedHashSet, func(i int, j int) bool {
		return k.sortedHashSet[i] < k.sortedHashSet[j]
	})
}

func (k *ketama) delHash(val uint32) {
	for i := 0; i < len(k.sortedHashSet); i++ {
		if k.sortedHashSet[i] == val {
			k.sortedHashSet = append(k.sortedHashSet[:i], k.sortedHashSet[i+1:]...)
		}
	}
}

func (k *ketama) delete(a *grpc.Address) {
	for i := 0; i < len(k.servers); i++ {
		if k.servers[i].Addr == a.Addr {
			k.servers = append(k.servers[:i], k.servers[i+1:]...)
			break
		}
	}
	for h, r := range k.replica {
		if r.Addr == a.Addr {
			delete(k.replica, h)
			k.delHash(h)
		}
	}
}

func (k *ketama) checkExist(u *naming.Update) bool {
	for _, r := range k.servers {
		if r.Addr == u.Addr {
			return true
		}
	}
	return false
}

func (k *ketama) Update(u naming.Update) error {
	k.Lock()
	defer k.Unlock()

	a := &grpc.Address{Addr: u.Addr, Metadata: u.Metadata}
	switch u.Op {
	case naming.Add:
		if k.checkExist(&u) {
			return ErrServerExisted
		}
		k.add(a)
	case naming.Delete:
		if !k.checkExist(&u) {
			return ErrServerNotExisted
		}
		k.delete(a)
	default:
		return ErrUnsupportOp
	}
	return nil
}

func (k *ketama) Servers() []grpc.Address {
	k.RLock()
	defer k.RUnlock()

	as := make([]grpc.Address, 0, len(k.servers))
	for _, s := range k.servers {
		as = append(as, *s)
	}
	return as
}

func (k *ketama) Get(h Hasher) (grpc.Address, error) {
	k.RLock()
	defer k.RUnlock()

	length := len(k.sortedHashSet)
	if length == 0 {
		return grpc.Address{}, ErrNoServer
	}
	hash32 := h.Hash32()
	idx := sort.Search(length, func(i int) bool {
		return k.sortedHashSet[i] >= hash32
	})
	if idx >= length {
		idx = 0
	}
	n := k.replica[k.sortedHashSet[idx]]
	return *n, nil
}
