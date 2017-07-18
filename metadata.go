package grpclb

import (
	"github.com/tevino/abool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

type server struct {
	addr      grpc.Address
	connected abool.AtomicBool
	currConns uint64
}

// AddServer new an add Update event for server registry
func AddServer(addr string, weight WeightLvl) naming.Update {
	return naming.Update{
		Op:   naming.Add,
		Addr: addr,
		// because of the grpc-go lib user Address as map's key,
		// so the Metadata can't be a interface{} type,
		// if do then thrown an error about unhashabled key type.
		Metadata: weight,
	}
}

// DeleteServer new a delete Update event for server registry
func DeleteServer(addr string) naming.Update {
	return naming.Update{
		Op:   naming.Delete,
		Addr: addr,
	}
}

// WeightLvl the weight level of endpoint
type WeightLvl int

const (
	Level1 WeightLvl = (iota + 1) * 100
	Level2
	Level3
	Level4
	Level5
	Level6
	Level7
	Level8
	Level9
	Level10
)

func weightFromMetadata(meta interface{}) WeightLvl {
	var w WeightLvl
	if m, ok := meta.(float64); ok {
		w = WeightLvl(m)
	}
	if w <= Level1 {
		w = Level1
	}
	return w
}
