package grpclb

import "google.golang.org/grpc/naming"

// NewAddUpdate new an add Update event for server registry
func NewAddUpdate(addr string, weight WeightLvl) naming.Update {
	return naming.Update{
		Op:   naming.Add,
		Addr: addr,
		// because of the grpc-go lib user Address as map's key,
		// so the Metadata can't be a interface{} type,
		// if do then thrown an error about unhashabled key type.
		Metadata: weight,
	}
}

// NewDeleteUpdate new a delete Update event for server registry
func NewDeleteUpdate(addr string) naming.Update {
	return naming.Update{
		Op:   naming.Delete,
		Addr: addr,
	}
}
