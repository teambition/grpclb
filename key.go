package grpclb

import (
	"hash/fnv"
	"strconv"

	"golang.org/x/net/context"
)

// Hasher hash method implemention
type Hasher interface {
	// Hash32 uint32 result
	Hash32() uint32
}

type anyType struct {
	hash32 uint32
}

func newAnyType(value interface{}) (Hasher, bool) {
	var data []byte
	switch v := value.(type) {
	case string:
		data = []byte(v)
	case uint32:
		data = []byte(strconv.FormatUint(uint64(v), 10))
	default:
		return nil, false
	}
	h := fnv.New32a()
	h.Write(data)
	return &anyType{hash32: h.Sum32()}, true
}

// Hash32 Hasher implement with fnv algorithm.
func (a *anyType) Hash32() uint32 {
	return a.hash32
}

// fromContext get Hasher from Context by key.
func fromContext(ctx context.Context, key interface{}) (Hasher, bool) {
	return newAnyType(ctx.Value(key))
}

// ToContext set Hasher into Context.
func ToContext(ctx context.Context, key, val interface{}) context.Context {
	return context.WithValue(ctx, key, val)
}
