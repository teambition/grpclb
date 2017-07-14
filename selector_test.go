package grpclb

import (
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

func Test_ketama_Update(t *testing.T) {
	type args struct {
		u naming.Update
	}
	u1 := naming.Update{Op: naming.Add, Addr: "host1"}
	k1 := newKetama()

	u2 := naming.Update{Op: naming.Add, Addr: "host2"}
	k2 := newKetama()
	k2.Update(u2)

	u3 := naming.Update{Op: naming.Delete, Addr: "host3"}
	k3 := newKetama()
	k3.Update(naming.Update{Op: naming.Add, Addr: "host3"})

	u4 := naming.Update{Op: naming.Delete, Addr: "host4"}
	k4 := newKetama()

	tests := []struct {
		name    string
		k       *ketama
		args    args
		wantErr bool
	}{
		{"add a unexisted address", k1, args{u1}, false},
		{"add an existed address", k2, args{u2}, true},
		{"delete an existed address", k3, args{u3}, false},
		{"delete a unexisted address", k4, args{u4}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.k.Update(tt.args.u); (err != nil) != tt.wantErr {
				t.Errorf("ketama.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_ketama_Servers(t *testing.T) {
	k1 := newKetama()
	w1 := []grpc.Address{}

	k2 := newKetama()
	k2.Update(naming.Update{Op: naming.Add, Addr: "host2.1"})
	k2.Update(naming.Update{Op: naming.Add, Addr: "host2.2"})
	w2 := []grpc.Address{{Addr: "host2.1"}, {Addr: "host2.2"}}

	k3 := newKetama()
	k3.Update(naming.Update{Op: naming.Add, Addr: "host3.1"})
	k3.Update(naming.Update{Op: naming.Add, Addr: "host3.1"})
	w3 := []grpc.Address{{Addr: "host3.1"}}

	k4 := newKetama()
	k4.Update(naming.Update{Op: naming.Add, Addr: "host4.1"})
	k4.Update(naming.Update{Op: naming.Delete, Addr: "host4.1"})
	w4 := []grpc.Address{}

	tests := []struct {
		name string
		k    *ketama
		want []grpc.Address
	}{
		{"no servers", k1, w1},
		{"one server", k2, w2},
		{"add servers repeat", k3, w3},
		{"add then delete server", k4, w4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.Servers(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ketama.Servers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ketama_Get(t *testing.T) {
	type args struct {
		h Hasher
	}
	u1 := naming.Update{Op: naming.Add, Addr: "host1", Metadata: Level1}
	u2 := naming.Update{Op: naming.Add, Addr: "host2", Metadata: Level10}
	u3 := naming.Update{Op: naming.Add, Addr: "host3", Metadata: Level1}

	k := newKetama()
	k.Update(u1)
	k.Update(u2)
	k.Update(u3)

	tests := []struct {
		name    string
		k       *ketama
		args    args
		want    grpc.Address
		wantErr bool
	}{
		{"get hahser 1", k, args{&strOrNum{1}}, grpc.Address{Addr: "host2", Metadata: Level10}, false},
		{"get hahser 2", k, args{&strOrNum{11232984}}, grpc.Address{Addr: "host3", Metadata: Level1}, false},
		{"get hahser 3", k, args{&strOrNum{487378432}}, grpc.Address{Addr: "host2", Metadata: Level10}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.k.Get(tt.args.h)
			if (err != nil) != tt.wantErr {
				t.Errorf("ketama.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ketama.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
