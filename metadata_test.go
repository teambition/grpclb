package grpclb

import (
	"testing"

	"google.golang.org/grpc/naming"
)

func Test_weightFromUpdate(t *testing.T) {
	type args struct {
		u *naming.Update
	}
	tests := []struct {
		name string
		args args
		want WeightLvl
	}{
		// TODO: Add test cases.
		{"get weigth lvl1 from metadata", args{&naming.Update{Addr: "", Metadata: float64(Level1)}}, Level1},
		{"get weigth lvl10 from metadata", args{&naming.Update{Addr: "", Metadata: float64(Level10)}}, Level10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := weightFromUpdate(tt.args.u); got != tt.want {
				t.Errorf("weightFromUpdate() = %v, want %v", got, tt.want)
			}
		})
	}
}
