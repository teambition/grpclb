package grpclb

import "testing"

func Test_getWeight(t *testing.T) {
	type args struct {
		meta interface{}
	}
	tests := []struct {
		name string
		args args
		want WeightLvl
	}{
		{"get weigth lvl1 from metadata", args{float64(Level1)}, Level1},
		{"get weigth lvl10 from metadata", args{float64(Level10)}, Level10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWeight(tt.args.meta); got != tt.want {
				t.Errorf("getWeight() = %v, want %v", got, tt.want)
			}
		})
	}
}
