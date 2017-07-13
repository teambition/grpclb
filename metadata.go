package grpclb

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

func getWeight(meta interface{}) WeightLvl {
	var w WeightLvl
	if m, ok := meta.(float64); ok {
		w = WeightLvl(m)
	}
	if w <= Level1 {
		w = Level1
	}
	return w
}
