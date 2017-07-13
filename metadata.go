package grpclb

// Metadata meta type store into address.Metadata
type Metadata interface {
	Weight() weightLvl
}

// weightLvl the weight level of endpoint
type weightLvl int

func (w weightLvl) Weight() weightLvl {
	return w
}

const (
	Level1 weightLvl = (iota + 1) * 100
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

func getWeight(meta interface{}) weightLvl {
	var w weightLvl
	if m, ok := meta.(Metadata); ok {
		w = m.Weight()
	}
	if w == 0 {
		w = Level1
	}
	return w
}
