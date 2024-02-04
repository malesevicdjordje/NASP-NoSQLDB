package structures

type LSMTree struct {
	maxLevel int
	maxSize  int
}

func NewLSMTree(maxLevels, maxSize int) *LSMTree {
	return &LSMTree{
		maxLevel: maxLevels,
		maxSize:  maxSize,
	}
}
