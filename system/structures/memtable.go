package structures

type MemoryTable struct {
	skipList  SkipList
	size      uint
	threshold uint
	maxSize   uint
}

func NewMemoryTable(height int, maxSize, threshold uint) *MemoryTable {
	sl := CreateSkipList(height)
	mt := MemoryTable{*sl, 0, threshold, maxSize}
	return &mt
}
