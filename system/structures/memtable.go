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

func (mt *MemoryTable) Insert(key string, value []byte, isTombstone bool) {
	mt.size++
	mt.skipList.Insert(key, value, isTombstone)
}

func (mt *MemoryTable) Erase(key string) bool {
	removedElement := mt.skipList.Delete(key)
	return removedElement != nil
}

func (mt *MemoryTable) Modify(key string, value []byte, isTombstone bool) {
	node := mt.skipList.Retrieve(key)
	if node == nil {
		mt.Insert(key, value, isTombstone)
	} else {
		node.Value = value
	}
}
