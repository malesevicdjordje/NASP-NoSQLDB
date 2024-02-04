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

func (mt *MemoryTable) Modify(key string, value []byte, isTombstone bool) {
	node := mt.skipList.Retrieve(key)
	if node == nil {
		mt.Insert(key, value, isTombstone)
	} else {
		node.Value = value
	}
}

func (mt *MemoryTable) Erase(key string) bool {
	removedElement := mt.skipList.Delete(key)
	return removedElement != nil
}

func (mt *MemoryTable) Lookup(key string) (found, deleted bool, value []byte) {
	node := mt.skipList.Retrieve(key)
	if node == nil {
		found, deleted, value = false, false, nil
	} else if node.Tombstone {
		found, deleted, value = true, true, nil
	} else {
		found, deleted, value = true, false, node.Value
	}
	return
}

func (mt *MemoryTable) CurrentSize() uint {
	return mt.size
}

func (mt *MemoryTable) PerformFlush() {
	filename := findSSTableFilename("1")
	NewSSTable(*mt, filename)
}

func (mt *MemoryTable) ShouldFlush() bool {
	usagePercentage := float64(mt.size) / float64(mt.maxSize) * 100
	return usagePercentage >= float64(mt.threshold)
}
