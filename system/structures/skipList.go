package structures

import "time"

type SkipList struct {
	maxLvl  int
	currLvl int
	size    int
	head    *Element
}

func CreateSkipList(maxLevel int) *SkipList {
	root := Element{
		Checksum:  CRC32([]byte("head")),
		Timestamp: time.Now().String(),
		Tombstone: false,
		Key:       "head",
		Value:     nil,
		Next:      make([]*Element, maxLevel+1),
	}
	skipList := SkipList{maxLevel, 1, 1, &root}
	return &skipList
}
