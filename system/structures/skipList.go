package structures

import (
	"hash/crc32"
	"math/rand"
	"time"
)

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
		NextNodes: make([]*Element, maxLevel+1),
	}
	skipList := SkipList{maxLevel, 1, 1, &root}
	return &skipList
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (skipList *SkipList) generateLevel() int {
	level := 0

	for rand.Int31n(2) == 1 && level <= skipList.maxLvl {
		level++
	}

	if level > skipList.currLvl {
		skipList.currLvl = level
	}

	return level
}

func (skipList *SkipList) Insert(key string, value []byte, isTombstone bool) *Element {
	level := skipList.generateLevel()
	node := &Element{
		Checksum:  CRC32([]byte(key)),
		Timestamp: time.Now().String(),
		Tombstone: isTombstone,
		Key:       key,
		Value:     value,
		NextNodes: make([]*Element, level+1),
	}

	for i := skipList.currLvl - 1; i >= 0; i-- {
		current := skipList.head
		next := current.NextNodes[i]

		for next != nil && next.Key <= key {
			current = next
			next = current.NextNodes[i]
		}

		if i <= level {
			skipList.size++
			node.NextNodes[i] = next
			current.NextNodes[i] = node
		}
	}

	return node
}

func (skipList *SkipList) Delete(key string) *Element {
	current := skipList.head

	for i := skipList.currLvl - 1; i >= 0; i-- {
		next := current.NextNodes[i]

		for next != nil && next.Key <= key {
			current = next
			next = current.NextNodes[i]

			if current.Key == key {
				current.Tombstone = true
				current.Timestamp = time.Now().String()
				temp := current
				current = current.NextNodes[i]
				return temp
			}
		}
	}

	return nil
}

func (skipList *SkipList) Retrieve(key string) *Element {
	current := skipList.head

	for i := skipList.currLvl - 1; i >= 0; i-- {
		next := current.NextNodes[i]

		for next != nil && next.Key <= key {
			current = next
			next = current.NextNodes[i]

			if current.Key == key {
				return current
			}
		}
	}

	return nil
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
