package structures

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

// Index defines an interface for basic indexing operations.
type Index interface {
	Add(key string, offset uint)
	Search(key string, startOffset int64) (found bool, dataOffset int64)
	WriteToFile() (keys []string, offsets []uint)
}

// SimpleIndex represents a basic index structure.
type SimpleIndex struct {
	OffsetSize    uint
	KeySizeNumber uint
	Keys          []string
	Offsets       []uint
	FileName      string
}

// NewSimpleIndex creates a new SimpleIndex instance.
func NewSimpleIndex(keys []string, offsets []uint, fileName string) *SimpleIndex {
	index := SimpleIndex{FileName: fileName}
	for i, key := range keys {
		index.Add(key, offsets[i])
	}
	return &index
}

// Add adds a key and its corresponding offset to the index.
func (index *SimpleIndex) Add(key string, offset uint) {
	index.Keys = append(index.Keys, key)
	index.Offsets = append(index.Offsets, offset)
}

// Search finds a key in the index and returns its existence status and data offset.
func (index *SimpleIndex) Search(key string, startOffset int64) (found bool, dataOffset int64) {
	found = false
	dataOffset = 0

	file, err := os.Open(index.FileName)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	bytes := make([]byte, 8)
	_, err = reader.Read(bytes)
	if err != nil {
		log.Panic(err)
	}
	fileLen := binary.LittleEndian.Uint64(bytes)

	_, err = file.Seek(startOffset, 0)
	if err != nil {
		return false, 0
	}

	reader = bufio.NewReader(file)

	var i uint64
	for i = 0; i < fileLen; i++ {
		bytes := make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			log.Panic(err)
		}
		keyLen := binary.LittleEndian.Uint64(bytes)

		bytes = make([]byte, keyLen)
		_, err = reader.Read(bytes)
		if err != nil {
			log.Panic(err)
		}
		nodeKey := string(bytes[:])

		if nodeKey == key {
			found = true
		} else if nodeKey > key {
			return false, 0
		}

		bytes = make([]byte, 8)
		_, err = reader.Read(bytes)
		if err != nil {
			log.Panic(err)
		}
		newOffset := binary.LittleEndian.Uint64(bytes)

		if found {
			dataOffset = int64(newOffset)
			break
		}
	}

	return
}
