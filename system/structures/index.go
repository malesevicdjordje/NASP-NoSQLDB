package structures

import (
	"bufio"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
)

// Index defines an interface for basic indexing operations.
type Index interface {
	Search(key string, startOffset int64) (found bool, dataOffset int64)
	Add(key string, offset uint)
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

// Search finds a key in the index and returns its existence status and data offset.
func (index *SimpleIndex) SearchIndex(key string, startOffset int64, filename string) (found bool, dataOffset int64) {
	found = false
	dataOffset = 0

	file, err := os.Open(filename)
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

// Add adds a key and its corresponding offset to the index.
func (index *SimpleIndex) Add(key string, offset uint) {
	index.Keys = append(index.Keys, key)
	index.Offsets = append(index.Offsets, offset)
}

// WriteToFile writes the index data to a file and returns keys and offsets.
func (index *SimpleIndex) WriteToFile() (keys []string, offsets []uint) {
	currentOffset := uint(0)
	file, err := os.Create(index.FileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	bytesLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesLen, uint64(len(index.Keys)))
	bytesWritten, err := writer.Write(bytesLen)
	if err != nil {
		log.Fatal(err)
	}

	currentOffset += uint(bytesWritten)

	err = writer.Flush()
	if err != nil {
		return
	}

	rangeKeys := make([]string, 0)
	rangeOffsets := make([]uint, 0)
	sampleKeys := make([]string, 0)
	sampleOffsets := make([]uint, 0)
	for i := range index.Keys {
		key := index.Keys[i]
		offset := index.Offsets[i]
		if i == 0 || i == (len(index.Keys)-1) {
			rangeKeys = append(rangeKeys, key)
			rangeOffsets = append(rangeOffsets, currentOffset)
		} else if rand.Intn(100) > 50 {
			sampleKeys = append(sampleKeys, key)
			sampleOffsets = append(sampleOffsets, currentOffset)
		}
		bytes := []byte(key)

		keyLen := uint64(len(bytes))
		bytesLen := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytesLen, keyLen)
		bytesWritten, err := writer.Write(bytesLen)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(offset))
		bytesWritten, err = writer.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		currentOffset += uint(bytesWritten)
	}

	err = writer.Flush()
	if err != nil {
		return
	}

	keys = append(rangeKeys, sampleKeys...)
	offsets = append(rangeOffsets, sampleOffsets...)
	return
}
