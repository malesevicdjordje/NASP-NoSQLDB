package structures

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strconv"
)

const (
	WalPath         = "./system/data/wal/"
	CrcSize         = 4
	TimestampSize   = 19
	TombstoneSize   = 1
	KeySizeSize     = 8
	ValueSizeSize   = 8
	SegmentCapacity = 50
	LowWaterMark    = 0
)

func CalculateCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WalSegment struct {
	index    uint64
	data     []byte
	size     uint64
	capacity uint64
}

func (s *WalSegment) Index() uint64 {
	return s.index
}

func (s *WalSegment) Data() []byte {
	return s.data
}

func (s *WalSegment) AppendData(elemData []byte) int {
	for i := 0; i < len(elemData); i++ {
		if s.size >= s.capacity {
			return i
		}
		s.data = append(s.data, elemData[i])
		s.size++
	}
	return -1
}

func (s *WalSegment) Persist(walPath string) {
	path := walPath + "wal" + strconv.FormatUint(s.index, 10) + ".log"
	newFile, _ := os.Create(path)
	err := newFile.Close()
	if err != nil {
		fmt.Println(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	bufferedWriter := bufio.NewWriter(file)
	err = bufferedWriter.Flush()
	if err != nil {
		return
	}
	_, err = bufferedWriter.Write(s.data)
	err = bufferedWriter.Flush()
	if err != nil {
		return
	}
}

type WriteAheadLog struct {
	path           string
	lowWaterMark   uint
	segmentSize    uint
	segmentNames   map[uint64]string
	segments       []*WalSegment
	currentSegment *WalSegment
}

func (wal *WriteAheadLog) CurrentSegment() *WalSegment {
	return wal.currentSegment
}

func (wal *WriteAheadLog) Path() string {
	return wal.path
}

func NewWriteAheadLog(path string) *WriteAheadLog {
	wal := WriteAheadLog{
		path:           path,
		lowWaterMark:   LowWaterMark,
		segmentSize:    SegmentCapacity,
		segmentNames:   make(map[uint64]string),
		segments:       make([]*WalSegment, 0),
		currentSegment: &WalSegment{index: 0, data: nil, size: 0, capacity: SegmentCapacity},
	}
	return &wal
}

func (wal *WriteAheadLog) PersistCurrentSegment() {
	wal.currentSegment.Persist(wal.path)
	wal.segmentNames[wal.currentSegment.index] = "wal" + strconv.FormatUint(wal.currentSegment.index, 10) + ".log"
}

func (wal *WriteAheadLog) CreateNewSegment() {
	newSegment := WalSegment{
		index:    wal.currentSegment.index + 1,
		data:     make([]byte, 0, SegmentCapacity),
		size:     0,
		capacity: wal.currentSegment.capacity,
	}
	wal.PersistCurrentSegment()
	wal.segments = append(wal.segments, &newSegment)
	wal.currentSegment = &newSegment
	wal.PersistCurrentSegment()
}
