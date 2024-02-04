package structures

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
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

func (wal *WriteAheadLog) PutElement(elem *Element) bool {
	crc := make([]byte, CrcSize)
	binary.LittleEndian.PutUint32(crc, elem.Checksum)
	timestamp := make([]byte, TimestampSize)
	binary.LittleEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	tombstone := make([]byte, TombstoneSize)
	switch elem.Tombstone {
	case true:
		tombstone = []byte{1}
	case false:
		tombstone = []byte{0}
	}
	keySize := make([]byte, KeySizeSize)
	valueSize := make([]byte, ValueSizeSize)
	binary.LittleEndian.PutUint64(keySize, uint64(len(elem.Key)))
	binary.LittleEndian.PutUint64(valueSize, uint64(len(elem.Value)))

	key := []byte(elem.Key)
	value := elem.Value

	elemData := []byte{}
	elemData = append(elemData, crc...)
	elemData = append(elemData, timestamp...)
	elemData = append(elemData, tombstone...)
	elemData = append(elemData, keySize...)
	elemData = append(elemData, valueSize...)
	elemData = append(elemData, key...)
	elemData = append(elemData, value...)

	start := 0
	offset := 0
	for offset >= 0 {
		offset = wal.CurrentSegment().AppendData(elemData[start:])
		if offset != -1 {
			wal.CreateNewSegment()
			start += offset
		}
	}
	return true
}

func (wal *WriteAheadLog) ReadLatestSegment(path string) {
	files, err := ioutil.ReadDir(WalPath)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(files); i++ {
		indexStr := strings.Split(files[i].Name(), "wal")[1]
		indexStr = strings.Split(indexStr, ".log")[0]
		index, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			fmt.Println(err)
		}
		wal.segmentNames[index] = files[i].Name()
	}

	maxIndex := uint64(0)
	for key := range wal.segmentNames {
		if maxIndex < key {
			maxIndex = key
		}
	}
	index := maxIndex
	current := wal.segmentNames[index]

	file, err := os.Open(path + current)
	if err != nil {
		fmt.Println(err)
	}

	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}

	bufferedReader := bufio.NewReader(file)
	info, err := os.Stat(file.Name())
	if err != nil {
		fmt.Println(err)
	}
	numBytes := info.Size()

	bytes := make([]byte, numBytes)
	_, err = bufferedReader.Read(bytes)

	currentSegment := WalSegment{
		index:    index,
		data:     nil,
		size:     0,
		capacity: SegmentCapacity,
	}
	currentSegment.AppendData(bytes)

	wal.currentSegment = &currentSegment
	wal.segments = append(wal.segments, &currentSegment)

	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}
}
