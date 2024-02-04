package structures

import "hash/crc32"

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
