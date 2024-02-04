package structures

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
