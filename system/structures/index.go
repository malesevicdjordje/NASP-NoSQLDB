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
