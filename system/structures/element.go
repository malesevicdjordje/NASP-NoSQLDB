package structures

type Element struct {
	Checksum  uint32
	Timestamp string
	Tombstone bool
	Key       string
	Value     []byte
	NextNodes []*Element
}
