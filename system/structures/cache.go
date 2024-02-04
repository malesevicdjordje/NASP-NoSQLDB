package structures

type CacheNode struct {
	Key      string
	Value    []byte
	Next     *CacheNode
	Previous *CacheNode
}

func NewCacheNode(key string, value []byte) *CacheNode {
	return &CacheNode{
		Key:      key,
		Value:    value,
		Next:     nil,
		Previous: nil,
	}
}

type DoublyLinkedList struct {
	size    int
	head    *CacheNode
	tail    *CacheNode
	maxSize int
}

type LRUCache struct {
	list   *DoublyLinkedList
	values map[string][]byte
}

func NewLRUCache(maxSize int) *LRUCache {
	list := &DoublyLinkedList{
		size:    0,
		head:    nil,
		tail:    nil,
		maxSize: maxSize,
	}

	values := make(map[string][]byte)

	return &LRUCache{
		list:   list,
		values: values,
	}
}
