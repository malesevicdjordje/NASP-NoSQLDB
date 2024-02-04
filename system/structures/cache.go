package structures

import "fmt"

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

// Helper functions

func (cache *LRUCache) updateExistingNode(node *CacheNode, existingValue []byte) {
	// Update the value in the map
	cache.values[node.Key] = node.Value

	if list := cache.list; node.Key != list.head.Key {
		// Move the existing node to the head of the list
		cache.moveNodeToHead(node.Key)
	}
}

func (cache *LRUCache) moveNodeToHead(key string) {
	list := cache.list
	current := list.head

	for current != nil {
		if current.Key == key {
			if current != list.head {
				// Remove the node from its current position
				current.Previous.Next = current.Next

				if current.Next != nil {
					current.Next.Previous = current.Previous
				} else {
					// Update the tail pointer if the node is the tail
					list.tail = current.Previous
				}

				// Add the node to the head of the list
				cache.addNodeToHead(current)
			}
			break
		}

		current = current.Next
	}
}

func (cache *LRUCache) evictLRUNode() {
	list := cache.list

	// Remove the tail node
	delete(cache.values, list.tail.Key)

	if list.size > 1 {
		// Update the tail pointer
		list.tail.Previous.Next = nil
		list.tail = list.tail.Previous
	} else {
		// Reset the list when only one node is present
		list.head = nil
		list.tail = nil
	}

	list.size--
}

func (cache *LRUCache) addNodeToHead(node *CacheNode) {
	list := cache.list

	// Add the new node to the head of the list
	if list.head == nil {
		list.head = node
		list.tail = node
	} else {
		node.Next = list.head
		list.head.Previous = node
		list.head = node
	}

	list.size++
}

func (cache *LRUCache) removeNode(key string, value []byte) {
	list := cache.list
	current := list.head

	for current != nil {
		if current.Key == key {
			// Remove the node from the list
			if current.Next != nil {
				current.Next.Previous = current.Previous
			} else {
				// Update the tail pointer if the node is the tail
				list.tail = current.Previous
			}

			if current.Previous != nil {
				current.Previous.Next = current.Next
			} else {
				// Update the head pointer if the node is the head
				list.head = current.Next
			}

			// Remove the node from the map
			delete(cache.values, key)

			list.size--
			break
		}

		current = current.Next
	}
}

func (cache *LRUCache) Put(key string, value []byte) {
	list := cache.list
	node := NewCacheNode(key, value)

	if existingValue, exists := cache.values[node.Key]; exists {
		// Update existing node
		cache.updateExistingNode(node, existingValue)
		return
	}

	// Add new node
	cache.values[node.Key] = node.Value

	if list.size == list.maxSize {
		// Evict the least recently used node
		cache.evictLRUNode()
	}

	// Add the new node to the head of the list
	cache.addNodeToHead(node)
}

func (cache *LRUCache) Get(key string) (bool, []byte) {
	if value, exists := cache.values[key]; exists {
		// Move accessed node to the head of the list
		cache.moveNodeToHead(key)
		return true, value
	}
	return false, nil
}

func (cache *LRUCache) Delete(key string) bool {
	if value, exists := cache.values[key]; exists {
		// Remove node from the list
		cache.removeNode(key, value)
		return true
	}
	return false
}

func (cache *LRUCache) Print() {
	list := cache.list
	fmt.Println("\nLinked List:")

	current := list.head
	for current != nil {
		fmt.Println(current.Key)
		current = current.Next
	}

	fmt.Println("\nMap:")
	for key, value := range cache.values {
		fmt.Printf("Key: %s, Value: %s\n", key, string(value))
	}
}
