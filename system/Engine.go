package system

import (
	"KVSystem/config"
	"KVSystem/system/structures"
)

type Engine struct {
	Wal         *structures.WriteAheadLog
	memTable    *structures.MemoryTable
	cache       *structures.LRUCache
	lsm         *structures.LSMTree
	TokenBucket *structures.RateLimiter
	Config      *config.Config
}
