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

func (e *Engine) Init() {
	e.Config = config.GetSystemConfig()
	e.Wal = structures.NewWriteAheadLog(structures.WalPath)
	e.memTable = structures.NewMemoryTable(e.Config.MemTableParameters.SkipListMaxHeight,
		uint(e.Config.MemTableParameters.MaxMemTableSize),
		uint(e.Config.MemTableParameters.MemTableThreshold))
	e.cache = structures.NewLRUCache(e.Config.CacheParameters.CacheMaxData)
	e.lsm = structures.NewLSMTree(e.Config.LSMParameters.LSMMaxLevel, e.Config.LSMParameters.LSMLevelSize)
	rate := int64(e.Config.TokenBucketParameters.TokenBucketInterval)
	e.TokenBucket = structures.NewRateLimiter(rate, e.Config.TokenBucketParameters.TokenBucketMaxTokens)
}
