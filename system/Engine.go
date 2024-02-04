package system

import (
	"KVSystem/config"
	"KVSystem/system/structures"
	"fmt"
	"time"
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

func (e *Engine) Put(key string, value []byte, tombstone bool) bool {

	elem := structures.Element{
		Key:       key,
		Value:     value,
		NextNodes: nil,
		Timestamp: time.Now().String(),
		Tombstone: tombstone,
		Checksum:  structures.CRC32(value),
	}
	e.Wal.PutElement(&elem)
	e.memTable.Insert(key, value, tombstone)
	e.cache.Put(key, value)

	if e.memTable.ShouldFlush() {
		e.memTable.PerformFlush()
		e.Wal.RemoveOldSegments()
		e.lsm.PerformCompaction("system/data/sstable/", 1)
		e.memTable = structures.NewMemoryTable(e.Config.MemTableParameters.SkipListMaxHeight,
			uint(e.Config.MemTableParameters.MaxMemTableSize),
			uint(e.Config.MemTableParameters.MemTableThreshold))
	}

	return true
}

func (e *Engine) Get(key string) (bool, []byte) {
	ok, deleted, value := e.memTable.Lookup(key)
	if ok && deleted {
		return false, nil
	} else if ok {
		e.cache.Put(key, value)
		//fmt.Println("Found in memtable.")
		return true, value
	}
	ok, value = e.cache.Get(key)
	if ok {
		//fmt.Println("Found in cache.")
		e.cache.Put(key, value)
		return true, value
	}
	ok, value = structures.SearchThroughSSTables(key, e.Config.LSMParameters.LSMMaxLevel)
	if ok {
		//fmt.Println("Found in sstable.")
		e.cache.Put(key, value)
		//if strings.Trim(string(value), " ") != "" {
		//	return true, value
		//}
		return true, value
	}
	return false, nil
}

func (e *Engine) Delete(key string) bool {
	if e.memTable.Erase(key) {
		e.cache.Delete(key)
		return true
	}
	if e.memTable.Erase("hll-" + key) {
		e.cache.Delete("hll-" + key)
		return true
	}
	if e.memTable.Erase("cms-" + key) {
		e.cache.Delete("cms-" + key)
		return true
	}
	ok, value := e.Get(key)
	if !ok {
		keyHLL := "hll-" + key
		ok, value = e.Get(keyHLL)
		if !ok {
			keyCMS := "cms-" + key
			ok, value = e.Get(keyCMS)
			if !ok {
				return false
			} else {
				key = keyCMS
			}
		} else {
			key = keyHLL
		}
	}
	e.Put(key, value, true)
	e.cache.Delete(key)
	return true
}

func (e *Engine) Edit(key string, value []byte) bool {
	e.memTable.Modify(key, value, false)
	elem := structures.Element{
		Key:       key,
		Value:     value,
		NextNodes: nil,
		Timestamp: time.Now().String(),
		Tombstone: false,
		Checksum:  structures.CRC32(value),
	}

	e.Wal.PutElement(&elem)

	e.cache.Put(key, value)

	return true
}

func (e *Engine) GetAsString(key string) string {
	ok, val := e.Get(key)
	var value string
	if !ok {
		ok, val = e.Get("hll-" + key)
		if ok {
			hll := structures.DeserializeHLL(val)
			value = "It's a HLL with Estimation: " + fmt.Sprintf("%f", hll.Evaluate())
		} else {
			ok, val = e.Get("cms-" + key)
			if ok {
				value = "It's a CMS"
			}
			if !ok {
				value = "Data with given key does not exist !"
			}
		}
	} else {
		value = string(val)
	}
	return value
}
