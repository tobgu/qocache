package cache

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type Cache interface {
	Put(key string, item interface{}, byteSize int) error
	Get(key string) (interface{}, bool)
	Stats() CacheStats
}

// 16 bytes for string head
// 8 for pointer to element
// 40 bytes map entry overhead estimate for now (see https://stackoverflow.com/questions/15313105/memory-overhead-of-maps-in-go)
const mapEntrySize = 16 + 8 + 40

const maxStatHistory = 1000

type LruCache struct {
	lock            *sync.Mutex              // 8 byte
	keyMap          map[string]*list.Element // mapEntrySize / entry
	lruList         *list.List               // 8 + 40 byte
	maxSize         int                      // 8 byte
	currentSize     int                      // 8 byte
	maxAge          time.Duration            // 8 byte
	timesToEviction []time.Duration          // 16 byte
	lastStat        time.Time                // 8 byte
}

type cacheEntry struct {
	// 40 byte overhead for the Element
	item       interface{} // 16 byte
	createTime time.Time   // 8 byte
	key        string      // 16 byte + string length
	size       int         // 8 byte
	// ~ 88 byte + string length
}

func newCacheEntry(key string, item interface{}, itemSize int) cacheEntry {
	return cacheEntry{
		item:       item,
		createTime: time.Now(),
		key:        key,
		// See struct definition for the reasoning behind the below numbers
		size: 40 + 16 + 8 + 16 + 8 + len(key) + itemSize + mapEntrySize,
	}
}

func (ce *cacheEntry) hasExpired(maxAge time.Duration) bool {
	return maxAge > 0 && time.Now().Sub(ce.createTime) > maxAge
}

func (c *LruCache) Put(key string, item interface{}, byteSize int) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.keyMap[key]; ok {
		c.remove(elem)
	}

	newEntry := newCacheEntry(key, item, byteSize)

	// Evict old entries if needed to fit new entry in cache
	for c.currentSize+newEntry.size > c.maxSize {
		elem := c.lruList.Back()
		removed := c.remove(elem)
		if !removed {
			return fmt.Errorf("cannot fit %d bytes in cache", newEntry.size)
		}
	}

	elem := c.lruList.PushFront(newEntry)
	c.keyMap[key] = elem
	c.currentSize += newEntry.size
	return nil
}

func (c *LruCache) Get(key string) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	elem, ok := c.keyMap[key]
	if !ok {
		return nil, false
	}

	entry := elem.Value.(cacheEntry)
	if entry.hasExpired(c.maxAge) {
		c.remove(elem)
		return nil, false
	}

	c.lruList.MoveToFront(elem)
	return entry.item, true
}

type CacheStats struct {
	TimeToEviction []time.Duration
	ByteSize       int
	ItemCount      int
	StatDuration   time.Duration
}

func (c *LruCache) Stats() CacheStats {
	c.lock.Lock()
	defer c.lock.Unlock()

	lastStat := time.Now()
	stat := CacheStats{
		TimeToEviction: c.timesToEviction,
		ByteSize:       c.currentSize,
		ItemCount:      len(c.keyMap),
		StatDuration:   lastStat.Sub(c.lastStat),
	}
	c.lastStat = lastStat
	c.timesToEviction = make([]time.Duration, 0, len(c.timesToEviction))

	return stat
}

// Returns true if the element is removed, false otherwise
func (c *LruCache) remove(elem *list.Element) bool {
	if elem == nil {
		return false
	}

	entry := elem.Value.(cacheEntry)
	timeToEviction := time.Now().Sub(entry.createTime)
	if len(c.timesToEviction) <= maxStatHistory {
		c.timesToEviction = append(c.timesToEviction, timeToEviction)
	}

	delete(c.keyMap, entry.key)
	c.lruList.Remove(elem)
	c.currentSize -= entry.size
	return true
}

// Don't allow cache sizes less than 1 Mb to avoid edge cases
// with very small caches.
const minMaxSize = 1000000

func New(maxSize int, maxAge time.Duration) *LruCache {
	if maxSize <= minMaxSize {
		maxSize = minMaxSize
	}

	return &LruCache{
		lock:    &sync.Mutex{},
		keyMap:  make(map[string]*list.Element),
		lruList: list.New(),
		maxSize: maxSize,
		maxAge:  maxAge,
		// Rough estimate of the overhead of this structure
		currentSize: 60,
		lastStat:    time.Now()}
}

// TODO: Make thread safety optional?
// TODO: In addition to byte size make it possible to set a maxCount
// TODO: Make maximum history size configurable
// TODO: Move to own repo?
