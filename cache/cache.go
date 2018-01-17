package cache

import (
	qf "github.com/tobgu/qframe"
	"sync"
)

type Cache interface {
	Put(key string, frame qf.QFrame)
	Get(key string) (qf.QFrame, bool)
}

type mapCache struct {
	lock   *sync.Mutex
	theMap map[string]qf.QFrame
}

func (c *mapCache) Put(key string, frame qf.QFrame) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.theMap[key] = frame
}

func (c *mapCache) Get(key string) (qf.QFrame, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	f, ok := c.theMap[key]
	return f, ok
}

func New() *mapCache {
	return &mapCache{lock: &sync.Mutex{}, theMap: make(map[string]qf.QFrame)}
}
